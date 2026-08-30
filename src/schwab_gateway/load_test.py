"""Bounded, read-only full-session load driver for the gateway HTTP contract."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import signal
import statistics
import time
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from schwab_gateway_sdk.client import (
    GatewayAuthenticationError,
    GatewayAuthorizationError,
    GatewayCapacityError,
    GatewayMarketDataClient,
    GatewayResponseError,
    GatewayTimeoutError,
    GatewayUnavailableError,
)

UTC = dt.timezone.utc
MAX_DURATION_SECONDS = 86_400.0
MAX_CONCURRENCY = 32
Endpoint = Literal["spot", "option_chain", "history"]


@dataclass(frozen=True)
class MonitorWindow:
    symbol: str
    start_seconds: float
    duration_seconds: float


@dataclass(frozen=True)
class EntryBurst:
    symbol: str
    start_seconds: float
    requests: int = 3


@dataclass(frozen=True)
class LoadTestConfig:
    base_url: str
    expiration: dt.date
    duration_seconds: float
    output_root: Path
    symbols: tuple[str, ...] = ("SPX", "NDX", "XSP")
    collector_interval_seconds: float = 60.0
    monitor_interval_seconds: float = 2.0
    max_concurrency: int = 6
    timeout_seconds: float = 5.0
    monitor_windows: tuple[MonitorWindow, ...] = ()
    entry_bursts: tuple[EntryBurst, ...] = ()
    api_key_environment: str = "SCHWAB_GATEWAY_API_KEY"

    def __post_init__(self) -> None:
        symbols = tuple(dict.fromkeys(value.strip().upper() for value in self.symbols))
        if not symbols or any(not value for value in symbols):
            raise ValueError("at least one non-empty symbol is required")
        object.__setattr__(self, "symbols", symbols)
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must use http or https")
        if not 0 < self.duration_seconds <= MAX_DURATION_SECONDS:
            raise ValueError("duration must be greater than zero and at most 86400 seconds")
        if self.collector_interval_seconds <= 0 or self.monitor_interval_seconds <= 0:
            raise ValueError("request intervals must be greater than zero")
        if not 1 <= self.max_concurrency <= MAX_CONCURRENCY:
            raise ValueError("max_concurrency must be between 1 and 32")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        for window in self.monitor_windows:
            if window.symbol.upper() not in symbols:
                raise ValueError(f"monitor symbol {window.symbol!r} is not configured")
            if window.start_seconds < 0 or window.duration_seconds <= 0:
                raise ValueError(
                    "monitor windows require a nonnegative start and positive duration"
                )
            if window.start_seconds + window.duration_seconds > self.duration_seconds:
                raise ValueError("monitor window exceeds bounded session duration")
        for burst in self.entry_bursts:
            if burst.symbol.upper() not in symbols:
                raise ValueError(f"burst symbol {burst.symbol!r} is not configured")
            start_in_session = 0 <= burst.start_seconds <= self.duration_seconds
            if not start_in_session or not 1 <= burst.requests <= 20:
                raise ValueError(
                    "entry bursts require an in-session start and 1 through 20 requests"
                )


@dataclass(frozen=True, order=True)
class PlannedRequest:
    offset_seconds: float
    sequence: int
    stage: str
    endpoint: Endpoint
    symbol: str


def build_schedule(config: LoadTestConfig) -> tuple[PlannedRequest, ...]:
    """Build the complete stable schedule before any network activity begins."""
    jobs: list[PlannedRequest] = []
    sequence = 0

    def add(offset: float, stage: str, endpoint: Endpoint, symbol: str) -> None:
        nonlocal sequence
        jobs.append(PlannedRequest(round(offset, 6), sequence, stage, endpoint, symbol.upper()))
        sequence += 1

    # Warm each collector-facing cache once. History is intentionally warm-up only.
    for symbol in config.symbols:
        for endpoint in ("spot", "history", "option_chain"):
            add(0.0, "warmup", endpoint, symbol)

    offset = config.collector_interval_seconds
    while offset < config.duration_seconds:
        for symbol in config.symbols:
            add(offset, "collector", "spot", symbol)
            add(offset, "collector", "option_chain", symbol)
        offset += config.collector_interval_seconds

    for window in config.monitor_windows:
        offset = window.start_seconds
        end = window.start_seconds + window.duration_seconds
        while offset < end:
            add(offset, "monitor", "option_chain", window.symbol)
            offset += config.monitor_interval_seconds

    for burst in config.entry_bursts:
        for _ in range(burst.requests):
            add(burst.start_seconds, "entry_burst", "option_chain", burst.symbol)
    return tuple(sorted(jobs))


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _safe_config(config: LoadTestConfig) -> dict[str, Any]:
    value = asdict(config)
    value["expiration"] = config.expiration.isoformat()
    value["output_root"] = str(config.output_root.resolve())
    value["api_key"] = "not_recorded"
    return value


def _failure(exc: Exception) -> tuple[int | None, str, str]:
    mapping: tuple[tuple[type[Exception], int | None, str], ...] = (
        (GatewayAuthenticationError, 401, "authentication"),
        (GatewayAuthorizationError, 403, "authorization"),
        (GatewayCapacityError, 429, "capacity"),
        (GatewayTimeoutError, 504, "timeout"),
        (GatewayUnavailableError, 503, "unavailable"),
        (GatewayResponseError, None, "contract_or_http_response"),
    )
    for kind, status, error_class in mapping:
        if isinstance(exc, kind):
            return status, error_class, type(exc).__name__
    return None, "unexpected", type(exc).__name__


def _response_metadata(endpoint: Endpoint, response: Any) -> dict[str, Any]:
    if endpoint == "spot":
        item = response.spot
        count = 1
    elif endpoint == "history":
        item = response.history
        count = len(item.bars)
    else:
        item = response.option_chain
        count = len(item.contracts)
    return {
        "schema_version": response.schema_version,
        "schema_valid": True,
        "stale": item.stale,
        "age_seconds": item.age_seconds,
        "contract_count": count,
        "data_quality_flag_count": len(item.data_quality_flags),
    }


class EvidenceRecorder:
    def __init__(
        self,
        config: LoadTestConfig,
        *,
        clock: Callable[[], dt.datetime] = lambda: dt.datetime.now(UTC),
        run_id: str | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        stamp = clock().strftime("%Y%m%dT%H%M%S.%fZ")
        self.run_id = run_id or f"{stamp}-{uuid.uuid4().hex[:12]}"
        self.run_dir = config.output_root.resolve() / self.run_id
        self.events_path = self.run_dir / "requests.ndjson"
        self.manifest_path = self.run_dir / "manifest.json"
        self._handle: Any = None
        self._rows: list[dict[str, Any]] = []
        self.started_at: dt.datetime | None = None

    def start(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=False)
        self._handle = self.events_path.open("x", encoding="utf-8")
        self.started_at = self.clock()

    def record(self, row: dict[str, Any]) -> None:
        if self._handle is None:
            raise RuntimeError("evidence recorder is not started")
        # A fixed allowlist makes accidental response-body or credential persistence fail closed.
        allowed = {
            "sequence", "stage", "endpoint", "symbol", "scheduled_offset_seconds",
            "started_at", "finished_at", "latency_ms", "status_code", "status_class",
            "error_class", "exception_class", "schema_version", "schema_valid", "stale",
            "age_seconds", "contract_count", "data_quality_flag_count",
        }
        if set(row) - allowed:
            raise ValueError("evidence row contains a prohibited field")
        self._handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        self._handle.flush()
        self._rows.append(row)

    def finalize(self, *, termination_reason: str, planned_count: int) -> Path:
        if self._handle is None or self.started_at is None:
            raise RuntimeError("evidence recorder is not started")
        self._handle.close()
        self._handle = None
        digest = hashlib.sha256(self.events_path.read_bytes()).hexdigest()
        summaries: dict[str, dict[str, Any]] = {}
        for stage in sorted({row["stage"] for row in self._rows}):
            rows = [row for row in self._rows if row["stage"] == stage]
            latencies = [float(row["latency_ms"]) for row in rows]
            statuses = Counter(row["status_class"] for row in rows)
            summaries[stage] = {
                "request_count": len(rows),
                "success_count": statuses["success"],
                "error_count": len(rows) - statuses["success"],
                "latency_ms": {
                    "min": min(latencies) if latencies else None,
                    "median": statistics.median(latencies) if latencies else None,
                    "max": max(latencies) if latencies else None,
                },
                "status_classes": dict(sorted(statuses.items())),
                "stale_count": sum(row.get("stale") is True for row in rows),
                "contract_count": sum(row.get("contract_count") or 0 for row in rows),
                "schema_failure_count": sum(row.get("schema_valid") is False for row in rows),
            }
        manifest = {
            "schema_version": "1.0",
            "run_id": self.run_id,
            "purpose": "read_only_gateway_http_load_test",
            "response_bodies_recorded": False,
            "credentials_recorded": False,
            "config": _safe_config(self.config),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.clock()),
            "termination_reason": termination_reason,
            "planned_request_count": planned_count,
            "completed_request_count": len(self._rows),
            "events": {"path": self.events_path.name, "sha256": digest},
            "stage_summaries": summaries,
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        return self.manifest_path


async def run_load_test(
    config: LoadTestConfig,
    client: GatewayMarketDataClient,
    *,
    recorder: EvidenceRecorder | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> Path:
    schedule = build_schedule(config)
    recorder = recorder or EvidenceRecorder(config)
    recorder.start()
    semaphore = asyncio.Semaphore(config.max_concurrency)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError):
            pass
    session_start = monotonic()

    async def execute(job: PlannedRequest) -> None:
        delay = session_start + job.offset_seconds - monotonic()
        if delay > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                return
            except TimeoutError:
                pass
        if stop.is_set() or monotonic() - session_start > config.duration_seconds:
            return
        async with semaphore:
            started_wall = dt.datetime.now(UTC)
            started = monotonic()
            row: dict[str, Any] = {
                "sequence": job.sequence,
                "stage": job.stage,
                "endpoint": job.endpoint,
                "symbol": job.symbol,
                "scheduled_offset_seconds": job.offset_seconds,
                "started_at": _iso(started_wall),
            }
            try:
                if job.endpoint == "spot":
                    response = await client.get_spot(job.symbol)
                elif job.endpoint == "history":
                    response = await client.get_history(job.symbol, frequency="minute")
                else:
                    response = await client.get_option_chain(job.symbol, config.expiration)
                row.update(
                    status_code=200,
                    status_class="success",
                    error_class=None,
                    exception_class=None,
                )
                row.update(_response_metadata(job.endpoint, response))
            except Exception as exc:  # evidence must survive every SDK failure class
                status, error_class, exception_class = _failure(exc)
                row.update(
                    status_code=status,
                    status_class="error",
                    error_class=error_class,
                    exception_class=exception_class,
                    schema_version=None,
                    schema_valid=False if isinstance(exc, GatewayResponseError) else None,
                    stale=None,
                    age_seconds=None,
                    contract_count=0,
                    data_quality_flag_count=0,
                )
            row["latency_ms"] = round((monotonic() - started) * 1000, 3)
            row["finished_at"] = _iso(dt.datetime.now(UTC))
            recorder.record(row)

    termination = "completed"
    try:
        async with asyncio.TaskGroup() as group:
            for job in schedule:
                group.create_task(execute(job))
    except asyncio.CancelledError:
        termination = "cancelled"
        raise
    finally:
        if stop.is_set():
            termination = "signal"
        for signum in installed_signals:
            loop.remove_signal_handler(signum)
        manifest = recorder.finalize(termination_reason=termination, planned_count=len(schedule))
    return manifest


def _monitor(value: str) -> MonitorWindow:
    try:
        symbol, start, duration = value.split(":", 2)
        return MonitorWindow(symbol.upper(), float(start), float(duration))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected SYMBOL:START_SECONDS:DURATION_SECONDS") from exc


def _burst(value: str) -> EntryBurst:
    try:
        symbol, start, requests = value.split(":", 2)
        return EntryBurst(symbol.upper(), float(start), int(requests))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected SYMBOL:START_SECONDS:REQUESTS") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--expiration", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--duration-seconds", required=True, type=float)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--symbols", default="SPX,NDX,XSP")
    parser.add_argument("--collector-interval-seconds", type=float, default=60.0)
    parser.add_argument("--monitor-interval-seconds", type=float, default=2.0)
    parser.add_argument("--monitor-window", action="append", type=_monitor, default=[])
    parser.add_argument("--entry-burst", action="append", type=_burst, default=[])
    parser.add_argument("--max-concurrency", type=int, default=6)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--api-key-environment", default="SCHWAB_GATEWAY_API_KEY")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    key = os.environ.get(args.api_key_environment, "")
    if not key:
        parser.error(f"{args.api_key_environment} must contain the scoped load-test API key")
    try:
        config = LoadTestConfig(
            base_url=args.base_url,
            expiration=args.expiration,
            duration_seconds=args.duration_seconds,
            output_root=args.output_root,
            symbols=tuple(args.symbols.split(",")),
            collector_interval_seconds=args.collector_interval_seconds,
            monitor_interval_seconds=args.monitor_interval_seconds,
            max_concurrency=args.max_concurrency,
            timeout_seconds=args.timeout_seconds,
            monitor_windows=tuple(args.monitor_window),
            entry_bursts=tuple(args.entry_burst),
            api_key_environment=args.api_key_environment,
        )
    except ValueError as exc:
        parser.error(str(exc))

    async def execute() -> Path:
        async with GatewayMarketDataClient(
            config.base_url, key, timeout_seconds=config.timeout_seconds
        ) as client:
            return await run_load_test(config, client)

    manifest = asyncio.run(execute())
    print(manifest)


if __name__ == "__main__":
    main()
