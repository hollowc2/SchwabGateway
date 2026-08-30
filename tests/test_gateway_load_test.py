"""Tests for the bounded read-only full-session HTTP load driver."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from schwab_gateway_sdk.client import GatewayCapacityError, GatewayResponseError

from schwab_gateway.load_test import (
    EntryBurst,
    EvidenceRecorder,
    LoadTestConfig,
    MonitorWindow,
    build_schedule,
    run_load_test,
)

EXPIRATION = dt.date(2026, 9, 18)


def _config(tmp_path: Path, **overrides: Any) -> LoadTestConfig:
    params: dict[str, Any] = dict(
        base_url="http://gateway.internal:8080",
        expiration=EXPIRATION,
        duration_seconds=300.0,
        output_root=tmp_path / "evidence",
    )
    params.update(overrides)
    return LoadTestConfig(**params)


# --- Config validation --------------------------------------------------------------


def test_config_normalizes_and_deduplicates_symbols(tmp_path: Path) -> None:
    config = _config(tmp_path, symbols=(" spx ", "NDX", "spx"))
    assert config.symbols == ("SPX", "NDX")


@pytest.mark.parametrize(
    "overrides",
    [
        {"symbols": ()},
        {"symbols": ("SPX", "")},
        {"base_url": "ftp://gateway"},
        {"duration_seconds": 0.0},
        {"duration_seconds": 90_000.0},
        {"collector_interval_seconds": 0.0},
        {"max_concurrency": 0},
        {"max_concurrency": 64},
        {"timeout_seconds": 0.0},
    ],
)
def test_config_rejects_out_of_bounds_inputs(tmp_path: Path, overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _config(tmp_path, **overrides)


def test_config_rejects_monitor_window_past_session_end(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="monitor window exceeds"):
        _config(
            tmp_path,
            duration_seconds=100.0,
            monitor_windows=(MonitorWindow("SPX", start_seconds=80.0, duration_seconds=40.0),),
        )


def test_config_rejects_monitor_window_for_unconfigured_symbol(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not configured"):
        _config(
            tmp_path,
            monitor_windows=(MonitorWindow("AAPL", start_seconds=0.0, duration_seconds=10.0),),
        )


@pytest.mark.parametrize("requests", [0, 21])
def test_config_rejects_entry_burst_request_count(tmp_path: Path, requests: int) -> None:
    with pytest.raises(ValueError, match="1 through 20 requests"):
        _config(
            tmp_path,
            entry_bursts=(EntryBurst("SPX", start_seconds=10.0, requests=requests),),
        )


# --- Schedule ----------------------------------------------------------------------


def test_build_schedule_is_sorted_and_covers_every_stage(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        symbols=("SPX", "NDX"),
        duration_seconds=180.0,
        collector_interval_seconds=60.0,
        monitor_interval_seconds=2.0,
        monitor_windows=(MonitorWindow("SPX", start_seconds=0.0, duration_seconds=4.0),),
        entry_bursts=(EntryBurst("NDX", start_seconds=30.0, requests=3),),
    )
    schedule = build_schedule(config)

    offsets = [job.offset_seconds for job in schedule]
    assert offsets == sorted(offsets)

    stages = {job.stage for job in schedule}
    assert stages == {"warmup", "collector", "monitor", "entry_burst"}

    # warmup: spot + history + option_chain for each symbol, once, at offset 0
    warmup = [job for job in schedule if job.stage == "warmup"]
    assert len(warmup) == 2 * 3
    assert all(job.offset_seconds == 0.0 for job in warmup)

    # collector: spot + option_chain per symbol at 60 and 120 (not at 180 == duration)
    collector = [job for job in schedule if job.stage == "collector"]
    assert {job.offset_seconds for job in collector} == {60.0, 120.0}
    assert len(collector) == 2 * 2 * 2

    # monitor: one option_chain every 2s across the 4s window
    monitor = [job for job in schedule if job.stage == "monitor"]
    assert [job.offset_seconds for job in monitor] == [0.0, 2.0]
    assert all(job.endpoint == "option_chain" and job.symbol == "SPX" for job in monitor)

    burst = [job for job in schedule if job.stage == "entry_burst"]
    assert len(burst) == 3
    assert all(job.offset_seconds == 30.0 and job.symbol == "NDX" for job in burst)


# --- Evidence recorder -----------------------------------------------------------


def _started_recorder(tmp_path: Path) -> EvidenceRecorder:
    recorder = EvidenceRecorder(_config(tmp_path), run_id="fixed-run")
    recorder.start()
    return recorder


def test_recorder_rejects_rows_with_fields_outside_the_allowlist(tmp_path: Path) -> None:
    recorder = _started_recorder(tmp_path)
    with pytest.raises(ValueError, match="prohibited field"):
        recorder.record({"stage": "warmup", "response_body": {"secret": 1}})


def test_recorder_manifest_pins_the_event_digest_and_asserts_no_bodies(tmp_path: Path) -> None:
    recorder = _started_recorder(tmp_path)
    recorder.record(
        {
            "sequence": 0,
            "stage": "warmup",
            "endpoint": "spot",
            "symbol": "SPX",
            "scheduled_offset_seconds": 0.0,
            "started_at": "2026-09-18T13:30:00Z",
            "finished_at": "2026-09-18T13:30:00Z",
            "latency_ms": 12.5,
            "status_code": 200,
            "status_class": "success",
        }
    )
    manifest_path = recorder.finalize(termination_reason="completed", planned_count=1)

    manifest = json.loads(manifest_path.read_text())
    assert manifest["response_bodies_recorded"] is False
    assert manifest["credentials_recorded"] is False
    assert manifest["config"]["api_key"] == "not_recorded"
    assert manifest["completed_request_count"] == 1
    assert manifest["stage_summaries"]["warmup"]["request_count"] == 1

    events = (recorder.run_dir / "requests.ndjson").read_bytes()
    assert manifest["events"]["sha256"] == hashlib.sha256(events).hexdigest()


# --- End-to-end run --------------------------------------------------------------


class _FakeClient:
    """Minimal stand-in for GatewayMarketDataClient."""

    def __init__(self, *, fail_option_chain: bool = False) -> None:
        self.fail_option_chain = fail_option_chain
        self.calls: list[tuple[str, str]] = []

    async def get_spot(self, symbol: str) -> Any:
        self.calls.append(("spot", symbol))
        return SimpleNamespace(
            schema_version="1.0",
            spot=SimpleNamespace(stale=False, age_seconds=0.2, data_quality_flags=()),
        )

    async def get_history(self, symbol: str, *, frequency: str = "daily") -> Any:
        self.calls.append(("history", symbol))
        return SimpleNamespace(
            schema_version="1.0",
            history=SimpleNamespace(
                bars=(1, 2, 3), stale=False, age_seconds=1.0, data_quality_flags=()
            ),
        )

    async def get_option_chain(self, symbol: str, expiration: dt.date) -> Any:
        self.calls.append(("option_chain", symbol))
        if self.fail_option_chain:
            raise GatewayCapacityError("gateway shed the request")
        return SimpleNamespace(
            schema_version="1.0",
            option_chain=SimpleNamespace(
                contracts=(1, 2), stale=False, age_seconds=0.5, data_quality_flags=("x",)
            ),
        )


@pytest.mark.asyncio
async def test_run_load_test_writes_a_complete_manifest(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        symbols=("SPX",),
        duration_seconds=0.4,
        collector_interval_seconds=0.1,
    )
    client = _FakeClient()
    recorder = EvidenceRecorder(config, run_id="e2e")

    manifest_path = await run_load_test(config, client, recorder=recorder)
    manifest = json.loads(manifest_path.read_text())

    assert manifest["termination_reason"] == "completed"
    assert manifest["completed_request_count"] == len(recorder._rows)
    assert manifest["completed_request_count"] >= 3  # at least the warm-up reads

    rows = [
        json.loads(line)
        for line in (recorder.run_dir / "requests.ndjson").read_text().splitlines()
    ]
    assert {row["status_class"] for row in rows} == {"success"}
    assert manifest["stage_summaries"]["warmup"]["success_count"] == 3


@pytest.mark.asyncio
async def test_run_load_test_records_every_sdk_failure_class(tmp_path: Path) -> None:
    config = _config(tmp_path, symbols=("SPX",), duration_seconds=0.2)
    client = _FakeClient(fail_option_chain=True)
    recorder = EvidenceRecorder(config, run_id="fail")

    manifest_path = await run_load_test(config, client, recorder=recorder)
    manifest = json.loads(manifest_path.read_text())

    warmup = manifest["stage_summaries"]["warmup"]
    assert warmup["error_count"] >= 1
    chain_rows = [row for row in recorder._rows if row["endpoint"] == "option_chain"]
    assert chain_rows and all(row["status_code"] == 429 for row in chain_rows)
    assert all(row["error_class"] == "capacity" for row in chain_rows)


def test_gateway_response_error_is_a_schema_failure_class() -> None:
    assert issubclass(GatewayResponseError, Exception)
