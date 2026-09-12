"""Bounded raw Schwab chart and Level I equity-stream evidence capture."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import math
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from schwab.contrib.util import StreamJsonDecoder
from schwab_token_store import AtomicFileTokenStore, AtomicTokenManager

from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.order_book_capture import (
    DEFAULT_MAX_RECONNECTS,
    DEFAULT_RECONNECT_BASE_DELAY_SECONDS,
    DEFAULT_STREAM_LOGIN_TIMEOUT_SECONDS,
    MAX_CAPTURE_DURATION_SECONDS,
    MAX_CAPTURE_SYMBOLS,
    MAX_RECONNECT_DELAY_SECONDS,
    SYMBOL_PATTERN,
    bootstrap_stream_under_token_lock,
)

UTC = dt.timezone.utc
SERVICES = ("CHART_EQUITY", "LEVELONE_EQUITIES")


@dataclass(frozen=True)
class EquityStreamCaptureRequest:
    symbols: tuple[str, ...]
    duration_seconds: float
    output_root: Path
    display_timezone: str = "America/New_York"

    def __post_init__(self) -> None:
        symbols = tuple(symbol.strip().upper() for symbol in self.symbols)
        if not symbols:
            raise ValueError("at least one equity-stream symbol is required")
        if len(symbols) > MAX_CAPTURE_SYMBOLS:
            raise ValueError(f"at most {MAX_CAPTURE_SYMBOLS} symbols are allowed")
        if len(set(symbols)) != len(symbols):
            raise ValueError("equity-stream symbols must be unique")
        if any(not SYMBOL_PATTERN.fullmatch(symbol) for symbol in symbols):
            raise ValueError("one or more equity-stream symbols are invalid")
        if (
            not math.isfinite(self.duration_seconds)
            or not 1 <= self.duration_seconds <= MAX_CAPTURE_DURATION_SECONDS
        ):
            raise ValueError("capture duration must be between 1 second and 24 hours")
        if not self.output_root.is_absolute():
            raise ValueError("equity-stream output root must be absolute")
        try:
            ZoneInfo(self.display_timezone)
        except ZoneInfoNotFoundError:
            raise ValueError("equity-stream display timezone is invalid") from None
        object.__setattr__(self, "symbols", symbols)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class EquityStreamRecorder:
    """Preserve raw frames and relabeled messages with a hashed manifest."""

    def __init__(
        self,
        request: EquityStreamCaptureRequest,
        *,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self.request = request
        self._clock = clock or (lambda: dt.datetime.now(UTC))
        self.run_directory: Path | None = None
        self.raw_path: Path | None = None
        self.messages_path: Path | None = None
        self.connection_events_path: Path | None = None
        self.manifest_path: Path | None = None
        self.started_at: dt.datetime | None = None
        self.raw_frame_count = 0
        self.message_counts = {service: 0 for service in SERVICES}
        self.connection_attempt_count = 0
        self.successful_connection_count = 0
        self.connection_failure_count = 0
        self.reconnect_count = 0
        self._raw_handle: Any = None
        self._messages_handle: Any = None
        self._connection_events_handle: Any = None
        self._finalized = False

    def start(self) -> None:
        if self.started_at is not None:
            raise RuntimeError("equity-stream recorder is already started")
        now = self._clock()
        if now.utcoffset() is None:
            raise ValueError("equity-stream recorder clock must be timezone-aware")
        self.started_at = now.astimezone(UTC)
        timestamp = self.started_at.strftime("%Y%m%dT%H%M%S.%fZ")
        symbol_slug = "-".join(symbol.replace("$", "DOLLAR") for symbol in self.request.symbols)
        run_directory = self.request.output_root / f"schwab_equity_stream_{symbol_slug}_{timestamp}"
        self.request.output_root.mkdir(parents=True, exist_ok=True)
        run_directory.mkdir(mode=0o700, exist_ok=False)
        self.run_directory = run_directory
        self.raw_path = run_directory / "raw_frames.jsonseq"
        self.messages_path = run_directory / "relabeled_messages.ndjson"
        self.connection_events_path = run_directory / "connection_events.ndjson"
        self.manifest_path = run_directory / "manifest.json"
        self._raw_handle = self.raw_path.open("xb")
        self._messages_handle = self.messages_path.open("x", encoding="utf-8")
        self._connection_events_handle = self.connection_events_path.open("x", encoding="utf-8")
        for path in (self.raw_path, self.messages_path, self.connection_events_path):
            os.chmod(path, 0o600)

    def record_raw_frame(self, raw: str) -> dt.datetime:
        if self._raw_handle is None:
            raise RuntimeError("equity-stream recorder is not started")
        received_at = self._clock().astimezone(UTC)
        self._raw_handle.write(b"\x1e" + raw.encode("utf-8") + b"\n")
        self.raw_frame_count += 1
        return received_at

    def record_message(
        self, service: str, message: Any, *, received_at: dt.datetime | None
    ) -> None:
        if self._messages_handle is None:
            raise RuntimeError("equity-stream recorder is not started")
        if service not in SERVICES:
            raise ValueError("unsupported equity-stream service")
        envelope = {
            "schema_version": "1.0",
            "provider": "Charles Schwab streaming",
            "service": service,
            "received_at": (received_at or self._clock()).astimezone(UTC).isoformat(),
            "message": message,
        }
        self._messages_handle.write(
            json.dumps(envelope, sort_keys=True, separators=(",", ":"), default=str) + "\n"
        )
        self.message_counts[service] += 1

    def record_connection_event(
        self,
        event: str,
        *,
        connection_id: int,
        failure_class: str | None = None,
        retry_delay_seconds: float | None = None,
    ) -> None:
        if self._connection_events_handle is None:
            raise RuntimeError("equity-stream recorder is not started")
        payload = {
            "schema_version": "1.0",
            "event": event,
            "connection_id": connection_id,
            "timestamp": self._clock().astimezone(UTC).isoformat(),
            "failure_class": failure_class,
            "retry_delay_seconds": retry_delay_seconds,
        }
        self._connection_events_handle.write(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        )
        if event == "connecting":
            self.connection_attempt_count += 1
        elif event == "connected":
            self.successful_connection_count += 1
            if connection_id > 1:
                self.reconnect_count += 1
        elif event == "connection_failed":
            self.connection_failure_count += 1

    def finalize(self, *, termination_reason: str, failure_class: str | None = None) -> Path:
        if self._finalized:
            assert self.manifest_path is not None
            return self.manifest_path
        if self.started_at is None or self.manifest_path is None:
            raise RuntimeError("equity-stream recorder is not started")
        ended_at = self._clock().astimezone(UTC)
        for handle in (self._raw_handle, self._messages_handle, self._connection_events_handle):
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
        assert self.raw_path and self.messages_path and self.connection_events_path
        manifest = {
            "schema_version": "1.0",
            "purpose": "equity_stream_research",
            "provider": "Charles Schwab streaming",
            "source_library": "schwab-py",
            "services": list(SERVICES),
            "symbols": list(self.request.symbols),
            "requested_duration_seconds": self.request.duration_seconds,
            "display_timezone": self.request.display_timezone,
            "started_at": self.started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "path_scope": "relative_to_manifest",
            "raw_path": self.raw_path.name,
            "messages_path": self.messages_path.name,
            "connection_events_path": self.connection_events_path.name,
            "raw_sha256": _sha256(self.raw_path),
            "messages_sha256": _sha256(self.messages_path),
            "connection_events_sha256": _sha256(self.connection_events_path),
            "raw_frame_count": self.raw_frame_count,
            "message_counts": self.message_counts,
            "connection_attempt_count": self.connection_attempt_count,
            "successful_connection_count": self.successful_connection_count,
            "connection_failure_count": self.connection_failure_count,
            "reconnect_count": self.reconnect_count,
            "termination_reason": termination_reason,
            "failure_class": failure_class,
        }
        with self.manifest_path.open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(self.manifest_path, 0o600)
        self._finalized = True
        return self.manifest_path


class CapturingEquityJsonDecoder(StreamJsonDecoder):
    def __init__(self, recorder: EquityStreamRecorder) -> None:
        self._recorder = recorder
        self.last_received_at: dt.datetime | None = None

    def decode_json_string(self, raw: str) -> Any:
        payload = json.loads(raw)
        if self._contains_target_service(payload):
            self.last_received_at = self._recorder.record_raw_frame(raw)
        return payload

    @staticmethod
    def _contains_target_service(payload: Any) -> bool:
        if not isinstance(payload, Mapping):
            return False
        data = payload.get("data")
        return isinstance(data, list) and any(
            isinstance(item, Mapping) and item.get("service") in SERVICES for item in data
        )


async def capture_equity_stream_with_reconnects(
    manager: AtomicTokenManager,
    upstream_settings: GatewayUpstreamSettings,
    client_factory: Callable[..., Any],
    request: EquityStreamCaptureRequest,
    recorder: EquityStreamRecorder,
    *,
    stream_client_factory: Callable[[Any], Any] | None = None,
    login_timeout_seconds: float = DEFAULT_STREAM_LOGIN_TIMEOUT_SECONDS,
    max_reconnects: int = DEFAULT_MAX_RECONNECTS,
    reconnect_base_delay_seconds: float = DEFAULT_RECONNECT_BASE_DELAY_SECONDS,
) -> None:
    if login_timeout_seconds <= 0:
        raise ValueError("stream login timeout must be positive")
    if max_reconnects < 0:
        raise ValueError("maximum reconnects must be nonnegative")
    if reconnect_base_delay_seconds < 0:
        raise ValueError("reconnect base delay must be nonnegative")
    if stream_client_factory is None:
        from schwab.streaming import StreamClient

        stream_client_factory = StreamClient

    recorder.start()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + request.duration_seconds
    connection_id = 0
    last_error: Exception | None = None

    while loop.time() < deadline:
        connection_id += 1
        recorder.record_connection_event("connecting", connection_id=connection_id)
        stream: Any = None
        try:
            stream = await bootstrap_stream_under_token_lock(
                manager,
                upstream_settings,
                client_factory,
                stream_client_factory,
                login_timeout_seconds=min(
                    login_timeout_seconds,
                    max(deadline - loop.time(), 0.001),
                ),
            )
            recorder.record_connection_event("connected", connection_id=connection_id)
            decoder = CapturingEquityJsonDecoder(recorder)
            stream.set_json_decoder(decoder)
            stream.add_chart_equity_handler(
                lambda message: recorder.record_message(
                    "CHART_EQUITY", message, received_at=decoder.last_received_at
                )
            )
            stream.add_level_one_equity_handler(
                lambda message: recorder.record_message(
                    "LEVELONE_EQUITIES", message, received_at=decoder.last_received_at
                )
            )
            await stream.chart_equity_subs(list(request.symbols))
            await stream.level_one_equity_subs(list(request.symbols))
            while (remaining := deadline - loop.time()) > 0:
                try:
                    await asyncio.wait_for(stream.handle_message(), timeout=remaining)
                except TimeoutError:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            recorder.record_connection_event(
                "connection_failed",
                connection_id=connection_id,
                failure_class=type(exc).__name__,
            )
        finally:
            if stream is not None:
                try:
                    await stream.logout()
                except Exception:
                    pass

        reconnects_used = connection_id - 1
        if reconnects_used >= max_reconnects:
            assert last_error is not None
            raise last_error
        delay = min(
            reconnect_base_delay_seconds * (2**reconnects_used),
            MAX_RECONNECT_DELAY_SECONDS,
        )
        if deadline - loop.time() <= delay:
            return
        recorder.record_connection_event(
            "reconnect_scheduled",
            connection_id=connection_id,
            failure_class=type(last_error).__name__ if last_error else None,
            retry_delay_seconds=delay,
        )
        await asyncio.sleep(delay)


def run_equity_stream_capture(
    request: EquityStreamCaptureRequest,
    upstream_settings: GatewayUpstreamSettings,
    client_factory: Callable[..., Any],
    *,
    recorder: EquityStreamRecorder | None = None,
    stream_client_factory: Callable[[Any], Any] | None = None,
    login_timeout_seconds: float = DEFAULT_STREAM_LOGIN_TIMEOUT_SECONDS,
    max_reconnects: int = DEFAULT_MAX_RECONNECTS,
) -> Path:
    recorder = recorder or EquityStreamRecorder(request)
    manager = AtomicTokenManager(AtomicFileTokenStore(upstream_settings.token_path))
    reason = "completed"
    failure_class: str | None = None
    try:
        asyncio.run(
            capture_equity_stream_with_reconnects(
                manager,
                upstream_settings,
                client_factory,
                request,
                recorder,
                stream_client_factory=stream_client_factory,
                login_timeout_seconds=login_timeout_seconds,
                max_reconnects=max_reconnects,
            )
        )
    except KeyboardInterrupt:
        reason = "interrupted"
    except Exception as exc:
        reason = "stream_error"
        failure_class = type(exc).__name__
        raise
    finally:
        if recorder.started_at is not None:
            recorder.finalize(termination_reason=reason, failure_class=failure_class)
    if recorder.manifest_path is None:
        raise RuntimeError("equity-stream capture did not start")
    return recorder.manifest_path
