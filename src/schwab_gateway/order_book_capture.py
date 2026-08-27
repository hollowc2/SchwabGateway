"""Lossless-first Schwab order-book capture for offline research.

Every relevant websocket text frame is stored before any normalization. Derived
snapshots and the final evidence manifest live beside, never in place of, that raw file.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from schwab.contrib.util import StreamJsonDecoder
from schwab_gateway_sdk.models import OrderBookSnapshotV1
from schwab_token_store import AtomicFileTokenStore, AtomicTokenManager

from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.order_book import (
    BOOK_SERVICE_BY_VENUE,
    OrderBookMalformedError,
    OrderBookVenue,
    normalize_schwab_book_message,
)

UTC = dt.timezone.utc
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9$._/-]{1,32}$")
MAX_CAPTURE_SYMBOLS = 25
MAX_CAPTURE_DURATION_SECONDS = 86_400.0


class OrderBookCaptureError(RuntimeError):
    """The research capture could not complete with an auditable result."""


@dataclass(frozen=True)
class OrderBookCaptureRequest:
    venue: OrderBookVenue
    symbols: tuple[str, ...]
    duration_seconds: float
    output_root: Path
    display_timezone: str = "America/New_York"

    def __post_init__(self) -> None:
        normalized_symbols = tuple(symbol.strip().upper() for symbol in self.symbols)
        if not normalized_symbols:
            raise ValueError("at least one order-book symbol is required")
        if len(normalized_symbols) > MAX_CAPTURE_SYMBOLS:
            raise ValueError(f"at most {MAX_CAPTURE_SYMBOLS} order-book symbols are allowed")
        if len(set(normalized_symbols)) != len(normalized_symbols):
            raise ValueError("order-book symbols must be unique")
        if any(not SYMBOL_PATTERN.fullmatch(symbol) for symbol in normalized_symbols):
            raise ValueError("one or more order-book symbols are invalid")
        if self.venue not in BOOK_SERVICE_BY_VENUE:
            raise ValueError("unsupported order-book venue")
        if (
            not math.isfinite(self.duration_seconds)
            or not 1 <= self.duration_seconds <= MAX_CAPTURE_DURATION_SECONDS
        ):
            raise ValueError(
                "order-book duration must be between 1 second and 24 hours"
            )
        if not self.output_root.is_absolute():
            raise ValueError("order-book output root must be absolute")
        try:
            ZoneInfo(self.display_timezone)
        except ZoneInfoNotFoundError:
            raise ValueError("order-book display timezone is invalid") from None
        object.__setattr__(self, "symbols", normalized_symbols)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class OrderBookResearchRecorder:
    """Write raw frames, derived snapshots, and a reproducibility manifest."""

    def __init__(
        self,
        request: OrderBookCaptureRequest,
        *,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self.request = request
        self._clock = clock or (lambda: dt.datetime.now(UTC))
        self.run_directory: Path | None = None
        self.raw_path: Path | None = None
        self.normalized_path: Path | None = None
        self.manifest_path: Path | None = None
        self.started_at: dt.datetime | None = None
        self.ended_at: dt.datetime | None = None
        self.raw_frame_count = 0
        self.normalized_snapshot_count = 0
        self.malformed_snapshot_count = 0
        self.sequence_gap_count = 0
        self.missing_sequence_count = 0
        self.snapshots_without_sequence_count = 0
        self.duplicate_or_out_of_order_count = 0
        self.dropped_event_count = 0
        self._first_event_timestamp: dt.datetime | None = None
        self._last_event_timestamp: dt.datetime | None = None
        self._last_sequence: dict[str, int] = {}
        self._raw_handle: Any = None
        self._normalized_handle: Any = None
        self._finalized = False

    def start(self) -> None:
        if self.started_at is not None:
            raise RuntimeError("order-book recorder is already started")
        started_at = self._clock()
        if started_at.utcoffset() is None:
            raise ValueError("order-book recorder clock must be timezone-aware")
        self.started_at = started_at.astimezone(UTC)
        timestamp = self.started_at.strftime("%Y%m%dT%H%M%S.%fZ")
        symbol_slug = "-".join(symbol.replace("$", "DOLLAR") for symbol in self.request.symbols)
        run_name = f"schwab_{self.request.venue.lower()}_book_{symbol_slug}_{timestamp}"
        self.request.output_root.mkdir(parents=True, exist_ok=True)
        run_directory = self.request.output_root / run_name
        run_directory.mkdir(mode=0o700, exist_ok=False)
        self.run_directory = run_directory
        self.raw_path = run_directory / "raw_frames.jsonseq"
        self.normalized_path = run_directory / "normalized_snapshots.ndjson"
        self.manifest_path = run_directory / "manifest.json"
        self._raw_handle = self.raw_path.open("xb")
        self._normalized_handle = self.normalized_path.open("x", encoding="utf-8")
        os.chmod(self.raw_path, 0o600)
        os.chmod(self.normalized_path, 0o600)

    def record_raw_frame(self, raw: str, received_at: dt.datetime) -> None:
        if self._raw_handle is None:
            raise RuntimeError("order-book recorder is not started")
        if received_at.utcoffset() is None:
            raise ValueError("raw frame receipt timestamp must be timezone-aware")
        encoded = raw.encode("utf-8")
        # RFC 7464 JSON text sequence framing preserves every websocket JSON text byte.
        self._raw_handle.write(b"\x1e" + encoded + b"\n")
        self.raw_frame_count += 1

    def record_malformed_snapshot(self) -> None:
        self.malformed_snapshot_count += 1

    def record_snapshot(self, snapshot: OrderBookSnapshotV1) -> None:
        if self._normalized_handle is None:
            raise RuntimeError("order-book recorder is not started")
        flags = list(snapshot.data_quality_flags)
        if snapshot.sequence is None:
            self.snapshots_without_sequence_count += 1
            flags.append("missing_sequence")
        else:
            previous = self._last_sequence.get(snapshot.symbol)
            if previous is not None and snapshot.sequence <= previous:
                self.duplicate_or_out_of_order_count += 1
                flags.append("duplicate_or_out_of_order_sequence")
            elif previous is not None and snapshot.sequence > previous + 1:
                self.sequence_gap_count += 1
                self.missing_sequence_count += snapshot.sequence - previous - 1
                flags.append("sequence_gap")
            self._last_sequence[snapshot.symbol] = snapshot.sequence
        if tuple(flags) != snapshot.data_quality_flags:
            snapshot = snapshot.model_copy(update={"data_quality_flags": tuple(flags)})

        self._normalized_handle.write(snapshot.model_dump_json() + "\n")
        self.normalized_snapshot_count += 1
        if snapshot.event_timestamp is not None:
            if self._first_event_timestamp is None:
                self._first_event_timestamp = snapshot.event_timestamp
            self._last_event_timestamp = snapshot.event_timestamp

    def finalize(
        self,
        *,
        termination_reason: str,
        failure_class: str | None = None,
    ) -> Path:
        if self._finalized:
            assert self.manifest_path is not None
            return self.manifest_path
        if self.started_at is None or self.manifest_path is None:
            raise RuntimeError("order-book recorder is not started")
        self.ended_at = self._clock().astimezone(UTC)
        for handle in (self._raw_handle, self._normalized_handle):
            if handle is not None:
                handle.flush()
                os.fsync(handle.fileno())
                handle.close()
        self._raw_handle = None
        self._normalized_handle = None
        assert self.raw_path is not None
        assert self.normalized_path is not None
        manifest = {
            "schema_version": "1.0",
            "purpose": "order_book_research",
            "provider": "Charles Schwab streaming",
            "source_library": "schwab-py",
            "service": BOOK_SERVICE_BY_VENUE[self.request.venue],
            "venue": self.request.venue,
            "is_consolidated": False,
            "symbols": list(self.request.symbols),
            "requested_duration_seconds": self.request.duration_seconds,
            "display_timezone": self.request.display_timezone,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "first_event_timestamp": (
                self._first_event_timestamp.isoformat()
                if self._first_event_timestamp is not None
                else None
            ),
            "last_event_timestamp": (
                self._last_event_timestamp.isoformat()
                if self._last_event_timestamp is not None
                else None
            ),
            "raw_format": "RFC 7464 JSON text sequence of Schwab websocket frames",
            "path_scope": "relative_to_manifest",
            "raw_path": self.raw_path.name,
            "normalized_path": self.normalized_path.name,
            "raw_sha256": _sha256(self.raw_path),
            "normalized_sha256": _sha256(self.normalized_path),
            "raw_frame_count": self.raw_frame_count,
            "normalized_snapshot_count": self.normalized_snapshot_count,
            "malformed_snapshot_count": self.malformed_snapshot_count,
            "sequence_gap_count": self.sequence_gap_count,
            "missing_sequence_count": self.missing_sequence_count,
            "snapshots_without_sequence_count": self.snapshots_without_sequence_count,
            "sequence_continuity_observable": self.snapshots_without_sequence_count == 0,
            "duplicate_or_out_of_order_count": self.duplicate_or_out_of_order_count,
            "dropped_event_count": self.dropped_event_count,
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


class CapturingBookJsonDecoder(StreamJsonDecoder):
    """Tee relevant raw book frames before schwab-py relabels their fields."""

    def __init__(
        self,
        *,
        service: str,
        recorder: OrderBookResearchRecorder,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._service = service
        self._recorder = recorder
        self._clock = clock or (lambda: dt.datetime.now(UTC))
        self.last_received_at: dt.datetime | None = None

    def decode_json_string(self, raw: str) -> Any:
        payload = json.loads(raw)
        received_at = self._clock().astimezone(UTC)
        if self._contains_target_service(payload):
            self._recorder.record_raw_frame(raw, received_at)
            self.last_received_at = received_at
        return payload

    def _contains_target_service(self, payload: Any) -> bool:
        if not isinstance(payload, Mapping):
            return False
        data = payload.get("data")
        return isinstance(data, list) and any(
            isinstance(item, Mapping) and item.get("service") == self._service
            for item in data
        )


async def capture_order_book_stream(
    client: Any,
    request: OrderBookCaptureRequest,
    recorder: OrderBookResearchRecorder,
    *,
    stream_client_factory: Callable[[Any], Any] | None = None,
) -> None:
    """Capture one bounded venue subscription using a preauthenticated client."""

    if stream_client_factory is None:
        from schwab.streaming import StreamClient

        stream_client_factory = StreamClient
    stream = stream_client_factory(client)
    service = BOOK_SERVICE_BY_VENUE[request.venue]
    decoder = CapturingBookJsonDecoder(service=service, recorder=recorder)

    def handle_book(message: Any) -> None:
        if decoder.last_received_at is None:
            recorder.record_malformed_snapshot()
            return
        try:
            snapshots = normalize_schwab_book_message(
                message,
                venue=request.venue,
                gateway_received_at=decoder.last_received_at,
            )
        except OrderBookMalformedError:
            recorder.record_malformed_snapshot()
            return
        for snapshot in snapshots:
            recorder.record_snapshot(snapshot)

    logged_in = False
    try:
        await stream.login()
        logged_in = True
        stream.set_json_decoder(decoder)
        recorder.start()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + request.duration_seconds
        if request.venue == "NASDAQ":
            stream.add_nasdaq_book_handler(handle_book)
            await stream.nasdaq_book_subs(list(request.symbols))
        else:
            stream.add_nyse_book_handler(handle_book)
            await stream.nyse_book_subs(list(request.symbols))

        while (remaining := deadline - loop.time()) > 0:
            try:
                await asyncio.wait_for(stream.handle_message(), timeout=remaining)
            except TimeoutError:
                break
    finally:
        try:
            if logged_in:
                await stream.logout()
        finally:
            close = getattr(client, "close_async_session", None)
            if callable(close):
                await close()


def run_exclusive_order_book_capture(
    request: OrderBookCaptureRequest,
    upstream_settings: GatewayUpstreamSettings,
    client_factory: Callable[..., Any],
    *,
    recorder: OrderBookResearchRecorder | None = None,
    stream_client_factory: Callable[[Any], Any] | None = None,
) -> Path:
    """Run a capture while holding the token lock for the complete stream lifetime."""

    recorder = recorder or OrderBookResearchRecorder(request)
    manager = AtomicTokenManager(AtomicFileTokenStore(upstream_settings.token_path))

    def run_locked(token_read_func: Callable[[], Any], token_write_func: Callable[..., None]):
        client = client_factory(
            upstream_settings.api_key.get_secret_value(),
            upstream_settings.app_secret.get_secret_value(),
            token_read_func,
            token_write_func,
            asyncio=True,
            enforce_enums=True,
        )
        return asyncio.run(
            capture_order_book_stream(
                client,
                request,
                recorder,
                stream_client_factory=stream_client_factory,
            )
        )

    reason = "completed"
    failure_class: str | None = None
    try:
        manager.run_access_transaction(run_locked)
    except KeyboardInterrupt:
        reason = "interrupted"
    except Exception as exc:
        reason = "stream_error"
        failure_class = type(exc).__name__
        raise
    finally:
        if recorder.started_at is not None:
            recorder.finalize(
                termination_reason=reason,
                failure_class=failure_class,
            )
    if recorder.manifest_path is None:
        raise OrderBookCaptureError("order-book capture did not start")
    return recorder.manifest_path


def parse_symbols(values: Sequence[str]) -> tuple[str, ...]:
    """Parse repeated/comma-separated CLI symbol values without widening scope."""

    return tuple(
        part.strip().upper()
        for value in values
        for part in value.split(",")
        if part.strip()
    )
