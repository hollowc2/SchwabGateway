"""Pin the exact bytes and modes of every evidence file the research tools write.

Digests were captured from the writers before their file helpers were shared.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path
from types import SimpleNamespace

from schwab_gateway_sdk.models import (
    OrderBookLevelV1,
    OrderBookSnapshotV1,
    PriceBarV1,
    SessionHistoryV1,
)

from schwab_gateway.equity_stream_capture import EquityStreamCaptureRequest, EquityStreamRecorder
from schwab_gateway.export_session_history import export_session_history
from schwab_gateway.order_book_analysis import write_derived_dataset
from schwab_gateway.order_book_capture import OrderBookCaptureRequest, OrderBookResearchRecorder

NOW = dt.datetime(2026, 9, 29, 15, 0, tzinfo=dt.UTC)


def _fingerprint(directory: Path) -> dict[str, tuple[int, str]]:
    return {
        path.relative_to(directory).as_posix(): (
            path.stat().st_mode & 0o777,
            hashlib.sha256(path.read_bytes()).hexdigest()[:16],
        )
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def _snapshot(second: int, bid_size: int) -> OrderBookSnapshotV1:
    at = NOW + dt.timedelta(seconds=second)
    return OrderBookSnapshotV1(
        symbol="AAPL",
        venue="NASDAQ",
        service="NASDAQ_BOOK",
        sequence=second + 1,
        gateway_received_at=at,
        event_timestamp=at,
        bids=(OrderBookLevelV1(price=100.0, total_size=bid_size, participant_count=0),),
        asks=(OrderBookLevelV1(price=100.2, total_size=5, participant_count=0),),
    )


def test_order_book_capture_and_derived_dataset_bytes(tmp_path: Path) -> None:
    times = iter((NOW, NOW + dt.timedelta(seconds=1), NOW + dt.timedelta(seconds=4)))
    request = OrderBookCaptureRequest(
        venue="NASDAQ", symbols=("AAPL",), duration_seconds=4, output_root=tmp_path / "capture"
    )
    recorder = OrderBookResearchRecorder(request, clock=lambda: next(times))
    recorder.start()
    recorder.record_connection_event("connected", connection_id=1)
    recorder.record_raw_frame('{"data":[{"service":"NASDAQ_BOOK","content":[]}]}', NOW)
    recorder.record_snapshot(_snapshot(0, 10))
    recorder.record_snapshot(_snapshot(2, 14))
    manifest = recorder.finalize(termination_reason="completed")
    write_derived_dataset(
        manifest, tmp_path / "derived", depth_levels=1, clock=lambda: NOW + dt.timedelta(minutes=1)
    )

    run = "schwab_nasdaq_book_AAPL_20260929T150000.000000Z"
    assert _fingerprint(tmp_path) == {
        f"capture/{run}/connection_events.ndjson": (0o600, "68c44f25f91e47dd"),
        f"capture/{run}/manifest.json": (0o600, "ad70b040c79d72b0"),
        f"capture/{run}/normalized_snapshots.ndjson": (0o600, "e287940d61b20798"),
        f"capture/{run}/raw_frames.jsonseq": (0o600, "616cb8b90aede5c6"),
        "derived/manifest.json": (0o600, "8e7e95949d452188"),
        "derived/order_book_metrics.ndjson": (0o600, "5462f60a2d50e08a"),
    }


def test_equity_stream_capture_bytes(tmp_path: Path) -> None:
    times = iter((NOW, NOW, NOW + dt.timedelta(seconds=1), NOW + dt.timedelta(seconds=2)))
    request = EquityStreamCaptureRequest(
        symbols=("AAPL",), duration_seconds=60, output_root=tmp_path
    )
    recorder = EquityStreamRecorder(request, clock=lambda: next(times))
    recorder.start()
    received_at = recorder.record_raw_frame('{"data":[{"service":"CHART_EQUITY","content":[]}]}')
    recorder.record_message("CHART_EQUITY", {"content": [{"key": "AAPL"}]}, received_at=received_at)
    recorder.finalize(termination_reason="completed")

    run = "schwab_equity_stream_AAPL_20260929T150000.000000Z"
    assert _fingerprint(tmp_path) == {
        f"{run}/connection_events.ndjson": (0o600, "e3b0c44298fc1c14"),
        f"{run}/manifest.json": (0o600, "6b2474cf8e5aa0a7"),
        f"{run}/raw_frames.jsonseq": (0o600, "bd8edd39c2a2fae9"),
        f"{run}/relabeled_messages.ndjson": (0o600, "cb8e05f5a0e265ad"),
    }


async def test_session_history_export_bytes(tmp_path: Path) -> None:
    def history(session: str, hour: int) -> SimpleNamespace:
        at = dt.datetime(2026, 9, 28, hour, tzinfo=dt.UTC)
        bar = PriceBarV1(timestamp=at, open=1, high=2, low=0.5, close=1.5, volume=10)
        return SimpleNamespace(
            session_history=SessionHistoryV1(
                symbol="AAPL",
                date=dt.date(2026, 9, 28),
                session=session,
                candles=(bar,),
                event_timestamp=at,
                gateway_received_at=NOW,
                source="test",
                stale=False,
                age_seconds=0,
            )
        )

    class Client:
        async def get_session_history(self, _symbol, _date, *, session):
            return history(session, 15 if session == "regular" else 12)

    await export_session_history(
        client=Client(),
        symbol="AAPL",
        date=dt.date(2026, 9, 28),
        output_root=tmp_path,
        clock=lambda: NOW,
    )

    run = "AAPL/2026-09-28/20260929T150000.000000Z"
    assert _fingerprint(tmp_path) == {
        f"{run}/candles_1m.json": (0o600, "235bf7e4ddcf1bbd"),
        f"{run}/manifest.json": (0o600, "e774bd41392cfec4"),
    }
