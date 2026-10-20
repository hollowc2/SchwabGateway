"""Pin the exact bytes and modes of the evidence files the session-history export writes."""

from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path
from types import SimpleNamespace

from schwab_gateway_sdk.models import (
    PriceBarV1,
    SessionHistoryV1,
)

from schwab_gateway.export_session_history import export_session_history

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
