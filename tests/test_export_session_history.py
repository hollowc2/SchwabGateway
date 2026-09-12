from __future__ import annotations

import datetime as dt
import hashlib
import json
from types import SimpleNamespace

import pytest
from schwab_gateway_sdk.models import PriceBarV1, SessionHistoryV1

from schwab_gateway.export_session_history import export_session_history

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 11, 22, 0, tzinfo=UTC)
DATE = dt.date(2026, 9, 11)


def _history(session: str, hour: int, *, stale: bool = False):
    timestamp = dt.datetime(2026, 9, 11, hour, tzinfo=UTC)
    return SimpleNamespace(
        session_history=SessionHistoryV1(
            symbol="AAPL",
            date=DATE,
            session=session,
            candles=(
                PriceBarV1(
                    timestamp=timestamp,
                    open=100,
                    high=101,
                    low=99,
                    close=100.5,
                    volume=1000,
                ),
            ),
            event_timestamp=timestamp,
            gateway_received_at=NOW,
            source="test",
            stale=stale,
            age_seconds=0,
        )
    )


class FakeClient:
    async def get_session_history(self, _symbol, _date, *, session):
        return _history(session, 15 if session == "regular" else 12)


async def test_export_is_hashed_non_overwriting_and_combines_both_sessions(tmp_path):
    manifest_path = await export_session_history(
        client=FakeClient(),
        symbol="aapl",
        date=DATE,
        output_root=tmp_path,
        clock=lambda: NOW,
    )
    manifest = json.loads(manifest_path.read_text())
    candles_path = manifest_path.parent / manifest["candles_path"]
    candles = json.loads(candles_path.read_text())

    assert manifest["candle_count"] == 2
    assert manifest["regular_candle_count"] == 1
    assert manifest["extended_candle_count"] == 1
    assert manifest["candles_sha256"] == hashlib.sha256(candles_path.read_bytes()).hexdigest()
    assert [row["timestamp"] for row in candles["candles"]] == sorted(
        row["timestamp"] for row in candles["candles"]
    )
    with pytest.raises(FileExistsError):
        await export_session_history(
            client=FakeClient(),
            symbol="AAPL",
            date=DATE,
            output_root=tmp_path,
            clock=lambda: NOW,
        )


async def test_export_fails_closed_on_stale_session(tmp_path):
    class StaleClient(FakeClient):
        async def get_session_history(self, _symbol, _date, *, session):
            return _history(session, 15 if session == "regular" else 12, stale=True)

    with pytest.raises(RuntimeError, match="stale"):
        await export_session_history(
            client=StaleClient(),
            symbol="AAPL",
            date=DATE,
            output_root=tmp_path,
            clock=lambda: NOW,
        )
