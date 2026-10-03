"""Completed-session cache for ``/v1/session-history``.

No test here contacts Schwab: the provider is an in-memory fake and time is injected.
"""

from __future__ import annotations

import asyncio
import datetime as dt

import httpx
import pytest
from aiohttp.test_utils import TestServer
from schwab_token_store import TokenManagerState
from support import BlockingSpotUpstream, FakeReadiness, serving, single_principal_authenticator

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.api import create_app
from schwab_gateway.upstream import (
    DirectSchwabSessionHistoryUpstream,
    UpstreamUnavailableError,
    session_history_cache_events,
)

UTC = dt.timezone.utc
COMPLETED = dt.date(2026, 9, 25)
# 06:00 in New York on Monday 2026-09-28: 09-25 is complete, 09-28 has not started.
NOW = dt.datetime(2026, 9, 28, 10, 0, tzinfo=UTC)
HEADERS = {"X-Internal-API-Key": "valid-key"}


def _candle(at: dt.datetime, close: float) -> dict:
    return {
        "datetime": int(at.timestamp() * 1000),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 10,
    }


def _day(date: dt.date) -> list[dict]:
    """One regular bar (10:00 New York) and one after-hours bar (17:00 New York)."""
    regular = dt.datetime.combine(date, dt.time(14, 0), tzinfo=UTC)
    return [_candle(regular, 1.0), _candle(regular + dt.timedelta(hours=7), 2.0)]


class _Clock:
    def __init__(self, now: dt.datetime = NOW) -> None:
        self.now = now

    def utcnow(self) -> dt.datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += dt.timedelta(seconds=seconds)


class _Provider:
    def __init__(self, day=_day) -> None:
        self.day = day
        self.calls: list[tuple[str, dt.date]] = []

    async def get_session_bars(self, symbol: str, date: dt.date):
        self.calls.append((symbol, date))
        return self.day(date)


def _hits() -> float:
    return session_history_cache_events.labels(outcome="hit")._value.get()


@pytest.mark.asyncio
async def test_completed_session_is_fetched_once_and_ages_on_later_hits() -> None:
    clock = _Clock()
    provider = _Provider()
    upstream = DirectSchwabSessionHistoryUpstream(provider, utcnow=clock.utcnow)
    hits_before = _hits()

    fetched = await upstream.get_session_history("$VIX", COMPLETED, "regular")
    clock.advance(3600)
    cached = await upstream.get_session_history("$VIX", COMPLETED, "regular")

    assert provider.calls == [("$VIX", COMPLETED)]
    assert _hits() == hits_before + 1
    assert cached.candles == fetched.candles
    assert cached.gateway_received_at == fetched.gateway_received_at
    assert cached.age_seconds == fetched.age_seconds + 3600


@pytest.mark.asyncio
async def test_hit_recomputes_the_stale_flag() -> None:
    # 00:30 New York on Saturday 09-26: Friday's session is complete and its last bar
    # (17:00 New York) is under a day old.
    clock = _Clock(dt.datetime(2026, 9, 26, 4, 30, tzinfo=UTC))
    upstream = DirectSchwabSessionHistoryUpstream(_Provider(), utcnow=clock.utcnow)

    fetched = await upstream.get_session_history("AAPL", COMPLETED, "extended")
    clock.advance(86400)
    cached = await upstream.get_session_history("AAPL", COMPLETED, "extended")

    assert fetched.stale is False
    assert "stale" not in fetched.data_quality_flags
    assert cached.stale is True
    assert cached.data_quality_flags.count("stale") == 1


@pytest.mark.asyncio
async def test_todays_session_in_new_york_is_never_cached() -> None:
    today = dt.date(2026, 9, 28)
    # 22:00 in New York on 09-28 is already 09-29 in UTC.
    clock = _Clock(dt.datetime(2026, 9, 29, 2, 0, tzinfo=UTC))
    provider = _Provider()
    upstream = DirectSchwabSessionHistoryUpstream(provider, utcnow=clock.utcnow)

    await upstream.get_session_history("AAPL", today, "extended")
    await upstream.get_session_history("AAPL", today, "extended")

    assert provider.calls == [("AAPL", today), ("AAPL", today)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "day",
    [
        pytest.param(lambda _date: [], id="no_bars"),
        pytest.param(
            lambda date: [*_day(date), {"datetime": "not-a-time"}],
            id="malformed_bar_dropped",
        ),
    ],
)
async def test_incomplete_reads_are_not_cached(day) -> None:
    clock = _Clock()
    provider = _Provider(day)
    upstream = DirectSchwabSessionHistoryUpstream(provider, utcnow=clock.utcnow)

    await upstream.get_session_history("AAPL", COMPLETED, "regular")
    await upstream.get_session_history("AAPL", COMPLETED, "regular")

    assert len(provider.calls) == 2


@pytest.mark.asyncio
async def test_failed_fetch_is_not_cached() -> None:
    class FailingOnce(_Provider):
        async def get_session_bars(self, symbol: str, date: dt.date):
            self.calls.append((symbol, date))
            if len(self.calls) == 1:
                raise RuntimeError("transient")
            return self.day(date)

    provider = FailingOnce()
    upstream = DirectSchwabSessionHistoryUpstream(provider, utcnow=_Clock().utcnow)

    with pytest.raises(UpstreamUnavailableError):
        await upstream.get_session_history("AAPL", COMPLETED, "regular")
    recovered = await upstream.get_session_history("AAPL", COMPLETED, "regular")

    assert len(provider.calls) == 2
    assert [bar.close for bar in recovered.candles] == [1.0]


@pytest.mark.asyncio
async def test_sessions_are_cached_separately_and_evicted_least_recently_used() -> None:
    provider = _Provider()
    upstream = DirectSchwabSessionHistoryUpstream(
        provider, cache_max_entries=2, utcnow=_Clock().utcnow
    )
    other = dt.date(2026, 9, 24)

    regular = await upstream.get_session_history("AAPL", COMPLETED, "regular")
    extended = await upstream.get_session_history("AAPL", COMPLETED, "extended")
    await upstream.get_session_history("AAPL", COMPLETED, "regular")  # most recent now
    await upstream.get_session_history("AAPL", other, "regular")  # evicts extended
    await upstream.get_session_history("AAPL", COMPLETED, "regular")
    await upstream.get_session_history("AAPL", COMPLETED, "extended")

    assert [bar.close for bar in regular.candles] == [1.0]
    assert [bar.close for bar in extended.candles] == [2.0]
    assert provider.calls == [
        ("AAPL", COMPLETED),
        ("AAPL", COMPLETED),
        ("AAPL", other),
        ("AAPL", COMPLETED),
    ]


@pytest.mark.asyncio
async def test_a_session_larger_than_the_byte_budget_is_not_cached() -> None:
    provider = _Provider()
    upstream = DirectSchwabSessionHistoryUpstream(
        provider, cache_max_bytes=64, utcnow=_Clock().utcnow
    )

    await upstream.get_session_history("AAPL", COMPLETED, "regular")
    await upstream.get_session_history("AAPL", COMPLETED, "regular")

    assert len(provider.calls) == 2


def test_cache_bounds_must_be_positive() -> None:
    with pytest.raises(ValueError):
        DirectSchwabSessionHistoryUpstream(_Provider(), cache_max_entries=0)
    with pytest.raises(ValueError):
        DirectSchwabSessionHistoryUpstream(_Provider(), cache_max_bytes=0)


# --- API fast path ---------------------------------------------------------------------


class _Quotes:
    async def get_quotes(self, symbols):
        raise UpstreamUnavailableError("unused")


def _one_slot_app(upstream, readiness: FakeReadiness, spot=None):
    # Capacity 1 means a request that reaches the scheduler while the slot is occupied
    # is rejected with 429, so a 200 proves the scheduler was bypassed.
    return create_app(
        _Quotes(),
        single_principal_authenticator(),
        token_readiness_provider=readiness,
        session_history_upstream=upstream,
        spot_upstream=spot,
        admission_policy=AdmissionPolicy(protected_capacity=1, background_capacity=1),
    )


SESSION_PARAMS = {"symbol": "$VIX", "date": COMPLETED.isoformat(), "session": "regular"}


@pytest.mark.asyncio
async def test_cached_session_is_served_without_waiting_for_the_busy_scheduler_slot() -> None:
    clock = _Clock()
    provider = _Provider()
    upstream = DirectSchwabSessionHistoryUpstream(provider, utcnow=clock.utcnow)
    spot = BlockingSpotUpstream()
    server = TestServer(_one_slot_app(upstream, FakeReadiness(), spot=spot))
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            warm = await client.get("/v1/session-history", params=SESSION_PARAMS, headers=HEADERS)
            clock.advance(60)
            busy = asyncio.create_task(
                client.get("/v1/spot", params={"symbol": "SPX"}, headers=HEADERS)
            )
            await spot.entered.wait()
            hit = await asyncio.wait_for(
                client.get("/v1/session-history", params=SESSION_PARAMS, headers=HEADERS),
                timeout=1,
            )
            spot.release.set()
            await busy
    finally:
        spot.release.set()
        await server.close()

    assert warm.status_code == 200
    assert hit.status_code == 200
    assert provider.calls == [("$VIX", COMPLETED)]
    warm_body = warm.json()["session_history"]
    hit_body = hit.json()["session_history"]
    assert hit_body["candles"] == warm_body["candles"]
    assert hit_body["age_seconds"] == warm_body["age_seconds"] + 60


@pytest.mark.asyncio
async def test_warm_session_cache_still_fails_closed_when_gateway_is_not_ready() -> None:
    provider = _Provider()
    upstream = DirectSchwabSessionHistoryUpstream(provider, utcnow=_Clock().utcnow)
    readiness = FakeReadiness()
    async with serving(_one_slot_app(upstream, readiness)) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            warm = await client.get("/v1/session-history", params=SESSION_PARAMS, headers=HEADERS)
            readiness.state = TokenManagerState.EXPIRED
            refused = await client.get(
                "/v1/session-history", params=SESSION_PARAMS, headers=HEADERS
            )

    assert warm.status_code == 200
    assert refused.status_code == 503
    assert refused.json()["error"]["code"] == "gateway_not_ready"
    assert provider.calls == [("$VIX", COMPLETED)]
