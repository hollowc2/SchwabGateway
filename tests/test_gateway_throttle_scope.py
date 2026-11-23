"""Schwab 429 cooldowns are scoped to the throttled operation family; offline only."""

from __future__ import annotations

import asyncio
import datetime as dt
from email.utils import format_datetime

import httpx
import pytest
from schwab_gateway_sdk.client import (
    GatewayMarketDataClient,
    GatewayRateLimitError,
    GatewayUnavailableError,
)
from support import FakeReadiness, serving
from test_gateway_failure_budgets import Result, failure, request_for
from test_gateway_option_chain import (
    CHAIN_PARAMS,
    EXPIRATION,
    HEADERS,
    _BlockingProvider,
    _Clock,
    _one_slot_app,
    _payload,
    _Provider,
)

from schwab_gateway import api
from schwab_gateway.auth import PriorityClass
from schwab_gateway.failures import ACCOUNT_WIDE, FailureMetadata, ReadCooldowns, classify
from schwab_gateway.token_adapter import SchwabClientOperationError
from schwab_gateway.upstream import DirectSchwabOptionChainUpstream


class _Reads:
    """Count upstream calls per operation; the first ``throttle`` call raises a 429."""

    def __init__(self, throttle: str | None = None, retry_after: float | None = None) -> None:
        self.calls: list[str] = []
        self._throttle = throttle
        self._retry_after = retry_after

    def read(self, operation: str):
        async def build():
            self.calls.append(operation)
            if operation == self._throttle and self.calls.count(operation) == 1:
                raise failure("rate_limited", self._retry_after)
            return Result()

        return build


async def _serve(request, operation: str, reads: _Reads):
    return await api._serve_upstream(request, operation, reads.read(operation))


def test_cooldowns_scope_by_family_and_escalate_only_on_account_wide_evidence() -> None:
    cooldowns = ReadCooldowns()

    assert cooldowns.arm("chains", 1.0, now=10.0) == "chains"
    assert cooldowns.remaining("chains", 10.5) == 0.5
    assert cooldowns.remaining("quotes", 10.5) == 0.0
    assert cooldowns.remaining("chains", 11.0) == 0.0
    # A second family throttled while the first is still cooling is account-wide.
    cooldowns.arm("chains", 1.0, now=20.0)
    assert cooldowns.arm("quotes", 2.0, now=20.5) == ACCOUNT_WIDE
    assert cooldowns.remaining("price_history", 21.0) == 1.5
    # After both expire, one family's 429 is scoped again.
    assert cooldowns.arm("quotes", 1.0, now=30.0) == "quotes"
    # A 429 from an operation with no known family cools everything.
    assert cooldowns.arm(None, 1.0, now=40.0) == ACCOUNT_WIDE
    assert cooldowns.remaining("movers", 40.5) == 0.5


@pytest.mark.parametrize("retry_after,header", [(None, "1"), (2, "2")])
async def test_chain_429_sheds_only_chain_reads(retry_after, header) -> None:
    request = request_for()
    reads = _Reads(throttle="option_chain", retry_after=retry_after)

    throttled = await _serve(request, "option_chain", reads)
    again = await _serve(request, "option_chain", reads)
    summary = await _serve(request, "chain_metadata", reads)
    others = [await _serve(request, op, reads) for op in ("quotes", "spot", "history")]

    assert throttled.status == again.status == summary.status == 503
    for response in (throttled, again, summary):
        assert b"upstream_rate_limited" in response.body
        assert response.headers["Retry-After"] == header
    # The chain summary is the same Schwab /chains read, so it is shed with the chain.
    assert [response.status for response in others] == [200, 200, 200]
    assert reads.calls == ["option_chain", "quotes", "spot", "history"]


async def test_queued_reads_after_a_chain_429_only_shed_the_chain_family() -> None:
    request = request_for()
    scheduler = request.app[api.EXECUTION_SCHEDULER_KEY]
    release = asyncio.Event()
    calls: list[str] = []

    async def throttled_chain():
        calls.append("option_chain")
        await release.wait()
        raise failure("rate_limited")

    def read(operation: str):
        async def build():
            calls.append(operation)
            return Result()

        return build

    owner = asyncio.create_task(api._serve_upstream(request, "option_chain", throttled_chain))
    while not calls:
        await asyncio.sleep(0)
    queued = [
        asyncio.create_task(api._serve_upstream(request, operation, read(operation)))
        for operation in ("spot", "option_chain", "history")
    ]
    while scheduler.snapshot().queued_protected < 3:
        await asyncio.sleep(0)
    release.set()
    responses = await asyncio.gather(owner, *queued)

    assert [response.status for response in responses] == [503, 200, 503, 200]
    assert responses[2].headers["Retry-After"] == "1"
    # The queued chain read was shed at dispatch without calling Schwab.
    assert calls == ["option_chain", "spot", "history"]
    assert scheduler.snapshot().total == 0


async def test_local_cooldown_rejection_never_extends_the_upstream_cooldown() -> None:
    request = request_for()
    loop = asyncio.get_running_loop()
    scheduler = request.app[api.EXECUTION_SCHEDULER_KEY]
    cooldowns = request.app[api.READ_COOLDOWNS_KEY]
    release = asyncio.Event()
    reads = _Reads()

    async def blocker():
        await release.wait()

    blocked = asyncio.create_task(scheduler.execute(
        PriorityClass.PROTECTED, "history", blocker,
        queue_timeout_seconds=2, execution_timeout_seconds=3,
    ))
    # Queued before the 429, so it passes the pre-dispatch check and meets the
    # cooldown at dispatch -- the path that used to re-arm max(1s, remaining).
    queued = asyncio.create_task(_serve(request, "option_chain", reads))
    while scheduler.snapshot().queued_protected < 1:
        await asyncio.sleep(0)
    armed_until = loop.time() + 0.2
    cooldowns.arm("chains", 0.2, loop.time())
    release.set()
    at_dispatch = await queued
    before_dispatch = await _serve(request, "option_chain", reads)
    await blocked

    assert at_dispatch.status == before_dispatch.status == 503
    assert at_dispatch.headers["Retry-After"] == before_dispatch.headers["Retry-After"] == "1"
    assert reads.calls == []
    assert loop.time() + cooldowns.remaining("chains", loop.time()) <= armed_until + 0.01
    await asyncio.sleep(max(0.0, armed_until - loop.time()) + 0.02)
    recovered = await _serve(request, "option_chain", reads)
    assert recovered.status == 200 and reads.calls == ["option_chain"]


async def test_second_family_429_during_a_chain_cooldown_cools_every_family() -> None:
    request = request_for()
    chain = _Reads(throttle="option_chain")
    quotes = _Reads(throttle="quotes")

    assert (await _serve(request, "option_chain", chain)).status == 503
    assert (await _serve(request, "quotes", quotes)).status == 503
    history = await _serve(request, "history", quotes)

    assert history.status == 503 and history.headers["Retry-After"] == "1"
    assert quotes.calls == ["quotes"]


class _ThrottledChainProvider(_BlockingProvider):
    async def get_option_chain(self, symbol, expiration):
        await super().get_option_chain(symbol, expiration)
        raise SchwabClientOperationError(
            "Schwab client operation failed", FailureMetadata("rate_limited")
        )


class _CountingCooldowns(ReadCooldowns):
    def __init__(self) -> None:
        super().__init__()
        self.arms = 0

    def arm(self, family, seconds, now):
        self.arms += 1
        return super().arm(family, seconds, now)


async def test_coalesced_waiters_report_one_429_without_re_arming_it() -> None:
    clock = _Clock()
    provider = _ThrottledChainProvider(_payload())
    upstream = DirectSchwabOptionChainUpstream(
        provider, monotonic_clock=clock.monotonic, utcnow=clock.utcnow
    )
    app = _one_slot_app(upstream)
    cooldowns = _CountingCooldowns()
    app[api.READ_COOLDOWNS_KEY] = cooldowns
    async with serving(app) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            get = lambda: client.get("/v1/option-chain", params=CHAIN_PARAMS, headers=HEADERS)  # noqa: E731
            owner = asyncio.create_task(get())
            await provider.entered.wait()
            joiners = [asyncio.create_task(get()) for _ in range(3)]
            await asyncio.sleep(0.05)
            provider.release.set()
            responses = await asyncio.gather(owner, *joiners)
            shed = await get()

    assert [r.status_code for r in responses + [shed]] == [503] * 5
    assert all(r.json()["error"]["code"] == "upstream_rate_limited" for r in responses)
    assert all(r.headers["Retry-After"] == "1" for r in responses + [shed])
    assert len(provider.calls) == 1
    assert cooldowns.arms == 1


async def test_cached_chain_is_still_served_during_a_chain_cooldown() -> None:
    clock = _Clock()
    provider = _Provider(_payload())
    upstream = DirectSchwabOptionChainUpstream(
        provider, monotonic_clock=clock.monotonic, utcnow=clock.utcnow
    )
    app = _one_slot_app(upstream)
    async with serving(app) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            warm = await client.get("/v1/option-chain", params=CHAIN_PARAMS, headers=HEADERS)
            app[api.READ_COOLDOWNS_KEY].arm("chains", 30.0, asyncio.get_running_loop().time())
            hit = await client.get("/v1/option-chain", params=CHAIN_PARAMS, headers=HEADERS)
            other = await client.get(
                "/v1/option-chain",
                params={**CHAIN_PARAMS, "symbol": "NDX"},
                headers=HEADERS,
            )

    assert warm.status_code == hit.status_code == 200
    assert hit.json()["option_chain"]["gateway_received_at"] == (
        warm.json()["option_chain"]["gateway_received_at"]
    )
    assert other.status_code == 503 and other.headers["Retry-After"] == "30"
    assert provider.calls == [("SPX", EXPIRATION)]


async def test_sdk_sees_the_gateway_cooldown_when_schwab_omits_retry_after() -> None:
    provider = _ThrottledChainProvider(_payload())
    provider.release.set()
    upstream = DirectSchwabOptionChainUpstream(provider)
    async with serving(_one_slot_app(upstream, FakeReadiness())) as server:
        sdk = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        try:
            with pytest.raises(GatewayRateLimitError) as upstream_429:
                await sdk.get_option_chain("SPX", EXPIRATION)
            with pytest.raises(GatewayRateLimitError) as local:
                await sdk.get_option_chain("SPX", EXPIRATION)
        finally:
            await sdk.close()

    assert upstream_429.value.retry_after_seconds == local.value.retry_after_seconds == 1
    # SDKs older than 0.8.1 (ButterflyGuy pins 0.8.0) see the same base class.
    assert isinstance(local.value, GatewayUnavailableError)
    assert len(provider.calls) == 1


class _LongRetryAfterProvider(_BlockingProvider):
    """A chain 429 whose raw Retry-After header goes through the real parser."""

    def __init__(self, payload, header: str) -> None:
        super().__init__(payload)
        self.header = header

    async def get_option_chain(self, symbol, expiration):
        await super().get_option_chain(symbol, expiration)
        request = httpx.Request("GET", "https://synthetic.invalid/chains")
        response = httpx.Response(429, headers={"Retry-After": self.header}, request=request)
        raise SchwabClientOperationError(
            "Schwab client operation failed",
            classify(httpx.HTTPStatusError("429", request=request, response=response)),
        )


@pytest.mark.parametrize("http_date", [False, True])
async def test_long_retry_after_is_never_reported_as_the_default_cooldown(http_date) -> None:
    header = (
        format_datetime(dt.datetime.now(dt.UTC) + dt.timedelta(seconds=121), usegmt=True)
        if http_date
        else "120"
    )
    provider = _LongRetryAfterProvider(_payload(), header)
    provider.release.set()
    upstream = DirectSchwabOptionChainUpstream(provider)
    async with serving(_one_slot_app(upstream, FakeReadiness())) as server:
        sdk = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        try:
            with pytest.raises(GatewayRateLimitError) as upstream_429:
                await sdk.get_option_chain("SPX", EXPIRATION)
            with pytest.raises(GatewayRateLimitError) as local:
                await sdk.get_option_chain("SPX", EXPIRATION)
        finally:
            await sdk.close()

    # Schwab asked for two minutes; the gateway neither shortens it nor reopens sooner.
    assert upstream_429.value.retry_after_seconds >= 120
    assert local.value.retry_after_seconds >= 119
    assert len(provider.calls) == 1
