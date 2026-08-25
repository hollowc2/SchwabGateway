"""Contract proof for the bounded, normalized full option-chain surface."""

from __future__ import annotations

import asyncio
import datetime as dt

import httpx
import pytest
from aiohttp.test_utils import TestServer
from pydantic import ValidationError
from schwab_gateway_sdk.client import GatewayMarketDataClient
from schwab_gateway_sdk.models import (
    MAX_OPTION_CHAIN_CONTRACTS_V1,
    OptionChainV1,
    OptionContractV1,
)
from schwab_token_store import TokenManagerHealth, TokenManagerState

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.api import create_app
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)
from schwab_gateway.upstream import (
    DirectSchwabOptionChainUpstream,
    UpstreamMalformedError,
    UpstreamUnavailableError,
    normalize_schwab_option_chain,
    option_chain_negative_time_value_normalizations,
)

UTC = dt.timezone.utc
EXPIRATION = dt.date(2026, 8, 24)
RECEIVED_AT = dt.datetime(2026, 8, 24, 17, 0, 1, tzinfo=UTC)
EVENT_MILLIS = int((RECEIVED_AT - dt.timedelta(seconds=1)).timestamp() * 1000)


def _contract(symbol: str, **overrides: object) -> dict[str, object]:
    contract: dict[str, object] = {
        "symbol": symbol,
        "bid": 1.1,
        "ask": 1.3,
        "mark": 1.2,
        "last": 1.15,
        "totalVolume": 17,
        "openInterest": 29,
        "volatility": 18.5,
        "delta": 0.51,
        "gamma": 0.02,
        "theta": -0.11,
        "vega": 0.08,
        "bidSize": 3,
        "askSize": 4,
        "rho": 0.01,
        "intrinsicValue": 2.5,
        "timeValue": 1.2,
        "inTheMoney": True,
        "daysToExpiration": 0,
        "multiplier": 100.0,
        "theoreticalOptionValue": 1.22,
        "quoteTimeInLong": EVENT_MILLIS,
    }
    contract.update(overrides)
    return contract


def _payload() -> dict[str, object]:
    return {
        "underlyingPrice": 6450.25,
        "underlying": {"quoteTime": EVENT_MILLIS},
        "callExpDateMap": {
            "2026-08-24:0": {
                "6450.0": [_contract("SPXW 260824C06450000")],
                "6460.0": [
                    _contract("SPXW 260824C06460000-a"),
                    _contract("SPXW 260824C06460000-b"),
                ],
            },
            "2026-08-25:1": {
                "6450.0": [_contract("SPXW 260825C06450000")],
            },
        },
        "putExpDateMap": {
            "2026-08-24:0": {
                "6450.0": [_contract("SPXW 260824P06450000", delta=-0.49)],
            }
        },
    }


def test_normalizer_preserves_every_consumer_field_and_every_matching_contract() -> None:
    chain = normalize_schwab_option_chain(
        "SPX",
        _payload(),
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert chain.symbol == "SPX"
    assert chain.expiration == EXPIRATION
    assert chain.underlying_price == 6450.25
    assert chain.call_contract_count == 3
    assert chain.put_contract_count == 1
    assert chain.strike_count == 2
    assert chain.event_timestamp == RECEIVED_AT - dt.timedelta(seconds=1)
    assert chain.age_seconds == 1
    assert chain.stale is False
    assert [contract.symbol for contract in chain.contracts] == [
        "SPXW 260824C06450000",
        "SPXW 260824C06460000-a",
        "SPXW 260824C06460000-b",
        "SPXW 260824P06450000",
    ]

    contract = chain.contracts[0]
    assert contract.model_dump(mode="json") == {
        "symbol": "SPXW 260824C06450000",
        "option_type": "CALL",
        "expiration": "2026-08-24",
        "strike": 6450.0,
        "bid": 1.1,
        "ask": 1.3,
        "mark": 1.2,
        "last": 1.15,
        "total_volume": 17,
        "open_interest": 29,
        "volatility": 18.5,
        "delta": 0.51,
        "gamma": 0.02,
        "theta": -0.11,
        "vega": 0.08,
        "bid_size": 3,
        "ask_size": 4,
        "rho": 0.01,
        "intrinsic_value": 2.5,
        "time_value": 1.2,
        "in_the_money": True,
        "days_to_expiration": 0,
        "multiplier": 100.0,
        "theoretical_option_value": 1.22,
        "event_timestamp": "2026-08-24T17:00:00Z",
        "stale": False,
        "age_seconds": 1.0,
        "data_quality_flags": [],
    }


def test_normalizer_maps_schwab_optional_analytics_sentinels_to_null() -> None:
    payload = _payload()
    first_call = payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0]
    sentinel_fields = (
        "volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "intrinsicValue",
        "timeValue",
        "theoreticalOptionValue",
    )
    first_call.update({field: -999 for field in sentinel_fields})

    chain = normalize_schwab_option_chain(
        "SPX",
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    contract = chain.contracts[0]
    assert contract.bid == 1.1
    assert contract.ask == 1.3
    assert contract.mark == 1.2
    normalized = contract.model_dump(mode="json")
    assert {
        normalized[field]
        for field in (
            "volatility",
            "delta",
            "gamma",
            "theta",
            "vega",
            "rho",
            "intrinsic_value",
            "time_value",
            "theoretical_option_value",
        )
    } == {None}


@pytest.mark.parametrize("negative_time_value", [-265.57, -833.552, -14.6])
def test_normalizer_maps_negative_schwab_time_value_to_null_with_observability(
    negative_time_value: float,
) -> None:
    payload = _payload()
    payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0][
        "timeValue"
    ] = negative_time_value
    before = option_chain_negative_time_value_normalizations._value.get()

    chain = normalize_schwab_option_chain(
        "SPX",
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert chain.contracts[0].time_value is None
    assert chain.contracts[0].bid == 1.1
    assert chain.contracts[0].ask == 1.3
    assert chain.contracts[0].mark == 1.2
    assert (
        chain.call_contract_count,
        chain.put_contract_count,
        chain.strike_count,
        len(chain.contracts),
    ) == (3, 1, 2, 4)
    assert option_chain_negative_time_value_normalizations._value.get() == before + 1


@pytest.mark.parametrize(
    ("time_value", "expected"),
    [(1.2, 1.2), (0.0, 0.0), (None, None)],
)
def test_normalizer_preserves_nonnegative_or_null_time_value(
    time_value: float | None,
    expected: float | None,
) -> None:
    payload = _payload()
    payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0]["timeValue"] = time_value

    chain = normalize_schwab_option_chain(
        "SPX",
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert chain.contracts[0].time_value == expected


def test_normalizer_maps_missing_time_value_to_null() -> None:
    payload = _payload()
    del payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0]["timeValue"]

    chain = normalize_schwab_option_chain(
        "SPX",
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert chain.contracts[0].time_value is None


def test_mixed_age_chain_keeps_rows_and_counts_without_aggregate_stale() -> None:
    payload = _payload()
    stale_millis = int((RECEIVED_AT - dt.timedelta(minutes=2)).timestamp() * 1000)
    payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0][
        "quoteTimeInLong"
    ] = stale_millis

    chain = normalize_schwab_option_chain(
        "SPX",
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert len(chain.contracts) == 4
    assert chain.call_contract_count == 3
    assert chain.put_contract_count == 1
    assert chain.strike_count == 2
    assert chain.contracts[0].stale is True
    assert all(not contract.stale for contract in chain.contracts[1:])
    assert chain.event_timestamp == RECEIVED_AT - dt.timedelta(seconds=1)
    assert chain.age_seconds == 1
    assert chain.stale is False
    assert "stale_contracts_present" in chain.data_quality_flags
    assert "stale" not in chain.data_quality_flags


@pytest.mark.parametrize(
    "payload",
    [
        {"underlyingPrice": 645.0},
        {
            "underlyingPrice": 645.0,
            "callExpDateMap": {
                "2026-08-24:0": {"645": [_contract("XSP CALL")]}
            },
        },
    ],
)
def test_normalizer_refuses_empty_or_one_sided_strategy_chains(payload) -> None:
    with pytest.raises(ValueError, match="contracts|both call and put"):
        normalize_schwab_option_chain(
            "XSP",
            payload,
            EXPIRATION,
            received_at=RECEIVED_AT,
            stale_after_seconds=90,
        )


def test_normalizer_refuses_oversized_chains_instead_of_truncating(monkeypatch) -> None:
    monkeypatch.setattr("schwab_gateway.upstream.MAX_OPTION_CHAIN_CONTRACTS_V1", 2)
    payload = {
        "callExpDateMap": {
            "2026-08-24:0": {
                "1": [_contract("C1")],
                "2": [_contract("C2")],
                "3": [_contract("C3")],
            }
        }
    }

    with pytest.raises(ValueError, match="maximum contract count"):
        normalize_schwab_option_chain(
            "SPX",
            payload,
            EXPIRATION,
            received_at=RECEIVED_AT,
            stale_after_seconds=90,
        )


def test_model_accepts_the_5000_contract_boundary_and_rejects_one_more() -> None:
    contracts = tuple(
        OptionContractV1(
            symbol=f"SPXW {option_type[0]} {index}",
            option_type=option_type,
            expiration=EXPIRATION,
            strike=float(index + 1),
            bid=1.1,
            ask=1.3,
            mark=1.2,
            stale=False,
        )
        for option_type in ("CALL", "PUT")
        for index in range(MAX_OPTION_CHAIN_CONTRACTS_V1 // 2)
    )
    base = {
        "symbol": "SPX",
        "expiration": EXPIRATION,
        "call_contract_count": 2500,
        "put_contract_count": 2500,
        "strike_count": 2500,
        "gateway_received_at": RECEIVED_AT,
        "source": "test",
        "stale": False,
    }

    chain = OptionChainV1(
        **base,
        contracts=contracts,
    )
    assert len(chain.contracts) == 5000

    with pytest.raises(ValidationError, match="at most 5000"):
        OptionChainV1(
            **{
                **base,
                "call_contract_count": 2501,
                "put_contract_count": 2500,
            },
            contracts=contracts + (contracts[0],),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("strike", float("nan"), "strike"),
        ("strike", 0.0, "strike"),
        ("bid", float("inf"), "prices"),
        ("ask", -0.01, "prices"),
        ("mark", float("nan"), "prices"),
        ("delta", float("inf"), "numeric fields"),
        ("time_value", float("nan"), "time value"),
        ("time_value", float("inf"), "time value"),
        ("time_value", -0.01, "time value"),
    ],
)
def test_contract_model_rejects_nonfinite_or_unsafe_numbers(
    field: str, value: float, message: str
) -> None:
    values = {
        "symbol": "SPXW 260824C06450000",
        "option_type": "CALL",
        "expiration": EXPIRATION,
        "strike": 6450.0,
        "bid": 1.1,
        "ask": 1.3,
        "mark": 1.2,
        "stale": False,
        field: value,
    }
    with pytest.raises(ValidationError, match=message):
        OptionContractV1(**values)


def test_contract_model_rejects_a_crossed_market() -> None:
    with pytest.raises(ValidationError, match="bid must not exceed ask"):
        OptionContractV1(
            symbol="SPXW 260824C06450000",
            option_type="CALL",
            expiration=EXPIRATION,
            strike=6450,
            bid=1.3,
            ask=1.2,
            mark=1.25,
            stale=False,
        )


@pytest.mark.parametrize("missing", ["bid", "ask", "mark"])
def test_contract_model_requires_selection_and_valuation_prices(missing: str) -> None:
    values = {
        "symbol": "SPXW 260824C06450000",
        "option_type": "CALL",
        "expiration": EXPIRATION,
        "strike": 6450,
        "bid": 1.1,
        "ask": 1.3,
        "mark": 1.2,
        "stale": False,
    }
    del values[missing]
    with pytest.raises(ValidationError):
        OptionContractV1(**values)


def test_chain_model_rejects_count_mismatch_and_duplicate_symbols() -> None:
    call = OptionContractV1(
        symbol="SPXW 260824C06450000",
        option_type="CALL",
        expiration=EXPIRATION,
        strike=6450,
        bid=1.1,
        ask=1.3,
        mark=1.2,
        stale=False,
    )
    put = OptionContractV1(
        symbol="SPXW 260824P06450000",
        option_type="PUT",
        expiration=EXPIRATION,
        strike=6450,
        bid=1.1,
        ask=1.3,
        mark=1.2,
        stale=False,
    )
    base = {
        "symbol": "SPX",
        "expiration": EXPIRATION,
        "call_contract_count": 1,
        "put_contract_count": 1,
        "strike_count": 1,
        "gateway_received_at": RECEIVED_AT,
        "source": "test",
        "stale": False,
    }

    with pytest.raises(ValidationError, match="counts must match"):
        OptionChainV1(**{**base, "call_contract_count": 2}, contracts=(call, put))

    duplicate_put = put.model_copy(update={"symbol": call.symbol})
    with pytest.raises(ValidationError, match="symbols must be unique"):
        OptionChainV1(**base, contracts=(call, duplicate_put))


def test_normalizer_preserves_internal_spaces_in_contract_symbols_exactly() -> None:
    payload = _payload()
    internal_symbol = "SPXW  260824C06450000"
    payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0]["symbol"] = internal_symbol

    chain = normalize_schwab_option_chain(
        "SPX",
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert chain.contracts[0].symbol == internal_symbol


@pytest.mark.parametrize(
    ("underlying", "contract_root"),
    [("SPX", "SPXW"), ("NDX", "NDX"), ("XSP", "XSP")],
)
def test_normalizer_preserves_index_and_contract_symbol_families(
    underlying: str,
    contract_root: str,
) -> None:
    payload = _payload()
    contract_number = 0
    for map_key in ("callExpDateMap", "putExpDateMap"):
        strike_map = payload[map_key]["2026-08-24:0"]
        for contracts in strike_map.values():
            for contract in contracts:
                contract_number += 1
                contract["symbol"] = f"{contract_root} CONTRACT {contract_number}"

    chain = normalize_schwab_option_chain(
        underlying,
        payload,
        EXPIRATION,
        received_at=RECEIVED_AT,
        stale_after_seconds=90,
    )

    assert chain.symbol == underlying
    assert all(contract.symbol.startswith(contract_root) for contract in chain.contracts)
    assert (chain.call_contract_count, chain.put_contract_count) == (3, 1)


@pytest.mark.parametrize(
    "payload",
    [
        {"callExpDateMap": []},
        {"callExpDateMap": {"2026-08-24:0": []}},
        {"callExpDateMap": {"2026-08-24:0": {"not-a-strike": []}}},
        {"callExpDateMap": {"2026-08-24:0": {"6450": {}}}},
        {"callExpDateMap": {"2026-08-24:0": {"6450": ["not-an-object"]}}},
        {"callExpDateMap": {"2026-08-24:0": {"6450": [{}]}}},
    ],
)
def test_normalizer_fails_closed_on_malformed_contract_structures(payload) -> None:
    with pytest.raises(ValueError):
        normalize_schwab_option_chain(
            "SPX",
            payload,
            EXPIRATION,
            received_at=RECEIVED_AT,
            stale_after_seconds=90,
        )


class _Provider:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dt.date]] = []

    async def get_option_chain(self, symbol: str, expiration: dt.date):
        self.calls.append((symbol, expiration))
        return self.payload


class _Clock:
    def __init__(self) -> None:
        self.monotonic_value = 100.0
        self.utc_value = RECEIVED_AT

    def monotonic(self) -> float:
        return self.monotonic_value

    def utcnow(self) -> dt.datetime:
        return self.utc_value

    def advance(self, seconds: float) -> None:
        self.monotonic_value += seconds
        self.utc_value += dt.timedelta(seconds=seconds)


@pytest.mark.asyncio
async def test_direct_upstream_maps_malformed_chain_to_bounded_failure() -> None:
    provider = _Provider({"callExpDateMap": []})
    upstream = DirectSchwabOptionChainUpstream(provider)

    with pytest.raises(UpstreamMalformedError):
        await upstream.get_option_chain("SPX", EXPIRATION)
    with pytest.raises(UpstreamMalformedError):
        await upstream.get_option_chain("SPX", EXPIRATION)

    assert provider.calls == [("SPX", EXPIRATION), ("SPX", EXPIRATION)]


@pytest.mark.asyncio
async def test_direct_upstream_cache_hit_preserves_evidence_and_recomputes_age() -> None:
    clock = _Clock()
    provider = _Provider(_payload())
    upstream = DirectSchwabOptionChainUpstream(
        provider,
        monotonic_clock=clock.monotonic,
        utcnow=clock.utcnow,
    )

    first = await upstream.get_option_chain("$SPX", EXPIRATION)
    clock.advance(3.5)
    cached = await upstream.get_option_chain("$SPX", EXPIRATION)

    assert provider.calls == [("$SPX", EXPIRATION)]
    assert cached.gateway_received_at == first.gateway_received_at
    assert cached.event_timestamp == first.event_timestamp
    assert [item.event_timestamp for item in cached.contracts] == [
        item.event_timestamp for item in first.contracts
    ]
    assert cached.age_seconds == 4.5
    assert cached.contracts[0].age_seconds == 4.5


@pytest.mark.asyncio
async def test_direct_upstream_negative_time_value_cache_miss_and_hit_are_identical() -> None:
    clock = _Clock()
    payload = _payload()
    payload["putExpDateMap"]["2026-08-24:0"]["6450.0"][0]["timeValue"] = -14.6
    provider = _Provider(payload)
    upstream = DirectSchwabOptionChainUpstream(
        provider,
        monotonic_clock=clock.monotonic,
        utcnow=clock.utcnow,
    )

    first = await upstream.get_option_chain("XSP", EXPIRATION)
    clock.advance(3.5)
    cached = await upstream.get_option_chain("XSP", EXPIRATION)

    assert provider.calls == [("XSP", EXPIRATION)]
    assert first.contracts[-1].time_value is None
    assert cached.contracts[-1].time_value is None
    assert cached.contracts[-1].model_dump(exclude={"age_seconds"}) == (
        first.contracts[-1].model_dump(exclude={"age_seconds"})
    )


@pytest.mark.asyncio
async def test_direct_upstream_cached_contract_can_cross_stale_threshold() -> None:
    clock = _Clock()
    provider = _Provider(_payload())
    upstream = DirectSchwabOptionChainUpstream(
        provider,
        stale_after_seconds=2,
        monotonic_clock=clock.monotonic,
        utcnow=clock.utcnow,
    )

    await upstream.get_option_chain("SPX", EXPIRATION)
    clock.advance(2)
    cached = await upstream.get_option_chain("SPX", EXPIRATION)

    assert cached.stale is True
    assert all(contract.stale for contract in cached.contracts)
    assert "stale" in cached.data_quality_flags
    assert all("stale" in contract.data_quality_flags for contract in cached.contracts)


class _FailAfterFirstProvider(_Provider):
    async def get_option_chain(self, symbol: str, expiration: dt.date):
        self.calls.append((symbol, expiration))
        if len(self.calls) > 1:
            raise RuntimeError("test upstream failure")
        return self.payload


@pytest.mark.asyncio
async def test_direct_upstream_expiry_fails_closed_without_stale_fallback() -> None:
    clock = _Clock()
    provider = _FailAfterFirstProvider(_payload())
    upstream = DirectSchwabOptionChainUpstream(
        provider,
        monotonic_clock=clock.monotonic,
        utcnow=clock.utcnow,
    )

    await upstream.get_option_chain("SPX", EXPIRATION)
    clock.advance(4)
    with pytest.raises(UpstreamUnavailableError, match="option chain request failed"):
        await upstream.get_option_chain("SPX", EXPIRATION)
    await asyncio.sleep(0)
    with pytest.raises(UpstreamUnavailableError, match="option chain request failed"):
        await upstream.get_option_chain("SPX", EXPIRATION)

    assert len(provider.calls) == 3
    assert upstream._cache == {}


class _BlockingProvider(_Provider):
    def __init__(self, payload: dict[str, object]) -> None:
        super().__init__(payload)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def get_option_chain(self, symbol: str, expiration: dt.date):
        self.calls.append((symbol, expiration))
        self.entered.set()
        await self.release.wait()
        return self.payload


@pytest.mark.asyncio
async def test_direct_upstream_coalesces_same_key_and_shields_cancelled_waiter() -> None:
    provider = _BlockingProvider(_payload())
    upstream = DirectSchwabOptionChainUpstream(provider)
    cancelled_waiter = asyncio.create_task(
        upstream.get_option_chain("SPX", EXPIRATION)
    )
    await provider.entered.wait()
    surviving_waiter = asyncio.create_task(
        upstream.get_option_chain("SPX", EXPIRATION)
    )

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter
    provider.release.set()
    result = await surviving_waiter
    await asyncio.sleep(0)

    assert result.symbol == "SPX"
    assert provider.calls == [("SPX", EXPIRATION)]
    assert upstream._inflight == {}


class _DistinctKeyProvider(_Provider):
    def __init__(self, payload: dict[str, object]) -> None:
        super().__init__(payload)
        self.all_entered = asyncio.Event()
        self.release = asyncio.Event()

    async def get_option_chain(self, symbol: str, expiration: dt.date):
        self.calls.append((symbol, expiration))
        if len(self.calls) == 2:
            self.all_entered.set()
        await self.release.wait()
        return self.payload


@pytest.mark.asyncio
async def test_direct_upstream_never_coalesces_distinct_exact_keys() -> None:
    provider = _DistinctKeyProvider(_payload())
    upstream = DirectSchwabOptionChainUpstream(provider)
    spx = asyncio.create_task(upstream.get_option_chain("SPX", EXPIRATION))
    dollar_spx = asyncio.create_task(upstream.get_option_chain("$SPX", EXPIRATION))

    await asyncio.wait_for(provider.all_entered.wait(), timeout=1)
    provider.release.set()
    results = await asyncio.gather(spx, dollar_spx)

    assert [result.symbol for result in results] == ["SPX", "$SPX"]
    assert set(provider.calls) == {("SPX", EXPIRATION), ("$SPX", EXPIRATION)}


@pytest.mark.asyncio
async def test_direct_upstream_bounds_detached_timeouts_and_recovers() -> None:
    provider = _DistinctKeyProvider(_payload())
    upstream = DirectSchwabOptionChainUpstream(provider, max_inflight=2)

    async def timed_out_read(symbol: str) -> None:
        async with asyncio.timeout(0.01):
            await upstream.get_option_chain(symbol, EXPIRATION)

    results = await asyncio.gather(
        timed_out_read("SPX"),
        timed_out_read("NDX"),
        return_exceptions=True,
    )
    assert all(isinstance(result, TimeoutError) for result in results)
    assert len(upstream._inflight) == 2

    with pytest.raises(UpstreamUnavailableError, match="in-flight capacity"):
        await upstream.get_option_chain("XSP", EXPIRATION)
    assert len(upstream._inflight) == 2

    provider.release.set()
    for _ in range(10):
        if not upstream._inflight:
            break
        await asyncio.sleep(0)
    assert upstream._inflight == {}

    recovered = await upstream.get_option_chain("RUT", EXPIRATION)
    assert recovered.symbol == "RUT"


@pytest.mark.asyncio
async def test_direct_upstream_prunes_globally_and_enforces_storage_bounds() -> None:
    clock = _Clock()
    provider = _Provider(_payload())
    upstream = DirectSchwabOptionChainUpstream(
        provider,
        cache_max_entries=2,
        monotonic_clock=clock.monotonic,
        utcnow=clock.utcnow,
    )

    await upstream.get_option_chain("SPX", EXPIRATION)
    await upstream.get_option_chain("NDX", EXPIRATION)
    await upstream.get_option_chain("XSP", EXPIRATION)
    assert list(upstream._cache) == [("NDX", EXPIRATION), ("XSP", EXPIRATION)]
    assert upstream._cache_bytes <= upstream._cache_max_bytes

    clock.advance(4)
    await upstream.get_option_chain("RUT", EXPIRATION)
    assert list(upstream._cache) == [("RUT", EXPIRATION)]

    uncached_provider = _Provider(_payload())
    uncached = DirectSchwabOptionChainUpstream(uncached_provider, cache_max_bytes=1)
    await uncached.get_option_chain("SPX", EXPIRATION)
    await uncached.get_option_chain("SPX", EXPIRATION)
    assert len(uncached_provider.calls) == 2
    assert uncached._cache_bytes == 0


class _Readiness:
    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=TokenManagerState.READY,
            reason="test",
            updated_at=RECEIVED_AT,
        )


class _Quotes:
    async def get_quotes(self, _symbols: tuple[str, ...]) -> tuple:
        return ()


class _OptionChainUpstream:
    def __init__(self, payload: dict[str, object] | None = None) -> None:
        self.calls: list[tuple[str, dt.date]] = []
        self.payload = payload or _payload()

    async def get_option_chain(self, symbol: str, expiration: dt.date) -> OptionChainV1:
        self.calls.append((symbol, expiration))
        return normalize_schwab_option_chain(
            symbol,
            self.payload,
            expiration,
            received_at=RECEIVED_AT,
            stale_after_seconds=90,
        )


def _authenticator() -> InternalKeyAuthenticator:
    return InternalKeyAuthenticator(
        (
            InternalPrincipal(
                client_id="butterfly-guy",
                key_sha256=hash_api_key("valid-key"),
                capabilities=frozenset({"market_data:read"}),
                priority_class=PriorityClass.PROTECTED,
            ),
        )
    )


@pytest.mark.asyncio
async def test_sdk_calls_distinct_full_chain_route_and_returns_typed_contract() -> None:
    upstream = _OptionChainUpstream()
    app = create_app(
        _Quotes(),
        _authenticator(),
        token_readiness_provider=_Readiness(),
        option_chain_upstream=upstream,
    )
    server = TestServer(app)
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_option_chain("SPX", EXPIRATION)
        await client.close()
    finally:
        await server.close()

    assert response.schema_version == "1.0"
    assert response.option_chain.symbol == "SPX"
    assert len(response.option_chain.contracts) == 4
    assert upstream.calls == [("SPX", EXPIRATION)]


@pytest.mark.asyncio
async def test_negative_time_value_is_null_through_http_and_sdk_contract() -> None:
    payload = _payload()
    payload["callExpDateMap"]["2026-08-24:0"]["6450.0"][0]["timeValue"] = -265.57
    upstream = _OptionChainUpstream(payload)
    app = create_app(
        _Quotes(),
        _authenticator(),
        token_readiness_provider=_Readiness(),
        option_chain_upstream=upstream,
    )
    server = TestServer(app)
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_option_chain("SPX", EXPIRATION)
        await client.close()
    finally:
        await server.close()

    assert response.option_chain.contracts[0].time_value is None
    assert response.option_chain.contracts[0].mark == 1.2
    assert upstream.calls == [("SPX", EXPIRATION)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"symbol": "SPX"},
        {"symbol": "bad symbol", "expiration": "2026-08-24"},
        {"symbol": "SPX", "expiration": "not-a-date"},
    ],
)
async def test_full_chain_route_validates_before_calling_upstream(params) -> None:
    upstream = _OptionChainUpstream()
    app = create_app(
        _Quotes(),
        _authenticator(),
        token_readiness_provider=_Readiness(),
        option_chain_upstream=upstream,
    )
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            response = await client.get(
                "/v1/option-chain",
                params=params,
                headers={"X-Internal-API-Key": "valid-key"},
            )
    finally:
        await server.close()

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert upstream.calls == []


class _BlockingOptionChainUpstream(_OptionChainUpstream):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def get_option_chain(self, symbol: str, expiration: dt.date) -> OptionChainV1:
        self.entered.set()
        await self.release.wait()
        return await super().get_option_chain(symbol, expiration)


@pytest.mark.asyncio
async def test_full_chain_capacity_is_bounded_and_fails_closed_with_429() -> None:
    upstream = _BlockingOptionChainUpstream()
    app = create_app(
        _Quotes(),
        _authenticator(),
        token_readiness_provider=_Readiness(),
        option_chain_upstream=upstream,
        admission_policy=AdmissionPolicy(protected_capacity=1, background_capacity=1),
    )
    server = TestServer(app)
    await server.start_server()
    first: asyncio.Task[httpx.Response] | None = None
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            params = {"symbol": "SPX", "expiration": EXPIRATION.isoformat()}
            headers = {"X-Internal-API-Key": "valid-key"}
            first = asyncio.create_task(
                client.get("/v1/option-chain", params=params, headers=headers)
            )
            await upstream.entered.wait()
            rejected = await client.get(
                "/v1/option-chain", params=params, headers=headers
            )
            upstream.release.set()
            admitted = await first
    finally:
        upstream.release.set()
        if first is not None and not first.done():
            await first
        await server.close()

    assert admitted.status_code == 200
    assert rejected.status_code == 429
    assert rejected.json()["error"] == {
        "code": "gateway_capacity_exceeded",
        "message": "gateway request capacity is unavailable",
    }


@pytest.mark.asyncio
async def test_full_chain_timeout_fails_closed_with_504() -> None:
    upstream = _BlockingOptionChainUpstream()
    app = create_app(
        _Quotes(),
        _authenticator(),
        token_readiness_provider=_Readiness(),
        option_chain_upstream=upstream,
        upstream_timeout_seconds=0.01,
    )
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            response = await client.get(
                "/v1/option-chain",
                params={"symbol": "SPX", "expiration": EXPIRATION.isoformat()},
                headers={"X-Internal-API-Key": "valid-key"},
            )
    finally:
        upstream.release.set()
        await server.close()

    assert response.status_code == 504
    assert response.json()["error"] == {
        "code": "upstream_timeout",
        "message": "market data upstream timed out",
    }
