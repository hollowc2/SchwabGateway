"""Fake-backed proof of the collector-facing gateway surfaces: /v1/spot and /v1/chain."""

from __future__ import annotations

import asyncio
import datetime as dt

import httpx
import pytest
from aiohttp.test_utils import TestServer
from schwab_gateway_sdk.client import (
    GatewayAuthenticationError,
    GatewayAuthorizationError,
    GatewayCapacityError,
    GatewayMarketDataClient,
    GatewayResponseError,
    GatewayTimeoutError,
    GatewayUnavailableError,
)
from schwab_gateway_sdk.models import ChainMetadataV1, SpotV1
from schwab_token_store import (
    TokenManagerHealth,
    TokenManagerState,
)

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.api import create_app
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)
from schwab_gateway.upstream import (
    DirectSchwabChainMetadataUpstream,
    DirectSchwabSpotUpstream,
    UpstreamMalformedError,
    UpstreamUnavailableError,
)

EXPIRATION = dt.date(2026, 8, 6)


def authenticator(*, capability: str | None = "market_data:read") -> InternalKeyAuthenticator:
    return InternalKeyAuthenticator(
        (
            InternalPrincipal(
                client_id="butterfly-guy",
                key_sha256=hash_api_key("valid-key"),
                capabilities=frozenset({capability} if capability else set()),
                priority_class=PriorityClass.PROTECTED,
            ),
        )
    )


class FakeReadiness:
    def __init__(self, state: TokenManagerState = TokenManagerState.READY) -> None:
        self.state = state

    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=self.state,
            reason="fake reason",
            updated_at=dt.datetime.now(dt.timezone.utc),
        )


class FakeQuoteUpstream:
    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple:
        return ()


class FakeSpotUpstream:
    def __init__(self, price: float = 5000.25, error: Exception | None = None) -> None:
        self.price = price
        self.error = error
        self.calls: list[str] = []

    async def get_spot(self, symbol: str) -> SpotV1:
        self.calls.append(symbol)
        if self.error is not None:
            raise self.error
        return SpotV1(
            symbol=symbol,
            price=self.price,
            gateway_received_at=dt.datetime.now(dt.timezone.utc),
            source="fake_spot",
            stale=False,
            age_seconds=0.5,
        )


class FakeChainUpstream:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, dt.date]] = []

    async def get_chain_metadata(self, symbol: str, expiration: dt.date) -> ChainMetadataV1:
        self.calls.append((symbol, expiration))
        if self.error is not None:
            raise self.error
        return ChainMetadataV1(
            symbol=symbol,
            expiration=expiration,
            underlying_price=5000.25,
            call_contract_count=3,
            put_contract_count=3,
            strike_count=3,
            gateway_received_at=dt.datetime.now(dt.timezone.utc),
            source="fake_chain",
            stale=False,
            age_seconds=1.0,
        )


def app(
    *,
    spot_upstream=None,
    chain_upstream=None,
    capability: str | None = "market_data:read",
    ready: bool = True,
    upstream_timeout_seconds: float = 3.0,
    admission_policy: AdmissionPolicy | None = None,
):
    return create_app(
        FakeQuoteUpstream(),
        authenticator(capability=capability),
        upstream_timeout_seconds=upstream_timeout_seconds,
        token_readiness_provider=FakeReadiness(
            TokenManagerState.READY if ready else TokenManagerState.MISSING
        ),
        admission_policy=admission_policy,
        spot_upstream=spot_upstream,
        chain_upstream=chain_upstream,
    )


# --- typed success through the real in-process app -------------------------------------


@pytest.mark.asyncio
async def test_client_to_http_gateway_to_fake_spot_upstream_returns_typed_contract() -> None:
    upstream = FakeSpotUpstream(price=5123.5)
    server = TestServer(app(spot_upstream=upstream))
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_spot("$SPX")
        await client.close()
    finally:
        await server.close()

    assert response.schema_version == "1.0"
    assert response.spot.symbol == "$SPX"
    assert response.spot.price == 5123.5
    assert response.spot.stale is False
    assert upstream.calls == ["$SPX"]


@pytest.mark.asyncio
async def test_client_to_http_gateway_to_fake_chain_upstream_returns_metadata_only() -> None:
    upstream = FakeChainUpstream()
    server = TestServer(app(chain_upstream=upstream))
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_chain_metadata("SPX", EXPIRATION)
        raw = await client._client.get(
            "/v1/chain",
            params={"symbol": "SPX", "expiration": EXPIRATION.isoformat()},
            headers={"X-Internal-API-Key": "valid-key"},
        )
        await client.close()
    finally:
        await server.close()

    assert response.chain.symbol == "SPX"
    assert response.chain.expiration == EXPIRATION
    assert response.chain.call_contract_count == 3
    assert response.chain.strike_count == 3
    assert upstream.calls == [("SPX", EXPIRATION), ("SPX", EXPIRATION)]

    body = raw.json()
    assert set(body) == {"schema_version", "chain"}
    assert not any(
        key in raw.text
        for key in ("callExpDateMap", "putExpDateMap", "strikePrice", "bid", "ask")
    )


# --- authentication, capability, and validation ----------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/spot", {"symbol": "$SPX"}),
        ("/v1/chain", {"symbol": "SPX", "expiration": "2026-08-06"}),
    ],
)
async def test_missing_key_is_401_and_wrong_capability_is_403(
    path: str, params: dict[str, str]
) -> None:
    server = TestServer(
        app(
            spot_upstream=FakeSpotUpstream(),
            chain_upstream=FakeChainUpstream(),
            capability=None,
        )
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            missing = await http.get(path, params=params)
            invalid = await http.get(
                path, params=params, headers={"X-Internal-API-Key": "wrong-key"}
            )
            denied = await http.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )
    finally:
        await server.close()

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert "wrong-key" not in invalid.text
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "capability_denied"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/spot", {}),
        ("/v1/spot", {"symbol": ""}),
        ("/v1/spot", {"symbol": "bad symbol"}),
        ("/v1/spot", {"symbol": "A" * 33}),
        ("/v1/chain", {"symbol": "SPX"}),
        ("/v1/chain", {"symbol": "SPX", "expiration": ""}),
        ("/v1/chain", {"symbol": "SPX", "expiration": "not-a-date"}),
        ("/v1/chain", {"symbol": "SPX", "expiration": "20260806"}),
        ("/v1/chain", {"symbol": "SPX", "expiration": "2026-08-06T00:00:00"}),
        ("/v1/chain", {"symbol": "bad symbol", "expiration": "2026-08-06"}),
    ],
)
async def test_malformed_parameters_are_400_before_any_upstream_call(
    path: str, params: dict[str, str]
) -> None:
    spot_upstream = FakeSpotUpstream()
    chain_upstream = FakeChainUpstream()
    server = TestServer(app(spot_upstream=spot_upstream, chain_upstream=chain_upstream))
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )
    finally:
        await server.close()

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert spot_upstream.calls == []
    assert chain_upstream.calls == []


# --- readiness, capacity, and upstream failures ----------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/spot", {"symbol": "$SPX"}),
        ("/v1/chain", {"symbol": "SPX", "expiration": "2026-08-06"}),
    ],
)
async def test_not_ready_is_503_and_never_reaches_the_upstream(
    path: str, params: dict[str, str]
) -> None:
    spot_upstream = FakeSpotUpstream()
    chain_upstream = FakeChainUpstream()
    server = TestServer(
        app(spot_upstream=spot_upstream, chain_upstream=chain_upstream, ready=False)
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )
    finally:
        await server.close()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "gateway_not_ready"
    assert spot_upstream.calls == []
    assert chain_upstream.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/spot", {"symbol": "$SPX"}),
        ("/v1/chain", {"symbol": "SPX", "expiration": "2026-08-06"}),
    ],
)
async def test_exhausted_capacity_is_429(path: str, params: dict[str, str]) -> None:
    release = asyncio.Event()

    class BlockingUpstream:
        async def get_spot(self, symbol: str) -> SpotV1:
            await release.wait()
            raise UpstreamUnavailableError("never returns a value in this test")

        async def get_chain_metadata(self, symbol: str, expiration: dt.date):
            await release.wait()
            raise UpstreamUnavailableError("never returns a value in this test")

    blocking = BlockingUpstream()
    server = TestServer(
        app(
            spot_upstream=blocking,
            chain_upstream=blocking,
            admission_policy=AdmissionPolicy(protected_capacity=1, background_capacity=1),
        )
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            held = asyncio.create_task(
                http.get(path, params=params, headers={"X-Internal-API-Key": "valid-key"})
            )
            await asyncio.sleep(0.05)
            rejected = await http.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )
            release.set()
            await held
    finally:
        await server.close()

    assert rejected.status_code == 429
    assert rejected.json()["error"]["code"] == "gateway_capacity_exceeded"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (UpstreamUnavailableError("upstream down"), 503, "upstream_unavailable"),
        (UpstreamMalformedError("garbage"), 502, "upstream_malformed"),
        (ValueError("garbage"), 502, "upstream_malformed"),
    ],
)
@pytest.mark.parametrize("surface", ["spot", "chain"])
async def test_upstream_failures_map_to_bounded_status_codes(
    surface: str, error: Exception, status: int, code: str
) -> None:
    if surface == "spot":
        kwargs = {"spot_upstream": FakeSpotUpstream(error=error)}
        path, params = "/v1/spot", {"symbol": "$SPX"}
    else:
        kwargs = {"chain_upstream": FakeChainUpstream(error=error)}
        path, params = "/v1/chain", {"symbol": "SPX", "expiration": "2026-08-06"}

    server = TestServer(app(**kwargs))
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )
    finally:
        await server.close()

    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert "garbage" not in response.text
    assert "upstream down" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["spot", "chain"])
async def test_upstream_timeout_is_504_for_both_surfaces(surface: str) -> None:
    class SlowUpstream:
        async def get_spot(self, symbol: str):
            await asyncio.sleep(0.05)

        async def get_chain_metadata(self, symbol: str, expiration: dt.date):
            await asyncio.sleep(0.05)

    slow = SlowUpstream()
    server = TestServer(
        app(spot_upstream=slow, chain_upstream=slow, upstream_timeout_seconds=0.001)
    )
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        with pytest.raises(GatewayTimeoutError):
            if surface == "spot":
                await client.get_spot("$SPX")
            else:
                await client.get_chain_metadata("SPX", EXPIRATION)
        await client.close()
    finally:
        await server.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["spot", "chain"])
async def test_undeclared_surfaces_fail_closed_as_unavailable(surface: str) -> None:
    server = TestServer(app())
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        with pytest.raises(GatewayUnavailableError):
            if surface == "spot":
                await client.get_spot("$SPX")
            else:
                await client.get_chain_metadata("SPX", EXPIRATION)
        await client.close()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_upstream_returning_a_different_subject_is_rejected_as_malformed() -> None:
    class WrongSymbolUpstream:
        async def get_spot(self, symbol: str) -> SpotV1:
            return SpotV1(
                symbol="OTHER",
                price=1.0,
                gateway_received_at=dt.datetime.now(dt.timezone.utc),
                source="fake_spot",
                stale=False,
            )

        async def get_chain_metadata(self, symbol: str, expiration: dt.date):
            return ChainMetadataV1(
                symbol=symbol,
                expiration=expiration + dt.timedelta(days=1),
                call_contract_count=0,
                put_contract_count=0,
                strike_count=0,
                gateway_received_at=dt.datetime.now(dt.timezone.utc),
                source="fake_chain",
                stale=False,
            )

    wrong = WrongSymbolUpstream()
    server = TestServer(app(spot_upstream=wrong, chain_upstream=wrong))
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            spot_response = await http.get(
                "/v1/spot",
                params={"symbol": "$SPX"},
                headers={"X-Internal-API-Key": "valid-key"},
            )
            chain_response = await http.get(
                "/v1/chain",
                params={"symbol": "SPX", "expiration": "2026-08-06"},
                headers={"X-Internal-API-Key": "valid-key"},
            )
    finally:
        await server.close()

    assert spot_response.status_code == 502
    assert chain_response.status_code == 502
    assert spot_response.json()["error"]["code"] == "upstream_malformed"
    assert chain_response.json()["error"]["code"] == "upstream_malformed"


# --- route table -----------------------------------------------------------------------


def test_new_surfaces_add_no_account_or_order_route() -> None:
    application = app(spot_upstream=FakeSpotUpstream(), chain_upstream=FakeChainUpstream())
    shapes = {(route.method, route.resource.canonical) for route in application.router.routes()}

    assert {path for _method, path in shapes} == {
        "/health",
        "/ready",
        "/metrics",
        "/v1/quotes",
        "/v1/spot",
        "/v1/chain",
    }
    assert {method for method, _path in shapes} == {"GET", "HEAD"}
    assert not any(
        sensitive in path
        for _method, path in shapes
        for sensitive in ("account", "order", "position", "transaction", "stream")
    )


# --- client error classification -------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, GatewayAuthenticationError),
        (403, GatewayAuthorizationError),
        (429, GatewayCapacityError),
        (504, GatewayTimeoutError),
        (502, GatewayUnavailableError),
        (503, GatewayUnavailableError),
        (418, GatewayResponseError),
    ],
)
@pytest.mark.parametrize("surface", ["spot", "chain"])
async def test_client_maps_every_status_to_a_bounded_error(
    surface: str, status: int, expected: type[Exception]
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"schema_version": "1.0", "error": {}})

    async with httpx.AsyncClient(
        base_url="http://gateway.invalid",
        transport=httpx.MockTransport(handler),
    ) as http:
        client = GatewayMarketDataClient("http://gateway.invalid", "key", client=http)
        with pytest.raises(expected):
            if surface == "spot":
                await client.get_spot("$SPX")
            else:
                await client.get_chain_metadata("SPX", EXPIRATION)


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["spot", "chain"])
async def test_client_rejects_an_invalid_contract_and_never_retries(surface: str) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"schema_version": "1.0", "unexpected": True})

    async with httpx.AsyncClient(
        base_url="http://gateway.invalid",
        transport=httpx.MockTransport(handler),
    ) as http:
        client = GatewayMarketDataClient("http://gateway.invalid", "key", client=http)
        with pytest.raises(GatewayResponseError):
            if surface == "spot":
                await client.get_spot("$SPX")
            else:
                await client.get_chain_metadata("SPX", EXPIRATION)

    assert len(calls) == 1
    assert calls[0].headers["X-Internal-API-Key"] == "key"


@pytest.mark.asyncio
async def test_client_transport_failures_do_not_retry() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ConnectError("refused", request=request)

    async with httpx.AsyncClient(
        base_url="http://gateway.invalid",
        transport=httpx.MockTransport(handler),
    ) as http:
        client = GatewayMarketDataClient("http://gateway.invalid", "key", client=http)
        with pytest.raises(GatewayUnavailableError):
            await client.get_spot("$SPX")

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_client_rejects_empty_symbol_and_non_date_expiration_before_any_request() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(
        base_url="http://gateway.invalid",
        transport=httpx.MockTransport(handler),
    ) as http:
        client = GatewayMarketDataClient("http://gateway.invalid", "key", client=http)
        with pytest.raises(ValueError):
            await client.get_spot("   ")
        with pytest.raises(ValueError):
            await client.get_chain_metadata("SPX", dt.datetime(2026, 8, 6, tzinfo=dt.timezone.utc))

    assert calls == []


# --- direct upstream normalization -----------------------------------------------------


@pytest.mark.asyncio
async def test_direct_spot_upstream_reports_unknown_freshness_honestly() -> None:
    class Provider:
        async def get_spot_price(self, symbol: str = "$SPX") -> float:
            return 5000.5

    result = await DirectSchwabSpotUpstream(Provider()).get_spot("$SPX")

    assert result.price == 5000.5
    assert result.event_timestamp is None
    assert result.age_seconds is None
    assert result.stale is True
    assert "missing_event_timestamp" in result.data_quality_flags


@pytest.mark.asyncio
async def test_direct_spot_upstream_classifies_provider_failure_as_unavailable() -> None:
    class Failing:
        async def get_spot_price(self, symbol: str = "$SPX") -> float:
            raise RuntimeError("/private/token/path leaked")

    with pytest.raises(UpstreamUnavailableError) as excinfo:
        await DirectSchwabSpotUpstream(Failing()).get_spot("$SPX")

    assert "/private" not in str(excinfo.value)


@pytest.mark.asyncio
async def test_direct_spot_upstream_classifies_malformed_payload_as_malformed_not_unavailable() -> (
    None
):
    """A parse failure (e.g. ``extract_spot_price`` raising on a malformed payload) must
    surface as ``UpstreamMalformedError``, not be folded into the same
    ``UpstreamUnavailableError`` a genuine fetch failure produces."""

    class MalformedPayload:
        async def get_spot_price(self, symbol: str = "$SPX") -> float:
            raise ValueError("spot response carried no usable price")

    with pytest.raises(UpstreamMalformedError):
        await DirectSchwabSpotUpstream(MalformedPayload()).get_spot("$SPX")


@pytest.mark.asyncio
async def test_direct_spot_upstream_still_classifies_fetch_failure_as_unavailable() -> None:
    """A non-parse failure (network/timeout/adapter error) must still be unavailable."""

    class FetchFailure:
        async def get_spot_price(self, symbol: str = "$SPX") -> float:
            raise RuntimeError("connection reset")

    with pytest.raises(UpstreamUnavailableError):
        await DirectSchwabSpotUpstream(FetchFailure()).get_spot("$SPX")


@pytest.mark.asyncio
async def test_direct_chain_upstream_summarizes_without_contract_rows() -> None:
    payload = {
        "underlyingPrice": 5000.25,
        "underlying": {"quoteTime": 1785000000000},
        "callExpDateMap": {
            "2026-08-06:0": {
                "5000.0": [{"bid": 1.0}],
                "5010.0": [{"bid": 0.5}],
            }
        },
        "putExpDateMap": {
            "2026-08-06:0": {"5000.0": [{"bid": 1.1}]},
            "2026-08-07:1": {"4990.0": [{"bid": 2.0}]},
        },
    }

    class Provider:
        async def get_option_chain(self, symbol: str, expiration: dt.date):
            return payload

    result = await DirectSchwabChainMetadataUpstream(Provider()).get_chain_metadata(
        "SPX", EXPIRATION
    )

    assert result.call_contract_count == 2
    assert result.put_contract_count == 1
    assert result.strike_count == 2
    assert result.underlying_price == 5000.25
    assert result.event_timestamp is not None
    dumped = result.model_dump(mode="json")
    assert "callExpDateMap" not in dumped
    assert set(dumped) == {
        "symbol",
        "expiration",
        "underlying_price",
        "call_contract_count",
        "put_contract_count",
        "strike_count",
        "event_timestamp",
        "gateway_received_at",
        "source",
        "stale",
        "age_seconds",
        "data_quality_flags",
    }


@pytest.mark.asyncio
async def test_direct_chain_upstream_tolerates_a_payload_with_no_expiration_map() -> None:
    """``extract_chain_metadata`` now matches both live parsers on this shape (a payload
    present but with neither ``callExpDateMap`` nor ``putExpDateMap``): it is a legitimate
    zero-count chain, not a malformed one, so ``GET /v1/chain`` must return 200, not 502.
    """

    class Provider:
        async def get_option_chain(self, symbol: str, expiration: dt.date):
            return {"status": "FAILED"}

    fields = await DirectSchwabChainMetadataUpstream(Provider()).get_chain_metadata(
        "SPX", EXPIRATION
    )
    assert fields.call_contract_count == 0
    assert fields.put_contract_count == 0
    assert fields.strike_count == 0


async def test_direct_chain_upstream_rejects_a_payload_that_is_not_an_object() -> None:
    """The one shape ``extract_chain_metadata`` still refuses: not a dict at all."""

    class Provider:
        async def get_option_chain(self, symbol: str, expiration: dt.date):
            return "not-a-chain-payload"

    with pytest.raises(UpstreamMalformedError):
        await DirectSchwabChainMetadataUpstream(Provider()).get_chain_metadata(
            "SPX", EXPIRATION
        )
