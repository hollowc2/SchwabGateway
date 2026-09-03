from __future__ import annotations

import asyncio
import datetime as dt

import httpx
import pytest
from aiohttp.test_utils import TestServer
from schwab_gateway_sdk.client import (
    GatewayAuthorizationError,
    GatewayMarketDataClient,
    GatewayTimeoutError,
)
from schwab_gateway_sdk.models import QuoteV1
from schwab_token_store import (
    TokenManagerHealth,
    TokenManagerState,
)

from schwab_gateway.api import create_app, gateway_requests
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)


class FakeQuoteUpstream:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]:
        self.calls.append(symbols)
        now = dt.datetime.now(dt.timezone.utc)
        return tuple(
            QuoteV1(
                symbol=symbol,
                event_timestamp=now,
                gateway_received_at=now,
                source="fake_schwab",
                bid=100.0,
                ask=100.2,
                mark=100.1,
                stale=False,
                age_seconds=0,
            )
            for symbol in symbols
        )


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


class FakeTokenReadinessProvider:
    def __init__(self, state: TokenManagerState, reason: str = "fake reason") -> None:
        self.state = state
        self.reason = reason

    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=self.state,
            reason=self.reason,
            updated_at=dt.datetime.now(dt.timezone.utc),
        )


@pytest.mark.asyncio
async def test_client_to_http_gateway_to_fake_upstream_contract() -> None:
    upstream = FakeQuoteUpstream()
    server = TestServer(
        create_app(
            upstream,
            authenticator(),
            token_readiness_provider=FakeTokenReadinessProvider(TokenManagerState.READY),
        )
    )
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_quotes(["AAPL", "MSFT"])
        await client.close()
    finally:
        await server.close()

    assert response.schema_version == "1.0"
    assert [quote.symbol for quote in response.quotes] == ["AAPL", "MSFT"]
    assert response.quotes[0].bid == 100.0
    assert upstream.calls == [("AAPL", "MSFT")]


@pytest.mark.asyncio
async def test_gateway_authentication_authorization_and_health_contracts() -> None:
    server = TestServer(create_app(FakeQuoteUpstream(), authenticator(capability=None)))
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            health = await http.get("/health")
            missing = await http.get("/v1/quotes", params={"symbols": "AAPL"})
            invalid = await http.get(
                "/v1/quotes",
                params={"symbols": "AAPL"},
                headers={"X-Internal-API-Key": "invalid"},
            )
            client = GatewayMarketDataClient(
                str(server.make_url("/")),
                "valid-key",
                client=http,
            )
            with pytest.raises(GatewayAuthorizationError):
                await client.get_quotes(["AAPL"])
    finally:
        await server.close()

    assert health.status_code == 200
    assert health.json()["service"] == "schwab-gateway"
    assert "key" not in health.text.lower()
    assert missing.status_code == 401
    assert invalid.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(TokenManagerState))
async def test_ready_maps_every_token_manager_state_to_bounded_response(
    state: TokenManagerState,
) -> None:
    provider = FakeTokenReadinessProvider(
        state,
        reason="access-secret and /private/token/path must never be exposed",
    )
    server = TestServer(
        create_app(
            FakeQuoteUpstream(),
            authenticator(),
            token_readiness_provider=provider,
        )
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get("/ready")
    finally:
        await server.close()

    payload = response.json()
    assert response.status_code == (200 if state is TokenManagerState.READY else 503)
    assert payload["status"] == ("ready" if state is TokenManagerState.READY else "not_ready")
    assert payload["token_state"] == state.value
    assert payload["reason"]
    assert "secret" not in response.text
    assert "/private" not in response.text


@pytest.mark.asyncio
async def test_ready_tracks_fake_refresh_failure_and_recovery() -> None:
    provider = FakeTokenReadinessProvider(TokenManagerState.READY)
    server = TestServer(
        create_app(
            FakeQuoteUpstream(),
            authenticator(),
            token_readiness_provider=provider,
        )
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            ready = await http.get("/ready")
            provider.state = TokenManagerState.REFRESHING
            refreshing = await http.get("/ready")
            provider.state = TokenManagerState.REFRESH_FAILED
            failed = await http.get("/ready")
            provider.state = TokenManagerState.READY
            recovered = await http.get("/ready")
    finally:
        await server.close()

    assert [response.status_code for response in (ready, refreshing, failed, recovered)] == [
        200,
        503,
        503,
        200,
    ]
    assert [response.json()["reason"] for response in (ready, refreshing, failed, recovered)] == [
        "token_ready",
        "token_refreshing",
        "token_refresh_failed",
        "token_ready",
    ]


@pytest.mark.asyncio
async def test_ready_fails_closed_without_an_injected_provider() -> None:
    server = TestServer(create_app(FakeQuoteUpstream(), authenticator()))
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get("/ready")
    finally:
        await server.close()

    assert response.status_code == 503
    assert response.json()["token_state"] == TokenManagerState.UNINITIALIZED.value
    assert response.json()["reason"] == "token_not_checked"


@pytest.mark.asyncio
async def test_ready_fails_closed_when_provider_fails_without_exposing_its_error() -> None:
    class FailingProvider:
        def health(self) -> TokenManagerHealth:
            raise RuntimeError("access-secret at /private/token/path")

    server = TestServer(
        create_app(
            FakeQuoteUpstream(),
            authenticator(),
            token_readiness_provider=FailingProvider(),
        )
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get("/ready")
    finally:
        await server.close()

    assert response.status_code == 503
    assert response.json()["token_state"] == TokenManagerState.UNINITIALIZED.value
    assert response.json()["reason"] == "token_readiness_unavailable"
    assert "secret" not in response.text
    assert "/private" not in response.text


@pytest.mark.asyncio
async def test_gateway_validates_symbols_and_exposes_no_order_routes() -> None:
    app = create_app(FakeQuoteUpstream(), authenticator())
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get(
                "/v1/quotes",
                params={"symbols": "AAPL,bad symbol"},
                headers={"X-Internal-API-Key": "valid-key"},
            )
            missing_order = await http.post(
                "/v1/orders",
                headers={"X-Internal-API-Key": "valid-key"},
            )
            metrics = await http.get("/metrics")
    finally:
        await server.close()

    route_shapes = {(route.method, route.resource.canonical) for route in app.router.routes()}
    assert response.status_code == 400
    assert missing_order.status_code == 404
    assert (
        'gateway_client_requests_total{operation="unknown",status="404"} 1.0'
        in metrics.text
    )
    assert all(path != "/v1/orders" for _method, path in route_shapes)
    assert all(method != "POST" for method, _path in route_shapes)


@pytest.mark.asyncio
async def test_client_disconnect_is_recorded_as_499_not_500(capfd) -> None:
    class SlowUpstream:
        async def get_quotes(self, _symbols):
            await asyncio.sleep(1.0)
            return ()

    server = TestServer(
        create_app(
            SlowUpstream(),
            authenticator(),
            token_readiness_provider=FakeTokenReadinessProvider(TokenManagerState.READY),
        )
    )
    await server.start_server()

    def _count(status: str) -> float:
        return (
            gateway_requests.labels(operation="quotes_v1", status=status)._value.get()
        )

    before_499 = _count("499")
    before_500 = _count("500")
    try:
        async with httpx.AsyncClient(timeout=0.1) as http:
            with pytest.raises(httpx.TimeoutException):
                await http.get(
                    str(server.make_url("/v1/quotes?symbols=AAPL")),
                    headers={"X-Internal-API-Key": "valid-key"},
                )
        await asyncio.sleep(0.05)
    finally:
        await server.close()

    assert _count("499") == before_499 + 1
    assert _count("500") == before_500
    request_logs = [
        line
        for line in capfd.readouterr().out.splitlines()
        if "gateway_request " in line and "quotes_v1" in line
    ]
    assert request_logs
    assert all("status=499" in line for line in request_logs)
    assert all("caller=butterfly-guy" in line for line in request_logs)


@pytest.mark.asyncio
async def test_gateway_surfaces_upstream_timeout() -> None:
    class SlowUpstream:
        async def get_quotes(self, _symbols):
            await asyncio.sleep(0.05)
            return ()

    server = TestServer(
        create_app(
            SlowUpstream(),
            authenticator(),
            upstream_timeout_seconds=0.001,
            token_readiness_provider=FakeTokenReadinessProvider(TokenManagerState.READY),
        )
    )
    await server.start_server()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        with pytest.raises(GatewayTimeoutError):
            await client.get_quotes(["AAPL"])
        await client.close()
    finally:
        await server.close()
