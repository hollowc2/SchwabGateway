from __future__ import annotations

import asyncio
import datetime as dt
import time

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
from support import FakeReadiness, serving, single_principal_authenticator

from schwab_gateway import api
from schwab_gateway.api import (
    create_app,
    gateway_event_loop_lag_distribution,
    gateway_quote_partial_responses,
    gateway_requests,
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


@pytest.mark.asyncio
async def test_client_to_http_gateway_to_fake_upstream_contract() -> None:
    upstream = FakeQuoteUpstream()
    async with serving(
        create_app(
            upstream,
            single_principal_authenticator(),
            token_readiness_provider=FakeReadiness(TokenManagerState.READY),
        )
    ) as server:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_quotes(["AAPL", "MSFT"])
        await client.close()

    assert response.schema_version == "1.0"
    assert [quote.symbol for quote in response.quotes] == ["AAPL", "MSFT"]
    assert response.quotes[0].bid == 100.0
    assert upstream.calls == [("AAPL", "MSFT")]


@pytest.mark.asyncio
async def test_gateway_authentication_authorization_and_health_contracts() -> None:
    async with serving(
        create_app(FakeQuoteUpstream(), single_principal_authenticator(capability=None))
    ) as server:
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
    provider = FakeReadiness(
        state,
        reason="access-secret and /private/token/path must never be exposed",
    )
    async with serving(
        create_app(
            FakeQuoteUpstream(),
            single_principal_authenticator(),
            token_readiness_provider=provider,
        )
    ) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get("/ready")

    payload = response.json()
    assert response.status_code == (200 if state is TokenManagerState.READY else 503)
    assert payload["status"] == ("ready" if state is TokenManagerState.READY else "not_ready")
    assert payload["token_state"] == state.value
    assert payload["reason"]
    assert "secret" not in response.text
    assert "/private" not in response.text


@pytest.mark.asyncio
async def test_ready_tracks_fake_refresh_failure_and_recovery() -> None:
    provider = FakeReadiness(TokenManagerState.READY)
    async with serving(
        create_app(
            FakeQuoteUpstream(),
            single_principal_authenticator(),
            token_readiness_provider=provider,
        )
    ) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            ready = await http.get("/ready")
            provider.state = TokenManagerState.REFRESHING
            refreshing = await http.get("/ready")
            provider.state = TokenManagerState.REFRESH_FAILED
            failed = await http.get("/ready")
            provider.state = TokenManagerState.READY
            recovered = await http.get("/ready")

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
    async with serving(create_app(FakeQuoteUpstream(), single_principal_authenticator())) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get("/ready")

    assert response.status_code == 503
    assert response.json()["token_state"] == TokenManagerState.UNINITIALIZED.value
    assert response.json()["reason"] == "token_not_checked"


@pytest.mark.asyncio
async def test_ready_fails_closed_when_provider_fails_without_exposing_its_error() -> None:
    class FailingProvider:
        def health(self) -> TokenManagerHealth:
            raise RuntimeError("access-secret at /private/token/path")

    async with serving(
        create_app(
            FakeQuoteUpstream(),
            single_principal_authenticator(),
            token_readiness_provider=FailingProvider(),
        )
    ) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as http:
            response = await http.get("/ready")

    assert response.status_code == 503
    assert response.json()["token_state"] == TokenManagerState.UNINITIALIZED.value
    assert response.json()["reason"] == "token_readiness_unavailable"
    assert "secret" not in response.text
    assert "/private" not in response.text


@pytest.mark.asyncio
async def test_gateway_validates_symbols_and_exposes_no_order_routes() -> None:
    app = create_app(FakeQuoteUpstream(), single_principal_authenticator())
    async with serving(app) as server:
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

    route_shapes = {(route.method, route.resource.canonical) for route in app.router.routes()}
    assert response.status_code == 400
    assert missing_order.status_code == 404
    assert 'gateway_client_requests_total{operation="unknown",status="404"} 1.0' in metrics.text
    for operation in ("spot_v1", "option_chain_v1", "history_v1"):
        for status in ("503", "504"):
            assert (
                f'gateway_client_requests_total{{operation="{operation}",status="{status}"}}'
            ) in metrics.text
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
            single_principal_authenticator(),
            token_readiness_provider=FakeReadiness(TokenManagerState.READY),
        )
    )
    await server.start_server()

    def _count(status: str) -> float:
        return gateway_requests.labels(operation="quotes_v1", status=status)._value.get()

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
async def test_request_log_skips_successful_probes_and_records_query(capfd) -> None:
    readiness = FakeReadiness(TokenManagerState.READY)
    async with serving(
        create_app(
            FakeQuoteUpstream(),
            single_principal_authenticator(),
            token_readiness_provider=readiness,
        )
    ) as server:
        async with httpx.AsyncClient() as http:
            for path in ("/health", "/ready", "/metrics"):
                assert (await http.get(str(server.make_url(path)))).status_code == 200
            readiness.state = TokenManagerState.EXPIRED
            assert (await http.get(str(server.make_url("/ready")))).status_code == 503
            readiness.state = TokenManagerState.READY
            quote = await http.get(
                str(server.make_url("/v1/quotes?symbols=AAPL,MSFT")),
                headers={"X-Internal-API-Key": "valid-key"},
            )

    assert quote.status_code == 200
    request_logs = [
        line for line in capfd.readouterr().out.splitlines() if "gateway_request " in line
    ]
    assert len(request_logs) == 2
    assert "operation=ready" in request_logs[0]
    assert "status=503" in request_logs[0]
    assert "operation=quotes_v1" in request_logs[1]
    assert "query='symbols=AAPL,MSFT'" in request_logs[1]


@pytest.mark.asyncio
async def test_gateway_surfaces_upstream_timeout() -> None:
    class SlowUpstream:
        async def get_quotes(self, _symbols):
            await asyncio.sleep(0.05)
            return ()

    async with serving(
        create_app(
            SlowUpstream(),
            single_principal_authenticator(),
            upstream_timeout_seconds=0.001,
            token_readiness_provider=FakeReadiness(TokenManagerState.READY),
        )
    ) as server:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        with pytest.raises(GatewayTimeoutError):
            await client.get_quotes(["AAPL"])
        await client.close()


@pytest.mark.asyncio
async def test_partial_quote_set_fails_closed_and_names_missing_symbols(capfd) -> None:
    class OmittingUpstream(FakeQuoteUpstream):
        async def get_quotes(self, symbols):
            quotes = await super().get_quotes(symbols)
            return tuple(quote for quote in quotes if quote.symbol != "ZZZQ")

    server = TestServer(
        create_app(
            OmittingUpstream(),
            single_principal_authenticator(),
            token_readiness_provider=FakeReadiness(TokenManagerState.READY),
        )
    )
    await server.start_server()
    failed = gateway_quote_partial_responses.labels(outcome="failed")
    before = failed._value.get()
    try:
        async with httpx.AsyncClient() as http:
            response = await http.get(
                str(server.make_url("/v1/quotes?symbols=AAPL,ZZZQ,MSFT")),
                headers={"X-Internal-API-Key": "valid-key"},
            )
    finally:
        await server.close()

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_malformed"
    assert failed._value.get() == before + 1
    partial_logs = [
        line
        for line in capfd.readouterr().out.splitlines()
        if "gateway_quote_partial_symbol_set" in line
    ]
    assert len(partial_logs) == 1
    assert "outcome=failed" in partial_logs[0]
    assert "missing_count=1" in partial_logs[0]
    assert "missing_symbols=['ZZZQ']" in partial_logs[0]
    assert "requested_count=3" in partial_logs[0]


class OmittingQuoteUpstream(FakeQuoteUpstream):
    """Behaves like Schwab dropping unquotable tickers from an otherwise good batch."""

    def __init__(self, omitted: set[str]) -> None:
        super().__init__()
        self.omitted = omitted

    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]:
        quotes = await super().get_quotes(symbols)
        return tuple(quote for quote in quotes if quote.symbol not in self.omitted)


@pytest.mark.asyncio
async def test_opted_in_partial_quote_set_serves_returned_quotes_and_names_missing(
    capfd,
) -> None:
    server = TestServer(
        create_app(
            OmittingQuoteUpstream({"ZZZQ"}),
            single_principal_authenticator(),
            token_readiness_provider=FakeReadiness(TokenManagerState.READY),
        )
    )
    await server.start_server()
    served = gateway_quote_partial_responses.labels(outcome="served_partial")
    before = served._value.get()
    try:
        client = GatewayMarketDataClient(str(server.make_url("/")), "valid-key")
        response = await client.get_available_quotes(["AAPL", "ZZZQ", "MSFT"])
        await client.close()
    finally:
        await server.close()

    assert [quote.symbol for quote in response.quotes] == ["AAPL", "MSFT"]
    assert response.missing_symbols == ("ZZZQ",)
    assert served._value.get() == before + 1
    partial_logs = [
        line
        for line in capfd.readouterr().out.splitlines()
        if "gateway_quote_partial_symbol_set" in line
    ]
    assert len(partial_logs) == 1
    assert "[info" in partial_logs[0]
    assert "outcome=served_partial" in partial_logs[0]
    assert "missing_symbols=['ZZZQ']" in partial_logs[0]


@pytest.mark.asyncio
async def test_allow_partial_keeps_the_default_shape_and_its_own_limits() -> None:
    async with serving(
        create_app(
            OmittingQuoteUpstream({"ZZZQ"}),
            single_principal_authenticator(),
            token_readiness_provider=FakeReadiness(TokenManagerState.READY),
        )
    ) as server:
        async with httpx.AsyncClient(
            base_url=str(server.make_url("/")),
            headers={"X-Internal-API-Key": "valid-key"},
        ) as http:
            complete = await http.get(
                "/v1/quotes", params={"symbols": "AAPL,MSFT", "allow_partial": "true"}
            )
            default = await http.get("/v1/quotes", params={"symbols": "AAPL,MSFT"})
            nothing = await http.get(
                "/v1/quotes", params={"symbols": "ZZZQ", "allow_partial": "true"}
            )
            invalid = await http.get(
                "/v1/quotes", params={"symbols": "AAPL", "allow_partial": "yes"}
            )

    assert complete.status_code == 200
    assert complete.json()["missing_symbols"] == []
    # Callers pinned to an older SDK reject unknown fields, so the default response
    # must never grow ``missing_symbols``.
    assert default.status_code == 200
    assert set(default.json()) == {"schema_version", "quotes"}
    assert nothing.status_code == 502
    assert nothing.json()["error"]["code"] == "upstream_malformed"
    assert invalid.status_code == 400
    assert invalid.json()["error"]["code"] == "invalid_request"


def test_partial_quote_log_bounds_the_symbol_list(capfd) -> None:
    requested = tuple(f"S{index}" for index in range(25))

    api._log_partial_quote_response(requested, {})

    line = next(
        line
        for line in capfd.readouterr().out.splitlines()
        if "gateway_quote_partial_symbol_set" in line
    )
    assert "missing_count=25" in line
    assert "'S9'" in line
    assert "'S10'" not in line


def _lag_samples() -> dict[str, float]:
    return {
        sample.name + str(sample.labels.get("le", "")): sample.value
        for family in gateway_event_loop_lag_distribution.collect()
        for sample in family.samples
    }


@pytest.mark.asyncio
async def test_event_loop_lag_sampler_records_every_sample_in_a_histogram(
    monkeypatch,
) -> None:
    monkeypatch.setattr(api, "EVENT_LOOP_LAG_INTERVAL_SECONDS", 0.01)
    name = "gateway_event_loop_lag_distribution_seconds"
    before = _lag_samples()
    task = asyncio.create_task(api._observe_event_loop_lag())
    try:
        await asyncio.sleep(0.03)
        time.sleep(0.06)  # deliberately block the loop past the 50 ms bucket
        await asyncio.sleep(0.03)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    after = _lag_samples()

    assert after[f"{name}_count"] - before[f"{name}_count"] >= 3
    # At least one sample landed above 50 ms, i.e. outside the 0.05 cumulative bucket.
    new_total = after[f"{name}_count"] - before[f"{name}_count"]
    new_fast = after[f"{name}_bucket0.05"] - before[f"{name}_bucket0.05"]
    assert new_total - new_fast >= 1
