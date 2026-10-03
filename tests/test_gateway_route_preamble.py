"""Pin the fixed check order every market-data route applies before any upstream work.

Capability is checked before parameters, and parameters before readiness, on every
route; validation failures return the exact parser message as ``invalid_request``.
"""

from __future__ import annotations

import httpx
import pytest
from schwab_token_store import TokenManagerState
from support import EmptyQuoteUpstream, FakeReadiness, serving, single_principal_authenticator

from schwab_gateway.api import create_app

# (path, invalid query, exact invalid_request message)
INVALID_REQUESTS = [
    ("/v1/quotes", {"symbols": "AAPL,AAPL"}, "symbols must be unique"),
    (
        "/v1/quotes",
        {"symbols": "AAPL", "allow_partial": "yes"},
        "allow_partial must be 'true' or 'false'",
    ),
    ("/v1/spot", {}, "a symbol is required"),
    (
        "/v1/chain",
        {"symbol": "SPX", "expiration": "2026-9-1"},
        "the expiration must be an ISO-8601 date",
    ),
    ("/v1/option-chain", {"symbol": "SPX"}, "an expiration is required"),
    ("/v1/history", {"symbol": "AAPL", "days_back": "x"}, "days_back must be an integer"),
    (
        "/v1/history",
        {"symbol": "AAPL", "frequency": "minute", "days_back": "6"},
        "days_back must be between 1 and 5",
    ),
    ("/v1/movers", {"index": "NOPE"}, "index must be one of the supported Schwab mover indexes"),
    (
        "/v1/session-history",
        {"symbol": "AAPL", "date": "2026-09-28"},
        "session must be 'regular' or 'extended'",
    ),
    (
        "/v1/order-book/recent",
        {"symbol": "AAPL", "venue": "NASDAQ", "limit": "0"},
        "limit must be between 1 and 1000",
    ),
    (
        "/v1/order-book/stream",
        {"symbols": ",".join(f"S{i}" for i in range(26)), "venue": "NASDAQ"},
        "at most 25 order-book stream symbols are allowed",
    ),
    (
        "/v1/order-book/stream",
        {"symbols": "AAPL", "venue": "ARCA"},
        "venue must be 'NASDAQ' or 'NYSE'",
    ),
]


def _app(*, capability: bool):
    return create_app(
        EmptyQuoteUpstream(),
        single_principal_authenticator(capability="market_data:read" if capability else None),
        token_readiness_provider=FakeReadiness(TokenManagerState.REFRESHING),
    )


async def _get(app, path: str, params: dict[str, str]) -> httpx.Response:
    async with serving(app) as server:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            return await client.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )


@pytest.mark.parametrize(("path", "params", "message"), INVALID_REQUESTS)
async def test_capability_is_checked_before_parameters(path, params, message) -> None:
    response = await _get(_app(capability=False), path, params)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "capability_denied"


@pytest.mark.parametrize(("path", "params", "message"), INVALID_REQUESTS)
async def test_parameters_are_checked_before_readiness(path, params, message) -> None:
    response = await _get(_app(capability=True), path, params)

    assert response.status_code == 400
    assert response.json() == {
        "schema_version": "1.0",
        "error": {"code": "invalid_request", "message": message},
    }
