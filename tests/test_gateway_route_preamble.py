"""Pin the fixed check order every market-data route applies before any upstream work.

Capability is checked before parameters, and parameters before readiness, on every
route; validation failures return the exact parser message as ``invalid_request``.
"""

from __future__ import annotations

import datetime as dt

import httpx
import pytest
from aiohttp.test_utils import TestServer
from schwab_token_store import TokenManagerHealth, TokenManagerState

from schwab_gateway.api import create_app
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)

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


class _Quotes:
    async def get_quotes(self, _symbols: tuple[str, ...]) -> tuple:
        return ()


class _NotReady:
    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=TokenManagerState.REFRESHING,
            reason="test",
            updated_at=dt.datetime.now(dt.UTC),
        )


def _app(*, capability: bool):
    principal = InternalPrincipal(
        client_id="butterfly-guy",
        key_sha256=hash_api_key("valid-key"),
        capabilities=frozenset({"market_data:read"} if capability else set()),
        priority_class=PriorityClass.PROTECTED,
    )
    return create_app(
        _Quotes(), InternalKeyAuthenticator((principal,)), token_readiness_provider=_NotReady()
    )


async def _get(app, path: str, params: dict[str, str]) -> httpx.Response:
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            return await client.get(
                path, params=params, headers={"X-Internal-API-Key": "valid-key"}
            )
    finally:
        await server.close()


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
