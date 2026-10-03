"""Shared fakes and server helpers for the gateway HTTP tests."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiohttp import web
from aiohttp.test_utils import TestServer
from schwab_token_store import TokenManagerHealth, TokenManagerState

from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)
from schwab_gateway.upstream import UpstreamUnavailableError

VALID_KEY = "valid-key"


def single_principal_authenticator(
    *, capability: str | None = "market_data:read"
) -> InternalKeyAuthenticator:
    """ButterflyGuy as the only, protected principal, keyed by ``VALID_KEY``."""
    return InternalKeyAuthenticator(
        (
            InternalPrincipal(
                client_id="butterfly-guy",
                key_sha256=hash_api_key(VALID_KEY),
                capabilities=frozenset({capability} if capability else set()),
                priority_class=PriorityClass.PROTECTED,
            ),
        )
    )


class FakeReadiness:
    """Token readiness whose ``state`` a test may change; the gateway reads only the state."""

    def __init__(
        self, state: TokenManagerState = TokenManagerState.READY, reason: str = "fake reason"
    ) -> None:
        self.state = state
        self.reason = reason

    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=self.state,
            reason=self.reason,
            updated_at=dt.datetime.now(dt.UTC),
        )


class EmptyQuoteUpstream:
    """The required quote surface for apps whose tests exercise other routes."""

    async def get_quotes(self, _symbols: tuple[str, ...]) -> tuple:
        return ()


class BlockingSpotUpstream:
    """Hold the single scheduler slot until released."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def get_spot(self, symbol: str):
        self.entered.set()
        await self.release.wait()
        raise UpstreamUnavailableError("test spot upstream")


@asynccontextmanager
async def serving(app: web.Application) -> AsyncIterator[TestServer]:
    """Run ``app`` on a real local HTTP server for the block's lifetime."""
    server = TestServer(app)
    await server.start_server()
    try:
        yield server
    finally:
        await server.close()
