"""Run the isolated read-only gateway, with explicit demo data or real Schwab reads."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any

from aiohttp import web
from schwab_gateway_sdk.models import QuoteV1
from schwab_token_store import (
    AtomicFileTokenStore,
    AtomicTokenManager,
    TokenManagerState,
)

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.api import StaticTokenReadinessProvider, create_app
from schwab_gateway.auth import InternalKeyAuthenticator
from schwab_gateway.config import GatewaySettings
from schwab_gateway.live_provider import (
    GatewayUpstreamSettings,
    LockedSchwabMarketDataProvider,
    TokenReadinessRecovery,
)
from schwab_gateway.logging import get_logger, setup_logging
from schwab_gateway.token_adapter import LockedSchwabClientAdapter
from schwab_gateway.upstream import (
    DirectSchwabChainMetadataUpstream,
    DirectSchwabHistoryUpstream,
    DirectSchwabMoversUpstream,
    DirectSchwabOptionChainUpstream,
    DirectSchwabQuoteUpstream,
    DirectSchwabSessionHistoryUpstream,
    DirectSchwabSpotUpstream,
)

log = get_logger(__name__)


class DemoQuoteUpstream:
    """Deterministic smoke-test upstream; never contacts Schwab."""

    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]:
        received_at = dt.datetime.now(dt.timezone.utc)
        return tuple(
            QuoteV1(
                symbol=symbol,
                gateway_received_at=received_at,
                source="foundation_demo",
                mark=100.0,
                stale=False,
                data_quality_flags=("demo_data_not_for_trading",),
            )
            for symbol in symbols
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--demo",
        action="store_true",
        help="serve deterministic fake quotes; no Schwab credentials are read",
    )
    parser.add_argument(
        "--serve-live",
        action="store_true",
        help="serve real Schwab market data through the locked token manager",
    )
    parser.add_argument("--authorize-real-credential-read", action="store_true")
    parser.add_argument("--confirm-single-token-writer", action="store_true")
    return parser


def build_demo_app(settings: GatewaySettings) -> web.Application:
    """Fake-only application. No credential is read and no upstream exists."""
    authenticator = InternalKeyAuthenticator.from_file(settings.internal_keys_path)
    return create_app(
        DemoQuoteUpstream(),
        authenticator,
        upstream_timeout_seconds=settings.upstream_timeout_seconds,
        token_readiness_provider=StaticTokenReadinessProvider(TokenManagerState.READY),
        admission_policy=AdmissionPolicy(
            protected_capacity=settings.protected_capacity,
            background_capacity=settings.background_capacity,
        ),
    )


def build_live_app(
    settings: GatewaySettings,
    upstream_settings: GatewayUpstreamSettings,
    client_factory: Any,
) -> web.Application:
    """Real application: three read surfaces over one locked token manager.

    The manager is loaded once here, before the application is returned, and a failure
    propagates so the process refuses to start. That startup load is not optional
    bookkeeping: ``/ready`` and every route gate on ``TokenManagerState.READY``, and the
    manager only reaches READY inside a transaction. Without priming it, no request could
    ever be admitted to produce the transaction that would make it ready, and the gateway
    would answer 503 forever while looking healthy at the process level.
    """
    authenticator = InternalKeyAuthenticator.from_file(settings.internal_keys_path)
    manager = AtomicTokenManager(AtomicFileTokenStore(upstream_settings.token_path))
    adapter = LockedSchwabClientAdapter(
        manager,
        client_factory,
        api_key=upstream_settings.api_key.get_secret_value(),
        app_secret=upstream_settings.app_secret.get_secret_value(),
    )
    provider = LockedSchwabMarketDataProvider(adapter)

    # One token read, no Schwab request. Fails closed.
    manager.load()

    app = create_app(
        DirectSchwabQuoteUpstream(provider),
        authenticator,
        upstream_timeout_seconds=settings.upstream_timeout_seconds,
        token_readiness_provider=manager,
        admission_policy=AdmissionPolicy(
            protected_capacity=settings.protected_capacity,
            background_capacity=settings.background_capacity,
        ),
        spot_upstream=DirectSchwabSpotUpstream(provider),
        chain_upstream=DirectSchwabChainMetadataUpstream(provider),
        option_chain_upstream=DirectSchwabOptionChainUpstream(provider),
        history_upstream=DirectSchwabHistoryUpstream(provider),
        movers_upstream=DirectSchwabMoversUpstream(provider),
        session_history_upstream=DirectSchwabSessionHistoryUpstream(provider),
    )
    app.cleanup_ctx.append(_readiness_recovery_ctx(TokenReadinessRecovery(manager)))
    return app


def _readiness_recovery_ctx(recovery: TokenReadinessRecovery) -> Any:
    """Run readiness recovery for the lifetime of the application.

    Registered only in live mode: the demo app's readiness is a static fake with nothing
    to recover.
    """

    async def ctx(_app: web.Application) -> AsyncIterator[None]:
        task = asyncio.create_task(recovery.run_forever())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    return ctx


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.demo == args.serve_live:
        parser.error("choose exactly one of --demo or --serve-live")
    if args.serve_live and not (
        args.authorize_real_credential_read and args.confirm_single_token_writer
    ):
        parser.error(
            "live serving requires explicit real-credential and single-token-writer "
            "confirmations"
        )

    settings = GatewaySettings()
    setup_logging(settings.log_level, json_output=True)

    if args.demo:
        app = build_demo_app(settings)
        upstream_name = "demo"
    else:
        from schwab.auth import client_from_access_functions

        app = build_live_app(settings, GatewayUpstreamSettings(), client_from_access_functions)
        upstream_name = "schwab_locked"

    log.info(
        "schwab_gateway_foundation_starting",
        bind_host=settings.bind_host,
        port=settings.port,
        upstream=upstream_name,
        order_writes_enabled=False,
    )
    web.run_app(app, host=settings.bind_host, port=settings.port)


if __name__ == "__main__":
    main()
