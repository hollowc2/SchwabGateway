"""Tests for the gateway runner's demo and live modes.

No test here reads a real credential or token document or contacts Schwab. Token
documents are synthetic files in ``tmp_path`` and the client factory is never invoked,
because none of these tests performs a market-data request.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from schwab_token_store import (
    AtomicFileTokenStore,
    AtomicTokenManager,
    TokenManagerState,
    TokenMissingError,
)

from schwab_gateway import runner
from schwab_gateway.api import (
    CHAIN_UPSTREAM_KEY,
    HISTORY_UPSTREAM_KEY,
    MOVERS_UPSTREAM_KEY,
    OPTION_CHAIN_UPSTREAM_KEY,
    SESSION_HISTORY_UPSTREAM_KEY,
    SPOT_UPSTREAM_KEY,
    TOKEN_READINESS_PROVIDER_KEY,
    UPSTREAM_KEY,
    event_loop_lag_context,
    execution_scheduler_context,
)
from schwab_gateway.config import GatewaySettings
from schwab_gateway.live_provider import (
    TokenReadinessRecovery,
    UpstreamWarmupReadiness,
)
from schwab_gateway.upstream import (
    DirectSchwabChainMetadataUpstream,
    DirectSchwabHistoryUpstream,
    DirectSchwabMoversUpstream,
    DirectSchwabOptionChainUpstream,
    DirectSchwabQuoteUpstream,
    DirectSchwabSessionHistoryUpstream,
    DirectSchwabSpotUpstream,
)

KEYS_PAYLOAD = {
    "version": 1,
    "clients": [
        {
            "id": "butterfly-guy",
            "key_sha256": "a" * 64,
            "capabilities": ["market_data:read"],
            "priority_class": "protected",
        }
    ],
}


def _keys_file(tmp_path: Path) -> Path:
    path = tmp_path / "schwab-gateway-keys.json"
    path.write_text(json.dumps(KEYS_PAYLOAD), encoding="utf-8")
    path.chmod(0o600)
    return path


def _token_file(tmp_path: Path) -> Path:
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {
                "creation_timestamp": int(time.time() - 60),
                "token": {
                    "access_token": "synthetic-access",
                    "refresh_token": "synthetic-refresh",
                },
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _settings(tmp_path: Path) -> GatewaySettings:
    return GatewaySettings(internal_keys_path=_keys_file(tmp_path))


def _upstream_settings(token_path: Path) -> runner.GatewayUpstreamSettings:
    return runner.GatewayUpstreamSettings(
        SCHWAB_API_KEY="fake-key",
        SCHWAB_SECRET_KEY="fake-secret",
        SCHWAB_TOKEN_PATH=str(token_path),
    )


def _unused_factory(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("no client should be constructed while building the app")


# --- Argument gating -----------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param([], id="no_mode"),
        pytest.param(["--demo", "--serve-live"], id="both_modes"),
    ],
)
def test_exactly_one_mode_is_required(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        runner.main(argv)
    assert exc.value.code == 2


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["--serve-live"], id="no_confirmations"),
        pytest.param(
            ["--serve-live", "--authorize-real-credential-read"],
            id="missing_single_writer",
        ),
        pytest.param(
            ["--serve-live", "--confirm-single-token-writer"],
            id="missing_credential_authorization",
        ),
    ],
)
def test_live_serving_refuses_without_both_confirmations(argv: list[str]) -> None:
    """Refusal happens in argparse, before any setting, key file, or token is touched."""
    with pytest.raises(SystemExit) as exc:
        runner.main(argv)
    assert exc.value.code == 2


def test_demo_mode_needs_no_confirmations() -> None:
    parser = runner.build_parser()
    args = parser.parse_args(["--demo"])
    assert args.demo is True
    assert args.serve_live is False


def test_runner_enables_transport_handler_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, dict[str, object]]] = []
    settings = SimpleNamespace(
        internal_keys_path=Path("/unused"),
        log_level="INFO",
        bind_host="127.0.0.1",
        port=8010,
    )
    app = object()
    monkeypatch.setattr(runner, "GatewaySettings", lambda: settings)
    monkeypatch.setattr(runner, "build_demo_app", lambda _settings: app)
    monkeypatch.setattr(runner, "setup_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runner.web,
        "run_app",
        lambda value, **kwargs: calls.append((value, kwargs)),
    )

    runner.main(["--demo"])

    assert calls == [
        (
            app,
            {
                "host": "127.0.0.1",
                "port": 8010,
                "handler_cancellation": True,
            },
        )
    ]


# --- Demo mode is unchanged ----------------------------------------------------------


def test_demo_app_declares_no_spot_or_chain_upstream(tmp_path: Path) -> None:
    app = runner.build_demo_app(_settings(tmp_path))

    assert isinstance(app[UPSTREAM_KEY], runner.DemoQuoteUpstream)
    # The fail-closed stubs, not real upstreams.
    assert not isinstance(app[SPOT_UPSTREAM_KEY], DirectSchwabSpotUpstream)
    assert not isinstance(app[CHAIN_UPSTREAM_KEY], DirectSchwabChainMetadataUpstream)
    assert not isinstance(app[OPTION_CHAIN_UPSTREAM_KEY], DirectSchwabOptionChainUpstream)
    assert not isinstance(app[HISTORY_UPSTREAM_KEY], DirectSchwabHistoryUpstream)
    assert not isinstance(app[MOVERS_UPSTREAM_KEY], DirectSchwabMoversUpstream)
    assert not isinstance(app[SESSION_HISTORY_UPSTREAM_KEY], DirectSchwabSessionHistoryUpstream)


def test_demo_app_uses_the_static_readiness_provider(tmp_path: Path) -> None:
    app = runner.build_demo_app(_settings(tmp_path))
    assert not isinstance(app[TOKEN_READINESS_PROVIDER_KEY], AtomicTokenManager)


# --- Live mode -----------------------------------------------------------------------


def test_live_app_declares_all_seven_real_upstreams(tmp_path: Path) -> None:
    app = runner.build_live_app(
        _settings(tmp_path),
        _upstream_settings(_token_file(tmp_path)),
        _unused_factory,
    )

    assert isinstance(app[UPSTREAM_KEY], DirectSchwabQuoteUpstream)
    assert isinstance(app[SPOT_UPSTREAM_KEY], DirectSchwabSpotUpstream)
    assert isinstance(app[CHAIN_UPSTREAM_KEY], DirectSchwabChainMetadataUpstream)
    assert isinstance(app[OPTION_CHAIN_UPSTREAM_KEY], DirectSchwabOptionChainUpstream)
    assert isinstance(app[HISTORY_UPSTREAM_KEY], DirectSchwabHistoryUpstream)
    assert isinstance(app[MOVERS_UPSTREAM_KEY], DirectSchwabMoversUpstream)
    assert isinstance(app[SESSION_HISTORY_UPSTREAM_KEY], DirectSchwabSessionHistoryUpstream)


def test_live_app_reports_real_manager_readiness(tmp_path: Path) -> None:
    """The readiness surface must reflect the real token manager, not a static fake."""
    app = runner.build_live_app(
        _settings(tmp_path),
        _upstream_settings(_token_file(tmp_path)),
        _unused_factory,
    )

    provider = app[TOKEN_READINESS_PROVIDER_KEY]
    assert isinstance(provider, UpstreamWarmupReadiness)


def test_live_app_gates_readiness_on_a_warmup_round_trip(tmp_path: Path) -> None:
    """``manager.load()`` reaches READY without touching Schwab, so a freshly built app
    must report not-ready until one real upstream read has succeeded -- otherwise a cold
    worker serves the first in-flight protected reads and drops them.
    """
    app = runner.build_live_app(
        _settings(tmp_path),
        _upstream_settings(_token_file(tmp_path)),
        _unused_factory,
    )
    provider = app[TOKEN_READINESS_PROVIDER_KEY]
    assert provider.is_warm is False
    assert provider.health().state is TokenManagerState.REFRESHING


def test_live_app_refuses_to_build_when_the_token_is_unusable(tmp_path: Path) -> None:
    """A missing token fails closed at startup rather than serving 503 forever."""
    missing = tmp_path / "absent-tokens.json"

    with pytest.raises(TokenMissingError):
        runner.build_live_app(
            _settings(tmp_path),
            _upstream_settings(missing),
            _unused_factory,
        )


def test_live_app_builds_no_client_and_makes_no_request(tmp_path: Path) -> None:
    """Startup reads the token document; it does not construct a client or call Schwab."""
    calls: list[tuple] = []

    def recording_factory(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        raise AssertionError("client construction must not happen at startup")

    runner.build_live_app(
        _settings(tmp_path),
        _upstream_settings(_token_file(tmp_path)),
        recording_factory,
    )
    assert calls == []


def test_live_app_registers_readiness_recovery(tmp_path: Path) -> None:
    app = runner.build_live_app(
        _settings(tmp_path),
        _upstream_settings(_token_file(tmp_path)),
        _unused_factory,
    )
    # scheduler drain + event-loop lag sampler + upstream warmup + readiness recovery
    assert len(app.cleanup_ctx) == 4
    assert execution_scheduler_context in app.cleanup_ctx
    assert event_loop_lag_context in app.cleanup_ctx


def test_live_app_registers_opt_in_order_book_feed(tmp_path: Path) -> None:
    app = runner.build_live_app(
        GatewaySettings(
            internal_keys_path=_keys_file(tmp_path),
            order_book_stream_enabled=True,
            order_book_stream_symbols="AAPL",
        ),
        _upstream_settings(_token_file(tmp_path)),
        _unused_factory,
    )
    # scheduler drain + lag sampler + upstream warmup + readiness recovery + feed
    assert len(app.cleanup_ctx) == 5


def test_live_app_refuses_enabled_order_book_feed_without_symbols(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no symbols"):
        runner.build_live_app(
            GatewaySettings(
                internal_keys_path=_keys_file(tmp_path),
                order_book_stream_enabled=True,
            ),
            _upstream_settings(_token_file(tmp_path)),
            _unused_factory,
        )


def test_demo_app_registers_only_scheduler_cleanup(tmp_path: Path) -> None:
    """The demo has no recovery loop, but still owns a scheduled fake worker."""
    app = runner.build_demo_app(_settings(tmp_path))
    assert list(app.cleanup_ctx) == [execution_scheduler_context]


# --- Readiness recovery --------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_does_not_touch_the_token_while_ready(tmp_path: Path) -> None:
    """A healthy gateway must never take the token lock on the recovery path."""
    manager = AtomicTokenManager(AtomicFileTokenStore(_token_file(tmp_path)))
    manager.load()
    loads = 0

    original_load = manager.load

    def counting_load():
        nonlocal loads
        loads += 1
        return original_load()

    manager.load = counting_load  # type: ignore[method-assign]

    assert await TokenReadinessRecovery(manager).attempt_once() is True
    assert loads == 0


@pytest.mark.asyncio
async def test_recovery_relifts_a_latched_manager(tmp_path: Path) -> None:
    """The latch this fixes: a token-level failure that no request can clear.

    Every route and /ready gate on READY, so once the manager leaves READY the request
    that would have produced a recovering transaction is itself refused.
    """
    token = _token_file(tmp_path)
    manager = AtomicTokenManager(AtomicFileTokenStore(token))
    manager.load()
    assert manager.health().state is TokenManagerState.READY

    # Latch it the way a real token-level failure would.
    token.unlink()
    with pytest.raises(TokenMissingError):
        manager.load()
    assert manager.health().state is not TokenManagerState.READY

    recovery = TokenReadinessRecovery(manager)
    assert await recovery.attempt_once() is False
    assert manager.health().state is not TokenManagerState.READY

    # The transient condition clears; the next tick recovers without any request.
    _token_file(tmp_path)
    assert await recovery.attempt_once() is True
    assert manager.health().state is TokenManagerState.READY


@pytest.mark.asyncio
async def test_recovery_leaves_the_recorded_state_alone_when_it_keeps_failing(
    tmp_path: Path,
) -> None:
    manager = AtomicTokenManager(AtomicFileTokenStore(tmp_path / "absent.json"))
    recovery = TokenReadinessRecovery(manager)

    assert await recovery.attempt_once() is False
    first = manager.health().state
    assert await recovery.attempt_once() is False
    assert manager.health().state is first


def test_recovery_interval_must_be_positive(tmp_path: Path) -> None:
    manager = AtomicTokenManager(AtomicFileTokenStore(_token_file(tmp_path)))
    with pytest.raises(ValueError):
        TokenReadinessRecovery(manager, interval_seconds=0)


# --- Upstream warmup readiness ------------------------------------------------------


@pytest.mark.asyncio
async def test_warmup_gate_reports_not_ready_until_a_read_succeeds(
    tmp_path: Path,
) -> None:
    manager = AtomicTokenManager(AtomicFileTokenStore(_token_file(tmp_path)))
    manager.load()
    calls = 0

    async def warmup() -> None:
        nonlocal calls
        calls += 1

    gate = UpstreamWarmupReadiness(manager, warmup)

    assert gate.is_warm is False
    assert gate.health().state is TokenManagerState.REFRESHING

    assert await gate.attempt_once() is True
    assert gate.is_warm is True
    assert calls == 1
    # Once warm the gate delegates to the real manager and stops calling the warmup.
    assert gate.health().state is manager.health().state
    assert await gate.attempt_once() is True
    assert calls == 1


@pytest.mark.asyncio
async def test_warmup_gate_stays_not_ready_while_the_read_keeps_failing() -> None:
    manager = SimpleNamespace(health=lambda: None)

    async def warmup() -> None:
        raise RuntimeError("schwab is unavailable")

    gate = UpstreamWarmupReadiness(manager, warmup)  # type: ignore[arg-type]

    assert await gate.attempt_once() is False
    assert gate.is_warm is False
    assert gate.health().state is TokenManagerState.REFRESHING


@pytest.mark.asyncio
async def test_warmup_gate_run_until_warm_stops_on_first_success() -> None:
    outcomes = iter([False, False, True])

    async def warmup() -> None:
        if not next(outcomes):
            raise RuntimeError("not yet")

    gate = UpstreamWarmupReadiness(
        SimpleNamespace(health=lambda: "delegated"),  # type: ignore[arg-type]
        warmup,
        retry_seconds=0.001,
    )
    await gate.run_until_warm()
    assert gate.is_warm is True
    assert gate.health() == "delegated"


@pytest.mark.asyncio
async def test_warmup_ctx_blocks_startup_until_warm_then_yields() -> None:
    manager = SimpleNamespace(health=lambda: "delegated")

    async def warmup() -> None:
        return None

    gate = UpstreamWarmupReadiness(manager, warmup)  # type: ignore[arg-type]
    ctx = runner._upstream_warmup_ctx(gate)(None)  # type: ignore[arg-type]

    await ctx.__anext__()
    assert gate.is_warm is True
    with pytest.raises(StopAsyncIteration):
        await ctx.__anext__()


@pytest.mark.asyncio
async def test_warmup_ctx_starts_serving_not_ready_when_warmup_fails() -> None:
    manager = SimpleNamespace(health=lambda: "delegated")
    attempts = 0

    async def warmup() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("schwab down")

    gate = UpstreamWarmupReadiness(manager, warmup, retry_seconds=0.01)  # type: ignore[arg-type]
    ctx = runner._upstream_warmup_ctx(gate, startup_timeout_seconds=0.05)(None)  # type: ignore[arg-type]

    await ctx.__anext__()
    # Startup completed (yielded) even though the gate never warmed.
    assert gate.is_warm is False
    with pytest.raises(StopAsyncIteration):
        await ctx.__anext__()
    assert attempts >= 1


def test_warmup_retry_interval_must_be_positive() -> None:
    async def warmup() -> None:
        return None

    with pytest.raises(ValueError):
        UpstreamWarmupReadiness(
            SimpleNamespace(),  # type: ignore[arg-type]
            warmup,
            retry_seconds=0,
        )


def test_live_app_exposes_no_account_or_order_route(tmp_path: Path) -> None:
    app = runner.build_live_app(
        _settings(tmp_path),
        _upstream_settings(_token_file(tmp_path)),
        _unused_factory,
    )

    paths = {resource.canonical for resource in app.router.resources()}
    assert paths == {
        "/health",
        "/ready",
        "/metrics",
        "/v1/quotes",
        "/v1/spot",
        "/v1/chain",
        "/v1/option-chain",
        "/v1/history",
        "/v1/movers",
        "/v1/session-history",
        "/v1/order-book/recent",
        "/v1/order-book/stream",
    }
