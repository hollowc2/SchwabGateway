"""Fake-verification adapter for schwab-py's access-function lifecycle."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Protocol, TypeVar

from schwab_token_store import (
    AtomicTokenManager,
    TokenManagerError,
    TokenReadCallback,
    TokenWriteCallback,
)

from schwab_gateway.logging import get_logger

log = get_logger(__name__)

ClientT = TypeVar("ClientT")
OperationResult = TypeVar("OperationResult")


class SchwabAccessFunctionClientFactory(Protocol[ClientT]):
    """Signature of schwab.auth.client_from_access_functions in schwab-py 1.5.1."""

    def __call__(
        self,
        api_key: str,
        app_secret: str,
        token_read_func: TokenReadCallback,
        token_write_func: TokenWriteCallback,
        asyncio: bool = False,
        enforce_enums: bool = True,
    ) -> ClientT: ...


class SchwabTokenAdapterError(RuntimeError):
    """Base error with a bounded message safe for gateway logs and responses."""


class SchwabClientConstructionError(SchwabTokenAdapterError):
    pass


class SchwabClientOperationError(SchwabTokenAdapterError):
    pass


class LockedSchwabClientAdapter:
    """Construct and use one injected client inside a token-manager transaction.

    A new client (and `requests.Session`) is built on every call because
    `token_read_func`/`token_write_func` are transaction-scoped closures handed to us
    by `AtomicTokenManager.run_access_transaction` for the duration of the held lock —
    schwab-py's client captures them at construction time, so a client built outside a
    transaction would close over stale callbacks. Reusing a session would need the
    token manager to expose a stable read/write pair usable across transactions, which
    is a bigger change than this per-call TCP+TLS cost (tens-to-~100ms) justifies on
    its own; it is not the dominant contributor to option-chain latency (see the
    2026-09 latency investigation) and is accepted as-is for now.

    ``http_timeout_seconds`` replaces schwab-py's 30-second default on every client
    built here. The scheduler never cancels a call that overruns its budget. It waits
    for the call to finish before it frees the single execution slot. So this timeout,
    not the scheduler budget, bounds how long one stalled Schwab response can block
    every other read. On 2026-09-29, a stalled spot read held the slot for 28.9 seconds
    and 13 queued protected reads returned 503.
    """

    def __init__(
        self,
        token_manager: AtomicTokenManager,
        client_factory: SchwabAccessFunctionClientFactory[ClientT],
        *,
        api_key: str,
        app_secret: str,
        http_timeout_seconds: float | None = None,
    ) -> None:
        if http_timeout_seconds is not None and not (
            math.isfinite(http_timeout_seconds) and http_timeout_seconds > 0
        ):
            raise ValueError("Schwab HTTP timeout must be finite and positive")
        self._token_manager = token_manager
        self._client_factory = client_factory
        self._api_key = api_key
        self._app_secret = app_secret
        self._http_timeout_seconds = http_timeout_seconds

    def execute(
        self,
        operation: Callable[[ClientT], OperationResult],
    ) -> OperationResult:
        """Run client construction and one operation without letting callbacks escape."""

        def run_locked(
            token_read_func: TokenReadCallback,
            token_write_func: TokenWriteCallback,
        ) -> OperationResult:
            try:
                client = self._client_factory(
                    self._api_key,
                    self._app_secret,
                    token_read_func,
                    token_write_func,
                    asyncio=False,
                    enforce_enums=True,
                )
                if self._http_timeout_seconds is not None:
                    client.set_timeout(self._http_timeout_seconds)
            except TokenManagerError:
                raise
            except Exception as exc:
                log.warning(
                    "schwab_token_adapter_failed",
                    reason="client_construction_failed",
                    error_type=type(exc).__name__,
                )
                raise SchwabClientConstructionError(
                    "Schwab client construction failed"
                ) from None

            try:
                return operation(client)
            except TokenManagerError:
                raise
            except Exception as exc:
                # The class name (for example ``ReadTimeout`` or ``HTTPStatusError``) tells
                # a stall apart from a Schwab error. The message can carry a URL, so it is
                # not logged.
                log.warning(
                    "schwab_token_adapter_failed",
                    reason="client_operation_failed",
                    error_type=type(exc).__name__,
                )
                raise SchwabClientOperationError(
                    "Schwab client operation failed"
                ) from None

        return self._token_manager.run_access_transaction(run_locked)
