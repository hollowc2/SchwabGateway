"""Fake-verification adapter for schwab-py's access-function lifecycle."""

from __future__ import annotations

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
    """

    def __init__(
        self,
        token_manager: AtomicTokenManager,
        client_factory: SchwabAccessFunctionClientFactory[ClientT],
        *,
        api_key: str,
        app_secret: str,
    ) -> None:
        self._token_manager = token_manager
        self._client_factory = client_factory
        self._api_key = api_key
        self._app_secret = app_secret

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
            except TokenManagerError:
                raise
            except Exception:
                log.warning(
                    "schwab_token_adapter_failed",
                    reason="client_construction_failed",
                )
                raise SchwabClientConstructionError(
                    "Schwab client construction failed"
                ) from None

            try:
                return operation(client)
            except TokenManagerError:
                raise
            except Exception:
                log.warning(
                    "schwab_token_adapter_failed",
                    reason="client_operation_failed",
                )
                raise SchwabClientOperationError(
                    "Schwab client operation failed"
                ) from None

        return self._token_manager.run_access_transaction(run_locked)
