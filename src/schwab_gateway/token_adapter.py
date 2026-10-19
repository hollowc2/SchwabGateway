"""Fake-verification adapter for schwab-py's access-function lifecycle."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Protocol, TypeVar

import httpx
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


def _upstream_status(exc: Exception) -> dict[str, int]:
    """Schwab's HTTP status for an ``httpx.HTTPStatusError``, or nothing."""

    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(status_code, int) and not isinstance(status_code, bool):
        return {"upstream_status": status_code}
    return {}


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

    ``http_timeout_seconds`` replaces schwab-py's 30-second default on every client.
    An optional shorter connect timeout limits connection stalls independently. HTTP
    timeouts apply per phase (and per received chunk), not to total operation time.
    The scheduler never cancels a call that overruns its budget: it retains the slot
    until the operation and token transaction finish.
    """

    def __init__(
        self,
        token_manager: AtomicTokenManager,
        client_factory: SchwabAccessFunctionClientFactory[ClientT],
        *,
        api_key: str,
        app_secret: str,
        http_timeout_seconds: float | None = None,
        http_connect_timeout_seconds: float | None = None,
    ) -> None:
        if http_timeout_seconds is not None and not (
            math.isfinite(http_timeout_seconds) and http_timeout_seconds > 0
        ):
            raise ValueError("Schwab HTTP timeout must be finite and positive")
        if http_connect_timeout_seconds is not None:
            if not math.isfinite(http_connect_timeout_seconds) or http_connect_timeout_seconds <= 0:
                raise ValueError("Schwab HTTP connect timeout must be finite and positive")
            if http_timeout_seconds is None or http_connect_timeout_seconds > http_timeout_seconds:
                raise ValueError("Schwab HTTP connect timeout requires a matching HTTP timeout")
        self._token_manager = token_manager
        self._client_factory = client_factory
        self._api_key = api_key
        self._app_secret = app_secret
        self._http_timeout_seconds = http_timeout_seconds
        self._http_connect_timeout_seconds = http_connect_timeout_seconds

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
                    timeout: float | httpx.Timeout = self._http_timeout_seconds
                    if self._http_connect_timeout_seconds is not None:
                        timeout = httpx.Timeout(
                            self._http_timeout_seconds,
                            connect=self._http_connect_timeout_seconds,
                        )
                    client.set_timeout(timeout)
            except TokenManagerError:
                raise
            except Exception as exc:
                log.warning(
                    "schwab_token_adapter_failed",
                    reason="client_construction_failed",
                    error_type=type(exc).__name__,
                )
                raise SchwabClientConstructionError("Schwab client construction failed") from None

            try:
                return operation(client)
            except TokenManagerError:
                raise
            except Exception as exc:
                # The class name (for example ``ReadTimeout`` or ``HTTPStatusError``) tells
                # a stall apart from a Schwab error, and Schwab's status code tells a
                # rejected request from a Schwab outage. The message can carry a URL, so
                # it is not logged.
                log.warning(
                    "schwab_token_adapter_failed",
                    reason="client_operation_failed",
                    error_type=type(exc).__name__,
                    **_upstream_status(exc),
                )
                raise SchwabClientOperationError("Schwab client operation failed") from None

        return self._token_manager.run_access_transaction(run_locked)
