"""Real Schwab market-data reads bound to the single locked token manager.

This is the bridge between the proven token machinery and the gateway's three read
surfaces. It exposes only ``SpotPriceProvider``, ``OptionChainProvider``, and
``EquityQuoteProvider``; there is no account, order, transaction, or streaming method to
call, so no such request can be issued through this object.

Three properties are deliberate and load-bearing:

- **One transaction per call.** Every read runs inside its own
  ``LockedSchwabClientAdapter.execute``, which constructs a client, performs one
  operation, persists any rotation, and invalidates its callbacks before releasing the
  token lock. That is the lifecycle the adapter was fake-proven and host-proven under, and
  it is why the gateway can hold a production token safely.
- **The lock serializes everything.** The token manager holds an exclusive lock for the
  duration of each transaction, so concurrent gateway requests queue behind one another
  regardless of the admission policy's capacities. Admission bounds queue depth here, not
  parallelism.
- **No retries.** ``SchwabClientWrapper._retry`` retries three times with backoff on the
  direct path. This one does not, because retrying inside a held token lock multiplies
  the time every other caller waits, and the gateway client is specified to add no
  retries of its own. A failed read is a failed read.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from schwab_token_store import (
    AtomicTokenManager,
    TokenManagerError,
    TokenManagerState,
)

from schwab_gateway.logging import get_logger
from schwab_gateway.token_adapter import LockedSchwabClientAdapter

log = get_logger(__name__)

DEFAULT_QUOTE_BATCH_SIZE = 150
DEFAULT_READINESS_RECOVERY_SECONDS = 30.0


class GatewayUpstreamSettings(BaseSettings):
    """Real credential inputs for a live-serving gateway process.

    Deliberately a separate class from ``GatewayCredentialProbeSettings`` rather than a
    reuse of it. That class and the module it lives in are members of the credential
    proof's reviewed archive, whose SHA-256 is gated on Helios; editing or widening it
    would change the archive hash for a proof that is already complete.
    """

    model_config = SettingsConfigDict(extra="ignore")

    api_key: SecretStr = Field(validation_alias="SCHWAB_API_KEY")
    app_secret: SecretStr = Field(validation_alias="SCHWAB_SECRET_KEY")
    token_path: Path = Field(validation_alias="SCHWAB_TOKEN_PATH", repr=False)

    @field_validator("token_path")
    @classmethod
    def token_path_must_be_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("gateway token path must be absolute")
        return value


def extract_spot_price(payload: Any, symbol: str) -> float:
    """Pull a spot price out of a Schwab quote response.

    This mirrors ``SchwabClientWrapper.get_spot_price`` (``data/schwab_client.py:122-130``)
    exactly, including the ``lastPrice`` -> ``mark`` -> ``closePrice`` preference and the
    unprefixed-symbol fallback, so a gateway spot read and a direct spot read cannot
    disagree about the same payload. ``data/schwab_client.py`` is not modified to share
    this helper; the duplication is pinned by a differential test instead.

    Known gap, deliberately mirrored rather than fixed here: the field preference uses
    ``or``, so a legitimate ``0`` price in every one of ``lastPrice``/``mark``/
    ``closePrice`` is indistinguishable from a missing price and this raises instead of
    returning ``0.0``. The identical gap exists in ``SchwabClientWrapper.get_spot_price``,
    which is the live production spot-price path and out of scope to change. Fixing it
    only here would make the gateway's spot read disagree with the direct path on that one
    payload shape, which is worse than both sharing the same known limitation — so this
    must stay bug-for-bug identical to the direct path until both are fixed together.
    """
    if not isinstance(payload, dict):
        raise ValueError("spot response was not an object")
    quote = payload.get(symbol, payload.get(symbol.lstrip("$"), {}))
    if not isinstance(quote, dict):
        raise ValueError("spot response entry was not an object")
    if "quote" in quote:
        quote = quote["quote"]
    if not isinstance(quote, dict):
        raise ValueError("spot response quote was not an object")
    price = quote.get("lastPrice") or quote.get("mark") or quote.get("closePrice")
    if not price:
        raise ValueError("spot response carried no usable price")
    return float(price)


@contextmanager
def _closing_session(client: Any) -> Iterator[None]:
    """Close the per-transaction HTTP session the client factory opened.

    Each transaction builds its own client, so each one owns a session that would
    otherwise leak. This is the same teardown the credential probe uses.
    """
    try:
        yield
    finally:
        close = getattr(getattr(client, "session", None), "close", None)
        if callable(close):
            close()


class LockedSchwabMarketDataProvider:
    """Read-only Schwab market data through one locked token transaction per call."""

    def __init__(self, adapter: LockedSchwabClientAdapter) -> None:
        self._adapter = adapter

    async def _execute(self, operation: Any) -> Any:
        """Run one synchronous locked transaction without blocking the event loop."""
        return await asyncio.to_thread(self._adapter.execute, operation)

    async def get_spot_price(self, symbol: str = "$SPX") -> float:
        def operation(client: Any) -> Any:
            with _closing_session(client):
                response = client.get_quote(symbol)
                response.raise_for_status()
                return response.json()

        # Parsing runs outside the locked transaction so a malformed payload (a
        # ``ValueError`` from ``extract_spot_price``) surfaces as itself rather than
        # being folded into the adapter's generic ``SchwabClientOperationError`` for a
        # failed fetch. That keeps the two failure modes distinguishable at the gateway
        # boundary the same way ``get_option_chain``/``normalize_schwab_chain_metadata``
        # already are.
        payload = await self._execute(operation)
        return extract_spot_price(payload, symbol)

    async def get_option_chain(
        self, symbol: str, expiration: dt.date
    ) -> dict[str, Any]:
        def operation(client: Any) -> dict[str, Any]:
            with _closing_session(client):
                response = client.get_option_chain(
                    symbol,
                    from_date=expiration,
                    to_date=expiration,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("option chain response was not an object")
                return payload

        return await self._execute(operation)

    async def get_equity_quotes(
        self,
        symbols: list[str],
        *,
        batch_size: int = DEFAULT_QUOTE_BATCH_SIZE,
    ) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}
        if batch_size < 1:
            raise ValueError("quote batch size must be positive")

        def operation(client: Any) -> dict[str, dict[str, Any]]:
            with _closing_session(client):
                fields = [client.Quote.Fields.QUOTE, client.Quote.Fields.EXTENDED]
                results: dict[str, dict[str, Any]] = {}
                for start in range(0, len(symbols), batch_size):
                    response = client.get_quotes(
                        symbols[start : start + batch_size],
                        fields=fields,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if isinstance(payload, dict):
                        results.update(payload)
                return results

        # Every batch shares one transaction, so a multi-batch scanner request takes the
        # token lock once rather than once per batch.
        return await self._execute(operation)


class TokenReadinessRecovery:
    """Re-prime a latched token manager from outside the request path.

    A token-level failure moves the manager out of ``READY``. Every route and ``/ready``
    then refuse with ``gateway_not_ready`` — including the request that would have
    produced the transaction that would make it ready again — so nothing recovers on its
    own. That is correct fail-closed behaviour for a missing, expired, or corrupt token,
    but a lock timeout is transient: another writer simply held the document too long.

    This retries ``load()`` on a fixed interval and only while the manager is not ready,
    so a healthy gateway never touches the token document on this path and a latched one
    cannot spin. A still-failing load leaves the state exactly as it was.
    """

    def __init__(
        self,
        manager: AtomicTokenManager,
        *,
        interval_seconds: float = DEFAULT_READINESS_RECOVERY_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("readiness recovery interval must be positive")
        self._manager = manager
        self._interval_seconds = interval_seconds

    async def attempt_once(self) -> bool:
        """Return True when the manager is ready, recovering it first if it is not."""
        if self._manager.health().state is TokenManagerState.READY:
            return True
        try:
            await asyncio.to_thread(self._manager.load)
        except TokenManagerError:
            # The manager has already recorded its own bounded state; nothing to add.
            log.warning(
                "gateway_readiness_recovery_failed",
                state=self._manager.health().state.value,
            )
            return False
        log.info("gateway_readiness_recovered")
        return True

    async def run_forever(self) -> None:
        """Recover readiness forever, surviving any failure a single attempt can raise.

        ``asyncio.CancelledError`` still propagates so shutdown can cancel this task; any
        other exception from an attempt (including one ``attempt_once`` does not itself
        catch, such as a raise from ``health()``) is caught here so the loop keeps ticking
        instead of dying silently and latching readiness forever.
        """
        while True:
            await asyncio.sleep(self._interval_seconds)
            try:
                await self.attempt_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("gateway_readiness_recovery_attempt_crashed", reason="unexpected_error")
