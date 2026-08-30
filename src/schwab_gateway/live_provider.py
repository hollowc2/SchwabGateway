"""Real Schwab market-data reads bound to the single locked token manager.

This is the bridge between the proven token machinery and the gateway's read surfaces. It
exposes only ``SpotPriceProvider``, ``OptionChainProvider``, ``EquityQuoteProvider``,
``PriceHistoryProvider``, ``MarketMoversProvider``, and ``SessionHistoryProvider``; there
is no account, order, transaction, or streaming method to call, so no such request can be
issued through this object.

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
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from prometheus_client import Histogram
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
EASTERN = ZoneInfo("America/New_York")
# Schwab's standard full extended-hours window: pre-market open through after-hours
# close. Wide enough to capture both the regular and extended segments of one calendar
# date in a single fetch; the upstream normalizer does the actual session split.
EXTENDED_SESSION_WINDOW_START = dt.time(4, 0)
EXTENDED_SESSION_WINDOW_END = dt.time(20, 0)

upstream_operation_latency = Histogram(
    "schwab_gateway_upstream_operation_latency_seconds",
    "Live Schwab transaction latency including worker and token-lock wait",
    ["operation", "status"],
)


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


def extract_spot_price_and_timestamp(
    payload: Any, symbol: str
) -> tuple[float, dt.datetime | None]:
    """Pull a spot price and freshest quote/trade timestamp from a Schwab response.

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
    timestamps: list[dt.datetime] = []
    for name in ("quoteTime", "tradeTime", "quoteTimeInLong", "tradeTimeInLong"):
        try:
            millis = int(quote.get(name))
        except (TypeError, ValueError):
            continue
        if millis > 0:
            timestamps.append(dt.datetime.fromtimestamp(millis / 1000, tz=dt.timezone.utc))
    return float(price), max(timestamps) if timestamps else None


def extract_spot_price(payload: Any, symbol: str) -> float:
    """Pull a spot price out of a Schwab quote response.

    Kept as the direct-wrapper parity helper; the gateway's timestamped spot path calls
    ``extract_spot_price_and_timestamp`` so freshness is not discarded.
    """
    price, _timestamp = extract_spot_price_and_timestamp(payload, symbol)
    return price


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
        self._worker_active = False
        self._worker_lease_guard = threading.Lock()

    async def _acquire_worker_lease(self) -> None:
        while True:
            with self._worker_lease_guard:
                if not self._worker_active:
                    self._worker_active = True
                    return
            await asyncio.sleep(0.001)

    def _release_worker_lease(self) -> None:
        with self._worker_lease_guard:
            self._worker_active = False

    async def _execute(self, operation_name: str, operation: Any) -> Any:
        """Run one synchronous locked transaction behind a one-worker lease.

        Response cancellation propagates immediately so the API timeout can return 504.
        The detached daemon worker retains the lease until its synchronous token
        transaction finishes. Requests that arrive meanwhile wait on the lease and can
        time out without spawning more blocked threads.
        """
        started = time.perf_counter()
        status = "error"
        try:
            await self._acquire_worker_lease()
            loop = asyncio.get_running_loop()
            completion: asyncio.Future[Any] = loop.create_future()

            def deliver_result(result: Any = None, error: BaseException | None = None) -> None:
                if not completion.done():
                    if error is None:
                        completion.set_result(result)
                    else:
                        completion.set_exception(error)

            def consume_unobserved_result(future: asyncio.Future[Any]) -> None:
                if future.cancelled():
                    return
                try:
                    future.exception()
                except Exception:
                    pass

            completion.add_done_callback(consume_unobserved_result)

            def execute_and_signal() -> None:
                worker_error: BaseException | None = None
                try:
                    result = self._adapter.execute(operation)
                except BaseException as error:
                    result = None
                    worker_error = error
                self._release_worker_lease()
                loop.call_soon_threadsafe(deliver_result, result, worker_error)

            worker = threading.Thread(
                target=execute_and_signal,
                name="schwab-gateway-read",
                daemon=True,
            )
            try:
                worker.start()
            except Exception:
                self._release_worker_lease()
                raise
            result = await asyncio.shield(completion)
            status = "success"
            return result
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        finally:
            upstream_operation_latency.labels(
                operation=operation_name,
                status=status,
            ).observe(time.perf_counter() - started)

    async def get_spot_price(self, symbol: str = "$SPX") -> float:
        price, _timestamp = await self.get_spot_snapshot(symbol)
        return price

    async def get_spot_snapshot(
        self, symbol: str = "$SPX"
    ) -> tuple[float, dt.datetime | None]:
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
        payload = await self._execute("spot", operation)
        return extract_spot_price_and_timestamp(payload, symbol)

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

        return await self._execute("option_chain", operation)

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
        return await self._execute("quotes", operation)

    async def get_daily_bars(self, symbol: str, days_back: int = 10) -> list[dict[str, Any]]:
        """Fetch daily OHLCV bars.

        Mirrors ``SchwabClientWrapper.get_daily_bars``: a fixed ``period_type=MONTH,
        period=1`` request, the same shape Schwab has always been asked for here. This
        adapter's client is constructed with ``enforce_enums=True`` (unlike the direct
        wrapper's ``enforce_enums=False``), so a real ``Period`` enum member is passed
        rather than a raw int. ``days_back`` does not change the Schwab request -- it
        bounds the response after normalization, the same way it does not change the
        direct wrapper's request either.
        """

        def operation(client: Any) -> list[dict[str, Any]]:
            with _closing_session(client):
                response = client.get_price_history(
                    symbol,
                    period_type=client.PriceHistory.PeriodType.MONTH,
                    period=client.PriceHistory.Period.ONE_MONTH,
                    frequency_type=client.PriceHistory.FrequencyType.DAILY,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("price history response was not an object")
                candles = payload.get("candles")
                if not isinstance(candles, list):
                    raise ValueError("price history response carried no candle list")
                return candles

        return await self._execute("daily_history", operation)

    async def get_intraday_bars(self, symbol: str, days_back: int = 1) -> list[dict[str, Any]]:
        """Fetch per-minute OHLCV bars for the trailing ``days_back`` days.

        Unlike ``get_daily_bars``, ``period`` is not passed at all: schwab-py's
        ``Period`` enum only defines a handful of fixed day counts (1, 2, 3, 4, 5, 10),
        and ``period`` is documented as unnecessary when ``start_datetime``/
        ``end_datetime`` are supplied, so an explicit date window is used instead. That
        keeps an arbitrary, gateway-bounded ``days_back`` compatible with this adapter's
        ``enforce_enums=True`` client without guessing at enum coverage.
        """

        def operation(client: Any) -> list[dict[str, Any]]:
            with _closing_session(client):
                today = dt.date.today()
                start = today - dt.timedelta(days=days_back)
                response = client.get_price_history(
                    symbol,
                    period_type=client.PriceHistory.PeriodType.DAY,
                    frequency_type=client.PriceHistory.FrequencyType.MINUTE,
                    frequency=client.PriceHistory.Frequency.EVERY_MINUTE,
                    start_datetime=dt.datetime.combine(start, dt.time.min),
                    end_datetime=dt.datetime.combine(today, dt.time.max),
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("price history response was not an object")
                candles = payload.get("candles")
                if not isinstance(candles, list):
                    raise ValueError("price history response carried no candle list")
                return candles

        return await self._execute("minute_history", operation)

    async def get_market_movers(
        self, index: str, *, sort_order: str = "PERCENT_CHANGE_UP"
    ) -> list[dict[str, Any]]:
        """Return Schwab's top-movers list for one index/exchange bucket.

        Mirrors ``SchwabClientWrapper.get_market_movers``, with one adjustment for this
        adapter's ``enforce_enums=True`` client: ``index`` is converted to the real
        ``Movers.Index`` enum member by value rather than passed as a raw string.
        """

        def operation(client: Any) -> list[dict[str, Any]]:
            with _closing_session(client):
                movers_index = client.Movers.Index(index)
                sort = getattr(client.Movers.SortOrder, sort_order, sort_order)
                response = client.get_movers(movers_index, sort_order=sort)
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, list):
                    return payload
                if isinstance(payload, dict):
                    return payload.get("screeners", payload.get("movers", []))
                raise ValueError("movers response was neither a list nor an object")

        return await self._execute("movers", operation)

    async def get_session_bars(self, symbol: str, date: dt.date) -> list[dict[str, Any]]:
        """Fetch one calendar day's minute bars, spanning pre-market through after-hours.

        ``/v1/session-history`` needs both the regular and extended segments of a single
        date in one fetch so the normalizer can split them. Unlike ``get_intraday_bars``,
        the request window is timezone-aware (``America/New_York``) rather than naive,
        because getting the calendar-date boundary right is the entire point here.
        ``period`` is omitted, same as ``get_intraday_bars``: it is unnecessary once
        ``start_datetime``/``end_datetime`` are given, and schwab-py's ``Period`` enum
        only covers a handful of fixed day counts anyway.
        """

        def operation(client: Any) -> list[dict[str, Any]]:
            with _closing_session(client):
                start = dt.datetime.combine(date, EXTENDED_SESSION_WINDOW_START, tzinfo=EASTERN)
                end = dt.datetime.combine(date, EXTENDED_SESSION_WINDOW_END, tzinfo=EASTERN)
                response = client.get_price_history(
                    symbol,
                    period_type=client.PriceHistory.PeriodType.DAY,
                    frequency_type=client.PriceHistory.FrequencyType.MINUTE,
                    frequency=client.PriceHistory.Frequency.EVERY_MINUTE,
                    start_datetime=start,
                    end_datetime=end,
                    need_extended_hours_data=True,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("price history response was not an object")
                candles = payload.get("candles")
                if not isinstance(candles, list):
                    raise ValueError("price history response carried no candle list")
                return candles

        return await self._execute("session_history", operation)


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
