"""Replaceable read-only quote upstream and Schwab response normalization."""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

from schwab_gateway_sdk.chain_metadata import extract_chain_metadata
from schwab_gateway_sdk.models import (
    ChainMetadataV1,
    HistoryV1,
    MoverIndex,
    MoversV1,
    MoverV1,
    PriceBarV1,
    QuoteV1,
    SessionHistoryV1,
    SpotV1,
)

UTC = dt.timezone.utc
EASTERN = ZoneInfo("America/New_York")
# Regular session is 09:30-16:00 America/New_York; a candle stamped exactly 16:00:00 is
# the first extended (post-market) bar, not the last regular one, so the upper bound is
# exclusive. v1 does not special-case early-close calendar days.
REGULAR_SESSION_START = dt.time(9, 30)
REGULAR_SESSION_END = dt.time(16, 0)


class EquityQuoteProvider(Protocol):
    async def get_equity_quotes(
        self, symbols: list[str], *, batch_size: int = 150
    ) -> dict[str, dict[str, Any]]: ...


class OptionChainProvider(Protocol):
    async def get_option_chain(
        self, symbol: str, expiration: dt.date
    ) -> dict[str, Any]: ...


class SpotPriceProvider(Protocol):
    async def get_spot_price(self, symbol: str = "$SPX") -> float: ...


class PriceHistoryProvider(Protocol):
    async def get_daily_bars(
        self, symbol: str, days_back: int = 10
    ) -> list[dict[str, Any]]: ...

    async def get_intraday_bars(
        self, symbol: str, days_back: int = 1
    ) -> list[dict[str, Any]]: ...


class MarketMoversProvider(Protocol):
    async def get_market_movers(
        self, index: str, *, sort_order: str = "PERCENT_CHANGE_UP"
    ) -> list[dict[str, Any]]: ...


class SessionHistoryProvider(Protocol):
    async def get_session_bars(
        self, symbol: str, date: dt.date
    ) -> list[dict[str, Any]]: ...


class QuoteUpstream(Protocol):
    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]: ...


class SpotUpstream(Protocol):
    async def get_spot(self, symbol: str) -> SpotV1: ...


class ChainMetadataUpstream(Protocol):
    async def get_chain_metadata(
        self, symbol: str, expiration: dt.date
    ) -> ChainMetadataV1: ...


class HistoryUpstream(Protocol):
    async def get_history(
        self, symbol: str, frequency: Literal["daily", "minute"], days_back: int
    ) -> HistoryV1: ...


class MoversUpstream(Protocol):
    async def get_movers(
        self, index: MoverIndex, direction: Literal["up", "down"]
    ) -> MoversV1: ...


class SessionHistoryUpstream(Protocol):
    async def get_session_history(
        self, symbol: str, date: dt.date, session: Literal["regular", "extended"]
    ) -> SessionHistoryV1: ...


class UpstreamUnavailableError(RuntimeError):
    pass


class UpstreamMalformedError(RuntimeError):
    pass


def _number(payload: dict[str, Any], name: str) -> float | None:
    value = payload.get(name)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _integer(payload: dict[str, Any], name: str) -> int | None:
    value = payload.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _event_time(payload: dict[str, Any]) -> dt.datetime | None:
    candidates = [
        value
        for name in ("quoteTime", "tradeTime")
        if (value := _integer(payload, name)) is not None and value > 0
    ]
    if not candidates:
        return None
    return dt.datetime.fromtimestamp(max(candidates) / 1000, tz=UTC)


def normalize_schwab_quote(
    symbol: str,
    payload: dict[str, Any],
    *,
    received_at: dt.datetime,
    stale_after_seconds: float,
) -> QuoteV1:
    regular = payload.get("quote") if isinstance(payload.get("quote"), dict) else {}
    extended = payload.get("extended") if isinstance(payload.get("extended"), dict) else {}
    regular_time = _event_time(regular)
    extended_time = _event_time(extended)
    if extended_time is not None and (regular_time is None or extended_time > regular_time):
        current = extended
        event_timestamp = extended_time
        session = "extended"
    else:
        current = regular
        event_timestamp = regular_time
        session = "regular" if regular else None

    age_seconds = (
        max(0.0, (received_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    flags: list[str] = []
    bid = _number(current, "bidPrice")
    ask = _number(current, "askPrice")
    if bid is None:
        flags.append("missing_bid")
    if ask is None:
        flags.append("missing_ask")
    if bid is not None and ask is not None and bid > ask:
        flags.append("crossed_market")
    if event_timestamp is None:
        flags.append("missing_event_timestamp")

    stale = age_seconds is None or age_seconds > stale_after_seconds
    if stale:
        flags.append("stale")
    return QuoteV1(
        symbol=symbol,
        event_timestamp=event_timestamp,
        gateway_received_at=received_at,
        source="schwab_rest_quote",
        session=session,
        bid=bid,
        ask=ask,
        bid_size=_integer(current, "bidSize"),
        ask_size=_integer(current, "askSize"),
        last=_number(current, "lastPrice"),
        last_size=_integer(current, "lastSize"),
        mark=_number(current, "mark"),
        volume=_integer(current, "totalVolume"),
        close=_number(regular, "closePrice"),
        net_percent_change=_number(regular, "netPercentChange"),
        stale=stale,
        age_seconds=age_seconds,
        data_quality_flags=tuple(flags),
    )


def normalize_schwab_spot(
    symbol: str,
    price: float | None,
    *,
    received_at: dt.datetime,
) -> SpotV1:
    """Wrap a direct spot read.

    ``SpotPriceProvider.get_spot_price`` returns a bare float, so no upstream event time
    survives the direct adapter. Staleness is therefore reported the same way the quote
    normalizer reports an absent event time: unknown age means stale.
    """
    flags: list[str] = []
    if price is None:
        flags.append("missing_price")
    flags.append("missing_event_timestamp")
    flags.append("stale")
    return SpotV1(
        symbol=symbol,
        price=price,
        event_timestamp=None,
        gateway_received_at=received_at,
        source="schwab_rest_spot",
        stale=True,
        age_seconds=None,
        data_quality_flags=tuple(flags),
    )


def normalize_schwab_chain_metadata(
    symbol: str,
    payload: dict[str, Any],
    expiration: dt.date,
    *,
    received_at: dt.datetime,
    stale_after_seconds: float,
) -> ChainMetadataV1:
    fields = extract_chain_metadata(payload, expiration)
    age_seconds = (
        max(0.0, (received_at - fields.event_timestamp).total_seconds())
        if fields.event_timestamp is not None
        else None
    )
    stale = age_seconds is None or age_seconds > stale_after_seconds
    flags = fields.data_quality_flags + (("stale",) if stale else ())
    return ChainMetadataV1(
        symbol=symbol,
        expiration=expiration,
        underlying_price=fields.underlying_price,
        call_contract_count=fields.call_contract_count,
        put_contract_count=fields.put_contract_count,
        strike_count=fields.strike_count,
        event_timestamp=fields.event_timestamp,
        gateway_received_at=received_at,
        source="schwab_rest_chain",
        stale=stale,
        age_seconds=age_seconds,
        data_quality_flags=flags,
    )


SORT_ORDER_BY_DIRECTION: dict[Literal["up", "down"], str] = {
    "up": "PERCENT_CHANGE_UP",
    "down": "PERCENT_CHANGE_DOWN",
}


def _bar_from_candle(candle: Any) -> PriceBarV1 | None:
    if not isinstance(candle, dict):
        return None
    timestamp_ms = _integer(candle, "datetime")
    open_ = _number(candle, "open")
    high = _number(candle, "high")
    low = _number(candle, "low")
    close = _number(candle, "close")
    volume = _integer(candle, "volume")
    if timestamp_ms is None or timestamp_ms <= 0:
        return None
    if None in (open_, high, low, close, volume):
        return None
    try:
        return PriceBarV1(
            timestamp=dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC),
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )
    except ValueError:
        # A field validator rejected this one candle (e.g. negative volume); drop it
        # rather than fail the whole series.
        return None


def normalize_schwab_history(
    symbol: str,
    frequency: Literal["daily", "minute"],
    candles: Any,
    *,
    received_at: dt.datetime,
    stale_after_seconds: float,
    days_back: int | None = None,
) -> HistoryV1:
    if not isinstance(candles, list):
        raise ValueError("price history response was not a list of candles")

    flags: list[str] = []
    bars: list[PriceBarV1] = []
    dropped = 0
    for candle in candles:
        bar = _bar_from_candle(candle)
        if bar is None:
            dropped += 1
            continue
        bars.append(bar)
    if dropped:
        flags.append("malformed_bars_dropped")
    if not bars:
        flags.append("no_bars_returned")
    elif days_back is not None and days_back > 0:
        bars = bars[-days_back:]

    event_timestamp = bars[-1].timestamp if bars else None
    age_seconds = (
        max(0.0, (received_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    stale = age_seconds is None or age_seconds > stale_after_seconds
    if stale:
        flags.append("stale")
    return HistoryV1(
        symbol=symbol,
        frequency=frequency,
        bars=tuple(bars),
        event_timestamp=event_timestamp,
        gateway_received_at=received_at,
        source="schwab_rest_price_history",
        stale=stale,
        age_seconds=age_seconds,
        data_quality_flags=tuple(flags),
    )


def _mover_from_item(item: Any) -> MoverV1 | None:
    if not isinstance(item, dict):
        return None
    symbol = item.get("symbol") or item.get("ticker")
    if not isinstance(symbol, str) or not symbol:
        return None
    change_percent = _number(item, "changePercent")
    if change_percent is None:
        change_percent = _number(item, "netPercentChange")
    change = _number(item, "change")
    if change is None:
        change = _number(item, "netChange")
    last_price = _number(item, "lastPrice")
    if last_price is None:
        last_price = _number(item, "last")
    volume = _integer(item, "totalVolume")
    if volume is None:
        volume = _integer(item, "volume")
    try:
        return MoverV1(
            symbol=symbol,
            last_price=last_price,
            change=change,
            change_percent=change_percent,
            volume=volume,
        )
    except ValueError:
        return None


def normalize_schwab_movers(
    index: MoverIndex,
    direction: Literal["up", "down"],
    items: Any,
    *,
    received_at: dt.datetime,
) -> MoversV1:
    if not isinstance(items, list):
        raise ValueError("movers response was not a list of movers")

    flags: list[str] = []
    movers: list[MoverV1] = []
    dropped = 0
    for item in items:
        mover = _mover_from_item(item)
        if mover is None:
            dropped += 1
            continue
        movers.append(mover)
    if dropped:
        flags.append("malformed_movers_dropped")
    # The movers endpoint carries no per-item or response event time, so age is always
    # unknown here -- the same honest staleness contract ``normalize_schwab_spot`` uses.
    flags.append("missing_event_timestamp")
    flags.append("stale")
    return MoversV1(
        index=index,
        direction=direction,
        movers=tuple(movers),
        event_timestamp=None,
        gateway_received_at=received_at,
        source="schwab_rest_movers",
        stale=True,
        age_seconds=None,
        data_quality_flags=tuple(flags),
    )


class DirectSchwabHistoryUpstream:
    """Normalize a price-history read from the direct adapter inside the gateway boundary."""

    def __init__(
        self,
        provider: PriceHistoryProvider,
        *,
        daily_stale_after_seconds: float = 86400.0,
        minute_stale_after_seconds: float = 900.0,
    ) -> None:
        self._provider = provider
        self._daily_stale_after_seconds = daily_stale_after_seconds
        self._minute_stale_after_seconds = minute_stale_after_seconds

    async def get_history(
        self, symbol: str, frequency: Literal["daily", "minute"], days_back: int
    ) -> HistoryV1:
        try:
            if frequency == "daily":
                candles = await self._provider.get_daily_bars(symbol, days_back)
            else:
                candles = await self._provider.get_intraday_bars(symbol, days_back)
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab price history request failed") from exc
        try:
            return normalize_schwab_history(
                symbol,
                frequency,
                candles,
                received_at=dt.datetime.now(UTC),
                stale_after_seconds=(
                    self._daily_stale_after_seconds
                    if frequency == "daily"
                    else self._minute_stale_after_seconds
                ),
                days_back=days_back,
            )
        except ValueError as exc:
            raise UpstreamMalformedError("Schwab price history response was invalid") from exc


class DirectSchwabMoversUpstream:
    """Normalize a movers read from the direct adapter inside the gateway boundary."""

    def __init__(self, provider: MarketMoversProvider) -> None:
        self._provider = provider

    async def get_movers(
        self, index: MoverIndex, direction: Literal["up", "down"]
    ) -> MoversV1:
        try:
            items = await self._provider.get_market_movers(
                index, sort_order=SORT_ORDER_BY_DIRECTION[direction]
            )
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab movers request failed") from exc
        try:
            return normalize_schwab_movers(
                index, direction, items, received_at=dt.datetime.now(UTC)
            )
        except ValueError as exc:
            raise UpstreamMalformedError("Schwab movers response was invalid") from exc


def _is_regular_session(timestamp: dt.datetime) -> bool:
    local_time = timestamp.astimezone(EASTERN).time()
    return REGULAR_SESSION_START <= local_time < REGULAR_SESSION_END


def normalize_schwab_session_history(
    symbol: str,
    date: dt.date,
    session: Literal["regular", "extended"],
    candles: Any,
    *,
    received_at: dt.datetime,
    stale_after_seconds: float,
) -> SessionHistoryV1:
    """Split one calendar day's candles into its regular or extended segment.

    ``candles`` is expected to span the full day (pre-market through after-hours); the
    split itself -- not Schwab -- decides which bars belong to which session, since
    Schwab's price-history payload carries no per-candle session marker the way its quote
    payload does.
    """
    if not isinstance(candles, list):
        raise ValueError("session history response was not a list of candles")

    flags: list[str] = []
    bars: list[PriceBarV1] = []
    dropped = 0
    for candle in candles:
        bar = _bar_from_candle(candle)
        if bar is None:
            dropped += 1
            continue
        is_regular = _is_regular_session(bar.timestamp)
        if is_regular is (session == "regular"):
            bars.append(bar)
    if dropped:
        flags.append("malformed_bars_dropped")
    if not bars:
        flags.append("no_bars_returned")

    event_timestamp = bars[-1].timestamp if bars else None
    age_seconds = (
        max(0.0, (received_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    stale = age_seconds is None or age_seconds > stale_after_seconds
    if stale:
        flags.append("stale")
    return SessionHistoryV1(
        symbol=symbol,
        date=date,
        session=session,
        candles=tuple(bars),
        event_timestamp=event_timestamp,
        gateway_received_at=received_at,
        source="schwab_rest_session_history",
        stale=stale,
        age_seconds=age_seconds,
        data_quality_flags=tuple(flags),
    )


class DirectSchwabSessionHistoryUpstream:
    """Normalize a point-in-time session read from the direct adapter.

    Distinct from ``DirectSchwabHistoryUpstream``: this fetches one calendar day and
    splits it into the regular or extended segment, rather than a trailing window ending
    now. Both share ``_bar_from_candle``, so a malformed candle is dropped identically.
    """

    def __init__(
        self, provider: SessionHistoryProvider, *, stale_after_seconds: float = 86400.0
    ) -> None:
        self._provider = provider
        self._stale_after_seconds = stale_after_seconds

    async def get_session_history(
        self, symbol: str, date: dt.date, session: Literal["regular", "extended"]
    ) -> SessionHistoryV1:
        try:
            candles = await self._provider.get_session_bars(symbol, date)
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab session history request failed") from exc
        try:
            return normalize_schwab_session_history(
                symbol,
                date,
                session,
                candles,
                received_at=dt.datetime.now(UTC),
                stale_after_seconds=self._stale_after_seconds,
            )
        except ValueError as exc:
            raise UpstreamMalformedError("Schwab session history response was invalid") from exc


class DirectSchwabSpotUpstream:
    """Normalize a spot read from the direct adapter inside the gateway boundary."""

    def __init__(self, provider: SpotPriceProvider) -> None:
        self._provider = provider

    async def get_spot(self, symbol: str) -> SpotV1:
        try:
            price = await self._provider.get_spot_price(symbol)
        except ValueError as exc:
            # Raised by spot-price extraction/parsing (e.g. ``extract_spot_price`` on a
            # malformed Schwab payload), not by the fetch itself.
            raise UpstreamMalformedError("Schwab spot response was invalid") from exc
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab spot request failed") from exc
        try:
            value = float(price)
        except (TypeError, ValueError) as exc:
            raise UpstreamMalformedError("Schwab spot response was not numeric") from exc
        return normalize_schwab_spot(symbol, value, received_at=dt.datetime.now(UTC))


class DirectSchwabChainMetadataUpstream:
    """Summarize a chain read from the direct adapter; contract rows never leave here."""

    def __init__(
        self,
        provider: OptionChainProvider,
        *,
        stale_after_seconds: float = 90.0,
    ) -> None:
        self._provider = provider
        self._stale_after_seconds = stale_after_seconds

    async def get_chain_metadata(
        self, symbol: str, expiration: dt.date
    ) -> ChainMetadataV1:
        try:
            payload = await self._provider.get_option_chain(symbol, expiration)
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab option chain request failed") from exc
        try:
            return normalize_schwab_chain_metadata(
                symbol,
                payload,
                expiration,
                received_at=dt.datetime.now(UTC),
                stale_after_seconds=self._stale_after_seconds,
            )
        except ValueError as exc:
            raise UpstreamMalformedError("Schwab option chain response was invalid") from exc


class DirectSchwabQuoteUpstream:
    """Normalize quotes from the direct adapter inside the gateway boundary."""

    def __init__(
        self,
        provider: EquityQuoteProvider,
        *,
        stale_after_seconds: float = 15.0,
    ) -> None:
        self._provider = provider
        self._stale_after_seconds = stale_after_seconds

    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]:
        try:
            payloads = await self._provider.get_equity_quotes(list(symbols))
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab quote request failed") from exc
        received_at = dt.datetime.now(UTC)
        return tuple(
            normalize_schwab_quote(
                symbol,
                payloads[symbol],
                received_at=received_at,
                stale_after_seconds=self._stale_after_seconds,
            )
            for symbol in symbols
            if symbol in payloads and isinstance(payloads[symbol], dict)
        )
