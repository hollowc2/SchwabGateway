"""Replaceable read-only quote upstream and Schwab response normalization."""

from __future__ import annotations

import asyncio
import datetime as dt
import math
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

from prometheus_client import Counter, Gauge, Histogram
from schwab_gateway_sdk.chain_metadata import extract_chain_metadata
from schwab_gateway_sdk.models import (
    MAX_OPTION_CHAIN_CONTRACTS_V1,
    ChainMetadataV1,
    HistoryV1,
    MoverIndex,
    MoversV1,
    MoverV1,
    OptionChainV1,
    OptionContractV1,
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
DEFAULT_OPTION_CHAIN_CACHE_TTL_SECONDS = 4.0
MAX_OPTION_CHAIN_CACHE_TTL_SECONDS = 4.0
DEFAULT_OPTION_CHAIN_CACHE_MAX_ENTRIES = 16
MAX_OPTION_CHAIN_CACHE_ENTRIES = 16
MAX_OPTION_CHAIN_CACHE_BYTES = 64 * 1024 * 1024
DEFAULT_OPTION_CHAIN_MAX_INFLIGHT = 4
MAX_OPTION_CHAIN_MAX_INFLIGHT = 16

option_chain_cache_events = Counter(
    "gateway_option_chain_cache_events_total",
    "Bounded normalized option-chain cache decisions",
    ["outcome"],
)
option_chain_cache_age_seconds = Histogram(
    "gateway_option_chain_cache_age_seconds",
    "Age of a normalized option chain when served from the bounded cache",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 2.5, 3, 4),
)
option_chain_cache_entries = Gauge(
    "gateway_option_chain_cache_entries",
    "Number of normalized option chains retained in the bounded cache",
)
option_chain_cache_bytes = Gauge(
    "gateway_option_chain_cache_bytes",
    "Serialized bytes retained in the bounded normalized option-chain cache",
)
option_chain_inflight = Gauge(
    "gateway_option_chain_inflight",
    "Number of distinct option-chain upstream fetches currently in flight",
)
option_chain_negative_time_value_normalizations = Counter(
    "gateway_option_chain_negative_time_value_normalizations_total",
    "Schwab option contracts whose negative timeValue was normalized to null",
)


@dataclass(frozen=True, slots=True)
class _CachedOptionChain:
    created_at: float
    expires_at: float
    payload: bytes


def _observed_holiday(date: dt.date) -> dt.date:
    if date.weekday() == 5:
        return date - dt.timedelta(days=1)
    if date.weekday() == 6:
        return date + dt.timedelta(days=1)
    return date


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> dt.date:
    first = dt.date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + dt.timedelta(days=offset + (occurrence - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    next_month = (
        dt.date(year + 1, 1, 1)
        if month == 12
        else dt.date(year, month + 1, 1)
    )
    last = next_month - dt.timedelta(days=1)
    return last - dt.timedelta(days=(last.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> dt.date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    line = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * line) // 451
    month = (h + line - 7 * m + 114) // 31
    day = (h + line - 7 * m + 114) % 31 + 1
    return dt.date(year, month, day)


def _market_holidays(year: int) -> frozenset[dt.date]:
    holidays = {
        _observed_holiday(dt.date(year, 1, 1)),
        _observed_holiday(dt.date(year, 7, 4)),
        _observed_holiday(dt.date(year, 12, 25)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _last_weekday(year, 5, 0),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _easter_sunday(year) - dt.timedelta(days=2),
    }
    if year >= 2022:
        holidays.add(_observed_holiday(dt.date(year, 6, 19)))
    observed_next_new_year = _observed_holiday(dt.date(year + 1, 1, 1))
    if observed_next_new_year.year == year:
        holidays.add(observed_next_new_year)
    return frozenset(holidays)


def _is_trading_day(date: dt.date) -> bool:
    return date.weekday() < 5 and date not in _market_holidays(date.year)


def _latest_completed_session(at: dt.datetime) -> dt.date:
    eastern = at.astimezone(EASTERN)
    candidate = eastern.date()
    if not _is_trading_day(candidate) or eastern.time() < REGULAR_SESSION_END:
        candidate -= dt.timedelta(days=1)
    while not _is_trading_day(candidate):
        candidate -= dt.timedelta(days=1)
    return candidate


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


class OptionChainUpstream(Protocol):
    async def get_option_chain(
        self, symbol: str, expiration: dt.date
    ) -> OptionChainV1: ...


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


def _optional_analytic_number(payload: dict[str, Any], name: str) -> float | None:
    """Normalize Schwab's -999 missing-value sentinel for optional analytics."""
    value = _number(payload, name)
    return None if value == -999.0 else value


def _optional_time_value(payload: dict[str, Any]) -> float | None:
    """Normalize Schwab's nonphysical negative optional time-value analytic.

    Schwab can emit negative ``timeValue`` values on otherwise valid contracts. The
    executable market fields remain bid/ask/mark; this derived analytic is nullable in
    the v1 contract, so an invalid negative value is represented as unavailable rather
    than changing any price or rejecting the complete chain.
    """
    value = _optional_analytic_number(payload, "timeValue")
    if value is not None and value < 0:
        option_chain_negative_time_value_normalizations.inc()
        return None
    return value


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
        for name in ("quoteTime", "tradeTime", "quoteTimeInLong", "tradeTimeInLong")
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
    event_timestamp: dt.datetime | None = None,
    stale_after_seconds: float = 15.0,
) -> SpotV1:
    """Wrap a spot read while preserving honest upstream freshness when available."""
    flags: list[str] = []
    if price is None:
        flags.append("missing_price")
    age_seconds = (
        max(0.0, (received_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    if event_timestamp is None:
        flags.append("missing_event_timestamp")
    stale = age_seconds is None or age_seconds > stale_after_seconds
    if stale:
        flags.append("stale")
    return SpotV1(
        symbol=symbol,
        price=price,
        event_timestamp=event_timestamp,
        gateway_received_at=received_at,
        source="schwab_rest_spot",
        stale=stale,
        age_seconds=age_seconds,
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


def normalize_schwab_option_chain(
    symbol: str,
    payload: dict[str, Any],
    expiration: dt.date,
    *,
    received_at: dt.datetime,
    stale_after_seconds: float,
) -> OptionChainV1:
    """Normalize every contract for exactly one expiration without truncation.

    A one-symbol, one-expiration request bounds the Schwab read. The wire response is
    additionally capped at ``MAX_OPTION_CHAIN_CONTRACTS_V1``; exceeding the cap raises
    instead of returning a partial strike set, which would be unsafe for strategy
    selection and position valuation.
    """
    fields = extract_chain_metadata(payload, expiration)
    contracts: list[OptionContractV1] = []
    expiration_text = str(expiration)

    for option_type, map_key in (
        ("CALL", "callExpDateMap"),
        ("PUT", "putExpDateMap"),
    ):
        exp_map = payload.get(map_key, {})
        if exp_map is None:
            exp_map = {}
        if not isinstance(exp_map, dict):
            raise ValueError(f"{map_key} was not an object")
        for exp_key, strike_map in exp_map.items():
            if not isinstance(exp_key, str):
                raise ValueError("option-chain expiration key was not a string")
            if expiration_text not in exp_key:
                continue
            if not isinstance(strike_map, dict):
                raise ValueError("option-chain strike map was not an object")
            for strike_text, options in strike_map.items():
                try:
                    strike = float(strike_text)
                except (TypeError, ValueError) as exc:
                    raise ValueError("option-chain strike was not numeric") from exc
                if not isinstance(options, list):
                    raise ValueError("option-chain contracts were not a list")
                for option in options:
                    if not isinstance(option, dict):
                        raise ValueError("option-chain contract was not an object")
                    if len(contracts) >= MAX_OPTION_CHAIN_CONTRACTS_V1:
                        raise ValueError(
                            "option chain exceeded the maximum contract count"
                        )
                    event_timestamp = _event_time(option)
                    age_seconds = (
                        max(0.0, (received_at - event_timestamp).total_seconds())
                        if event_timestamp is not None
                        else None
                    )
                    contract_flags: list[str] = []
                    if event_timestamp is None:
                        contract_flags.append("missing_event_timestamp")
                    contract_stale = (
                        age_seconds is None or age_seconds > stale_after_seconds
                    )
                    if contract_stale:
                        contract_flags.append("stale")
                    contracts.append(
                        OptionContractV1(
                            symbol=option.get("symbol"),
                            option_type=option_type,
                            expiration=expiration,
                            strike=strike,
                            bid=_number(option, "bid"),
                            ask=_number(option, "ask"),
                            mark=_number(option, "mark"),
                            last=_number(option, "last"),
                            total_volume=_integer(option, "totalVolume"),
                            open_interest=_integer(option, "openInterest"),
                            volatility=_optional_analytic_number(option, "volatility"),
                            delta=_optional_analytic_number(option, "delta"),
                            gamma=_optional_analytic_number(option, "gamma"),
                            theta=_optional_analytic_number(option, "theta"),
                            vega=_optional_analytic_number(option, "vega"),
                            bid_size=_integer(option, "bidSize"),
                            ask_size=_integer(option, "askSize"),
                            rho=_optional_analytic_number(option, "rho"),
                            intrinsic_value=_optional_analytic_number(
                                option, "intrinsicValue"
                            ),
                            time_value=_optional_time_value(option),
                            in_the_money=option.get("inTheMoney"),
                            days_to_expiration=_integer(option, "daysToExpiration"),
                            multiplier=_number(option, "multiplier"),
                            theoretical_option_value=_optional_analytic_number(
                                option, "theoreticalOptionValue"
                            ),
                            event_timestamp=event_timestamp,
                            stale=contract_stale,
                            age_seconds=age_seconds,
                            data_quality_flags=tuple(contract_flags),
                        )
                    )

    contract_timestamps = [
        contract.event_timestamp
        for contract in contracts
        if contract.event_timestamp is not None
    ]
    all_contracts_timestamped = bool(contracts) and len(contract_timestamps) == len(contracts)
    event_timestamp = max(contract_timestamps) if contract_timestamps else None
    age_seconds = (
        max(0.0, (received_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    stale_contract_count = sum(contract.stale for contract in contracts)
    # The aggregate describes the freshest delivered snapshot, while each row retains
    # its own freshness. Consumers validate counts first, then omit stale/unknown rows;
    # one stale deep contract must not invalidate otherwise fresh strikes.
    stale = not contracts or stale_contract_count == len(contracts)
    flags = [
        flag for flag in fields.data_quality_flags if flag != "missing_event_timestamp"
    ]
    if not all_contracts_timestamped:
        flags.append("missing_contract_event_timestamp")
    if stale_contract_count and not stale:
        flags.append("stale_contracts_present")
    if stale:
        flags.append("stale")
    call_contract_count = sum(
        contract.option_type == "CALL" for contract in contracts
    )
    put_contract_count = sum(contract.option_type == "PUT" for contract in contracts)
    return OptionChainV1(
        symbol=symbol,
        expiration=expiration,
        underlying_price=fields.underlying_price,
        call_contract_count=call_contract_count,
        put_contract_count=put_contract_count,
        strike_count=len({contract.strike for contract in contracts}),
        contracts=tuple(contracts),
        event_timestamp=event_timestamp,
        gateway_received_at=received_at,
        source="schwab_rest_option_chain",
        stale=stale,
        age_seconds=age_seconds,
        data_quality_flags=tuple(flags),
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
        raise ValueError("price history contained malformed candles")
    if not bars:
        flags.append("no_bars_returned")
    elif days_back is not None and days_back > 0:
        if frequency == "daily":
            bars = bars[-days_back:]
        else:
            # ``days_back`` means calendar days for minute history, not candles. The
            # provider deliberately fetches a slightly wider explicit window; normalize
            # it to the newest available date plus the preceding N-1 Eastern calendar
            # dates here. Exact point-in-time reads belong on ``/v1/session-history``.
            current_date = received_at.astimezone(EASTERN).date()
            available_dates = {
                bar.timestamp.astimezone(EASTERN).date()
                for bar in bars
                if bar.timestamp.astimezone(EASTERN).date() <= current_date
            }
            # Between midnight and the first bar of a new Eastern session, Schwab's
            # explicit trailing window legitimately ends on the prior session. Anchor
            # the bounded response to the newest date actually returned so a calendar
            # rollover cannot discard every valid bar. Once today's first bar exists,
            # the anchor naturally advances to today. Freshness remains honest below:
            # prior-session minute history is still marked stale overnight.
            anchor_date = max(available_dates, default=current_date)
            first_date = anchor_date - dt.timedelta(days=days_back - 1)
            bars = [
                bar
                for bar in bars
                if first_date
                <= bar.timestamp.astimezone(EASTERN).date()
                <= anchor_date
            ]

    event_timestamp = bars[-1].timestamp if bars else None
    age_seconds = (
        max(0.0, (received_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    if frequency == "daily" and event_timestamp is not None:
        event_session = event_timestamp.astimezone(EASTERN).date()
        stale = event_session < _latest_completed_session(received_at)
    else:
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
            timestamped_reader = getattr(self._provider, "get_spot_snapshot", None)
            if callable(timestamped_reader):
                price, event_timestamp = await timestamped_reader(symbol)
            else:
                price = await self._provider.get_spot_price(symbol)
                event_timestamp = None
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
        return normalize_schwab_spot(
            symbol,
            value,
            received_at=dt.datetime.now(UTC),
            event_timestamp=event_timestamp,
        )


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


def _reevaluate_option_chain_freshness(
    chain: OptionChainV1,
    *,
    evaluated_at: dt.datetime,
    stale_after_seconds: float,
) -> OptionChainV1:
    contracts: list[OptionContractV1] = []
    for contract in chain.contracts:
        age_seconds = (
            max(0.0, (evaluated_at - contract.event_timestamp).total_seconds())
            if contract.event_timestamp is not None
            else None
        )
        stale = age_seconds is None or age_seconds > stale_after_seconds
        flags = [flag for flag in contract.data_quality_flags if flag != "stale"]
        if stale:
            flags.append("stale")
        contracts.append(
            contract.model_copy(
                update={
                    "age_seconds": age_seconds,
                    "stale": stale,
                    "data_quality_flags": tuple(flags),
                }
            )
        )

    event_timestamp = chain.event_timestamp
    age_seconds = (
        max(0.0, (evaluated_at - event_timestamp).total_seconds())
        if event_timestamp is not None
        else None
    )
    stale_count = sum(contract.stale for contract in contracts)
    stale = not contracts or stale_count == len(contracts)
    flags = [
        flag
        for flag in chain.data_quality_flags
        if flag not in {"stale", "stale_contracts_present"}
    ]
    if stale_count and not stale:
        flags.append("stale_contracts_present")
    if stale:
        flags.append("stale")
    return chain.model_copy(
        update={
            "contracts": tuple(contracts),
            "age_seconds": age_seconds,
            "stale": stale,
            "data_quality_flags": tuple(flags),
        }
    )


class DirectSchwabOptionChainUpstream:
    """Normalize, coalesce, and briefly cache complete one-expiration chains."""

    def __init__(
        self,
        provider: OptionChainProvider,
        *,
        stale_after_seconds: float = 90.0,
        cache_ttl_seconds: float = DEFAULT_OPTION_CHAIN_CACHE_TTL_SECONDS,
        cache_max_entries: int = DEFAULT_OPTION_CHAIN_CACHE_MAX_ENTRIES,
        cache_max_bytes: int = MAX_OPTION_CHAIN_CACHE_BYTES,
        max_inflight: int = DEFAULT_OPTION_CHAIN_MAX_INFLIGHT,
        monotonic_clock: Callable[[], float] = time.monotonic,
        utcnow: Callable[[], dt.datetime] | None = None,
    ) -> None:
        if (
            not math.isfinite(cache_ttl_seconds)
            or not 0 < cache_ttl_seconds <= MAX_OPTION_CHAIN_CACHE_TTL_SECONDS
        ):
            raise ValueError("option-chain cache TTL must be greater than 0 and at most 4s")
        if not 1 <= cache_max_entries <= MAX_OPTION_CHAIN_CACHE_ENTRIES:
            raise ValueError("option-chain cache capacity must be between 1 and 16")
        if not 1 <= cache_max_bytes <= MAX_OPTION_CHAIN_CACHE_BYTES:
            raise ValueError("option-chain cache byte capacity must be between 1 and 64 MiB")
        if not 1 <= max_inflight <= MAX_OPTION_CHAIN_MAX_INFLIGHT:
            raise ValueError("option-chain in-flight capacity must be between 1 and 16")
        self._provider = provider
        self._stale_after_seconds = stale_after_seconds
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache_max_entries = cache_max_entries
        self._cache_max_bytes = cache_max_bytes
        self._max_inflight = max_inflight
        self._cache_bytes = 0
        self._monotonic_clock = monotonic_clock
        self._utcnow = utcnow or (lambda: dt.datetime.now(UTC))
        self._cache: OrderedDict[tuple[str, dt.date], _CachedOptionChain] = OrderedDict()
        self._inflight: dict[tuple[str, dt.date], asyncio.Task[OptionChainV1]] = {}

    async def get_option_chain(
        self, symbol: str, expiration: dt.date
    ) -> OptionChainV1:
        key = (symbol, expiration)
        now = self._monotonic_clock()
        self._prune_expired(now)
        cached = self._cache.get(key)
        if cached is not None:
            option_chain_cache_events.labels(outcome="hit").inc()
            option_chain_cache_age_seconds.observe(max(0.0, now - cached.created_at))
            self._cache.move_to_end(key)
            return _reevaluate_option_chain_freshness(
                OptionChainV1.model_validate_json(cached.payload),
                evaluated_at=self._utcnow(),
                stale_after_seconds=self._stale_after_seconds,
            )

        fetch = self._inflight.get(key)
        if fetch is not None:
            option_chain_cache_events.labels(outcome="coalesced").inc()
            return await asyncio.shield(fetch)

        if len(self._inflight) >= self._max_inflight:
            option_chain_cache_events.labels(outcome="inflight_rejected").inc()
            raise UpstreamUnavailableError("option-chain in-flight capacity is unavailable")

        option_chain_cache_events.labels(outcome="miss").inc()
        option_chain_cache_events.labels(outcome="upstream").inc()
        fetch = asyncio.create_task(self._fetch_option_chain(symbol, expiration, key))
        self._inflight[key] = fetch
        option_chain_inflight.set(len(self._inflight))
        fetch.add_done_callback(
            lambda completed, cache_key=key: self._finish_fetch(cache_key, completed)
        )
        return await asyncio.shield(fetch)

    async def _fetch_option_chain(
        self,
        symbol: str,
        expiration: dt.date,
        key: tuple[str, dt.date],
    ) -> OptionChainV1:
        try:
            payload = await self._provider.get_option_chain(symbol, expiration)
        except Exception as exc:
            raise UpstreamUnavailableError("Schwab option chain request failed") from exc
        try:
            chain = normalize_schwab_option_chain(
                symbol,
                payload,
                expiration,
                received_at=self._utcnow(),
                stale_after_seconds=self._stale_after_seconds,
            )
        except ValueError as exc:
            raise UpstreamMalformedError("Schwab option chain response was invalid") from exc
        now = self._monotonic_clock()
        payload_bytes = chain.model_dump_json().encode()
        if len(payload_bytes) <= self._cache_max_bytes:
            previous = self._cache.pop(key, None)
            if previous is not None:
                self._cache_bytes -= len(previous.payload)
            self._cache[key] = _CachedOptionChain(
                created_at=now,
                expires_at=now + self._cache_ttl_seconds,
                payload=payload_bytes,
            )
            self._cache_bytes += len(payload_bytes)
            self._evict_to_bounds()
            self._update_cache_gauges()
        return chain

    def _prune_expired(self, now: float) -> None:
        expired = [key for key, value in self._cache.items() if value.expires_at <= now]
        for key in expired:
            value = self._cache.pop(key)
            self._cache_bytes -= len(value.payload)
            option_chain_cache_events.labels(outcome="eviction").inc()
        self._update_cache_gauges()

    def _evict_to_bounds(self) -> None:
        while (
            len(self._cache) > self._cache_max_entries
            or self._cache_bytes > self._cache_max_bytes
        ):
            _, value = self._cache.popitem(last=False)
            self._cache_bytes -= len(value.payload)
            option_chain_cache_events.labels(outcome="eviction").inc()

    def _update_cache_gauges(self) -> None:
        option_chain_cache_entries.set(len(self._cache))
        option_chain_cache_bytes.set(self._cache_bytes)

    def _finish_fetch(
        self,
        key: tuple[str, dt.date],
        completed: asyncio.Task[OptionChainV1],
    ) -> None:
        if self._inflight.get(key) is completed:
            del self._inflight[key]
            option_chain_inflight.set(len(self._inflight))
        if completed.cancelled():
            return
        try:
            completed.exception()
        except Exception:
            pass


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
