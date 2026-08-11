"""Replaceable read-only quote upstream and Schwab response normalization."""

from __future__ import annotations

import datetime as dt
from typing import Any, Protocol

from schwab_gateway_sdk.chain_metadata import extract_chain_metadata
from schwab_gateway_sdk.models import ChainMetadataV1, QuoteV1, SpotV1

UTC = dt.timezone.utc


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


class QuoteUpstream(Protocol):
    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]: ...


class SpotUpstream(Protocol):
    async def get_spot(self, symbol: str) -> SpotV1: ...


class ChainMetadataUpstream(Protocol):
    async def get_chain_metadata(
        self, symbol: str, expiration: dt.date
    ) -> ChainMetadataV1: ...


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
