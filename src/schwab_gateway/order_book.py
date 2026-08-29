"""Normalize venue-specific Schwab Level II book messages for research capture.

The Schwab stream services exposed by schwab-py are snapshots, not a consolidated
national market system book. Raw websocket frames must be preserved separately; the
models produced here are derived, validated research artifacts.
"""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Mapping
from typing import Any, Literal

from schwab_gateway_sdk.models import (
    OrderBookLevelV1,
    OrderBookParticipantV1,
    OrderBookSnapshotV1,
)

UTC = dt.timezone.utc
OrderBookVenue = Literal["NASDAQ", "NYSE"]
BOOK_SERVICE_BY_VENUE: dict[OrderBookVenue, str] = {
    "NASDAQ": "NASDAQ_BOOK",
    "NYSE": "NYSE_BOOK",
}


class OrderBookMalformedError(ValueError):
    """A Schwab book message could not be normalized without inventing data."""


def _required_number(payload: Mapping[str, Any], name: str) -> float:
    try:
        value = float(payload[name])
    except (KeyError, TypeError, ValueError):
        raise OrderBookMalformedError(f"order-book {name} is invalid") from None
    if not math.isfinite(value):
        raise OrderBookMalformedError(f"order-book {name} is invalid")
    return value


def _required_nonnegative_int(payload: Mapping[str, Any], name: str) -> int:
    try:
        raw = payload[name]
        value = int(raw)
    except (KeyError, TypeError, ValueError):
        raise OrderBookMalformedError(f"order-book {name} is invalid") from None
    if isinstance(raw, float) and not raw.is_integer():
        raise OrderBookMalformedError(f"order-book {name} is invalid")
    if value < 0:
        raise OrderBookMalformedError(f"order-book {name} is invalid")
    return value


def _optional_nonnegative_int(payload: Mapping[str, Any], name: str) -> int | None:
    if name not in payload or payload[name] is None:
        return None
    return _required_nonnegative_int(payload, name)


def _event_timestamp(value: Any) -> dt.datetime | None:
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    try:
        return dt.datetime.fromtimestamp(millis / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _participants(
    value: Any,
    *,
    exchange_field: str,
    size_field: str,
) -> tuple[OrderBookParticipantV1, ...]:
    if not isinstance(value, list):
        raise OrderBookMalformedError("order-book participant list is invalid")
    normalized: list[OrderBookParticipantV1] = []
    for participant in value:
        if not isinstance(participant, Mapping):
            raise OrderBookMalformedError("order-book participant is invalid")
        exchange = participant.get(exchange_field)
        if not isinstance(exchange, str) or not exchange.strip():
            raise OrderBookMalformedError("order-book participant exchange is invalid")
        normalized.append(
            OrderBookParticipantV1(
                exchange=exchange,
                size=_required_nonnegative_int(participant, size_field),
                sequence=_optional_nonnegative_int(participant, "SEQUENCE"),
            )
        )
    return tuple(normalized)


def _levels(value: Any, *, side: Literal["bid", "ask"]) -> tuple[OrderBookLevelV1, ...]:
    if not isinstance(value, list):
        raise OrderBookMalformedError(f"order-book {side} levels are invalid")
    prefix = side.upper()
    participant_list_field = f"{prefix}S"
    levels: list[OrderBookLevelV1] = []
    for raw_level in value:
        if not isinstance(raw_level, Mapping):
            raise OrderBookMalformedError(f"order-book {side} level is invalid")
        participants = _participants(
            raw_level.get(participant_list_field),
            exchange_field="EXCHANGE",
            size_field=f"{prefix}_VOLUME",
        )
        price = _required_number(raw_level, f"{prefix}_PRICE")
        if price <= 0:
            raise OrderBookMalformedError(f"order-book {side} price is invalid")
        total_size = _required_nonnegative_int(raw_level, "TOTAL_VOLUME")
        participant_count = _required_nonnegative_int(raw_level, f"NUM_{prefix}S")
        levels.append(
            OrderBookLevelV1(
                price=price,
                total_size=total_size,
                participant_count=participant_count,
                participants=participants,
            )
        )
    return tuple(
        sorted(levels, key=lambda level: level.price, reverse=side == "bid")
    )


def normalize_schwab_book_message(
    message: Any,
    *,
    venue: OrderBookVenue,
    gateway_received_at: dt.datetime,
) -> tuple[OrderBookSnapshotV1, ...]:
    """Normalize one schwab-py labeled book message into validated snapshots.

    The caller must write the original websocket frame before invoking this function.
    Malformed derived content raises rather than silently deleting price levels.
    """

    if gateway_received_at.utcoffset() is None:
        raise ValueError("gateway_received_at must be timezone-aware")
    if venue not in BOOK_SERVICE_BY_VENUE:
        raise ValueError("unsupported order-book venue")
    if not isinstance(message, Mapping):
        raise OrderBookMalformedError("order-book message is not an object")
    expected_service = BOOK_SERVICE_BY_VENUE[venue]
    if message.get("service") != expected_service:
        raise OrderBookMalformedError("order-book service does not match venue")
    content = message.get("content")
    if not isinstance(content, list) or not content:
        raise OrderBookMalformedError("order-book content is empty or invalid")

    snapshots: list[OrderBookSnapshotV1] = []
    for entry in content:
        if not isinstance(entry, Mapping):
            raise OrderBookMalformedError("order-book content entry is invalid")
        symbol = entry.get("key")
        if not isinstance(symbol, str) or not symbol.strip():
            raise OrderBookMalformedError("order-book symbol is invalid")
        sequence = _optional_nonnegative_int(entry, "seq")
        event_timestamp = _event_timestamp(entry.get("BOOK_TIME"))
        bids = _levels(entry.get("BIDS"), side="bid")
        asks = _levels(entry.get("ASKS"), side="ask")

        flags: list[str] = []
        if event_timestamp is None:
            flags.append("missing_book_timestamp")
        if not bids:
            flags.append("empty_bid_book")
        if not asks:
            flags.append("empty_ask_book")
        if any(level.participant_count != len(level.participants) for level in (*bids, *asks)):
            flags.append("participant_count_mismatch")
        if any(
            level.total_size != sum(participant.size for participant in level.participants)
            for level in (*bids, *asks)
        ):
            flags.append("participant_size_mismatch")

        snapshots.append(
            OrderBookSnapshotV1(
                symbol=symbol.strip().upper(),
                venue=venue,
                service=expected_service,
                sequence=sequence,
                event_timestamp=event_timestamp,
                gateway_received_at=gateway_received_at,
                bids=bids,
                asks=asks,
                data_quality_flags=tuple(flags),
            )
        )
    return tuple(snapshots)
