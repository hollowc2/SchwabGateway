"""Versioned, transport-neutral gateway API models."""

from __future__ import annotations

import datetime as dt
import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

MAX_OPTION_CHAIN_CONTRACTS_V1 = 5000


class GatewayModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class QuoteV1(GatewayModel):
    symbol: str
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    session: str | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    last: float | None = None
    last_size: int | None = None
    mark: float | None = None
    volume: int | None = None
    close: float | None = None
    net_percent_change: float | None = None
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("age_seconds must be nonnegative")
        return value


class QuoteResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    quotes: tuple[QuoteV1, ...]


class SpotV1(GatewayModel):
    symbol: str
    price: float | None = None
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("age_seconds must be nonnegative")
        return value


class SpotResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    spot: SpotV1


class ChainMetadataV1(GatewayModel):
    """Bounded, fixed-shape option-chain summary. Never carries contract rows."""

    symbol: str
    expiration: dt.date
    underlying_price: float | None = None
    call_contract_count: int
    put_contract_count: int
    strike_count: int
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("age_seconds must be nonnegative")
        return value

    @field_validator("call_contract_count", "put_contract_count", "strike_count")
    @classmethod
    def counts_must_be_nonnegative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("chain metadata counts must be nonnegative")
        return value


class ChainMetadataResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    chain: ChainMetadataV1


class OptionContractV1(GatewayModel):
    """One normalized Schwab option contract for strategy and valuation consumers."""

    symbol: str
    option_type: Literal["CALL", "PUT"]
    expiration: dt.date
    strike: float
    bid: float
    ask: float
    mark: float
    last: float | None = None
    total_volume: int | None = None
    open_interest: int | None = None
    volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    rho: float | None = None
    intrinsic_value: float | None = None
    time_value: float | None = None
    in_the_money: bool | None = None
    days_to_expiration: int | None = None
    multiplier: float | None = None
    theoretical_option_value: float | None = None
    event_timestamp: dt.datetime | None = None
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("symbol")
    @classmethod
    def symbol_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("option contract symbol must not be empty")
        return value

    @field_validator(
        "total_volume",
        "open_interest",
        "bid_size",
        "ask_size",
        "days_to_expiration",
    )
    @classmethod
    def counts_must_be_nonnegative(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("option contract counts must be nonnegative")
        return value

    @field_validator("strike")
    @classmethod
    def strike_must_be_finite_and_positive(cls, value: float) -> float:
        if not math.isfinite(value) or value <= 0:
            raise ValueError("option contract strike must be finite and positive")
        return value

    @field_validator("bid", "ask", "mark", "last")
    @classmethod
    def prices_must_be_finite_and_nonnegative(
        cls, value: float | None
    ) -> float | None:
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError("option contract prices must be finite and nonnegative")
        return value

    @field_validator(
        "volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "intrinsic_value",
        "multiplier",
        "theoretical_option_value",
    )
    @classmethod
    def numeric_fields_must_be_finite(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("option contract numeric fields must be finite")
        return value

    @field_validator("time_value")
    @classmethod
    def time_value_must_be_finite_and_nonnegative(
        cls, value: float | None
    ) -> float | None:
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError("option contract time value must be finite and nonnegative")
        return value

    @field_validator("event_timestamp")
    @classmethod
    def timestamp_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_finite_and_nonnegative(
        cls, value: float | None
    ) -> float | None:
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError("age_seconds must be finite and nonnegative")
        return value

    @model_validator(mode="after")
    def bid_must_not_exceed_ask(self) -> OptionContractV1:
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("option contract bid must not exceed ask")
        return self


class OptionChainV1(GatewayModel):
    """A complete normalized chain for one symbol and one expiration.

    The response is hard-bounded at ``MAX_OPTION_CHAIN_CONTRACTS_V1``. The gateway
    refuses an oversized upstream payload instead of silently truncating contracts,
    because a partial strike set is unsafe for strategy selection and position marks.
    """

    symbol: str
    expiration: dt.date
    underlying_price: float | None = None
    call_contract_count: int
    put_contract_count: int
    strike_count: int
    contracts: tuple[OptionContractV1, ...]
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("contracts")
    @classmethod
    def contracts_must_be_bounded(
        cls, value: tuple[OptionContractV1, ...]
    ) -> tuple[OptionContractV1, ...]:
        if not value:
            raise ValueError("option chain must contain contracts")
        if len(value) > MAX_OPTION_CHAIN_CONTRACTS_V1:
            raise ValueError(
                f"option chain must contain at most {MAX_OPTION_CHAIN_CONTRACTS_V1} contracts"
            )
        sides = {contract.option_type for contract in value}
        if sides != {"CALL", "PUT"}:
            raise ValueError("option chain must contain both call and put contracts")
        return value

    @field_validator("call_contract_count", "put_contract_count", "strike_count")
    @classmethod
    def counts_must_be_nonnegative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("option chain counts must be nonnegative")
        return value

    @field_validator("underlying_price")
    @classmethod
    def underlying_price_must_be_finite_and_positive(
        cls, value: float | None
    ) -> float | None:
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError("underlying price must be finite and positive")
        return value

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError("age_seconds must be finite and nonnegative")
        return value

    @model_validator(mode="after")
    def counts_must_match_delivered_contracts(self) -> OptionChainV1:
        calls = sum(contract.option_type == "CALL" for contract in self.contracts)
        puts = sum(contract.option_type == "PUT" for contract in self.contracts)
        strikes = len({contract.strike for contract in self.contracts})
        if (
            self.call_contract_count != calls
            or self.put_contract_count != puts
            or self.strike_count != strikes
        ):
            raise ValueError("option chain counts must match delivered contracts")
        symbols = [contract.symbol for contract in self.contracts]
        if len(set(symbols)) != len(symbols):
            raise ValueError("option chain contract symbols must be unique")
        return self


class OptionChainResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    option_chain: OptionChainV1


class PriceBarV1(GatewayModel):
    timestamp: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_timezone_aware(cls, value: dt.datetime) -> dt.datetime:
        if value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("volume")
    @classmethod
    def volume_must_be_nonnegative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("bar volume must be nonnegative")
        return value


class HistoryV1(GatewayModel):
    symbol: str
    frequency: Literal["daily", "minute"]
    bars: tuple[PriceBarV1, ...]
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("age_seconds must be nonnegative")
        return value


class HistoryResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    history: HistoryV1


class SessionHistoryV1(GatewayModel):
    """One trading session's (regular or extended) 1-minute candles for one date.

    Distinct from ``HistoryV1``: that model is a trailing window ending "now" (for the
    equity scanner's rolling-average reads); this one is a point-in-time lookup for an
    arbitrary past date, split into exactly the regular or extended segment of that
    date, for after-hours-earnings candle archival.
    """

    symbol: str
    date: dt.date
    session: Literal["regular", "extended"]
    candles: tuple[PriceBarV1, ...]
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("age_seconds must be nonnegative")
        return value


class SessionHistoryResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    session_history: SessionHistoryV1


MoverIndex = Literal[
    "$DJI",
    "$COMPX",
    "$SPX",
    "NYSE",
    "NASDAQ",
    "OTCBB",
    "INDEX_ALL",
    "EQUITY_ALL",
    "OPTION_ALL",
    "OPTION_PUT",
    "OPTION_CALL",
]


class MoverV1(GatewayModel):
    symbol: str
    last_price: float | None = None
    change: float | None = None
    change_percent: float | None = None
    volume: int | None = None

    @field_validator("volume")
    @classmethod
    def volume_must_be_nonnegative(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("mover volume must be nonnegative")
        return value


class MoversV1(GatewayModel):
    index: MoverIndex
    direction: Literal["up", "down"]
    movers: tuple[MoverV1, ...]
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: str
    stale: bool
    age_seconds: float | None = None
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("gateway timestamps must be timezone-aware")
        return value

    @field_validator("age_seconds")
    @classmethod
    def age_must_be_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("age_seconds must be nonnegative")
        return value


class MoversResponseV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    movers: MoversV1


class OrderBookParticipantV1(GatewayModel):
    """One venue participant contributing size at a displayed price level."""

    exchange: str
    size: int
    sequence: int | None = None

    @field_validator("exchange")
    @classmethod
    def exchange_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("order-book participant exchange must not be empty")
        return value

    @field_validator("size")
    @classmethod
    def size_must_be_nonnegative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("order-book participant size must be nonnegative")
        return value


class OrderBookLevelV1(GatewayModel):
    """One normalized bid or ask level from a venue-specific Schwab book."""

    price: float
    total_size: int
    participant_count: int
    participants: tuple[OrderBookParticipantV1, ...] = ()

    @field_validator("price")
    @classmethod
    def price_must_be_finite_and_positive(cls, value: float) -> float:
        if not math.isfinite(value) or value <= 0:
            raise ValueError("order-book price must be finite and positive")
        return value

    @field_validator("total_size", "participant_count")
    @classmethod
    def counts_must_be_nonnegative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("order-book counts must be nonnegative")
        return value


class OrderBookSnapshotV1(GatewayModel):
    """A venue-specific Schwab Level II snapshot; never consolidated depth."""

    schema_version: Literal["1.0"] = "1.0"
    symbol: str
    venue: Literal["NASDAQ", "NYSE"]
    service: Literal["NASDAQ_BOOK", "NYSE_BOOK"]
    connection_id: int = 1
    continuity_epoch: int = 1
    sequence: int | None = None
    event_timestamp: dt.datetime | None = None
    gateway_received_at: dt.datetime
    source: Literal["schwab_streaming"] = "schwab_streaming"
    is_consolidated: Literal[False] = False
    bids: tuple[OrderBookLevelV1, ...]
    asks: tuple[OrderBookLevelV1, ...]
    data_quality_flags: tuple[str, ...] = ()

    @field_validator("connection_id", "continuity_epoch")
    @classmethod
    def connection_fields_must_be_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("order-book connection fields must be positive")
        return value

    @field_validator("symbol")
    @classmethod
    def symbol_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("order-book symbol must not be empty")
        return value

    @field_validator("event_timestamp", "gateway_received_at")
    @classmethod
    def timestamps_must_be_timezone_aware(
        cls, value: dt.datetime | None
    ) -> dt.datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("order-book timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def venue_must_match_service(self) -> OrderBookSnapshotV1:
        expected = f"{self.venue}_BOOK"
        if self.service != expected:
            raise ValueError("order-book venue must match the Schwab service")
        bid_prices = [level.price for level in self.bids]
        ask_prices = [level.price for level in self.asks]
        if bid_prices != sorted(bid_prices, reverse=True):
            raise ValueError("order-book bids must be sorted highest first")
        if ask_prices != sorted(ask_prices):
            raise ValueError("order-book asks must be sorted lowest first")
        return self


class GatewayHealthV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    status: Literal["ok", "ready", "not_ready"]
    service: Literal["schwab-gateway"] = "schwab-gateway"
    timestamp: dt.datetime


class GatewayReadinessV1(GatewayHealthV1):
    """Bounded token-readiness detail for gateway operators."""

    token_state: Literal[
        "uninitialized", "ready", "refreshing", "missing", "corrupt", "expired",
        "revoked", "reauthorization_required", "lock_timeout", "refresh_failed",
        "persistence_failed",
    ]
    reason: Literal[
        "token_not_checked", "token_ready", "token_refreshing", "token_missing",
        "token_corrupt", "refresh_token_expired", "token_revoked",
        "token_reauthorization_required", "token_lock_timeout", "token_refresh_failed",
        "token_persistence_failed", "token_readiness_unavailable",
    ]


class GatewayErrorDetailV1(GatewayModel):
    code: str
    message: str


class GatewayErrorV1(GatewayModel):
    schema_version: Literal["1.0"] = "1.0"
    error: GatewayErrorDetailV1
