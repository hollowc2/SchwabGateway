"""Versioned, transport-neutral gateway API models."""

from __future__ import annotations

import datetime as dt
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator


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
