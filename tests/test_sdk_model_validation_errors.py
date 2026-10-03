"""Pin the SDK models' timestamp and age validation errors exactly."""

from __future__ import annotations

import datetime as dt

import pytest
from pydantic import ValidationError
from schwab_gateway_sdk.models import (
    ChainMetadataV1,
    HistoryV1,
    MoversV1,
    OptionContractV1,
    PriceBarV1,
    QuoteV1,
    SessionHistoryV1,
    SpotV1,
)

AWARE = dt.datetime(2026, 9, 29, 15, tzinfo=dt.UTC)
NAIVE = AWARE.replace(tzinfo=None)
COMMON = {"gateway_received_at": AWARE, "source": "test", "stale": False}
GATEWAY_MODELS = [
    (QuoteV1, {"symbol": "A", **COMMON}),
    (SpotV1, {"symbol": "A", **COMMON}),
    (
        ChainMetadataV1,
        {
            "symbol": "A",
            "expiration": AWARE.date(),
            "call_contract_count": 0,
            "put_contract_count": 0,
            "strike_count": 0,
            **COMMON,
        },
    ),
    (HistoryV1, {"symbol": "A", "frequency": "daily", "bars": (), **COMMON}),
    (
        SessionHistoryV1,
        {"symbol": "A", "date": AWARE.date(), "session": "regular", "candles": (), **COMMON},
    ),
    (MoversV1, {"index": "$SPX", "direction": "up", "movers": (), **COMMON}),
]
CONTRACT = {
    "symbol": "SPXW C5000",
    "option_type": "CALL",
    "expiration": AWARE.date(),
    "strike": 5000,
    "bid": 1,
    "ask": 2,
    "mark": 1.5,
    "stale": False,
}


def _errors(model, **fields) -> list[tuple[str, tuple, str]]:
    with pytest.raises(ValidationError) as raised:
        model(**fields)
    return [(e["type"], e["loc"], e["msg"]) for e in raised.value.errors()]


@pytest.mark.parametrize(("model", "fields"), GATEWAY_MODELS)
def test_gateway_model_timestamp_and_age_errors(model, fields) -> None:
    aware_message = "Value error, gateway timestamps must be timezone-aware"
    assert _errors(model, **{**fields, "event_timestamp": NAIVE}) == [
        ("value_error", ("event_timestamp",), aware_message)
    ]
    assert _errors(model, **{**fields, "gateway_received_at": NAIVE}) == [
        ("value_error", ("gateway_received_at",), aware_message)
    ]
    assert _errors(model, **{**fields, "age_seconds": -1}) == [
        ("value_error", ("age_seconds",), "Value error, age_seconds must be nonnegative")
    ]
    # These models deliberately accept non-finite ages; only the option models reject them.
    assert model(**{**fields, "age_seconds": float("inf")}).age_seconds == float("inf")
    assert model(**{**fields, "event_timestamp": None, "age_seconds": None}).age_seconds is None


@pytest.mark.parametrize("age", [-1, float("nan"), float("inf")])
def test_option_contract_age_must_be_finite_and_nonnegative(age: float) -> None:
    assert _errors(OptionContractV1, **{**CONTRACT, "age_seconds": age}) == [
        ("value_error", ("age_seconds",), "Value error, age_seconds must be finite and nonnegative")
    ]


def test_option_contract_and_bar_timestamps_must_be_aware() -> None:
    aware_message = "Value error, gateway timestamps must be timezone-aware"
    assert _errors(OptionContractV1, **{**CONTRACT, "event_timestamp": NAIVE}) == [
        ("value_error", ("event_timestamp",), aware_message)
    ]
    bar = {"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
    assert _errors(PriceBarV1, timestamp=NAIVE, **bar) == [
        ("value_error", ("timestamp",), aware_message)
    ]
