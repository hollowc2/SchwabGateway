from __future__ import annotations

import datetime as dt

import pytest
from pydantic import ValidationError
from schwab_gateway_sdk.models import ChainMetadataV1

from schwab_gateway.upstream import (
    normalize_schwab_chain_metadata,
    normalize_schwab_quote,
    normalize_schwab_spot,
)


def test_quote_normalization_preserves_missing_fields_and_staleness() -> None:
    received_at = dt.datetime(2026, 8, 3, 21, 0, tzinfo=dt.timezone.utc)
    quote = normalize_schwab_quote(
        "AAPL",
        {
            "quote": {
                "lastPrice": 201.5,
                "tradeTime": int((received_at - dt.timedelta(seconds=20)).timestamp() * 1000),
            }
        },
        received_at=received_at,
        stale_after_seconds=15,
    )

    assert quote.bid is None
    assert quote.ask is None
    assert quote.last == 201.5
    assert quote.age_seconds == 20
    assert quote.stale is True
    assert quote.data_quality_flags == ("missing_bid", "missing_ask", "stale")


def test_quote_normalization_uses_fresher_extended_session() -> None:
    received_at = dt.datetime(2026, 8, 3, 21, 0, tzinfo=dt.timezone.utc)
    quote = normalize_schwab_quote(
        "AAPL",
        {
            "quote": {
                "lastPrice": 200,
                "tradeTime": int((received_at - dt.timedelta(minutes=5)).timestamp() * 1000),
            },
            "extended": {
                "bidPrice": 201,
                "askPrice": 201.2,
                "lastPrice": 201.1,
                "tradeTime": int((received_at - dt.timedelta(seconds=2)).timestamp() * 1000),
            },
        },
        received_at=received_at,
        stale_after_seconds=15,
    )

    assert quote.session == "extended"
    assert quote.bid == 201
    assert quote.ask == 201.2
    assert quote.stale is False
    assert quote.data_quality_flags == ()


def test_quote_contract_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        normalize_schwab_quote(
            "AAPL",
            {},
            received_at=dt.datetime(2026, 8, 3, 21, 0),
            stale_after_seconds=15,
        )


def test_chain_metadata_contract_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        normalize_schwab_chain_metadata(
            "SPX",
            {"callExpDateMap": {}},
            dt.date(2026, 8, 6),
            received_at=dt.datetime(2026, 8, 6, 21, 0),
            stale_after_seconds=90,
        )


def test_spot_contract_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        normalize_schwab_spot("$SPX", 5000.0, received_at=dt.datetime(2026, 8, 6, 21, 0))


def test_chain_metadata_contract_rejects_negative_counts_and_ages() -> None:
    received_at = dt.datetime(2026, 8, 6, 21, 0, tzinfo=dt.timezone.utc)
    base = {
        "symbol": "SPX",
        "expiration": dt.date(2026, 8, 6),
        "call_contract_count": 1,
        "put_contract_count": 1,
        "strike_count": 1,
        "gateway_received_at": received_at,
        "source": "test",
        "stale": False,
    }

    with pytest.raises(ValidationError, match="nonnegative"):
        ChainMetadataV1(**{**base, "strike_count": -1})
    with pytest.raises(ValidationError, match="nonnegative"):
        ChainMetadataV1(**{**base, "age_seconds": -1.0})
    with pytest.raises(ValidationError):
        ChainMetadataV1(**{**base, "unexpected_field": 1})


def test_chain_metadata_normalization_marks_an_old_quote_time_stale() -> None:
    received_at = dt.datetime(2026, 8, 6, 21, 0, tzinfo=dt.timezone.utc)
    stale_time = int((received_at - dt.timedelta(seconds=600)).timestamp() * 1000)
    metadata = normalize_schwab_chain_metadata(
        "SPX",
        {
            "underlying": {"quoteTime": stale_time},
            "callExpDateMap": {"2026-08-06:0": {"5000.0": [{"bid": 1.0}]}},
        },
        dt.date(2026, 8, 6),
        received_at=received_at,
        stale_after_seconds=90,
    )

    assert metadata.age_seconds == 600
    assert metadata.stale is True
    assert metadata.put_contract_count == 0
    assert metadata.data_quality_flags == (
        "missing_underlying_price",
        "missing_put_contracts",
        "stale",
    )
