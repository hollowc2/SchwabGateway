from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError
from schwab_gateway_sdk.models import ChainMetadataV1

from schwab_gateway.upstream import (
    normalize_schwab_chain_metadata,
    normalize_schwab_history,
    normalize_schwab_movers,
    normalize_schwab_quote,
    normalize_schwab_session_history,
    normalize_schwab_spot,
)

EASTERN = ZoneInfo("America/New_York")


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


def test_quote_normalization_sources_close_and_net_percent_change_from_regular_session() -> None:
    received_at = dt.datetime(2026, 8, 3, 21, 0, tzinfo=dt.timezone.utc)
    quote = normalize_schwab_quote(
        "AAPL",
        {
            "quote": {
                "lastPrice": 200,
                "closePrice": 198.5,
                "netPercentChange": 0.76,
                "tradeTime": int((received_at - dt.timedelta(minutes=5)).timestamp() * 1000),
            },
            "extended": {
                "lastPrice": 201.1,
                "closePrice": 999,
                "netPercentChange": 999,
                "tradeTime": int((received_at - dt.timedelta(seconds=2)).timestamp() * 1000),
            },
        },
        received_at=received_at,
        stale_after_seconds=15,
    )

    assert quote.session == "extended"
    assert quote.close == 198.5
    assert quote.net_percent_change == 0.76


def test_quote_normalization_leaves_close_and_net_percent_change_none_when_absent() -> None:
    received_at = dt.datetime(2026, 8, 3, 21, 0, tzinfo=dt.timezone.utc)
    quote = normalize_schwab_quote(
        "AAPL",
        {"quote": {"lastPrice": 201.5}},
        received_at=received_at,
        stale_after_seconds=15,
    )

    assert quote.close is None
    assert quote.net_percent_change is None


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


def test_history_normalization_fails_closed_on_any_malformed_candle() -> None:
    received_at = dt.datetime(2026, 8, 10, 21, 0, tzinfo=dt.timezone.utc)
    good_ms = int((received_at - dt.timedelta(hours=1)).timestamp() * 1000)
    with pytest.raises(ValueError, match="malformed candles"):
        normalize_schwab_history(
            "AAPL",
            "daily",
            [
                {
                    "datetime": good_ms,
                    "open": 1,
                    "high": 2,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100,
                },
                {"datetime": good_ms, "open": 1, "high": 2, "low": 0.5, "close": 1.5},
                "not-a-candle",
            ],
            received_at=received_at,
            stale_after_seconds=86400,
        )


def test_history_normalization_applies_days_back_after_dropping_bad_candles() -> None:
    received_at = dt.datetime(2026, 8, 10, 21, 0, tzinfo=dt.timezone.utc)
    candles = [
        {
            "datetime": int((received_at - dt.timedelta(days=n)).timestamp() * 1000),
            "open": 1,
            "high": 2,
            "low": 0.5,
            "close": float(n),
            "volume": 100,
        }
        for n in range(5, 0, -1)
    ]
    history = normalize_schwab_history(
        "AAPL",
        "daily",
        candles,
        received_at=received_at,
        stale_after_seconds=86400,
        days_back=2,
    )

    assert len(history.bars) == 2
    assert [bar.close for bar in history.bars] == [2.0, 1.0]


def test_minute_history_days_back_means_calendar_days_not_candle_count() -> None:
    received_at = dt.datetime(2026, 8, 10, 21, 0, tzinfo=dt.timezone.utc)
    candles = []
    for days_ago in (2, 1, 0):
        for hour in (14, 15, 16):
            timestamp = received_at - dt.timedelta(days=days_ago, hours=21 - hour)
            candles.append(
                {
                    "datetime": int(timestamp.timestamp() * 1000),
                    "open": 1,
                    "high": 2,
                    "low": 0.5,
                    "close": float(days_ago * 10 + hour),
                    "volume": 100,
                }
            )

    history = normalize_schwab_history(
        "AAPL",
        "minute",
        candles,
        received_at=received_at,
        stale_after_seconds=86400,
        days_back=2,
    )

    assert len(history.bars) == 6
    assert {bar.timestamp.date() for bar in history.bars} == {
        dt.date(2026, 8, 9),
        dt.date(2026, 8, 10),
    }


def test_minute_history_keeps_prior_session_just_after_eastern_midnight() -> None:
    received_at = dt.datetime(2026, 8, 25, 4, 5, tzinfo=dt.timezone.utc)
    prior_session = dt.date(2026, 8, 24)
    candles = [
        _candle(
            dt.datetime.combine(
                prior_session,
                dt.time(hour, minute),
                tzinfo=EASTERN,
            ),
            close=float(hour),
        )
        for hour, minute in ((9, 30), (12, 0), (15, 59))
    ]

    history = normalize_schwab_history(
        "$SPX",
        "minute",
        candles,
        received_at=received_at,
        stale_after_seconds=900,
        days_back=1,
    )

    assert len(history.bars) == 3
    assert {
        bar.timestamp.astimezone(EASTERN).date() for bar in history.bars
    } == {prior_session}
    assert history.stale is True
    assert "stale" in history.data_quality_flags
    assert "no_bars_returned" not in history.data_quality_flags


@pytest.mark.parametrize(
    "received_at",
    [
        dt.datetime(2026, 8, 24, 14, 0, tzinfo=dt.timezone.utc),  # Monday before close
        dt.datetime(2026, 9, 8, 13, 0, tzinfo=dt.timezone.utc),  # Tuesday after Labor Day
    ],
)
def test_daily_history_treats_friday_as_fresh_across_weekend_or_holiday(
    received_at: dt.datetime,
) -> None:
    friday = (
        dt.datetime(2026, 8, 21, 20, 0, tzinfo=dt.timezone.utc)
        if received_at.month == 8
        else dt.datetime(2026, 9, 4, 20, 0, tzinfo=dt.timezone.utc)
    )
    history = normalize_schwab_history(
        "AAPL",
        "daily",
        [
            {
                "datetime": int(friday.timestamp() * 1000),
                "open": 1,
                "high": 2,
                "low": 0.5,
                "close": 1.5,
                "volume": 100,
            }
        ],
        received_at=received_at,
        stale_after_seconds=86400,
    )

    assert history.age_seconds is not None and history.age_seconds > 86400
    assert history.stale is False


def test_daily_history_recognizes_early_close_as_completed_session() -> None:
    early_close = dt.date(2026, 11, 27)
    event_timestamp = dt.datetime.combine(
        early_close, dt.time(13), tzinfo=EASTERN
    ).astimezone(dt.timezone.utc)
    received_at = dt.datetime.combine(
        early_close, dt.time(13, 30), tzinfo=EASTERN
    ).astimezone(dt.timezone.utc)

    history = normalize_schwab_history(
        "AAPL",
        "daily",
        [_candle(event_timestamp, 1.0)],
        received_at=received_at,
        stale_after_seconds=86400,
    )

    assert history.stale is False


def test_history_normalization_marks_no_bars_and_rejects_a_non_list_payload() -> None:
    received_at = dt.datetime(2026, 8, 10, 21, 0, tzinfo=dt.timezone.utc)
    empty = normalize_schwab_history(
        "AAPL", "daily", [], received_at=received_at, stale_after_seconds=86400
    )
    assert empty.bars == ()
    assert "no_bars_returned" in empty.data_quality_flags
    assert empty.stale is True

    with pytest.raises(ValueError):
        normalize_schwab_history(
            "AAPL",
            "daily",
            {"candles": []},
            received_at=received_at,
            stale_after_seconds=86400,
        )


def test_history_contract_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        normalize_schwab_history(
            "AAPL",
            "daily",
            [],
            received_at=dt.datetime(2026, 8, 10, 21, 0),
            stale_after_seconds=86400,
        )


def _candle(timestamp: dt.datetime, close: float) -> dict:
    return {
        "datetime": int(timestamp.timestamp() * 1000),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 100,
    }


def test_session_history_splits_regular_and_extended_candles() -> None:
    """2026-08-12 is EDT (UTC-4): 08:00/16:00/17:00 ET are pre-/post-market, 09:30/10:00
    ET are regular -- the 16:00:00 boundary belongs to extended, not regular."""
    received_at = dt.datetime(2026, 8, 13, 12, 0, tzinfo=dt.timezone.utc)
    date = dt.date(2026, 8, 12)
    utc = dt.timezone.utc
    candles = [
        _candle(dt.datetime(2026, 8, 12, 12, 0, tzinfo=utc), 1.0),  # 08:00 ET premarket
        _candle(dt.datetime(2026, 8, 12, 13, 30, tzinfo=utc), 2.0),  # 09:30 ET regular open
        _candle(dt.datetime(2026, 8, 12, 14, 0, tzinfo=utc), 3.0),  # 10:00 ET regular
        _candle(dt.datetime(2026, 8, 12, 20, 0, tzinfo=utc), 4.0),  # 16:00 ET first after-hours
        _candle(dt.datetime(2026, 8, 12, 21, 0, tzinfo=utc), 5.0),  # 17:00 ET after-hours
    ]

    regular = normalize_schwab_session_history(
        "AAPL", date, "regular", candles, received_at=received_at, stale_after_seconds=86400
    )
    extended = normalize_schwab_session_history(
        "AAPL", date, "extended", candles, received_at=received_at, stale_after_seconds=86400
    )

    assert [c.close for c in regular.candles] == [2.0, 3.0]
    assert [c.close for c in extended.candles] == [1.0, 4.0, 5.0]
    assert regular.symbol == "AAPL"
    assert regular.date == date
    assert regular.session == "regular"
    assert extended.session == "extended"


@pytest.mark.parametrize("date", [dt.date(2026, 11, 27), dt.date(2026, 12, 24)])
def test_session_history_uses_early_close_boundary(date: dt.date) -> None:
    received_at = dt.datetime.combine(
        date + dt.timedelta(days=1), dt.time(12), tzinfo=dt.timezone.utc
    )
    candles = [
        _candle(dt.datetime.combine(date, dt.time(12, 59), tzinfo=EASTERN), 1.0),
        _candle(dt.datetime.combine(date, dt.time(13, 0), tzinfo=EASTERN), 2.0),
    ]

    regular = normalize_schwab_session_history(
        "AAPL", date, "regular", candles, received_at=received_at, stale_after_seconds=86400
    )
    extended = normalize_schwab_session_history(
        "AAPL", date, "extended", candles, received_at=received_at, stale_after_seconds=86400
    )

    assert [bar.close for bar in regular.candles] == [1.0]
    assert [bar.close for bar in extended.candles] == [2.0]
    assert "early_close" in regular.data_quality_flags
    assert "early_close" in extended.data_quality_flags


def test_session_history_marks_exchange_holiday_and_returns_no_session_bars() -> None:
    date = dt.date(2026, 11, 26)  # Thanksgiving
    received_at = dt.datetime(2026, 11, 27, 17, 0, tzinfo=dt.timezone.utc)
    unexpected = [_candle(dt.datetime.combine(date, dt.time(10), tzinfo=EASTERN), 1.0)]

    result = normalize_schwab_session_history(
        "AAPL",
        date,
        "regular",
        unexpected,
        received_at=received_at,
        stale_after_seconds=86400,
    )

    assert result.candles == ()
    assert "market_holiday" in result.data_quality_flags
    assert "no_bars_returned" in result.data_quality_flags


def test_session_history_drops_malformed_candles_and_flags_them() -> None:
    received_at = dt.datetime(2026, 8, 13, 12, 0, tzinfo=dt.timezone.utc)
    good = _candle(dt.datetime(2026, 8, 12, 14, 0, tzinfo=dt.timezone.utc), 1.0)  # 10:00 ET
    result = normalize_schwab_session_history(
        "AAPL",
        dt.date(2026, 8, 12),
        "regular",
        [good, {"datetime": good["datetime"], "open": 1}, "not-a-candle"],
        received_at=received_at,
        stale_after_seconds=86400,
    )

    assert len(result.candles) == 1
    assert "malformed_bars_dropped" in result.data_quality_flags


def test_session_history_marks_no_bars_when_the_session_is_empty() -> None:
    received_at = dt.datetime(2026, 8, 13, 12, 0, tzinfo=dt.timezone.utc)
    regular_only = [_candle(dt.datetime(2026, 8, 12, 14, 0, tzinfo=dt.timezone.utc), 1.0)]

    extended = normalize_schwab_session_history(
        "AAPL",
        dt.date(2026, 8, 12),
        "extended",
        regular_only,
        received_at=received_at,
        stale_after_seconds=86400,
    )

    assert extended.candles == ()
    assert "no_bars_returned" in extended.data_quality_flags
    assert extended.stale is True


def test_session_history_rejects_a_non_list_payload() -> None:
    with pytest.raises(ValueError):
        normalize_schwab_session_history(
            "AAPL",
            dt.date(2026, 8, 12),
            "regular",
            {"candles": []},
            received_at=dt.datetime(2026, 8, 13, 12, 0, tzinfo=dt.timezone.utc),
            stale_after_seconds=86400,
        )


def test_session_history_contract_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        normalize_schwab_session_history(
            "AAPL",
            dt.date(2026, 8, 12),
            "regular",
            [],
            received_at=dt.datetime(2026, 8, 13, 12, 0),
            stale_after_seconds=86400,
        )


def test_movers_normalization_drops_malformed_items_and_reports_unknown_freshness() -> None:
    received_at = dt.datetime(2026, 8, 10, 21, 0, tzinfo=dt.timezone.utc)
    movers = normalize_schwab_movers(
        "$SPX",
        "up",
        [
            {"symbol": "AAPL", "netPercentChange": 3.5, "netChange": 2.0, "lastPrice": 200.0},
            {"ticker": "MSFT", "changePercent": 1.2},
            {"no_symbol": True},
            "not-a-mover",
        ],
        received_at=received_at,
    )

    assert [mover.symbol for mover in movers.movers] == ["AAPL", "MSFT"]
    assert movers.movers[0].change_percent == 3.5
    assert movers.movers[0].change == 2.0
    assert movers.movers[1].change_percent == 1.2
    assert "malformed_movers_dropped" in movers.data_quality_flags
    assert movers.stale is True
    assert movers.age_seconds is None
    assert "missing_event_timestamp" in movers.data_quality_flags


def test_movers_normalization_rejects_a_non_list_payload() -> None:
    received_at = dt.datetime(2026, 8, 10, 21, 0, tzinfo=dt.timezone.utc)
    with pytest.raises(ValueError):
        normalize_schwab_movers(
            "$SPX", "up", {"screeners": []}, received_at=received_at
        )


def test_movers_contract_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        normalize_schwab_movers(
            "$SPX", "up", [], received_at=dt.datetime(2026, 8, 10, 21, 0)
        )


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
