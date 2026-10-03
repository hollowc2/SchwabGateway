"""Characterization of the recurring US-equity calendar rules.

The expected dates are the rules' own output, pinned so caching or relocating the calendar
cannot silently change which sessions count as holidays or early closes.
"""

from __future__ import annotations

import datetime as dt

import pytest

from schwab_gateway import market_calendar

HOLIDAYS = {
    2021: "01-01 01-18 02-15 04-02 05-31 07-05 09-06 11-25 12-24",
    2022: "01-17 02-21 04-15 05-30 06-20 07-04 09-05 11-24 12-26",
    2023: "01-02 01-16 02-20 04-07 05-29 06-19 07-04 09-04 11-23 12-25",
    2024: "01-01 01-15 02-19 03-29 05-27 06-19 07-04 09-02 11-28 12-25",
    2025: "01-01 01-20 02-17 04-18 05-26 06-19 07-04 09-01 11-27 12-25",
    2026: "01-01 01-19 02-16 04-03 05-25 06-19 07-03 09-07 11-26 12-25",
    2027: "01-01 01-18 02-15 03-26 05-31 06-18 07-05 09-06 11-25 12-24",
    2028: "01-17 02-21 04-14 05-29 06-19 07-04 09-04 11-23 12-25",
    2029: "01-01 01-15 02-19 03-30 05-28 06-19 07-04 09-03 11-22 12-25",
    2030: "01-01 01-21 02-18 04-19 05-27 06-19 07-04 09-02 11-28 12-25",
}
EARLY_CLOSES = {
    2021: "11-26",
    2022: "11-25",
    2023: "07-03 11-24",
    2024: "07-03 11-29 12-24",
    2025: "07-03 11-28 12-24",
    2026: "11-27 12-24",
    2027: "11-26",
    2028: "07-03 11-24",
    2029: "07-03 11-23 12-24",
    2030: "07-03 11-29 12-24",
}


def _dates(year: int, spec: str) -> frozenset[dt.date]:
    return frozenset(dt.date.fromisoformat(f"{year}-{day}") for day in spec.split())


@pytest.mark.parametrize("year", sorted(HOLIDAYS))
def test_recurring_holidays_and_early_closes_are_pinned(year: int) -> None:
    assert market_calendar.market_holidays(year) == _dates(year, HOLIDAYS[year])
    assert market_calendar.early_close_sessions(year) == _dates(year, EARLY_CLOSES[year])


@pytest.mark.parametrize(
    ("date", "expected"),
    [
        (dt.date(2026, 9, 28), dt.time(16, 0)),
        (dt.date(2026, 11, 27), dt.time(13, 0)),
        (dt.date(2026, 11, 26), None),
        (dt.date(2026, 10, 3), None),
        (dt.date(2021, 12, 31), dt.time(16, 0)),
    ],
)
def test_regular_session_end_is_pinned(date: dt.date, expected: dt.time | None) -> None:
    assert market_calendar.regular_session_end(date) == expected


@pytest.mark.parametrize(
    ("at", "expected"),
    [
        # Before the close the previous trading day is the latest completed session.
        (dt.datetime(2026, 9, 29, 19, 59, tzinfo=dt.UTC), dt.date(2026, 9, 28)),
        (dt.datetime(2026, 9, 29, 20, 0, tzinfo=dt.UTC), dt.date(2026, 9, 29)),
        # An early close completes the session at 13:00 New York time.
        (dt.datetime(2026, 11, 27, 18, 0, tzinfo=dt.UTC), dt.date(2026, 11, 27)),
        # Weekends and holidays walk back to the last trading day.
        (dt.datetime(2026, 10, 4, 15, 0, tzinfo=dt.UTC), dt.date(2026, 10, 2)),
        (dt.datetime(2026, 11, 26, 18, 0, tzinfo=dt.UTC), dt.date(2026, 11, 25)),
    ],
)
def test_latest_completed_session_is_pinned(at: dt.datetime, expected: dt.date) -> None:
    assert market_calendar.latest_completed_session(at) == expected
