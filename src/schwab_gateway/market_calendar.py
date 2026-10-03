"""Recurring US-equity trading calendar in America/New_York."""

from __future__ import annotations

import datetime as dt
import functools
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
# Regular session is normally 09:30-16:00 America/New_York and ends at 13:00 on the
# recurring US-equity early-close dates below. A candle stamped exactly at the close is
# the first extended (post-market) bar, not the last regular one, so the upper bound is
# exclusive.
REGULAR_SESSION_START = dt.time(9, 30)
REGULAR_SESSION_END = dt.time(16, 0)
EARLY_CLOSE_SESSION_END = dt.time(13, 0)


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
    next_month = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
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


@functools.cache
def market_holidays(year: int) -> frozenset[dt.date]:
    holidays = {
        _observed_holiday(dt.date(year, 7, 4)),
        _observed_holiday(dt.date(year, 12, 25)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _last_weekday(year, 5, 0),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _easter_sunday(year) - dt.timedelta(days=2),
    }
    new_year = dt.date(year, 1, 1)
    # NYSE does not observe New Year's Day on the preceding Friday when January 1 falls
    # on a Saturday (e.g. 2021-12-31 and 2027-12-31 are regular sessions); a Sunday
    # January 1 still moves to Monday January 2.
    if new_year.weekday() != 5:
        holidays.add(_observed_holiday(new_year))
    if year >= 2022:
        holidays.add(_observed_holiday(dt.date(year, 6, 19)))
    return frozenset(holidays)


def is_trading_day(date: dt.date) -> bool:
    return date.weekday() < 5 and date not in market_holidays(date.year)


@functools.cache
def early_close_sessions(year: int) -> frozenset[dt.date]:
    """Return recurring 13:00 ET US-equity closes for ``year``.

    NYSE/Nasdaq publish their calendars annually. These rules cover the recurring
    Independence Day, day-after-Thanksgiving, and Christmas Eve closes without claiming
    to predict one-off exchange closures. The trading-day check handles years in which
    July 3 or December 24 is instead an observed full holiday.
    """
    candidates = {
        dt.date(year, 7, 3),
        _nth_weekday(year, 11, 3, 4) + dt.timedelta(days=1),
        dt.date(year, 12, 24),
    }
    return frozenset(date for date in candidates if is_trading_day(date))


def regular_session_end(date: dt.date) -> dt.time | None:
    if not is_trading_day(date):
        return None
    if date in early_close_sessions(date.year):
        return EARLY_CLOSE_SESSION_END
    return REGULAR_SESSION_END


def latest_completed_session(at: dt.datetime) -> dt.date:
    eastern = at.astimezone(EASTERN)
    candidate = eastern.date()
    session_end = regular_session_end(candidate)
    if session_end is None or eastern.time() < session_end:
        candidate -= dt.timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= dt.timedelta(days=1)
    return candidate
