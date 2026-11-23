"""Bounded failure metadata; never retain upstream text, URLs or headers."""

from __future__ import annotations

import datetime as dt
import math
from email.utils import parsedate_to_datetime
from typing import Literal

import httpx
from prometheus_client import Counter

FailureKind = Literal['rate_limited', 'connect_timeout', 'scheduler_timeout', 'other']
upstream_failures = Counter(
    'schwab_gateway_upstream_failures_total',
    'Failed upstream attempts or scheduler deadlines by bounded cause',
    ['operation', 'cause'],
)

upstream_attempts = Counter(
    'schwab_gateway_upstream_attempts_total',
    'Actual provider transaction attempts, distinct from HTTP requests and cache hits',
    ['operation'],
)

read_recovery = Counter(
    'schwab_gateway_read_recovery_total',
    'Bounded read recovery decisions, separate from request and provider attempt counts',
    ['outcome'],
)


# Longest cooldown one Schwab 429 can arm, so a malformed or extreme header cannot shed
# a family until restart. No Schwab 429 through 2026-11-22 asked for more than 1s.
MAX_RETRY_AFTER_SECONDS = 3600.0


def retry_after_seconds(value: str | None, *, now: dt.datetime | None = None) -> float | None:
    """Accept integer seconds or a timezone-aware future HTTP-date.

    A long value is kept, never read as missing, so callers are not told to retry sooner
    than Schwab asked; only one beyond ``MAX_RETRY_AFTER_SECONDS`` is capped to it.
    Malformed and past values are rejected.
    """
    if value is None or len(value) > 128:
        return None
    try:
        if value.strip().isdigit():
            seconds = float(value.strip())
        else:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return None
            seconds = (date - (now or dt.datetime.now(dt.UTC))).total_seconds()
    except (ValueError, TypeError, OverflowError):
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


class FailureMetadata:
    """Allowlisted attributes copied across the transaction error boundary."""

    def __init__(self, cause: FailureKind = 'other', retry_after: float | None = None):
        self.cause = cause
        self.retry_after = retry_after


def classify(exc: BaseException) -> FailureMetadata:
    # Only traverse explicit causes retained by the normalizers, not arbitrary contexts.
    for _ in range(8):
        if isinstance(exc, FailureMetadata):
            return exc
        if isinstance(exc, httpx.ConnectTimeout):
            return FailureMetadata('connect_timeout')
        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
            return FailureMetadata('rate_limited', retry_after_seconds(
                exc.response.headers.get('Retry-After')
            ))
        if exc.__cause__ is None:
            break
        exc = exc.__cause__
    return FailureMetadata()


ACCOUNT_WIDE = 'account'


class ReadCooldowns:
    """Local cooldowns armed by Schwab 429s, scoped to the Schwab operation family.

    Between 2026-11-07 and 11-22 every Schwab 429 the gateway saw (36 of them) was an
    option-chain read, while spot, quote and history reads on the same key never were.
    A single shared cooldown therefore turned each chain throttle into 503s for spot and
    quote reads that Schwab would have served. A 429 now cools only its own family. The
    gateway escalates to an account-wide cooldown only on evidence of account-wide
    throttling: a second family throttled while another family is still cooling, or a
    429 from an operation with no known family.

    Callers pass the same monotonic clock to every method. Only an observed Schwab 429
    may call ``arm``; a local rejection must never extend a cooldown.
    """

    def __init__(self) -> None:
        self._until: dict[str, float] = {}

    def remaining(self, family: str | None, now: float) -> float:
        scopes = (ACCOUNT_WIDE,) if family is None else (family, ACCOUNT_WIDE)
        return max(0.0, *(self._until.get(scope, 0.0) - now for scope in scopes))

    def arm(self, family: str | None, seconds: float, now: float) -> str:
        """Start or extend a cooldown and return the scope that was armed."""
        other_family_cooling = any(
            scope not in (family, ACCOUNT_WIDE) and until > now
            for scope, until in self._until.items()
        )
        scope = ACCOUNT_WIDE if family is None or other_family_cooling else family
        self._until[scope] = max(self._until.get(scope, 0.0), now + seconds)
        return scope
