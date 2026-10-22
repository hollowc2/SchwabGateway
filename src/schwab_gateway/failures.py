"""Bounded failure metadata; never retain upstream text, URLs or headers."""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
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


def retry_after_seconds(value: str | None, *, now: dt.datetime | None = None) -> float | None:
    """Accept seconds or timezone-aware HTTP-date within a 60-second decision bound.

    Values beyond the bound are rejected, never shortened into permission to retry.
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
    return seconds if math.isfinite(seconds) and 0 <= seconds <= 60 else None


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


@dataclass
class ReadBackoff:
    until: float = 0.0
