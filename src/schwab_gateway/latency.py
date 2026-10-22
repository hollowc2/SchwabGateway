"""Cumulative latency buckets retain legacy boundaries and resolve readiness gates."""

from prometheus_client import Histogram

LATENCY_BUCKETS = tuple(sorted(set(Histogram.DEFAULT_BUCKETS) | {1.25, 1.5, 1.75, 2.0}))
