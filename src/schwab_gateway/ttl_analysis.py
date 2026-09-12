"""Analyze gateway scheduler histograms for an option-chain cache TTL."""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass, field

import httpx
from prometheus_client.parser import text_string_to_metric_families

from schwab_gateway.upstream import MAX_OPTION_CHAIN_CACHE_TTL_SECONDS

METRIC_NAMES = (
    "schwab_gateway_scheduler_queue_wait_seconds",
    "schwab_gateway_scheduler_upstream_execution_seconds",
)


@dataclass
class Histogram:
    """Cumulative Prometheus histogram buckets aggregated across label sets."""

    buckets: dict[float, float] = field(default_factory=dict)

    def percentile(self, percentile: float) -> float | None:
        if not 0 < percentile <= 1:
            raise ValueError("percentile must be greater than 0 and at most 1")
        if not self.buckets:
            return None
        total = max(self.buckets.values())
        if total <= 0:
            return None
        target = total * percentile
        return next(
            (boundary for boundary in sorted(self.buckets) if self.buckets[boundary] >= target),
            None,
        )


@dataclass(frozen=True)
class TtlRecommendation:
    queue_wait_percentile_seconds: float
    execution_percentile_seconds: float
    conservative_latency_seconds: float
    uncapped_seconds: float
    recommended_seconds: float
    ceiling_seconds: float

    @property
    def exceeds_ceiling(self) -> bool:
        return self.uncapped_seconds > self.ceiling_seconds


def parse_histograms(text: str, *, operation: str) -> dict[str, Histogram]:
    """Parse and aggregate scheduler histogram buckets for one operation."""

    result = {name: Histogram() for name in METRIC_NAMES}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if not sample.name.endswith("_bucket"):
                continue
            metric_name = sample.name.removesuffix("_bucket")
            if metric_name not in result or sample.labels.get("operation") != operation:
                continue
            raw_boundary = sample.labels.get("le")
            if raw_boundary is None:
                continue
            boundary = math.inf if raw_boundary == "+Inf" else float(raw_boundary)
            result[metric_name].buckets[boundary] = (
                result[metric_name].buckets.get(boundary, 0.0) + float(sample.value)
            )
    return result


def recommend_ttl(
    histograms: dict[str, Histogram],
    *,
    percentile: float,
    headroom_seconds: float,
    current_ttl_seconds: float,
    ceiling_seconds: float = MAX_OPTION_CHAIN_CACHE_TTL_SECONDS,
) -> TtlRecommendation | None:
    """Return a conservative bounded TTL recommendation, or ``None`` without samples."""

    if headroom_seconds < 0 or current_ttl_seconds <= 0 or ceiling_seconds <= 0:
        raise ValueError("headroom must be nonnegative and TTL values must be positive")
    queue_wait = histograms[METRIC_NAMES[0]].percentile(percentile)
    execution = histograms[METRIC_NAMES[1]].percentile(percentile)
    if queue_wait is None or execution is None:
        return None
    conservative_latency = queue_wait + execution
    uncapped = max(conservative_latency + headroom_seconds, current_ttl_seconds)
    return TtlRecommendation(
        queue_wait_percentile_seconds=queue_wait,
        execution_percentile_seconds=execution,
        conservative_latency_seconds=conservative_latency,
        uncapped_seconds=uncapped,
        recommended_seconds=min(uncapped, ceiling_seconds),
        ceiling_seconds=ceiling_seconds,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8011")
    parser.add_argument("--operation", default="option_chain")
    parser.add_argument("--percentile", type=float, default=0.99)
    parser.add_argument("--headroom-seconds", type=float, default=1.0)
    parser.add_argument("--current-ttl-seconds", type=float, default=4.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    response = httpx.get(f"{args.gateway_url.rstrip('/')}/metrics", timeout=10.0)
    response.raise_for_status()
    histograms = parse_histograms(response.text, operation=args.operation)
    recommendation = recommend_ttl(
        histograms,
        percentile=args.percentile,
        headroom_seconds=args.headroom_seconds,
        current_ttl_seconds=args.current_ttl_seconds,
    )
    label = f"p{args.percentile * 100:g}"
    if recommendation is None:
        print(
            f"No {args.operation!r} samples exist for both scheduler histograms; "
            "collect a representative PAPER session before recommending a TTL.",
            file=sys.stderr,
        )
        return 1
    if not all(
        math.isfinite(value)
        for value in (
            recommendation.queue_wait_percentile_seconds,
            recommendation.execution_percentile_seconds,
        )
    ):
        print(
            f"{label} falls in a +Inf bucket; investigate scheduler contention "
            "instead of changing the TTL.",
            file=sys.stderr,
        )
        return 1
    print(f"operation={args.operation!r} {label}")
    print(f"queue_wait_seconds={recommendation.queue_wait_percentile_seconds:.3f}")
    print(f"upstream_execution_seconds={recommendation.execution_percentile_seconds:.3f}")
    print(f"conservative_latency_seconds={recommendation.conservative_latency_seconds:.3f}")
    print(f"recommended_ttl_seconds={recommendation.recommended_seconds:.3f}")
    if recommendation.exceeds_ceiling:
        print(
            f"WARNING: uncapped recommendation {recommendation.uncapped_seconds:.3f}s "
            f"exceeds the {recommendation.ceiling_seconds:.3f}s code ceiling.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
