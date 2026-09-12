# ruff: noqa: E501 -- Prometheus exposition samples are intentionally literal.

from __future__ import annotations

import math

import pytest

from schwab_gateway.ttl_analysis import Histogram, parse_histograms, recommend_ttl

SAMPLE_METRICS = """\
# TYPE schwab_gateway_scheduler_queue_wait_seconds histogram
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="option_chain",priority_class="protected",le="0.5"} 2
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="option_chain",priority_class="protected",le="1.0"} 8
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="option_chain",priority_class="protected",le="+Inf"} 10
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="option_chain",priority_class="background",le="0.5"} 3
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="option_chain",priority_class="background",le="1.0"} 4
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="option_chain",priority_class="background",le="+Inf"} 5
schwab_gateway_scheduler_queue_wait_seconds_bucket{operation="spot",priority_class="protected",le="1.0"} 99
# TYPE schwab_gateway_scheduler_upstream_execution_seconds histogram
schwab_gateway_scheduler_upstream_execution_seconds_bucket{operation="option_chain",outcome="success",le="1.0"} 4
schwab_gateway_scheduler_upstream_execution_seconds_bucket{operation="option_chain",outcome="success",le="2.5"} 10
schwab_gateway_scheduler_upstream_execution_seconds_bucket{operation="option_chain",outcome="success",le="+Inf"} 10
"""


def test_parse_histograms_filters_operation_and_aggregates_label_sets() -> None:
    histograms = parse_histograms(SAMPLE_METRICS, operation="option_chain")

    assert histograms["schwab_gateway_scheduler_queue_wait_seconds"].buckets == {
        0.5: 5,
        1.0: 12,
        math.inf: 15,
    }


def test_recommendation_uses_conservative_sum_headroom_and_ceiling() -> None:
    histograms = {
        "schwab_gateway_scheduler_queue_wait_seconds": Histogram(
            {0.5: 98, 1.0: 100}
        ),
        "schwab_gateway_scheduler_upstream_execution_seconds": Histogram(
            {2.5: 99, 5.0: 100}
        ),
    }

    result = recommend_ttl(
        histograms,
        percentile=0.99,
        headroom_seconds=1.0,
        current_ttl_seconds=4.0,
        ceiling_seconds=3.0,
    )

    assert result is not None
    assert result.conservative_latency_seconds == 3.5
    assert result.uncapped_seconds == 4.5
    assert result.recommended_seconds == 3.0
    assert result.exceeds_ceiling


def test_recommendation_requires_both_histograms_to_have_samples() -> None:
    histograms = parse_histograms(SAMPLE_METRICS, operation="history")

    assert recommend_ttl(
        histograms,
        percentile=0.99,
        headroom_seconds=1.0,
        current_ttl_seconds=4.0,
    ) is None


@pytest.mark.parametrize("percentile", [0.0, -0.1, 1.1])
def test_histogram_rejects_invalid_percentile(percentile: float) -> None:
    with pytest.raises(ValueError, match="percentile"):
        Histogram({1.0: 1}).percentile(percentile)
