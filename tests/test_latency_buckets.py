"""Synthetic measurement regression; no production observations are implied."""

import pytest
from prometheus_client import CollectorRegistry, Histogram

from schwab_gateway.latency import LATENCY_BUCKETS


def estimate(values, buckets, quantile):
    target = len(values) * quantile
    previous_count = previous_bound = 0
    for bound in buckets:
        count = sum(value <= bound for value in values)
        if count >= target:
            return previous_bound + (bound - previous_bound) * (
                target - previous_count
            ) / (count - previous_count)
        previous_count, previous_bound = count, bound


def test_old_false_classification():
    values = [0.9] * 94 + [1.49] * 6
    assert estimate(values, Histogram.DEFAULT_BUCKETS, .99) > 1.5
    assert estimate(values, LATENCY_BUCKETS, .99) < 1.5
    assert sum(value > 1.5 for value in values) == 0


@pytest.mark.parametrize("values,exceeded", [
    ([1.49, 1.5, 1.51], 1), ([2.0] * 100, 100), ([1.5] * 100, 0),
])
def test_cumulative_threshold(values, exceeded):
    metric = Histogram("latency", "test", buckets=LATENCY_BUCKETS,
                       registry=CollectorRegistry())
    for value in values:
        metric.observe(value)
    samples = {s.labels.get("le"): s.value for s in metric.collect()[0].samples
               if s.name == "latency_bucket"}
    assert samples["+Inf"] - samples["1.5"] == exceeded
    assert set(Histogram.DEFAULT_BUCKETS) <= set(LATENCY_BUCKETS)


def test_recorded_window_replay():
    import runpy
    from pathlib import Path

    result = runpy.run_path(str(Path(__file__).parents[1] / 'tools/replay_latency.py'))['replay']()
    assert result['chain_requests'] == 8422
    assert result['exact_log_p99_seconds'] == pytest.approx(1.49836)
    assert result['legacy_histogram_p99_seconds'] == pytest.approx(2.2503617)
    assert result['candidate_histogram_p99_seconds'] == pytest.approx(1.4992568)
    assert result['reconstructed_queue_windows'] == 716
    assert result['legacy_false_crossings'] == 240
    assert result['candidate_disagreements'] == 0
    assert result['genuinely_slow_windows'] == 67
