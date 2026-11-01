"""Replay sanitized log durations; queue timestamps are reconstructed approximations.

Run: uv run python tools/replay_latency.py
Fixture contains only operation categories and numeric durations/relative timestamps.
"""

import bisect
import json
import math
from pathlib import Path

from prometheus_client import Histogram

from schwab_gateway.latency import LATENCY_BUCKETS


def histogram_quantile(values, buckets, q=.99):
    values = sorted(values)
    previous_count = previous_bound = 0
    for bound in buckets:
        count = bisect.bisect_right(values, bound)
        if count >= q * len(values):
            return previous_bound + (bound - previous_bound) * (
                q * len(values) - previous_count
            ) / (count - previous_count)
        previous_count, previous_bound = count, bound


def exact_quantile(values, q):
    values = sorted(values)
    # Nearest rank matches the historical request-log report (not linear interpolation).
    return values[max(0, math.ceil(len(values) * q) - 1)]


def replay():
    data = json.loads((Path(__file__).resolve().parents[1] / 'tests/fixtures/'
                       'reliability-2026-10-21.json').read_text())
    values = data['request_latency_seconds']['option_chain_v1']
    windows = false_legacy = disagreements_candidate = genuinely_slow = 0
    queues = data['queue_dispatch_offset_and_wait_seconds']
    for end in range(300, int(data['window_seconds']) + 1, 15):
        waits = [wait for stamp, wait in queues if end-300 < stamp <= end]
        if len(waits) <= 20:
            continue
        windows += 1
        bad = sum(wait > 1.5 for wait in waits) / len(waits) > .01
        old = histogram_quantile(waits, Histogram.DEFAULT_BUCKETS) > 1.5
        candidate = (len(waits) - bisect.bisect_right(sorted(waits), 1.5)) / len(waits) > .01
        false_legacy += old and not bad
        genuinely_slow += bad
        disagreements_candidate += candidate != bad
    return {
        'chain_requests': len(values), 'exact_log_p99_seconds': exact_quantile(values, .99),
        'legacy_histogram_p99_seconds': histogram_quantile(values, Histogram.DEFAULT_BUCKETS),
        'candidate_histogram_p99_seconds': histogram_quantile(values, LATENCY_BUCKETS),
        'reconstructed_queue_windows': windows, 'legacy_false_crossings': false_legacy,
        'candidate_disagreements': disagreements_candidate,
        'genuinely_slow_windows': genuinely_slow,
    }


if __name__ == '__main__':
    print(json.dumps(replay(), indent=2))
