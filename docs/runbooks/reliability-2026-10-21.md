# Reliability fixes: development evidence

SPX launch remains NO-GO. No production change or real-provider load is authorized
by this work. NDX/XSP paper services and frozen prospective-v2 evidence stay unchanged.

## Stage 1: measurement

The supplied fixed-window log evidence (13:30–16:33:56.745867 UTC) reports exact
chain p99 1.49836s, legacy histogram estimate 2.25036s and finer-bucket replay
estimate 1.49926s. These results were reproduced from a sanitized numeric fixture with
`uv run python tools/replay_latency.py`. Exact log p99 uses nearest rank; linear
interpolation would instead give 1.4953927s. Queue timestamps are reconstructed
from completion time minus rounded execution duration, so queue replay remains
an approximation. Across 716 five-minute windows on a 15-second grid, legacy
interpolation falsely crosses 240 times; the explicit fraction disagrees zero
times and retains 67 genuinely slow windows.
The exact log statistic must not be substituted with a histogram estimate.

`tests/test_latency_buckets.py` is a reproducible synthetic equivalent: 94 reads
at .9s and six at 1.49s. Legacy interpolation falsely exceeds 1.5s; candidate
interpolation does not. Cumulative counts distinguish exactly-at from above.
TTL analysis continues using conservative bucket upper bounds; no TTL changes.
Existing metric names, labels and all default bucket boundaries are retained.

The queue warning uses >1% strictly above 1.5s, >20 observations over five
minutes and a ten-minute hold. `rate`/`increase` are applied before aggregation,
so resets remain per-series. Zero traffic cannot fire the warning. Old exporters
use a conservative >1s bound until the exact bucket is present; false positives
remain possible during migration, but missing exact buckets do not silently turn
warnings off. A separate missing-buckets warning covers absent fallback buckets.
A lost exporter remains covered by the existing Down alert.

Future approved rollout: validate rules, deploy exporter first, retain old rules
until all series expose 1.5s and at least five minutes have elapsed, then install
rules and verify evaluation. During mixed rollout, use per-series bucket queries;
aggregate histogram quantiles/TTL recommendations only over homogeneous bucket
schemas. Roll back rules first to the recorded previous bytes in place, validate
and reload using the runbooks, then restore the recorded immutable gateway image
from its release directory. Do not change thresholds, TTLs or worker concurrency.


## Stage 2: collector collisions

Consumer review found SPX aligned to minute :00 while NDX/XSP slept 60 seconds
AFTER each pass; the two-second runtime startup phases do not survive those
loops. ButterflyGuy's linked collector-only change uses existing minute-grid
logic at +0/+2/+4 seconds. No position or entry orchestration changes are needed.
Single-flight expansion is not justified by known request keys: distinct symbols,
SPX-only VIX, and existing chain coalescing already cover the obvious overlap.

The reproducible consumer synthetic FIFO burst model holds monitor arrivals and
990 upstream attempts constant: queue p95 2.9 -> 1.5s, latency p95 3.35 -> 1.7s,
max latency 4.5 -> 1.9s. This deliberately burst-heavy model is diagnostic, not
recorded-window replay or proof that production targets will pass. No acceptance
gates are relaxed. See the consumer PR's collector phase evidence for assumptions.
