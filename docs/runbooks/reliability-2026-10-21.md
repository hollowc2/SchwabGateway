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
loops. Read-only checks of all three running containers confirmed SPX align=true,
offset=0 and NDX/XSP alignment fields absent, with 60s configured for each. ButterflyGuy's linked collector-only change uses existing minute-grid
logic at +0/+2/+4 seconds. No position or entry orchestration changes are needed.
Single-flight expansion is not justified by known request keys: distinct symbols,
SPX-only VIX, and existing chain coalescing already cover the obvious overlap.

The reproducible consumer synthetic FIFO burst model holds monitor arrivals and
990 upstream attempts constant: queue p95 2.9 -> 1.5s, latency p95 3.35 -> 1.7s,
max latency 4.5 -> 1.9s. This deliberately burst-heavy model is diagnostic, not
recorded-window replay or proof that production targets will pass. No acceptance
gates are relaxed. See the consumer PR's collector phase evidence for assumptions.

## Stage 3: classified failures and bounded recovery

Read-only Helios checks reconfirmed the supplied image and zero restarts. Installed
schwab-py is 1.5.1 and httpx 0.28.1. Reconstructed gateway dispatch peaks were 4/7/17/41
per rolling 1/2/5/60 seconds, respectively, over 3,727 completions. Counts exclude
external credential traffic and may miss outstanding operations at window edges.
No quota policy or root cause for 429 is established. Do not infer one from 41/min.

Known shared-credential paths include all three ButterflyGuy direct account/order
reads and reconciliation, gateway market-data reads, SDK automatic access-token
refresh inside a transaction, and the host's token keepalive. User crontab has two
keepalive references (the tool documents hourly refresh plus a weekly reminder),
and nine AfterHours job references. AfterHours uses session-history reads (24 in
the fixed gateway window). This inventory does not prove that other hosts or
processes cannot use the same application credential. We did not inspect secrets,
call account/order endpoints, modify jobs, or touch the frozen updater.

Gateway SDK and provider perform no automatic HTTP retry. The installed synchronous
Schwab client delegates reads to its HTTP session; no configured transport retry is
introduced here. ButterflyGuy's separate direct client has a retry loop for 429,
5xx and transport failures. Gateway-authoritative reads use the gateway SDK rather
than that direct loop. Direct broker retries remain outside this change's scope.

The adapter now copies only an allowlisted cause and validated Retry-After across
its sanitized exception boundary. Numeric integer seconds and timezone-aware HTTP
dates are accepted in [0,60] seconds; malformed, past and out-of-bound values are
rejected rather than shortened into retry permission. Raw headers, URLs, bodies,
exception text and tokens never enter response metadata or metric labels. SDK
subclasses preserve rate-limit/connection-timeout distinctions while remaining
compatible with existing GatewayUnavailableError handling. Wire schema stays 1.0.

429 is never retried automatically. It establishes a shared fail-fast read cooldown
of the validated interval (minimum 1s), or 1s when metadata is unavailable/invalid.
Queued reads check the same gate before fetching. Fresh cache hits remain usable
under the existing TTL; no stale fallback is added. This is local incident damping,
not a credential-wide quota limiter. Long Retry-After values are not forwarded as
bounded retry permission; consumers still fail closed without automatic retries.

Only explicitly listed market-data operations may retry ConnectTimeout once, after
100ms outside the scheduler and token transaction. The conservative replacement
deadline is original request start + existing execution budget (3s normally),
including initial queue wait and backoff; original first-attempt queue/execution
contracts are unchanged. At least 1.1s must remain before backoff. The replacement
queue and execution both share that absolute deadline. A timed-out or cancelled
operation retains worker ownership until completion; it never triggers a retry.
No scheduler timeout is retried; no account, order, token or credential operation
is eligible. We deliberately do not pool clients: their token callbacks are scoped
to the current locked transaction. Credential-proof entry points and token-manager
scope rules remain unchanged; any future immutable release/proof archive must be
rebuilt and reviewed for its new source identity, never reuse the old archive hash.

Provider transaction-attempt and failure-cause counters are separate from HTTP
request counts and scheduler dispatch counts. Scheduler execution timeouts are
classified independently of HTTP ConnectTimeout. Admission/queue/timeout metrics
and strict priority ownership remain authoritative. Tests use fake transactions
and clocks/events, never Schwab load, and cover retries, cancellation, drain, shared
cooldown, safe metadata and absolute second-attempt queue deadlines.

### Remaining coordination and future validation

A credential-wide controller requires each direct broker/keepalive/other-host client
to participate. Concrete next integration: a local/secured admission service keyed
by a configured nonsecret credential alias; all clients acquire a bounded lease
before an attempt, report 429 plus sanitized Retry-After, and release admission
before backoff. Keep token transactions separate, preserve monitor priority and
fail closed on coordinator failure. Inventory every host, obtain consumer-specific
identities and attempt metrics, confirm Schwab's actual account/application policy,
and measure short bursts before choosing any global quota. External direct-client
integration and complete credential ownership inventory remain dependencies; a
gateway-only limiter cannot satisfy them.

After CI and separately authorized rollout: deploy the exporter first, wait five
minutes before alert changes, then deploy the exact consumer candidate one paper
service at a time. Retain all baseline images and in-place alert backups. Check
health/readiness, zero restarts, fresh snapshots, unchanged monitor cadence,
upstream attempts versus HTTP counts, classified failures, cooldown/retry counts,
and exact log percentiles against the existing full-session acceptance gates.
Use no synthetic production load without separate authorization. Abort on new
503/504, token degradation, stale data or monitor gaps; restore only affected
images/rules using deploy.md and rollback.md. SPX remains NO-GO throughout; a
latency improvement alone does not authorize launch.

## Final development validation

Gateway: 828 tests passed, no skips; Ruff passed; all three distributions built;
Prometheus 3.10.0 rule tests passed in a network-disabled local container. Actual
base/demo and immutable production Compose overlays rendered with synthetic inputs.
The suite emits existing aiohttp request-key and websockets deprecation warnings.

Consumer: 1,366 tests passed with 37 environment-gated skips (30 require local PostgreSQL binaries,
six require private NDX snapshots, one requires CI_DATABASE_URL); the isolated TimescaleDB 2.30.1-pg16
migration/risk-read smoke separately passed. Ruff and actual Compose rendering passed.
Graph refresh ran with SQL extraction unavailable (`tree_sitter_sql` missing); its
large generated serialization diff is isolated from the collector commit. No
production load, real Schwab request, order, alert delivery, deployment, service
restart, credential change or frozen research edit was used as validation.

The consumer paper overlay bind-mounts /opt/butterflyguy/configs. An image-only
rollout cannot activate collector phases. Future approval must name the two mounted
paper config edits and exact backup/restore paths as well as images. Verify the
mounted paths are not the frozen prospective-v2 paths, preserve strategy/guard
fields, and restore only the affected service's config and image on rollback.
Consumer overlay validation used Compose --no-env-resolution with synthetic inputs;
production env files were neither read nor copied.
