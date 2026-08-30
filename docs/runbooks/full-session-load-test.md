# Full-session market-data load test

This runbook prepares and executes an auditable, read-only SchwabGateway load test on
Helios. It models the PAPER SPX, NDX, and XSP collector and position-monitor traffic.
It does not authorize an order, a live-money consumer, a service restart, a deployment,
a key-file replacement, or a Prometheus configuration change.

The gateway and direct ButterflyGuy clients share Schwab credentials and token storage.
A synthetic gateway test can therefore contend with otherwise direct consumers. Run the
real-provider stages only while the PAPER/live guards, database, broker, and rollback
gates below are satisfied.

## Test objective and scope

The full-session test must prove all of the following for one regular US-equity session:

- SPX, NDX, and XSP collector traffic at a 60-second cadence;
- spot, complete single-expiration option-chain, and minute-history warm-up reads;
- configurable two-second option-chain monitor windows;
- bounded entry-time bursts;
- protected and background admission behavior;
- continuous health, token readiness, and Prometheus collection;
- reproducible request evidence without response bodies, credentials, account data, or
  order data; and
- no degradation of the shared token or unrelated trading services.

The test is not an exchange-data completeness study. Schwab is the provider, all option
chains remain subject to the gateway's validation contract, and order-book depth remains
venue-specific when enabled.

## Immutable inputs

Record these before requesting approval:

```text
session_date_eastern=
operator=
gateway_git_sha=
gateway_release_tag=
gateway_image_id=
gateway_container=
gateway_restart_count_start=
candidate_image_id=
candidate_restart_count_start=
butterfly_git_sha=
butterfly_image_id=
load_driver_git_sha=
target_base_url=
credential_application_id=
credential_priority_class=
expiration_spx=
expiration_ndx=
expiration_xsp=
evidence_output_root=
prometheus_target=
rollback_gateway_image=
rollback_spx_image=
rollback_ndx_image=
rollback_xsp_image=
```

Use exact image IDs or repository digests, never mutable tags. Preserve the starting
restart counters; do not recreate a healthy container merely to reset a counter. The
current candidate has historical restarts, so success means its counter does not
increase during the approved observation window.

## Dedicated load-test credentials

Use a new `market_data:read` key with application ID
`schwab-gateway-full-session-background` for the synthetic candidate test. Keep the
existing protected ButterflyGuy key separate for the later PAPER-consumer stage.

Prepare, but do not activate, a replacement digest document and one-time plaintext file
at new paths. Never print either file or render secret-bearing Compose configuration:

```bash
umask 077
uv run schwab-gateway-issue-keys \
  --existing-input /absolute/path/to/current-digest-keys.json \
  --output /absolute/path/to/staged-load-test-digests.json \
  --plaintext-output /absolute/path/to/schwab-gateway-full-session-background.key \
  --application-id schwab-gateway-full-session-background \
  --capability market_data:read \
  --priority background
```

Both outputs must be new regular files with mode `0600`. The digest document contains no
plaintext key. The plaintext file contains only the generated key and must never be
committed, copied into evidence, passed on a command line, or printed.

Replacing the mounted digest document and recreating a gateway so it loads the new key
is a live configuration change. Stop and obtain explicit approval naming the file,
gateway instance, exact image, recreate command, validation, and rollback. Prefer adding
the key to the isolated candidate first. Do not rotate or remove existing keys as part
of a load test.

## Required code gates

Before touching Helios:

1. Gateway, SDK, and OpenAPI release versions match; wire schema remains `1.0`.
2. The complete gateway suite, lint, and all three package builds pass.
3. ButterflyGuy's gateway provider is tested against the exact SDK release.
4. Account, token, transaction, reconciliation, and order methods remain on the direct
   broker client.
5. PAPER mode is true, all live-trading guards are false, and shadow reads are false.
6. The load driver refuses unbounded duration/concurrency, accepts an absolute mode-0600
   key file, refuses to overwrite evidence, and stores no response bodies or secrets.
7. Prometheus rules pass `promtool check rules` before any separately approved install.

## Read-only Helios preflight

Collect a sanitized baseline immediately before the session:

- hostname, session date, exact Git/image identities, container start times, health,
  restart counts, OOM state, and bounded logs;
- `/health`, `/ready`, and `/metrics` for the selected gateway;
- production and candidate loopback ports and network aliases;
- token state and refresh-result counters without token contents;
- disk, memory, swap, CPU, and process resident memory;
- request/admission/cache/in-flight/lock/event-loop/upstream/subscriber metrics;
- Prometheus target health and rule evaluation state; and
- key/token file path, owner, mode, inode, size, and modification time only.

Also require database `OPEN` trades and nonterminal broker intents to be zero, then run
the existing authenticated, read-only broker audit for SPX/NDX/XSP positions and active,
missing, unmapped, or duplicate orders. Do not call a broker write API. If anything is
not flat, postpone the synthetic provider-load stage.

The observed Helios baseline on 2026-08-29 was production healthy/ready with zero
restarts, about 56 MiB resident container memory, and 33 GiB free root-disk space.
Production had no ordinary quote/chain traffic because the PAPER applications were in
direct mode. Treat this only as historical context and recapture every value.

## Staged execution

Every stage writes to a new evidence directory. A failed stage ends the run; do not
silently continue at a higher load.

### Stage 0: offline and fake-provider validation

Run the load-driver unit/integration tests and a short demo/fake-provider exercise. Prove
manifest hashes, non-overwrite behavior, cancellation, bounded concurrency, and secret
redaction without using Schwab credentials.

### Stage 1: candidate smoke

Use the background key against the loopback-only candidate for 15 minutes. Run the three
60-second collectors, one short monitor window, and small entry bursts. Require all
acceptance gates before continuing.

### Stage 2: candidate stepped load

Run, in order:

1. SPX collector only for 30 minutes;
2. SPX, NDX, and XSP collectors for 30 minutes;
3. the three collectors plus one 30-minute two-second monitor window;
4. overlapping SPX/NDX/XSP 30-minute monitor windows; and
5. a 15-minute cool-down with collectors only.

This stage tests the background admission pool. A separate, explicitly approved short
test may use a protected-scoped test key to prove pool isolation; never saturate the
protected pool while a real consumer depends on it.

### Stage 3: full-session synthetic profile

Start before the regular session and end after the close, using the exact session's
0-DTE expiration. The driver command is:

```bash
uv run schwab-gateway-load-test \
  --base-url http://127.0.0.1:8012 \
  --api-key-file /absolute/path/to/schwab-gateway-full-session-background.key \
  --expiration YYYY-MM-DD \
  --duration-seconds 24600 \
  --output-root /absolute/path/to/nonexistent-evidence-root \
  --monitor-window SPX:1800:1800 \
  --monitor-window NDX:7200:1800 \
  --monitor-window XSP:12600:1800 \
  --monitor-window SPX:18000:1800 \
  --monitor-window NDX:18000:1800 \
  --monitor-window XSP:18000:1800 \
  --entry-burst SPX:1740:4 \
  --entry-burst NDX:7140:4 \
  --entry-burst XSP:12540:4
```

Offsets are seconds from driver start. Set the actual start time so collector coverage
includes 09:30 through 16:00 Eastern. Adjust only the documented monitor-window timing,
not the 60-second collector or two-second monitor cadence, and record the exact command
without the key value. The driver evidence—not terminal output—is the source of truth.

### Stage 4: PAPER consumer session

This is a separate deployment/cutover. It requires explicit approval to deploy the exact
ButterflyGuy image and recreate each PAPER service one at a time. SPX, then NDX, then XSP
must each clear `gateway_market_data_warming` after fresh spot, option-chain, and minute
history reads. Observe one complete session with account/order operations still direct.

Do not enable a real-money XSP workflow. The four-second option-chain cache is a PAPER
contract; live-money use requires a separately reviewed force-fresh policy.

## Acceptance gates

The session passes only when all mandatory gates pass:

- health and readiness remain good; no readiness flap lasts two scrapes;
- gateway, candidate, and PAPER restart-count deltas are zero; no OOM or unexpected exit;
- no token missing/corrupt/expired/revoked/refresh/persistence/lock-timeout result;
- no malformed contract or unexpected schema/quality failure;
- no HTTP `502` or `503` during the regular-session test window;
- HTTP `429` and `504` totals are zero for the intended profile; any injected saturation
  test is reported separately and must prove bounded recovery;
- option-chain p95 latency is below 1.5 seconds and p99 below 2.0 seconds;
- all other read p95 latencies are below 1.0 second;
- event-loop lag p99 is below 100 milliseconds;
- token-lock wait p99 is below 500 milliseconds and hold p99 below the configured
  upstream timeout;
- option-chain in-flight work never exceeds its configured bound;
- protected traffic is not rejected during a background saturation test;
- order-book subscriber drops remain zero when the stream is in scope;
- resident memory increases by less than 128 MiB from the post-warm baseline and does not
  rise monotonically through cool-down;
- process CPU remains below 70 percent for five consecutive minutes;
- every scheduled request is accounted for in the manifest, with cancellations and
  transport/contract failures explicitly classified; and
- database snapshots remain fresh and there is no decision divergence attributable to
  the gateway.

Threshold changes require a written reason before the run. Never loosen a threshold
after seeing a failure and call the same evidence a pass.

## Evidence and integrity

Keep the driver's raw request-event NDJSON immutable. It must contain only operation,
symbol, stage, scheduled/start/end timestamps, latency, status or bounded error class,
schema version, freshness/age, contract/bar counts, and quality flags. It must not
contain headers, URLs with credentials, response bodies, raw market-data rows, account
identifiers, orders, positions, or tokens.

The manifest must record provider, Eastern session date, UTC observation range, exact
commits/images, target, application ID and priority (not key), workload configuration,
event file path/hash/count, stage summaries, error counts, latency quantiles, termination
reason, and acceptance results. Store Prometheus range-query results and the sanitized
preflight/postflight records as separate hashed evidence; do not rewrite driver evidence.

## Abort and rollback

Abort immediately on token degradation, repeated `429/504`, readiness flapping, unsafe
latency, unbounded memory/event-loop lag, malformed data, consumer divergence, unexpected
position/order activity, or any order-write indication. Stop only the load driver first;
preserve its partial evidence and manifest.

Candidate smoke/synthetic stages normally require no service rollback because the load
driver is the only new process. If an approved key-file or candidate deployment change
was made, use its exact recorded restore command. For a PAPER-consumer stage, restore
only the affected service's retained direct-mode image after repeating the database and
authenticated broker flatness gates. Never debug by repeatedly recreating a failed live
service in place.

## Mandatory approval stop

Before issuing/activating a key, changing Prometheus, deploying an image, recreating a
gateway or PAPER service, or starting real Schwab load, provide one approval request that
names:

- target host, service, session date, exact commits/images, target URL, and workload;
- preflight and broker/database flatness results;
- key application ID/priority and exact digest-file activation plan;
- expected impact on the shared token and PAPER services;
- monitoring queries and acceptance/abort thresholds;
- evidence paths; and
- exact stop and rollback procedures.

Any change to target, image, credential scope, workload, or rollback procedure invalidates
the approval and requires a new stop.
