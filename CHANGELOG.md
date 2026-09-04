# Changelog

## 0.4.4 - 2026-09-04

Follow-up to a read-only latency investigation done against the frozen `0.4.3`
build (ButterflyGuy repo's
`docs/runbooks/schwab-gateway-option-chain-latency-investigation.md`), which
traced `/v1/option-chain` cache-miss fetches taking 4.2-6.2s against a 4s cache
TTL.

- Raise the option-chain cache TTL ceiling (`MAX_OPTION_CHAIN_CACHE_TTL_SECONDS`,
  enforced in both `upstream.py` and `config.py`) from 4s to 8s. The gateway's
  own metrics show zero `operation="option_chain"` samples on the live
  container as of this release (restarted with no accumulated history), so 8s
  is sized from the investigation's small 4.2-6.2s sample with headroom, not
  from a full-session p99 -- re-tune
  `SCHWAB_GATEWAY_OPTION_CHAIN_CACHE_TTL_SECONDS` once
  `schwab_gateway_scheduler_upstream_execution_seconds` /
  `_queue_wait_seconds` for `operation="option_chain"` have a real trading-day
  sample. Deploying the new TTL value is a separate follow-up from this
  release.
- Stop re-parsing the cached option chain through
  `OptionChainV1.model_validate_json` on every cache hit; cache the already-
  parsed model and only re-run the freshness/staleness recomputation that
  actually depends on wall-clock time. Cache-hit latency should no longer
  include a full JSON re-parse of the largest chains.
- Add `SchwabGatewayOptionChainEndToEndLatencyHigh` to `infra/alerts.yml`,
  approximating true end-to-end option-chain latency (queue wait + execution,
  not just execution) since the existing `upstream_timeout_seconds` budget
  starts at scheduler dispatch and cannot see time spent queued behind other
  operation types.
- Document, rather than change, two known tradeoffs the investigation
  confirmed are real but are not the dominant latency source: the scheduler's
  single physical execution slot is shared across all operation types by
  design (`scheduler.py`, unchanged since 0.4.0), and each locked Schwab
  client transaction constructs a fresh client/session because schwab-py
  captures the transaction-scoped token callbacks at construction time
  (`token_adapter.py`). Both are called out in code comments with what would
  need to change to relax them, should queue-wait metrics justify it later.

## 0.4.3 - 2026-09-04

- Raise the `/v1/history` `frequency=daily` `days_back` ceiling from 20 to 250 (a full
  trading year), so charts like AfterHoursLab's event-page "Daily context" can show a
  real trend instead of ~1 month. The live provider now requests a trailing year
  (`period_type=YEAR`) from Schwab instead of one month so there is enough data to trim
  down to the requested window.

## 0.4.2 - 2026-09-03

- Raise the Compose `SCHWAB_GATEWAY_BACKGROUND_QUEUE_TIMEOUT_SECONDS` fallback from
  one to five seconds so it tracks the code default; without this the container still
  received the old one-second budget regardless of the 0.4.1 change.

## 0.4.1 - 2026-09-03

- Record HTTP peer disconnects under their own `499` metric status with the authenticated
  caller label instead of logging them as `500`/`anonymous` server errors.
- Raise the default background queue-wait budget from one to five seconds so bursty
  background fan-outs queue behind the single worker instead of shedding as `503`.
- Gate live readiness on one successful Schwab round-trip at startup, so a redeploy
  serves `503` not-ready until the client is warm instead of dropping the first
  in-flight protected reads on a cold worker. A failing warmup keeps retrying.

## 0.4.0 - 2026-09-02

- Add a bounded strict-priority scheduler for the single Schwab execution slot, with
  protected FIFO dispatch ahead of queued background work.
- Separate protected/background queue-wait budgets from the three-second upstream
  execution timeout and classify queue expiry as `503 gateway_queue_timeout`.
- Retain the worker slot after caller cancellation or execution timeout until the real
  synchronous worker finishes, and add scheduler lifecycle metrics and an offline proof.
- Enforce absolute queue and execution deadlines at dispatch/completion boundaries, drain
  the scheduler on graceful shutdown, and propagate HTTP peer disconnect cancellation.
- Route optional order-book stream login through the same scheduler as bounded background
  work so it cannot acquire the token transaction ahead of queued protected reads.
- Raise the SDK's default whole-request timeout to 12 seconds so three serialized
  protected reads can use the server's seven-second queue budget plus execution budget.

## 0.3.0 - 2026-08-29

- Add authenticated, bounded recent order-book HTTP and WebSocket surfaces.
- Add venue-specific NASDAQ and NYSE order-book capture with immutable raw evidence,
  normalized snapshots, derived research metrics, and verified catalogs.
- Add an asynchronous atomic token transaction for bounded stream login without
  holding the event loop or token lock during recording.
- Record full-session candidate validation and successful live promotion evidence.

## 0.2.5 - 2026-08-22

- Honor recurring US-equity holidays and scheduled early closes when splitting
  historical sessions.
- Normalize crossed Schwab option markets and negative optional time-value analytics.
- Bound and cache complete single-expiration option chains for paper-trading consumers.
