# Changelog

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
