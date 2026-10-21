# Operations

Notes for running SchwabGateway in production. Start with the
[README](../README.md) quickstart.

## Adding consumers

Add paper strategies one at a time, and prove each one through a full session under
real collector and position-monitor load before adding the next. The contract tests do
not prove multi-consumer capacity.

## Timeout and reliability controls

`SCHWAB_GATEWAY_UPSTREAM_TIMEOUT_SECONDS` is the dispatched execution budget (3s
by default). It does not include queue wait and does not cancel running Schwab work.
`SCHWAB_GATEWAY_HTTP_TIMEOUT_SECONDS` controls HTTP read/write/pool timeouts; when
unset outside Compose, it follows the execution budget. The optional
`SCHWAB_GATEWAY_HTTP_CONNECT_TIMEOUT_SECONDS` overrides connection establishment
and must not exceed the HTTP timeout. Compose defaults these HTTP settings to 3s
and 1s respectively. HTTP timeouts apply per network phase, and read timeouts per
chunk, not as a hard total-operation deadline. Reducing them can produce earlier
upstream failures; raising them can hold the single worker longer after a 504.

`schwab_gateway_scheduler_timeout_drain_seconds` measures operation time beyond its
execution deadline, with eventual outcome labels. The matching
`schwab_gateway_scheduler_timeout_drain_active` gauge stays at 1 while a timed-out
operation still owns the worker. Completion logs report `timeout_drain_ms` and
`execution_timeout_seconds`; a late successful fetch can still populate the cache.

`schwab_gateway_refresh_token_expires_timestamp_seconds` reports the last validated
authorization creation time plus the refresh-token lifetime, not access-token expiry.
Zero means unknown. Access-token refresh does not move this deadline; interactive
reauthorization does. The rules in `infra/alerts.yml` warn within 24 hours of expiry,
when more than 1% of protected queue waits exceed 1.5s with sufficient traffic, and when
post-timeout drains consume more than 2 worker-seconds in 5 minutes or stay active
for 1 minute. Alert rule deployment/reload requires separate production approval.

Consumer polling should be staggered, and retries bounded and jittered, in the
consumer's own repository. The SDK does not automatically retry failed reads.
The gateway retries eligible connection timeouts at most once within the original
budget, outside the worker/token transaction; 429 responses are never retried.
See [reliability evidence and rollout](runbooks/reliability-2026-10-21.md) for
classified failures, the shared local cooldown and exporter/rule ordering.

## Runbooks

- Deploy: [`runbooks/deploy.md`](runbooks/deploy.md)
- Roll back: [`runbooks/rollback.md`](runbooks/rollback.md)
- Full-session load test and option-chain TTL analysis:
  [`runbooks/full-session-load-test.md`](runbooks/full-session-load-test.md)
