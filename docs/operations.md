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
reauthorization does, within about 5 minutes even with no traffic, because a background
probe re-reads the token document on that interval without affecting readiness. The
rules in `infra/alerts.yml` warn within 24 hours of expiry, when more than 1% of
protected queue waits exceed 1.5s with sufficient traffic, and when
post-timeout drains consume more than 2 worker-seconds in 5 minutes or stay active
for 1 minute. Alert rule deployment/reload requires separate production approval.

Consumer polling should be staggered, and retries bounded and jittered, in the
consumer's own repository. The SDK does not automatically retry failed reads.
The gateway retries eligible connection timeouts at most once within the original
budget, outside the worker/token transaction; 429 responses are never retried.
See [reliability evidence and rollout](runbooks/reliability-2026-10-21.md) for
classified failures, the shared local cooldown and exporter/rule ordering.

## DNS incident checks

When upstream connection failures coincide across unrelated services, compare the
host resolver with Docker's resolver from the affected containers:

```bash
python3 tools/probe_dns.py --container schwab_gateway_live --container turtlequant-bot
```

The standard-library-only script runs once, emits one JSON line per hostname and
environment, and exits nonzero if any lookup fails. Each lookup has a three-second
deadline, including Docker execution; use `--timeout-seconds` to override it. By
default it resolves `api.schwabapi.com` and `api.binance.com`; `--hostnames` selects
other names. It performs DNS lookups only and accesses no credentials. Running it
does not install a recurring monitor or alter DNS configuration.

For a transient incident, preserve the timestamped probe output alongside gateway
and consumer logs. A successful probe after recovery cannot prove which resolver
failed earlier. Have an authorized operator inspect `systemd-resolved` and kernel
journals for the same UTC window when system-journal access is unavailable. Resolver
configuration changes and recurring production monitoring require deployment review.

## Runbooks

- Deploy: [`runbooks/deploy.md`](runbooks/deploy.md)
- Roll back: [`runbooks/rollback.md`](runbooks/rollback.md)
- Full-session load test and option-chain TTL analysis:
  [`runbooks/full-session-load-test.md`](runbooks/full-session-load-test.md)
