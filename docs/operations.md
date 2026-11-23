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

The optional user units in `infra/systemd/schwab-gateway-dns-probe.{service,timer}`
are prepared for Helios's existing `billy` user manager (`Linger=yes`). They run
the probe 15 seconds after the previous run finishes, with a 25-second service
deadline. Runs do not overlap. Each probe emits six JSON lines into the user's
existing journal. A failed lookup exits nonzero and marks that run failed; the
timer continues scheduling later runs. This captures evidence without delivering
external alerts. The units have not been enabled by this preparation.

After approval to enable `schwab-gateway-dns-probe.timer` on Helios:

1. Record the gateway/trading container identities, existing unit/script bytes
   (or their absence), and timer enabled/active states in a timestamped rollback
   directory. Use the approved source commit for all three files.
2. Install the probe at
   `~/.local/share/schwab-gateway-dns-monitor/probe_dns.py` and the two units in
   `~/.config/systemd/user/`, preserving any previous versions in the rollback
   directory. Validate with `systemd-analyze --user verify` before enabling.
3. Run `systemctl --user daemon-reload`, then
   `systemctl --user enable --now schwab-gateway-dns-probe.timer`. Verify at least
   two completed runs and inspect their JSON with
   `journalctl --user -u schwab-gateway-dns-probe.service -o cat`.
4. Confirm the gateway remains ready and the recorded application containers
   retain their identities and restart counts. Observe user-journal usage and
   the next DNS incident; an ordinary successful run proves current resolution
   only. Existing journald retention controls apply.

Rollback: disable and stop this timer with
`systemctl --user disable --now schwab-gateway-dns-probe.timer`, stop an active
probe service, restore any saved prior unit/script bytes and their prior timer
states, and reload the user manager. If all three files were previously absent,
the disabled inert files may remain; no file deletion or application restart is
needed. Installing the capture timer does not change resolver configuration.

## Schwab throttling

The gateway never retries a Schwab 429. It fails the read with
`503 upstream_rate_limited` and a `Retry-After` header, then sheds further reads in
the same Schwab family locally until the cooldown ends. The cooldown is Schwab's
`Retry-After`, or 1s when Schwab omits or garbles the header. A long value is passed
on unshortened; only one above 3600s is capped, so a bad header cannot shed a family
until restart. No Schwab 429 through 2026-11-22 asked for more than 1s.

| Family | Gateway operations |
| --- | --- |
| `chains` | `option_chain`, `chain_metadata` |
| `quotes` | `quotes`, `spot` |
| `price_history` | `history`, `session_history` |
| `movers` | `movers` |

Evidence for family scope: every Schwab 429 from 2026-11-07 to 11-22 (36) was an
option chain, at three to five upstream calls in the 10s before each one. No quote,
spot or history read on the same key was throttled. The shared cooldown that this
replaced shed at least 14 arriving spot and quote reads with 503s during those
chain cooldowns. Assumptions: spot and batch quotes share a family, and no Schwab
429 so far has told them apart. A second family throttled while another is still
cooling is taken as account-wide throttling and cools every family. Schwab's
published limits sit behind the developer portal; re-check this table against them
when they are available.

Signals that look alike:

- `schwab_gateway_upstream_failures_total{cause="rate_limited"}`: real Schwab 429s.
- `schwab_gateway_read_recovery_total{outcome="cooldown_rejected"}`: reads the gateway
  shed locally during a cooldown, before or at dispatch. No Schwab call was made.
- `gateway_capacity_exceeded` (HTTP 429), `schwab_gateway_scheduler_capacity_rejections_total`:
  the caller's bounded pool was full. Unrelated to Schwab.
- The `gateway_upstream_cooldown_armed` log line names the operation, the scope armed
  and whether Schwab sent `Retry-After`.

ButterflyGuy (SDK 0.8.0 pinned) retries a failed read once after 250ms. That retry
always lands inside the 1s minimum cooldown and is shed locally, so one Schwab 429
shows as two protected 503s. Direct-mode SPX shares the gateway's Schwab app key and
token file but not its scheduler or cooldowns; the gateway cannot see its 429s.

## Protected-failure alert rollout

`SchwabGatewayProtectedRequestFailure` reads `gateway_priority_requests_total`, which
first ships in v0.8.4. Roll out in this order:

1. Deploy the gateway image. The rule file on Helios keeps working because
   `gateway_client_requests_total` is unchanged.
2. Confirm the new series is scraped:
   `gateway_priority_requests_total{priority_class="protected",status="503"}`.
3. Install `infra/alerts.yml` to `/opt/monitoring/prometheus-alerts/schwab-gateway.yml`,
   validate with `promtool check rules`, and reload Prometheus. This also brings the
   three-in-ten-minutes thresholds from #59, which were never installed.

If the rules land before the image, the request rule has no data and stays silent.
`SchwabGatewayProtectedSchedulerFailure` still pages on protected queue timeouts,
upstream timeouts and pool rejections, but Schwab 429s and other upstream 503s go
unpaged until the image is deployed. To roll back the image, restore the previous rule
file first.

The two protected rules overlap by design. A protected upstream timeout is both a 504
and a scheduler upstream timeout, so a timeout cluster pages twice. A cluster of Schwab
429s pages only the request rule, and protected pool rejections (HTTP 429) page only
the scheduler rule.

## Deprecated: `/v1/chain`

The chain summary repeats counts that `/v1/option-chain` already returns. It had no
production traffic between 2026-11-05 and 2026-11-22. Its only known caller is
ButterflyGuy's shadow-read comparator (`gateway_client/shadow.py`), which is disabled
(`SCHWAB_GATEWAY_SHADOW_READS=false`) and is a cutover-validation and rollback tool.
`schwab_gateway_sdk.chain_metadata.extract_chain_metadata` is not deprecated; the
full-chain normalizer uses it.

To retire it:

1. Point ButterflyGuy's shadow comparator at `/v1/option-chain` counts, or retire
   shadow mode, and release that.
2. Watch `gateway_request` logs and `gateway_client_requests_total{operation="chain_v1"}`
   for zero calls across a full month, including a monthly backfill on the 1st and a
   quarterly expiration.
3. Remove the route and its upstream. Remove `get_chain_metadata` and the chain-metadata
   models from the SDK in a minor release only after no pinned consumer imports them.

## Runbooks

- Deploy: [`runbooks/deploy.md`](runbooks/deploy.md)
- Roll back: [`runbooks/rollback.md`](runbooks/rollback.md)
- Full-session load test and option-chain TTL analysis:
  [`runbooks/full-session-load-test.md`](runbooks/full-session-load-test.md)
