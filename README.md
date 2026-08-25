# SchwabGateway

SchwabGateway is an internal, read-only HTTP service for bounded Charles Schwab
market-data reads. The repository also builds `schwab_gateway_sdk` and
`schwab_token_store` as independent Python packages.

The v1 contract exposes only `GET /health`, `/ready`, `/metrics`, `/v1/quotes`,
`/v1/spot`, `/v1/chain`, `/v1/option-chain`, `/v1/history`, `/v1/movers`, and
`/v1/session-history`. There are no account, position, transaction, streaming, or order
routes, and
`SCHWAB_GATEWAY_ORDER_WRITES_ENABLED` must remain false.

`/v1/chain` remains the metadata-only compatibility surface. `/v1/option-chain` returns
normalized contracts for exactly one symbol and expiration, with a hard limit of 5000
contracts. The cap is above the observed 30-day maxima in ButterflyGuy snapshots (1120
NDX, 1000 SPX, and 650 XSP) while keeping response memory bounded. Oversized or malformed
chains fail closed; the gateway never silently truncates a strategy input. Spot responses
preserve Schwab quote/trade timestamps for freshness checks. Minute history `days_back`
means Eastern calendar days; exact historical regular/extended sessions use
`/v1/session-history`.

The full-chain contract also refuses empty or one-sided chains, non-finite numbers,
nonpositive strikes, negative prices, and crossed bid/ask markets. The metadata-only
route continues to report degenerate chain summaries for compatibility and diagnostics.

Successful normalized full chains are cached for a fixed, non-sliding three seconds per
exact `(symbol, expiration)` key, with same-key in-flight reads coalesced. Cached models
retain their original `gateway_received_at` and event timestamps; only age/stale fields
are reevaluated when served. Failures are never cached. Retention is bounded to 16 keys
and 64 MiB of serialized validated models, and can only be reduced with
`SCHWAB_GATEWAY_OPTION_CHAIN_CACHE_TTL_SECONDS` and
`SCHWAB_GATEWAY_OPTION_CHAIN_CACHE_MAX_ENTRIES`. Distinct cold-chain work is separately
bounded to four in-flight keys by default (maximum 16) with
`SCHWAB_GATEWAY_OPTION_CHAIN_MAX_INFLIGHT`; excess misses fail closed instead of queuing
behind the single credential worker.

The gateway serializes Schwab reads under the single token lock. Admission still bounds
the protected/background in-flight pools and full-chain requests return the standard
`429` (capacity) or `504` (upstream timeout) errors. Before promoting multiple paper
strategies, stage one consumer at a time and prove a full session under the intended
collector/position-monitor polling load; do not infer multi-consumer capacity from the
contract tests alone.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv build
uv build --package schwab-gateway-sdk
uv build --package schwab-token-store
```

Generate a test-only key file at a protected path, then point the demo profile at it:

```bash
uv run schwab-gateway-issue-keys \
  --output /tmp/schwab-gateway-demo-keys.json \
  --application-id demo-consumer \
  --capability market_data:read \
  --priority background
SCHWAB_GATEWAY_DEMO_KEYS_PATH=/tmp/schwab-gateway-demo-keys.json \
  docker compose --profile demo up --build
```

See `openapi.yaml`, `docs/runbooks/helios.md`, and `docs/runbooks/rollback.md`.
