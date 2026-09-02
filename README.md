<p align="center">
  <img src="logo.jpg" alt="SchwabGateway" width="720">
</p>

# SchwabGateway

An internal, read-only HTTP service for bounded Charles Schwab market-data reads.
The repository also builds two standalone Python packages: `schwab_gateway_sdk`
(client) and `schwab_token_store` (token storage).

The v1 wire contract is defined in `openapi.yaml`.

## Endpoints

| Route | Purpose |
| --- | --- |
| `GET /health`, `GET /ready`, `GET /metrics` | Liveness, readiness, Prometheus metrics |
| `GET /v1/quotes` | Current quotes for one or more symbols |
| `GET /v1/spot` | Single-symbol spot with Schwab quote/trade timestamps |
| `GET /v1/chain` | Metadata-only option-chain summary (compatibility surface) |
| `GET /v1/option-chain` | Normalized contracts for one symbol + expiration (≤ 5000) |
| `GET /v1/history` | Minute bars; `days_back` counts Eastern calendar days |
| `GET /v1/movers` | Market movers |
| `GET /v1/session-history` | Exact regular/extended session bars for a point in time |
| `GET /v1/order-book/recent` | Authenticated recent venue order-book snapshots |
| `GET /v1/order-book/stream` | Authenticated read-only WebSocket order-book stream |

For offline research, the repo also ships a standalone equity order-book recorder
that captures one `NASDAQ_BOOK` or `NYSE_BOOK` stream with a hashed evidence
manifest. See `docs/order-book-research.md`.

## Safety boundaries

- **Read-only.** No account, position, transaction, or order-entry routes exist.
  `SCHWAB_GATEWAY_ORDER_WRITES_ENABLED` must remain false.
- **Fails closed.** Freshness-gated order-book and option-chain reads return errors
  during feed outages rather than serving stale or truncated data. The gateway never
  silently truncates a chain.
- **Bounded and protected-first.** One strict-priority FIFO scheduler feeds the single
  Schwab worker. Protected and background capacity are independent; background work is
  delayed or shed before it can consume ButterflyGuy capacity. `429` means class
  capacity is full, `503 gateway_queue_timeout` means dispatch wait expired, and `504
  upstream_timeout` means a dispatched operation exceeded its three-second budget.
- **Venue-specific depth.** `NASDAQ_BOOK` / `NYSE_BOOK` are Level II books for one
  venue, not consolidated market depth.
- **Chain cache is paper-only.** Successful full chains are cached for a fixed 4
  seconds per `(symbol, expiration)`. Any real-money workflow must use an explicitly
  reviewed force-fresh policy instead.
- Before promoting multiple paper strategies, stage one consumer at a time and prove
  a full session under real collector/position-monitor load; the contract tests do
  not establish multi-consumer capacity.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run schwab-gateway-scheduler-proof
uv build
uv build --package schwab-gateway-sdk
uv build --package schwab-token-store
```

Run the demo profile against a test-only key file:

```bash
uv run schwab-gateway-issue-keys \
  --output /tmp/schwab-gateway-demo-keys.json \
  --application-id demo-consumer \
  --capability market_data:read \
  --priority background

SCHWAB_GATEWAY_DEMO_KEYS_PATH=/tmp/schwab-gateway-demo-keys.json \
  docker compose --profile demo up --build
```

## Deployment & versioning

Production deployment and rollback are covered by `docs/runbooks/helios.md` and
`docs/runbooks/rollback.md`.

The gateway distribution, `openapi.yaml`, and the SDK are released together and share
a version whenever the HTTP or SDK surface changes. The wire `schema_version` moves
only on an incompatible JSON contract. `schwab_token_store` is versioned
independently because it can be installed on its own.
