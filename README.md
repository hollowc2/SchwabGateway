<p align="center">
  <img src="logo.jpg" alt="SchwabGateway" width="720">
</p>

# SchwabGateway

A read-only HTTP service that gives internal consumers safe, rate-bounded access to
Charles Schwab market data.

This repository also publishes two Python packages:

- **`schwab_gateway_sdk`**: the typed client for the gateway.
- **`schwab_token_store`**: shared storage for Schwab OAuth tokens.

The v1 API contract is defined in [`openapi.yaml`](openapi.yaml).

## Endpoints

| Route | Returns |
| --- | --- |
| `GET /health`, `/ready`, `/metrics` | Liveness, readiness, Prometheus metrics |
| `GET /v1/quotes` | Current quotes for one or more symbols |
| `GET /v1/spot` | Spot price for one symbol, with Schwab quote and trade timestamps |
| `GET /v1/chain` | Option-chain summary without contracts (kept for compatibility) |
| `GET /v1/option-chain` | Normalized contracts for one symbol and expiration (max 5,000) |
| `GET /v1/history` | Minute bars; `days_back` counts Eastern calendar days |
| `GET /v1/movers` | Market movers |
| `GET /v1/session-history` | Regular and extended session bars for a point in time |
| `GET /v1/order-book/recent` | Recent order-book snapshots for one venue |
| `GET /v1/order-book/stream` | Live order-book snapshots over WebSocket |

## Guides

- [SDK order books](docs/sdk-order-books.md): read recent snapshots and stream live
  depth from Python.
- [Order-book research](docs/order-book-research.md): record a `NASDAQ_BOOK` or
  `NYSE_BOOK` stream with a hashed evidence manifest.
- [Equity-data research](docs/equity-data-research.md): record `CHART_EQUITY` and
  Level I streams, and export one day of one-minute candles.

## Guarantees

- **Read-only.** There are no account, position, transaction, or order routes.
  `SCHWAB_GATEWAY_ORDER_WRITES_ENABLED` must stay `false`.
- **Fails closed.** During a feed outage, order-book and option-chain reads return an
  error instead of stale data. Option chains are never truncated.
- **Protected traffic first.** All Schwab calls go through one strict-priority FIFO
  scheduler and a single worker. Protected and background traffic have separate
  capacity, and background work is delayed or dropped before it can affect protected
  work. This includes order-book stream logins; only the open socket runs outside the
  scheduler.
- **Venue-specific depth.** `NASDAQ_BOOK` and `NYSE_BOOK` each show one venue's
  Level II book. Neither is consolidated market depth.
- **Chain caching is for paper trading only.** Complete chains are cached briefly per
  `(symbol, expiration)`: 4 seconds by default, 8 seconds in the production PAPER
  profile. Real-money workflows need a reviewed force-fresh policy.

### Error codes

| Status | Meaning |
| --- | --- |
| `429` | Capacity for your priority class is full |
| `503 gateway_queue_timeout` | The request waited too long to be dispatched |
| `504 upstream_timeout` | Schwab took longer than the 3-second budget |

### Adding consumers

Add paper strategies one at a time, and prove each one through a full session under
real collector and position-monitor load before adding the next. The contract tests do
not prove multi-consumer capacity.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run schwab-gateway-scheduler-proof
uv run schwab-gateway-recommend-option-chain-ttl --help
uv build
uv build --package schwab-gateway-sdk
uv build --package schwab-token-store
```

To run the demo profile, first issue a test-only key file:

```bash
uv run schwab-gateway-issue-keys \
  --output /tmp/schwab-gateway-demo-keys.json \
  --application-id demo-consumer \
  --capability market_data:read \
  --priority background

SCHWAB_GATEWAY_DEMO_KEYS_PATH=/tmp/schwab-gateway-demo-keys.json \
  docker compose --profile demo up --build
```

## Operations

- Deploy: [`docs/runbooks/helios.md`](docs/runbooks/helios.md)
- Roll back: [`docs/runbooks/rollback.md`](docs/runbooks/rollback.md)
- Full-session load test and option-chain TTL analysis:
  [`docs/runbooks/full-session-load-test.md`](docs/runbooks/full-session-load-test.md)

## Versioning

- The gateway, `openapi.yaml`, and the SDK share one version. It changes whenever the
  HTTP or SDK surface changes.
- The wire `schema_version` changes only for incompatible JSON changes.
- `schwab_token_store` has its own version because it can be installed separately.
