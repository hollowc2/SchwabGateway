# SchwabGateway

SchwabGateway is an internal, read-only HTTP service for bounded Charles Schwab
market-data reads. The repository also builds `schwab_gateway_sdk` and
`schwab_token_store` as independent Python packages.

The v1 contract exposes only `GET /health`, `/ready`, `/metrics`, `/v1/quotes`,
`/v1/spot`, and `/v1/chain`. There are no account, position, transaction, streaming, or
order routes, and `SCHWAB_GATEWAY_ORDER_WRITES_ENABLED` must remain false.

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
