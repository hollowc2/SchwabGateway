<p align="center">
  <img src="logo.jpg" alt="SchwabGateway" width="720">
</p>

# SchwabGateway

**One process owns your Schwab credentials, and every bot reads market data through it.**

If you run more than one script against the Schwab API, such as a scanner, a
backtester's live feed, and a couple of paper-trading bots, they end up competing for
the same rate limit and the same OAuth token. Two processes refreshing at once can
corrupt `tokens.json`, and a chatty background job can starve the bot that matters.

SchwabGateway is a small, read-only HTTP service that sits between your programs and
Schwab:

- **One token owner.** Refreshes are locked and atomic, so concurrent refreshes can't
  corrupt the token file.
- **Priority scheduling.** Each client key is `protected` or `background`; background
  work is delayed or dropped before it can slow protected work.
- **Read-only by design.** There are no account, position, or order routes.
- **Fails closed.** During an outage you get an error, not stale data.
- **Typed Python SDK**, an OpenAPI contract, and Prometheus metrics.

### How does this relate to schwab-py?

SchwabGateway is built on [schwab-py](https://github.com/alexgolec/schwab-py) and uses
the same token file format. schwab-py is a client library for one program.
SchwabGateway is for when several programs need Schwab data at once: they call the
gateway over HTTP with their own keys and never touch the Schwab credentials.

## Quickstart

You need Docker, [uv](https://docs.astral.sh/uv/), and a Schwab developer app from
[developer.schwab.com](https://developer.schwab.com) with Market Data access and a
callback URL (for example `https://127.0.0.1:8182`).

**1. Clone and install.**

```bash
git clone https://github.com/hollowc2/SchwabGateway.git
cd SchwabGateway
uv sync
```

**2. Create the initial token** with schwab-py's manual login flow. It prints a URL; log
in, then paste back the URL you were redirected to.

```bash
export SCHWAB_API_KEY=<app key> SCHWAB_SECRET_KEY=<app secret>
mkdir -p ~/schwab-tokens
uv run python -c "
import schwab, os
schwab.auth.client_from_manual_flow(
    os.environ['SCHWAB_API_KEY'], os.environ['SCHWAB_SECRET_KEY'],
    'https://127.0.0.1:8182', os.path.expanduser('~/schwab-tokens/tokens.json'))
"
```

Schwab refresh tokens last 7 days, so repeat this step weekly. The
`schwab_gateway_refresh_token_expires_timestamp_seconds` metric tells you when.

**3. Issue a client key** for each program that will call the gateway:

```bash
uv run schwab-gateway-issue-keys \
  --output ~/schwab-gateway-keys.json \
  --application-id my-scanner \
  --capability market_data:read \
  --priority protected
```

The keys file stores only digests. The plaintext key is printed once, so save it.

**4. Start the gateway** from the same shell, so it sees the exported credentials.
Both files from steps 2 and 3 are private to your user, so run the container as you:

```bash
cp .env.example .env
# Edit .env:
#   SCHWAB_GATEWAY_TOKEN_DIR=/home/<you>/schwab-tokens
#   SCHWAB_GATEWAY_KEYS_PATH=/home/<you>/schwab-gateway-keys.json
#   SCHWAB_GATEWAY_UID=<output of id -u>
#   SCHWAB_GATEWAY_GID=<output of id -g>
docker compose --profile live up -d --build
```

**5. Read some data.**

```bash
KEY=<plaintext key from step 3>
curl -H "X-Internal-API-Key: $KEY" "http://127.0.0.1:8011/v1/quotes?symbols=AAPL,SPY"
```

Or from Python:

```python
import asyncio
from schwab_gateway_sdk import GatewayMarketDataClient

async def main() -> None:
    async with GatewayMarketDataClient("http://127.0.0.1:8011", "<your key>") as client:
        print(await client.get_quotes(["AAPL", "SPY"]))

asyncio.run(main())
```

To try the API without Schwab credentials, see the demo profile under
[Development](#development).

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

## Guarantees

`/v1/quotes` accepts the exact padded 21-character OCC option symbols returned by
Schwab option chains. Spaces are preserved upstream; other symbol routes keep
their ticker validation. Targeted option bid/ask freshness follows the quote
timestamp and the 300-second option-contract policy. A successful request or a
newer last trade does not refresh an old option quote. Equity quote policy is
unchanged. `allow_partial=true` names missing contracts in `missing_symbols`.

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

## Guides

- [SDK order books](docs/sdk-order-books.md): read recent snapshots and stream live
  depth from Python.
- [Order-book research](docs/order-book-research.md): record a `NASDAQ_BOOK` or
  `NYSE_BOOK` stream with a hashed evidence manifest.
- [Equity-data research](docs/equity-data-research.md): record `CHART_EQUITY` and
  Level I streams, and export one day of one-minute candles.

## Running in production

Timeouts, alerting, adding consumers, and deploy/rollback runbooks are in
[docs/operations.md](docs/operations.md).

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

## Versioning

- The gateway, `openapi.yaml`, and the SDK share one version. It changes whenever the
  HTTP or SDK surface changes.
- The wire `schema_version` changes only for incompatible JSON changes.
- `schwab_token_store` has its own version because it can be installed separately.
