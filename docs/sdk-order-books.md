# SDK order books

`schwab_gateway_sdk.GatewayMarketDataClient` reads the gateway's venue-specific
Level II books over HTTP and WebSocket. Both use your SDK API key, and every response is
validated against the versioned gateway models before it reaches you.

## Recent snapshots

```python
response = await client.get_recent_order_book("AAPL", venue="NASDAQ", limit=100)
```

## Live stream

Open the stream as an async context manager. Its connection closes cleanly however the
loop ends: normal completion, cancellation, an error, or an early `break`.

```python
import os

from schwab_gateway_sdk import GatewayMarketDataClient


async def consume_depth() -> None:
    async with GatewayMarketDataClient(
        os.environ["SCHWAB_GATEWAY_URL"],
        os.environ["SCHWAB_GATEWAY_API_KEY"],
    ) as client:
        async with client.stream_order_books(
            ["AAPL", "MSFT"], venue="NASDAQ"
        ) as snapshots:
            async for snapshot in snapshots:
                print(
                    snapshot.symbol,
                    snapshot.connection_id,
                    snapshot.continuity_epoch,
                    snapshot.sequence,
                    snapshot.bids[:1],
                    snapshot.asks[:1],
                )
```

### Inputs

- **Symbols:** trimmed, uppercased, and validated. They must be unique, with at most 25
  per subscription.
- **Venue:** `NASDAQ` or `NYSE`.

### Validation

Every `OrderBookStreamEnvelopeV1` message is validated. The stream raises an error on
malformed JSON, binary messages, an unexpected venue, or a symbol you did not request.

### Errors and reconnects

- The client never retries or reconnects on its own. A normal server close ends the
  loop.
- Auth, permission, capacity, timeout, transport, and contract failures raise the same
  `Gateway*Error` classes as the HTTP client.
- If you reconnect, use your own bounded retry policy. Treat `connection_id`,
  `continuity_epoch`, and `sequence` as continuity boundaries. This feed is not
  lossless and is not a consolidated tape.
