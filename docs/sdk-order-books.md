# SDK order-book client

`schwab_gateway_sdk.GatewayMarketDataClient` owns the typed HTTP and WebSocket
transport for SchwabGateway's venue-specific Level II books. Both surfaces authenticate
with the SDK API key and validate the versioned gateway models before returning data.

Recent snapshots retain the existing SDK call:

```python
response = await client.get_recent_order_book("AAPL", venue="NASDAQ", limit=100)
```

Use the WebSocket stream as an async context manager so its single owned connection is
closed deterministically after normal completion, cancellation, failure, or partial
iteration:

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

Symbols are stripped, uppercased, validated, and required to be unique; a subscription
accepts at most 25. The venue is normalized to `NASDAQ` or `NYSE`. Every incoming
`OrderBookStreamEnvelopeV1` is validated, and the client fails closed on malformed JSON,
binary messages, an unexpected venue, or an unrequested symbol.

There is no automatic retry or reconnect. A normal server close ends iteration. Failed
authentication, authorization, capacity, timeout, transport, and contract conditions use
the same `Gateway*Error` classes as the HTTP client. Callers that reconnect must apply
their own bounded policy and treat `connection_id`, `continuity_epoch`, and `sequence` as
continuity boundaries; this venue-specific feed is not a lossless or consolidated tape.
