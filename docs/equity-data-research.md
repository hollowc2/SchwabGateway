# Equity-data research

Two standalone tools for collecting general equity evidence: live chart and Level I
streams, and one-minute session candles. Neither tool calls account, position,
transaction, or order methods.

## Record chart and Level I streams

`schwab-gateway-capture-equity-streams` subscribes to `CHART_EQUITY` and
`LEVELONE_EQUITIES` for the same symbols.

```bash
uv run schwab-gateway-capture-equity-streams \
  --symbols AAPL,MSFT \
  --duration-seconds 600 \
  --output-root /absolute/path/to/equity-stream-research \
  --authorize-real-credential-read \
  --confirm-shared-token-bootstrap
```

- **Confirmation flags are required** because the command reads the approved Schwab
  application and token. Never put secrets on the command line.
- **Duration:** 1 second to 24 hours.
- **Symbols:** uppercased, unique, at most 25.
- **Token lock:** held only during login, then released before subscribing (the same
  approach as the order-book recorder). Reconnects use bounded backoff and get a new
  connection ID.

Each run creates a new directory (mode `0700`) containing:

| File | Contents |
| --- | --- |
| `raw_frames.jsonseq` | WebSocket JSON exactly as received, before schwab-py relabels fields |
| `relabeled_messages.ndjson` | Receipt time, service, and the relabeled payload |
| `connection_events.ndjson` | Connection and retry events, without credentials |
| `manifest.json` | Requested scope, counts, final status, relative paths, and SHA-256 hashes |

**Limits**

- Level I carries the latest quote and trade fields. It is not a full time-and-sales
  tape.
- Chart updates do not replace the session-candle export below.
- For venue-specific Level II, use the [order-book recorder](order-book-research.md).

## Export one-minute session candles

`schwab-gateway-export-session-history` reads regular and extended session candles for
one symbol and date through the gateway SDK.

```bash
uv run schwab-gateway-export-session-history AAPL 2026-09-11 \
  --output-root /absolute/path/to/equity-session-history
```

- Set `SCHWAB_GATEWAY_URL` and `SCHWAB_GATEWAY_API_KEY` through the approved secret
  mechanism. Don't copy either into evidence.
- Use a read-only key for a background-priority research consumer.
- Stale or empty data is rejected. Candles are sorted and de-duplicated by timestamp.
- Output goes to `<symbol>/<date>/<retrieval time>/` and contains `candles_1m.json` and
  a `manifest.json` with counts and a SHA-256 hash. Existing runs are never overwritten.

This export only calls the gateway, so it never touches Schwab OAuth tokens or account
data.

Running either tool against real credentials or a deployed gateway requires explicit
operational approval.
