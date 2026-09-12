# Generic equity stream and candle evidence

These standalone tools own the generic chart, Level I, and session-candle evidence that
does not belong in a trading-strategy repository. They do not expose or call account,
position, transaction, or order methods.

## Capture chart and Level I streams

`schwab-gateway-capture-equity-streams` subscribes to `CHART_EQUITY` and
`LEVELONE_EQUITIES` for the same bounded symbol set. It uses the order-book recorder's
short atomic token-lock bootstrap: the token lock is held only for login, then released
before subscription handling. Reconnects use bounded backoff and new connection IDs.

```bash
uv run schwab-gateway-capture-equity-streams \
  --symbols AAPL,MSFT \
  --duration-seconds 600 \
  --output-root /absolute/path/to/equity-stream-research \
  --authorize-real-credential-read \
  --confirm-shared-token-bootstrap
```

The two explicit confirmations are mandatory because the command reads the approved
Schwab application/token configuration. Never place secret values on the command line.
The duration is one second through 24 hours and the normalized, unique symbol set is
capped at 25.

Every invocation creates a new mode-0700 run directory containing:

- `raw_frames.jsonseq`: matching websocket JSON texts preserved before schwab-py field
  relabeling;
- `relabeled_messages.ndjson`: receipt timestamp, service, and the relabeled handler
  payload;
- `connection_events.ndjson`: credential-free connection and retry boundaries;
- `manifest.json`: requested scope, counts, termination/failure status, relative paths,
  and SHA-256 hashes.

Level I is state update data containing the latest quote/trade fields; it is not a
complete time-and-sales tape. Chart updates are not a substitute for the REST session
history export below. Venue-specific Level II remains owned by the stricter recorder in
`order-book-research.md`.

## Export one-minute session candles

`schwab-gateway-export-session-history` uses only the pinned gateway SDK and an external
read-only gateway key. The key should identify a background-priority research consumer.
The command fetches both regular and extended segments, rejects stale or empty evidence,
sorts and deduplicates candles by timestamp, and refuses to overwrite an existing run.

```bash
uv run schwab-gateway-export-session-history AAPL 2026-09-11 \
  --output-root /absolute/path/to/equity-session-history
```

Supply `SCHWAB_GATEWAY_URL` and `SCHWAB_GATEWAY_API_KEY` through the approved secret
environment mechanism rather than copying either into evidence. Output is nested by
symbol, date, and retrieval timestamp and contains `candles_1m.json` plus `manifest.json`
with counts and a SHA-256 hash.

Historical session export is a gateway read, so it does not touch Schwab OAuth tokens or
account data. Running either tool against real credentials or a deployed gateway remains
an explicitly approved operational action.
