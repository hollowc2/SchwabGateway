# Schwab order-book research capture

The order-book recorder is a standalone, read-only evidence collector. It subscribes to
exactly one Schwab equity book service per run:

- `NASDAQ_BOOK`
- `NYSE_BOOK`

The resulting depth is venue-specific. It must not be described or analyzed as a
consolidated US-equity order book. Options books and time-and-sales are outside this first
contract.

## Safety boundary

The recorder uses the same atomic token manager as the HTTP gateway. Each initial login
or reconnect holds the exclusive token lock only for a bounded Schwab login handshake
(eight seconds by default). Token callbacks are invalidated and the lock is released
before subscription handling or recording begins. Other token consumers can continue to
run, although an HTTP request may briefly wait behind that login transaction.

No account, position, transaction, or order method is used or exposed.

## Capture one bounded run

The existing `SCHWAB_API_KEY`, `SCHWAB_SECRET_KEY`, and absolute `SCHWAB_TOKEN_PATH`
environment settings must identify the approved Schwab application and token. Do not put
those values on the command line or in capture output.

```bash
uv run schwab-gateway-capture-order-books \
  --venue NASDAQ \
  --symbols AAPL,MSFT \
  --duration-seconds 600 \
  --output-root /absolute/path/to/order-book-research \
  --display-timezone America/New_York \
  --authorize-real-credential-read \
  --confirm-shared-token-bootstrap \
  --max-reconnects 3
```

The duration is measured from subscription startup and must be between one second and 24
hours. Symbols are normalized to uppercase, must be unique, and are capped at 25 per run.
The output root must be absolute. Each invocation creates a new timestamped directory and
refuses to overwrite an existing run.

## Evidence layout

Each successful or post-subscription failed run contains:

- `raw_frames.jsonseq`: relevant websocket JSON texts using RFC 7464 record separators;
  each Schwab frame is preserved before schwab-py relabels numeric fields.
- `normalized_snapshots.ndjson`: validated research models with venue, service, sequence,
  timestamps, price levels, aggregate size, participant contributions, connection ID,
  and continuity epoch.
- `connection_events.ndjson`: credential-free connection, failure, and retry boundaries.
- `manifest.json`: provider, requested scope, actual UTC range, display timezone,
  manifest-relative evidence paths, SHA-256 hashes, event counts, malformed counts,
  sequence gaps, missing sequences, duplicates/out-of-order observations, drops, and
  termination reason, reconnect counts, and continuity epoch counts. Relative paths keep
  the manifest valid when a container-mounted
  capture directory is viewed from its host or moved intact.

Raw and normalized data are intentionally separate. Never repair or replace the raw file
with normalized output. A missing manifest means the stream did not progress far enough
to establish a capture run; a non-`completed` termination reason means the evidence is
partial and must be treated accordingly.

## Interpretation limits

- Schwab book messages are treated as snapshots, not trade prints or executable orders.
- A sequence gap is disclosed in both the affected normalized snapshot and manifest; no
  missing depth is synthesized.
- Continuity never crosses a reconnect boundary. The first snapshot after a reconnect is
  flagged and sequence comparisons restart inside its new epoch.
- If Schwab omits a per-symbol sequence, the snapshot is flagged `missing_sequence`, the
  manifest counts it, and `sequence_continuity_observable` is false. Zero detected gaps
  must not be interpreted as proof of continuity in that case.
- Empty sides and participant total/count mismatches are retained with quality flags.
- Structurally malformed snapshots are excluded from normalized output, counted in the
  manifest, and remain available in the raw evidence.
- Historical depth exists only for intervals captured live. This feature does not backfill
  an order book.

## Derived research datasets

Derivation first verifies the capture's normalized SHA-256 and row count, then creates a
new non-overwriting directory. It computes spread, midpoint, top-level microprice, depth,
imbalance, midpoint movement, and snapshot-delta add/removal rates. Those rates are
explicitly inferred from adjacent snapshots; they are not exchange order events.

```bash
uv run schwab-gateway-derive-order-books \
  --capture-manifest /evidence/run/manifest.json \
  --output-directory /evidence/run/derived_v1 \
  --depth-levels 10
```

The derived manifest pins both the source manifest and normalized evidence hashes and
labels liquidity/price correlations as descriptive, not causal.

## Catalog and retention plan

Catalog refreshes verify raw, normalized, and connection-event hashes. The retention rule
only marks older captures as `archive_copy_then_verify`; it never deletes or rewrites a
capture. Any later deletion remains a separately approved operation after archive hashes
are verified.

```bash
uv run schwab-gateway-catalog-order-books \
  --evidence-root /evidence \
  --output /evidence/catalog.json \
  --archive-after-days 30
```

## Gateway recent snapshots and WebSocket

The live feed is opt-in. Configure one venue and at most 25 symbols:

```text
SCHWAB_GATEWAY_ORDER_BOOK_STREAM_ENABLED=true
SCHWAB_GATEWAY_ORDER_BOOK_STREAM_VENUE=NASDAQ
SCHWAB_GATEWAY_ORDER_BOOK_STREAM_SYMBOLS=AAPL,MSFT
```

`GET /v1/order-book/recent?symbol=AAPL&venue=NASDAQ&limit=100` returns oldest-to-newest
bounded snapshots. `/v1/order-book/stream?symbols=AAPL&venue=NASDAQ` upgrades to a
WebSocket. Both use the existing `X-Internal-API-Key` authentication and
`market_data:read` capability. Subscriber queues are bounded; a slow client may skip
intermediate snapshots and must use continuity fields rather than assuming losslessness.
Recent reads fail closed with `503` when the configured feed is disconnected, has no
snapshot for the symbol, or the newest in-memory snapshot exceeds the configured maximum
age (15 seconds by default). WebSocket connections use separate protected/background
capacity pools held for each socket's complete lifetime; excess upgrades receive `429`.

The first live shared-token, venue, derivation, catalog, and consumer smoke results are
recorded in `order-book-validation-2026-08-27.md`.
