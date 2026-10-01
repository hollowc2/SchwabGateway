# Order-book research

A standalone, read-only recorder for Schwab equity order books. Each run subscribes to
one service:

- `NASDAQ_BOOK`
- `NYSE_BOOK`

Each service shows one venue's depth. Never describe or analyze it as a consolidated US
equity book. Options books and time-and-sales are not supported.

## Safety

- The recorder uses the same atomic token manager as the gateway.
- Each login or reconnect holds the token lock only for the Schwab login handshake
  (8 seconds by default). The lock is released before subscribing or recording.
- Other token users keep running. A gateway request may wait briefly during a login.
- No account, position, transaction, or order methods are used.

## Record a run

`SCHWAB_API_KEY`, `SCHWAB_SECRET_KEY`, and an absolute `SCHWAB_TOKEN_PATH` must point to
the approved Schwab application and token. Never put these values on the command line
or in output.

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

- **Duration:** 1 second to 24 hours, measured from subscription start.
- **Symbols:** uppercased, unique, at most 25.
- **Output root:** must be absolute. Each run gets a new timestamped directory and never
  overwrites an existing one.

## Output files

Every completed run, and every run that fails after subscribing, contains:

| File | Contents |
| --- | --- |
| `raw_frames.jsonseq` | WebSocket JSON in RFC 7464 format, saved before schwab-py relabels numeric fields |
| `normalized_snapshots.ndjson` | Validated snapshots: venue, service, sequence, timestamps, price levels, sizes, participants, connection ID, continuity epoch |
| `connection_events.ndjson` | Connections, failures, and retries, without credentials |
| `manifest.json` | Scope, UTC time range, display timezone, file paths, SHA-256 hashes, and quality counts |

The manifest's quality counts cover events, malformed messages, sequence gaps, missing
sequences, duplicates, out-of-order messages, drops, reconnects, and continuity epochs,
plus the termination reason. Paths are relative, so the manifest stays valid when the
directory is moved or viewed from outside a container.

**Reading a run**

- Raw and normalized files are kept separate. Never overwrite the raw file with
  normalized data.
- No manifest means the stream never started a capture.
- Any termination reason other than `completed` means the evidence is partial.

## Interpretation limits

- Book messages are snapshots. They are not trades or executable orders.
- Sequence gaps are flagged on the affected snapshot and in the manifest. Missing depth
  is never filled in.
- Continuity resets at every reconnect. The first snapshot after a reconnect is flagged,
  and sequence checks restart in the new epoch.
- If Schwab omits a sequence number, the snapshot is flagged `missing_sequence` and
  `sequence_continuity_observable` is `false`. In that case, zero gaps does not prove
  continuity.
- Empty sides and participant total/count mismatches are kept and flagged.
- Malformed snapshots are left out of the normalized file, counted in the manifest, and
  kept in the raw file.
- History exists only for time you recorded live. There is no backfill.

## Derive research datasets

Derivation checks the capture's normalized SHA-256 and row count, then writes to a new
directory. It computes spread, midpoint, top-of-book microprice, depth, imbalance,
midpoint movement, and add/remove rates. The rates are inferred from consecutive
snapshots; they are not exchange order events.

```bash
uv run schwab-gateway-derive-order-books \
  --capture-manifest /evidence/run/manifest.json \
  --output-directory /evidence/run/derived_v1 \
  --depth-levels 10
```

The derived manifest pins the hashes of both the source manifest and the normalized
file. Liquidity/price correlations are labeled descriptive, not causal.

## Catalog and retention

Refreshing the catalog verifies the hashes of the raw, normalized, and connection-event
files. Captures older than the threshold are marked `archive_copy_then_verify`. Nothing
is deleted or rewritten. Deletion is a separate, approved step after archive hashes are
verified.

```bash
uv run schwab-gateway-catalog-order-books \
  --evidence-root /evidence \
  --output /evidence/catalog.json \
  --archive-after-days 30
```

## Serving order books from the gateway

The live feed is off by default. To turn it on, set one venue and up to 25 symbols:

```text
SCHWAB_GATEWAY_ORDER_BOOK_STREAM_ENABLED=true
SCHWAB_GATEWAY_ORDER_BOOK_STREAM_VENUE=NASDAQ
SCHWAB_GATEWAY_ORDER_BOOK_STREAM_SYMBOLS=AAPL,MSFT
```

| Route | Behavior |
| --- | --- |
| `GET /v1/order-book/recent?symbol=AAPL&venue=NASDAQ&limit=100` | Recent snapshots, oldest first |
| `/v1/order-book/stream?symbols=AAPL&venue=NASDAQ` | Upgrades to a WebSocket |

- **Auth:** `X-Internal-API-Key` with the `market_data:read` capability.
- **Slow clients:** subscriber queues are bounded, so a slow client can miss snapshots.
  Use the continuity fields to detect gaps.
- **Stale data:** recent reads return `503` if the feed is disconnected, has no snapshot
  for the symbol, or its newest snapshot is older than the max age (15 seconds by
  default).
- **Capacity:** each WebSocket holds a slot in the protected or background pool while it
  is open. When the pool is full, new connections get `429`.

For a Python client, see [SDK order books](sdk-order-books.md).
