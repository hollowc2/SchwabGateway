# Schwab order-book research capture

The order-book recorder is a standalone, read-only evidence collector. It subscribes to
exactly one Schwab equity book service per run:

- `NASDAQ_BOOK`
- `NYSE_BOOK`

The resulting depth is venue-specific. It must not be described or analyzed as a
consolidated US-equity order book. Options books and time-and-sales are outside this first
contract.

## Safety boundary

The recorder uses the same atomic token manager as the HTTP gateway and holds its
exclusive token lock from stream login through logout. Stop the HTTP gateway and ensure
that no other process uses the same token file before starting a capture. Both confirmation
flags are mandatory; they document this operating decision but do not stop other
processes automatically.

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
  --confirm-exclusive-token-lock
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
  timestamps, price levels, aggregate size, and participant contributions.
- `manifest.json`: provider, requested scope, actual UTC range, display timezone,
  manifest-relative evidence paths, SHA-256 hashes, event counts, malformed counts,
  sequence gaps, missing sequences, duplicates/out-of-order observations, drops, and
  termination reason. Relative paths keep the manifest valid when a container-mounted
  capture directory is viewed from its host or moved intact.

Raw and normalized data are intentionally separate. Never repair or replace the raw file
with normalized output. A missing manifest means the stream did not progress far enough
to establish a capture run; a non-`completed` termination reason means the evidence is
partial and must be treated accordingly.

## Interpretation limits

- Schwab book messages are treated as snapshots, not trade prints or executable orders.
- A sequence gap is disclosed in both the affected normalized snapshot and manifest; no
  missing depth is synthesized.
- If Schwab omits a per-symbol sequence, the snapshot is flagged `missing_sequence`, the
  manifest counts it, and `sequence_continuity_observable` is false. Zero detected gaps
  must not be interpreted as proof of continuity in that case.
- Empty sides and participant total/count mismatches are retained with quality flags.
- Structurally malformed snapshots are excluded from normalized output, counted in the
  manifest, and remain available in the raw evidence.
- Historical depth exists only for intervals captured live. This feature does not backfill
  an order book.
