# Order-book validation — 2026-08-27

This validation used committed feature code on `codex/additional-features` and real
Schwab market data on Helios. It did not deploy, rebuild, or restart either running
gateway. Live and candidate remained healthy on image
`sha256:7ea140f43cf6e1bce8cc1c0328ca991060fb96e09e8f916c79e9972fc2aa2100`.

All times below are UTC; the capture manifests specify `America/New_York` as the display
timezone. Schwab `NASDAQ_BOOK` and `NYSE_BOOK` are venue-specific, not consolidated.

## Shared-token proof

A 30-second AAPL/NASDAQ capture completed while the gateways remained up. The candidate
continued serving authenticated `afterhours-lab` quote requests with HTTP 200 responses
during subsequent AAPL and IBM stream logins. This validates the intended operating
boundary: a short token transaction for login followed by lock-free WebSocket recording.

- Evidence directory:
  `/opt/schwab-order-book-evidence/schwab_nasdaq_book_AAPL_20260827T172043.991508Z`
- Raw frames: 28
- Normalized snapshots: 27
- Connections/reconnects: 1/0
- Raw SHA-256: `5ff189bf8428f54440d859c94460d0d54acbf9540a007b74458c292df2046d4a`
- Derived rows: 27
- Derived metrics SHA-256:
  `ae7f211cc3b187ad0190e0ac2559252e69a237118ae0cd79eea71dfffd462893`

## Fifteen-minute midday matrix

### AAPL / NASDAQ

- Evidence directory:
  `/opt/schwab-order-book-evidence/schwab_nasdaq_book_AAPL_20260827T172128.977467Z`
- Started: `2026-08-27T17:21:28.977467+00:00`
- Raw frames / normalized snapshots: 886 / 886
- Malformed / dropped: 0 / 0
- Connections / reconnects: 1 / 0
- Termination: `completed`
- Raw SHA-256: `a586f3f22337391aba535637ffb558a7123def8564d83677a09f28112dddbf6a`
- Normalized SHA-256:
  `75311965e20625e705d23b279a647f3b7461f65d11e43d45e24bab0fe00a4873`
- Derived rows: 886
- Derived metrics SHA-256:
  `a77e9e1404a954047099d6053879344ea4d2750b045e660c51839efae1202f3a`
- Catalog status: `verified`

### IBM / NYSE

- Evidence directory:
  `/opt/schwab-order-book-evidence/schwab_nyse_book_IBM_20260827T172159.294243Z`
- Started: `2026-08-27T17:21:59.294243+00:00`
- Raw frames / normalized snapshots: 581 / 581
- Malformed / dropped: 0 / 0
- Connections / reconnects: 1 / 0
- Termination: `completed`
- Raw SHA-256: `d3cab91eda643bfed69fc569907c5f74a7a62c5f374939c84d6330d1b83d81d7`
- Normalized SHA-256:
  `ddbaba0f8fbae42a91975056cc666ff74d8438a92fdd3d9f70644e1895693c91`
- Derived rows: 581
- Derived metrics SHA-256:
  `25b7c9046f97d5ea73c75ab4d800a04e58b6c5ec1e65dba1e553b16a9c647bdc`
- Catalog status: `verified`

The non-destructive catalog is
`/opt/schwab-order-book-evidence/catalog_20260827_midday.json`.

Schwab omitted per-symbol sequence values from every snapshot in both long captures.
Consequently, `sequence_continuity_observable` is false even though no reconnect occurred.
This must not be reported as proof that no messages were missed.

## Authenticated consumer smoke

A temporary loopback-only gateway built from feature commit `62ddc9f` subscribed to
AAPL/NASDAQ. The recent endpoint returned three snapshots with `is_consolidated=false`,
`connection_id=1`, and `missing_sequence`. An authenticated WebSocket client then
received one `order_book_snapshot` envelope for AAPL/NASDAQ. The temporary container was
stopped and auto-removed; both existing gateways remained healthy and unchanged.

## Temporal coverage still required

These completed samples cover the midday session. Opening and closing depth cannot be
backfilled: those two validation cells require new live captures during future 09:30 and
16:00 Eastern windows. Until those captures exist and their catalogs verify, performance
or quality conclusions must be scoped to midday only.
