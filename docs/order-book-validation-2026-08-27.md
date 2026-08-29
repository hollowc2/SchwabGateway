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

## August 28 full-session candidate gate

The candidate at Git revision
`aa9d6e65a91c14eadf70df1c3da15101fb84d3f9`, running exact image
`sha256:f1d287294864c05b00ca201d1d86f8344f0d6f61121074982f92667468fec7f0`,
completed an AAPL/NASDAQ capture from 09:20 through 16:10 Eastern. This fills the
previously missing opening and closing validation windows without changing the
venue-specific scope of the result.

- Evidence directory:
  `/opt/schwab-order-book-evidence/gateway_candidate_AAPL_NASDAQ_2026-08-28`
- Capture window: `2026-08-28T13:20:00Z` through `2026-08-28T20:10:00Z`
- Exit code: 0
- Raw snapshots / malformed: 23,638 / 0
- Connections / reconnects: 1 / 0
- Subscriber drops / logged errors / container restarts: 0 / 0 / 0
- First / last gateway receipt:
  `2026-08-28T13:20:02.606178Z` / `2026-08-28T20:09:59.824185Z`
- Manifest SHA-256:
  `9892975dd29a3f7c7b9fcd032bc06d9820cddd799aeb80b8b427ec3dea5a59c3`
- Raw SHA-256:
  `45cf31fd950f1f0e477146d61a4ad43bd51d1bca18095b8c2a2c068d01adf8fa`

The derived validation windows are overlapping views of the immutable raw evidence:

- Opening, 09:20–10:00 Eastern: 2,149 rows; SHA-256
  `5bc20812f79b287262ca0a28099d87a54399549a5e55746a26844cbf5c490dc6`
- Regular session, 09:30–16:00 Eastern: 22,919 rows; SHA-256
  `75703fe305a4f39d9348238c0a8b353c488452833d0ce69409df63185b40d079`
- Closing, 15:30–16:10 Eastern: 2,091 rows; SHA-256
  `2db3c64a62af768e61e0d1e48048cb6cbd2f49f48b646e313f742bb7c2f3c5c8`

An independent read-only copy and parse reproduced every declared file hash and line
count. All 23,638 envelopes parsed as AAPL `NASDAQ_BOOK` snapshots with
`is_consolidated=false`, one connection ID, one continuity epoch, and monotonically
ordered gateway and event timestamps. The candidate remained healthy after the capture;
its metrics reported zero bounded-subscriber drops, and bounded logs contained one
initial stream connection with no reconnect or error event.

Five transient crossed snapshots were observed and are retained as a data-quality
limitation: 09:35:25 Eastern (1 cent), 15:06:24 (2 cents), 15:54:30 (10 cents),
15:54:40 (3 cents), and 15:58:45 (1 cent). Nine additional snapshots were locked. No
crossed or locked snapshot was discarded or rewritten.

Schwab omitted the snapshot-level sequence number in all 23,638 records. Participant
sequence fields do not substitute for a feed-level sequence, so the single connection,
zero reconnects, and zero local subscriber drops are not proof that Schwab delivered
every intermediate update. Gateway subscriber queues are bounded and may skip
intermediate snapshots under slow-consumer pressure. The result applies only to Schwab's
NASDAQ-specific book and must not be represented as consolidated US-equity depth.

**Candidate result: PASS.** The full-session candidate gate passed with the limitations
above; no order-write surface was enabled or exercised.

## Live promotion and rollback baseline

The read-only Helios preflight at `2026-08-29T00:17:51Z` found production healthy on
rollback image
`sha256:7ea140f43cf6e1bce8cc1c0328ca991060fb96e09e8f916c79e9972fc2aa2100`,
with zero restarts, token state `ready`, loopback-only port 8011, order writes disabled,
and Prometheus target `schwab-gateway:8011` up. The exact candidate image remained
healthy and isolated on loopback port 8012. The stopped emergency legacy container
`butterfly_schwab_gateway_live` and image
`sha256:6eb9f560effae529a2f578b5a4e5a1b0da2fd124cb4566fe9b097f01ec8b0ec8`
also remained available.

The default `/opt/schwab-gateway` checkout is a dirty detached `v0.1.0` worktree with
untracked production and candidate overlays, and its production Compose render does not
select the intended candidate image. The deployment gate therefore fails closed until
an exact clean release checkout is staged without overwriting those files and Corey
approves the named production recreate and rollback packet.

**Live result: PENDING EXPLICIT APPROVAL.** No production rebuild, restart, deployment,
monitoring change, or candidate action occurred during this validation update. The
primary rollback baseline is the exact healthy standalone image `sha256:7ea140f...`; the
preserved legacy image above is the emergency fallback.
