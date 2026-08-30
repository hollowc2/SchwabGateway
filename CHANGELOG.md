# Changelog

## 0.3.0 - Unreleased

- Add authenticated, bounded recent order-book HTTP and WebSocket surfaces.
- Add venue-specific NASDAQ and NYSE order-book capture with immutable raw evidence,
  normalized snapshots, derived research metrics, and verified catalogs.
- Add an asynchronous atomic token transaction for bounded stream login without
  holding the event loop or token lock during recording.
- Record full-session candidate validation and successful live promotion evidence.

## 0.2.5 - 2026-08-22

- Honor recurring US-equity holidays and scheduled early closes when splitting
  historical sessions.
- Normalize crossed Schwab option markets and negative optional time-value analytics.
- Bound and cache complete single-expiration option chains for paper-trading consumers.
