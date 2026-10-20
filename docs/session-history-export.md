# Session-history export

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
data, and it calls no account, position, transaction, or order methods.

Running it against a deployed gateway requires explicit operational approval.
