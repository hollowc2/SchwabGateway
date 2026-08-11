# Migration provenance

This parity extraction originated from
`hollowc2/ButterflyGuy@122c4ba9451a5349d4edd99024342ba9673637a9` on 2026-08-10.

Source paths:

- `src/butterfly_guy/schwab_gateway/` → `src/schwab_gateway/`
- `src/butterfly_guy/gateway_client/` → `packages/sdk/src/schwab_gateway_sdk/`
- `src/butterfly_guy/schwab_gateway/token_manager.py` →
  `packages/token-store/src/schwab_token_store/__init__.py`
- gateway operator scripts, focused tests, Compose, and alert rules were adapted from
  their corresponding ButterflyGuy paths.

The extraction deliberately excludes strategy, risk, execution, database, scanner,
reporting, charting, account, and order behavior.
