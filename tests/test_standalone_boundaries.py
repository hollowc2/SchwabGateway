from __future__ import annotations

import ast
import json
from pathlib import Path

import yaml


def python_files() -> list[Path]:
    return [*Path("src").rglob("*.py"), *Path("packages").rglob("*.py")]


def test_standalone_has_zero_butterfly_imports() -> None:
    offenders: list[str] = []
    for path in python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "butterfly_guy"
            ):
                offenders.append(str(path))
            if isinstance(node, ast.Import) and any(
                alias.name.startswith("butterfly_guy") for alias in node.names
            ):
                offenders.append(str(path))
    assert offenders == []


def test_contract_contains_only_parity_routes() -> None:
    """This lock is deliberately widened, not incidentally edited.

    ``/v1/history`` and ``/v1/movers`` were added on top of the original
    ButterflyGuy-parity extraction (see ``MIGRATION_PROVENANCE.md``) to expose the two
    additional read-only Schwab surfaces (``get_daily_bars``/``get_intraday_bars`` and
    ``get_market_movers``) the equity scanner needs before it can be extracted onto the
    gateway SDK.

    ``/v1/session-history`` was added separately, for a different consumer
    (AfterHoursLab) and a different shape of request: a point-in-time regular-or-extended
    session lookup for one past date, as opposed to ``/v1/history``'s trailing window
    ending "now". The two were deliberately kept as separate routes rather than merged,
    after a cross-session design conflict surfaced that exact question -- see the git
    history around this test for that discussion.

    All three remain strictly read-only market data: no account, order, position, or
    transaction route was added, and ``SCHWAB_GATEWAY_ORDER_WRITES_ENABLED`` is untouched.
    """
    contract = yaml.safe_load(Path("openapi.yaml").read_text())
    assert contract["openapi"] == "3.1.0"
    assert set(contract["paths"]) == {
        "/health",
        "/ready",
        "/metrics",
        "/v1/quotes",
        "/v1/spot",
        "/v1/chain",
        "/v1/history",
        "/v1/movers",
        "/v1/session-history",
    }
    serialized = json.dumps(contract).lower()
    for forbidden in ("/account", "/order", "/position", "/transaction", "/stream"):
        assert forbidden not in serialized


def test_golden_fixture_is_redacted_and_pinned() -> None:
    fixture = json.loads(Path("tests/fixtures/schwab_gateway_http_v1.json").read_text())
    assert fixture["captured_from_commit"] == (
        "122c4ba9451a5349d4edd99024342ba9673637a9"
    )
    serialized = json.dumps(fixture).lower()
    assert "schema_version" in serialized
    assert "authentication_required" in serialized
    for forbidden in ('"access_token":', '"refresh_token":', "accountnumber", '"api_key":'):
        assert forbidden not in serialized
