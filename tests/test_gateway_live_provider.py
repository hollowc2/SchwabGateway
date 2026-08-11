"""Tests for the real Schwab provider bound to the locked token manager.

No test here reads a credential, a token document, or contacts Schwab. The client factory
is a fake with the schwab-py 1.5.1 access-function signature, and every response is a
synthetic stub.
"""

from __future__ import annotations

import datetime as dt
import inspect
from typing import Any

import pytest

from schwab_gateway.config import GatewayCredentialProbeSettings
from schwab_gateway.live_provider import (
    GatewayUpstreamSettings,
    LockedSchwabMarketDataProvider,
    extract_spot_price,
)
from schwab_gateway.token_adapter import (
    LockedSchwabClientAdapter,
    SchwabClientOperationError,
)
from schwab_gateway.upstream import (
    EquityQuoteProvider,
    OptionChainProvider,
    SpotPriceProvider,
)

EXPIRATION = dt.date(2026, 3, 10)


class _Response:
    def __init__(self, payload: Any, *, raises: Exception | None = None) -> None:
        self._payload = payload
        self._raises = raises

    def raise_for_status(self) -> None:
        if self._raises is not None:
            raise self._raises

    def json(self) -> Any:
        return self._payload


class _Session:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _FakeQuoteFields:
    QUOTE = "quote"
    EXTENDED = "extended"


class _FakeQuote:
    Fields = _FakeQuoteFields


class _FakeClient:
    """Minimal stand-in for a schwab-py client. Exposes only read methods."""

    Quote = _FakeQuote

    def __init__(self) -> None:
        self.session = _Session()
        self.quote_calls: list[str] = []
        self.chain_calls: list[tuple[str, dt.date, dt.date]] = []
        self.quotes_calls: list[list[str]] = []
        self.quote_response: Any = _Response({"$SPX": {"quote": {"lastPrice": 5500.0}}})
        self.chain_response: Any = _Response({"callExpDateMap": {}, "putExpDateMap": {}})
        self.quotes_responses: list[Any] = []

    def get_quote(self, symbol: str) -> Any:
        self.quote_calls.append(symbol)
        return self.quote_response

    def get_option_chain(self, symbol: str, *, from_date: dt.date, to_date: dt.date) -> Any:
        self.chain_calls.append((symbol, from_date, to_date))
        return self.chain_response

    def get_quotes(self, symbols: list[str], *, fields: list[str]) -> Any:
        self.quotes_calls.append(list(symbols))
        assert fields == [_FakeQuoteFields.QUOTE, _FakeQuoteFields.EXTENDED]
        return self.quotes_responses.pop(0)


class _RecordingManager:
    """Stands in for AtomicTokenManager, recording transaction boundaries."""

    def __init__(self) -> None:
        self.transactions = 0
        self.open = False
        self.max_concurrent = 0

    def run_access_transaction(self, operation: Any) -> Any:
        self.transactions += 1
        self.open = True
        self.max_concurrent = max(self.max_concurrent, 1)
        try:
            return operation(lambda: None, lambda _token: None)
        finally:
            self.open = False


def _provider(client: _FakeClient) -> tuple[LockedSchwabMarketDataProvider, _RecordingManager]:
    manager = _RecordingManager()
    adapter = LockedSchwabClientAdapter(
        manager,  # type: ignore[arg-type]
        lambda *args, **kwargs: client,
        api_key="fake-key",
        app_secret="fake-secret",
    )
    return LockedSchwabMarketDataProvider(adapter), manager


# --- Surface boundary ----------------------------------------------------------------


def test_provider_exposes_only_the_three_read_surfaces() -> None:
    """No account, order, transaction, movers, or streaming method exists to call."""
    public = {
        name
        for name, _ in inspect.getmembers(LockedSchwabMarketDataProvider, inspect.isfunction)
        if not name.startswith("_")
    }
    assert public == {"get_spot_price", "get_option_chain", "get_equity_quotes"}


@pytest.mark.parametrize(
    ("protocol", "method"),
    [
        (SpotPriceProvider, "get_spot_price"),
        (OptionChainProvider, "get_option_chain"),
        (EquityQuoteProvider, "get_equity_quotes"),
    ],
)
def test_provider_signatures_match_the_declared_read_protocols(
    protocol: Any, method: str
) -> None:
    """The protocols are not runtime-checkable, so match signatures explicitly."""
    expected = inspect.signature(getattr(protocol, method))
    actual = inspect.signature(getattr(LockedSchwabMarketDataProvider, method))
    assert actual.parameters == expected.parameters


# --- Spot ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spot_read_runs_in_one_transaction_and_closes_its_session() -> None:
    client = _FakeClient()
    provider, manager = _provider(client)

    assert await provider.get_spot_price("$SPX") == 5500.0
    assert client.quote_calls == ["$SPX"]
    assert manager.transactions == 1
    assert client.session.closed == 1


@pytest.mark.asyncio
async def test_spot_read_closes_its_session_even_when_the_call_fails() -> None:
    client = _FakeClient()
    client.quote_response = _Response(None, raises=RuntimeError("upstream refused"))
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_spot_price("$SPX")
    assert client.session.closed == 1


@pytest.mark.asyncio
async def test_spot_read_raises_bare_value_error_on_a_malformed_payload() -> None:
    """Parsing runs outside the locked transaction, so a malformed payload must raise a
    bare ``ValueError`` from ``extract_spot_price`` rather than the adapter's generic
    ``SchwabClientOperationError`` -- that is what lets the gateway boundary tell a
    malformed response apart from a genuine fetch failure."""
    client = _FakeClient()
    client.quote_response = _Response({"$SPX": {"quote": {}}})
    provider, _ = _provider(client)

    with pytest.raises(ValueError):
        await provider.get_spot_price("$SPX")


@pytest.mark.asyncio
async def test_spot_read_does_not_retry() -> None:
    """The direct path retries three times; inside a held token lock this one must not."""
    client = _FakeClient()
    client.quote_response = _Response(None, raises=RuntimeError("upstream refused"))
    provider, manager = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_spot_price("$SPX")
    assert client.quote_calls == ["$SPX"]
    assert manager.transactions == 1


# --- Chain ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chain_read_pins_the_expiration_to_a_single_day() -> None:
    client = _FakeClient()
    provider, _ = _provider(client)

    await provider.get_option_chain("$SPX", EXPIRATION)
    assert client.chain_calls == [("$SPX", EXPIRATION, EXPIRATION)]


@pytest.mark.asyncio
async def test_chain_read_rejects_a_non_object_payload() -> None:
    client = _FakeClient()
    client.chain_response = _Response(["not", "an", "object"])
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_option_chain("$SPX", EXPIRATION)


# --- Quotes --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_symbol_list_takes_no_transaction_at_all() -> None:
    client = _FakeClient()
    provider, manager = _provider(client)

    assert await provider.get_equity_quotes([]) == {}
    assert manager.transactions == 0


@pytest.mark.asyncio
async def test_all_quote_batches_share_one_token_transaction() -> None:
    client = _FakeClient()
    client.quotes_responses = [
        _Response({"AAA": {"quote": {}}, "BBB": {"quote": {}}}),
        _Response({"CCC": {"quote": {}}}),
    ]
    provider, manager = _provider(client)

    result = await provider.get_equity_quotes(["AAA", "BBB", "CCC"], batch_size=2)

    assert set(result) == {"AAA", "BBB", "CCC"}
    assert client.quotes_calls == [["AAA", "BBB"], ["CCC"]]
    # Two HTTP batches, one lock acquisition.
    assert manager.transactions == 1
    assert client.session.closed == 1


@pytest.mark.asyncio
async def test_quote_batch_size_must_be_positive() -> None:
    client = _FakeClient()
    provider, manager = _provider(client)

    with pytest.raises(ValueError):
        await provider.get_equity_quotes(["AAA"], batch_size=0)
    assert manager.transactions == 0


# --- Spot-price extraction, pinned against the live client ---------------------------

SPOT_PAYLOADS = [
    pytest.param({"$SPX": {"quote": {"lastPrice": 5500.0}}}, 5500.0, id="nested_last"),
    pytest.param({"$SPX": {"lastPrice": 5501.0}}, 5501.0, id="flat_last"),
    pytest.param({"SPX": {"quote": {"lastPrice": 5502.0}}}, 5502.0, id="unprefixed_symbol"),
    pytest.param({"$SPX": {"quote": {"mark": 5503.0}}}, 5503.0, id="mark_fallback"),
    pytest.param({"$SPX": {"quote": {"closePrice": 5504.0}}}, 5504.0, id="close_fallback"),
    pytest.param(
        {"$SPX": {"quote": {"lastPrice": 0, "mark": 5505.0}}},
        5505.0,
        id="zero_last_falls_through",
    ),
]


@pytest.mark.parametrize(
    ("payload", "expected"), [(p.values[0], p.values[1]) for p in SPOT_PAYLOADS]
)
def test_extract_spot_price_matches_the_expected_preference_order(
    payload: dict[str, Any], expected: float
) -> None:
    assert extract_spot_price(payload, "$SPX") == expected


REJECTED_SPOT_PAYLOADS = [
    pytest.param({}, id="empty"),
    pytest.param({"$SPX": {}}, id="no_price_fields"),
    pytest.param({"$SPX": {"quote": {"lastPrice": None}}}, id="null_price"),
    pytest.param(["not", "a", "dict"], id="not_an_object"),
    pytest.param({"$SPX": "not-an-object"}, id="entry_not_an_object"),
    # A legitimate all-zero quote is a known, deliberately-mirrored gap: ``or`` treats a
    # real 0 as falsy and falls through every field, so this raises even though 0.0 could
    # be the true spot price. See ``extract_spot_price``'s docstring.
    pytest.param(
        {"$SPX": {"quote": {"lastPrice": 0, "mark": 0, "closePrice": 0}}},
        id="all_fields_legitimately_zero",
    ),
]


@pytest.mark.parametrize("payload", [p.values[0] for p in REJECTED_SPOT_PAYLOADS])
def test_extract_spot_price_rejects_unusable_payloads(payload: Any) -> None:
    with pytest.raises(ValueError):
        extract_spot_price(payload, "$SPX")


# --- Settings ------------------------------------------------------------------------


def test_upstream_settings_require_an_absolute_token_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCHWAB_API_KEY", "fake-key")
    monkeypatch.setenv("SCHWAB_SECRET_KEY", "fake-secret")
    monkeypatch.setenv("SCHWAB_TOKEN_PATH", "relative/tokens.json")

    with pytest.raises(ValueError):
        GatewayUpstreamSettings()

    monkeypatch.setenv("SCHWAB_TOKEN_PATH", "/opt/butterflyguy/tokens.json")
    settings = GatewayUpstreamSettings()
    assert settings.token_path.is_absolute()


def test_upstream_settings_do_not_expose_secrets_in_their_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCHWAB_API_KEY", "fake-key-value")
    monkeypatch.setenv("SCHWAB_SECRET_KEY", "fake-secret-value")
    monkeypatch.setenv("SCHWAB_TOKEN_PATH", "/opt/butterflyguy/tokens.json")

    rendered = repr(GatewayUpstreamSettings())
    assert "fake-key-value" not in rendered
    assert "fake-secret-value" not in rendered
    assert "/opt/butterflyguy/tokens.json" not in rendered


def test_upstream_settings_stay_in_agreement_with_the_credential_probe_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard for the deliberate duplication of these two settings classes.

    ``GatewayUpstreamSettings`` (here) and ``GatewayCredentialProbeSettings``
    (``schwab_gateway/config.py``) are intentionally separate classes with identical
    fields, env var aliases, and absolute-token-path validation, because ``config.py`` is
    a member of a reviewed credential-proof archive pinned by SHA-256 and must not be
    edited or reused. Nothing enforces that the two stay identical except this test: if
    either class's fields, aliases, or validation drift from the other, this fails
    immediately instead of the drift going unnoticed.
    """
    upstream_fields = GatewayUpstreamSettings.model_fields
    probe_fields = GatewayCredentialProbeSettings.model_fields

    assert set(upstream_fields) == set(probe_fields)
    for name, field in upstream_fields.items():
        assert field.validation_alias == probe_fields[name].validation_alias

    monkeypatch.setenv("SCHWAB_API_KEY", "fake-key")
    monkeypatch.setenv("SCHWAB_SECRET_KEY", "fake-secret")
    monkeypatch.setenv("SCHWAB_TOKEN_PATH", "relative/tokens.json")

    with pytest.raises(ValueError):
        GatewayUpstreamSettings()
    with pytest.raises(ValueError):
        GatewayCredentialProbeSettings()

    monkeypatch.setenv("SCHWAB_TOKEN_PATH", "/opt/butterflyguy/tokens.json")
    assert GatewayUpstreamSettings().token_path.is_absolute()
    assert GatewayCredentialProbeSettings().token_path.is_absolute()
