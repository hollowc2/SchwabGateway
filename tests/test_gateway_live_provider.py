"""Tests for the real Schwab provider bound to the locked token manager.

No test here reads a credential, a token document, or contacts Schwab. The client factory
is a fake with the schwab-py 1.5.1 access-function signature, and every response is a
synthetic stub.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import threading
from enum import Enum
from typing import Any

import pytest

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.auth import PriorityClass
from schwab_gateway.config import GatewayCredentialProbeSettings
from schwab_gateway.live_provider import (
    GatewayUpstreamSettings,
    LockedSchwabMarketDataProvider,
    extract_spot_price,
    extract_spot_price_and_timestamp,
    upstream_operation_latency,
)
from schwab_gateway.scheduler import ExecutionScheduler, SchedulerUpstreamTimeoutError
from schwab_gateway.token_adapter import (
    LockedSchwabClientAdapter,
    SchwabClientOperationError,
)
from schwab_gateway.upstream import (
    EquityQuoteProvider,
    MarketMoversProvider,
    OptionChainProvider,
    PriceHistoryProvider,
    SessionHistoryProvider,
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


class _FakePeriodType(Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class _FakePeriod(Enum):
    ONE_MONTH = 1
    ONE_YEAR = 2


class _FakeFrequencyType(Enum):
    DAILY = "daily"
    MINUTE = "minute"


class _FakeFrequency(Enum):
    EVERY_MINUTE = 1


class _FakePriceHistory:
    PeriodType = _FakePeriodType
    Period = _FakePeriod
    FrequencyType = _FakeFrequencyType
    Frequency = _FakeFrequency


class _FakeMoversIndex(Enum):
    DJI = "$DJI"
    SPX = "$SPX"
    NASDAQ = "NASDAQ"


class _FakeMoversSortOrder(Enum):
    PERCENT_CHANGE_UP = "PERCENT_CHANGE_UP"
    PERCENT_CHANGE_DOWN = "PERCENT_CHANGE_DOWN"


class _FakeMovers:
    Index = _FakeMoversIndex
    SortOrder = _FakeMoversSortOrder


class _FakeClient:
    """Minimal stand-in for a schwab-py client. Exposes only read methods."""

    Quote = _FakeQuote
    PriceHistory = _FakePriceHistory
    Movers = _FakeMovers

    def __init__(self) -> None:
        self.session = _Session()
        self.quote_calls: list[str] = []
        self.chain_calls: list[tuple[str, dt.date, dt.date]] = []
        self.quotes_calls: list[list[str]] = []
        self.price_history_calls: list[dict[str, Any]] = []
        self.movers_calls: list[tuple[Any, Any]] = []
        self.quote_response: Any = _Response({"$SPX": {"quote": {"lastPrice": 5500.0}}})
        self.chain_response: Any = _Response({"callExpDateMap": {}, "putExpDateMap": {}})
        self.quotes_responses: list[Any] = []
        self.price_history_response: Any = _Response({"candles": []})
        self.movers_response: Any = _Response([])

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

    def get_price_history(self, symbol: str, **kwargs: Any) -> Any:
        self.price_history_calls.append({"symbol": symbol, **kwargs})
        return self.price_history_response

    def get_movers(self, index: Any, *, sort_order: Any = None) -> Any:
        self.movers_calls.append((index, sort_order))
        return self.movers_response


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


def test_provider_exposes_only_read_only_market_data_methods() -> None:
    """No account, order, transaction, or streaming method exists to call.

    ``get_spot_snapshot`` is the timestamp-preserving form of the same Schwab quote read
    as ``get_spot_price``; it does not widen the upstream API. ``get_session_bars`` was added
    alongside the trailing-window ``get_daily_bars``/``get_intraday_bars`` to back the
    new ``/v1/session-history`` route (a point-in-time regular/extended session lookup
    for AfterHoursLab's earnings-candle archival, distinct from the trailing window the
    other two serve). Every added surface is still a bounded, read-only Schwab
    market-data call.
    """
    public = {
        name
        for name, _ in inspect.getmembers(LockedSchwabMarketDataProvider, inspect.isfunction)
        if not name.startswith("_")
    }
    assert public == {
        "get_spot_price",
        "get_spot_snapshot",
        "get_option_chain",
        "get_equity_quotes",
        "get_daily_bars",
        "get_intraday_bars",
        "get_market_movers",
        "get_session_bars",
    }


@pytest.mark.parametrize(
    ("protocol", "method"),
    [
        (SpotPriceProvider, "get_spot_price"),
        (OptionChainProvider, "get_option_chain"),
        (EquityQuoteProvider, "get_equity_quotes"),
        (PriceHistoryProvider, "get_daily_bars"),
        (PriceHistoryProvider, "get_intraday_bars"),
        (MarketMoversProvider, "get_market_movers"),
        (SessionHistoryProvider, "get_session_bars"),
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


def _latency_count(operation: str, status: str) -> float:
    for metric in upstream_operation_latency.collect():
        for sample in metric.samples:
            if (
                sample.name.endswith("_count")
                and sample.labels == {"operation": operation, "status": status}
            ):
                return sample.value
    return 0.0


@pytest.mark.asyncio
async def test_execute_records_upstream_latency_by_operation_and_status() -> None:
    client = _FakeClient()
    provider, _ = _provider(client)
    before_success = _latency_count("spot", "success")

    await provider.get_spot_price("$SPX")
    assert _latency_count("spot", "success") == before_success + 1

    client.quote_response = _Response(None, raises=RuntimeError("upstream refused"))
    before_error = _latency_count("spot", "error")
    with pytest.raises(SchwabClientOperationError):
        await provider.get_spot_price("$SPX")
    assert _latency_count("spot", "error") == before_error + 1


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


@pytest.mark.asyncio
async def test_timeout_returns_promptly_and_worker_lease_prevents_a_second_thread() -> None:
    entered = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    class BlockingAdapter:
        def execute(self, operation):
            nonlocal calls
            with calls_lock:
                calls += 1
            return operation(None)

    def blocking_operation(_client):
        entered.set()
        release.wait(timeout=5)
        return "complete"

    provider = LockedSchwabMarketDataProvider(BlockingAdapter())
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                provider._execute("spot", blocking_operation), timeout=0.02
            )
        assert loop.time() - started < 0.2
        assert entered.is_set()
        assert calls == 1

        # This request times out waiting for the provider lease. It must not create a
        # second daemon thread while the first synchronous transaction is still blocked.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                provider._execute("spot", blocking_operation), timeout=0.02
            )
        assert calls == 1
    finally:
        release.set()

    assert await asyncio.wait_for(
        provider._execute("spot", blocking_operation), timeout=0.5
    ) == "complete"
    assert calls == 2


@pytest.mark.asyncio
async def test_scheduler_timeout_retains_physical_provider_worker_until_thread_exits() -> None:
    entered = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    class BlockingAdapter:
        def execute(self, operation):
            nonlocal calls
            with calls_lock:
                calls += 1
            return operation(None)

    def blocking_operation(_client):
        entered.set()
        release.wait(timeout=5)
        return "complete"

    provider = LockedSchwabMarketDataProvider(BlockingAdapter())
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=2, background_capacity=1)
    )
    timed_out = asyncio.create_task(
        scheduler.execute(
            PriorityClass.PROTECTED,
            "spot",
            lambda: provider._execute("spot", blocking_operation),
            queue_timeout_seconds=1,
            execution_timeout_seconds=0.01,
        )
    )
    assert await asyncio.to_thread(entered.wait, 1)
    with pytest.raises(SchedulerUpstreamTimeoutError):
        await timed_out

    following = asyncio.create_task(
        scheduler.execute(
            PriorityClass.PROTECTED,
            "spot",
            lambda: provider._execute("spot", blocking_operation),
            queue_timeout_seconds=1,
            execution_timeout_seconds=1,
        )
    )
    await asyncio.sleep(0.02)
    assert calls == 1
    assert scheduler.snapshot().worker_active is True
    release.set()

    assert await following == "complete"
    await scheduler.wait_idle()
    assert calls == 2
    assert scheduler.snapshot().total == 0
    assert scheduler.snapshot().task_count == 0


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


# --- Daily bars ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daily_bars_request_shape_and_transaction_bounds() -> None:
    client = _FakeClient()
    client.price_history_response = _Response({"candles": [{"close": 1.0}]})
    provider, manager = _provider(client)

    result = await provider.get_daily_bars("AAPL", days_back=10)

    assert result == [{"close": 1.0}]
    assert client.price_history_calls == [
        {
            "symbol": "AAPL",
            "period_type": _FakePeriodType.YEAR,
            "period": _FakePeriod.ONE_YEAR,
            "frequency_type": _FakeFrequencyType.DAILY,
        }
    ]
    assert manager.transactions == 1
    assert client.session.closed == 1


@pytest.mark.asyncio
async def test_daily_bars_rejects_a_non_object_payload() -> None:
    client = _FakeClient()
    client.price_history_response = _Response(["not", "an", "object"])
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_daily_bars("AAPL")


@pytest.mark.asyncio
async def test_daily_bars_rejects_a_payload_with_no_candle_list() -> None:
    client = _FakeClient()
    client.price_history_response = _Response({"status": "FAILED"})
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_daily_bars("AAPL")


# --- Intraday bars ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_intraday_bars_request_shape_bounds_the_date_window() -> None:
    client = _FakeClient()
    client.price_history_response = _Response({"candles": [{"close": 2.0}]})
    provider, manager = _provider(client)

    result = await provider.get_intraday_bars("AAPL", days_back=2)

    assert result == [{"close": 2.0}]
    assert len(client.price_history_calls) == 1
    call = client.price_history_calls[0]
    assert call["symbol"] == "AAPL"
    assert call["period_type"] is _FakePeriodType.DAY
    assert call["frequency_type"] is _FakeFrequencyType.MINUTE
    assert call["frequency"] is _FakeFrequency.EVERY_MINUTE
    assert "period" not in call
    today = dt.date.today()
    assert call["start_datetime"] == dt.datetime.combine(today - dt.timedelta(days=2), dt.time.min)
    assert call["end_datetime"] == dt.datetime.combine(today, dt.time.max)
    assert manager.transactions == 1
    assert client.session.closed == 1


# --- Market movers ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_market_movers_converts_index_and_sort_order_to_real_enums() -> None:
    client = _FakeClient()
    client.movers_response = _Response([{"symbol": "AAPL"}])
    provider, manager = _provider(client)

    result = await provider.get_market_movers("$SPX", sort_order="PERCENT_CHANGE_DOWN")

    assert result == [{"symbol": "AAPL"}]
    assert client.movers_calls == [
        (_FakeMoversIndex.SPX, _FakeMoversSortOrder.PERCENT_CHANGE_DOWN)
    ]
    assert manager.transactions == 1
    assert client.session.closed == 1


@pytest.mark.asyncio
async def test_market_movers_unwraps_a_screeners_object_payload() -> None:
    client = _FakeClient()
    client.movers_response = _Response({"screeners": [{"symbol": "MSFT"}]})
    provider, _ = _provider(client)

    assert await provider.get_market_movers("$SPX") == [{"symbol": "MSFT"}]


@pytest.mark.asyncio
async def test_market_movers_rejects_an_unknown_index() -> None:
    client = _FakeClient()
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_market_movers("NOT_AN_INDEX")


@pytest.mark.asyncio
async def test_market_movers_rejects_a_non_list_non_object_payload() -> None:
    client = _FakeClient()
    client.movers_response = _Response("not-a-list-or-object")
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_market_movers("$SPX")


# --- Session history -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_bars_request_shape_spans_the_full_extended_day() -> None:
    from zoneinfo import ZoneInfo

    eastern = ZoneInfo("America/New_York")
    client = _FakeClient()
    client.price_history_response = _Response({"candles": [{"close": 3.0}]})
    provider, manager = _provider(client)

    date = dt.date(2026, 8, 12)
    result = await provider.get_session_bars("AAPL", date)

    assert result == [{"close": 3.0}]
    assert len(client.price_history_calls) == 1
    call = client.price_history_calls[0]
    assert call["symbol"] == "AAPL"
    assert call["period_type"] is _FakePeriodType.DAY
    assert call["frequency_type"] is _FakeFrequencyType.MINUTE
    assert call["frequency"] is _FakeFrequency.EVERY_MINUTE
    assert "period" not in call
    assert call["need_extended_hours_data"] is True
    assert call["start_datetime"] == dt.datetime(2026, 8, 12, 4, 0, tzinfo=eastern)
    assert call["end_datetime"] == dt.datetime(2026, 8, 12, 20, 0, tzinfo=eastern)
    assert manager.transactions == 1
    assert client.session.closed == 1


@pytest.mark.asyncio
async def test_session_bars_rejects_a_non_object_payload() -> None:
    client = _FakeClient()
    client.price_history_response = _Response(["not", "an", "object"])
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_session_bars("AAPL", dt.date(2026, 8, 12))


@pytest.mark.asyncio
async def test_session_bars_rejects_a_payload_with_no_candle_list() -> None:
    client = _FakeClient()
    client.price_history_response = _Response({"status": "FAILED"})
    provider, _ = _provider(client)

    with pytest.raises(SchwabClientOperationError):
        await provider.get_session_bars("AAPL", dt.date(2026, 8, 12))


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


def test_extract_spot_price_preserves_freshest_quote_or_trade_timestamp() -> None:
    payload = {
        "$SPX": {
            "quote": {
                "lastPrice": 5500.0,
                "quoteTime": 1_700_000_000_000,
                "tradeTime": 1_700_000_001_000,
            }
        }
    }

    price, timestamp = extract_spot_price_and_timestamp(payload, "$SPX")

    assert price == 5500.0
    assert timestamp == dt.datetime.fromtimestamp(1_700_000_001, tz=dt.timezone.utc)


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
