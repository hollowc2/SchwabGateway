"""aiohttp API for the minimal read-only Schwab gateway."""

from __future__ import annotations

import asyncio
import datetime as dt
import re
import time
from typing import Literal, Protocol, cast

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from schwab_gateway_sdk.models import (
    ChainMetadataResponseV1,
    GatewayHealthV1,
    GatewayReadinessV1,
    HistoryResponseV1,
    MoversResponseV1,
    OptionChainResponseV1,
    OrderBookRecentResponseV1,
    OrderBookStreamEnvelopeV1,
    QuoteResponseV1,
    SessionHistoryResponseV1,
    SpotResponseV1,
)
from schwab_token_store import (
    TokenManagerHealth,
    TokenManagerState,
)

from schwab_gateway.admission import (
    AdmissionCapacityError,
    AdmissionController,
    AdmissionPolicy,
)
from schwab_gateway.auth import (
    AUTHENTICATOR_KEY,
    PRINCIPAL_KEY,
    InternalKeyAuthenticator,
    authentication_middleware,
    require_capability,
)
from schwab_gateway.logging import get_logger
from schwab_gateway.order_book_store import OrderBookSnapshotStore, OrderBookVenue
from schwab_gateway.upstream import (
    ChainMetadataUpstream,
    HistoryUpstream,
    MoversUpstream,
    OptionChainUpstream,
    QuoteUpstream,
    SessionHistoryUpstream,
    SpotUpstream,
    UpstreamMalformedError,
    UpstreamUnavailableError,
)

log = get_logger(__name__)
UTC = dt.timezone.utc
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9$._/-]{1,32}$")
MAX_SYMBOLS = 100
HISTORY_FREQUENCIES = ("daily", "minute")
# (minimum, maximum, default) bars-back bounds per history frequency. The daily default
# is 20, not the direct wrapper's 10, because it matches the equity scanner's actual
# consumer: ButterflyGuy's fetch_avg_volumes/prior_session_pct_change compute a 20-day
# rolling average, so a caller that omits days_back gets a window that already satisfies
# that lookback instead of needing to know to override it.
HISTORY_DAYS_BACK_BOUNDS: dict[str, tuple[int, int, int]] = {
    "daily": (1, 20, 20),
    "minute": (1, 5, 1),
}
MOVER_INDEXES = frozenset(
    {
        "$DJI",
        "$COMPX",
        "$SPX",
        "NYSE",
        "NASDAQ",
        "OTCBB",
        "INDEX_ALL",
        "EQUITY_ALL",
        "OPTION_ALL",
        "OPTION_PUT",
        "OPTION_CALL",
    }
)
MOVER_DIRECTIONS = ("up", "down")
SESSION_TYPES = ("regular", "extended")
ORDER_BOOK_VENUES = ("NASDAQ", "NYSE")
MAX_RECENT_ORDER_BOOK_SNAPSHOTS = 1000
MAX_STREAM_ORDER_BOOK_SYMBOLS = 25

gateway_requests = Counter(
    "gateway_client_requests_total",
    "Internal gateway client requests",
    ["operation", "status"],
)
gateway_latency = Histogram(
    "gateway_client_request_latency_seconds",
    "Internal gateway request latency",
    ["operation"],
)
gateway_admission = Counter(
    "gateway_admission_total",
    "Bounded gateway admission decisions",
    ["priority_class", "outcome"],
)

UPSTREAM_KEY = web.AppKey("gateway_quote_upstream", QuoteUpstream)
SPOT_UPSTREAM_KEY = web.AppKey("gateway_spot_upstream", SpotUpstream)
CHAIN_UPSTREAM_KEY = web.AppKey("gateway_chain_upstream", ChainMetadataUpstream)
OPTION_CHAIN_UPSTREAM_KEY = web.AppKey(
    "gateway_option_chain_upstream", OptionChainUpstream
)
HISTORY_UPSTREAM_KEY = web.AppKey("gateway_history_upstream", HistoryUpstream)
MOVERS_UPSTREAM_KEY = web.AppKey("gateway_movers_upstream", MoversUpstream)
SESSION_HISTORY_UPSTREAM_KEY = web.AppKey(
    "gateway_session_history_upstream", SessionHistoryUpstream
)
UPSTREAM_TIMEOUT_KEY = web.AppKey("gateway_upstream_timeout", float)
TOKEN_READINESS_PROVIDER_KEY = web.AppKey(
    "gateway_token_readiness_provider", "TokenReadinessProvider"
)
ADMISSION_CONTROLLER_KEY = web.AppKey(
    "gateway_admission_controller", AdmissionController
)
ORDER_BOOK_STORE_KEY = web.AppKey("gateway_order_book_store", OrderBookSnapshotStore)


class TokenReadinessProvider(Protocol):
    """Injected boundary for the token manager's bounded readiness state."""

    def health(self) -> TokenManagerHealth: ...


class StaticTokenReadinessProvider:
    """Deterministic fake-only readiness provider for the demo runner."""

    def __init__(self, state: TokenManagerState) -> None:
        self._state = state

    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=self._state,
            reason="static_provider",
            updated_at=dt.datetime.now(UTC),
        )


class _UnavailableSpotUpstream:
    """Fail closed when an app declares no spot surface."""

    async def get_spot(self, _symbol: str):
        raise UpstreamUnavailableError("spot upstream is not configured")


class _UnavailableChainMetadataUpstream:
    """Fail closed when an app declares no chain-metadata surface."""

    async def get_chain_metadata(self, _symbol: str, _expiration: dt.date):
        raise UpstreamUnavailableError("chain upstream is not configured")


class _UnavailableOptionChainUpstream:
    """Fail closed when an app declares no full option-chain surface."""

    async def get_option_chain(self, _symbol: str, _expiration: dt.date):
        raise UpstreamUnavailableError("option-chain upstream is not configured")


class _UnavailableHistoryUpstream:
    """Fail closed when an app declares no history surface."""

    async def get_history(self, _symbol: str, _frequency: str, _days_back: int):
        raise UpstreamUnavailableError("history upstream is not configured")


class _UnavailableMoversUpstream:
    """Fail closed when an app declares no movers surface."""

    async def get_movers(self, _index: str, _direction: str):
        raise UpstreamUnavailableError("movers upstream is not configured")


class _UnavailableSessionHistoryUpstream:
    """Fail closed when an app declares no session-history surface."""

    async def get_session_history(self, _symbol: str, _date: dt.date, _session: str):
        raise UpstreamUnavailableError("session history upstream is not configured")


class _UnavailableTokenReadinessProvider:
    """Fail closed when an app has no injected readiness dependency."""

    def health(self) -> TokenManagerHealth:
        return TokenManagerHealth(
            state=TokenManagerState.UNINITIALIZED,
            reason="provider_not_configured",
            updated_at=dt.datetime.now(UTC),
        )


READINESS_REASON_BY_STATE = {
    TokenManagerState.UNINITIALIZED: "token_not_checked",
    TokenManagerState.READY: "token_ready",
    TokenManagerState.REFRESHING: "token_refreshing",
    TokenManagerState.MISSING: "token_missing",
    TokenManagerState.CORRUPT: "token_corrupt",
    TokenManagerState.EXPIRED: "refresh_token_expired",
    TokenManagerState.REVOKED: "token_revoked",
    TokenManagerState.REAUTHORIZATION_REQUIRED: "token_reauthorization_required",
    TokenManagerState.LOCK_TIMEOUT: "token_lock_timeout",
    TokenManagerState.REFRESH_FAILED: "token_refresh_failed",
    TokenManagerState.PERSISTENCE_FAILED: "token_persistence_failed",
}
READINESS_UNAVAILABLE_REASON = "token_readiness_unavailable"


def _json(model, *, status: int = 200) -> web.Response:
    return web.json_response(model.model_dump(mode="json"), status=status)


def _error(code: str, message: str, status: int) -> web.Response:
    return web.json_response(
        {
            "schema_version": "1.0",
            "error": {"code": code, "message": message},
        },
        status=status,
    )


def _parse_symbols(request: web.Request) -> tuple[str, ...]:
    value = request.query.get("symbols", "")
    symbols = tuple(part.strip().upper() for part in value.split(",") if part.strip())
    if not symbols:
        raise ValueError("at least one symbol is required")
    if len(symbols) > MAX_SYMBOLS:
        raise ValueError(f"at most {MAX_SYMBOLS} symbols are allowed")
    if len(set(symbols)) != len(symbols):
        raise ValueError("symbols must be unique")
    if any(not SYMBOL_PATTERN.fullmatch(symbol) for symbol in symbols):
        raise ValueError("one or more symbols are invalid")
    return symbols


def _parse_symbol(request: web.Request) -> str:
    symbol = request.query.get("symbol", "").strip().upper()
    if not symbol:
        raise ValueError("a symbol is required")
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("the symbol is invalid")
    return symbol


def _parse_iso_date(
    request: web.Request, name: str, *, required_message: str, format_message: str
) -> dt.date:
    value = request.query.get(name, "").strip()
    if not value:
        raise ValueError(required_message)
    if len(value) != 10:
        raise ValueError(format_message)
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(format_message) from exc


def _parse_expiration(request: web.Request) -> dt.date:
    return _parse_iso_date(
        request,
        "expiration",
        required_message="an expiration is required",
        format_message="the expiration must be an ISO-8601 date",
    )


def _parse_session_date(request: web.Request) -> dt.date:
    return _parse_iso_date(
        request,
        "date",
        required_message="a date is required",
        format_message="the date must be an ISO-8601 date",
    )


def _parse_session(request: web.Request) -> Literal["regular", "extended"]:
    value = request.query.get("session", "").strip().lower()
    if value not in SESSION_TYPES:
        raise ValueError("session must be 'regular' or 'extended'")
    return value  # type: ignore[return-value]


def _parse_frequency(request: web.Request) -> Literal["daily", "minute"]:
    value = request.query.get("frequency", "daily").strip().lower()
    if value not in HISTORY_FREQUENCIES:
        raise ValueError("frequency must be 'daily' or 'minute'")
    return value  # type: ignore[return-value]


def _parse_days_back(request: web.Request, frequency: Literal["daily", "minute"]) -> int:
    minimum, maximum, default = HISTORY_DAYS_BACK_BOUNDS[frequency]
    value = request.query.get("days_back", "").strip()
    if not value:
        return default
    try:
        days_back = int(value)
    except ValueError as exc:
        raise ValueError("days_back must be an integer") from exc
    if not minimum <= days_back <= maximum:
        raise ValueError(f"days_back must be between {minimum} and {maximum}")
    return days_back


def _parse_index(request: web.Request):
    value = request.query.get("index", "").strip().upper()
    if value not in MOVER_INDEXES:
        raise ValueError("index must be one of the supported Schwab mover indexes")
    return value


def _parse_direction(request: web.Request) -> Literal["up", "down"]:
    value = request.query.get("direction", "up").strip().lower()
    if value not in MOVER_DIRECTIONS:
        raise ValueError("direction must be 'up' or 'down'")
    return value  # type: ignore[return-value]


def _parse_order_book_venue(request: web.Request) -> OrderBookVenue:
    value = request.query.get("venue", "").strip().upper()
    if value not in ORDER_BOOK_VENUES:
        raise ValueError("venue must be 'NASDAQ' or 'NYSE'")
    return cast(OrderBookVenue, value)


def _parse_order_book_limit(request: web.Request) -> int:
    raw = request.query.get("limit", "100").strip()
    try:
        limit = int(raw)
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc
    if not 1 <= limit <= MAX_RECENT_ORDER_BOOK_SNAPSHOTS:
        raise ValueError("limit must be between 1 and 1000")
    return limit


@web.middleware
async def audit_middleware(request: web.Request, handler) -> web.StreamResponse:
    started = time.perf_counter()
    status = 500
    operation = request.match_info.route.name or "unknown"
    caller = "anonymous"
    try:
        response = await handler(request)
        status = response.status
        principal = request.get(PRINCIPAL_KEY)
        if principal is not None:
            caller = principal.client_id
        return response
    except web.HTTPException as exc:
        status = exc.status
        raise
    finally:
        elapsed = time.perf_counter() - started
        gateway_requests.labels(operation=operation, status=str(status)).inc()
        gateway_latency.labels(operation=operation).observe(elapsed)
        log.info(
            "gateway_request",
            caller=caller,
            operation=operation,
            status=status,
            latency_ms=round(elapsed * 1000, 2),
        )


async def health(_request: web.Request) -> web.Response:
    return _json(
        GatewayHealthV1(
            status="ok",
            timestamp=dt.datetime.now(UTC),
        )
    )


def _token_readiness(app: web.Application) -> tuple[TokenManagerState, str]:
    try:
        manager_health = app[TOKEN_READINESS_PROVIDER_KEY].health()
        state = manager_health.state
        reason = READINESS_REASON_BY_STATE.get(state)
    except Exception:
        state = TokenManagerState.UNINITIALIZED
        reason = None
        log.warning("gateway_readiness_provider_failed", reason="provider_unavailable")
    if reason is None:
        state = TokenManagerState.UNINITIALIZED
        reason = READINESS_UNAVAILABLE_REASON
    return state, reason


async def ready(_request: web.Request) -> web.Response:
    state, reason = _token_readiness(_request.app)
    is_ready = state is TokenManagerState.READY
    return _json(
        GatewayReadinessV1(
            status="ready" if is_ready else "not_ready",
            timestamp=dt.datetime.now(UTC),
            token_state=state.value,
            reason=reason,
        ),
        status=200 if is_ready else 503,
    )


async def metrics(_request: web.Request) -> web.Response:
    return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


async def quotes(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbols = _parse_symbols(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    state, _reason = _token_readiness(request.app)
    if state is not TokenManagerState.READY:
        return _error("gateway_not_ready", "gateway is not ready", 503)

    principal = request[PRINCIPAL_KEY]
    priority = principal.priority_class
    try:
        async with request.app[ADMISSION_CONTROLLER_KEY].admit(priority):
            gateway_admission.labels(
                priority_class=priority.value,
                outcome="admitted",
            ).inc()
            try:
                async with asyncio.timeout(request.app[UPSTREAM_TIMEOUT_KEY]):
                    result = await request.app[UPSTREAM_KEY].get_quotes(symbols)
                by_symbol = {quote.symbol: quote for quote in result}
                if set(by_symbol) != set(symbols):
                    raise UpstreamMalformedError("upstream returned a partial symbol set")
                ordered = tuple(by_symbol[symbol] for symbol in symbols)
                return _json(QuoteResponseV1(quotes=ordered))
            except TimeoutError:
                return _error("upstream_timeout", "quote upstream timed out", 504)
            except UpstreamUnavailableError:
                return _error("upstream_unavailable", "quote upstream is unavailable", 503)
            except (UpstreamMalformedError, ValueError):
                return _error("upstream_malformed", "quote upstream returned invalid data", 502)
    except AdmissionCapacityError:
        gateway_admission.labels(
            priority_class=priority.value,
            outcome="rejected",
        ).inc()
        return _error(
            "gateway_capacity_exceeded",
            "gateway request capacity is unavailable",
            429,
        )


async def _serve_upstream(request: web.Request, build_response) -> web.Response:
    """Readiness, admission, timeout, and upstream classification for a market-data read.

    Callers must have already checked capability and validated their parameters, so this
    preserves the quote handler's fixed order: capability, validation, readiness,
    admission, upstream.
    """
    state, _reason = _token_readiness(request.app)
    if state is not TokenManagerState.READY:
        return _error("gateway_not_ready", "gateway is not ready", 503)

    principal = request[PRINCIPAL_KEY]
    priority = principal.priority_class
    try:
        async with request.app[ADMISSION_CONTROLLER_KEY].admit(priority):
            gateway_admission.labels(
                priority_class=priority.value,
                outcome="admitted",
            ).inc()
            try:
                async with asyncio.timeout(request.app[UPSTREAM_TIMEOUT_KEY]):
                    return await build_response()
            except TimeoutError:
                return _error("upstream_timeout", "market data upstream timed out", 504)
            except UpstreamUnavailableError:
                return _error(
                    "upstream_unavailable", "market data upstream is unavailable", 503
                )
            except (UpstreamMalformedError, ValueError):
                return _error(
                    "upstream_malformed", "market data upstream returned invalid data", 502
                )
    except AdmissionCapacityError:
        gateway_admission.labels(
            priority_class=priority.value,
            outcome="rejected",
        ).inc()
        return _error(
            "gateway_capacity_exceeded",
            "gateway request capacity is unavailable",
            429,
        )


async def spot(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbol = _parse_symbol(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    async def build_response() -> web.Response:
        result = await request.app[SPOT_UPSTREAM_KEY].get_spot(symbol)
        if result.symbol != symbol:
            raise UpstreamMalformedError("upstream returned a different symbol")
        return _json(SpotResponseV1(spot=result))

    return await _serve_upstream(request, build_response)


async def chain_metadata(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbol = _parse_symbol(request)
        expiration = _parse_expiration(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    async def build_response() -> web.Response:
        result = await request.app[CHAIN_UPSTREAM_KEY].get_chain_metadata(symbol, expiration)
        if result.symbol != symbol or result.expiration != expiration:
            raise UpstreamMalformedError("upstream returned a different chain")
        return _json(ChainMetadataResponseV1(chain=result))

    return await _serve_upstream(request, build_response)


async def option_chain(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbol = _parse_symbol(request)
        expiration = _parse_expiration(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    async def build_response() -> web.Response:
        result = await request.app[OPTION_CHAIN_UPSTREAM_KEY].get_option_chain(
            symbol, expiration
        )
        if result.symbol != symbol or result.expiration != expiration:
            raise UpstreamMalformedError("upstream returned a different option chain")
        return _json(OptionChainResponseV1(option_chain=result))

    return await _serve_upstream(request, build_response)


async def history(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbol = _parse_symbol(request)
        frequency = _parse_frequency(request)
        days_back = _parse_days_back(request, frequency)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    async def build_response() -> web.Response:
        result = await request.app[HISTORY_UPSTREAM_KEY].get_history(
            symbol, frequency, days_back
        )
        if result.symbol != symbol or result.frequency != frequency:
            raise UpstreamMalformedError("upstream returned a different history series")
        return _json(HistoryResponseV1(history=result))

    return await _serve_upstream(request, build_response)


async def movers(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        index = _parse_index(request)
        direction = _parse_direction(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    async def build_response() -> web.Response:
        result = await request.app[MOVERS_UPSTREAM_KEY].get_movers(index, direction)
        if result.index != index or result.direction != direction:
            raise UpstreamMalformedError("upstream returned different movers")
        return _json(MoversResponseV1(movers=result))

    return await _serve_upstream(request, build_response)


async def session_history(request: web.Request) -> web.Response:
    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbol = _parse_symbol(request)
        date = _parse_session_date(request)
        session = _parse_session(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    async def build_response() -> web.Response:
        result = await request.app[SESSION_HISTORY_UPSTREAM_KEY].get_session_history(
            symbol, date, session
        )
        if result.symbol != symbol or result.date != date or result.session != session:
            raise UpstreamMalformedError("upstream returned a different session history")
        return _json(SessionHistoryResponseV1(session_history=result))

    return await _serve_upstream(request, build_response)


async def recent_order_book(request: web.Request) -> web.Response:
    """Return bounded recent venue depth without implying a consolidated book."""

    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbol = _parse_symbol(request)
        venue = _parse_order_book_venue(request)
        limit = _parse_order_book_limit(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)
    snapshots = request.app[ORDER_BOOK_STORE_KEY].recent(symbol, venue, limit=limit)
    return _json(
        OrderBookRecentResponseV1(
            symbol=symbol,
            venue=venue,
            snapshots=snapshots,
            generated_at=dt.datetime.now(UTC),
        )
    )


async def stream_order_book(request: web.Request) -> web.StreamResponse:
    """Stream authenticated snapshots through a bounded slow-consumer queue."""

    denied = require_capability(request, "market_data:read")
    if denied is not None:
        return denied
    try:
        symbols = _parse_symbols(request)
        if len(symbols) > MAX_STREAM_ORDER_BOOK_SYMBOLS:
            raise ValueError("at most 25 order-book stream symbols are allowed")
        venue = _parse_order_book_venue(request)
    except ValueError as exc:
        return _error("invalid_request", str(exc), 400)

    store = request.app[ORDER_BOOK_STORE_KEY]
    subscription = store.subscribe(frozenset(symbols), venue)
    socket = web.WebSocketResponse(heartbeat=30, autoping=True)
    await socket.prepare(request)
    try:
        for symbol in symbols:
            for snapshot in store.recent(symbol, venue, limit=1):
                envelope = OrderBookStreamEnvelopeV1(snapshot=snapshot)
                await socket.send_json(envelope.model_dump(mode="json"))
        while not socket.closed:
            snapshot_task = asyncio.create_task(subscription.queue.get())
            receive_task = asyncio.create_task(socket.receive())
            done, pending = await asyncio.wait(
                (snapshot_task, receive_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if receive_task in done:
                message = receive_task.result()
                if message.type in {
                    web.WSMsgType.CLOSE,
                    web.WSMsgType.CLOSED,
                    web.WSMsgType.ERROR,
                }:
                    break
            if snapshot_task in done:
                envelope = OrderBookStreamEnvelopeV1(snapshot=snapshot_task.result())
                await socket.send_json(envelope.model_dump(mode="json"))
    finally:
        store.unsubscribe(subscription)
        await socket.close()
    return socket


def create_app(
    upstream: QuoteUpstream,
    authenticator: InternalKeyAuthenticator,
    *,
    upstream_timeout_seconds: float = 3.0,
    token_readiness_provider: TokenReadinessProvider | None = None,
    admission_policy: AdmissionPolicy | None = None,
    spot_upstream: SpotUpstream | None = None,
    chain_upstream: ChainMetadataUpstream | None = None,
    option_chain_upstream: OptionChainUpstream | None = None,
    history_upstream: HistoryUpstream | None = None,
    movers_upstream: MoversUpstream | None = None,
    session_history_upstream: SessionHistoryUpstream | None = None,
    order_book_store: OrderBookSnapshotStore | None = None,
) -> web.Application:
    if upstream_timeout_seconds <= 0:
        raise ValueError("upstream timeout must be positive")
    app = web.Application(middlewares=[audit_middleware, authentication_middleware])
    app[UPSTREAM_KEY] = upstream
    app[SPOT_UPSTREAM_KEY] = spot_upstream or _UnavailableSpotUpstream()
    app[CHAIN_UPSTREAM_KEY] = chain_upstream or _UnavailableChainMetadataUpstream()
    app[OPTION_CHAIN_UPSTREAM_KEY] = (
        option_chain_upstream or _UnavailableOptionChainUpstream()
    )
    app[HISTORY_UPSTREAM_KEY] = history_upstream or _UnavailableHistoryUpstream()
    app[MOVERS_UPSTREAM_KEY] = movers_upstream or _UnavailableMoversUpstream()
    app[SESSION_HISTORY_UPSTREAM_KEY] = (
        session_history_upstream or _UnavailableSessionHistoryUpstream()
    )
    app[AUTHENTICATOR_KEY] = authenticator
    app[UPSTREAM_TIMEOUT_KEY] = upstream_timeout_seconds
    app[TOKEN_READINESS_PROVIDER_KEY] = (
        token_readiness_provider or _UnavailableTokenReadinessProvider()
    )
    app[ADMISSION_CONTROLLER_KEY] = AdmissionController(
        admission_policy or AdmissionPolicy(protected_capacity=8, background_capacity=8)
    )
    app[ORDER_BOOK_STORE_KEY] = order_book_store or OrderBookSnapshotStore()
    app.router.add_get("/health", health, name="health")
    app.router.add_get("/ready", ready, name="ready")
    app.router.add_get("/metrics", metrics, name="metrics")
    app.router.add_get("/v1/quotes", quotes, name="quotes_v1")
    app.router.add_get("/v1/spot", spot, name="spot_v1")
    app.router.add_get("/v1/chain", chain_metadata, name="chain_v1")
    app.router.add_get("/v1/option-chain", option_chain, name="option_chain_v1")
    app.router.add_get("/v1/history", history, name="history_v1")
    app.router.add_get("/v1/movers", movers, name="movers_v1")
    app.router.add_get("/v1/session-history", session_history, name="session_history_v1")
    app.router.add_get("/v1/order-book/recent", recent_order_book, name="order_book_recent_v1")
    app.router.add_get("/v1/order-book/stream", stream_order_book, name="order_book_stream_v1")
    return app
