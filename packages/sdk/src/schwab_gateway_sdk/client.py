"""Fail-closed HTTP client for the read-only gateway contract."""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Literal
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
from pydantic import ValidationError
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosedError, InvalidStatus, WebSocketException

from schwab_gateway_sdk.models import (
    ChainMetadataResponseV1,
    HistoryResponseV1,
    MoversResponseV1,
    OptionChainResponseV1,
    OrderBookRecentResponseV1,
    OrderBookSnapshotV1,
    OrderBookStreamEnvelopeV1,
    QuoteResponseV1,
    SessionHistoryResponseV1,
    SpotResponseV1,
)

OrderBookVenue = Literal["NASDAQ", "NYSE"]
_ORDER_BOOK_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9$._/-]{1,32}$")
_MAX_ORDER_BOOK_SYMBOLS = 25


class GatewayClientError(RuntimeError):
    """Base error for gateway transport and contract failures."""


class GatewayAuthenticationError(GatewayClientError):
    pass


class GatewayAuthorizationError(GatewayClientError):
    pass


class GatewayTimeoutError(GatewayClientError):
    pass


class GatewayUnavailableError(GatewayClientError):
    pass


class GatewayQueueTimeoutError(GatewayUnavailableError):
    """The server shed a request whose bounded dispatch wait expired."""


class GatewayCapacityError(GatewayClientError):
    pass


class GatewayResponseError(GatewayClientError):
    pass


def _error_code(response: httpx.Response) -> str | None:
    """Read only the bounded error discriminator; malformed bodies fail closed."""
    try:
        payload = response.json()
        error = payload.get("error") if isinstance(payload, dict) else None
        code = error.get("code") if isinstance(error, dict) else None
    except ValueError:
        return None
    return code if isinstance(code, str) else None


def _normalize_order_book_venue(venue: str) -> OrderBookVenue:
    normalized = venue.strip().upper()
    if normalized not in {"NASDAQ", "NYSE"}:
        raise ValueError("venue must be 'NASDAQ' or 'NYSE'")
    return normalized  # type: ignore[return-value]


def _normalize_order_book_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(symbol.strip().upper() for symbol in symbols)
    if not normalized or any(not symbol for symbol in normalized):
        raise ValueError("at least one non-empty order-book symbol is required")
    if len(normalized) > _MAX_ORDER_BOOK_SYMBOLS:
        raise ValueError(
            f"at most {_MAX_ORDER_BOOK_SYMBOLS} order-book symbols are allowed"
        )
    if len(set(normalized)) != len(normalized):
        raise ValueError("order-book symbols must be unique")
    if any(not _ORDER_BOOK_SYMBOL_PATTERN.fullmatch(symbol) for symbol in normalized):
        raise ValueError("one or more order-book symbols are invalid")
    return normalized


def _websocket_error_code(exc: InvalidStatus) -> str | None:
    try:
        payload = json.loads(exc.response.body)
        error = payload.get("error") if isinstance(payload, dict) else None
        code = error.get("code") if isinstance(error, dict) else None
    except (TypeError, UnicodeDecodeError, ValueError):
        return None
    return code if isinstance(code, str) else None


class GatewayMarketDataClient:
    """Typed client for gateway market-data endpoints only."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_seconds: float = 12.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("gateway API key is required")
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout_seconds),
        )
        self._order_book_connections: set[ClientConnection] = set()

    async def get_quotes(self, symbols: Sequence[str]) -> QuoteResponseV1:
        requested = tuple(symbols)
        if not requested:
            raise ValueError("at least one symbol is required")
        try:
            response = await self._client.get(
                "/v1/quotes",
                params={"symbols": ",".join(requested)},
                headers={"X-Internal-API-Key": self._api_key},
            )
        except httpx.TimeoutException as exc:
            raise GatewayTimeoutError("gateway quote request timed out") from exc
        except httpx.TransportError as exc:
            raise GatewayUnavailableError("gateway quote request unavailable") from exc

        if response.status_code == 401:
            raise GatewayAuthenticationError("gateway authentication failed")
        if response.status_code == 403:
            raise GatewayAuthorizationError("gateway capability denied")
        if response.status_code == 429:
            raise GatewayCapacityError("gateway request capacity is unavailable")
        if response.status_code == 503 and _error_code(response) == "gateway_queue_timeout":
            raise GatewayQueueTimeoutError("gateway worker queue wait timed out")
        if response.status_code == 504:
            raise GatewayTimeoutError("gateway quote upstream timed out")
        if response.status_code in {502, 503}:
            raise GatewayUnavailableError("gateway upstream is unavailable")
        if response.status_code != 200:
            raise GatewayResponseError(
                f"gateway quote request failed with status {response.status_code}"
            )
        try:
            return QuoteResponseV1.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise GatewayResponseError("gateway returned an invalid quote contract") from exc

    async def _get_typed(self, path: str, params: dict[str, str], model: type):
        """Fail-closed GET for the collector-facing surfaces. No retries."""
        try:
            response = await self._client.get(
                path,
                params=params,
                headers={"X-Internal-API-Key": self._api_key},
            )
        except httpx.TimeoutException as exc:
            raise GatewayTimeoutError("gateway market data request timed out") from exc
        except httpx.TransportError as exc:
            raise GatewayUnavailableError("gateway market data request unavailable") from exc

        if response.status_code == 401:
            raise GatewayAuthenticationError("gateway authentication failed")
        if response.status_code == 403:
            raise GatewayAuthorizationError("gateway capability denied")
        if response.status_code == 429:
            raise GatewayCapacityError("gateway request capacity is unavailable")
        if response.status_code == 503 and _error_code(response) == "gateway_queue_timeout":
            raise GatewayQueueTimeoutError("gateway worker queue wait timed out")
        if response.status_code == 504:
            raise GatewayTimeoutError("gateway market data upstream timed out")
        if response.status_code in {502, 503}:
            raise GatewayUnavailableError("gateway upstream is unavailable")
        if response.status_code != 200:
            raise GatewayResponseError(
                f"gateway market data request failed with status {response.status_code}"
            )
        try:
            return model.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise GatewayResponseError("gateway returned an invalid market data contract") from exc

    async def get_spot(self, symbol: str) -> SpotResponseV1:
        requested = symbol.strip()
        if not requested:
            raise ValueError("a symbol is required")
        return await self._get_typed("/v1/spot", {"symbol": requested}, SpotResponseV1)

    async def get_chain_metadata(
        self, symbol: str, expiration: dt.date
    ) -> ChainMetadataResponseV1:
        requested = symbol.strip()
        if not requested:
            raise ValueError("a symbol is required")
        if not isinstance(expiration, dt.date) or isinstance(expiration, dt.datetime):
            raise ValueError("an expiration date is required")
        return await self._get_typed(
            "/v1/chain",
            {"symbol": requested, "expiration": expiration.isoformat()},
            ChainMetadataResponseV1,
        )

    async def get_option_chain(
        self, symbol: str, expiration: dt.date
    ) -> OptionChainResponseV1:
        """Fetch a complete normalized chain for one expiration. No retries."""
        requested = symbol.strip()
        if not requested:
            raise ValueError("a symbol is required")
        if not isinstance(expiration, dt.date) or isinstance(expiration, dt.datetime):
            raise ValueError("an expiration date is required")
        return await self._get_typed(
            "/v1/option-chain",
            {"symbol": requested, "expiration": expiration.isoformat()},
            OptionChainResponseV1,
        )

    async def get_history(
        self, symbol: str, *, frequency: str = "daily", days_back: int | None = None
    ) -> HistoryResponseV1:
        requested = symbol.strip()
        if not requested:
            raise ValueError("a symbol is required")
        if frequency not in {"daily", "minute"}:
            raise ValueError("frequency must be 'daily' or 'minute'")
        params = {"symbol": requested, "frequency": frequency}
        if days_back is not None:
            params["days_back"] = str(days_back)
        return await self._get_typed("/v1/history", params, HistoryResponseV1)

    async def get_movers(self, index: str, *, direction: str = "up") -> MoversResponseV1:
        requested = index.strip()
        if not requested:
            raise ValueError("an index is required")
        if direction not in {"up", "down"}:
            raise ValueError("direction must be 'up' or 'down'")
        return await self._get_typed(
            "/v1/movers", {"index": requested, "direction": direction}, MoversResponseV1
        )

    async def get_session_history(
        self, symbol: str, date: dt.date, *, session: str = "regular"
    ) -> SessionHistoryResponseV1:
        requested = symbol.strip()
        if not requested:
            raise ValueError("a symbol is required")
        if not isinstance(date, dt.date) or isinstance(date, dt.datetime):
            raise ValueError("a date is required")
        if session not in {"regular", "extended"}:
            raise ValueError("session must be 'regular' or 'extended'")
        return await self._get_typed(
            "/v1/session-history",
            {"symbol": requested, "date": date.isoformat(), "session": session},
            SessionHistoryResponseV1,
        )

    async def get_recent_order_book(
        self,
        symbol: str,
        *,
        venue: str,
        limit: int = 100,
    ) -> OrderBookRecentResponseV1:
        requested = symbol.strip()
        requested_venue = venue.strip().upper()
        if not requested:
            raise ValueError("a symbol is required")
        if requested_venue not in {"NASDAQ", "NYSE"}:
            raise ValueError("venue must be 'NASDAQ' or 'NYSE'")
        if not 1 <= limit <= 1000:
            raise ValueError("order-book limit must be between 1 and 1000")
        return await self._get_typed(
            "/v1/order-book/recent",
            {
                "symbol": requested,
                "venue": requested_venue,
                "limit": str(limit),
            },
            OrderBookRecentResponseV1,
        )

    def _order_book_stream_url(
        self, symbols: tuple[str, ...], venue: OrderBookVenue
    ) -> str:
        parsed = urlsplit(self._base_url)
        websocket_scheme = {"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}.get(
            parsed.scheme.lower()
        )
        if websocket_scheme is None or not parsed.netloc:
            raise ValueError("gateway base URL must use http, https, ws, or wss")
        query = urlencode({"symbols": ",".join(symbols), "venue": venue})
        return urlunsplit(
            (websocket_scheme, parsed.netloc, "/v1/order-book/stream", query, "")
        )

    @staticmethod
    def _raise_websocket_status(exc: InvalidStatus) -> None:
        status = exc.response.status_code
        if status == 401:
            raise GatewayAuthenticationError("gateway authentication failed") from exc
        if status == 403:
            raise GatewayAuthorizationError("gateway capability denied") from exc
        if status == 429:
            raise GatewayCapacityError(
                "gateway request capacity is unavailable"
            ) from exc
        if status == 503 and _websocket_error_code(exc) == "gateway_queue_timeout":
            raise GatewayQueueTimeoutError("gateway worker queue wait timed out") from exc
        if status == 504:
            raise GatewayTimeoutError("gateway order-book stream timed out") from exc
        if status in {502, 503}:
            raise GatewayUnavailableError(
                "gateway order-book stream is unavailable"
            ) from exc
        raise GatewayResponseError(
            f"gateway WebSocket upgrade failed with status {status}"
        ) from exc

    @asynccontextmanager
    async def stream_order_books(
        self,
        symbols: Sequence[str],
        *,
        venue: str,
    ) -> AsyncIterator[AsyncIterator[OrderBookSnapshotV1]]:
        """Open one authenticated, non-reconnecting venue order-book stream.

        The context manager owns exactly one WebSocket. Leaving the context closes it,
        including after partial iteration or cancellation. Every message is validated
        before its snapshot is yielded.
        """
        requested = _normalize_order_book_symbols(symbols)
        normalized_venue = _normalize_order_book_venue(venue)
        url = self._order_book_stream_url(requested, normalized_venue)
        try:
            connection = await connect(
                url,
                additional_headers={"X-Internal-API-Key": self._api_key},
                open_timeout=self._timeout_seconds,
                close_timeout=self._timeout_seconds,
                ping_interval=30,
            )
        except InvalidStatus as exc:
            self._raise_websocket_status(exc)
        except TimeoutError as exc:
            raise GatewayTimeoutError("gateway order-book stream timed out") from exc
        except (OSError, WebSocketException) as exc:
            raise GatewayUnavailableError("gateway order-book stream failed") from exc

        self._order_book_connections.add(connection)

        async def snapshots() -> AsyncIterator[OrderBookSnapshotV1]:
            try:
                async for payload in connection:
                    if not isinstance(payload, str):
                        raise GatewayResponseError(
                            "gateway returned a non-text order-book stream payload"
                        )
                    try:
                        envelope = OrderBookStreamEnvelopeV1.model_validate_json(payload)
                    except (ValueError, ValidationError) as exc:
                        raise GatewayResponseError(
                            "gateway returned an invalid order-book stream contract"
                        ) from exc
                    snapshot = envelope.snapshot
                    if snapshot.venue != normalized_venue:
                        raise GatewayResponseError(
                            "gateway streamed a mismatched order-book venue"
                        )
                    if snapshot.symbol not in requested:
                        raise GatewayResponseError(
                            "gateway streamed an unrequested order-book symbol"
                        )
                    yield snapshot
            except ConnectionClosedError as exc:
                raise GatewayUnavailableError(
                    "gateway order-book stream closed unexpectedly"
                ) from exc
            except (OSError, WebSocketException) as exc:
                raise GatewayUnavailableError("gateway order-book stream failed") from exc

        try:
            yield snapshots()
        finally:
            self._order_book_connections.discard(connection)
            await connection.close()

    async def close(self) -> None:
        connections = tuple(self._order_book_connections)
        self._order_book_connections.clear()
        for connection in connections:
            await connection.close()
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> GatewayMarketDataClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
