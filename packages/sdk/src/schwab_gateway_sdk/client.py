"""Fail-closed HTTP client for the read-only gateway contract."""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

import httpx
from pydantic import ValidationError

from schwab_gateway_sdk.models import (
    ChainMetadataResponseV1,
    HistoryResponseV1,
    MoversResponseV1,
    OptionChainResponseV1,
    QuoteResponseV1,
    SessionHistoryResponseV1,
    SpotResponseV1,
)


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


class GatewayCapacityError(GatewayClientError):
    pass


class GatewayResponseError(GatewayClientError):
    pass


class GatewayMarketDataClient:
    """Typed client for gateway market-data endpoints only."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_seconds: float = 5.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("gateway API key is required")
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
        )

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

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> GatewayMarketDataClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
