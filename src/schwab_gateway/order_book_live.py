"""Long-running venue order-book feed for the gateway's bounded recent store."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import Callable, Mapping
from typing import Any

from schwab.contrib.util import StreamJsonDecoder
from schwab_token_store import AtomicTokenManager

from schwab_gateway.auth import PriorityClass
from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.logging import get_logger
from schwab_gateway.order_book import (
    BOOK_SERVICE_BY_VENUE,
    OrderBookMalformedError,
    OrderBookVenue,
    normalize_schwab_book_message,
)
from schwab_gateway.order_book_capture import bootstrap_stream_under_token_lock
from schwab_gateway.order_book_store import OrderBookSnapshotStore
from schwab_gateway.scheduler import ExecutionScheduler

UTC = dt.timezone.utc
log = get_logger(__name__)


class _LiveBookDecoder(StreamJsonDecoder):
    def __init__(self, service: str) -> None:
        self._service = service
        self.last_received_at: dt.datetime | None = None

    def decode_json_string(self, raw: str) -> Any:
        payload = json.loads(raw)
        if isinstance(payload, Mapping):
            data = payload.get("data")
            if isinstance(data, list) and any(
                isinstance(item, Mapping) and item.get("service") == self._service
                for item in data
            ):
                self.last_received_at = dt.datetime.now(UTC)
        return payload


class OrderBookLiveFeed:
    """Reconnect a configured read-only stream and publish normalized snapshots."""

    def __init__(
        self,
        manager: AtomicTokenManager,
        upstream_settings: GatewayUpstreamSettings,
        client_factory: Callable[..., Any],
        store: OrderBookSnapshotStore,
        *,
        venue: OrderBookVenue,
        symbols: tuple[str, ...],
        scheduler: ExecutionScheduler,
        queue_timeout_seconds: float,
        stream_client_factory: Callable[[Any], Any] | None = None,
        login_timeout_seconds: float = 8.0,
    ) -> None:
        if not symbols:
            raise ValueError("live order-book feed requires at least one symbol")
        if len(symbols) > 25:
            raise ValueError("live order-book feed supports at most 25 symbols")
        self._manager = manager
        self._upstream_settings = upstream_settings
        self._client_factory = client_factory
        self._store = store
        self._venue = venue
        self._symbols = symbols
        self._scheduler = scheduler
        self._queue_timeout_seconds = queue_timeout_seconds
        self._stream_client_factory = stream_client_factory
        self._login_timeout_seconds = login_timeout_seconds

    async def run_forever(self) -> None:
        if self._stream_client_factory is None:
            from schwab.streaming import StreamClient

            stream_client_factory = StreamClient
        else:
            stream_client_factory = self._stream_client_factory
        failures = 0
        connection_id = 0
        while True:
            stream: Any = None
            self._store.mark_feed_state(self._venue, "connecting")
            try:
                stream = await self._scheduler.execute(
                    PriorityClass.BACKGROUND,
                    "order_book_stream_login",
                    lambda: bootstrap_stream_under_token_lock(
                        self._manager,
                        self._upstream_settings,
                        self._client_factory,
                        stream_client_factory,
                        login_timeout_seconds=self._login_timeout_seconds,
                    ),
                    queue_timeout_seconds=self._queue_timeout_seconds,
                    execution_timeout_seconds=self._login_timeout_seconds,
                )
                connection_id += 1
                decoder = _LiveBookDecoder(BOOK_SERVICE_BY_VENUE[self._venue])
                seen_symbols: set[str] = set()

                def handle_book(message: Any) -> None:
                    nonlocal failures
                    if decoder.last_received_at is None:
                        return
                    try:
                        snapshots = normalize_schwab_book_message(
                            message,
                            venue=self._venue,
                            gateway_received_at=decoder.last_received_at,
                        )
                    except OrderBookMalformedError:
                        return
                    if snapshots:
                        # Reset backoff only after the connection carries validated data,
                        # not merely after login. Subscription/transport failure loops
                        # therefore retain exponential backoff and stop hammering tokens.
                        failures = 0
                    for snapshot in snapshots:
                        flags = list(snapshot.data_quality_flags)
                        if snapshot.sequence is None:
                            flags.append("missing_sequence")
                        if connection_id > 1 and snapshot.symbol not in seen_symbols:
                            flags.append("reconnect_boundary")
                        seen_symbols.add(snapshot.symbol)
                        self._store.publish(
                            snapshot.model_copy(
                                update={
                                    "connection_id": connection_id,
                                    "continuity_epoch": connection_id,
                                    "data_quality_flags": tuple(dict.fromkeys(flags)),
                                }
                            )
                        )

                stream.set_json_decoder(decoder)
                if self._venue == "NASDAQ":
                    stream.add_nasdaq_book_handler(handle_book)
                    await stream.nasdaq_book_subs(list(self._symbols))
                else:
                    stream.add_nyse_book_handler(handle_book)
                    await stream.nyse_book_subs(list(self._symbols))
                self._store.mark_feed_state(self._venue, "connected")
                log.info(
                    "gateway_order_book_stream_connected",
                    venue=self._venue,
                    symbol_count=len(self._symbols),
                    connection_id=connection_id,
                )
                while True:
                    await stream.handle_message()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._store.mark_feed_state(self._venue, "disconnected")
                failures += 1
                delay = min(2 ** min(failures - 1, 3), 8)
                log.warning(
                    "gateway_order_book_stream_reconnecting",
                    venue=self._venue,
                    failure_class=type(exc).__name__,
                    retry_delay_seconds=delay,
                )
                await asyncio.sleep(delay)
            finally:
                self._store.mark_feed_state(self._venue, "disconnected")
                if stream is not None:
                    try:
                        await stream.logout()
                    except Exception:
                        pass
