"""Authenticated recent-snapshot HTTP and WebSocket order-book contracts."""

from __future__ import annotations

import asyncio
import datetime as dt

import pytest
from aiohttp import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer
from schwab_gateway_sdk.models import OrderBookLevelV1, OrderBookSnapshotV1

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.api import create_app
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)
from schwab_gateway.order_book_store import OrderBookSnapshotStore

UTC = dt.timezone.utc


class _UnusedQuoteUpstream:
    async def get_quotes(self, _symbols):
        return ()


def _authenticator() -> InternalKeyAuthenticator:
    return InternalKeyAuthenticator(
        (
            InternalPrincipal(
                client_id="butterfly-guy",
                key_sha256=hash_api_key("valid-key"),
                capabilities=frozenset({"market_data:read"}),
                priority_class=PriorityClass.PROTECTED,
            ),
        )
    )


def _snapshot(sequence: int) -> OrderBookSnapshotV1:
    now = dt.datetime(2026, 8, 27, 17, 0, sequence, tzinfo=UTC)
    return OrderBookSnapshotV1(
        symbol="AAPL",
        venue="NASDAQ",
        service="NASDAQ_BOOK",
        sequence=sequence,
        event_timestamp=now,
        gateway_received_at=now,
        bids=(
            OrderBookLevelV1(price=100, total_size=10, participant_count=0),
        ),
        asks=(
            OrderBookLevelV1(price=100.1, total_size=8, participant_count=0),
        ),
    )


@pytest.mark.asyncio
async def test_recent_order_book_is_authenticated_bounded_and_venue_specific() -> None:
    store = OrderBookSnapshotStore(history_limit=10)
    store.publish(_snapshot(1))
    store.publish(_snapshot(2))
    client = TestClient(
        TestServer(
            create_app(
                _UnusedQuoteUpstream(),
                _authenticator(),
                order_book_store=store,
            )
        )
    )
    await client.start_server()
    try:
        unauthorized = await client.get(
            "/v1/order-book/recent?symbol=AAPL&venue=NASDAQ"
        )
        response = await client.get(
            "/v1/order-book/recent?symbol=AAPL&venue=NASDAQ&limit=1",
            headers={"X-Internal-API-Key": "valid-key"},
        )
        payload = await response.json()
    finally:
        await client.close()

    assert unauthorized.status == 401
    assert response.status == 200
    assert payload["venue"] == "NASDAQ"
    assert payload["is_consolidated"] is False
    assert payload["stale"] is False
    assert payload["age_seconds"] >= 0
    assert [item["sequence"] for item in payload["snapshots"]] == [2]


@pytest.mark.asyncio
async def test_order_book_websocket_requires_auth_and_fans_out_snapshots() -> None:
    store = OrderBookSnapshotStore(history_limit=10)
    store.mark_feed_state("NASDAQ", "connected")
    client = TestClient(
        TestServer(
            create_app(
                _UnusedQuoteUpstream(),
                _authenticator(),
                order_book_store=store,
            )
        )
    )
    await client.start_server()
    try:
        with pytest.raises(WSServerHandshakeError) as denied:
            await client.ws_connect(
                "/v1/order-book/stream?symbols=AAPL&venue=NASDAQ"
            )
        socket = await client.ws_connect(
            "/v1/order-book/stream?symbols=AAPL&venue=NASDAQ",
            headers={"X-Internal-API-Key": "valid-key"},
        )
        store.publish(_snapshot(3))
        message = await socket.receive_json(timeout=1)
        await socket.close()
    finally:
        await client.close()

    assert denied.value.status == 401
    assert message["type"] == "order_book_snapshot"
    assert message["snapshot"]["sequence"] == 3


@pytest.mark.asyncio
async def test_failed_websocket_upgrade_does_not_leak_subscription() -> None:
    store = OrderBookSnapshotStore(history_limit=10)
    store.mark_feed_state("NASDAQ", "connected")
    client = TestClient(
        TestServer(
            create_app(
                _UnusedQuoteUpstream(),
                _authenticator(),
                order_book_store=store,
            )
        )
    )
    await client.start_server()
    try:
        response = await client.get(
            "/v1/order-book/stream?symbols=AAPL&venue=NASDAQ",
            headers={"X-Internal-API-Key": "valid-key"},
        )
    finally:
        await client.close()

    assert response.status == 400
    assert store.subscription_count == 0


@pytest.mark.asyncio
async def test_order_book_websocket_capacity_is_bounded_and_released() -> None:
    store = OrderBookSnapshotStore(history_limit=10)
    store.mark_feed_state("NASDAQ", "connected")
    client = TestClient(
        TestServer(
            create_app(
                _UnusedQuoteUpstream(),
                _authenticator(),
                order_book_store=store,
                order_book_stream_policy=AdmissionPolicy(
                    protected_capacity=1,
                    background_capacity=1,
                ),
            )
        )
    )
    await client.start_server()
    path = "/v1/order-book/stream?symbols=AAPL&venue=NASDAQ"
    headers = {"X-Internal-API-Key": "valid-key"}
    try:
        first = await client.ws_connect(path, headers=headers)
        with pytest.raises(WSServerHandshakeError) as rejected:
            await client.ws_connect(path, headers=headers)
        await first.close()
        await asyncio.sleep(0)
        replacement = await client.ws_connect(path, headers=headers)
        await replacement.close()
    finally:
        await client.close()

    assert rejected.value.status == 429
    assert store.subscription_count == 0


@pytest.mark.asyncio
async def test_recent_order_book_fails_closed_when_snapshot_is_stale() -> None:
    now = [0.0]
    store = OrderBookSnapshotStore(
        history_limit=10,
        monotonic_clock=lambda: now[0],
    )
    store.publish(_snapshot(1))
    now[0] = 16.0
    client = TestClient(
        TestServer(
            create_app(
                _UnusedQuoteUpstream(),
                _authenticator(),
                order_book_store=store,
                order_book_max_age_seconds=15,
            )
        )
    )
    await client.start_server()
    try:
        response = await client.get(
            "/v1/order-book/recent?symbol=AAPL&venue=NASDAQ",
            headers={"X-Internal-API-Key": "valid-key"},
        )
        payload = await response.json()
    finally:
        await client.close()

    assert response.status == 503
    assert payload["error"]["code"] == "order_book_unavailable"
    assert "snapshot_stale" in payload["error"]["message"]


def test_slow_order_book_subscriber_drops_oldest_without_growing_unbounded() -> None:
    store = OrderBookSnapshotStore(history_limit=2, subscriber_queue_limit=1)
    subscription = store.subscribe(frozenset({"AAPL"}), "NASDAQ")
    store.publish(_snapshot(1))
    store.publish(_snapshot(2))

    assert subscription.queue.qsize() == 1
    assert subscription.queue.get_nowait().sequence == 2
    assert store.subscriber_drop_count == 1
