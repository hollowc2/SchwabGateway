"""Standalone SDK coverage for the authenticated order-book WebSocket client."""

from __future__ import annotations

import asyncio
import json

import pytest
import schwab_gateway_sdk.client as client_module
from aiohttp import web
from aiohttp.test_utils import TestServer
from schwab_gateway_sdk import (
    GatewayAuthenticationError,
    GatewayAuthorizationError,
    GatewayCapacityError,
    GatewayMarketDataClient,
    GatewayResponseError,
    GatewayTimeoutError,
    GatewayUnavailableError,
)


def _snapshot(*, symbol: str = "AAPL", venue: str = "NASDAQ") -> dict:
    return {
        "schema_version": "1.0",
        "symbol": symbol,
        "venue": venue,
        "service": f"{venue}_BOOK",
        "connection_id": 7,
        "continuity_epoch": 3,
        "sequence": 42,
        "event_timestamp": "2026-08-27T17:00:00Z",
        "gateway_received_at": "2026-08-27T17:00:00.100000Z",
        "source": "schwab_streaming",
        "is_consolidated": False,
        "bids": [
            {
                "price": 100.0,
                "total_size": 5,
                "participant_count": 1,
                "participants": [{"exchange": "Q", "size": 5, "sequence": 41}],
            }
        ],
        "asks": [
            {
                "price": 100.1,
                "total_size": 8,
                "participant_count": 0,
                "participants": [],
            }
        ],
        "data_quality_flags": ["sampled"],
    }


def _envelope(*, symbol: str = "AAPL", venue: str = "NASDAQ") -> dict:
    return {
        "schema_version": "1.0",
        "type": "order_book_snapshot",
        "snapshot": _snapshot(symbol=symbol, venue=venue),
    }


@pytest.mark.asyncio
async def test_stream_authenticates_normalizes_query_and_preserves_contract() -> None:
    seen: dict[str, str] = {}

    async def stream(request: web.Request) -> web.WebSocketResponse:
        seen.update(request.query)
        seen["api_key"] = request.headers["X-Internal-API-Key"]
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_json(_envelope())
        await socket.send_json(_envelope(symbol="MSFT"))
        await socket.close()
        return socket

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", stream)
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:
            async with client.stream_order_books(
                [" aapl ", "msft"], venue="nasdaq"
            ) as snapshots:
                received = [snapshot async for snapshot in snapshots]

    assert [snapshot.symbol for snapshot in received] == ["AAPL", "MSFT"]
    first = received[0]
    assert first.connection_id == 7
    assert first.continuity_epoch == 3
    assert first.sequence == 42
    assert first.event_timestamp is not None
    assert first.source == "schwab_streaming"
    assert first.data_quality_flags == ("sampled",)
    assert seen == {
        "symbols": "AAPL,MSFT",
        "venue": "NASDAQ",
        "api_key": "secret",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, GatewayAuthenticationError),
        (403, GatewayAuthorizationError),
        (429, GatewayCapacityError),
        (503, GatewayUnavailableError),
    ],
)
async def test_stream_classifies_failed_upgrades(status: int, error_type: type[Exception]) -> None:
    async def rejected(_request: web.Request) -> web.Response:
        return web.json_response({"error": {"code": "rejected"}}, status=status)

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", rejected)
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:
            with pytest.raises(error_type):
                async with client.stream_order_books(["AAPL"], venue="NASDAQ"):
                    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("not-json", "invalid"),
        (json.dumps({**_envelope(), "unexpected": True}), "invalid"),
        (json.dumps(_envelope(symbol="MSFT")), "unrequested"),
        (json.dumps(_envelope(venue="NYSE")), "mismatched"),
    ],
)
async def test_stream_rejects_malformed_or_mismatched_payloads(
    payload: str, message: str
) -> None:
    async def stream(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_str(payload)
        return socket

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", stream)
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:
            with pytest.raises(GatewayResponseError, match=message):
                async with client.stream_order_books(
                    ["AAPL"], venue="NASDAQ"
                ) as snapshots:
                    await anext(snapshots)


@pytest.mark.asyncio
async def test_stream_normal_server_close_ends_iteration() -> None:
    async def stream(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.close()
        return socket

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", stream)
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:
            async with client.stream_order_books(
                ["AAPL"], venue="NASDAQ"
            ) as snapshots:
                assert [snapshot async for snapshot in snapshots] == []


@pytest.mark.asyncio
async def test_stream_partial_iteration_closes_socket() -> None:
    server_saw_close = asyncio.Event()

    async def stream(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_json(_envelope())
        async for _message in socket:
            pass
        server_saw_close.set()
        return socket

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", stream)
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:
            async with client.stream_order_books(
                ["AAPL"], venue="NASDAQ"
            ) as snapshots:
                assert (await anext(snapshots)).symbol == "AAPL"
        await asyncio.wait_for(server_saw_close.wait(), timeout=1)


@pytest.mark.asyncio
async def test_stream_does_not_reclassify_caller_exceptions() -> None:
    async def stream(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_json(_envelope())
        return socket

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", stream)
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:
            with pytest.raises(OSError, match="caller failure"):
                async with client.stream_order_books(
                    ["AAPL"], venue="NASDAQ"
                ) as snapshots:
                    await anext(snapshots)
                    raise OSError("caller failure")


@pytest.mark.asyncio
async def test_stream_cancellation_closes_socket() -> None:
    snapshot_received = asyncio.Event()
    server_saw_close = asyncio.Event()

    async def stream(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_json(_envelope())
        async for _message in socket:
            pass
        server_saw_close.set()
        return socket

    app = web.Application()
    app.router.add_get("/v1/order-book/stream", stream)
    blocker = asyncio.Event()
    async with TestServer(app) as server:
        async with GatewayMarketDataClient(str(server.make_url("/")), "secret") as client:

            async def consume() -> None:
                async with client.stream_order_books(
                    ["AAPL"], venue="NASDAQ"
                ) as snapshots:
                    await anext(snapshots)
                    snapshot_received.set()
                    await blocker.wait()

            task = asyncio.create_task(consume())
            await asyncio.wait_for(snapshot_received.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(server_saw_close.wait(), timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "error_type"),
    [(TimeoutError(), GatewayTimeoutError), (OSError(), GatewayUnavailableError)],
)
async def test_stream_classifies_connection_failures(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    error_type: type[Exception],
) -> None:
    attempts = 0

    async def fail_connect(*_args: object, **_kwargs: object) -> None:
        nonlocal attempts
        attempts += 1
        raise failure

    monkeypatch.setattr(client_module, "connect", fail_connect)
    async with GatewayMarketDataClient("http://gateway.invalid", "secret") as client:
        with pytest.raises(error_type):
            async with client.stream_order_books(["AAPL"], venue="NASDAQ"):
                pass
    assert attempts == 1


@pytest.mark.parametrize(
    ("symbols", "venue"),
    [
        ([], "NASDAQ"),
        ([""], "NASDAQ"),
        (["AAPL", "aapl"], "NASDAQ"),
        (["A A"], "NASDAQ"),
        (["AAPL"], "ARCA"),
    ],
)
@pytest.mark.asyncio
async def test_stream_rejects_unsafe_or_ambiguous_inputs(
    symbols: list[str], venue: str
) -> None:
    async with GatewayMarketDataClient("http://gateway.invalid", "secret") as client:
        with pytest.raises(ValueError):
            async with client.stream_order_books(symbols, venue=venue):
                pass
