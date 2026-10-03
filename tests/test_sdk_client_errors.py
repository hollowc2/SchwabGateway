"""Pin the SDK's exact exception class and message for every HTTP failure shape."""

from __future__ import annotations

import datetime as dt

import httpx
import pytest
from schwab_gateway_sdk.client import (
    GatewayAuthenticationError,
    GatewayAuthorizationError,
    GatewayCapacityError,
    GatewayMarketDataClient,
    GatewayQueueTimeoutError,
    GatewayResponseError,
    GatewayTimeoutError,
    GatewayUnavailableError,
)


def _error_body(code: str) -> dict:
    return {"schema_version": "1.0", "error": {"code": code, "message": "x"}}


# (status, json body, exception class, message template with {noun})
STATUS_CASES = [
    (401, None, GatewayAuthenticationError, "gateway authentication failed"),
    (403, None, GatewayAuthorizationError, "gateway capability denied"),
    (429, None, GatewayCapacityError, "gateway request capacity is unavailable"),
    (
        503,
        _error_body("gateway_queue_timeout"),
        GatewayQueueTimeoutError,
        "gateway worker queue wait timed out",
    ),
    (504, None, GatewayTimeoutError, "gateway {noun} upstream timed out"),
    (502, None, GatewayUnavailableError, "gateway upstream is unavailable"),
    (
        503,
        _error_body("upstream_unavailable"),
        GatewayUnavailableError,
        "gateway upstream is unavailable",
    ),
    (418, None, GatewayResponseError, "gateway {noun} request failed with status 418"),
    (
        200,
        {"schema_version": "1.0"},
        GatewayResponseError,
        "gateway returned an invalid {noun} contract",
    ),
]
TRANSPORT_CASES = [
    (httpx.ReadTimeout("slow"), GatewayTimeoutError, "gateway {noun} request timed out"),
    (httpx.ConnectError("down"), GatewayUnavailableError, "gateway {noun} request unavailable"),
]
CALLS = [
    ("quote", lambda client: client.get_quotes(["AAPL"])),
    ("quote", lambda client: client.get_available_quotes(["AAPL"])),
    ("market data", lambda client: client.get_spot("$SPX")),
    ("market data", lambda client: client.get_option_chain("SPX", dt.date(2026, 9, 29))),
]


def _client(handler) -> GatewayMarketDataClient:
    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(base_url="http://gateway.test", transport=transport)
    return GatewayMarketDataClient("http://gateway.test", "key", client=http)


@pytest.mark.parametrize(("noun", "call"), CALLS)
@pytest.mark.parametrize(("status", "body", "error", "message"), STATUS_CASES)
async def test_status_maps_to_exact_exception(noun, call, status, body, error, message) -> None:
    client = _client(lambda _request: httpx.Response(status, json=body))

    with pytest.raises(error) as raised:
        await call(client)

    assert type(raised.value) is error
    assert str(raised.value) == message.format(noun=noun)


@pytest.mark.parametrize(("noun", "call"), CALLS)
@pytest.mark.parametrize(("failure", "error", "message"), TRANSPORT_CASES)
async def test_transport_failure_maps_to_exact_exception(noun, call, failure, error, message):
    def handler(_request: httpx.Request) -> httpx.Response:
        raise failure

    with pytest.raises(error) as raised:
        await call(_client(handler))

    assert type(raised.value) is error
    assert str(raised.value) == message.format(noun=noun)


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda client: client.get_spot("  "), "a symbol is required"),
        (
            lambda client: client.get_chain_metadata("", dt.date(2026, 9, 29)),
            "a symbol is required",
        ),
        (
            lambda client: client.get_option_chain("SPX", dt.datetime(2026, 9, 29)),
            "an expiration date is required",
        ),
        (
            lambda client: client.get_chain_metadata("SPX", "2026-09-29"),
            "an expiration date is required",
        ),
        (lambda client: client.get_history(" "), "a symbol is required"),
        (lambda client: client.get_movers(" "), "an index is required"),
        (
            lambda client: client.get_session_history("AAPL", dt.datetime(2026, 9, 29)),
            "a date is required",
        ),
        (
            lambda client: client.get_session_history(" ", dt.date(2026, 9, 29)),
            "a symbol is required",
        ),
        (lambda client: client.get_recent_order_book(" ", venue="NASDAQ"), "a symbol is required"),
        (
            lambda client: client.get_recent_order_book("AAPL", venue="arca"),
            "venue must be 'NASDAQ' or 'NYSE'",
        ),
        (lambda client: client.get_quotes([]), "at least one symbol is required"),
    ],
)
async def test_argument_validation_messages_are_pinned(call, message) -> None:
    client = _client(lambda _request: httpx.Response(500))

    with pytest.raises(ValueError) as raised:
        await call(client)

    assert type(raised.value) is ValueError
    assert str(raised.value) == message
