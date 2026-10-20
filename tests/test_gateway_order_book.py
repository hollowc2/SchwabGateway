"""Deterministic tests for venue-specific order-book normalization and the live feed."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from schwab_gateway_sdk.models import (
    OrderBookLevelV1,
    OrderBookParticipantV1,
    OrderBookSnapshotV1,
)

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.order_book import (
    OrderBookMalformedError,
    normalize_schwab_book_message,
)
from schwab_gateway.order_book_live import OrderBookLiveFeed
from schwab_gateway.order_book_store import OrderBookSnapshotStore
from schwab_gateway.scheduler import ExecutionScheduler

UTC = dt.timezone.utc
RECEIVED_AT = dt.datetime(2026, 8, 27, 16, 0, tzinfo=UTC)
BOOK_TIME_MILLIS = 1_777_305_600_000


def _book_message(*, sequence: int = 10) -> dict[str, Any]:
    return {
        "service": "NASDAQ_BOOK",
        "timestamp": BOOK_TIME_MILLIS,
        "command": "SUBS",
        "content": [
            {
                "seq": sequence,
                "key": "AAPL",
                "BOOK_TIME": BOOK_TIME_MILLIS,
                "BIDS": [
                    {
                        "BID_PRICE": 200.1,
                        "TOTAL_VOLUME": 7,
                        "NUM_BIDS": 2,
                        "BIDS": [
                            {"EXCHANGE": "Q", "BID_VOLUME": 5, "SEQUENCE": 100},
                            {"EXCHANGE": "P", "BID_VOLUME": 2, "SEQUENCE": 101},
                        ],
                    },
                    {
                        "BID_PRICE": 200.2,
                        "TOTAL_VOLUME": 3,
                        "NUM_BIDS": 1,
                        "BIDS": [{"EXCHANGE": "Q", "BID_VOLUME": 3, "SEQUENCE": 102}],
                    },
                ],
                "ASKS": [
                    {
                        "ASK_PRICE": 200.4,
                        "TOTAL_VOLUME": 4,
                        "NUM_ASKS": 1,
                        "ASKS": [{"EXCHANGE": "Q", "ASK_VOLUME": 4, "SEQUENCE": 103}],
                    },
                    {
                        "ASK_PRICE": 200.3,
                        "TOTAL_VOLUME": 6,
                        "NUM_ASKS": 1,
                        "ASKS": [{"EXCHANGE": "P", "ASK_VOLUME": 6, "SEQUENCE": 104}],
                    },
                ],
            }
        ],
    }


def test_normalizer_produces_sorted_venue_specific_snapshot() -> None:
    (snapshot,) = normalize_schwab_book_message(
        _book_message(),
        venue="NASDAQ",
        gateway_received_at=RECEIVED_AT,
    )

    assert snapshot.symbol == "AAPL"
    assert snapshot.venue == "NASDAQ"
    assert snapshot.service == "NASDAQ_BOOK"
    assert snapshot.is_consolidated is False
    assert snapshot.sequence == 10
    assert [level.price for level in snapshot.bids] == [200.2, 200.1]
    assert [level.price for level in snapshot.asks] == [200.3, 200.4]
    assert snapshot.bids[1].participants[0].exchange == "Q"
    assert snapshot.event_timestamp == dt.datetime.fromtimestamp(BOOK_TIME_MILLIS / 1000, tz=UTC)
    assert snapshot.data_quality_flags == ()


def test_normalizer_discloses_quality_issues_without_inventing_depth() -> None:
    message = _book_message()
    content = message["content"][0]
    content.pop("BOOK_TIME")
    content["ASKS"] = []
    content["BIDS"][0]["TOTAL_VOLUME"] = 99
    content["BIDS"][0]["NUM_BIDS"] = 3

    (snapshot,) = normalize_schwab_book_message(
        message,
        venue="NASDAQ",
        gateway_received_at=RECEIVED_AT,
    )

    assert set(snapshot.data_quality_flags) == {
        "missing_book_timestamp",
        "empty_ask_book",
        "participant_count_mismatch",
        "participant_size_mismatch",
    }


@pytest.mark.parametrize(
    "mutate",
    [
        lambda message: message.update(service="NYSE_BOOK"),
        lambda message: message["content"][0].update(key=""),
        lambda message: message["content"][0]["BIDS"][0].update(BID_PRICE=-1),
        lambda message: message["content"][0]["ASKS"][0].update(ASKS=None),
    ],
)
def test_normalizer_fails_closed_on_malformed_content(mutate) -> None:
    message = _book_message()
    mutate(message)
    with pytest.raises(OrderBookMalformedError):
        normalize_schwab_book_message(
            message,
            venue="NASDAQ",
            gateway_received_at=RECEIVED_AT,
        )


def test_transport_models_reject_consolidated_or_unsorted_books() -> None:
    level = OrderBookLevelV1(
        price=100,
        total_size=1,
        participant_count=1,
        participants=(OrderBookParticipantV1(exchange="Q", size=1),),
    )
    with pytest.raises(ValidationError):
        OrderBookSnapshotV1(
            symbol="AAPL",
            venue="NASDAQ",
            service="NASDAQ_BOOK",
            gateway_received_at=RECEIVED_AT,
            is_consolidated=True,
            bids=(level,),
            asks=(level,),
        )
    with pytest.raises(ValidationError):
        OrderBookSnapshotV1(
            symbol="AAPL",
            venue="NASDAQ",
            service="NASDAQ_BOOK",
            gateway_received_at=RECEIVED_AT,
            bids=(
                level.model_copy(update={"price": 99}),
                level.model_copy(update={"price": 100}),
            ),
            asks=(level,),
        )


class _FakeAsyncClient:
    def __init__(self) -> None:
        self.closed = False

    async def close_async_session(self) -> None:
        self.closed = True


class _ScopedAsyncManager:
    def __init__(self) -> None:
        self.in_transaction = False
        self.calls = 0

    async def run_access_transaction_async(self, operation):
        self.calls += 1
        self.in_transaction = True
        try:
            return await operation(lambda: {}, lambda _token: None)
        finally:
            self.in_transaction = False


async def test_live_feed_retains_backoff_until_validated_data_arrives(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manager = _ScopedAsyncManager()
    delays: list[int] = []

    class FailingSubscriptionStream:
        async def login(self) -> None:
            assert manager.in_transaction

        def set_json_decoder(self, _decoder: Any) -> None:
            pass

        def add_nasdaq_book_handler(self, _handler: Any) -> None:
            pass

        async def nasdaq_book_subs(self, _symbols: list[str]) -> None:
            raise ConnectionError("synthetic subscription failure")

        async def logout(self) -> None:
            pass

    async def fake_sleep(delay: int) -> None:
        delays.append(delay)
        if len(delays) == 3:
            raise asyncio.CancelledError

    monkeypatch.setattr("schwab_gateway.order_book_live.asyncio.sleep", fake_sleep)
    settings = GatewayUpstreamSettings(
        SCHWAB_API_KEY="fake-key",
        SCHWAB_SECRET_KEY="fake-secret",
        SCHWAB_TOKEN_PATH=tmp_path / "tokens.json",
    )
    store = OrderBookSnapshotStore()
    scheduler = ExecutionScheduler(AdmissionPolicy(protected_capacity=1, background_capacity=1))
    feed = OrderBookLiveFeed(
        manager,  # type: ignore[arg-type]
        settings,
        lambda *_args, **_kwargs: _FakeAsyncClient(),
        store,
        venue="NASDAQ",
        symbols=("AAPL",),
        scheduler=scheduler,
        queue_timeout_seconds=1,
        stream_client_factory=lambda _client: FailingSubscriptionStream(),
    )

    with pytest.raises(asyncio.CancelledError):
        await feed.run_forever()

    assert delays == [1, 2, 4]
    assert manager.calls == 3
    assert scheduler.snapshot().total == 0
    assert store.feed_state("NASDAQ") == "disconnected"
