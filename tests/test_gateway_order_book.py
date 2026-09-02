"""Deterministic tests for venue-specific order-book research capture."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
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
from schwab_gateway.capture_order_books import build_parser
from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.order_book import (
    OrderBookMalformedError,
    normalize_schwab_book_message,
)
from schwab_gateway.order_book_capture import (
    CapturingBookJsonDecoder,
    OrderBookCaptureRequest,
    OrderBookResearchRecorder,
    capture_order_book_stream,
    capture_order_book_with_reconnects,
    parse_symbols,
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
                        "BIDS": [
                            {"EXCHANGE": "Q", "BID_VOLUME": 3, "SEQUENCE": 102}
                        ],
                    },
                ],
                "ASKS": [
                    {
                        "ASK_PRICE": 200.4,
                        "TOTAL_VOLUME": 4,
                        "NUM_ASKS": 1,
                        "ASKS": [
                            {"EXCHANGE": "Q", "ASK_VOLUME": 4, "SEQUENCE": 103}
                        ],
                    },
                    {
                        "ASK_PRICE": 200.3,
                        "TOTAL_VOLUME": 6,
                        "NUM_ASKS": 1,
                        "ASKS": [
                            {"EXCHANGE": "P", "ASK_VOLUME": 6, "SEQUENCE": 104}
                        ],
                    },
                ],
            }
        ],
    }


def _request(tmp_path: Path, *, duration_seconds: float = 1) -> OrderBookCaptureRequest:
    return OrderBookCaptureRequest(
        venue="NASDAQ",
        symbols=("AAPL",),
        duration_seconds=duration_seconds,
        output_root=tmp_path,
    )


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
    assert snapshot.event_timestamp == dt.datetime.fromtimestamp(
        BOOK_TIME_MILLIS / 1000, tz=UTC
    )
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


def test_recorder_preserves_raw_frames_and_manifests_sequence_gaps(tmp_path: Path) -> None:
    times = iter(
        (
            RECEIVED_AT,
            RECEIVED_AT + dt.timedelta(seconds=2),
        )
    )
    recorder = OrderBookResearchRecorder(_request(tmp_path), clock=lambda: next(times))
    recorder.start()
    raw = '{"data":[{"service":"NASDAQ_BOOK","content":[]}]}'
    recorder.record_raw_frame(raw, RECEIVED_AT)
    first = normalize_schwab_book_message(
        _book_message(sequence=10), venue="NASDAQ", gateway_received_at=RECEIVED_AT
    )[0]
    second = normalize_schwab_book_message(
        _book_message(sequence=13), venue="NASDAQ", gateway_received_at=RECEIVED_AT
    )[0]
    recorder.record_snapshot(first)
    recorder.record_snapshot(second)
    manifest_path = recorder.finalize(termination_reason="completed")

    assert recorder.raw_path is not None
    expected_raw = b"\x1e" + raw.encode() + b"\n"
    assert recorder.raw_path.read_bytes() == expected_raw
    manifest = json.loads(manifest_path.read_text())
    assert manifest["is_consolidated"] is False
    assert manifest["path_scope"] == "relative_to_manifest"
    assert manifest["raw_path"] == "raw_frames.jsonseq"
    assert manifest["normalized_path"] == "normalized_snapshots.ndjson"
    assert manifest["raw_sha256"] == hashlib.sha256(expected_raw).hexdigest()
    assert manifest["raw_frame_count"] == 1
    assert manifest["normalized_snapshot_count"] == 2
    assert manifest["sequence_gap_count"] == 1
    assert manifest["missing_sequence_count"] == 2
    assert manifest["snapshots_without_sequence_count"] == 0
    assert manifest["sequence_continuity_observable"] is True
    assert manifest["dropped_event_count"] == 0
    assert manifest["termination_reason"] == "completed"
    assert recorder.normalized_path is not None
    normalized = [json.loads(line) for line in recorder.normalized_path.read_text().splitlines()]
    assert "sequence_gap" in normalized[1]["data_quality_flags"]


def test_recorder_discloses_when_sequence_continuity_is_unobservable(tmp_path: Path) -> None:
    times = iter((RECEIVED_AT, RECEIVED_AT + dt.timedelta(seconds=1)))
    recorder = OrderBookResearchRecorder(_request(tmp_path), clock=lambda: next(times))
    recorder.start()
    snapshot = normalize_schwab_book_message(
        _book_message(), venue="NASDAQ", gateway_received_at=RECEIVED_AT
    )[0].model_copy(update={"sequence": None})
    recorder.record_snapshot(snapshot)
    manifest = json.loads(
        recorder.finalize(termination_reason="completed").read_text()
    )

    assert manifest["snapshots_without_sequence_count"] == 1
    assert manifest["sequence_continuity_observable"] is False
    assert recorder.normalized_path is not None
    normalized = json.loads(recorder.normalized_path.read_text())
    assert "missing_sequence" in normalized["data_quality_flags"]


def test_decoder_captures_only_target_service_before_relabeling(tmp_path: Path) -> None:
    times = iter((RECEIVED_AT, RECEIVED_AT + dt.timedelta(seconds=1)))
    recorder = OrderBookResearchRecorder(_request(tmp_path), clock=lambda: next(times))
    recorder.start()
    decoder = CapturingBookJsonDecoder(
        service="NASDAQ_BOOK",
        recorder=recorder,
        clock=lambda: RECEIVED_AT,
    )
    raw_target = '{"data":[{"service":"NASDAQ_BOOK","content":[]}]}'
    raw_heartbeat = '{"notify":[{"heartbeat":"1777305600000"}]}'

    assert decoder.decode_json_string(raw_target)["data"][0]["service"] == "NASDAQ_BOOK"
    decoder.decode_json_string(raw_heartbeat)
    recorder.finalize(termination_reason="completed")

    assert recorder.raw_frame_count == 1
    assert decoder.last_received_at == RECEIVED_AT


class _FakeAsyncClient:
    def __init__(self) -> None:
        self.closed = False

    async def close_async_session(self) -> None:
        self.closed = True


class _DisconnectingStream:
    def __init__(self, _client: Any) -> None:
        self.decoder = None
        self.handler = None
        self.logged_in = False
        self.logged_out = False
        self.subscriptions: list[str] = []

    async def login(self) -> None:
        self.logged_in = True

    def set_json_decoder(self, decoder: Any) -> None:
        self.decoder = decoder

    def add_nasdaq_book_handler(self, handler: Any) -> None:
        self.handler = handler

    async def nasdaq_book_subs(self, symbols: list[str]) -> None:
        self.subscriptions = symbols

    async def handle_message(self) -> None:
        assert self.decoder is not None
        assert self.handler is not None
        raw = json.dumps({"data": [{"service": "NASDAQ_BOOK", "content": []}]})
        self.decoder.decode_json_string(raw)
        self.handler(_book_message())
        raise ConnectionError("synthetic disconnect")

    async def logout(self) -> None:
        self.logged_out = True


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


class _EpochStream(_DisconnectingStream):
    def __init__(self, client: Any, *, disconnect: bool) -> None:
        super().__init__(client)
        self.disconnect = disconnect
        self.handled = False
        self._manager: Any = None

    async def login(self) -> None:
        assert self._manager.in_transaction
        await super().login()

    async def handle_message(self) -> None:
        if not self.handled:
            self.handled = True
            assert self.decoder is not None
            assert self.handler is not None
            raw = json.dumps({"data": [{"service": "NASDAQ_BOOK", "content": []}]})
            self.decoder.decode_json_string(raw)
            self.handler(_book_message())
            if self.disconnect:
                raise ConnectionError("synthetic disconnect")
        await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_capture_stream_closes_client_and_preserves_pre_disconnect_data(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    recorder = OrderBookResearchRecorder(request)
    client = _FakeAsyncClient()
    stream: _DisconnectingStream | None = None

    def factory(fake_client: Any) -> _DisconnectingStream:
        nonlocal stream
        stream = _DisconnectingStream(fake_client)
        return stream

    with pytest.raises(ConnectionError, match="synthetic disconnect"):
        await capture_order_book_stream(
            client,
            request,
            recorder,
            stream_client_factory=factory,
        )

    assert stream is not None
    assert stream.logged_in is True
    assert stream.logged_out is True
    assert stream.subscriptions == ["AAPL"]
    assert client.closed is True
    assert recorder.raw_frame_count == 1
    assert recorder.normalized_snapshot_count == 1


@pytest.mark.asyncio
async def test_shared_bootstrap_reconnects_into_explicit_continuity_epochs(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path, duration_seconds=1)
    recorder = OrderBookResearchRecorder(request)
    manager = _ScopedAsyncManager()
    streams: list[_EpochStream] = []

    def client_factory(*_args, **_kwargs) -> _FakeAsyncClient:
        return _FakeAsyncClient()

    def stream_factory(client: Any) -> _EpochStream:
        stream = _EpochStream(client, disconnect=not streams)
        stream._manager = manager
        streams.append(stream)
        return stream

    settings = GatewayUpstreamSettings(
        SCHWAB_API_KEY="fake-key",
        SCHWAB_SECRET_KEY="fake-secret",
        SCHWAB_TOKEN_PATH=tmp_path / "tokens.json",
    )
    await capture_order_book_with_reconnects(
        manager,  # type: ignore[arg-type]
        settings,
        client_factory,
        request,
        recorder,
        stream_client_factory=stream_factory,
        reconnect_base_delay_seconds=0,
    )
    manifest = json.loads(recorder.finalize(termination_reason="completed").read_text())

    assert manager.calls == 2
    assert manager.in_transaction is False
    assert manifest["connection_attempt_count"] == 2
    assert manifest["successful_connection_count"] == 2
    assert manifest["reconnect_count"] == 1
    assert manifest["continuity_epoch_count"] == 2
    assert recorder.normalized_path is not None
    snapshots = [
        json.loads(line) for line in recorder.normalized_path.read_text().splitlines()
    ]
    assert [item["continuity_epoch"] for item in snapshots] == [1, 2]
    assert "reconnect_boundary" in snapshots[1]["data_quality_flags"]


def test_request_and_cli_keep_scope_explicit(tmp_path: Path) -> None:
    assert parse_symbols(["aapl, msft", "nvda"]) == ("AAPL", "MSFT", "NVDA")
    with pytest.raises(ValueError, match="absolute"):
        OrderBookCaptureRequest(
            venue="NASDAQ",
            symbols=("AAPL",),
            duration_seconds=60,
            output_root=Path("relative"),
        )
    args = build_parser().parse_args(
        [
            "--venue",
            "NYSE",
            "--symbols",
            "IBM",
            "--duration-seconds",
            "60",
            "--output-root",
            str(tmp_path),
        ]
    )
    assert args.venue == "NYSE"


@pytest.mark.asyncio
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
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=1, background_capacity=1)
    )
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
