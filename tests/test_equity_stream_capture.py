from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from schwab_gateway.capture_equity_streams import build_parser
from schwab_gateway.equity_stream_capture import (
    CapturingEquityJsonDecoder,
    EquityStreamCaptureRequest,
    EquityStreamRecorder,
    capture_equity_stream_with_reconnects,
)
from schwab_gateway.live_provider import GatewayUpstreamSettings

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 11, 16, 0, tzinfo=UTC)


def _request(tmp_path):
    return EquityStreamCaptureRequest(
        symbols=("aapl", "MSFT"),
        duration_seconds=60,
        output_root=tmp_path,
    )


def test_request_is_bounded_normalized_and_requires_absolute_output(tmp_path):
    request = _request(tmp_path)
    assert request.symbols == ("AAPL", "MSFT")
    with pytest.raises(ValueError, match="unique"):
        EquityStreamCaptureRequest(
            symbols=("AAPL", "aapl"), duration_seconds=60, output_root=tmp_path
        )
    with pytest.raises(ValueError, match="absolute"):
        EquityStreamCaptureRequest(
            symbols=("AAPL",), duration_seconds=60, output_root=Path(tmp_path.name)
        )


def test_decoder_preserves_only_target_raw_frames_and_manifest_hashes(tmp_path):
    times = iter((NOW, NOW, NOW + dt.timedelta(seconds=1), NOW + dt.timedelta(seconds=2)))
    recorder = EquityStreamRecorder(_request(tmp_path), clock=lambda: next(times))
    recorder.start()
    decoder = CapturingEquityJsonDecoder(recorder)
    target = '{"data":[{"service":"CHART_EQUITY","content":[]}]}'
    ignored = '{"data":[{"service":"ACCT_ACTIVITY","content":[]}]}'

    decoder.decode_json_string(target)
    decoder.decode_json_string(ignored)
    recorder.record_message(
        "CHART_EQUITY",
        {"service": "CHART_EQUITY", "content": [{"key": "AAPL"}]},
        received_at=decoder.last_received_at,
    )
    manifest_path = recorder.finalize(termination_reason="completed")
    manifest = json.loads(manifest_path.read_text())

    assert recorder.raw_path is not None
    expected_raw = b"\x1e" + target.encode() + b"\n"
    assert recorder.raw_path.read_bytes() == expected_raw
    assert manifest["raw_sha256"] == hashlib.sha256(expected_raw).hexdigest()
    assert manifest["raw_frame_count"] == 1
    assert manifest["message_counts"] == {
        "CHART_EQUITY": 1,
        "LEVELONE_EQUITIES": 0,
    }
    assert manifest["termination_reason"] == "completed"
    assert manifest["path_scope"] == "relative_to_manifest"
    assert "account" not in json.dumps(manifest).lower()
    assert "token" not in json.dumps(manifest).lower()


def test_cli_requires_explicit_credential_and_shared_token_confirmations():
    parser = build_parser()
    args = parser.parse_args(
        [
            "--symbols",
            "AAPL",
            "--duration-seconds",
            "60",
            "--output-root",
            "/tmp/evidence",
        ]
    )
    assert args.authorize_real_credential_read is False
    assert args.confirm_shared_token_bootstrap is False


async def test_capture_subscribes_to_chart_and_level_one_after_short_locked_bootstrap(
    tmp_path,
):
    class Manager:
        locked = False
        calls = 0

        async def run_access_transaction_async(self, operation):
            self.calls += 1
            self.locked = True
            try:
                return await operation(lambda: {}, lambda _token: None)
            finally:
                self.locked = False

    class Client:
        closed = False

        async def close_async_session(self):
            self.closed = True

    class Stream:
        def __init__(self, client):
            self.client = client
            self.decoder = None
            self.chart_handler = None
            self.level_one_handler = None
            self.chart_symbols = None
            self.level_one_symbols = None
            self.logged_out = False

        async def login(self):
            assert manager.locked is True

        def set_json_decoder(self, decoder):
            self.decoder = decoder

        def add_chart_equity_handler(self, handler):
            self.chart_handler = handler

        def add_level_one_equity_handler(self, handler):
            self.level_one_handler = handler

        async def chart_equity_subs(self, symbols):
            self.chart_symbols = symbols

        async def level_one_equity_subs(self, symbols):
            self.level_one_symbols = symbols

        async def handle_message(self):
            chart = {"service": "CHART_EQUITY", "content": [{"key": "AAPL"}]}
            level_one = {
                "service": "LEVELONE_EQUITIES",
                "content": [{"key": "AAPL"}],
            }
            self.decoder.decode_json_string(json.dumps({"data": [chart]}))
            self.chart_handler(chart)
            self.decoder.decode_json_string(json.dumps({"data": [level_one]}))
            self.level_one_handler(level_one)
            raise TimeoutError

        async def logout(self):
            self.logged_out = True

    manager = Manager()
    clients = []
    streams = []

    def client_factory(*_args, **_kwargs):
        client = Client()
        clients.append(client)
        return client

    def stream_factory(client):
        stream = Stream(client)
        streams.append(stream)
        return stream

    recorder = EquityStreamRecorder(_request(tmp_path))
    settings = GatewayUpstreamSettings(
        SCHWAB_API_KEY="fake-key",
        SCHWAB_SECRET_KEY="fake-secret",
        SCHWAB_TOKEN_PATH=tmp_path / "tokens.json",
    )
    await capture_equity_stream_with_reconnects(
        manager,  # type: ignore[arg-type]
        settings,
        client_factory,
        _request(tmp_path),
        recorder,
        stream_client_factory=stream_factory,
    )
    manifest = json.loads(recorder.finalize(termination_reason="completed").read_text())

    assert manager.calls == 1
    assert manager.locked is False
    assert clients[0].closed is True
    assert streams[0].chart_symbols == ["AAPL", "MSFT"]
    assert streams[0].level_one_symbols == ["AAPL", "MSFT"]
    assert streams[0].logged_out is True
    assert manifest["raw_frame_count"] == 2
    assert manifest["message_counts"] == {
        "CHART_EQUITY": 1,
        "LEVELONE_EQUITIES": 1,
    }
