"""Capture bounded Schwab chart and Level I equity streams as research evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from schwab_gateway.equity_stream_capture import (
    EquityStreamCaptureRequest,
    EquityStreamRecorder,
    run_equity_stream_capture,
)
from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.logging import get_logger, setup_logging
from schwab_gateway.order_book_capture import parse_symbols

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", required=True, action="append")
    parser.add_argument("--duration-seconds", required=True, type=float)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--display-timezone", default="America/New_York")
    parser.add_argument("--authorize-real-credential-read", action="store_true")
    parser.add_argument("--confirm-shared-token-bootstrap", action="store_true")
    parser.add_argument("--max-reconnects", type=int, default=3)
    parser.add_argument("--login-timeout-seconds", type=float, default=8.0)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not (args.authorize_real_credential_read and args.confirm_shared_token_bootstrap):
        parser.error(
            "equity-stream capture requires explicit real-credential and shared-token-"
            "bootstrap confirmations"
        )
    try:
        request = EquityStreamCaptureRequest(
            symbols=parse_symbols(args.symbols),
            duration_seconds=args.duration_seconds,
            output_root=args.output_root,
            display_timezone=args.display_timezone,
        )
    except ValueError as exc:
        parser.error(str(exc))

    setup_logging(json_output=True)
    from schwab.auth import client_from_access_functions

    recorder = EquityStreamRecorder(request)
    try:
        manifest = run_equity_stream_capture(
            request,
            GatewayUpstreamSettings(),
            client_from_access_functions,
            recorder=recorder,
            max_reconnects=args.max_reconnects,
            login_timeout_seconds=args.login_timeout_seconds,
        )
    except Exception as exc:
        log.error(
            "schwab_equity_stream_capture_failed",
            symbols=request.symbols,
            failure_class=type(exc).__name__,
            manifest_path=(
                str(recorder.manifest_path) if recorder.manifest_path is not None else None
            ),
        )
        raise SystemExit(1) from None
    log.info(
        "schwab_equity_stream_capture_completed",
        symbols=request.symbols,
        manifest_path=str(manifest),
    )
