"""Capture bounded Schwab venue order books as auditable research evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from schwab_gateway.live_provider import GatewayUpstreamSettings
from schwab_gateway.logging import get_logger, setup_logging
from schwab_gateway.order_book_capture import (
    OrderBookCaptureRequest,
    OrderBookResearchRecorder,
    parse_symbols,
    run_exclusive_order_book_capture,
)

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--venue",
        required=True,
        choices=("NASDAQ", "NYSE"),
        help="venue-specific Schwab equity book to capture",
    )
    parser.add_argument(
        "--symbols",
        required=True,
        action="append",
        help="comma-separated symbols; may be repeated, maximum 25",
    )
    parser.add_argument(
        "--duration-seconds",
        required=True,
        type=float,
        help="bounded capture duration from 1 through 86400 seconds",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        type=Path,
        help="absolute parent directory for a new, non-overwriting run directory",
    )
    parser.add_argument(
        "--display-timezone",
        default="America/New_York",
        help="manifest interpretation timezone; evidence timestamps remain UTC",
    )
    parser.add_argument("--authorize-real-credential-read", action="store_true")
    parser.add_argument(
        "--confirm-exclusive-token-lock",
        action="store_true",
        help="confirm that no HTTP gateway or other token consumer will run concurrently",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not (args.authorize_real_credential_read and args.confirm_exclusive_token_lock):
        parser.error(
            "order-book capture requires explicit real-credential and exclusive-token-lock "
            "confirmations"
        )
    try:
        request = OrderBookCaptureRequest(
            venue=args.venue,
            symbols=parse_symbols(args.symbols),
            duration_seconds=args.duration_seconds,
            output_root=args.output_root,
            display_timezone=args.display_timezone,
        )
    except ValueError as exc:
        parser.error(str(exc))

    setup_logging(json_output=True)
    from schwab.auth import client_from_access_functions

    recorder = OrderBookResearchRecorder(request)
    try:
        manifest = run_exclusive_order_book_capture(
            request,
            GatewayUpstreamSettings(),
            client_from_access_functions,
            recorder=recorder,
        )
    except Exception as exc:
        log.error(
            "schwab_order_book_capture_failed",
            venue=request.venue,
            symbols=request.symbols,
            failure_class=type(exc).__name__,
            manifest_path=(
                str(recorder.manifest_path) if recorder.manifest_path is not None else None
            ),
        )
        raise SystemExit(1) from None
    log.info(
        "schwab_order_book_capture_completed",
        venue=request.venue,
        symbols=request.symbols,
        manifest_path=str(manifest),
    )


if __name__ == "__main__":
    main()
