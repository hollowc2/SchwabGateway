"""Export one equity session's gateway candles as non-overwriting evidence."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from schwab_gateway_sdk import GatewayMarketDataClient

UTC = dt.timezone.utc
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9$._/-]{1,32}$")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def export_session_history(
    *,
    client: GatewayMarketDataClient,
    symbol: str,
    date: dt.date,
    output_root: Path,
    clock: Any = None,
) -> Path:
    normalized_symbol = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(normalized_symbol):
        raise ValueError("equity symbol is invalid")
    if not output_root.is_absolute():
        raise ValueError("session-history output root must be absolute")
    now = (clock or (lambda: dt.datetime.now(UTC)))().astimezone(UTC)
    regular, extended = await asyncio.gather(
        client.get_session_history(normalized_symbol, date, session="regular"),
        client.get_session_history(normalized_symbol, date, session="extended"),
    )
    responses = (regular.session_history, extended.session_history)
    if any(response.stale for response in responses):
        raise RuntimeError("gateway returned stale session-history evidence")
    candles_by_timestamp = {
        candle.timestamp: candle
        for response in responses
        for candle in response.candles
    }
    if not candles_by_timestamp:
        raise RuntimeError(f"gateway returned no candles for {normalized_symbol} on {date}")

    run_directory = (
        output_root
        / normalized_symbol.replace("/", "_")
        / date.isoformat()
        / now.strftime("%Y%m%dT%H%M%S.%fZ")
    )
    run_directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    candles_path = run_directory / "candles_1m.json"
    manifest_path = run_directory / "manifest.json"
    payload = {
        "schema_version": "1.0",
        "source": "schwab_gateway_session_history",
        "symbol": normalized_symbol,
        "session_date": date.isoformat(),
        "retrieved_at": now.isoformat(),
        "interval": "1m",
        "candles": [
            candle.model_dump(mode="json")
            for candle in sorted(candles_by_timestamp.values(), key=lambda item: item.timestamp)
        ],
    }
    with candles_path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(candles_path, 0o600)
    manifest = {
        "schema_version": "1.0",
        "purpose": "equity_session_history",
        "provider": "SchwabGateway",
        "symbol": normalized_symbol,
        "session_date": date.isoformat(),
        "retrieved_at": now.isoformat(),
        "path_scope": "relative_to_manifest",
        "candles_path": candles_path.name,
        "candles_sha256": _sha256(candles_path),
        "candle_count": len(candles_by_timestamp),
        "regular_candle_count": len(responses[0].candles),
        "extended_candle_count": len(responses[1].candles),
        "regular_quality_flags": list(responses[0].data_quality_flags),
        "extended_quality_flags": list(responses[1].data_quality_flags),
    }
    with manifest_path.open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(manifest_path, 0o600)
    return manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbol")
    parser.add_argument("date", type=dt.date.fromisoformat)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--gateway-url", default=os.getenv("SCHWAB_GATEWAY_URL"))
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    api_key = os.getenv("SCHWAB_GATEWAY_API_KEY")
    if not args.gateway_url:
        parser.error("SCHWAB_GATEWAY_URL is required")
    if not api_key:
        parser.error("SCHWAB_GATEWAY_API_KEY is required")

    async def run() -> Path:
        async with GatewayMarketDataClient(
            base_url=args.gateway_url,
            api_key=api_key,
            timeout_seconds=args.timeout_seconds,
        ) as client:
            return await export_session_history(
                client=client,
                symbol=args.symbol,
                date=args.date,
                output_root=args.output_root,
            )

    print(asyncio.run(run()))
