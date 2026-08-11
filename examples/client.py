"""Minimal SDK example; read the internal key from a protected file."""

import asyncio
from pathlib import Path

from schwab_gateway_sdk import GatewayMarketDataClient


async def main() -> None:
    api_key = Path("/run/secrets/schwab-gateway-api-key").read_text().strip()
    async with GatewayMarketDataClient("http://schwab-gateway:8011", api_key) as client:
        print(await client.get_quotes(["AAPL"]))


asyncio.run(main())
