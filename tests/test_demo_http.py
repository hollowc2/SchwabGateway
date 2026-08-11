from __future__ import annotations

import json

import httpx
import pytest
from aiohttp.test_utils import TestServer

from schwab_gateway.auth import hash_api_key
from schwab_gateway.config import GatewaySettings
from schwab_gateway.runner import build_demo_app


@pytest.mark.asyncio
async def test_demo_mode_over_real_http(tmp_path) -> None:
    keys = tmp_path / "keys.json"
    keys.write_text(
        json.dumps(
            {
                "version": 1,
                "clients": [
                    {
                        "id": "demo-consumer",
                        "key_sha256": hash_api_key("synthetic-demo-key"),
                        "capabilities": ["market_data:read"],
                        "priority_class": "background",
                    }
                ],
            }
        )
    )
    keys.chmod(0o600)
    app = build_demo_app(GatewaySettings(internal_keys_path=keys))
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            health = await client.get("/health")
            ready = await client.get("/ready")
            unauthorized = await client.get("/v1/quotes", params={"symbols": "AAPL"})
            quotes = await client.get(
                "/v1/quotes",
                params={"symbols": "AAPL"},
                headers={"X-Internal-API-Key": "synthetic-demo-key"},
            )
    finally:
        await server.close()

    assert (health.status_code, ready.status_code, unauthorized.status_code) == (200, 200, 401)
    assert quotes.status_code == 200
    assert quotes.json()["quotes"][0]["data_quality_flags"] == [
        "demo_data_not_for_trading"
    ]
