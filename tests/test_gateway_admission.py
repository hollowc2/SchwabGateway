from __future__ import annotations

import asyncio
import datetime as dt
import re

import httpx
import pytest
from aiohttp.test_utils import TestServer
from schwab_gateway_sdk.client import GatewayCapacityError, GatewayMarketDataClient
from schwab_gateway_sdk.models import QuoteV1
from schwab_token_store import TokenManagerState

from schwab_gateway import api
from schwab_gateway.admission import (
    AdmissionController,
    AdmissionPolicy,
)
from schwab_gateway.api import StaticTokenReadinessProvider, create_app
from schwab_gateway.auth import (
    InternalKeyAuthenticator,
    InternalPrincipal,
    PriorityClass,
    hash_api_key,
)
from schwab_gateway.upstream import UpstreamUnavailableError

KEYS = {
    "butterfly-guy": "synthetic-butterfly-key",
    "equity-scanner": "synthetic-scanner-key",
    "afterhours-lab": "synthetic-lab-key",
}


def authenticator() -> InternalKeyAuthenticator:
    return InternalKeyAuthenticator(
        tuple(
            InternalPrincipal(
                client_id=client_id,
                key_sha256=hash_api_key(key),
                capabilities=frozenset({"market_data:read"}),
                priority_class=(
                    PriorityClass.PROTECTED
                    if client_id == "butterfly-guy"
                    else PriorityClass.BACKGROUND
                ),
            )
            for client_id, key in KEYS.items()
        )
    )


def headers(client_id: str, **extra: str) -> dict[str, str]:
    return {"X-Internal-API-Key": KEYS[client_id], **extra}


def ready_provider() -> StaticTokenReadinessProvider:
    return StaticTokenReadinessProvider(TokenManagerState.READY)


class BlockingUpstream:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.changed = asyncio.Condition()
        self.release = asyncio.Event()

    async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]:
        async with self.changed:
            self.calls.append(symbols)
            self.changed.notify_all()
        await self.release.wait()
        now = dt.datetime.now(dt.timezone.utc)
        return tuple(
            QuoteV1(
                symbol=symbol,
                gateway_received_at=now,
                source="fake",
                stale=False,
            )
            for symbol in symbols
        )

    async def wait_for_calls(self, count: int) -> None:
        async with self.changed:
            await self.changed.wait_for(lambda: len(self.calls) >= count)


@pytest.mark.asyncio
async def test_protected_request_is_admitted_while_shared_background_pool_is_saturated() -> None:
    upstream = BlockingUpstream()
    app = create_app(
        upstream,
        authenticator(),
        token_readiness_provider=ready_provider(),
        admission_policy=AdmissionPolicy(protected_capacity=1, background_capacity=2),
    )
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            scanner = asyncio.create_task(
                client.get(
                    "/v1/quotes",
                    params={"symbols": "SCAN"},
                    headers=headers("equity-scanner"),
                )
            )
            lab = asyncio.create_task(
                client.get(
                    "/v1/quotes",
                    params={"symbols": "LAB"},
                    headers=headers("afterhours-lab"),
                )
            )
            await upstream.wait_for_calls(2)

            rejected = await client.get(
                "/v1/quotes",
                params={"symbols": "EXTRA"},
                headers=headers("equity-scanner"),
            )
            gateway_client = GatewayMarketDataClient(
                str(server.make_url("/")),
                KEYS["equity-scanner"],
                client=client,
            )
            with pytest.raises(GatewayCapacityError):
                await gateway_client.get_quotes(["CLIENT"])
            protected = asyncio.create_task(
                client.get(
                    "/v1/quotes",
                    params={"symbols": "SPX"},
                    headers=headers("butterfly-guy"),
                )
            )
            await upstream.wait_for_calls(3)
            assert not protected.done()

            upstream.release.set()
            responses = await asyncio.gather(scanner, lab, protected)
            metrics = await client.get("/metrics")
    finally:
        await server.close()

    assert rejected.status_code == 429
    assert rejected.json()["error"] == {
        "code": "gateway_capacity_exceeded",
        "message": "gateway request capacity is unavailable",
    }
    assert [response.status_code for response in responses] == [200, 200, 200]
    admission_lines = [
        line for line in metrics.text.splitlines() if line.startswith("gateway_admission_total{")
    ]
    assert admission_lines
    assert all("SCAN" not in line and "SPX" not in line for line in admission_lines)
    assert {
        match.group(1)
        for line in admission_lines
        if (match := re.search(r'priority_class="([^"]+)"', line))
    } <= {"protected", "background"}


@pytest.mark.asyncio
async def test_permits_release_after_success_failure_timeout_and_cancellation() -> None:
    controller = AdmissionController(
        AdmissionPolicy(protected_capacity=1, background_capacity=1)
    )

    async with controller.admit(PriorityClass.BACKGROUND):
        assert await controller.active_count(PriorityClass.BACKGROUND) == 1
    assert await controller.active_count(PriorityClass.BACKGROUND) == 0

    with pytest.raises(RuntimeError, match="synthetic failure"):
        async with controller.admit(PriorityClass.BACKGROUND):
            raise RuntimeError("synthetic failure")
    assert await controller.active_count(PriorityClass.BACKGROUND) == 0

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.001):
            async with controller.admit(PriorityClass.BACKGROUND):
                await asyncio.sleep(1)
    assert await controller.active_count(PriorityClass.BACKGROUND) == 0

    entered = asyncio.Event()

    async def cancellable() -> None:
        async with controller.admit(PriorityClass.BACKGROUND):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(cancellable())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await controller.active_count(PriorityClass.BACKGROUND) == 0


@pytest.mark.asyncio
async def test_normalized_upstream_failure_releases_permit_for_next_request() -> None:
    class FailOnceUpstream:
        calls = 0

        async def get_quotes(self, symbols: tuple[str, ...]) -> tuple[QuoteV1, ...]:
            self.calls += 1
            if self.calls == 1:
                raise UpstreamUnavailableError(
                    "raw-key=synthetic-secret token=synthetic-token "
                    "digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                )
            now = dt.datetime.now(dt.timezone.utc)
            return (
                QuoteV1(
                    symbol=symbols[0],
                    gateway_received_at=now,
                    source="fake",
                    stale=False,
                ),
            )

    app = create_app(
        FailOnceUpstream(),
        authenticator(),
        token_readiness_provider=ready_provider(),
        admission_policy=AdmissionPolicy(protected_capacity=1, background_capacity=1),
    )
    server = TestServer(app)
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            failed = await client.get(
                "/v1/quotes", params={"symbols": "AAPL"}, headers=headers("equity-scanner")
            )
            recovered = await client.get(
                "/v1/quotes", params={"symbols": "AAPL"}, headers=headers("equity-scanner")
            )
    finally:
        await server.close()

    assert failed.status_code == 503
    assert failed.json()["error"]["code"] == "upstream_unavailable"
    assert "synthetic-secret" not in failed.text
    assert "synthetic-token" not in failed.text
    assert "aaaaaaaa" not in failed.text
    assert recovered.status_code == 200


@pytest.mark.asyncio
async def test_identity_claim_header_cannot_override_authenticated_caller(monkeypatch) -> None:
    records: list[dict[str, object]] = []

    class RecordingLog:
        def info(self, _event: str, **fields: object) -> None:
            records.append(fields)

        def warning(self, _event: str, **_fields: object) -> None:
            pass

    upstream = BlockingUpstream()
    upstream.release.set()
    monkeypatch.setattr(api, "log", RecordingLog())
    server = TestServer(
        create_app(
            upstream,
            authenticator(),
            token_readiness_provider=ready_provider(),
        )
    )
    await server.start_server()
    try:
        async with httpx.AsyncClient(base_url=str(server.make_url("/"))) as client:
            response = await client.get(
                "/v1/quotes",
                params={"symbols": "AAPL"},
                headers=headers(
                    "equity-scanner",
                    **{"X-Internal-Caller-ID": "butterfly-guy"},
                ),
            )
    finally:
        await server.close()

    assert response.status_code == 200
    request_record = next(item for item in records if item.get("operation") == "quotes_v1")
    assert request_record["caller"] == "equity-scanner"
    assert set(request_record) == {"caller", "operation", "status", "latency_ms"}
    assert set(item.get("caller") for item in records) <= {
        "anonymous",
        "butterfly-guy",
        "equity-scanner",
        "afterhours-lab",
    }
