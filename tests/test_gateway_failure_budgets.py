"""Offline failures and retry budgets; no Schwab credential or endpoint is used."""

import asyncio
import datetime as dt
from email.utils import format_datetime

import httpx
import pytest
from aiohttp.test_utils import make_mocked_request
from pydantic import BaseModel
from support import EmptyQuoteUpstream, FakeReadiness, single_principal_authenticator

from schwab_gateway import api
from schwab_gateway.auth import InternalPrincipal, PriorityClass, hash_api_key
from schwab_gateway.failures import FailureMetadata, classify, retry_after_seconds
from schwab_gateway.token_adapter import SchwabClientOperationError
from schwab_gateway.upstream import UpstreamUnavailableError


class Result(BaseModel):
    value: int = 1


def request_for(budget=3):
    app = api.create_app(EmptyQuoteUpstream(), single_principal_authenticator(),
                         token_readiness_provider=FakeReadiness(),
                         upstream_timeout_seconds=budget)
    request = make_mocked_request('GET', '/v1/spot?symbol=$SPX', app=app)
    request[api.PRINCIPAL_KEY] = InternalPrincipal(
        'test', hash_api_key('synthetic'), frozenset({'market_data:read'}),
        PriorityClass.PROTECTED,
    )
    return request


def failure(cause, retry_after=None):
    exc = UpstreamUnavailableError('safe')
    exc.__cause__ = SchwabClientOperationError('safe', FailureMetadata(cause, retry_after))
    return exc


@pytest.mark.parametrize('raw,expected', [
    ('0', 0), ('2', 2), ('60', 60), ('61', None), ('-1', None), ('1.5', None),
    ('nan', None), ('bad-secret', None), (None, None), ('9' * 200, None),
])
def test_numeric_retry_after(raw, expected):
    assert retry_after_seconds(raw) == expected


def test_http_date_retry_after():
    now = dt.datetime(2026, 10, 21, tzinfo=dt.UTC)
    assert retry_after_seconds(format_datetime(now + dt.timedelta(seconds=20)), now=now) == 20
    assert retry_after_seconds(format_datetime(now - dt.timedelta(seconds=1)), now=now) is None
    assert retry_after_seconds(format_datetime(now + dt.timedelta(seconds=61)), now=now) is None
    assert retry_after_seconds('Wed, 21 Oct 2026 00:00:00', now=now) is None


@pytest.mark.parametrize('header,expected', [('2', 2), (None, None), ('secret', None)])
def test_only_allowlisted_metadata_survives(header, expected):
    response = httpx.Response(429, headers={'Retry-After': header} if header else {},
                              request=httpx.Request('GET', 'https://secret.invalid/?token=secret'),
                              text='secret')
    metadata = classify(httpx.HTTPStatusError(
        'secret', request=response.request, response=response,
    ))
    wrapped = SchwabClientOperationError('Schwab client operation failed', metadata)
    assert classify(wrapped).cause == 'rate_limited'
    assert wrapped.retry_after == expected
    assert 'secret' not in str(wrapped)
    assert 'secret' not in repr(vars(wrapped))


@pytest.mark.parametrize('retry_after', [None, 2])
async def test_429_never_retries_and_shared_cooldown_sheds_reads(retry_after):
    request = request_for()
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        raise failure('rate_limited', retry_after)

    response = await api._serve_upstream(request, 'spot', read)
    assert response.status == 503
    assert b'upstream_rate_limited' in response.body
    assert response.headers.get('Retry-After') == ('2' if retry_after else None)
    response = await api._serve_upstream(request, 'history', read)
    assert response.status == 503 and calls == 1
    assert request.app[api.EXECUTION_SCHEDULER_KEY].snapshot().total == 0


async def test_connection_retry_is_once_outside_worker():
    request = request_for()
    scheduler = request.app[api.EXECUTION_SCHEDULER_KEY]
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        assert scheduler.snapshot().worker_active
        if calls == 1:
            raise failure('connect_timeout')
        return Result()

    task = asyncio.create_task(api._serve_upstream(request, 'spot', read))
    while calls == 0:
        await asyncio.sleep(0)
    await asyncio.sleep(.02)
    # Backoff holds neither a scheduler allocation nor the worker.
    assert scheduler.snapshot().total == 0
    assert not scheduler.snapshot().worker_active
    response = await task
    assert response.status == 200 and calls == 2


@pytest.mark.parametrize('budget,operation,expected_calls', [
    (.05, 'spot', 1), (3, 'spot', 2), (3, 'orders', 1), (3, 'accounts', 1),
])
async def test_exhaustion_and_write_exclusion(budget, operation, expected_calls):
    request = request_for(budget)
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        raise failure('connect_timeout')

    response = await api._serve_upstream(request, operation, read)
    assert response.status == 503 and b'upstream_connect_timeout' in response.body
    assert calls == expected_calls
    assert request.app[api.EXECUTION_SCHEDULER_KEY].snapshot().total == 0


async def test_cancellation_during_backoff_never_retries():
    request = request_for()
    called = asyncio.Event()
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        called.set()
        raise failure('connect_timeout')

    task = asyncio.create_task(api._serve_upstream(request, 'spot', read))
    await called.wait()
    await asyncio.sleep(.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == 1
    assert request.app[api.EXECUTION_SCHEDULER_KEY].snapshot().total == 0


async def test_scheduler_timeout_never_starts_replacement_and_retains_worker():
    request = request_for(.02)
    release = asyncio.Event()
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        await release.wait()
        return Result()

    response = await api._serve_upstream(request, 'spot', read)
    assert response.status == 504 and calls == 1
    scheduler = request.app[api.EXECUTION_SCHEDULER_KEY]
    assert scheduler.snapshot().worker_active
    release.set()
    await scheduler.wait_idle()
    assert scheduler.snapshot().total == 0


async def test_retry_absolute_deadline_includes_second_queue_wait():
    request = request_for(1.2)
    scheduler = request.app[api.EXECUTION_SCHEDULER_KEY]
    calls = 0
    release = asyncio.Event()

    async def blocker():
        await release.wait()

    async def read():
        nonlocal calls
        calls += 1
        raise failure('connect_timeout')

    task = asyncio.create_task(api._serve_upstream(request, 'spot', read))
    while calls == 0:
        await asyncio.sleep(0)
    blocked = asyncio.create_task(scheduler.execute(
        PriorityClass.PROTECTED, 'history', blocker,
        queue_timeout_seconds=2, execution_timeout_seconds=3,
    ))
    response = await asyncio.wait_for(task, 1.5)
    assert response.status == 503 and b'gateway_queue_timeout' in response.body
    assert calls == 1 and scheduler.snapshot().worker_active
    release.set()
    await blocked
    await scheduler.wait_idle()
    assert scheduler.snapshot().total == 0


async def test_real_adapter_failure_metadata_metrics_and_token_lock_release(capsys):
    from test_gateway_live_provider import _FakeClient, _provider

    from schwab_gateway.failures import upstream_attempts, upstream_failures

    client = _FakeClient()
    provider, manager = _provider(client)
    request = request_for()
    calls = 0
    original = client.get_quote
    attempts = upstream_attempts.labels(operation='spot')
    failures = upstream_failures.labels(operation='spot', cause='connect_timeout')
    before_attempts, before_failures = attempts._value.get(), failures._value.get()

    def get_quote(symbol):
        nonlocal calls
        calls += 1
        assert manager.open
        if calls == 1:
            raise httpx.ConnectTimeout('credential-secret', request=httpx.Request(
                'GET', 'https://secret.invalid/?token=credential-secret',
            ))
        return original(symbol)

    client.get_quote = get_quote

    async def read():
        try:
            await provider.get_spot_snapshot('$SPX')
        except Exception as exc:
            raise UpstreamUnavailableError('safe') from exc
        return Result()

    task = asyncio.create_task(api._serve_upstream(request, 'spot', read))
    while calls == 0 or manager.open:
        await asyncio.sleep(0)
    await asyncio.sleep(.02)
    assert not manager.open
    assert request.app[api.EXECUTION_SCHEDULER_KEY].snapshot().total == 0
    response = await task
    assert response.status == 200 and calls == 2
    assert attempts._value.get() - before_attempts == 2
    assert failures._value.get() - before_failures == 1
    assert 'credential-secret' not in capsys.readouterr().out


@pytest.mark.parametrize('code,expected', [
    ('upstream_rate_limited', 'GatewayRateLimitError'),
    ('upstream_connect_timeout', 'GatewayConnectionTimeoutError'),
])
async def test_sdk_preserves_bounded_failure_without_another_retry(code, expected):
    from schwab_gateway_sdk.client import GatewayMarketDataClient, GatewayUnavailableError

    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={'error': {'code': code}},
                              headers={'Retry-After': '2'})

    async with httpx.AsyncClient(
        base_url="http://synthetic", transport=httpx.MockTransport(respond),
    ) as http:
        sdk = GatewayMarketDataClient('http://synthetic', 'synthetic', client=http)
        with pytest.raises(GatewayUnavailableError) as caught:
            await sdk.get_spot('$SPX')
    assert type(caught.value).__name__ == expected
    if code == 'upstream_rate_limited':
        assert caught.value.retry_after_seconds == 2
    assert calls == 1


async def test_replacement_execution_cannot_extend_original_budget():
    request = request_for(1.2)
    scheduler = request.app[api.EXECUTION_SCHEDULER_KEY]
    release = asyncio.Event()
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise failure('connect_timeout')
        await release.wait()
        return Result()

    response = await asyncio.wait_for(api._serve_upstream(request, 'spot', read), 1.5)
    assert response.status == 504 and calls == 2
    assert scheduler.snapshot().worker_active
    release.set()
    await scheduler.wait_idle()
    assert scheduler.snapshot().total == 0


@pytest.mark.parametrize('header,expected', [('2', 2), (None, None), ('bad-secret', None)])
async def test_429_metadata_crosses_real_token_transaction(header, expected, capsys):
    from test_gateway_live_provider import _FakeClient, _provider

    from schwab_gateway.failures import upstream_failures

    client = _FakeClient()
    client.quote_response = httpx.Response(
        429, headers={'Retry-After': header} if header else {}, text='body-secret',
        request=httpx.Request('GET', 'https://secret.invalid/?token=url-secret'),
    )
    provider, manager = _provider(client)
    counter = upstream_failures.labels(operation='spot', cause='rate_limited')
    before = counter._value.get()
    with pytest.raises(SchwabClientOperationError) as caught:
        await provider.get_spot_snapshot('$SPX')
    assert caught.value.cause == 'rate_limited'
    assert caught.value.retry_after == expected
    assert counter._value.get() - before == 1
    assert not manager.open
    captured = capsys.readouterr().out
    assert 'url-secret' not in captured and 'body-secret' not in captured
    assert 'bad-secret' not in captured



def test_new_failure_codes_are_in_published_contract():
    from pathlib import Path

    import yaml

    spec = yaml.safe_load((Path(__file__).parents[1] / 'openapi.yaml').read_text())
    codes = spec['components']['schemas']['GatewayErrorV1']['properties'][
        'error']['properties']['code']['enum']
    assert {'upstream_rate_limited', 'upstream_connect_timeout'} <= set(codes)
    retry_after = spec['components']['responses']['Error']['headers']['Retry-After']['schema']
    assert retry_after == {'type': 'integer', 'minimum': 1, 'maximum': 60}
