from __future__ import annotations

import copy
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from schwab_token_store import (
    AtomicFileTokenStore,
    AtomicTokenManager,
    TokenCallbackScopeError,
    TokenManagerState,
    TokenRefreshError,
)

from schwab_gateway.token_adapter import (
    LockedSchwabClientAdapter,
    SchwabClientConstructionError,
    SchwabClientOperationError,
)

NOW = 2_000_000_000.0
FAKE_API_KEY = "fake-api-key"
FAKE_APP_SECRET = "fake-app-secret"


def token_document(generation: int = 0) -> dict[str, Any]:
    return {
        "creation_timestamp": int(NOW - 60),
        "token": {
            "access_token": f"access-secret-{generation}",
            "refresh_token": f"refresh-secret-{generation}",
            "generation": generation,
        },
    }


def write_token(path: Path, token: object) -> None:
    path.write_text(json.dumps(token), encoding="utf-8")
    path.chmod(0o600)


def manager(path: Path) -> AtomicTokenManager:
    return AtomicTokenManager(
        AtomicFileTokenStore(path),
        lock_timeout_seconds=1.0,
        clock=lambda: NOW,
    )


class FakeClient:
    def __init__(self, token: dict[str, Any], sdk_token_writer) -> None:
        self.token = copy.deepcopy(token)
        self._sdk_token_writer = sdk_token_writer

    def rotate(self, generation: int) -> None:
        rotated = copy.deepcopy(self.token)
        rotated.update(
            access_token=f"access-secret-{generation}",
            refresh_token=f"refresh-secret-{generation}",
            generation=generation,
        )
        self._sdk_token_writer(rotated, "ignored-positional", ignored_keyword=True)
        self.token = rotated

    def write_raw(self, token: object) -> None:
        self._sdk_token_writer(token)


class FakeAccessFunctionFactory:
    """Mimic schwab-py 1.5.1 TokenMetadata wrapping without importing schwab."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.captured_read = None
        self.captured_sdk_write = None

    def __call__(
        self,
        api_key,
        app_secret,
        token_read_func,
        token_write_func,
        asyncio=False,
        enforce_enums=True,
    ) -> FakeClient:
        loaded = token_read_func()
        creation_timestamp = loaded["creation_timestamp"]

        def sdk_token_write(token, *args, **kwargs):
            return token_write_func(
                {
                    "creation_timestamp": creation_timestamp,
                    "token": token,
                },
                *args,
                **kwargs,
            )

        self.calls.append(
            {
                "api_key": api_key,
                "app_secret": app_secret,
                "asyncio": asyncio,
                "enforce_enums": enforce_enums,
            }
        )
        self.captured_read = token_read_func
        self.captured_sdk_write = sdk_token_write
        return FakeClient(loaded["token"], sdk_token_write)


def adapter(path: Path, factory=None) -> LockedSchwabClientAdapter:
    return LockedSchwabClientAdapter(
        manager(path),
        factory or FakeAccessFunctionFactory(),
        api_key=FAKE_API_KEY,
        app_secret=FAKE_APP_SECRET,
    )


def test_lock_covers_store_read_factory_operation_and_write_callback() -> None:
    events: list[str] = []

    class TrackingTransaction:
        def __init__(self, store) -> None:
            self.store = store
            self.token = token_document()

        def read(self):
            assert self.store.is_locked
            events.append("store_read")
            return copy.deepcopy(self.token)

        def write(self, token):
            assert self.store.is_locked
            events.append("store_write")
            self.token = copy.deepcopy(token)

    class TrackingStore:
        def __init__(self) -> None:
            self.is_locked = False
            self.transaction = TrackingTransaction(self)

        @contextmanager
        def locked(self, _timeout_seconds):
            assert not self.is_locked
            self.is_locked = True
            events.append("lock_enter")
            try:
                yield self.transaction
            finally:
                events.append("lock_exit")
                self.is_locked = False

    store = TrackingStore()
    factory = FakeAccessFunctionFactory()

    def observing_factory(*args, **kwargs):
        assert store.is_locked
        events.append("factory")
        return factory(*args, **kwargs)

    token_adapter = LockedSchwabClientAdapter(
        AtomicTokenManager(store, clock=lambda: NOW),
        observing_factory,
        api_key=FAKE_API_KEY,
        app_secret=FAKE_APP_SECRET,
    )

    def operation(client: FakeClient) -> None:
        assert store.is_locked
        events.append("operation")
        client.rotate(1)
        events.append("write_returned")

    token_adapter.execute(operation)

    assert events == [
        "lock_enter",
        "store_read",
        "factory",
        "operation",
        "store_write",
        "write_returned",
        "lock_exit",
    ]
    assert store.transaction.token == token_document(1)


def test_no_refresh_uses_exact_factory_signature_and_leaves_token_unchanged(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    factory = FakeAccessFunctionFactory()

    generation = adapter(path, factory).execute(lambda client: client.token["generation"])

    assert generation == 0
    assert factory.calls == [
        {
            "api_key": FAKE_API_KEY,
            "app_secret": FAKE_APP_SECRET,
            "asyncio": False,
            "enforce_enums": True,
        }
    ]
    assert json.loads(path.read_text(encoding="utf-8")) == token_document()


def test_each_metadata_wrapped_rotation_is_persisted_before_callback_returns(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    token_manager = manager(path)
    factory = FakeAccessFunctionFactory()
    token_adapter = LockedSchwabClientAdapter(
        token_manager,
        factory,
        api_key=FAKE_API_KEY,
        app_secret=FAKE_APP_SECRET,
    )

    def rotate_twice(client: FakeClient) -> int:
        client.rotate(1)
        assert json.loads(path.read_text(encoding="utf-8")) == token_document(1)
        client.rotate(2)
        assert json.loads(path.read_text(encoding="utf-8")) == token_document(2)
        return client.token["generation"]

    assert token_adapter.execute(rotate_twice) == 2
    assert token_manager.health().state is TokenManagerState.READY
    assert token_manager.health().reason == "refresh_succeeded"
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob(".tokens.json.*.tmp"))


def test_valid_rotation_survives_later_operation_failure_and_error_is_redacted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    fake_log = MagicMock()
    monkeypatch.setattr(
        "schwab_gateway.token_adapter.log",
        fake_log,
    )

    def rotate_then_fail(client: FakeClient) -> None:
        client.rotate(1)
        raise RuntimeError("later failure containing access-secret-1")

    with pytest.raises(SchwabClientOperationError) as exc:
        adapter(path).execute(rotate_then_fail)

    assert str(exc.value) == "Schwab client operation failed"
    assert json.loads(path.read_text(encoding="utf-8")) == token_document(1)
    audit_text = repr(fake_log.method_calls)
    assert "access-secret-1" not in audit_text
    assert "later failure" not in audit_text


def test_invalid_metadata_wrapped_callback_data_is_rejected_without_overwrite(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    token_manager = manager(path)
    factory = FakeAccessFunctionFactory()
    token_adapter = LockedSchwabClientAdapter(
        token_manager,
        factory,
        api_key=FAKE_API_KEY,
        app_secret=FAKE_APP_SECRET,
    )

    with pytest.raises(TokenRefreshError) as exc:
        token_adapter.execute(
            lambda client: client.write_raw({"access_token": "access-secret-invalid"})
        )

    assert str(exc.value) == "token write callback received invalid data"
    assert "access-secret-invalid" not in str(exc.value)
    assert json.loads(path.read_text(encoding="utf-8")) == token_document()
    assert token_manager.health().state is TokenManagerState.REFRESH_FAILED


def test_read_and_write_callbacks_are_rejected_after_transaction_scope(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    factory = FakeAccessFunctionFactory()

    adapter(path, factory).execute(lambda _client: None)

    with pytest.raises(TokenCallbackScopeError, match="outside its transaction scope"):
        factory.captured_read()
    with pytest.raises(TokenCallbackScopeError, match="outside its transaction scope"):
        factory.captured_sdk_write(token_document(1)["token"])
    assert json.loads(path.read_text(encoding="utf-8")) == token_document()


def test_concurrent_client_operations_cover_construction_and_cannot_lose_rotation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    first_operation_entered = threading.Event()
    release_first_operation = threading.Event()
    second_factory_entered = threading.Event()
    construction_count = 0
    construction_guard = threading.Lock()

    class CoordinatedFactory(FakeAccessFunctionFactory):
        def __call__(self, *args, **kwargs) -> FakeClient:
            nonlocal construction_count
            with construction_guard:
                construction_count += 1
                count = construction_count
            if count == 2:
                second_factory_entered.set()
            return super().__call__(*args, **kwargs)

    factory = CoordinatedFactory()
    token_adapters = [adapter(path, factory), adapter(path, factory)]

    def rotate(client: FakeClient) -> int:
        if client.token["generation"] == 0:
            first_operation_entered.set()
            assert release_first_operation.wait(timeout=2)
        generation = client.token["generation"] + 1
        client.rotate(generation)
        return generation

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(token_adapters[0].execute, rotate)
        assert first_operation_entered.wait(timeout=1)
        second = executor.submit(token_adapters[1].execute, rotate)
        assert not second_factory_entered.wait(timeout=0.05)
        release_first_operation.set()
        observed = sorted((first.result(timeout=2), second.result(timeout=2)))

    assert observed == [1, 2]
    assert second_factory_entered.is_set()
    assert json.loads(path.read_text(encoding="utf-8")) == token_document(2)


def test_factory_exception_text_and_fake_credentials_are_not_exposed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    fake_log = MagicMock()
    monkeypatch.setattr(
        "schwab_gateway.token_adapter.log",
        fake_log,
    )

    def failing_factory(*_args, **_kwargs):
        raise RuntimeError(
            "factory failure with access-secret-0 fake-api-key fake-app-secret"
        )

    with pytest.raises(SchwabClientConstructionError) as exc:
        adapter(path, failing_factory).execute(lambda _client: None)

    assert str(exc.value) == "Schwab client construction failed"
    exposed = str(exc.value) + repr(fake_log.method_calls)
    assert "access-secret-0" not in exposed
    assert FAKE_API_KEY not in exposed
    assert FAKE_APP_SECRET not in exposed
    assert "factory failure" not in exposed
