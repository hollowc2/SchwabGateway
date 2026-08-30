from __future__ import annotations

import copy
import json
import multiprocessing
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from schwab_token_store import (
    AtomicFileTokenStore,
    AtomicTokenManager,
    TokenCorruptError,
    TokenExpiredError,
    TokenLockTimeoutError,
    TokenManagerState,
    TokenMissingError,
    TokenPersistenceError,
    TokenReauthorizationRequiredError,
    TokenRefreshError,
    TokenRevokedError,
    token_lock_hold_seconds,
    token_lock_wait_seconds,
)

NOW = 2_000_000_000.0


def _histogram_count(histogram: object, **labels: str) -> float:
    """Read one labelled histogram's observation count out of the default registry."""
    target = "_".join(sorted(f"{key}={value}" for key, value in labels.items()))
    for metric in histogram.collect():
        for sample in metric.samples:
            if not sample.name.endswith("_count"):
                continue
            got = "_".join(sorted(f"{k}={v}" for k, v in sample.labels.items()))
            if got == target:
                return sample.value
    return 0.0


def token_document(generation: int = 0) -> dict:
    return {
        "creation_timestamp": int(NOW - 60),
        "token": {
            "access_token": f"access-secret-{generation}",
            "refresh_token": f"refresh-secret-{generation}",
            "generation": generation,
        },
    }


def write_token(path: Path, token: object, *, mode: int = 0o600) -> None:
    path.write_text(json.dumps(token), encoding="utf-8")
    path.chmod(mode)


def manager(
    path: Path,
    *,
    lock_timeout_seconds: float = 1.0,
) -> AtomicTokenManager:
    return AtomicTokenManager(
        AtomicFileTokenStore(path),
        lock_timeout_seconds=lock_timeout_seconds,
        clock=lambda: NOW,
    )


def increment_callback(token: dict) -> dict:
    refreshed = copy.deepcopy(token)
    generation = refreshed["token"]["generation"] + 1
    refreshed["token"].update(
        access_token=f"access-secret-{generation}",
        refresh_token=f"refresh-secret-{generation}",
        generation=generation,
    )
    return refreshed


def _process_refresh(path: str, start, results) -> None:
    start.wait(timeout=5)

    def delayed_increment(token: dict) -> dict:
        time.sleep(0.05)
        return increment_callback(token)

    try:
        value = manager(Path(path), lock_timeout_seconds=2).refresh(delayed_increment)
        results.put(value["token"]["generation"])
    except Exception as exc:  # pragma: no cover - diagnostic path for child process
        results.put(type(exc).__name__)


def test_load_validates_and_returns_a_defensive_copy(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    token_manager = manager(path)

    loaded = token_manager.load()
    loaded["token"]["access_token"] = "mutated"

    assert json.loads(path.read_text(encoding="utf-8")) == token_document()
    assert token_manager.health().state is TokenManagerState.READY
    assert token_manager.health().reason == "token_loaded"


def test_read_locked_uses_the_existing_lock_without_write_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    store = AtomicFileTokenStore(path)
    with store.locked(1.0):
        pass

    real_open = os.open
    lock_path = tmp_path / ".tokens.json.lock"
    observed_flags: list[int] = []

    def observe_open(target, flags, *args):
        if Path(target) == lock_path:
            observed_flags.append(flags)
        return real_open(target, flags, *args)

    monkeypatch.setattr(os, "open", observe_open)
    with store.read_locked(1.0) as transaction:
        assert transaction.read() == token_document()

    assert len(observed_flags) == 1
    assert observed_flags[0] & os.O_ACCMODE == os.O_RDONLY
    assert observed_flags[0] & os.O_CREAT == 0


def test_lock_acquisition_and_hold_are_observed_per_mode(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    store = AtomicFileTokenStore(path)
    with store.locked(1.0):  # create the lock file so read_locked can open it
        pass

    read_wait = _histogram_count(token_lock_wait_seconds, mode="shared", outcome="acquired")
    read_hold = _histogram_count(token_lock_hold_seconds, mode="shared")
    write_wait = _histogram_count(
        token_lock_wait_seconds, mode="exclusive", outcome="acquired"
    )
    write_hold = _histogram_count(token_lock_hold_seconds, mode="exclusive")

    with store.read_locked(1.0):
        pass
    with store.locked(1.0):
        pass

    assert _histogram_count(
        token_lock_wait_seconds, mode="shared", outcome="acquired"
    ) == read_wait + 1
    assert _histogram_count(token_lock_hold_seconds, mode="shared") == read_hold + 1
    assert _histogram_count(
        token_lock_wait_seconds, mode="exclusive", outcome="acquired"
    ) == write_wait + 1
    assert _histogram_count(token_lock_hold_seconds, mode="exclusive") == write_hold + 1


def test_lock_wait_timeout_is_observed_without_a_hold_sample(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    store = AtomicFileTokenStore(path)

    blocker = AtomicFileTokenStore(path)
    with blocker.locked(1.0):
        timeouts = _histogram_count(
            token_lock_wait_seconds, mode="exclusive", outcome="timeout"
        )
        holds = _histogram_count(token_lock_hold_seconds, mode="exclusive")

        with pytest.raises(TokenLockTimeoutError):
            with store.locked(0.01):
                pass

        assert _histogram_count(
            token_lock_wait_seconds, mode="exclusive", outcome="timeout"
        ) == timeouts + 1
        assert _histogram_count(token_lock_hold_seconds, mode="exclusive") == holds


def test_read_locked_refuses_to_create_a_missing_lock_file(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    store = AtomicFileTokenStore(path)
    lock_path = tmp_path / ".tokens.json.lock"

    with pytest.raises(TokenPersistenceError, match="opened read-only"):
        with store.read_locked(0):
            pass

    assert not lock_path.exists()


def test_read_locked_times_out_behind_an_exclusive_writer(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    store = AtomicFileTokenStore(path)

    def read() -> object:
        with store.read_locked(0.01) as transaction:
            return transaction.read()

    with store.locked(1.0):
        with ThreadPoolExecutor(max_workers=1) as executor:
            blocked = executor.submit(read)
            with pytest.raises(TokenLockTimeoutError):
                blocked.result(timeout=1)


@pytest.mark.parametrize(
    ("setup", "error", "state"),
    [
        ("missing", TokenMissingError, TokenManagerState.MISSING),
        ("malformed", TokenCorruptError, TokenManagerState.CORRUPT),
        ("insecure", TokenCorruptError, TokenManagerState.CORRUPT),
        ("expired", TokenExpiredError, TokenManagerState.EXPIRED),
    ],
)
def test_load_exposes_bounded_failure_states(
    tmp_path: Path,
    setup: str,
    error: type[Exception],
    state: TokenManagerState,
) -> None:
    path = tmp_path / "tokens.json"
    if setup == "malformed":
        path.write_text('{"token":', encoding="utf-8")
        path.chmod(0o600)
    elif setup == "insecure":
        write_token(path, token_document(), mode=0o644)
    elif setup == "expired":
        value = token_document()
        value["creation_timestamp"] = int(NOW - 8 * 24 * 60 * 60)
        write_token(path, value)

    token_manager = manager(path)

    with pytest.raises(error):
        token_manager.load()

    assert token_manager.health().state is state
    assert "secret" not in token_manager.health().reason


def test_fake_refresh_callback_is_locked_validated_and_atomically_persisted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document(), mode=0o600)
    token_manager = manager(path)
    received: list[dict] = []

    def fake_refresh(token: dict) -> dict:
        received.append(copy.deepcopy(token))
        return increment_callback(token)

    refreshed = token_manager.refresh(fake_refresh)

    assert received == [token_document()]
    assert refreshed == token_document(1)
    assert json.loads(path.read_text(encoding="utf-8")) == token_document(1)
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob(".tokens.json.*.tmp"))
    assert (tmp_path / ".tokens.json.lock").stat().st_mode & 0o777 == 0o600
    assert token_manager.health().state is TokenManagerState.READY
    assert token_manager.health().reason == "refresh_succeeded"


def test_callback_failure_preserves_original_and_redacts_error_and_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    original = token_document()
    write_token(path, original)
    fake_log = MagicMock()
    monkeypatch.setattr(
        "schwab_token_store.log",
        fake_log,
    )
    token_manager = manager(path)

    def fail_with_secret(token: dict) -> dict:
        token["token"]["access_token"] = "mutated-secret"
        raise RuntimeError("access-secret-0")

    with pytest.raises(TokenRefreshError) as exc:
        token_manager.refresh(fail_with_secret)

    assert str(exc.value) == "token refresh callback failed"
    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert token_manager.health().state is TokenManagerState.REFRESH_FAILED
    audit_text = repr(fake_log.method_calls)
    assert "access-secret-0" not in audit_text
    assert "refresh-secret-0" not in audit_text
    assert "mutated-secret" not in audit_text


def test_invalid_callback_result_preserves_original(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    original = token_document()
    write_token(path, original)
    token_manager = manager(path)

    with pytest.raises(TokenRefreshError, match="returned invalid data"):
        token_manager.refresh(lambda _token: {"access_token": "not-an-envelope"})

    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert token_manager.health().state is TokenManagerState.REFRESH_FAILED


def test_invalid_callback_object_cannot_leak_through_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class ExplodingValue:
        def __deepcopy__(self, _memo):
            raise RuntimeError("access-secret-0")

    path = tmp_path / "tokens.json"
    original = token_document()
    write_token(path, original)
    fake_log = MagicMock()
    monkeypatch.setattr(
        "schwab_token_store.log",
        fake_log,
    )
    token_manager = manager(path)
    invalid = token_document(1)
    invalid["unexpected"] = ExplodingValue()

    with pytest.raises(TokenRefreshError, match="returned invalid data") as exc:
        token_manager.refresh(lambda _token: invalid)

    assert "access-secret-0" not in str(exc.value)
    assert "access-secret-0" not in repr(fake_log.method_calls)
    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert token_manager.health().state is TokenManagerState.REFRESH_FAILED


@pytest.mark.parametrize(
    ("error", "state"),
    [
        (TokenRevokedError, TokenManagerState.REVOKED),
        (
            TokenReauthorizationRequiredError,
            TokenManagerState.REAUTHORIZATION_REQUIRED,
        ),
    ],
)
def test_rejected_refresh_preserves_token_and_sets_explicit_state(
    tmp_path: Path,
    error: type[Exception],
    state: TokenManagerState,
) -> None:
    path = tmp_path / "tokens.json"
    original = token_document()
    write_token(path, original)
    token_manager = manager(path)

    def reject(_token: dict) -> dict:
        raise error()

    with pytest.raises(error):
        token_manager.refresh(reject)

    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert token_manager.health().state is state


def test_concurrent_managers_serialize_the_entire_refresh_callback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def first_refresh(token: dict) -> dict:
        first_entered.set()
        assert release_first.wait(timeout=2)
        return increment_callback(token)

    def second_refresh(token: dict) -> dict:
        second_entered.set()
        return increment_callback(token)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(manager(path).refresh, first_refresh)
        assert first_entered.wait(timeout=1)
        second = executor.submit(manager(path).refresh, second_refresh)
        assert not second_entered.wait(timeout=0.05)
        release_first.set()
        assert first.result(timeout=2)["token"]["generation"] == 1
        assert second.result(timeout=2)["token"]["generation"] == 2

    assert json.loads(path.read_text(encoding="utf-8")) == token_document(2)


def test_process_lock_prevents_lost_refresh_updates(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    context = multiprocessing.get_context("fork")
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(target=_process_refresh, args=(str(path), start, results))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start.set()
    observed = sorted(results.get(timeout=5) for _ in processes)
    for process in processes:
        process.join(timeout=5)

    assert all(process.exitcode == 0 for process in processes)
    assert observed == [1, 2]
    assert json.loads(path.read_text(encoding="utf-8")) == token_document(2)


def test_lock_timeout_is_explicit_and_does_not_invoke_second_callback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    write_token(path, token_document())
    first_entered = threading.Event()
    release_first = threading.Event()
    second_called = False

    def hold_lock(token: dict) -> dict:
        first_entered.set()
        assert release_first.wait(timeout=2)
        return increment_callback(token)

    def must_not_run(token: dict) -> dict:
        nonlocal second_called
        second_called = True
        return increment_callback(token)

    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(manager(path).refresh, hold_lock)
        assert first_entered.wait(timeout=1)
        impatient = manager(path, lock_timeout_seconds=0.01)
        with pytest.raises(TokenLockTimeoutError):
            impatient.refresh(must_not_run)
        release_first.set()
        first.result(timeout=2)

    assert second_called is False
    assert impatient.health().state is TokenManagerState.LOCK_TIMEOUT
    assert json.loads(path.read_text(encoding="utf-8")) == token_document(1)


def test_replace_failure_leaves_original_and_removes_temporary_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "tokens.json"
    original = token_document()
    write_token(path, original)
    token_manager = manager(path)

    def fail_replace(_source, _destination) -> None:
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(TokenPersistenceError, match="persisted atomically"):
        token_manager.refresh(increment_callback)

    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert not list(tmp_path.glob(".tokens.json.*.tmp"))
    assert token_manager.health().state is TokenManagerState.PERSISTENCE_FAILED


def test_store_rejects_symlink_and_nonfinite_token_data(tmp_path: Path) -> None:
    real_path = tmp_path / "real-token.json"
    write_token(real_path, token_document())
    symlink_path = tmp_path / "tokens.json"
    symlink_path.symlink_to(real_path)

    with pytest.raises(TokenCorruptError):
        manager(symlink_path).load()

    value = token_document()
    value["token"]["extra"] = float("nan")
    write_token(real_path, value)
    with pytest.raises(TokenCorruptError):
        manager(real_path).load()
