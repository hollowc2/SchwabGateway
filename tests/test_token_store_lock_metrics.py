"""Pin token-store lock metrics and lock-file failure messages for both lock modes."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from prometheus_client import REGISTRY
from schwab_token_store import AtomicFileTokenStore, TokenLockTimeoutError, TokenPersistenceError


def _count(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(f"{name}_count", labels) or 0


def _store(tmp_path: Path) -> AtomicFileTokenStore:
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps({"token": {}}))
    os.chmod(path, 0o600)
    return AtomicFileTokenStore(path)


@pytest.mark.parametrize("mode", ["exclusive", "shared"])
def test_acquired_lock_observes_wait_and_hold(tmp_path: Path, mode: str) -> None:
    store = _store(tmp_path)
    with store.locked(1.0):
        pass  # creates the lock file the shared reader requires
    wait = _count("schwab_gateway_token_lock_wait_seconds", mode=mode, outcome="acquired")
    hold = _count("schwab_gateway_token_lock_hold_seconds", mode=mode)

    with store.locked(1.0) if mode == "exclusive" else store.read_locked(1.0) as transaction:
        assert transaction.read() == {"token": {}}

    assert _count("schwab_gateway_token_lock_wait_seconds", mode=mode, outcome="acquired") == (
        wait + 1
    )
    assert _count("schwab_gateway_token_lock_hold_seconds", mode=mode) == hold + 1


@pytest.mark.parametrize("mode", ["exclusive", "shared"])
def test_flock_timeout_observes_wait_without_hold(tmp_path: Path, mode: str) -> None:
    store = _store(tmp_path)
    contender = AtomicFileTokenStore(store.path)
    timeouts = _count("schwab_gateway_token_lock_wait_seconds", mode=mode, outcome="timeout")

    with store.locked(1.0):
        hold = _count("schwab_gateway_token_lock_hold_seconds", mode=mode)
        # Same path shares the thread lock (an RLock), so only the flock can block here.
        lock = contender.locked(0.01) if mode == "exclusive" else contender.read_locked(0.01)
        with pytest.raises(TokenLockTimeoutError, match="timed out waiting for the token lock"):
            with lock:
                pass
        assert _count("schwab_gateway_token_lock_hold_seconds", mode=mode) == hold

    assert _count("schwab_gateway_token_lock_wait_seconds", mode=mode, outcome="timeout") == (
        timeouts + 1
    )


def test_lock_file_open_failures_keep_their_messages(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with pytest.raises(TokenPersistenceError, match="^token lock file cannot be opened read-only$"):
        with store.read_locked(0):
            pass

    store._lock_path.mkdir()
    with pytest.raises(TokenPersistenceError, match="^token lock file cannot be opened safely$"):
        with store.locked(0):
            pass
