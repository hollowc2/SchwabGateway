"""Pin the token manager's state, reason and refresh counter for every failure class."""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY
from schwab_token_store import (
    AtomicTokenManager,
    TokenCallbackScopeError,
    TokenCorruptError,
    TokenExpiredError,
    TokenLockTimeoutError,
    TokenManagerError,
    TokenManagerState,
    TokenMissingError,
    TokenPersistenceError,
)

# (store error, state, reason, refresh counter result)
FAILURES = [
    (TokenMissingError("x"), TokenManagerState.MISSING, "token_missing", "missing"),
    (TokenExpiredError("x"), TokenManagerState.EXPIRED, "refresh_token_expired", "expired"),
    (TokenLockTimeoutError("x"), TokenManagerState.LOCK_TIMEOUT, "lock_timeout", "lock_timeout"),
    (
        TokenPersistenceError("x"),
        TokenManagerState.PERSISTENCE_FAILED,
        "store_unavailable",
        "persistence_error",
    ),
    (TokenCorruptError("x"), TokenManagerState.CORRUPT, "token_corrupt", "corrupt"),
    (TokenCallbackScopeError("x"), TokenManagerState.CORRUPT, "token_corrupt", "corrupt"),
]


class _FailingStore:
    def __init__(self, error: TokenManagerError) -> None:
        self.error = error

    def locked(self, timeout_seconds: float):
        raise self.error


def _refresh_count(result: str) -> float:
    return REGISTRY.get_sample_value("schwab_gateway_token_refresh_total", {"result": result}) or 0


@pytest.mark.parametrize(("error", "state", "reason", "_result"), FAILURES)
def test_load_failure_records_state_and_reason(error, state, reason, _result) -> None:
    manager = AtomicTokenManager(_FailingStore(error))

    with pytest.raises(type(error)):
        manager.load()

    assert (manager.health().state, manager.health().reason) == (state, reason)


@pytest.mark.parametrize(("error", "state", "reason", "result"), FAILURES)
def test_refresh_failure_records_state_reason_and_counter(error, state, reason, result) -> None:
    manager = AtomicTokenManager(_FailingStore(error))
    before = _refresh_count(result)

    with pytest.raises(type(error)):
        manager.refresh(lambda token: token)

    assert (manager.health().state, manager.health().reason) == (state, reason)
    assert _refresh_count(result) == before + 1


@pytest.mark.parametrize(("error", "state", "reason", "_result"), FAILURES)
def test_access_transaction_failure_before_entry_records_load_state(
    error, state, reason, _result
) -> None:
    manager = AtomicTokenManager(_FailingStore(error))

    with pytest.raises(type(error)):
        manager.run_access_transaction(lambda read, write: None)

    assert (manager.health().state, manager.health().reason) == (state, reason)
