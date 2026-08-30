"""Locked, atomic token persistence without a Schwab runtime dependency."""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import errno
import fcntl
import json
import math
import os
import stat
import tempfile
import threading
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, TypeVar

import structlog
from prometheus_client import Counter, Gauge, Histogram

log = structlog.get_logger(__name__)
UTC = dt.timezone.utc
DEFAULT_REFRESH_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_LOCK_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_TOKEN_BYTES = 1024 * 1024

token_refresh_total = Counter(
    "schwab_gateway_token_refresh_total",
    "Token refresh transactions by bounded result category",
    ["result"],
)
token_state = Gauge(
    "schwab_gateway_token_state",
    "Current single-token manager state (one-hot)",
    ["state"],
)
token_lock_wait_seconds = Histogram(
    "schwab_gateway_token_lock_wait_seconds",
    "Time spent waiting to acquire the token-file lock",
    ["mode", "outcome"],
)
token_lock_hold_seconds = Histogram(
    "schwab_gateway_token_lock_hold_seconds",
    "Time the token-file lock is held",
    ["mode"],
)

TokenDocument = dict[str, Any]
TokenRefreshCallback = Callable[[TokenDocument], Mapping[str, Any]]
TokenReadCallback = Callable[[], TokenDocument]
TransactionResult = TypeVar("TransactionResult")


class TokenWriteCallback(Protocol):
    """schwab-py-compatible token writer supplied to a client factory."""

    def __call__(
        self,
        token: Mapping[str, Any],
        *args: object,
        **kwargs: object,
    ) -> None: ...


TokenAccessOperation = Callable[
    [TokenReadCallback, TokenWriteCallback],
    TransactionResult,
]
AsyncTokenAccessOperation = Callable[
    [TokenReadCallback, TokenWriteCallback],
    Awaitable[TransactionResult],
]


class TokenManagerState(str, Enum):
    UNINITIALIZED = "uninitialized"
    READY = "ready"
    REFRESHING = "refreshing"
    MISSING = "missing"
    CORRUPT = "corrupt"
    EXPIRED = "expired"
    REVOKED = "revoked"
    REAUTHORIZATION_REQUIRED = "reauthorization_required"
    LOCK_TIMEOUT = "lock_timeout"
    REFRESH_FAILED = "refresh_failed"
    PERSISTENCE_FAILED = "persistence_failed"


@dataclass(frozen=True)
class TokenManagerHealth:
    state: TokenManagerState
    reason: str
    updated_at: dt.datetime


class TokenManagerError(RuntimeError):
    """Base class whose messages are safe to expose without token contents."""


class TokenMissingError(TokenManagerError):
    pass


class TokenCorruptError(TokenManagerError):
    pass


class TokenExpiredError(TokenManagerError):
    pass


class TokenLockTimeoutError(TokenManagerError):
    pass


class TokenPersistenceError(TokenManagerError):
    pass


class TokenRefreshError(TokenManagerError):
    pass


class TokenCallbackScopeError(TokenManagerError):
    pass


class TokenRevokedError(TokenManagerError):
    """A refresh callback uses this to classify an upstream revocation."""

    def __init__(self) -> None:
        super().__init__("token refresh was rejected as revoked")


class TokenReauthorizationRequiredError(TokenManagerError):
    """A refresh callback uses this when manual OAuth authorization is required."""

    def __init__(self) -> None:
        super().__init__("manual token reauthorization is required")


class TokenTransaction(Protocol):
    """Operations available only while a token-store lock is held."""

    def read(self) -> object: ...

    def write(self, token: Mapping[str, Any]) -> None: ...


class TokenReadTransaction(Protocol):
    """Read-only operations available while a shared token-store lock is held."""

    def read(self) -> object: ...


class TokenStore(Protocol):
    """Replaceable persistence boundary for one logical token document."""

    def locked(
        self,
        timeout_seconds: float,
    ) -> AbstractContextManager[TokenTransaction]: ...


_thread_locks_guard = threading.Lock()
_thread_locks: dict[str, threading.RLock] = {}


def _thread_lock_for(path: Path) -> threading.RLock:
    key = str(path.parent.resolve() / path.name)
    with _thread_locks_guard:
        return _thread_locks.setdefault(key, threading.RLock())


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    except OSError as exc:
        if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.EBADF}:
            raise
    finally:
        os.close(directory_fd)


class _AtomicFileTokenTransaction:
    def __init__(self, path: Path, max_token_bytes: int) -> None:
        self._path = path
        self._max_token_bytes = max_token_bytes

    def read(self) -> object:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            token_fd = os.open(self._path, flags)
        except FileNotFoundError:
            raise TokenMissingError("token document is missing") from None
        except OSError:
            raise TokenCorruptError("token document cannot be opened safely") from None

        try:
            file_stat = os.fstat(token_fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise TokenCorruptError("token document must be a regular file")
            if stat.S_IMODE(file_stat.st_mode) != 0o600:
                raise TokenCorruptError("token document permissions must be 0600")
            if file_stat.st_size > self._max_token_bytes:
                raise TokenCorruptError("token document exceeds the size limit")
            with os.fdopen(token_fd, encoding="utf-8") as token_file:
                token_fd = -1
                return json.load(token_file, parse_constant=_reject_json_constant)
        except TokenManagerError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            raise TokenCorruptError("token document is not valid JSON") from None
        finally:
            if token_fd >= 0:
                os.close(token_fd)

    def write(self, token: Mapping[str, Any]) -> None:
        temporary_path: Path | None = None
        temporary_fd = -1
        try:
            temporary_fd, raw_path = tempfile.mkstemp(
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                dir=self._path.parent,
            )
            temporary_path = Path(raw_path)
            os.fchmod(temporary_fd, 0o600)
            with os.fdopen(temporary_fd, "w", encoding="utf-8") as token_file:
                temporary_fd = -1
                json.dump(
                    token,
                    token_file,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                token_file.write("\n")
                token_file.flush()
                os.fsync(token_file.fileno())
            os.replace(temporary_path, self._path)
            temporary_path = None
            _fsync_directory(self._path.parent)
        except (OSError, TypeError, ValueError):
            raise TokenPersistenceError(
                "token document could not be persisted atomically"
            ) from None
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
            if temporary_path is not None:
                try:
                    temporary_path.unlink()
                except FileNotFoundError:
                    pass


class AtomicFileTokenStore:
    """One JSON token file protected by thread and process locks."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_token_bytes: int = DEFAULT_MAX_TOKEN_BYTES,
    ) -> None:
        self.path = Path(path).expanduser().absolute()
        if not self.path.name:
            raise ValueError("token path must name a file")
        if max_token_bytes <= 0:
            raise ValueError("max token bytes must be positive")
        self._max_token_bytes = max_token_bytes
        self._lock_path = self.path.with_name(f".{self.path.name}.lock")
        self._thread_lock = _thread_lock_for(self.path)

    @contextmanager
    def read_locked(self, timeout_seconds: float) -> Iterator[TokenReadTransaction]:
        """Read under the writers' lock without requiring a writable token mount.

        Persistent writers open ``.tokens.json.lock`` read/write and take an exclusive
        flock. A read-only consumer can open that already-created lock file read-only
        and take a shared flock, preserving coordination without granting the consumer
        permission to create, replace, or truncate credential files.
        """
        if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
            raise ValueError("lock timeout must be finite and nonnegative")
        if not self.path.parent.is_dir():
            raise TokenPersistenceError("token parent directory is unavailable")

        wait_started = time.monotonic()
        deadline = wait_started + timeout_seconds
        if not self._thread_lock.acquire(timeout=timeout_seconds):
            token_lock_wait_seconds.labels(mode="shared", outcome="timeout").observe(
                time.monotonic() - wait_started
            )
            raise TokenLockTimeoutError("timed out waiting for the token lock")

        lock_fd = -1
        hold_started: float | None = None
        try:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                lock_fd = os.open(self._lock_path, flags)
                lock_stat = os.fstat(lock_fd)
                if (
                    not stat.S_ISREG(lock_stat.st_mode)
                    or stat.S_IMODE(lock_stat.st_mode) != 0o600
                ):
                    raise OSError
            except OSError:
                if lock_fd >= 0:
                    os.close(lock_fd)
                    lock_fd = -1
                raise TokenPersistenceError(
                    "token lock file cannot be opened read-only"
                ) from None

            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        token_lock_wait_seconds.labels(
                            mode="shared", outcome="timeout"
                        ).observe(time.monotonic() - wait_started)
                        raise TokenLockTimeoutError("timed out waiting for the token lock")
                    time.sleep(min(0.01, remaining))
                except OSError:
                    raise TokenPersistenceError("token lock could not be acquired") from None

            token_lock_wait_seconds.labels(mode="shared", outcome="acquired").observe(
                time.monotonic() - wait_started
            )
            hold_started = time.monotonic()
            yield _AtomicFileTokenTransaction(self.path, self._max_token_bytes)
        finally:
            if hold_started is not None:
                token_lock_hold_seconds.labels(mode="shared").observe(
                    time.monotonic() - hold_started
                )
            if lock_fd >= 0:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)
            self._thread_lock.release()

    @contextmanager
    def locked(self, timeout_seconds: float) -> Iterator[TokenTransaction]:
        if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
            raise ValueError("lock timeout must be finite and nonnegative")
        if not self.path.parent.is_dir():
            raise TokenPersistenceError("token parent directory is unavailable")

        wait_started = time.monotonic()
        deadline = wait_started + timeout_seconds
        if not self._thread_lock.acquire(timeout=timeout_seconds):
            token_lock_wait_seconds.labels(mode="exclusive", outcome="timeout").observe(
                time.monotonic() - wait_started
            )
            raise TokenLockTimeoutError("timed out waiting for the token lock")

        lock_fd = -1
        hold_started: float | None = None
        try:
            flags = (
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                lock_fd = os.open(self._lock_path, flags, 0o600)
                os.fchmod(lock_fd, 0o600)
            except OSError:
                raise TokenPersistenceError("token lock file cannot be opened safely") from None

            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        token_lock_wait_seconds.labels(
                            mode="exclusive", outcome="timeout"
                        ).observe(time.monotonic() - wait_started)
                        raise TokenLockTimeoutError("timed out waiting for the token lock")
                    time.sleep(min(0.01, remaining))
                except OSError:
                    raise TokenPersistenceError("token lock could not be acquired") from None

            token_lock_wait_seconds.labels(
                mode="exclusive", outcome="acquired"
            ).observe(time.monotonic() - wait_started)
            hold_started = time.monotonic()
            yield _AtomicFileTokenTransaction(self.path, self._max_token_bytes)
        finally:
            if hold_started is not None:
                token_lock_hold_seconds.labels(mode="exclusive").observe(
                    time.monotonic() - hold_started
                )
            if lock_fd >= 0:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)
            self._thread_lock.release()


def validate_token_document(value: object) -> TokenDocument:
    """Validate only the stable schwab-py token envelope and required OAuth fields."""
    try:
        if not isinstance(value, Mapping):
            raise TokenCorruptError("token document must be an object")
        creation_timestamp = value.get("creation_timestamp")
        if (
            isinstance(creation_timestamp, bool)
            or not isinstance(creation_timestamp, (int, float))
            or not math.isfinite(float(creation_timestamp))
            or creation_timestamp <= 0
        ):
            raise TokenCorruptError("token creation timestamp is invalid")
        payload = value.get("token")
        if not isinstance(payload, Mapping):
            raise TokenCorruptError("token payload must be an object")
        required_fields = ("access_token", "refresh_token")
        if any(
            not isinstance(payload.get(name), str) or not payload[name]
            for name in required_fields
        ):
            raise TokenCorruptError("token payload is missing required OAuth fields")

        document = copy.deepcopy(dict(value))
        json.dumps(document, allow_nan=False)
    except TokenCorruptError:
        raise
    except Exception:
        raise TokenCorruptError("token document is not JSON serializable") from None
    return document


class AtomicTokenManager:
    """Serialize refreshes and atomically persist one validated token document."""

    def __init__(
        self,
        store: TokenStore,
        *,
        lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
        refresh_token_ttl_seconds: float = DEFAULT_REFRESH_TOKEN_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not math.isfinite(lock_timeout_seconds) or lock_timeout_seconds < 0:
            raise ValueError("lock timeout must be finite and nonnegative")
        if not math.isfinite(refresh_token_ttl_seconds) or refresh_token_ttl_seconds <= 0:
            raise ValueError("refresh token TTL must be finite and positive")
        self._store = store
        self._lock_timeout_seconds = lock_timeout_seconds
        self._refresh_token_ttl_seconds = refresh_token_ttl_seconds
        self._clock = clock
        self._health_lock = threading.Lock()
        now = self._timestamp()
        self._health = TokenManagerHealth(
            TokenManagerState.UNINITIALIZED,
            "not_checked",
            now,
        )
        token_state.labels(state=TokenManagerState.UNINITIALIZED.value).set(1)

    def health(self) -> TokenManagerHealth:
        with self._health_lock:
            return self._health

    def load(self) -> TokenDocument:
        try:
            with self._store.locked(self._lock_timeout_seconds) as transaction:
                token = self._load_from_transaction(transaction)
        except TokenManagerError as exc:
            self._record_load_failure(exc)
            raise
        self._transition(TokenManagerState.READY, "token_loaded")
        return copy.deepcopy(token)

    def refresh(self, callback: TokenRefreshCallback) -> TokenDocument:
        """Run one fake/replaceable refresh callback under the exclusive token lock."""
        try:
            with self._store.locked(self._lock_timeout_seconds) as transaction:
                current = self._load_from_transaction(transaction)
                self._transition(TokenManagerState.REFRESHING, "refresh_started")
                try:
                    refreshed_value = callback(copy.deepcopy(current))
                except TokenRevokedError:
                    token_refresh_total.labels(result="revoked").inc()
                    self._transition(TokenManagerState.REVOKED, "upstream_revoked")
                    raise
                except TokenReauthorizationRequiredError:
                    token_refresh_total.labels(result="reauthorization_required").inc()
                    self._transition(
                        TokenManagerState.REAUTHORIZATION_REQUIRED,
                        "manual_reauthorization_required",
                    )
                    raise
                except Exception:
                    token_refresh_total.labels(result="callback_error").inc()
                    self._transition(TokenManagerState.REFRESH_FAILED, "callback_error")
                    raise TokenRefreshError("token refresh callback failed") from None

                try:
                    refreshed = validate_token_document(refreshed_value)
                except TokenCorruptError:
                    token_refresh_total.labels(result="invalid_result").inc()
                    self._transition(TokenManagerState.REFRESH_FAILED, "invalid_refresh_result")
                    raise TokenRefreshError(
                        "token refresh callback returned invalid data"
                    ) from None

                transaction.write(refreshed)
        except (TokenRevokedError, TokenReauthorizationRequiredError, TokenRefreshError):
            raise
        except TokenManagerError as exc:
            self._record_refresh_failure(exc)
            raise

        token_refresh_total.labels(result="success").inc()
        self._transition(TokenManagerState.READY, "refresh_succeeded")
        return copy.deepcopy(refreshed)

    def run_access_transaction(
        self,
        operation: TokenAccessOperation[TransactionResult],
    ) -> TransactionResult:
        """Run an SDK-shaped token read/client operation/write lifecycle under one lock."""
        entered_store = False
        try:
            with self._store.locked(self._lock_timeout_seconds) as transaction:
                entered_store = True
                try:
                    current = self._load_from_transaction(transaction)
                except TokenManagerError as exc:
                    self._record_load_failure(exc)
                    raise
                self._transition(TokenManagerState.READY, "token_loaded")
                callbacks = _ScopedTokenCallbacks(self, transaction, current)
                try:
                    return operation(callbacks.read, callbacks.write)
                finally:
                    callbacks.close()
        except TokenManagerError as exc:
            if not entered_store:
                self._record_load_failure(exc)
            raise

    async def run_access_transaction_async(
        self,
        operation: AsyncTokenAccessOperation[TransactionResult],
    ) -> TransactionResult:
        """Run one async SDK token lifecycle while holding the exclusive lock.

        This is intentionally a transaction primitive, not a long-running stream
        primitive. Callers should complete token-dependent HTTP work (for example a
        Schwab stream login) inside ``operation`` and return only resources that no
        longer invoke the scoped token callbacks. The callbacks are invalidated and
        the token lock is released as soon as the awaited operation returns.
        """

        entered_store = False
        context = self._store.locked(self._lock_timeout_seconds)
        # AtomicFileTokenStore also owns a threading.RLock. A dedicated one-thread
        # executor guarantees context enter/exit happen on the same OS thread while
        # leaving the asyncio event loop responsive during flock contention.
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="token-lock")
        loop = asyncio.get_running_loop()
        enter_task = loop.run_in_executor(executor, context.__enter__)
        try:
            try:
                transaction = await asyncio.shield(enter_task)
            except asyncio.CancelledError:
                # to_thread cannot cancel a running flock acquisition. Wait for its
                # bounded result and release it before propagating cancellation.
                try:
                    await enter_task
                except Exception:
                    raise asyncio.CancelledError from None
                await asyncio.shield(
                    loop.run_in_executor(executor, context.__exit__, None, None, None)
                )
                raise
            entered_store = True
            try:
                current = self._load_from_transaction(transaction)
            except TokenManagerError as exc:
                self._record_load_failure(exc)
                raise
            self._transition(TokenManagerState.READY, "token_loaded")
            callbacks = _ScopedTokenCallbacks(self, transaction, current)
            try:
                return await operation(callbacks.read, callbacks.write)
            finally:
                callbacks.close()
        except TokenManagerError as exc:
            if not entered_store:
                self._record_load_failure(exc)
            raise
        finally:
            if entered_store:
                await asyncio.shield(
                    loop.run_in_executor(executor, context.__exit__, None, None, None)
                )
            executor.shutdown(wait=True)

    def _load_from_transaction(self, transaction: TokenTransaction) -> TokenDocument:
        token = validate_token_document(transaction.read())
        creation_timestamp = float(token["creation_timestamp"])
        if self._clock() >= creation_timestamp + self._refresh_token_ttl_seconds:
            raise TokenExpiredError("refresh token lifetime has expired")
        return token

    def _record_load_failure(self, exc: TokenManagerError) -> None:
        if isinstance(exc, TokenMissingError):
            self._transition(TokenManagerState.MISSING, "token_missing")
        elif isinstance(exc, TokenExpiredError):
            self._transition(TokenManagerState.EXPIRED, "refresh_token_expired")
        elif isinstance(exc, TokenLockTimeoutError):
            self._transition(TokenManagerState.LOCK_TIMEOUT, "lock_timeout")
        elif isinstance(exc, TokenPersistenceError):
            self._transition(TokenManagerState.PERSISTENCE_FAILED, "store_unavailable")
        else:
            self._transition(TokenManagerState.CORRUPT, "token_corrupt")

    def _record_refresh_failure(self, exc: TokenManagerError) -> None:
        if isinstance(exc, TokenMissingError):
            result = "missing"
            state = TokenManagerState.MISSING
            reason = "token_missing"
        elif isinstance(exc, TokenExpiredError):
            result = "expired"
            state = TokenManagerState.EXPIRED
            reason = "refresh_token_expired"
        elif isinstance(exc, TokenLockTimeoutError):
            result = "lock_timeout"
            state = TokenManagerState.LOCK_TIMEOUT
            reason = "lock_timeout"
        elif isinstance(exc, TokenPersistenceError):
            result = "persistence_error"
            state = TokenManagerState.PERSISTENCE_FAILED
            reason = "store_unavailable"
        else:
            result = "corrupt"
            state = TokenManagerState.CORRUPT
            reason = "token_corrupt"
        token_refresh_total.labels(result=result).inc()
        self._transition(state, reason)

    def _transition(self, state: TokenManagerState, reason: str) -> None:
        with self._health_lock:
            previous = self._health.state
            self._health = TokenManagerHealth(state, reason, self._timestamp())
        token_state.labels(state=previous.value).set(0)
        token_state.labels(state=state.value).set(1)
        log.info(
            "schwab_token_manager_transition",
            previous_state=previous.value,
            state=state.value,
            reason=reason,
        )

    def _timestamp(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self._clock(), tz=UTC)


class _ScopedTokenCallbacks:
    """Keep SDK token callbacks live only for one manager-owned transaction."""

    def __init__(
        self,
        manager: AtomicTokenManager,
        transaction: TokenTransaction,
        current: TokenDocument,
    ) -> None:
        self._manager = manager
        self._transaction = transaction
        self._current: TokenDocument | None = current
        self._active = True
        self._guard = threading.RLock()

    def read(self) -> TokenDocument:
        with self._guard:
            self._require_active()
            assert self._current is not None
            return copy.deepcopy(self._current)

    def write(
        self,
        token: Mapping[str, Any],
        *args: object,
        **kwargs: object,
    ) -> None:
        del args, kwargs
        with self._guard:
            self._require_active()
            self._manager._transition(TokenManagerState.REFRESHING, "refresh_started")
            try:
                refreshed = validate_token_document(token)
            except TokenCorruptError:
                token_refresh_total.labels(result="invalid_result").inc()
                self._manager._transition(
                    TokenManagerState.REFRESH_FAILED,
                    "invalid_refresh_result",
                )
                raise TokenRefreshError(
                    "token write callback received invalid data"
                ) from None

            try:
                self._transaction.write(refreshed)
            except TokenManagerError as exc:
                self._manager._record_refresh_failure(exc)
                raise
            self._current = refreshed
            token_refresh_total.labels(result="success").inc()
            self._manager._transition(TokenManagerState.READY, "refresh_succeeded")

    def close(self) -> None:
        with self._guard:
            self._active = False
            self._current = None

    def _require_active(self) -> None:
        if not self._active:
            raise TokenCallbackScopeError(
                "token callback is outside its transaction scope"
            )
