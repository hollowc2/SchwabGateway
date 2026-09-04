"""Bounded strict-priority scheduling for the gateway's one Schwab execution slot."""

from __future__ import annotations

import asyncio
import math
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

from prometheus_client import Counter, Gauge, Histogram

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.auth import PriorityClass
from schwab_gateway.logging import get_logger

log = get_logger(__name__)
T = TypeVar("T")

scheduler_queue_depth = Gauge(
    "schwab_gateway_scheduler_queue_depth",
    "Market-data requests waiting for the serialized Schwab worker",
    ["priority_class"],
)
scheduler_allocated = Gauge(
    "schwab_gateway_scheduler_allocated_requests",
    "Market-data requests running or waiting by bounded priority pool",
    ["priority_class"],
)
gateway_active_admitted = Gauge(
    "gateway_active_admitted_requests",
    "Market-data requests running or waiting by bounded priority pool",
    ["priority_class"],
)
scheduler_queue_wait = Histogram(
    "schwab_gateway_scheduler_queue_wait_seconds",
    "Time spent queued before dispatch, timeout, or caller cancellation",
    ["priority_class", "operation"],
)
scheduler_dispatches = Counter(
    "schwab_gateway_scheduler_dispatch_total",
    "Operations dispatched to the serialized Schwab worker",
    ["priority_class", "operation"],
)
scheduler_execution = Histogram(
    "schwab_gateway_scheduler_upstream_execution_seconds",
    "Actual upstream task duration, including detached timeout drain",
    ["priority_class", "operation", "outcome"],
)
scheduler_capacity_rejections = Counter(
    "schwab_gateway_scheduler_capacity_rejections_total",
    "Requests rejected because their bounded priority pool was full",
    ["priority_class"],
)
scheduler_queue_timeouts = Counter(
    "schwab_gateway_scheduler_queue_wait_timeouts_total",
    "Requests removed after their queue-wait budget expired",
    ["priority_class", "operation"],
)
scheduler_upstream_timeouts = Counter(
    "schwab_gateway_scheduler_upstream_timeouts_total",
    "Dispatched operations that exceeded their upstream execution budget",
    ["priority_class", "operation"],
)
scheduler_cancellations = Counter(
    "schwab_gateway_scheduler_cancellations_total",
    "Caller cancellations by scheduler lifecycle state",
    ["priority_class", "operation", "state"],
)
scheduler_worker_active = Gauge(
    "schwab_gateway_scheduler_worker_active",
    "One while the serialized Schwab execution slot is reserved",
)


class SchedulerCapacityError(RuntimeError):
    """The caller's independent priority pool is full."""


class SchedulerQueueTimeoutError(RuntimeError):
    """A request's queue-wait budget expired before dispatch."""


class SchedulerUpstreamTimeoutError(RuntimeError):
    """A dispatched operation exceeded its upstream execution budget."""


@dataclass(frozen=True, slots=True)
class SchedulerSnapshot:
    protected: int
    background: int
    queued_protected: int
    queued_background: int
    worker_active: bool
    task_count: int

    @property
    def total(self) -> int:
        return self.protected + self.background


@dataclass(slots=True)
class _Job:
    priority: PriorityClass
    operation_name: str
    operation: Callable[[], Awaitable[Any]]
    execution_timeout_seconds: float
    enqueued_at: float
    queue_deadline: float
    started: asyncio.Future[None]
    result: asyncio.Future[Any]
    state: Literal["queued", "running", "finished"] = "queued"
    caller_cancelled: bool = False
    queue_timed_out: bool = False


def _consume_future_exception(future: asyncio.Future[Any]) -> None:
    if future.cancelled():
        return
    try:
        future.exception()
    except (Exception, asyncio.CancelledError):
        pass


class ExecutionScheduler:
    """Strict-priority FIFO dispatcher for one non-preemptible execution slot.

    Capacity includes both the running operation and queued operations. A caller that
    disconnects from running work releases only its HTTP wait: the internal operation
    task and physical slot remain owned until the operation really completes.

    All operation types (spot, quotes, option-chain, history, session) share this one
    physical slot: `_dispatch_locked` will not start a second job while `_worker_active`
    is set, regardless of priority class. `_allocated`/`_limits` bound how many callers
    of each priority class may be admitted (running + queued), not how many run
    concurrently. This is a deliberate simplification from the single upstream token
    lock (schwab-py issues one HTTP call per locked transaction) rather than an
    oversight, but it means a burst of unrelated operations can push end-to-end latency
    for a latency-sensitive read like option-chain well past its own upstream execution
    time — see the 2026-09 option-chain latency investigation. Before adding a second
    execution slot or a per-operation pool, check `schwab_gateway_scheduler_queue_wait_seconds`
    by `operation`: if queueing concentrates in a few operation types, a dedicated slot
    for those is a smaller change than relaxing the single-slot model everywhere.
    """

    def __init__(self, policy: AdmissionPolicy) -> None:
        self._limits = {
            PriorityClass.PROTECTED: policy.protected_capacity,
            PriorityClass.BACKGROUND: policy.background_capacity,
        }
        self._allocated = {priority: 0 for priority in PriorityClass}
        self._queues: dict[PriorityClass, deque[_Job]] = {
            priority: deque() for priority in PriorityClass
        }
        self._lock = asyncio.Lock()
        self._worker_active = False
        self._lifecycle_tasks: set[asyncio.Task[None]] = set()
        self._accepting = True
        self._idle = asyncio.Event()
        self._idle.set()
        scheduler_worker_active.set(0)
        for priority in PriorityClass:
            self._update_class_metrics(priority)

    async def execute(
        self,
        priority: PriorityClass,
        operation_name: str,
        operation: Callable[[], Awaitable[T]],
        *,
        queue_timeout_seconds: float,
        execution_timeout_seconds: float,
    ) -> T:
        if not isinstance(priority, PriorityClass):
            raise ValueError("unknown gateway priority class")
        if not operation_name:
            raise ValueError("scheduler operation name is required")
        for value, label in (
            (queue_timeout_seconds, "queue timeout"),
            (execution_timeout_seconds, "execution timeout"),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"scheduler {label} must be finite and positive")

        loop = asyncio.get_running_loop()
        enqueued_at = loop.time()
        job = _Job(
            priority=priority,
            operation_name=operation_name,
            operation=operation,
            execution_timeout_seconds=execution_timeout_seconds,
            enqueued_at=enqueued_at,
            queue_deadline=enqueued_at + queue_timeout_seconds,
            started=loop.create_future(),
            result=loop.create_future(),
        )
        job.started.add_done_callback(_consume_future_exception)
        job.result.add_done_callback(_consume_future_exception)

        async with self._lock:
            if not self._accepting:
                raise SchedulerCapacityError("gateway scheduler is shutting down")
            if self._allocated[priority] >= self._limits[priority]:
                scheduler_capacity_rejections.labels(priority_class=priority.value).inc()
                log.warning(
                    "gateway_scheduler_capacity_rejected",
                    priority_class=priority.value,
                    operation=operation_name,
                )
                raise SchedulerCapacityError("gateway request capacity is unavailable")
            self._allocated[priority] += 1
            self._queues[priority].append(job)
            self._idle.clear()
            self._update_class_metrics(priority)
            self._dispatch_locked()

        try:
            try:
                async with asyncio.timeout_at(job.queue_deadline):
                    await asyncio.shield(job.started)
            except TimeoutError:
                async with self._lock:
                    if job.state == "queued":
                        self._expire_queued_locked(job, asyncio.get_running_loop().time())
                    if job.queue_timed_out:
                        raise SchedulerQueueTimeoutError(
                            "gateway worker queue wait timed out"
                        ) from None
                    # Dispatch won the boundary race. The execution budget, which began
                    # at dispatch, remains authoritative.
            return await asyncio.shield(job.result)
        except asyncio.CancelledError:
            async with self._lock:
                if job.state == "queued":
                    wait_seconds = asyncio.get_running_loop().time() - job.enqueued_at
                    self._remove_queued_locked(job)
                    scheduler_queue_wait.labels(
                        priority_class=priority.value,
                        operation=operation_name,
                    ).observe(wait_seconds)
                    cancellation_state = "queued"
                elif job.state == "running":
                    job.caller_cancelled = True
                    cancellation_state = "running"
                else:
                    raise
                scheduler_cancellations.labels(
                    priority_class=priority.value,
                    operation=operation_name,
                    state=cancellation_state,
                ).inc()
                log.info(
                    "gateway_scheduler_caller_cancelled",
                    priority_class=priority.value,
                    operation=operation_name,
                    state=cancellation_state,
                )
            raise

    def snapshot(self) -> SchedulerSnapshot:
        """Return bounded scheduler state for deterministic diagnostics and tests."""
        return SchedulerSnapshot(
            protected=self._allocated[PriorityClass.PROTECTED],
            background=self._allocated[PriorityClass.BACKGROUND],
            queued_protected=len(self._queues[PriorityClass.PROTECTED]),
            queued_background=len(self._queues[PriorityClass.BACKGROUND]),
            worker_active=self._worker_active,
            task_count=len(self._lifecycle_tasks),
        )

    async def wait_idle(self) -> None:
        """Wait until no running or queued scheduler lifecycle remains."""
        await self._idle.wait()
        tasks = tuple(self._lifecycle_tasks)
        if tasks:
            await asyncio.gather(*tasks)

    async def shutdown(self) -> None:
        """Stop admission, remove queued callers, and drain the physical worker.

        Cleanup cancellation is deliberately deferred until the running operation really
        finishes. This is the graceful-shutdown counterpart to caller cancellation: neither
        event is allowed to release the one physical slot while synchronous token work is
        still alive.
        """
        async with self._lock:
            self._accepting = False
            queued = tuple(job for queue in self._queues.values() for job in queue)
            for job in queued:
                self._remove_queued_locked(job)
            if queued:
                log.info("gateway_scheduler_shutdown_queue_removed", count=len(queued))

        waiter = asyncio.create_task(self._idle.wait())
        while not waiter.done():
            try:
                await asyncio.shield(waiter)
            except asyncio.CancelledError:
                log.warning("gateway_scheduler_shutdown_cancellation_deferred")
        await waiter

    def _update_class_metrics(self, priority: PriorityClass) -> None:
        scheduler_queue_depth.labels(priority_class=priority.value).set(
            len(self._queues[priority])
        )
        scheduler_allocated.labels(priority_class=priority.value).set(
            self._allocated[priority]
        )
        gateway_active_admitted.labels(priority_class=priority.value).set(
            self._allocated[priority]
        )

    def _remove_queued_locked(self, job: _Job) -> None:
        self._queues[job.priority].remove(job)
        job.state = "finished"
        job.started.cancel()
        job.result.cancel()
        self._allocated[job.priority] -= 1
        self._update_class_metrics(job.priority)
        if not any(self._allocated.values()):
            self._idle.set()

    def _expire_queued_locked(self, job: _Job, now: float) -> None:
        self._queues[job.priority].remove(job)
        job.state = "finished"
        job.queue_timed_out = True
        error = SchedulerQueueTimeoutError("gateway worker queue wait timed out")
        if not job.started.done():
            job.started.set_exception(error)
        if not job.result.done():
            job.result.cancel()
        self._allocated[job.priority] -= 1
        self._update_class_metrics(job.priority)
        wait_seconds = max(0.0, now - job.enqueued_at)
        scheduler_queue_wait.labels(
            priority_class=job.priority.value,
            operation=job.operation_name,
        ).observe(wait_seconds)
        scheduler_queue_timeouts.labels(
            priority_class=job.priority.value,
            operation=job.operation_name,
        ).inc()
        log.warning(
            "gateway_scheduler_queue_timeout",
            priority_class=job.priority.value,
            operation=job.operation_name,
            queue_wait_ms=round(wait_seconds * 1000, 2),
        )
        if not any(self._allocated.values()):
            self._idle.set()

    def _dispatch_locked(self) -> None:
        if self._worker_active:
            return
        now = asyncio.get_running_loop().time()
        while True:
            queue = self._queues[PriorityClass.PROTECTED]
            if not queue:
                queue = self._queues[PriorityClass.BACKGROUND]
            if not queue:
                if not any(self._allocated.values()):
                    self._idle.set()
                return
            if queue[0].queue_deadline > now:
                break
            self._expire_queued_locked(queue[0], now)

        job = queue.popleft()
        job.state = "running"
        self._worker_active = True
        scheduler_worker_active.set(1)
        self._update_class_metrics(job.priority)
        wait_seconds = max(0.0, now - job.enqueued_at)
        scheduler_queue_wait.labels(
            priority_class=job.priority.value,
            operation=job.operation_name,
        ).observe(wait_seconds)
        scheduler_dispatches.labels(
            priority_class=job.priority.value,
            operation=job.operation_name,
        ).inc()
        job.started.set_result(None)
        log.info(
            "gateway_scheduler_dispatched",
            priority_class=job.priority.value,
            operation=job.operation_name,
            queue_wait_ms=round(wait_seconds * 1000, 2),
        )
        task = asyncio.create_task(self._run_job(job))
        self._lifecycle_tasks.add(task)
        task.add_done_callback(self._lifecycle_tasks.discard)

    async def _run_job(self, job: _Job) -> None:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        execution_deadline = started_at + job.execution_timeout_seconds
        operation_completed_at: float | None = None
        outcome = "error"

        async def invoke() -> Any:
            nonlocal operation_completed_at
            try:
                return await job.operation()
            finally:
                operation_completed_at = loop.time()

        operation_task = asyncio.create_task(invoke())
        try:
            while not operation_task.done():
                remaining = execution_deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    done, _pending = await asyncio.wait(
                        {operation_task}, timeout=remaining
                    )
                except asyncio.CancelledError:
                    # The scheduler lifecycle is internal and non-preemptible. Defer its
                    # cancellation exactly as we defer an HTTP caller's cancellation.
                    log.warning("gateway_scheduler_lifecycle_cancellation_deferred")
                else:
                    if not done:
                        break

            completed_in_budget = (
                operation_completed_at is not None
                and operation_completed_at <= execution_deadline
            )
            if not completed_in_budget:
                scheduler_upstream_timeouts.labels(
                    priority_class=job.priority.value,
                    operation=job.operation_name,
                ).inc()
                if not job.result.done():
                    job.result.set_exception(
                        SchedulerUpstreamTimeoutError("upstream execution timed out")
                    )
                log.warning(
                    "gateway_scheduler_upstream_timeout",
                    priority_class=job.priority.value,
                    operation=job.operation_name,
                    execution_timeout_seconds=job.execution_timeout_seconds,
                )
                # Never cancel timed-out work. Even cancellation of this internal lifecycle
                # is deferred until the operation and any synchronous provider thread drain.
                while not operation_task.done():
                    try:
                        await asyncio.shield(operation_task)
                    except asyncio.CancelledError:
                        log.warning("gateway_scheduler_lifecycle_cancellation_deferred")
                    except Exception:
                        break
                if operation_task.cancelled():
                    outcome = "timeout_drained_cancelled"
                else:
                    try:
                        operation_task.result()
                    except Exception:
                        outcome = "timeout_drained_error"
                    else:
                        outcome = "timeout_drained_success"
            else:
                try:
                    value = operation_task.result()
                except asyncio.CancelledError:
                    outcome = "cancelled"
                    if not job.result.done():
                        job.result.cancel()
                except Exception as exc:
                    outcome = "error"
                    if not job.result.done():
                        job.result.set_exception(exc)
                else:
                    outcome = "success"
                    if not job.result.done():
                        job.result.set_result(value)
        finally:
            elapsed = loop.time() - started_at
            scheduler_execution.labels(
                priority_class=job.priority.value,
                operation=job.operation_name,
                outcome=outcome,
            ).observe(elapsed)
            log.info(
                "gateway_scheduler_execution_finished",
                priority_class=job.priority.value,
                operation=job.operation_name,
                outcome=outcome,
                execution_ms=round(elapsed * 1000, 2),
            )
            async with self._lock:
                job.state = "finished"
                self._allocated[job.priority] -= 1
                self._worker_active = False
                scheduler_worker_active.set(0)
                self._update_class_metrics(job.priority)
                self._dispatch_locked()
                if not any(self._allocated.values()):
                    self._idle.set()
