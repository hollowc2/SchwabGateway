"""Bounded strict-priority scheduling for the gateway's one Schwab execution slot."""

from __future__ import annotations

import asyncio
import math
import time
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
    started: asyncio.Future[None]
    result: asyncio.Future[Any]
    state: Literal["queued", "running", "finished"] = "queued"
    caller_cancelled: bool = False


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
        self._idle = asyncio.Event()
        self._idle.set()

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
        job = _Job(
            priority=priority,
            operation_name=operation_name,
            operation=operation,
            execution_timeout_seconds=execution_timeout_seconds,
            enqueued_at=time.perf_counter(),
            started=loop.create_future(),
            result=loop.create_future(),
        )
        job.result.add_done_callback(_consume_future_exception)

        async with self._lock:
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
                async with asyncio.timeout(queue_timeout_seconds):
                    await asyncio.shield(job.started)
            except TimeoutError:
                async with self._lock:
                    if job.state == "queued":
                        wait_seconds = time.perf_counter() - job.enqueued_at
                        self._remove_queued_locked(job)
                        scheduler_queue_wait.labels(
                            priority_class=priority.value,
                            operation=operation_name,
                        ).observe(wait_seconds)
                        scheduler_queue_timeouts.labels(
                            priority_class=priority.value,
                            operation=operation_name,
                        ).inc()
                        log.warning(
                            "gateway_scheduler_queue_timeout",
                            priority_class=priority.value,
                            operation=operation_name,
                            queue_wait_ms=round(wait_seconds * 1000, 2),
                        )
                        raise SchedulerQueueTimeoutError(
                            "gateway worker queue wait timed out"
                        ) from None
                    # Dispatch won the boundary race. The execution budget, which began
                    # at dispatch, remains authoritative.
            return await asyncio.shield(job.result)
        except asyncio.CancelledError:
            async with self._lock:
                if job.state == "queued":
                    wait_seconds = time.perf_counter() - job.enqueued_at
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

    def _dispatch_locked(self) -> None:
        if self._worker_active:
            return
        queue = self._queues[PriorityClass.PROTECTED]
        if not queue:
            queue = self._queues[PriorityClass.BACKGROUND]
        if not queue:
            if not any(self._allocated.values()):
                self._idle.set()
            return

        job = queue.popleft()
        job.state = "running"
        self._worker_active = True
        scheduler_worker_active.set(1)
        self._update_class_metrics(job.priority)
        wait_seconds = time.perf_counter() - job.enqueued_at
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
        started_at = time.perf_counter()
        outcome = "error"

        async def invoke() -> Any:
            return await job.operation()

        operation_task = asyncio.create_task(invoke())
        try:
            done, _pending = await asyncio.wait(
                {operation_task}, timeout=job.execution_timeout_seconds
            )
            if done:
                try:
                    value = operation_task.result()
                except asyncio.CancelledError:
                    outcome = "cancelled"
                    if not job.result.done():
                        job.result.cancel()
                except BaseException as exc:
                    outcome = "error"
                    if not job.result.done():
                        job.result.set_exception(exc)
                else:
                    outcome = "success"
                    if not job.result.done():
                        job.result.set_result(value)
            else:
                outcome = "timeout"
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
                # Never cancel a timed-out synchronous operation. The provider awaits a
                # shielded completion future, so retaining this task also retains the
                # physical worker lease until its daemon thread really exits.
                try:
                    await asyncio.shield(operation_task)
                except BaseException:
                    pass
        finally:
            elapsed = time.perf_counter() - started_at
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
