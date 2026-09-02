from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import pytest

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.auth import PriorityClass
from schwab_gateway.scheduler import (
    ExecutionScheduler,
    SchedulerCapacityError,
    SchedulerQueueTimeoutError,
    SchedulerUpstreamTimeoutError,
    gateway_active_admitted,
    scheduler_allocated,
    scheduler_execution,
    scheduler_queue_depth,
)


class ConcurrencyProbe:
    def __init__(self) -> None:
        self.active = 0
        self.maximum = 0
        self.started: list[str] = []
        self.finished: list[str] = []

    async def call(
        self,
        name: str,
        *,
        release: asyncio.Event | None = None,
        delay: float = 0,
        fail: bool = False,
    ) -> str:
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        self.started.append(name)
        try:
            if release is not None:
                await release.wait()
            if delay:
                await asyncio.sleep(delay)
            if fail:
                raise RuntimeError("synthetic upstream failure")
            return name
        finally:
            self.finished.append(name)
            self.active -= 1


async def wait_for(predicate: Callable[[], bool], *, timeout: float = 1) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


def submit(
    scheduler: ExecutionScheduler,
    priority: PriorityClass,
    name: str,
    operation: Callable[[], Awaitable[str]],
    *,
    queue_timeout: float = 1,
    execution_timeout: float = 1,
) -> asyncio.Task[str]:
    return asyncio.create_task(
        scheduler.execute(
            priority,
            name,
            operation,
            queue_timeout_seconds=queue_timeout,
            execution_timeout_seconds=execution_timeout,
        )
    )


@pytest.mark.asyncio
async def test_three_protected_requests_get_full_timeout_only_after_dispatch() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=3, background_capacity=1)
    )
    probe = ConcurrencyProbe()

    tasks = [
        submit(
            scheduler,
            PriorityClass.PROTECTED,
            f"protected-{index}",
            lambda index=index: probe.call(f"protected-{index}", delay=0.03),
            queue_timeout=0.2,
            execution_timeout=0.04,
        )
        for index in range(3)
    ]

    assert await asyncio.gather(*tasks) == [
        "protected-0",
        "protected-1",
        "protected-2",
    ]
    assert probe.started == ["protected-0", "protected-1", "protected-2"]
    assert probe.maximum == 1
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_protected_dispatches_before_queued_background_and_fifo_within_class() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=3, background_capacity=3)
    )
    probe = ConcurrencyProbe()
    release = asyncio.Event()

    active_background = submit(
        scheduler,
        PriorityClass.BACKGROUND,
        "background-0",
        lambda: probe.call("background-0", release=release),
    )
    await wait_for(lambda: probe.started == ["background-0"])
    background_1 = submit(
        scheduler,
        PriorityClass.BACKGROUND,
        "background-1",
        lambda: probe.call("background-1"),
    )
    background_2 = submit(
        scheduler,
        PriorityClass.BACKGROUND,
        "background-2",
        lambda: probe.call("background-2"),
    )
    protected_0 = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "protected-0",
        lambda: probe.call("protected-0"),
    )
    protected_1 = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "protected-1",
        lambda: probe.call("protected-1"),
    )
    release.set()

    await asyncio.gather(
        active_background,
        background_1,
        background_2,
        protected_0,
        protected_1,
    )
    assert probe.started == [
        "background-0",
        "protected-0",
        "protected-1",
        "background-1",
        "background-2",
    ]
    assert probe.maximum == 1


@pytest.mark.asyncio
async def test_mixed_burst_keeps_independent_capacity_and_one_worker() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=3, background_capacity=4)
    )
    probe = ConcurrencyProbe()
    release = asyncio.Event()

    first = submit(
        scheduler,
        PriorityClass.BACKGROUND,
        "background-0",
        lambda: probe.call("background-0", release=release),
    )
    await wait_for(lambda: probe.started == ["background-0"])
    background = [
        submit(
            scheduler,
            PriorityClass.BACKGROUND,
            f"background-{index}",
            lambda index=index: probe.call(f"background-{index}"),
        )
        for index in range(1, 4)
    ]
    protected = [
        submit(
            scheduler,
            PriorityClass.PROTECTED,
            f"protected-{index}",
            lambda index=index: probe.call(f"protected-{index}"),
        )
        for index in range(3)
    ]
    await wait_for(
        lambda: scheduler.snapshot().background == 4
        and scheduler.snapshot().protected == 3
    )
    with pytest.raises(SchedulerCapacityError):
        await scheduler.execute(
            PriorityClass.BACKGROUND,
            "background-rejected",
            lambda: probe.call("background-rejected"),
            queue_timeout_seconds=1,
            execution_timeout_seconds=1,
        )
    assert scheduler.snapshot().protected == 3
    release.set()

    await asyncio.gather(first, *background, *protected)
    assert probe.started == [
        "background-0",
        "protected-0",
        "protected-1",
        "protected-2",
        "background-1",
        "background-2",
        "background-3",
    ]
    assert probe.maximum == 1
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_queue_timeout_removes_job_and_it_never_runs() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=2, background_capacity=1)
    )
    probe = ConcurrencyProbe()
    release = asyncio.Event()
    active = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "active",
        lambda: probe.call("active", release=release),
    )
    await wait_for(lambda: probe.started == ["active"])

    with pytest.raises(SchedulerQueueTimeoutError):
        await scheduler.execute(
            PriorityClass.PROTECTED,
            "expires-in-queue",
            lambda: probe.call("expires-in-queue"),
            queue_timeout_seconds=0.01,
            execution_timeout_seconds=1,
        )
    assert scheduler.snapshot().protected == 1
    release.set()
    assert await active == "active"
    await asyncio.sleep(0)
    assert probe.started == ["active"]
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_dispatch_atomically_rejects_job_expired_during_event_loop_stall() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=2, background_capacity=1)
    )
    probe = ConcurrencyProbe()
    active_entered = asyncio.Event()
    block_loop = asyncio.Event()

    async def active() -> str:
        active_entered.set()
        await block_loop.wait()
        time.sleep(0.03)
        return await probe.call("active")

    first = submit(scheduler, PriorityClass.PROTECTED, "active", active)
    await active_entered.wait()
    expired = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "expired",
        lambda: probe.call("expired"),
        queue_timeout=0.005,
    )
    await wait_for(lambda: scheduler.snapshot().queued_protected == 1)
    block_loop.set()

    assert await first == "active"
    with pytest.raises(SchedulerQueueTimeoutError):
        await expired
    assert probe.started == ["active"]
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_execution_deadline_uses_completion_time_after_event_loop_stall() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=1, background_capacity=1)
    )

    async def late_operation() -> str:
        time.sleep(0.03)
        return "late"

    with pytest.raises(SchedulerUpstreamTimeoutError):
        await scheduler.execute(
            PriorityClass.PROTECTED,
            "late",
            late_operation,
            queue_timeout_seconds=1,
            execution_timeout_seconds=0.005,
        )
    await scheduler.wait_idle()
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_actual_timeout_keeps_slot_until_detached_operation_finishes() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=2, background_capacity=1)
    )
    probe = ConcurrencyProbe()
    release = asyncio.Event()
    timed_out = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "slow",
        lambda: probe.call("slow", release=release),
        execution_timeout=0.01,
    )
    with pytest.raises(SchedulerUpstreamTimeoutError):
        await timed_out
    assert probe.active == 1
    assert scheduler.snapshot().worker_active is True

    next_request = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "next",
        lambda: probe.call("next"),
    )
    await asyncio.sleep(0.02)
    assert probe.started == ["slow"]
    assert probe.maximum == 1
    release.set()
    assert await next_request == "next"
    assert probe.started == ["slow", "next"]
    assert probe.maximum == 1
    assert scheduler.snapshot().total == 0

    outcomes = {
        sample.labels["outcome"]
        for metric in scheduler_execution.collect()
        for sample in metric.samples
        if sample.name.endswith("_count") and sample.labels.get("operation") == "slow"
    }
    assert "timeout_drained_success" in outcomes


@pytest.mark.asyncio
async def test_actual_timeout_reports_eventual_drain_error() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=1, background_capacity=1)
    )
    release = asyncio.Event()

    async def fails_after_timeout() -> str:
        await release.wait()
        raise RuntimeError("late failure")

    timed_out = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "timeout-then-error",
        fails_after_timeout,
        execution_timeout=0.005,
    )
    with pytest.raises(SchedulerUpstreamTimeoutError):
        await timed_out
    release.set()
    await scheduler.wait_idle()

    outcomes = {
        sample.labels["outcome"]
        for metric in scheduler_execution.collect()
        for sample in metric.samples
        if sample.name.endswith("_count")
        and sample.labels.get("operation") == "timeout-then-error"
    }
    assert outcomes == {"timeout_drained_error"}


@pytest.mark.asyncio
async def test_queued_and_running_cancellation_release_only_the_caller() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=3, background_capacity=1)
    )
    probe = ConcurrencyProbe()
    release = asyncio.Event()
    running = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "running",
        lambda: probe.call("running", release=release),
    )
    await wait_for(lambda: probe.started == ["running"])
    queued = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "queued",
        lambda: probe.call("queued"),
    )
    await wait_for(lambda: scheduler.snapshot().queued_protected == 1)

    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert scheduler.snapshot().protected == 1

    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert probe.active == 1
    assert scheduler.snapshot().protected == 1
    release.set()
    await scheduler.wait_idle()
    assert probe.started == ["running"]
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_failure_is_one_attempt_and_releases_capacity() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=1, background_capacity=1)
    )
    probe = ConcurrencyProbe()

    with pytest.raises(RuntimeError, match="synthetic upstream failure"):
        await scheduler.execute(
            PriorityClass.PROTECTED,
            "fails",
            lambda: probe.call("fails", fail=True),
            queue_timeout_seconds=1,
            execution_timeout_seconds=1,
        )
    assert probe.started == ["fails"]
    assert scheduler.snapshot().total == 0


@pytest.mark.asyncio
async def test_shutdown_removes_queue_and_cannot_release_running_slot_early() -> None:
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=2, background_capacity=1)
    )
    probe = ConcurrencyProbe()
    release = asyncio.Event()
    running = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "shutdown-running",
        lambda: probe.call("shutdown-running", release=release),
    )
    await wait_for(lambda: probe.started == ["shutdown-running"])
    queued = submit(
        scheduler,
        PriorityClass.PROTECTED,
        "shutdown-queued",
        lambda: probe.call("shutdown-queued"),
    )
    await wait_for(lambda: scheduler.snapshot().queued_protected == 1)

    shutdown = asyncio.create_task(scheduler.shutdown())
    await wait_for(lambda: scheduler.snapshot().queued_protected == 0)
    with pytest.raises(asyncio.CancelledError):
        await queued
    shutdown.cancel()
    await asyncio.sleep(0)
    assert not shutdown.done()
    assert scheduler.snapshot().worker_active is True
    assert probe.active == 1

    release.set()
    assert await running == "shutdown-running"
    await shutdown
    assert probe.started == ["shutdown-running"]
    assert scheduler.snapshot().total == 0


def test_scheduler_initializes_zero_metric_series_for_both_classes() -> None:
    ExecutionScheduler(AdmissionPolicy(protected_capacity=1, background_capacity=1))

    for metric in (scheduler_queue_depth, scheduler_allocated, gateway_active_admitted):
        values = {
            sample.labels["priority_class"]: sample.value
            for family in metric.collect()
            for sample in family.samples
            if sample.name == metric._name  # noqa: SLF001 - Prometheus exposes no public name
        }
        assert values == {"protected": 0, "background": 0}
