"""Deterministic credential-free load proof for serialized priority scheduling."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass

from schwab_gateway.admission import AdmissionPolicy
from schwab_gateway.auth import PriorityClass
from schwab_gateway.scheduler import (
    ExecutionScheduler,
    SchedulerCapacityError,
    SchedulerUpstreamTimeoutError,
)


@dataclass(frozen=True, slots=True)
class SyntheticSchedulerProof:
    maximum_upstream_concurrency: int
    mixed_dispatch_order: tuple[str, ...]
    three_wide_protected_completed: bool
    protected_precedence_proven: bool
    background_capacity_shed: bool
    execution_timeout_drained: bool
    final_allocated: int
    final_queued: int
    final_lifecycle_tasks: int


class _FakeProvider:
    def __init__(self) -> None:
        self.active = 0
        self.maximum = 0
        self.started: list[str] = []

    async def call(
        self,
        name: str,
        *,
        release: asyncio.Event | None = None,
        delay: float = 0,
    ) -> str:
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        self.started.append(name)
        try:
            if release is not None:
                await release.wait()
            if delay:
                await asyncio.sleep(delay)
            return name
        finally:
            self.active -= 1


async def _until(predicate, *, timeout: float = 1) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


def _submit(
    scheduler: ExecutionScheduler,
    provider: _FakeProvider,
    priority: PriorityClass,
    name: str,
    *,
    release: asyncio.Event | None = None,
    delay: float = 0,
    queue_timeout: float = 1,
    execution_timeout: float = 1,
) -> asyncio.Task[str]:
    return asyncio.create_task(
        scheduler.execute(
            priority,
            name,
            lambda: provider.call(name, release=release, delay=delay),
            queue_timeout_seconds=queue_timeout,
            execution_timeout_seconds=execution_timeout,
        )
    )


async def run_synthetic_scheduler_proof() -> SyntheticSchedulerProof:
    """Exercise protected bursts, background pressure, and detached timeout drain."""
    scheduler = ExecutionScheduler(
        AdmissionPolicy(protected_capacity=3, background_capacity=4)
    )
    provider = _FakeProvider()
    background_release = asyncio.Event()
    first_background = _submit(
        scheduler,
        provider,
        PriorityClass.BACKGROUND,
        "background-slow",
        release=background_release,
    )
    await _until(lambda: provider.started == ["background-slow"])

    backgrounds = [
        _submit(
            scheduler,
            provider,
            PriorityClass.BACKGROUND,
            f"background-{index}",
        )
        for index in range(1, 4)
    ]
    protected = [
        _submit(
            scheduler,
            provider,
            PriorityClass.PROTECTED,
            symbol,
            delay=0.015,
            queue_timeout=0.2,
            execution_timeout=0.02,
        )
        for symbol in ("SPX", "NDX", "XSP")
    ]
    await _until(
        lambda: scheduler.snapshot().background == 4
        and scheduler.snapshot().protected == 3
    )
    try:
        await scheduler.execute(
            PriorityClass.BACKGROUND,
            "background-shed",
            lambda: provider.call("background-shed"),
            queue_timeout_seconds=1,
            execution_timeout_seconds=1,
        )
    except SchedulerCapacityError:
        background_shed = True
    else:  # pragma: no cover - the proof fails loudly if the invariant regresses
        background_shed = False

    background_release.set()
    results = await asyncio.gather(first_background, *backgrounds, *protected)
    mixed_order = tuple(provider.started)
    protected_completed = all(symbol in results for symbol in ("SPX", "NDX", "XSP"))
    protected_precedence = mixed_order == (
        "background-slow",
        "SPX",
        "NDX",
        "XSP",
        "background-1",
        "background-2",
        "background-3",
    )

    timeout_release = asyncio.Event()
    timed_out = _submit(
        scheduler,
        provider,
        PriorityClass.PROTECTED,
        "protected-timeout",
        release=timeout_release,
        execution_timeout=0.01,
    )
    try:
        await timed_out
    except SchedulerUpstreamTimeoutError:
        pass
    following = _submit(
        scheduler,
        provider,
        PriorityClass.PROTECTED,
        "protected-after-timeout",
    )
    await asyncio.sleep(0.02)
    retained = provider.started[-1] == "protected-timeout" and provider.active == 1
    timeout_release.set()
    await following
    await scheduler.wait_idle()
    snapshot = scheduler.snapshot()

    proof = SyntheticSchedulerProof(
        maximum_upstream_concurrency=provider.maximum,
        mixed_dispatch_order=mixed_order,
        three_wide_protected_completed=protected_completed,
        protected_precedence_proven=protected_precedence,
        background_capacity_shed=background_shed,
        execution_timeout_drained=(
            retained and provider.started[-1] == "protected-after-timeout"
        ),
        final_allocated=snapshot.total,
        final_queued=snapshot.queued_protected + snapshot.queued_background,
        final_lifecycle_tasks=snapshot.task_count,
    )
    if proof != SyntheticSchedulerProof(
        maximum_upstream_concurrency=1,
        mixed_dispatch_order=mixed_order,
        three_wide_protected_completed=True,
        protected_precedence_proven=True,
        background_capacity_shed=True,
        execution_timeout_drained=True,
        final_allocated=0,
        final_queued=0,
        final_lifecycle_tasks=0,
    ):
        raise RuntimeError(f"synthetic scheduler proof failed: {proof!r}")
    return proof


def main() -> None:
    print(json.dumps(asdict(asyncio.run(run_synthetic_scheduler_proof())), indent=2))


if __name__ == "__main__":
    main()
