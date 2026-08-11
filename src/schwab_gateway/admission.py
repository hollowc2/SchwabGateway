"""Bounded in-process admission policy for gateway market-data reads."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from schwab_gateway.auth import PriorityClass

MAX_CAPACITY_PER_CLASS = 256


class AdmissionCapacityError(RuntimeError):
    """The caller's bounded priority pool has no available permit."""


@dataclass(frozen=True)
class AdmissionPolicy:
    protected_capacity: int
    background_capacity: int

    def __post_init__(self) -> None:
        for value in (self.protected_capacity, self.background_capacity):
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError("gateway capacity must be an integer")
            if not 1 <= value <= MAX_CAPACITY_PER_CLASS:
                raise ValueError(
                    f"gateway capacity must be between 1 and {MAX_CAPACITY_PER_CLASS}"
                )


class AdmissionController:
    """Keep background work out of ButterflyGuy's protected capacity."""

    def __init__(self, policy: AdmissionPolicy) -> None:
        self._limits = {
            PriorityClass.PROTECTED: policy.protected_capacity,
            PriorityClass.BACKGROUND: policy.background_capacity,
        }
        self._active = {priority: 0 for priority in PriorityClass}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def admit(self, priority: PriorityClass) -> AsyncIterator[None]:
        if not isinstance(priority, PriorityClass):
            raise ValueError("unknown gateway priority class")
        async with self._lock:
            if self._active[priority] >= self._limits[priority]:
                raise AdmissionCapacityError("gateway request capacity is unavailable")
            self._active[priority] += 1
        try:
            yield
        finally:
            async with self._lock:
                self._active[priority] -= 1

    async def active_count(self, priority: PriorityClass) -> int:
        """Expose bounded state for deterministic fake-only tests."""
        if not isinstance(priority, PriorityClass):
            raise ValueError("unknown gateway priority class")
        async with self._lock:
            return self._active[priority]
