"""Bounded in-memory recent snapshot storage and WebSocket fan-out."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Literal

from prometheus_client import Counter
from schwab_gateway_sdk.models import OrderBookSnapshotV1

OrderBookVenue = Literal["NASDAQ", "NYSE"]

order_book_subscriber_drops = Counter(
    "gateway_order_book_subscriber_drops_total",
    "Order-book snapshots dropped from bounded slow-subscriber queues",
)


@dataclass(eq=False)
class OrderBookSubscription:
    symbols: frozenset[str]
    venue: OrderBookVenue
    queue: asyncio.Queue[OrderBookSnapshotV1]


class OrderBookSnapshotStore:
    """Keep bounded recent depth and isolate slow subscribers with bounded queues."""

    def __init__(self, *, history_limit: int = 1000, subscriber_queue_limit: int = 100) -> None:
        if not 1 <= history_limit <= 10_000:
            raise ValueError("order-book history limit must be between 1 and 10000")
        if not 1 <= subscriber_queue_limit <= 1000:
            raise ValueError("subscriber queue limit must be between 1 and 1000")
        self._history_limit = history_limit
        self._subscriber_queue_limit = subscriber_queue_limit
        self._history: dict[tuple[str, OrderBookVenue], deque[OrderBookSnapshotV1]] = (
            defaultdict(lambda: deque(maxlen=self._history_limit))
        )
        self._subscriptions: set[OrderBookSubscription] = set()
        self.subscriber_drop_count = 0

    def publish(self, snapshot: OrderBookSnapshotV1) -> None:
        key = (snapshot.symbol, snapshot.venue)
        self._history[key].append(snapshot)
        for subscription in tuple(self._subscriptions):
            if (
                subscription.venue != snapshot.venue
                or snapshot.symbol not in subscription.symbols
            ):
                continue
            if subscription.queue.full():
                try:
                    subscription.queue.get_nowait()
                except asyncio.QueueEmpty:  # pragma: no cover - defensive race guard
                    pass
                self.subscriber_drop_count += 1
                order_book_subscriber_drops.inc()
            subscription.queue.put_nowait(snapshot)

    def recent(
        self, symbol: str, venue: OrderBookVenue, *, limit: int
    ) -> tuple[OrderBookSnapshotV1, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("recent order-book limit must be between 1 and 1000")
        values = self._history.get((symbol, venue), ())
        return tuple(values)[-limit:]

    def subscribe(
        self, symbols: frozenset[str], venue: OrderBookVenue
    ) -> OrderBookSubscription:
        if not symbols:
            raise ValueError("at least one order-book subscription symbol is required")
        subscription = OrderBookSubscription(
            symbols=symbols,
            venue=venue,
            queue=asyncio.Queue(maxsize=self._subscriber_queue_limit),
        )
        self._subscriptions.add(subscription)
        return subscription

    def unsubscribe(self, subscription: OrderBookSubscription) -> None:
        self._subscriptions.discard(subscription)
