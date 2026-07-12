"""Chronological events and a stable deterministic priority queue."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.domain.execution.models import MarketSnapshot, Order, OrderType, TimeInForce
from app.domain.market_data.models import Side


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("event timestamp must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class SignalEvent:
    timestamp: datetime
    signal_id: str
    exchange: str
    symbol: str
    side: Side
    quantity: Decimal
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.GTC
    limit_price: Decimal | None = None
    calculation_delay: timedelta = timedelta(0)
    submission_delay: timedelta = timedelta(0)
    post_only: bool = False
    reduce_only: bool = False
    expires_at: datetime | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    reason: str = "strategy"

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp))
        if not self.signal_id:
            raise ValueError("signal_id is required")
        if self.quantity <= 0:
            raise ValueError("signal quantity must be positive")
        if self.calculation_delay < timedelta(0) or self.submission_delay < timedelta(0):
            raise ValueError("delays cannot be negative")
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit signal requires limit_price")
        if self.order_type is OrderType.MARKET and self.limit_price is not None:
            raise ValueError("market signal cannot include limit_price")
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", _utc(self.expires_at))
            eligible_at = self.timestamp + self.calculation_delay + self.submission_delay
            if self.expires_at <= eligible_at:
                raise ValueError("signal expiry must follow submission")


@dataclass(frozen=True, slots=True)
class OrderCreationEvent:
    timestamp: datetime
    signal: SignalEvent

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp))


@dataclass(frozen=True, slots=True)
class OrderSubmissionEvent:
    timestamp: datetime
    order: Order

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp))


@dataclass(frozen=True, slots=True)
class MarketEvent:
    snapshot: MarketSnapshot

    @property
    def timestamp(self) -> datetime:
        return self.snapshot.timestamp


@dataclass(frozen=True, slots=True)
class FundingEvent:
    timestamp: datetime
    exchange: str
    symbol: str
    rate: Decimal
    mark_price: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp))
        if self.mark_price <= 0:
            raise ValueError("funding mark price must be positive")


type BacktestEvent = (
    SignalEvent | OrderCreationEvent | OrderSubmissionEvent | MarketEvent | FundingEvent
)


def event_priority(event: BacktestEvent) -> int:
    """Market data wins ties, preventing a signal from consuming its source event."""
    if isinstance(event, MarketEvent):
        return 10
    if isinstance(event, FundingEvent):
        return 20
    if isinstance(event, SignalEvent):
        return 30
    if isinstance(event, OrderCreationEvent):
        return 40
    return 50


@dataclass(order=True, slots=True)
class _QueuedEvent:
    timestamp: datetime
    priority: int
    sequence: int
    event: BacktestEvent = field(compare=False)


class EventQueue:
    def __init__(self) -> None:
        self._heap: list[_QueuedEvent] = []
        self._sequence = 0

    def push(self, event: BacktestEvent) -> None:
        self._sequence += 1
        heapq.heappush(
            self._heap,
            _QueuedEvent(event.timestamp, event_priority(event), self._sequence, event),
        )

    def pop(self) -> BacktestEvent:
        if not self._heap:
            raise IndexError("event queue is empty")
        return heapq.heappop(self._heap).event

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __len__(self) -> int:
        return len(self._heap)
