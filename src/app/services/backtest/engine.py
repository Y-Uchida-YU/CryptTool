"""Minimal event-driven backtester with conservative causal execution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.domain.execution.models import Fill, InstrumentRules, Order, OrderType, TimeInForce
from app.domain.execution.simulator import ExecutionModelConfig, ExecutionSimulator
from app.domain.market_data.models import Side
from app.domain.portfolio.ledger import PortfolioLedger
from app.domain.portfolio.models import (
    FundingRecord,
    LiquidationDecision,
    PortfolioSnapshot,
    Position,
)
from app.services.backtest.events import (
    BacktestEvent,
    EventQueue,
    FundingEvent,
    MarketEvent,
    OrderCreationEvent,
    OrderSubmissionEvent,
    SignalEvent,
)

InstrumentKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class RejectedSignal:
    signal_id: str
    timestamp: datetime
    reason: str


@dataclass(frozen=True, slots=True)
class BacktestResult:
    processed_events: int
    event_trace: tuple[tuple[datetime, str], ...]
    orders: tuple[Order, ...]
    fills: tuple[Fill, ...]
    snapshots: tuple[PortfolioSnapshot, ...]
    funding: tuple[FundingRecord, ...]
    liquidations: tuple[LiquidationDecision, ...]
    rejected_signals: tuple[RejectedSignal, ...]
    final_cash: Decimal
    final_equity: Decimal
    run_id: str = ""
    data_snapshot_id: str = ""


class BacktestEngine:
    def __init__(
        self,
        initial_cash: Decimal,
        rules: Mapping[InstrumentKey, InstrumentRules],
        *,
        leverage: Decimal = Decimal("1"),
        execution_config: ExecutionModelConfig | None = None,
        run_id: str = "",
        data_snapshot_id: str = "",
    ) -> None:
        self.queue = EventQueue()
        self.execution = ExecutionSimulator(rules, execution_config)
        self.portfolio = PortfolioLedger(initial_cash, rules, leverage)
        self._order_sequence = 0
        self._exit_sequence = 0
        self._seen_signals: set[str] = set()
        self._exit_pending: set[InstrumentKey] = set()
        self._liquidation_pending: set[InstrumentKey] = set()
        self._trace: list[tuple[datetime, str]] = []
        self._liquidations: list[LiquidationDecision] = []
        self._rejected_signals: list[RejectedSignal] = []
        self.run_id = run_id
        self.data_snapshot_id = data_snapshot_id

    def add_event(self, event: BacktestEvent) -> None:
        self.queue.push(event)

    def add_events(self, events: Iterable[BacktestEvent]) -> None:
        for event in events:
            self.add_event(event)

    def run(self, *, maximum_events: int = 1_000_000) -> BacktestResult:
        processed = 0
        while self.queue:
            if processed >= maximum_events:
                raise RuntimeError("maximum event count exceeded")
            event = self.queue.pop()
            processed += 1
            self._trace.append((event.timestamp, type(event).__name__))
            self._dispatch(event)
        return BacktestResult(
            processed_events=processed,
            event_trace=tuple(self._trace),
            orders=tuple(self.execution.orders.values()),
            fills=tuple(self.execution.fills),
            snapshots=tuple(self.portfolio.snapshots),
            funding=tuple(self.portfolio.funding_records),
            liquidations=tuple(self._liquidations),
            rejected_signals=tuple(self._rejected_signals),
            final_cash=self.portfolio.cash,
            final_equity=self.portfolio.equity,
            run_id=self.run_id,
            data_snapshot_id=self.data_snapshot_id,
        )

    def _dispatch(self, event: BacktestEvent) -> None:
        if isinstance(event, MarketEvent):
            self._on_market(event)
        elif isinstance(event, FundingEvent):
            self._on_funding(event)
        elif isinstance(event, SignalEvent):
            self._on_signal(event)
        elif isinstance(event, OrderCreationEvent):
            self._on_order_creation(event)
        elif isinstance(event, OrderSubmissionEvent):
            self.execution.submit(event.order)

    def _on_signal(self, signal: SignalEvent) -> None:
        if signal.signal_id in self._seen_signals:
            self._rejected_signals.append(
                RejectedSignal(signal.signal_id, signal.timestamp, "duplicate signal_id")
            )
            return
        self._seen_signals.add(signal.signal_id)
        self.queue.push(
            OrderCreationEvent(
                timestamp=signal.timestamp + signal.calculation_delay,
                signal=signal,
            )
        )

    def _on_order_creation(self, event: OrderCreationEvent) -> None:
        signal = event.signal
        self._order_sequence += 1
        submitted_at = event.timestamp + signal.submission_delay
        order = Order(
            order_id=f"order-{self._order_sequence:012d}",
            signal_id=signal.signal_id,
            exchange=signal.exchange,
            symbol=signal.symbol,
            side=signal.side,
            quantity=signal.quantity,
            order_type=signal.order_type,
            time_in_force=signal.time_in_force,
            signal_at=signal.timestamp,
            created_at=event.timestamp,
            submitted_at=submitted_at,
            limit_price=signal.limit_price,
            post_only=signal.post_only,
            reduce_only=signal.reduce_only,
            expires_at=signal.expires_at,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
        )
        self.queue.push(OrderSubmissionEvent(timestamp=submitted_at, order=order))

    def _on_market(self, event: MarketEvent) -> None:
        snapshot = event.snapshot
        key = (snapshot.exchange, snapshot.symbol)
        self.portfolio.mark(snapshot)

        def cap(order: Order) -> Decimal:
            price = snapshot.ask if order.side is Side.BUY else snapshot.bid
            return self.portfolio.maximum_fill_quantity(order, price)

        def consume(order: Order, fill: Fill) -> None:
            self.portfolio.apply_fill(
                fill,
                reduce_only=order.reduce_only,
                stop_loss=order.stop_loss,
                take_profit=order.take_profit,
            )
            position = self.portfolio.position(fill.exchange, fill.symbol)
            if not position.is_open:
                closed_key = (fill.exchange, fill.symbol)
                self._exit_pending.discard(closed_key)
                self._liquidation_pending.discard(closed_key)

        self.execution.match(snapshot, cap, consume)
        # Fill price is an execution price; valuation remains the independent mark.
        self.portfolio.mark(snapshot)
        position = self.portfolio.positions.get(key)
        if position is not None and position.is_open and key not in self._exit_pending:
            trigger = position.protection_trigger(snapshot)
            if trigger is not None:
                self._schedule_exit(position, snapshot.timestamp, trigger)

        liquidation = self.portfolio.liquidation_decision(snapshot.timestamp)
        if liquidation is not None:
            new_liquidations = set(liquidation.instruments) - self._liquidation_pending
            if new_liquidations:
                self._liquidations.append(liquidation)
                self._liquidation_pending.update(new_liquidations)
            for order in self.execution.orders.values():
                if not order.reduce_only:
                    order.cancel("portfolio liquidation")
            for liquidation_key in new_liquidations:
                liquidation_position = self.portfolio.positions[liquidation_key]
                if liquidation_position.is_open and liquidation_key not in self._exit_pending:
                    self._schedule_exit(liquidation_position, snapshot.timestamp, "liquidation")
        self.portfolio.capture(snapshot.timestamp)

    def _on_funding(self, event: FundingEvent) -> None:
        self.portfolio.apply_funding(
            event.exchange,
            event.symbol,
            event.timestamp,
            event.rate,
            event.mark_price,
        )
        self.portfolio.capture(event.timestamp)

    def _schedule_exit(self, position: Position, timestamp: datetime, reason: str) -> None:
        key = (position.exchange, position.symbol)
        self._exit_pending.add(key)
        self._exit_sequence += 1
        side = Side.SELL if position.quantity > 0 else Side.BUY
        self.queue.push(
            SignalEvent(
                timestamp=timestamp,
                signal_id=f"{reason}-{self._exit_sequence:012d}",
                exchange=position.exchange,
                symbol=position.symbol,
                side=side,
                quantity=abs(position.quantity),
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.GTC,
                reduce_only=True,
                reason=reason,
            )
        )
