"""Auditable cash, position, margin, funding, and liquidation accounting."""

from __future__ import annotations

from collections.abc import Mapping
from copy import copy
from datetime import UTC, datetime
from decimal import Decimal

from app.domain.execution.models import ZERO, Fill, InstrumentRules, MarketSnapshot, Order
from app.domain.market_data.models import Side
from app.domain.portfolio.models import (
    FundingRecord,
    LiquidationDecision,
    PortfolioSnapshot,
    Position,
)

InstrumentKey = tuple[str, str]


class DuplicateFillError(ValueError):
    """Raised rather than silently applying the same fill twice."""


class PortfolioLedger:
    """Linear perpetual ledger; cash changes only through realized PnL, costs, and funding."""

    def __init__(
        self,
        initial_cash: Decimal,
        rules: Mapping[InstrumentKey, InstrumentRules],
        leverage: Decimal = Decimal("1"),
    ) -> None:
        if initial_cash <= ZERO:
            raise ValueError("initial_cash must be positive")
        if leverage <= ZERO:
            raise ValueError("leverage must be positive")
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.leverage = leverage
        self.rules = dict(rules)
        self.positions: dict[InstrumentKey, Position] = {}
        self.fills: list[Fill] = []
        self.funding_records: list[FundingRecord] = []
        self.snapshots: list[PortfolioSnapshot] = []
        self._processed_fill_ids: set[str] = set()

    def position(self, exchange: str, symbol: str) -> Position:
        key = (exchange, symbol)
        if key not in self.positions:
            self.positions[key] = Position(exchange=exchange, symbol=symbol)
        return self.positions[key]

    @property
    def unrealized_pnl(self) -> Decimal:
        return sum((position.unrealized_pnl for position in self.positions.values()), ZERO)

    @property
    def equity(self) -> Decimal:
        return self.cash + self.unrealized_pnl

    @property
    def gross_exposure(self) -> Decimal:
        return sum((position.notional for position in self.positions.values()), ZERO)

    @property
    def net_exposure(self) -> Decimal:
        return sum(
            (position.quantity * position.mark_price for position in self.positions.values()), ZERO
        )

    @property
    def margin_used(self) -> Decimal:
        return self.gross_exposure / self.leverage

    @property
    def maintenance_margin(self) -> Decimal:
        return sum(
            (
                position.notional * self.rules[key].maintenance_margin_rate
                for key, position in self.positions.items()
                if key in self.rules
            ),
            ZERO,
        )

    @property
    def available_margin(self) -> Decimal:
        return max(ZERO, self.equity - self.margin_used)

    def apply_fill(
        self,
        fill: Fill,
        *,
        reduce_only: bool = False,
        stop_loss: Decimal | None = None,
        take_profit: Decimal | None = None,
    ) -> Decimal:
        if fill.fill_id in self._processed_fill_ids:
            raise DuplicateFillError(f"duplicate fill: {fill.fill_id}")
        position = self.position(fill.exchange, fill.symbol)
        if reduce_only:
            if not position.is_open:
                raise ValueError("reduce-only fill has no open position")
            reduces = (position.quantity > ZERO and fill.side is Side.SELL) or (
                position.quantity < ZERO and fill.side is Side.BUY
            )
            if not reduces or fill.quantity > abs(position.quantity):
                raise ValueError("reduce-only fill would increase or reverse a position")
        # Validate protection on a copy first; invalid parameters must not leave partial accounting.
        candidate = copy(position)
        candidate.apply_fill(fill)
        if candidate.is_open and (stop_loss is not None or take_profit is not None):
            candidate.set_protection(stop_loss, take_profit)
        realized = position.apply_fill(fill)
        if position.is_open and (stop_loss is not None or take_profit is not None):
            position.set_protection(stop_loss, take_profit)
        self.cash += realized - fill.fee
        self._processed_fill_ids.add(fill.fill_id)
        self.fills.append(fill)
        return realized

    def mark(self, snapshot: MarketSnapshot) -> None:
        position = self.positions.get((snapshot.exchange, snapshot.symbol))
        if position is not None and position.is_open:
            position.mark_price = snapshot.valuation_price
            position.updated_at = snapshot.timestamp

    def apply_funding(
        self,
        exchange: str,
        symbol: str,
        timestamp: datetime,
        rate: Decimal,
        mark_price: Decimal,
    ) -> FundingRecord | None:
        if timestamp.tzinfo is None:
            raise ValueError("funding timestamp must be timezone-aware")
        if mark_price <= ZERO:
            raise ValueError("funding mark price must be positive")
        position = self.positions.get((exchange, symbol))
        if position is None or not position.is_open:
            return None
        # Positive funding: longs pay shorts. Signed quantity gives both cases.
        amount = -(position.quantity * mark_price * rate)
        self.cash += amount
        position.funding_pnl += amount
        record = FundingRecord(
            exchange=exchange,
            symbol=symbol,
            timestamp=timestamp.astimezone(UTC),
            rate=rate,
            mark_price=mark_price,
            position_quantity=position.quantity,
            amount=amount,
        )
        self.funding_records.append(record)
        return record

    def maximum_fill_quantity(self, order: Order, price: Decimal) -> Decimal:
        """Cap a fill by reduce-only semantics and currently available cross margin."""
        if price <= ZERO:
            return ZERO
        position = self.positions.get((order.exchange, order.symbol))
        current = position.quantity if position is not None else ZERO
        side_reduces = (current > ZERO and order.side is Side.SELL) or (
            current < ZERO and order.side is Side.BUY
        )
        closing_capacity = abs(current) if side_reduces else ZERO
        if order.reduce_only:
            return min(order.remaining_quantity, closing_capacity)
        rules = self.rules.get((order.exchange, order.symbol))
        fee_rate = rules.taker_fee_rate if rules is not None else ZERO
        margin_and_fee_per_unit = price / self.leverage + price * fee_rate
        opening_capacity = (
            self.available_margin / margin_and_fee_per_unit
            if margin_and_fee_per_unit > ZERO
            else ZERO
        )
        return min(order.remaining_quantity, closing_capacity + opening_capacity)

    def liquidation_decision(self, timestamp: datetime) -> LiquidationDecision | None:
        open_keys = tuple(sorted(key for key, value in self.positions.items() if value.is_open))
        if not open_keys or self.equity > self.maintenance_margin:
            return None
        return LiquidationDecision(
            timestamp=timestamp.astimezone(UTC),
            equity=self.equity,
            maintenance_margin=self.maintenance_margin,
            instruments=open_keys,
        )

    def capture(self, timestamp: datetime) -> PortfolioSnapshot:
        snapshot = PortfolioSnapshot(
            timestamp=timestamp.astimezone(UTC),
            cash=self.cash,
            equity=self.equity,
            unrealized_pnl=self.unrealized_pnl,
            gross_exposure=self.gross_exposure,
            net_exposure=self.net_exposure,
            margin_used=self.margin_used,
            maintenance_margin=self.maintenance_margin,
            available_margin=self.available_margin,
        )
        self.snapshots.append(snapshot)
        return snapshot
