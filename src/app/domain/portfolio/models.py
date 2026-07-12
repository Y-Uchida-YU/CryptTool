"""Portfolio state models for linear, quote-margined perpetual simulation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.domain.execution.models import ZERO, Fill, MarketSnapshot
from app.domain.market_data.models import Side


@dataclass(slots=True)
class Position:
    exchange: str
    symbol: str
    quantity: Decimal = ZERO  # positive long, negative short
    average_entry_price: Decimal = ZERO
    mark_price: Decimal = ZERO
    realized_pnl: Decimal = ZERO
    funding_pnl: Decimal = ZERO
    fees: Decimal = ZERO
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    opened_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_open(self) -> bool:
        return self.quantity != ZERO

    @property
    def unrealized_pnl(self) -> Decimal:
        if not self.is_open or self.mark_price <= ZERO:
            return ZERO
        return self.quantity * (self.mark_price - self.average_entry_price)

    @property
    def notional(self) -> Decimal:
        if not self.is_open or self.mark_price <= ZERO:
            return ZERO
        return abs(self.quantity) * self.mark_price

    def apply_fill(self, fill: Fill) -> Decimal:
        delta = fill.quantity if fill.side is Side.BUY else -fill.quantity
        previous = self.quantity
        realized = ZERO
        if previous == ZERO:
            self.quantity = delta
            self.average_entry_price = fill.price
            self.opened_at = fill.filled_at
        elif previous * delta > ZERO:
            total = abs(previous) + abs(delta)
            self.average_entry_price = (
                abs(previous) * self.average_entry_price + abs(delta) * fill.price
            ) / total
            self.quantity = previous + delta
        else:
            closing_quantity = min(abs(previous), abs(delta))
            direction = Decimal("1") if previous > ZERO else Decimal("-1")
            realized = closing_quantity * (fill.price - self.average_entry_price) * direction
            self.quantity = previous + delta
            if self.quantity == ZERO:
                self.average_entry_price = ZERO
                self.stop_loss = None
                self.take_profit = None
                self.opened_at = None
            elif previous * self.quantity < ZERO:
                self.average_entry_price = fill.price
                self.opened_at = fill.filled_at
        self.mark_price = fill.price
        self.realized_pnl += realized
        self.fees += fill.fee
        self.updated_at = fill.filled_at
        return realized

    def set_protection(self, stop_loss: Decimal | None, take_profit: Decimal | None) -> None:
        if not self.is_open:
            return
        reference = self.average_entry_price
        if stop_loss is not None:
            valid_stop = stop_loss < reference if self.quantity > ZERO else stop_loss > reference
            if not valid_stop:
                raise ValueError("stop loss is on the non-protective side of entry")
        if take_profit is not None:
            valid_take = (
                take_profit > reference if self.quantity > ZERO else take_profit < reference
            )
            if not valid_take:
                raise ValueError("take profit is on the non-profitable side of entry")
        self.stop_loss = stop_loss
        self.take_profit = take_profit

    def protection_trigger(self, snapshot: MarketSnapshot) -> str | None:
        if not self.is_open:
            return None
        executable = snapshot.bid if self.quantity > ZERO else snapshot.ask
        if self.stop_loss is not None and (
            (self.quantity > ZERO and executable <= self.stop_loss)
            or (self.quantity < ZERO and executable >= self.stop_loss)
        ):
            return "stop_loss"
        if self.take_profit is not None and (
            (self.quantity > ZERO and executable >= self.take_profit)
            or (self.quantity < ZERO and executable <= self.take_profit)
        ):
            return "take_profit"
        return None


@dataclass(frozen=True, slots=True)
class FundingRecord:
    exchange: str
    symbol: str
    timestamp: datetime
    rate: Decimal
    mark_price: Decimal
    position_quantity: Decimal
    amount: Decimal


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    timestamp: datetime
    cash: Decimal
    equity: Decimal
    unrealized_pnl: Decimal
    gross_exposure: Decimal
    net_exposure: Decimal
    margin_used: Decimal
    maintenance_margin: Decimal
    available_margin: Decimal


@dataclass(frozen=True, slots=True)
class LiquidationDecision:
    timestamp: datetime
    equity: Decimal
    maintenance_margin: Decimal
    instruments: tuple[tuple[str, str], ...]
