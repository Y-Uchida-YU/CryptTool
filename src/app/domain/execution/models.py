"""Exchange-neutral order and fill contracts used by research execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from enum import StrEnum

from app.domain.market_data.models import Side

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class TimeInForce(StrEnum):
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


class OrderStatus(StrEnum):
    CREATED = "created"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"

    @property
    def terminal(self) -> bool:
        return self in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }


class LiquidityRole(StrEnum):
    MAKER = "maker"
    TAKER = "taker"


@dataclass(frozen=True, slots=True)
class InstrumentRules:
    """Trading constraints and fee schedule captured with a backtest run."""

    tick_size: Decimal
    lot_size: Decimal
    minimum_notional: Decimal
    maker_fee_rate: Decimal = Decimal("0.0002")
    taker_fee_rate: Decimal = Decimal("0.0006")
    maintenance_margin_rate: Decimal = Decimal("0.005")

    def __post_init__(self) -> None:
        if self.tick_size <= ZERO or self.lot_size <= ZERO:
            raise ValueError("tick_size and lot_size must be positive")
        if self.minimum_notional < ZERO:
            raise ValueError("minimum_notional cannot be negative")
        if self.maker_fee_rate < ZERO or self.taker_fee_rate < ZERO:
            raise ValueError("fee rates cannot be negative")
        if not ZERO <= self.maintenance_margin_rate < ONE:
            raise ValueError("maintenance_margin_rate must be in [0, 1)")

    def quantize_quantity(self, quantity: Decimal) -> Decimal:
        if quantity <= ZERO:
            return ZERO
        return (quantity / self.lot_size).to_integral_value(rounding=ROUND_DOWN) * self.lot_size

    def quantize_price(self, price: Decimal) -> Decimal:
        if price <= ZERO:
            raise ValueError("price must be positive")
        return (price / self.tick_size).to_integral_value(rounding=ROUND_DOWN) * self.tick_size

    def quantize_limit_price(self, price: Decimal, side: Side) -> Decimal:
        """Round away from the spread so normalization cannot improve fill probability."""
        if price <= ZERO:
            raise ValueError("price must be positive")
        rounding = ROUND_DOWN if side is Side.BUY else ROUND_UP
        return (price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size

    def quantize_execution_price(self, price: Decimal, side: Side) -> Decimal:
        """Round fills against the simulated trader, avoiding optimistic prices."""
        if price <= ZERO:
            raise ValueError("price must be positive")
        rounding = ROUND_UP if side is Side.BUY else ROUND_DOWN
        return (price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size

    def validate_notional(self, quantity: Decimal, price: Decimal) -> bool:
        return quantity * price >= self.minimum_notional


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    exchange: str
    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    bid_quantity: Decimal
    ask_quantity: Decimal
    last_price: Decimal | None = None
    trade_quantity: Decimal | None = None
    mark_price: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp, "timestamp"))
        if self.bid <= ZERO or self.ask <= ZERO or self.bid >= self.ask:
            raise ValueError("snapshot requires positive, non-crossed bid/ask")
        if self.bid_quantity < ZERO or self.ask_quantity < ZERO:
            raise ValueError("book quantities cannot be negative")
        for value in (self.last_price, self.mark_price):
            if value is not None and value <= ZERO:
                raise ValueError("last and mark prices must be positive")
        if self.trade_quantity is not None and self.trade_quantity < ZERO:
            raise ValueError("trade_quantity cannot be negative")

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def valuation_price(self) -> Decimal:
        return self.mark_price or self.last_price or self.mid


@dataclass(slots=True)
class Order:
    order_id: str
    signal_id: str
    exchange: str
    symbol: str
    side: Side
    quantity: Decimal
    order_type: OrderType
    time_in_force: TimeInForce
    signal_at: datetime
    created_at: datetime
    submitted_at: datetime
    limit_price: Decimal | None = None
    post_only: bool = False
    reduce_only: bool = False
    expires_at: datetime | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    status: OrderStatus = OrderStatus.CREATED
    remaining_quantity: Decimal = field(init=False)
    rejection_reason: str | None = None

    def __post_init__(self) -> None:
        self.signal_at = _utc(self.signal_at, "signal_at")
        self.created_at = _utc(self.created_at, "created_at")
        self.submitted_at = _utc(self.submitted_at, "submitted_at")
        if not self.signal_at <= self.created_at <= self.submitted_at:
            raise ValueError("order timestamps must be signal <= created <= submitted")
        if self.quantity <= ZERO:
            raise ValueError("order quantity must be positive")
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit order requires limit_price")
        if self.order_type is OrderType.MARKET and self.limit_price is not None:
            raise ValueError("market order cannot have limit_price")
        for value in (self.limit_price, self.stop_loss, self.take_profit):
            if value is not None and value <= ZERO:
                raise ValueError("order prices must be positive")
        if self.expires_at is not None:
            self.expires_at = _utc(self.expires_at, "expires_at")
            if self.expires_at <= self.submitted_at:
                raise ValueError("expires_at must follow submitted_at")
        self.remaining_quantity = self.quantity

    @property
    def filled_quantity(self) -> Decimal:
        return self.quantity - self.remaining_quantity

    def reject(self, reason: str) -> None:
        if self.status.terminal:
            return
        self.status = OrderStatus.REJECTED
        self.rejection_reason = reason

    def cancel(self, reason: str, *, expired: bool = False) -> None:
        if self.status.terminal:
            return
        self.status = OrderStatus.EXPIRED if expired else OrderStatus.CANCELLED
        self.rejection_reason = reason

    def record_fill(self, quantity: Decimal) -> None:
        if self.status.terminal:
            raise ValueError("cannot fill a terminal order")
        if quantity <= ZERO or quantity > self.remaining_quantity:
            raise ValueError("invalid fill quantity")
        self.remaining_quantity -= quantity
        self.status = (
            OrderStatus.FILLED if self.remaining_quantity == ZERO else OrderStatus.PARTIALLY_FILLED
        )


@dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    order_id: str
    signal_id: str
    exchange: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    liquidity: LiquidityRole
    signal_at: datetime
    order_created_at: datetime
    submitted_at: datetime
    filled_at: datetime
    fee: Decimal
    spread_cost: Decimal = ZERO
    slippage_cost: Decimal = ZERO
    market_impact_cost: Decimal = ZERO

    def __post_init__(self) -> None:
        for name in ("signal_at", "order_created_at", "submitted_at", "filled_at"):
            object.__setattr__(self, name, _utc(getattr(self, name), name))
        if not self.signal_at <= self.order_created_at <= self.submitted_at < self.filled_at:
            raise ValueError("fill timestamps must preserve causality and use a later market event")
        if self.quantity <= ZERO or self.price <= ZERO:
            raise ValueError("fill quantity and price must be positive")
        if min(self.fee, self.spread_cost, self.slippage_cost, self.market_impact_cost) < ZERO:
            raise ValueError("execution costs cannot be negative")

    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price
