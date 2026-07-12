"""Conservative top-of-book execution model for event-driven research."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from app.domain.execution.models import (
    BPS,
    ZERO,
    Fill,
    InstrumentRules,
    LiquidityRole,
    MarketSnapshot,
    Order,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from app.domain.market_data.models import Side

InstrumentKey = tuple[str, str]
QuantityCap = Callable[[Order], Decimal]
FillConsumer = Callable[[Order, Fill], None]


@dataclass(frozen=True, slots=True)
class ExecutionModelConfig:
    participation_rate: Decimal = Decimal("0.25")
    passive_fill_fraction: Decimal = Decimal("0.10")
    slippage_bps: Decimal = Decimal("1.0")
    impact_coefficient_bps: Decimal = Decimal("5.0")
    impact_power: int = 1
    maximum_impact_bps: Decimal = Decimal("50")

    def __post_init__(self) -> None:
        if not ZERO < self.participation_rate <= Decimal("1"):
            raise ValueError("participation_rate must be in (0, 1]")
        if not ZERO <= self.passive_fill_fraction <= Decimal("1"):
            raise ValueError("passive_fill_fraction must be in [0, 1]")
        if min(self.slippage_bps, self.impact_coefficient_bps, self.maximum_impact_bps) < ZERO:
            raise ValueError("cost assumptions cannot be negative")
        if self.impact_power < 1:
            raise ValueError("impact_power must be at least one")


class ExecutionSimulator:
    """Matches submitted orders only against strictly later market snapshots."""

    def __init__(
        self,
        rules: Mapping[InstrumentKey, InstrumentRules],
        config: ExecutionModelConfig | None = None,
    ) -> None:
        self.rules = dict(rules)
        self.config = config or ExecutionModelConfig()
        self.orders: dict[str, Order] = {}
        self.fills: list[Fill] = []
        self._fill_sequence = 0

    def submit(self, order: Order) -> Order:
        if order.order_id in self.orders:
            raise ValueError(f"duplicate order_id: {order.order_id}")
        self.orders[order.order_id] = order
        rules = self.rules.get((order.exchange, order.symbol))
        if rules is None:
            order.reject("instrument rules unavailable")
            return order
        quantity = rules.quantize_quantity(order.quantity)
        if quantity <= ZERO:
            order.reject("quantity is below lot size")
            return order
        order.quantity = quantity
        order.remaining_quantity = quantity
        if order.limit_price is not None:
            order.limit_price = rules.quantize_limit_price(order.limit_price, order.side)
        if order.post_only and order.order_type is not OrderType.LIMIT:
            order.reject("post-only requires a limit order")
            return order
        order.status = OrderStatus.SUBMITTED
        return order

    def cancel_all(self, reason: str = "cancel_all") -> None:
        for order in self.orders.values():
            order.cancel(reason)

    def match(
        self,
        snapshot: MarketSnapshot,
        quantity_cap: QuantityCap | None = None,
        on_fill: FillConsumer | None = None,
    ) -> list[Fill]:
        rules = self.rules.get((snapshot.exchange, snapshot.symbol))
        if rules is None:
            return []
        produced: list[Fill] = []
        candidates = sorted(
            self.orders.values(), key=lambda item: (item.submitted_at, item.order_id)
        )
        for order in candidates:
            if (
                order.status.terminal
                or order.exchange != snapshot.exchange
                or order.symbol != snapshot.symbol
            ):
                continue
            # Strict inequality is the core anti-look-ahead rule: no same-event close fill.
            if snapshot.timestamp <= order.submitted_at:
                continue
            if order.expires_at is not None and snapshot.timestamp >= order.expires_at:
                order.cancel("order expired", expired=True)
                continue
            cap = order.remaining_quantity
            if quantity_cap is not None:
                cap = min(cap, max(ZERO, quantity_cap(order)))
            if cap <= ZERO:
                if order.reduce_only:
                    order.cancel("reduce-only order no longer reduces a position")
                continue
            if order.filled_quantity == ZERO and not rules.validate_notional(
                order.quantity, snapshot.mid
            ):
                order.reject("order is below minimum notional")
                continue
            match = self._match_details(order, snapshot, rules, cap)
            if match is None:
                if order.time_in_force in {TimeInForce.IOC, TimeInForce.FOK}:
                    order.cancel("time in force could not be satisfied")
                continue
            quantity, price, liquidity, spread, slippage, impact = match
            if order.time_in_force is TimeInForce.FOK and quantity < order.remaining_quantity:
                order.cancel("fill-or-kill quantity unavailable")
                continue
            fill = self._make_fill(
                order,
                snapshot,
                rules,
                quantity,
                price,
                liquidity,
                spread,
                slippage,
                impact,
            )
            order.record_fill(quantity)
            self.fills.append(fill)
            produced.append(fill)
            if on_fill is not None:
                on_fill(order, fill)
            if order.time_in_force is TimeInForce.IOC and order.remaining_quantity > ZERO:
                order.cancel("IOC remainder cancelled")
        return produced

    def _match_details(
        self,
        order: Order,
        snapshot: MarketSnapshot,
        rules: InstrumentRules,
        cap: Decimal,
    ) -> tuple[Decimal, Decimal, LiquidityRole, Decimal, Decimal, Decimal] | None:
        marketable = self._is_marketable(order, snapshot)
        if order.post_only and marketable:
            order.cancel("post-only order would take liquidity")
            return None
        if order.order_type is OrderType.MARKET or marketable:
            liquidity = LiquidityRole.TAKER
            depth = snapshot.ask_quantity if order.side is Side.BUY else snapshot.bid_quantity
            available = rules.quantize_quantity(depth * self.config.participation_rate)
            quantity = rules.quantize_quantity(min(cap, available))
            if quantity <= ZERO:
                return None
            top = snapshot.ask if order.side is Side.BUY else snapshot.bid
            impact_bps = self._impact_bps(quantity, depth)
            slippage_rate = self.config.slippage_bps / BPS
            impact_rate = impact_bps / BPS
            direction = Decimal("1") if order.side is Side.BUY else Decimal("-1")
            raw_price = top * (Decimal("1") + direction * (slippage_rate + impact_rate))
            price = rules.quantize_execution_price(raw_price, order.side)
            if order.limit_price is not None and (
                (order.side is Side.BUY and price > order.limit_price)
                or (order.side is Side.SELL and price < order.limit_price)
            ):
                return None
            spread = abs(top - snapshot.mid) * quantity
            slippage = top * slippage_rate * quantity
            impact = top * impact_rate * quantity
            return quantity, price, liquidity, spread, slippage, impact

        if not self._passive_trade_reached(order, snapshot):
            return None
        trade_quantity = snapshot.trade_quantity or ZERO
        available = rules.quantize_quantity(trade_quantity * self.config.passive_fill_fraction)
        quantity = rules.quantize_quantity(min(cap, available))
        if quantity <= ZERO or order.limit_price is None:
            return None
        return quantity, order.limit_price, LiquidityRole.MAKER, ZERO, ZERO, ZERO

    @staticmethod
    def _is_marketable(order: Order, snapshot: MarketSnapshot) -> bool:
        if order.order_type is OrderType.MARKET:
            return True
        if order.limit_price is None:
            return False
        if order.side is Side.BUY:
            return order.limit_price >= snapshot.ask
        return order.limit_price <= snapshot.bid

    @staticmethod
    def _passive_trade_reached(order: Order, snapshot: MarketSnapshot) -> bool:
        if order.limit_price is None or snapshot.last_price is None:
            return False
        if order.side is Side.BUY:
            return snapshot.last_price <= order.limit_price
        return snapshot.last_price >= order.limit_price

    def _impact_bps(self, quantity: Decimal, depth: Decimal) -> Decimal:
        if depth <= ZERO:
            return self.config.maximum_impact_bps
        ratio = quantity / depth
        impact = self.config.impact_coefficient_bps * (ratio**self.config.impact_power)
        return min(impact, self.config.maximum_impact_bps)

    def _make_fill(
        self,
        order: Order,
        snapshot: MarketSnapshot,
        rules: InstrumentRules,
        quantity: Decimal,
        price: Decimal,
        liquidity: LiquidityRole,
        spread: Decimal,
        slippage: Decimal,
        impact: Decimal,
    ) -> Fill:
        self._fill_sequence += 1
        fee_rate = (
            rules.maker_fee_rate if liquidity is LiquidityRole.MAKER else rules.taker_fee_rate
        )
        return Fill(
            fill_id=f"fill-{self._fill_sequence:012d}",
            order_id=order.order_id,
            signal_id=order.signal_id,
            exchange=order.exchange,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            liquidity=liquidity,
            signal_at=order.signal_at,
            order_created_at=order.created_at,
            submitted_at=order.submitted_at,
            filled_at=snapshot.timestamp,
            fee=quantity * price * fee_rate,
            spread_cost=spread,
            slippage_cost=slippage,
            market_impact_cost=impact,
        )
