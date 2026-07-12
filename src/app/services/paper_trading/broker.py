from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from app.domain.market_data.models import Side
from app.services.paper_trading.models import (
    PaperAccountSnapshot,
    PaperAuditEvent,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderStatus,
    PaperOrderType,
    PaperPosition,
    PaperQuote,
)


class PaperBroker:
    """Deterministic paper broker. Orders only fill on a later quote."""

    def __init__(
        self,
        initial_cash: Decimal,
        fee_rate: Decimal = Decimal("0.0006"),
        slippage_bps: Decimal = Decimal("2"),
        max_participation: Decimal = Decimal("0.10"),
        max_gross_leverage: Decimal = Decimal("1"),
        minimum_data_quality: float = 0.8,
    ) -> None:
        if initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        if not Decimal("0") <= max_participation <= Decimal("1"):
            raise ValueError("max_participation must be in [0, 1]")
        if max_gross_leverage <= 0 or max_gross_leverage > 1:
            raise ValueError("paper leverage must be in (0, 1]")
        if fee_rate < 0 or slippage_bps < 0:
            raise ValueError("paper execution costs cannot be negative")
        if not 0 <= minimum_data_quality <= 1:
            raise ValueError("minimum_data_quality must be in [0, 1]")
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.fee_rate = fee_rate
        self.slippage_bps = slippage_bps
        self.max_participation = max_participation
        self.max_gross_leverage = max_gross_leverage
        self.minimum_data_quality = minimum_data_quality
        self.orders: dict[str, PaperOrder] = {}
        self.positions: dict[str, PaperPosition] = {}
        self.fills: list[PaperFill] = []
        self.latest_quotes: dict[str, PaperQuote] = {}
        self.fees_paid = Decimal("0")
        self.kill_switch_reason: str | None = None
        self.operational_halt_reason: str | None = None
        self._last_sequence: dict[str, int] = {}
        self.audit_events: list[PaperAuditEvent] = []
        self.data_halted_symbols: set[str] = set()

    def submit(self, request: PaperOrderRequest, submitted_at: datetime) -> PaperOrder:
        submitted_at = self._utc(submitted_at)
        if request.client_order_id in self.orders:
            raise ValueError("duplicate client_order_id")
        order = PaperOrder(request=request, submitted_at=submitted_at)
        if self.kill_switch_reason or self.operational_halt_reason:
            order.status = PaperOrderStatus.REJECTED
            order.rejection_reason = (
                f"kill switch: {self.kill_switch_reason}"
                if self.kill_switch_reason
                else f"operational halt: {self.operational_halt_reason}"
            )
        self.orders[request.client_order_id] = order
        self._audit(
            submitted_at,
            "order_rejected" if order.status == PaperOrderStatus.REJECTED else "order_submitted",
            request.client_order_id,
            {"symbol": request.symbol, "side": request.side, "quantity": request.quantity},
        )
        return order

    def cancel(self, client_order_id: str, timestamp: datetime | None = None) -> PaperOrder:
        order = self.orders[client_order_id]
        if order.status in {PaperOrderStatus.OPEN, PaperOrderStatus.PARTIALLY_FILLED}:
            order.status = PaperOrderStatus.CANCELED
            self._audit(
                self._utc(timestamp or datetime.now(UTC)),
                "order_canceled",
                client_order_id,
                {"remaining_quantity": order.remaining_quantity},
            )
        return order

    def cancel_all(self) -> None:
        for order_id in tuple(self.orders):
            self.cancel(order_id)

    def activate_kill_switch(self, reason: str, timestamp: datetime | None = None) -> None:
        self.kill_switch_reason = reason
        self.cancel_all()
        self._audit(
            self._utc(timestamp or datetime.now(UTC)),
            "kill_switch",
            None,
            {"reason": reason},
        )

    def clear_data_halt(self, symbol: str, reason: str, timestamp: datetime) -> None:
        self.data_halted_symbols.discard(symbol)
        self._last_sequence.pop(symbol, None)
        self._audit(self._utc(timestamp), "data_halt_cleared", symbol, {"reason": reason})

    def halt(self, reason: str, timestamp: datetime) -> None:
        self.operational_halt_reason = reason
        self._audit(self._utc(timestamp), "operational_halt", None, {"reason": reason})

    def resume(self, reason: str, timestamp: datetime) -> None:
        self.operational_halt_reason = None
        self._audit(self._utc(timestamp), "operational_resume", None, {"reason": reason})

    def on_quote(self, quote: PaperQuote) -> tuple[PaperFill, ...]:
        previous_sequence = self._last_sequence.get(quote.symbol)
        if (
            quote.sequence is not None
            and previous_sequence is not None
            and quote.sequence <= previous_sequence
        ):
            return ()
        if (
            quote.sequence is not None
            and previous_sequence is not None
            and quote.sequence > previous_sequence + 1
        ):
            self.data_halted_symbols.add(quote.symbol)
            self._audit(
                quote.timestamp,
                "data_sequence_gap",
                quote.symbol,
                {"previous": previous_sequence, "current": quote.sequence},
            )
            return ()
        if quote.sequence is not None:
            self._last_sequence[quote.symbol] = quote.sequence
        self.latest_quotes[quote.symbol] = quote
        if (
            quote.data_quality_score < self.minimum_data_quality
            or self.kill_switch_reason
            or self.operational_halt_reason
            or quote.symbol in self.data_halted_symbols
        ):
            return ()
        generated: list[PaperFill] = []
        for order in self.orders.values():
            if order.request.symbol != quote.symbol or order.submitted_at >= quote.timestamp:
                continue
            if order.status not in {PaperOrderStatus.OPEN, PaperOrderStatus.PARTIALLY_FILLED}:
                continue
            fill = self._try_fill(order, quote)
            if fill is not None:
                generated.append(fill)
        return tuple(generated)

    def _try_fill(self, order: PaperOrder, quote: PaperQuote) -> PaperFill | None:
        request = order.request
        side_liquidity = quote.ask_size if request.side == Side.BUY else quote.bid_size
        available = side_liquidity * self.max_participation
        quantity = min(order.remaining_quantity, available)
        if quantity <= 0:
            return None
        touch = quote.ask if request.side == Side.BUY else quote.bid
        if request.order_type == PaperOrderType.LIMIT:
            limit_price = request.limit_price
            if limit_price is None:
                order.status = PaperOrderStatus.REJECTED
                order.rejection_reason = "limit price missing after validation"
                return None
            crosses = touch <= limit_price if request.side == Side.BUY else touch >= limit_price
            if not crosses:
                return None
            price = touch
            slippage_cost = Decimal("0")
        else:
            slip = self.slippage_bps / Decimal("10000")
            price = touch * (
                Decimal("1") + slip if request.side == Side.BUY else Decimal("1") - slip
            )
            slippage_cost = abs(price - touch) * quantity
        if not self._exposure_allowed(request.symbol, request.side, quantity, price):
            order.status = PaperOrderStatus.REJECTED
            order.rejection_reason = "maximum gross exposure exceeded"
            self._audit(
                quote.timestamp,
                "order_rejected",
                request.client_order_id,
                {"reason": order.rejection_reason},
            )
            return None
        notional = price * quantity
        fee = notional * self.fee_rate
        fill = PaperFill(
            fill_id=str(uuid4()),
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            timestamp=quote.timestamp,
            quantity=quantity,
            price=price,
            fee=fee,
            slippage_cost=slippage_cost,
        )
        self._apply_fill(fill)
        order.filled_quantity += quantity
        order.status = (
            PaperOrderStatus.FILLED
            if order.remaining_quantity == 0
            else PaperOrderStatus.PARTIALLY_FILLED
        )
        self.fills.append(fill)
        self._audit(
            fill.timestamp,
            "fill",
            fill.fill_id,
            {
                "order_id": fill.client_order_id,
                "symbol": fill.symbol,
                "quantity": fill.quantity,
                "price": fill.price,
                "fee": fill.fee,
                "slippage_cost": fill.slippage_cost,
            },
        )
        return fill

    def _exposure_allowed(self, symbol: str, side: Side, quantity: Decimal, price: Decimal) -> bool:
        signed = quantity if side == Side.BUY else -quantity
        candidate: dict[str, Decimal] = {
            key: position.quantity for key, position in self.positions.items()
        }
        candidate[symbol] = candidate.get(symbol, Decimal("0")) + signed
        gross = Decimal("0")
        for key, position_quantity in candidate.items():
            quote = self.latest_quotes.get(key)
            mark = (quote.bid + quote.ask) / 2 if quote else price
            gross += abs(position_quantity * mark)
        return gross <= self.equity() * self.max_gross_leverage

    def _apply_fill(self, fill: PaperFill) -> None:
        position = self.positions.setdefault(fill.symbol, PaperPosition(symbol=fill.symbol))
        signed = fill.quantity if fill.side == Side.BUY else -fill.quantity
        old_quantity = position.quantity
        new_quantity = old_quantity + signed
        if old_quantity == 0 or old_quantity * signed > 0:
            total_cost = position.average_entry * abs(old_quantity) + fill.price * abs(signed)
            position.average_entry = total_cost / abs(new_quantity)
        else:
            closed = min(abs(old_quantity), abs(signed))
            direction = Decimal("1") if old_quantity > 0 else Decimal("-1")
            realized = (fill.price - position.average_entry) * closed * direction
            position.realized_pnl += realized
            self.cash += realized
            if new_quantity == 0:
                position.average_entry = Decimal("0")
            elif old_quantity * new_quantity < 0:
                position.average_entry = fill.price
        position.quantity = new_quantity
        self.cash -= fill.fee
        self.fees_paid += fill.fee

    def apply_funding(self, symbol: str, rate: Decimal, timestamp: datetime) -> Decimal:
        position = self.positions.get(symbol)
        quote = self.latest_quotes.get(symbol)
        if position is None or quote is None:
            return Decimal("0")
        mark = (quote.bid + quote.ask) / 2
        payment = -(position.quantity * mark * rate)
        position.funding_pnl += payment
        self.cash += payment
        self._audit(
            self._utc(timestamp),
            "funding",
            symbol,
            {"rate": rate, "payment": payment},
        )
        return payment

    def equity(self) -> Decimal:
        unrealized = Decimal("0")
        for symbol, position in self.positions.items():
            quote = self.latest_quotes.get(symbol)
            if quote is not None:
                mark = (quote.bid + quote.ask) / 2
                unrealized += (mark - position.average_entry) * position.quantity
        return self.cash + unrealized

    def snapshot(self, timestamp: datetime) -> PaperAccountSnapshot:
        gross = Decimal("0")
        for symbol, position in self.positions.items():
            quote = self.latest_quotes.get(symbol)
            if quote is not None:
                gross += abs(position.quantity * ((quote.bid + quote.ask) / 2))
        return PaperAccountSnapshot(
            timestamp=self._utc(timestamp),
            cash=self.cash,
            equity=self.equity(),
            gross_exposure=gross,
            fees_paid=self.fees_paid,
            funding_pnl=sum(
                (position.funding_pnl for position in self.positions.values()), Decimal("0")
            ),
            open_orders=sum(
                order.status in {PaperOrderStatus.OPEN, PaperOrderStatus.PARTIALLY_FILLED}
                for order in self.orders.values()
            ),
            positions={symbol: position.quantity for symbol, position in self.positions.items()},
        )

    def _audit(
        self,
        timestamp: datetime,
        event_type: str,
        entity_id: str | None,
        payload: dict[str, object],
    ) -> None:
        self.audit_events.append(
            PaperAuditEvent(
                event_id=str(uuid4()),
                timestamp=self._utc(timestamp),
                event_type=event_type,
                entity_id=entity_id,
                payload={key: str(value) for key, value in payload.items()},
            )
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("paper timestamp must be timezone-aware")
        return value.astimezone(UTC)
