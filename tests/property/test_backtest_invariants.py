from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.domain.execution.models import (
    Fill,
    InstrumentRules,
    LiquidityRole,
    MarketSnapshot,
    Order,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from app.domain.execution.simulator import ExecutionModelConfig, ExecutionSimulator
from app.domain.market_data.models import Side
from app.domain.portfolio.ledger import DuplicateFillError, PortfolioLedger

NOW = datetime(2024, 1, 1, tzinfo=UTC)
KEY = ("test", "BTC-PERP")
RULES = {
    KEY: InstrumentRules(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        minimum_notional=Decimal("0"),
        maker_fee_rate=Decimal("0.0002"),
        taker_fee_rate=Decimal("0.0006"),
    )
}


def order(quantity: Decimal) -> Order:
    return Order(
        order_id="order",
        signal_id="signal",
        exchange=KEY[0],
        symbol=KEY[1],
        side=Side.BUY,
        quantity=quantity,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.GTC,
        signal_at=NOW,
        created_at=NOW,
        submitted_at=NOW,
    )


def fill(
    fill_id: str,
    side: Side,
    quantity: Decimal,
    price: Decimal,
    seconds: int,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id=f"order-{fill_id}",
        signal_id=f"signal-{fill_id}",
        exchange=KEY[0],
        symbol=KEY[1],
        side=side,
        quantity=quantity,
        price=price,
        liquidity=LiquidityRole.TAKER,
        signal_at=NOW,
        order_created_at=NOW,
        submitted_at=NOW,
        filled_at=NOW + timedelta(seconds=seconds),
        fee=Decimal("0"),
    )


@settings(max_examples=50, deadline=None)
@given(
    requested=st.decimals(min_value="0.001", max_value="10", places=3),
    depth=st.decimals(min_value="0", max_value="10", places=3),
)
def test_fills_never_exceed_order_or_available_participation(
    requested: Decimal, depth: Decimal
) -> None:
    simulator = ExecutionSimulator(
        RULES,
        ExecutionModelConfig(participation_rate=Decimal("0.25")),
    )
    submitted = simulator.submit(order(requested))
    snapshot = MarketSnapshot(
        exchange=KEY[0],
        symbol=KEY[1],
        timestamp=NOW + timedelta(seconds=1),
        bid=Decimal("99.99"),
        ask=Decimal("100.01"),
        bid_quantity=depth,
        ask_quantity=depth,
    )
    fills = simulator.match(snapshot)
    total = sum((item.quantity for item in fills), Decimal("0"))
    assert total <= submitted.quantity
    assert total <= depth * Decimal("0.25")
    assert submitted.remaining_quantity + total == submitted.quantity
    assert all(item.fee >= 0 for item in fills)


@settings(max_examples=40, deadline=None)
@given(
    quantity=st.decimals(min_value="0.001", max_value="5", places=3),
    entry=st.decimals(min_value="10", max_value="1000", places=2),
    change=st.decimals(min_value="-9", max_value="100", places=2),
)
def test_round_trip_cash_equals_realized_pnl_without_costs(
    quantity: Decimal, entry: Decimal, change: Decimal
) -> None:
    exit_price = entry + change
    ledger = PortfolioLedger(Decimal("10000"), RULES)
    ledger.apply_fill(fill("entry", Side.BUY, quantity, entry, 1))
    ledger.apply_fill(fill("exit", Side.SELL, quantity, exit_price, 2))
    assert ledger.position(*KEY).quantity == 0
    assert ledger.cash == Decimal("10000") + quantity * change


@settings(max_examples=30, deadline=None)
@given(quantity=st.decimals(min_value="0.001", max_value="5", places=3))
def test_duplicate_fill_is_rejected_without_balance_or_position_change(quantity: Decimal) -> None:
    ledger = PortfolioLedger(Decimal("10000"), RULES)
    event = fill("same", Side.BUY, quantity, Decimal("100"), 1)
    ledger.apply_fill(event)
    cash = ledger.cash
    position_quantity = ledger.position(*KEY).quantity
    with pytest.raises(DuplicateFillError):
        ledger.apply_fill(event)
    assert ledger.cash == cash
    assert ledger.position(*KEY).quantity == position_quantity
    assert len(ledger.fills) == 1


@settings(max_examples=30, deadline=None)
@given(delay_ms=st.integers(min_value=0, max_value=10_000))
def test_no_fill_can_reference_same_or_future_submission(delay_ms: int) -> None:
    submitted_at = NOW + timedelta(milliseconds=delay_ms)
    candidate = order(Decimal("1"))
    candidate.submitted_at = submitted_at
    simulator = ExecutionSimulator(RULES)
    simulator.submit(candidate)
    same_time = MarketSnapshot(
        exchange=KEY[0],
        symbol=KEY[1],
        timestamp=submitted_at,
        bid=Decimal("99.99"),
        ask=Decimal("100.01"),
        bid_quantity=Decimal("10"),
        ask_quantity=Decimal("10"),
    )
    assert simulator.match(same_time) == []
    assert candidate.status is OrderStatus.SUBMITTED
    later = MarketSnapshot(
        exchange=KEY[0],
        symbol=KEY[1],
        timestamp=submitted_at + timedelta(microseconds=1),
        bid=Decimal("99.99"),
        ask=Decimal("100.01"),
        bid_quantity=Decimal("10"),
        ask_quantity=Decimal("10"),
    )
    matched = simulator.match(later)
    assert all(item.submitted_at < item.filled_at for item in matched)
