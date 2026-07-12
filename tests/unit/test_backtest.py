from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

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
from app.domain.portfolio.ledger import PortfolioLedger
from app.services.backtest.engine import BacktestEngine
from app.services.backtest.events import (
    EventQueue,
    FundingEvent,
    MarketEvent,
    OrderCreationEvent,
    OrderSubmissionEvent,
    SignalEvent,
)

NOW = datetime(2024, 1, 1, tzinfo=UTC)
KEY = ("test", "BTC-PERP")


def rules(
    *,
    minimum_notional: str = "5",
    maker_fee: str = "0.0002",
    taker_fee: str = "0.0006",
) -> dict[tuple[str, str], InstrumentRules]:
    return {
        KEY: InstrumentRules(
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.1"),
            minimum_notional=Decimal(minimum_notional),
            maker_fee_rate=Decimal(maker_fee),
            taker_fee_rate=Decimal(taker_fee),
            maintenance_margin_rate=Decimal("0.005"),
        )
    }


def market(
    seconds: int,
    *,
    bid: str = "99.9",
    ask: str = "100.1",
    depth: str = "10",
    last: str | None = "100",
    traded: str | None = "10",
    mark: str | None = None,
) -> MarketEvent:
    return MarketEvent(
        MarketSnapshot(
            exchange=KEY[0],
            symbol=KEY[1],
            timestamp=NOW + timedelta(seconds=seconds),
            bid=Decimal(bid),
            ask=Decimal(ask),
            bid_quantity=Decimal(depth),
            ask_quantity=Decimal(depth),
            last_price=Decimal(last) if last is not None else None,
            trade_quantity=Decimal(traded) if traded is not None else None,
            mark_price=Decimal(mark) if mark is not None else None,
        )
    )


def signal(
    signal_id: str = "entry",
    *,
    timestamp: datetime = NOW,
    side: Side = Side.BUY,
    quantity: str = "1",
    **kwargs: object,
) -> SignalEvent:
    return SignalEvent(
        timestamp=timestamp,
        signal_id=signal_id,
        exchange=KEY[0],
        symbol=KEY[1],
        side=side,
        quantity=Decimal(quantity),
        **kwargs,
    )


def raw_order(
    order_id: str = "order",
    *,
    side: Side = Side.BUY,
    quantity: str = "1",
    order_type: OrderType = OrderType.MARKET,
    time_in_force: TimeInForce = TimeInForce.GTC,
    submitted_at: datetime = NOW,
    **kwargs: object,
) -> Order:
    return Order(
        order_id=order_id,
        signal_id=f"signal-{order_id}",
        exchange=KEY[0],
        symbol=KEY[1],
        side=side,
        quantity=Decimal(quantity),
        order_type=order_type,
        time_in_force=time_in_force,
        signal_at=NOW,
        created_at=NOW,
        submitted_at=submitted_at,
        **kwargs,
    )


def raw_fill(
    fill_id: str,
    *,
    side: Side = Side.BUY,
    quantity: str = "1",
    price: str = "100",
    fee: str = "0",
    filled_at: datetime = NOW + timedelta(seconds=1),
    **kwargs: object,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id=f"order-{fill_id}",
        signal_id=f"signal-{fill_id}",
        exchange=KEY[0],
        symbol=KEY[1],
        side=side,
        quantity=Decimal(quantity),
        price=Decimal(price),
        liquidity=LiquidityRole.TAKER,
        signal_at=NOW,
        order_created_at=NOW,
        submitted_at=NOW,
        filled_at=filled_at,
        fee=Decimal(fee),
        **kwargs,
    )


def test_signal_order_fill_timestamps_are_causal_and_costed() -> None:
    engine = BacktestEngine(Decimal("1000"), rules())
    engine.add_events(
        [
            signal(
                calculation_delay=timedelta(milliseconds=100),
                submission_delay=timedelta(milliseconds=200),
            ),
            market(0),
            market(1),
        ]
    )
    result = engine.run()

    assert len(result.fills) == 1
    fill = result.fills[0]
    assert fill.signal_at == NOW
    assert fill.order_created_at == NOW + timedelta(milliseconds=100)
    assert fill.submitted_at == NOW + timedelta(milliseconds=300)
    assert fill.filled_at == NOW + timedelta(seconds=1)
    assert fill.price > Decimal("100.1")
    assert fill.fee > 0
    assert fill.spread_cost > 0
    assert fill.slippage_cost > 0
    assert fill.market_impact_cost > 0
    assert result.event_trace[0][1] == "MarketEvent"


def test_partial_fill_then_completion_and_fok_unfilled() -> None:
    config = ExecutionModelConfig(participation_rate=Decimal("0.5"))
    engine = BacktestEngine(Decimal("1000"), rules(), execution_config=config)
    engine.add_events([signal(quantity="1"), market(0), market(1, depth="1"), market(2)])
    result = engine.run()
    order = result.orders[0]
    assert [fill.quantity for fill in result.fills] == [Decimal("0.5"), Decimal("0.5")]
    assert order.status is OrderStatus.FILLED
    assert order.filled_quantity == order.quantity

    no_fill = BacktestEngine(Decimal("1000"), rules(), execution_config=config)
    no_fill.add_events(
        [
            signal("fok", quantity="1", time_in_force=TimeInForce.FOK),
            market(0),
            market(1, depth="1"),
        ]
    )
    no_fill_result = no_fill.run()
    assert not no_fill_result.fills
    assert no_fill_result.orders[0].status is OrderStatus.CANCELLED


def test_passive_limit_is_maker_and_post_only_cross_is_cancelled() -> None:
    engine = BacktestEngine(Decimal("1000"), rules())
    engine.add_events(
        [
            signal(
                "maker",
                order_type=OrderType.LIMIT,
                limit_price=Decimal("99.04"),
                post_only=True,
            ),
            market(0),
            market(1, bid="98.8", ask="99.5", last="98.9", traded="20"),
        ]
    )
    result = engine.run()
    assert len(result.fills) == 1
    assert result.fills[0].liquidity is LiquidityRole.MAKER
    assert result.fills[0].price == Decimal("99.0")
    assert result.fills[0].fee == Decimal("0.01980")

    crossing = BacktestEngine(Decimal("1000"), rules())
    crossing.add_events(
        [
            signal(
                "cross",
                order_type=OrderType.LIMIT,
                limit_price=Decimal("101"),
                post_only=True,
            ),
            market(0),
            market(1),
        ]
    )
    crossing_result = crossing.run()
    assert crossing_result.orders[0].status is OrderStatus.CANCELLED
    assert not crossing_result.fills


def test_constraints_minimum_notional_ioc_and_duplicate_signal() -> None:
    engine = BacktestEngine(Decimal("1000"), rules(minimum_notional="10"))
    engine.add_events(
        [
            signal("small", quantity="0.05"),
            signal("small", quantity="1"),
            market(0),
            market(1),
        ]
    )
    result = engine.run()
    assert result.orders[0].status is OrderStatus.REJECTED
    assert result.orders[0].rejection_reason == "quantity is below lot size"
    assert result.rejected_signals[0].reason == "duplicate signal_id"

    below_notional = BacktestEngine(Decimal("1000"), rules(minimum_notional="20"))
    below_notional.add_events([signal("notional", quantity="0.1"), market(0), market(1)])
    below_notional_result = below_notional.run()
    assert below_notional_result.orders[0].status is OrderStatus.REJECTED
    assert below_notional_result.orders[0].rejection_reason == "order is below minimum notional"

    ioc = BacktestEngine(Decimal("1000"), rules())
    ioc.add_events(
        [
            signal("ioc", time_in_force=TimeInForce.IOC),
            market(0),
            market(1, depth="0"),
        ]
    )
    ioc_result = ioc.run()
    assert ioc_result.orders[0].status is OrderStatus.CANCELLED
    assert not ioc_result.fills


def test_funding_and_round_trip_position_accounting() -> None:
    engine = BacktestEngine(
        Decimal("1000"),
        rules(taker_fee="0"),
        execution_config=ExecutionModelConfig(
            slippage_bps=Decimal("0"), impact_coefficient_bps=Decimal("0")
        ),
    )
    engine.add_events(
        [
            signal(quantity="1"),
            market(0),
            market(1, bid="99.9", ask="100", mark="100"),
            FundingEvent(
                NOW + timedelta(seconds=2),
                KEY[0],
                KEY[1],
                Decimal("0.01"),
                Decimal("100"),
            ),
            SignalEvent(
                timestamp=NOW + timedelta(seconds=2),
                signal_id="exit",
                exchange=KEY[0],
                symbol=KEY[1],
                side=Side.SELL,
                quantity=Decimal("1"),
            ),
            market(3, bid="109", ask="109.1", mark="109"),
        ]
    )
    result = engine.run()
    position = engine.portfolio.position(*KEY)
    assert not position.is_open
    assert result.funding[0].amount == Decimal("-1.00")
    assert result.final_cash == Decimal("1008.00")
    assert position.realized_pnl == Decimal("9.0")


@pytest.mark.parametrize(
    ("protection", "trigger_market", "expected_prefix"),
    [
        ({"stop_loss": Decimal("95")}, market(2, bid="94", ask="94.2"), "stop_loss"),
        ({"take_profit": Decimal("105")}, market(2, bid="106", ask="106.2"), "take_profit"),
    ],
)
def test_protective_exit_uses_following_event(
    protection: dict[str, Decimal], trigger_market: MarketEvent, expected_prefix: str
) -> None:
    engine = BacktestEngine(Decimal("1000"), rules())
    engine.add_events(
        [
            signal(**protection),
            market(0),
            market(1),
            trigger_market,
            market(3, bid="93.8", ask="94"),
        ]
    )
    result = engine.run()
    assert len(result.fills) == 2
    assert result.fills[1].signal_id.startswith(expected_prefix)
    assert result.fills[1].filled_at == NOW + timedelta(seconds=3)
    assert not engine.portfolio.position(*KEY).is_open


def test_margin_cap_and_liquidation_create_reduce_only_exit() -> None:
    engine = BacktestEngine(Decimal("100"), rules(), leverage=Decimal("2"))
    engine.add_events(
        [
            signal(quantity="10"),
            market(0),
            market(1),
            market(2, bid="39.9", ask="40.1", mark="40"),
            market(3, bid="39", ask="39.2", mark="39"),
        ]
    )
    result = engine.run()
    assert result.fills[0].quantity < Decimal("2")
    assert len(result.liquidations) == 1
    assert result.fills[-1].signal_id.startswith("liquidation")
    assert not engine.portfolio.position(*KEY).is_open


def test_simultaneous_orders_are_processed_stably_without_exceeding_margin() -> None:
    engine = BacktestEngine(Decimal("100"), rules())
    engine.add_events([signal("one"), signal("two"), market(0), market(1)])
    result = engine.run()
    assert [fill.signal_id for fill in result.fills] == ["one"]
    assert result.snapshots[-1].margin_used <= result.snapshots[-1].equity


def test_instrument_and_market_contracts_reject_unsafe_values() -> None:
    with pytest.raises(ValueError, match="tick_size"):
        InstrumentRules(Decimal("0"), Decimal("1"), Decimal("0"))
    with pytest.raises(ValueError, match="minimum_notional"):
        InstrumentRules(Decimal("1"), Decimal("1"), Decimal("-1"))
    with pytest.raises(ValueError, match="fee rates"):
        InstrumentRules(Decimal("1"), Decimal("1"), Decimal("0"), maker_fee_rate=Decimal("-0.1"))
    with pytest.raises(ValueError, match="maintenance"):
        InstrumentRules(
            Decimal("1"),
            Decimal("1"),
            Decimal("0"),
            maintenance_margin_rate=Decimal("1"),
        )

    instrument = rules()[KEY]
    assert instrument.quantize_quantity(Decimal("-1")) == 0
    assert instrument.quantize_limit_price(Decimal("100.04"), Side.BUY) == Decimal("100.0")
    assert instrument.quantize_limit_price(Decimal("100.04"), Side.SELL) == Decimal("100.1")
    assert instrument.quantize_execution_price(Decimal("100.04"), Side.BUY) == Decimal("100.1")
    assert instrument.quantize_execution_price(Decimal("100.04"), Side.SELL) == Decimal("100.0")
    for operation in (
        instrument.quantize_price,
        lambda value: instrument.quantize_limit_price(value, Side.BUY),
        lambda value: instrument.quantize_execution_price(value, Side.BUY),
    ):
        with pytest.raises(ValueError, match="price"):
            operation(Decimal("0"))

    valid = market(0).snapshot
    assert valid.mid == Decimal("100.0")
    assert valid.valuation_price == Decimal("100")
    assert MarketSnapshot(
        KEY[0],
        KEY[1],
        NOW,
        Decimal("99"),
        Decimal("101"),
        Decimal("1"),
        Decimal("1"),
    ).valuation_price == Decimal("100")
    invalid_snapshots = [
        {"timestamp": datetime(2024, 1, 1), "bid": "99", "ask": "100", "depth": "1"},
        {"timestamp": NOW, "bid": "100", "ask": "100", "depth": "1"},
        {"timestamp": NOW, "bid": "99", "ask": "100", "depth": "-1"},
    ]
    for item in invalid_snapshots:
        with pytest.raises(ValueError):
            MarketSnapshot(
                KEY[0],
                KEY[1],
                item["timestamp"],
                Decimal(item["bid"]),
                Decimal(item["ask"]),
                Decimal(item["depth"]),
                Decimal("1"),
            )
    with pytest.raises(ValueError, match="last and mark"):
        MarketSnapshot(
            KEY[0],
            KEY[1],
            NOW,
            Decimal("99"),
            Decimal("100"),
            Decimal("1"),
            Decimal("1"),
            last_price=Decimal("0"),
        )
    with pytest.raises(ValueError, match="trade_quantity"):
        MarketSnapshot(
            KEY[0],
            KEY[1],
            NOW,
            Decimal("99"),
            Decimal("100"),
            Decimal("1"),
            Decimal("1"),
            trade_quantity=Decimal("-1"),
        )


def test_order_and_fill_contract_boundaries() -> None:
    with pytest.raises(ValueError, match="timestamps"):
        raw_order(submitted_at=NOW - timedelta(seconds=1))
    with pytest.raises(ValueError, match="quantity"):
        raw_order(quantity="0")
    with pytest.raises(ValueError, match="requires limit"):
        raw_order(order_type=OrderType.LIMIT)
    with pytest.raises(ValueError, match="cannot have limit"):
        raw_order(limit_price=Decimal("100"))
    with pytest.raises(ValueError, match="prices"):
        raw_order(stop_loss=Decimal("0"))
    with pytest.raises(ValueError, match="expires_at"):
        raw_order(expires_at=NOW)

    terminal = raw_order()
    terminal.reject("first")
    terminal.reject("ignored")
    terminal.cancel("ignored")
    assert terminal.rejection_reason == "first"
    with pytest.raises(ValueError, match="terminal"):
        terminal.record_fill(Decimal("1"))
    active = raw_order("active")
    with pytest.raises(ValueError, match="fill quantity"):
        active.record_fill(Decimal("2"))

    completed = raw_order("complete")
    completed.record_fill(Decimal("1"))
    assert completed.status is OrderStatus.FILLED
    event = raw_fill("valid", spread_cost=Decimal("1"))
    assert event.notional == Decimal("100")
    with pytest.raises(ValueError, match="causality"):
        raw_fill("time", filled_at=NOW)
    with pytest.raises(ValueError, match="quantity and price"):
        raw_fill("quantity", quantity="0")
    with pytest.raises(ValueError, match="costs"):
        raw_fill("fee", fee="-1")


def test_execution_configuration_and_submission_rejections() -> None:
    invalid_configs = [
        {"participation_rate": Decimal("0")},
        {"passive_fill_fraction": Decimal("2")},
        {"slippage_bps": Decimal("-1")},
        {"impact_power": 0},
    ]
    for values in invalid_configs:
        with pytest.raises(ValueError):
            ExecutionModelConfig(**values)

    simulator = ExecutionSimulator(rules())
    first = simulator.submit(raw_order("duplicate"))
    with pytest.raises(ValueError, match="duplicate"):
        simulator.submit(raw_order("duplicate"))
    simulator.cancel_all("shutdown")
    assert first.status is OrderStatus.CANCELLED

    unavailable = ExecutionSimulator({})
    missing = unavailable.submit(raw_order("missing"))
    assert missing.status is OrderStatus.REJECTED
    assert unavailable.match(market(1).snapshot) == []

    post_only_market = simulator.submit(raw_order("post-market", post_only=True))
    assert post_only_market.status is OrderStatus.REJECTED


def test_execution_expiry_caps_ioc_remainder_and_sell_costs() -> None:
    simulator = ExecutionSimulator(rules(), ExecutionModelConfig(participation_rate=Decimal("0.5")))
    expiring = simulator.submit(raw_order("expiry", expires_at=NOW + timedelta(milliseconds=500)))
    simulator.match(market(1).snapshot)
    assert expiring.status is OrderStatus.EXPIRED

    reduced = simulator.submit(raw_order("reduce", reduce_only=True))
    simulator.match(market(1).snapshot, lambda _order: Decimal("0"))
    assert reduced.status is OrderStatus.CANCELLED

    partial_ioc = simulator.submit(
        raw_order("partial-ioc", time_in_force=TimeInForce.IOC, quantity="1")
    )
    produced = simulator.match(market(2, depth="1").snapshot)
    assert produced[0].quantity == Decimal("0.5")
    assert partial_ioc.status is OrderStatus.CANCELLED

    simulator.submit(raw_order("sell", side=Side.SELL, quantity="1"))
    sell_fill = simulator.match(market(3).snapshot)[0]
    assert sell_fill.side is Side.SELL
    assert sell_fill.price < Decimal("99.9")
    assert sell_fill.spread_cost > 0


def test_limit_non_fill_paths_and_fill_or_kill_success() -> None:
    simulator = ExecutionSimulator(rules())
    too_tight = simulator.submit(
        raw_order(
            "tight",
            order_type=OrderType.LIMIT,
            limit_price=Decimal("100.1"),
        )
    )
    assert simulator.match(market(1).snapshot) == []
    assert too_tight.status is OrderStatus.SUBMITTED

    passive = simulator.submit(
        raw_order("passive", order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    )
    assert simulator.match(market(2, last=None, traded=None).snapshot) == []
    assert passive.status is OrderStatus.SUBMITTED

    fok = simulator.submit(raw_order("fok-success", time_in_force=TimeInForce.FOK))
    fills = simulator.match(market(3).snapshot)
    assert any(item.order_id == fok.order_id for item in fills)
    assert fok.status is OrderStatus.FILLED


def test_event_validation_queue_priority_and_run_guard() -> None:
    with pytest.raises(ValueError, match="timezone"):
        signal(timestamp=datetime(2024, 1, 1))
    with pytest.raises(ValueError, match="signal_id"):
        signal("")
    with pytest.raises(ValueError, match="quantity"):
        signal(quantity="0")
    with pytest.raises(ValueError, match="delays"):
        signal(calculation_delay=timedelta(seconds=-1))
    with pytest.raises(ValueError, match="requires limit"):
        signal(order_type=OrderType.LIMIT)
    with pytest.raises(ValueError, match="cannot include"):
        signal(limit_price=Decimal("100"))
    with pytest.raises(ValueError, match="expiry"):
        signal(expires_at=NOW)
    with pytest.raises(ValueError, match="funding mark"):
        FundingEvent(NOW, KEY[0], KEY[1], Decimal("0"), Decimal("0"))

    base_signal = signal("queue")
    base_order = raw_order("queue")
    queued_events = [
        OrderSubmissionEvent(NOW, base_order),
        OrderCreationEvent(NOW, base_signal),
        base_signal,
        FundingEvent(NOW, KEY[0], KEY[1], Decimal("0"), Decimal("100")),
        market(0),
    ]
    queue = EventQueue()
    for event in queued_events:
        queue.push(event)
    assert len(queue) == 5
    assert [type(queue.pop()).__name__ for _ in range(5)] == [
        "MarketEvent",
        "FundingEvent",
        "SignalEvent",
        "OrderCreationEvent",
        "OrderSubmissionEvent",
    ]
    assert not queue
    with pytest.raises(IndexError, match="empty"):
        queue.pop()

    engine = BacktestEngine(Decimal("100"), rules())
    engine.add_event(market(0))
    with pytest.raises(RuntimeError, match="maximum"):
        engine.run(maximum_events=0)


def test_portfolio_rejections_short_funding_flip_and_transactional_protection() -> None:
    with pytest.raises(ValueError, match="initial_cash"):
        PortfolioLedger(Decimal("0"), rules())
    with pytest.raises(ValueError, match="leverage"):
        PortfolioLedger(Decimal("100"), rules(), Decimal("0"))

    ledger = PortfolioLedger(Decimal("1000"), rules())
    assert ledger.apply_funding(*KEY, NOW, Decimal("0.1"), Decimal("100")) is None
    with pytest.raises(ValueError, match="timestamp"):
        ledger.apply_funding(*KEY, datetime(2024, 1, 1), Decimal("0"), Decimal("100"))
    with pytest.raises(ValueError, match="mark price"):
        ledger.apply_funding(*KEY, NOW, Decimal("0"), Decimal("0"))
    with pytest.raises(ValueError, match="no open"):
        ledger.apply_fill(raw_fill("reduce-empty"), reduce_only=True)

    with pytest.raises(ValueError, match="stop loss"):
        ledger.apply_fill(raw_fill("bad-stop"), stop_loss=Decimal("105"))
    assert ledger.cash == Decimal("1000")
    assert ledger.position(*KEY).quantity == 0
    assert not ledger.fills

    ledger.apply_fill(raw_fill("short", side=Side.SELL, price="100"))
    funding = ledger.apply_funding(*KEY, NOW, Decimal("0.01"), Decimal("100"))
    assert funding is not None and funding.amount == Decimal("1.00")
    with pytest.raises(ValueError, match="increase or reverse"):
        ledger.apply_fill(raw_fill("wrong-reduce", side=Side.SELL), reduce_only=True)
    ledger.apply_fill(raw_fill("add-short", side=Side.SELL, price="110"))
    assert ledger.position(*KEY).average_entry_price == Decimal("105")
    ledger.apply_fill(raw_fill("flip", side=Side.BUY, quantity="3", price="90"))
    position = ledger.position(*KEY)
    assert position.quantity == Decimal("1")
    assert position.average_entry_price == Decimal("90")
    assert position.realized_pnl == Decimal("30")
    assert ledger.maximum_fill_quantity(raw_order("cap"), Decimal("0")) == 0
    assert ledger.liquidation_decision(NOW) is None
