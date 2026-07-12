from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.domain.market_data.models import Side
from app.services.paper_trading.broker import PaperBroker
from app.services.paper_trading.models import (
    PaperAccountSnapshot,
    PaperAuditEvent,
    PaperFill,
    PaperOrderRequest,
    PaperOrderStatus,
    PaperOrderType,
    PaperQuote,
)
from app.services.paper_trading.report import daily_report, paper_period_report, weekly_report

NOW = datetime(2025, 1, 1, tzinfo=UTC)


def quote(
    seconds: int,
    *,
    bid: str = "99",
    ask: str = "101",
    size: str = "10",
    quality: float = 1.0,
    sequence: int | None = None,
) -> PaperQuote:
    return PaperQuote(
        symbol="BTC",
        timestamp=NOW + timedelta(seconds=seconds),
        bid=Decimal(bid),
        ask=Decimal(ask),
        bid_size=Decimal(size),
        ask_size=Decimal(size),
        data_quality_score=quality,
        sequence=sequence,
    )


def market_order(order_id: str = "one", quantity: str = "1") -> PaperOrderRequest:
    return PaperOrderRequest(
        client_order_id=order_id,
        symbol="BTC",
        side=Side.BUY,
        quantity=Decimal(quantity),
    )


def test_order_never_fills_on_same_quote_and_can_partially_fill() -> None:
    broker = PaperBroker(Decimal("10000"), max_participation=Decimal("0.1"))
    broker.submit(market_order(quantity="2"), NOW)
    assert broker.on_quote(quote(0)) == ()
    fills = broker.on_quote(quote(1, size="5", sequence=1))
    assert fills[0].quantity == Decimal("0.5")
    assert broker.orders["one"].status == PaperOrderStatus.PARTIALLY_FILLED
    assert fills[0].price > Decimal("101")
    assert fills[0].fee >= 0


def test_limit_unfilled_cancel_and_duplicate_sequence() -> None:
    broker = PaperBroker(Decimal("10000"))
    request = PaperOrderRequest(
        client_order_id="limit",
        symbol="BTC",
        side=Side.BUY,
        quantity=Decimal("1"),
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("100"),
    )
    broker.submit(request, NOW)
    assert broker.on_quote(quote(1, sequence=1)) == ()
    assert broker.on_quote(quote(2, ask="99.5", bid="99", sequence=1)) == ()
    assert broker.cancel("limit").status == PaperOrderStatus.CANCELED


def test_quality_gate_kill_switch_funding_and_report() -> None:
    broker = PaperBroker(Decimal("10000"), max_participation=Decimal("1"))
    broker.submit(market_order(), NOW)
    assert broker.on_quote(quote(1, quality=0.5)) == ()
    assert broker.on_quote(quote(2))
    payment = broker.apply_funding("BTC", Decimal("0.001"), NOW)
    assert payment < 0
    report = paper_period_report(broker, NOW + timedelta(days=1))
    assert report["fills"] == 1 and report["live_trading"] == 0
    assert daily_report(broker, NOW + timedelta(days=1))["fills"] == 1
    assert weekly_report(broker, NOW + timedelta(days=1))["fills"] == 1
    with pytest.raises(ValueError, match="timezone-aware"):
        paper_period_report(broker, datetime(2025, 1, 1))
    broker.activate_kill_switch("operator")
    rejected = broker.submit(market_order("two"), NOW + timedelta(days=1))
    assert rejected.status == PaperOrderStatus.REJECTED


def test_duplicate_client_order_id_and_exposure_rejection() -> None:
    broker = PaperBroker(Decimal("100"), max_participation=Decimal("1"))
    broker.submit(market_order(quantity="2"), NOW)
    with pytest.raises(ValueError, match="duplicate"):
        broker.submit(market_order(quantity="2"), NOW)
    assert broker.on_quote(quote(1))[0:] == ()
    assert broker.orders["one"].status == PaperOrderStatus.REJECTED
    assert any(event.event_type == "order_rejected" for event in broker.audit_events)


def test_sequence_gap_halts_until_explicit_recovery() -> None:
    broker = PaperBroker(Decimal("10000"), max_participation=Decimal("1"))
    broker.on_quote(quote(0, sequence=1))
    broker.submit(market_order(), NOW)
    assert broker.on_quote(quote(1, sequence=3)) == ()
    assert "BTC" in broker.data_halted_symbols
    broker.clear_data_halt("BTC", "REST snapshot reconciled", NOW + timedelta(seconds=2))
    assert broker.on_quote(quote(3, sequence=10))
    assert any(event.event_type == "data_sequence_gap" for event in broker.audit_events)


def test_paper_configuration_and_models_reject_unsafe_values() -> None:
    with pytest.raises(ValueError, match="initial_cash"):
        PaperBroker(Decimal("0"))
    with pytest.raises(ValueError, match="participation"):
        PaperBroker(Decimal("100"), max_participation=Decimal("2"))
    with pytest.raises(ValueError, match="leverage"):
        PaperBroker(Decimal("100"), max_gross_leverage=Decimal("2"))
    with pytest.raises(ValueError, match="costs"):
        PaperBroker(Decimal("100"), fee_rate=Decimal("-0.1"))
    with pytest.raises(ValueError, match="minimum_data_quality"):
        PaperBroker(Decimal("100"), minimum_data_quality=2)
    with pytest.raises(ValidationError, match="limit_price"):
        PaperOrderRequest(
            client_order_id="invalid-limit",
            symbol="BTC",
            side=Side.BUY,
            quantity=Decimal("1"),
            order_type=PaperOrderType.LIMIT,
        )
    with pytest.raises(ValidationError, match="cannot specify"):
        PaperOrderRequest(
            client_order_id="invalid-market",
            symbol="BTC",
            side=Side.BUY,
            quantity=Decimal("1"),
            limit_price=Decimal("100"),
        )
    with pytest.raises(ValidationError, match="timezone"):
        PaperQuote(
            symbol="BTC",
            timestamp=datetime(2025, 1, 1),
            bid=Decimal("99"),
            ask=Decimal("101"),
            bid_size=Decimal("1"),
            ask_size=Decimal("1"),
            data_quality_score=1,
        )
    with pytest.raises(ValidationError, match="positive spread"):
        quote(0, bid="101", ask="100")


def test_operational_halt_rejects_new_orders_and_requires_aware_timestamps() -> None:
    broker = PaperBroker(Decimal("1000"))
    broker.halt("stream down", NOW)
    rejected = broker.submit(market_order(), NOW)
    assert rejected.status == PaperOrderStatus.REJECTED
    assert "operational halt" in (rejected.rejection_reason or "")
    broker.resume("snapshot reconciled", NOW)
    with pytest.raises(ValueError, match="timezone-aware"):
        broker.submit(market_order("naive"), datetime(2025, 1, 1))
    with pytest.raises(ValueError, match="timezone-aware"):
        broker.snapshot(datetime(2025, 1, 1))


def test_limit_sell_close_and_position_reversal_accounting() -> None:
    broker = PaperBroker(Decimal("10000"), max_participation=Decimal("1"))
    broker.submit(market_order("long", "1"), NOW)
    broker.on_quote(quote(1, bid="100", ask="101"))
    sell = PaperOrderRequest(
        client_order_id="close-long",
        symbol="BTC",
        side=Side.SELL,
        quantity=Decimal("1"),
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("104"),
    )
    broker.submit(sell, NOW + timedelta(seconds=1))
    limit_fill = broker.on_quote(quote(2, bid="105", ask="106"))[0]
    assert limit_fill.slippage_cost == 0
    assert broker.positions["BTC"].quantity == 0
    assert broker.positions["BTC"].realized_pnl > 0

    short = PaperOrderRequest(
        client_order_id="short",
        symbol="BTC",
        side=Side.SELL,
        quantity=Decimal("2"),
    )
    broker.submit(short, NOW + timedelta(seconds=2))
    broker.on_quote(quote(3, bid="104", ask="105"))
    broker.submit(market_order("reverse", "3"), NOW + timedelta(seconds=3))
    broker.on_quote(quote(4, bid="102", ask="103"))
    assert broker.positions["BTC"].quantity == Decimal("1")
    assert broker.positions["BTC"].average_entry > 0
    assert broker.apply_funding("ETH", Decimal("0.001"), NOW) == 0


def test_paper_timestamp_models_reject_naive_values() -> None:
    naive = datetime(2025, 1, 1)
    with pytest.raises(ValidationError, match="fill timestamp"):
        PaperFill(
            fill_id="fill",
            client_order_id="order",
            symbol="BTC",
            side=Side.BUY,
            timestamp=naive,
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee=Decimal("0"),
            slippage_cost=Decimal("0"),
        )
    with pytest.raises(ValidationError, match="snapshot timestamp"):
        PaperAccountSnapshot(
            timestamp=naive,
            cash=Decimal("100"),
            equity=Decimal("100"),
            gross_exposure=Decimal("0"),
            fees_paid=Decimal("0"),
            funding_pnl=Decimal("0"),
            open_orders=0,
            positions={},
        )
    with pytest.raises(ValidationError, match="audit timestamp"):
        PaperAuditEvent(
            event_id="event",
            timestamp=naive,
            event_type="test",
            payload={},
        )
