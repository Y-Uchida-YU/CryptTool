from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.adapters.notifications.base import NullNotificationAdapter
from app.domain.market_data.models import Side
from app.services.paper_trading.broker import PaperBroker
from app.services.paper_trading.models import PaperOrderRequest, PaperQuote
from app.services.paper_trading.session import PaperTradingSession


@pytest.mark.asyncio
async def test_session_submits_then_fills_only_on_later_event() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)

    async def feed() -> AsyncIterator[PaperQuote]:
        for index in range(3):
            yield PaperQuote(
                symbol="BTC",
                timestamp=start + timedelta(seconds=index),
                bid=Decimal("99"),
                ask=Decimal("101"),
                bid_size=Decimal("10"),
                ask_size=Decimal("10"),
                data_quality_score=1,
                sequence=index,
            )

    emitted = False

    async def strategy(quote: PaperQuote) -> Sequence[PaperOrderRequest]:
        nonlocal emitted
        if emitted:
            return ()
        emitted = True
        return (
            PaperOrderRequest(
                client_order_id="session-order",
                symbol=quote.symbol,
                side=Side.BUY,
                quantity=Decimal("0.1"),
            ),
        )

    broker = PaperBroker(Decimal("1000"), max_participation=Decimal("1"))
    session = PaperTradingSession(broker, NullNotificationAdapter(), strategy)
    await session.run(feed())
    assert len(broker.fills) == 1
    assert broker.fills[0].timestamp == start + timedelta(seconds=1)
    await session.on_stream_disconnect(start + timedelta(seconds=3), "test disconnect")
    assert broker.operational_halt_reason is not None
    await session.on_stream_recovered(start + timedelta(seconds=4), "snapshot reconciled")
    assert broker.operational_halt_reason is None
    assert session.account_snapshot(start + timedelta(seconds=4)).positions["BTC"] == Decimal("0.1")


@pytest.mark.asyncio
async def test_session_does_not_calculate_signals_on_low_quality_data() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)

    async def feed() -> AsyncIterator[PaperQuote]:
        yield PaperQuote(
            symbol="BTC",
            timestamp=start,
            bid=Decimal("99"),
            ask=Decimal("101"),
            bid_size=Decimal("10"),
            ask_size=Decimal("10"),
            data_quality_score=0.1,
        )

    calls = 0

    async def strategy(quote: PaperQuote) -> Sequence[PaperOrderRequest]:
        nonlocal calls
        del quote
        calls += 1
        return ()

    session = PaperTradingSession(PaperBroker(Decimal("1000")), NullNotificationAdapter(), strategy)
    await session.run(feed())
    assert calls == 0
