from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from app.domain.market_data.models import Side
from app.services.paper_trading.broker import PaperBroker
from app.services.paper_trading.models import PaperOrderRequest, PaperQuote


@given(
    quantity=st.decimals(min_value="0.001", max_value="10", places=3),
    visible=st.decimals(min_value="0.001", max_value="20", places=3),
    participation=st.decimals(min_value="0.01", max_value="1", places=2),
)
def test_fills_never_exceed_order_or_visible_participation(
    quantity: Decimal, visible: Decimal, participation: Decimal
) -> None:
    now = datetime(2025, 1, 1, tzinfo=UTC)
    broker = PaperBroker(
        Decimal("1000000"),
        max_participation=participation,
        max_gross_leverage=Decimal("1"),
    )
    broker.submit(
        PaperOrderRequest(
            client_order_id="property",
            symbol="BTC",
            side=Side.BUY,
            quantity=quantity,
        ),
        now,
    )
    fills = broker.on_quote(
        PaperQuote(
            symbol="BTC",
            timestamp=now + timedelta(seconds=1),
            bid=Decimal("99"),
            ask=Decimal("100"),
            bid_size=visible,
            ask_size=visible,
            data_quality_score=1,
        )
    )
    if fills:
        assert fills[0].quantity <= quantity
        assert fills[0].quantity <= visible * participation
        assert fills[0].fee >= 0
