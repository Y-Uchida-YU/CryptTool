from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.services.paper_trading.broker import PaperBroker


def paper_period_report(broker: PaperBroker, end: datetime, days: int = 1) -> dict[str, str | int]:
    if end.tzinfo is None:
        raise ValueError("report end timestamp must be timezone-aware")
    end = end.astimezone(UTC)
    start = end - timedelta(days=days)
    fills = [fill for fill in broker.fills if start < fill.timestamp <= end]
    return {
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "fills": len(fills),
        "notional": str(sum((fill.price * fill.quantity for fill in fills), Decimal("0"))),
        "fees": str(sum((fill.fee for fill in fills), Decimal("0"))),
        "slippage": str(sum((fill.slippage_cost for fill in fills), Decimal("0"))),
        "equity": str(broker.equity()),
        "live_trading": 0,
    }


def daily_report(broker: PaperBroker, end: datetime) -> dict[str, str | int]:
    return paper_period_report(broker, end, days=1)


def weekly_report(broker: PaperBroker, end: datetime) -> dict[str, str | int]:
    return paper_period_report(broker, end, days=7)
