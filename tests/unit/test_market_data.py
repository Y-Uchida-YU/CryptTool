from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.domain.market_data.models import OHLCV
from app.services.ingestion.quality import validate_ohlcv


def bar(minute: int, volume: str = "10") -> OHLCV:
    return OHLCV(
        exchange="x",
        symbol="BTC/USD",
        timeframe="1m",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=minute),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        volume=Decimal(volume),
    )


def test_ohlcv_normalizes_utc_and_rejects_inconsistent() -> None:
    observation = bar(0)
    assert observation.timestamp.tzinfo == UTC
    assert observation.exchange_timestamp == observation.timestamp
    assert observation.received_at is not None and observation.available_at is not None
    assert observation.local_monotonic_time >= 0
    with pytest.raises(ValidationError, match="inconsistent"):
        OHLCV(
            exchange="x",
            symbol="BTC/USD",
            timeframe="1m",
            timestamp=datetime.now(UTC),
            open=100,
            high=99,
            low=90,
            close=101,
            volume=1,
        )


def test_validation_detects_duplicate_gap_and_order() -> None:
    rows = [bar(3), bar(0), bar(0)]
    result = validate_ohlcv(rows)
    assert {issue.code for issue in result.issues} == {"DUPLICATE", "GAP", "OUT_OF_ORDER"}
    assert len(result.accepted) == 2 and len(result.rejected) == 1
    assert result.quality_score < 1


def test_empty_is_fatal() -> None:
    assert validate_ohlcv([]).quality_score == 0
