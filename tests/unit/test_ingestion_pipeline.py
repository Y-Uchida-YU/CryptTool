from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.domain.market_data.models import OHLCV
from app.services.ingestion.pipeline import BackfillService, TokenBucketRateLimiter


def bar(minute: int) -> OHLCV:
    return OHLCV(
        exchange="simulation",
        symbol="BTCUSDT",
        timeframe="1m",
        timestamp=datetime(2025, 1, 1, tzinfo=UTC) + timedelta(minutes=minute),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("10"),
    )


@pytest.mark.asyncio
async def test_backfill_persists_only_after_quality_gate_and_advances_checkpoint() -> None:
    persisted: list[tuple[OHLCV, ...]] = []

    async def persist(rows):  # type: ignore[no-untyped-def]
        persisted.append(tuple(rows))

    service = BackfillService(persist)

    async def healthy():  # type: ignore[no-untyped-def]
        return [bar(0), bar(1)]

    result = await service.ingest(healthy)
    assert result.quality_score == 1
    assert persisted == [(bar(0), bar(1))]
    assert service.checkpoint == bar(1).timestamp

    async def duplicate():  # type: ignore[no-untyped-def]
        return [bar(2), bar(2)]

    rejected = await service.ingest(duplicate)
    assert rejected.quality_score < service.minimum_quality
    assert len(persisted) == 1
    assert service.checkpoint == bar(1).timestamp


@pytest.mark.asyncio
async def test_backfill_empty_and_persistence_failure_do_not_advance_checkpoint() -> None:
    async def noop(rows):  # type: ignore[no-untyped-def]
        del rows

    with pytest.raises(ValueError, match="minimum_quality"):
        BackfillService(noop, minimum_quality=2)

    async def persist_failure(rows):  # type: ignore[no-untyped-def]
        del rows
        raise RuntimeError("storage unavailable")

    service = BackfillService(persist_failure)

    async def empty():  # type: ignore[no-untyped-def]
        return []

    assert (await service.ingest(empty)).quality_score == 0
    assert service.checkpoint is None

    async def healthy():  # type: ignore[no-untyped-def]
        return [bar(0)]

    with pytest.raises(RuntimeError, match="storage unavailable"):
        await service.ingest(healthy)
    assert service.checkpoint is None


@pytest.mark.asyncio
async def test_token_bucket_validates_and_throttles(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(0, 1)
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(1, 0)

    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("app.services.ingestion.pipeline.asyncio.sleep", fake_sleep)
    limiter = TokenBucketRateLimiter(rate=2, capacity=1)
    await limiter.acquire()
    await limiter.acquire()
    assert len(delays) == 1 and delays[0] > 0
