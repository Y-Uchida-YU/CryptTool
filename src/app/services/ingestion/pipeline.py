import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import TypeVar

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from app.domain.market_data.models import OHLCV
from app.services.ingestion.quality import ValidationResult, validate_ohlcv

T = TypeVar("T")


class TokenBucketRateLimiter:
    def __init__(self, rate: float, capacity: int) -> None:
        if rate <= 0 or capacity < 1:
            raise ValueError("rate and capacity must be positive")
        self.rate, self.capacity, self.tokens = rate, capacity, float(capacity)
        self.updated = asyncio.get_running_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens < 1:
                await asyncio.sleep((1 - self.tokens) / self.rate)
                self.updated = asyncio.get_running_loop().time()
                self.tokens = 0
            else:
                self.tokens -= 1


class BackfillService:
    def __init__(
        self,
        persist: Callable[[Sequence[OHLCV]], Awaitable[None]],
        minimum_quality: float = 0.8,
    ) -> None:
        if not 0 <= minimum_quality <= 1:
            raise ValueError("minimum_quality must be in [0, 1]")
        self.persist = persist
        self.minimum_quality = minimum_quality
        self.checkpoint: datetime | None = None

    @retry(
        retry=retry_if_exception_type((TimeoutError, ConnectionError)),
        wait=wait_exponential_jitter(initial=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def ingest(self, fetch: Callable[[], Awaitable[Sequence[OHLCV]]]) -> ValidationResult:
        rows = list(await fetch())
        result = validate_ohlcv(rows)
        if result.accepted and result.quality_score >= self.minimum_quality:
            await self.persist(result.accepted)
            self.checkpoint = max(row.timestamp for row in result.accepted).astimezone(UTC)
        return result
