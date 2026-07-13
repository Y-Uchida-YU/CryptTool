from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Any

from app.domain.execution.live_models import (
    CancelAck,
    ExecutionFill,
    ExecutionOrderAck,
    LiveOpenOrder,
    LiveOrderRequest,
    LivePosition,
    ReconciledOrder,
)
from app.domain.market_data.models import OHLCV, FundingRate, Market, OpenInterest, OrderBook, Trade


class CapabilityUnavailableError(NotImplementedError):
    """Raised when an exchange does not publish a requested data type."""


class MarketDataAdapter(ABC):
    @abstractmethod
    async def fetch_markets(self) -> Sequence[Market]: ...
    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]: ...
    @abstractmethod
    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]: ...
    @abstractmethod
    async def fetch_open_interest(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[OpenInterest]: ...
    @abstractmethod
    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook: ...
    @abstractmethod
    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]: ...
    @abstractmethod
    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]: ...
    @abstractmethod
    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]: ...
    @abstractmethod
    def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]: ...
    @abstractmethod
    async def health_check(self) -> bool: ...


class ExecutionAdapter(ABC):
    @property
    @abstractmethod
    def adapter_name(self) -> str: ...
    @property
    @abstractmethod
    def is_concrete(self) -> bool: ...
    @abstractmethod
    async def place_order(self, request: LiveOrderRequest) -> ExecutionOrderAck: ...
    @abstractmethod
    async def cancel_order(self, order_id: str) -> CancelAck: ...
    @abstractmethod
    async def cancel_all_orders(self, symbol: str | None = None) -> Sequence[CancelAck]: ...
    @abstractmethod
    async def fetch_open_orders(self, symbol: str | None = None) -> Sequence[LiveOpenOrder]: ...
    @abstractmethod
    async def fetch_positions(self) -> Sequence[LivePosition]: ...
    @abstractmethod
    async def fetch_recent_fills(self, symbol: str, since: datetime) -> Sequence[ExecutionFill]: ...
    @abstractmethod
    async def lookup_order_by_client_id(self, client_order_id: str) -> ReconciledOrder | None: ...
    @abstractmethod
    async def close_position(self, symbol: str) -> ExecutionOrderAck: ...
    @abstractmethod
    async def health_check(self) -> bool: ...
