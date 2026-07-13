from collections.abc import Sequence

from app.adapters.exchanges.base import ExecutionAdapter
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionOrderAck,
    LiveOpenOrder,
    LiveOrderRequest,
    LivePosition,
)


class LiveTradingDisabledError(RuntimeError):
    """Raised whenever code attempts to use the permanently disabled adapter."""


class DisabledExecutionAdapter(ExecutionAdapter):
    @property
    def adapter_name(self) -> str:
        return "disabled"

    @property
    def is_concrete(self) -> bool:
        return False

    async def place_order(self, request: LiveOrderRequest) -> ExecutionOrderAck:
        del request
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def cancel_order(self, order_id: str) -> CancelAck:
        del order_id
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def cancel_all_orders(self, symbol: str | None = None) -> Sequence[CancelAck]:
        del symbol
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def fetch_open_orders(self, symbol: str | None = None) -> Sequence[LiveOpenOrder]:
        del symbol
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def fetch_positions(self) -> Sequence[LivePosition]:
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def fetch_recent_fills(self, symbol: str) -> Sequence[ExecutionOrderAck]:
        del symbol
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def lookup_order_by_client_id(self, request_id: str) -> ExecutionOrderAck | None:
        del request_id
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def close_position(self, symbol: str) -> ExecutionOrderAck:
        del symbol
        raise LiveTradingDisabledError("live execution adapter is disabled")

    async def health_check(self) -> bool:
        return False
