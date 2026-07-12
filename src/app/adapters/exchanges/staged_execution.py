from collections.abc import Sequence
from typing import NoReturn

from app.adapters.exchanges.base import ExecutionAdapter
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionOrderAck,
    LiveOpenOrder,
    LiveOrderRequest,
    LivePosition,
)


class ExecutionNotActivatedError(RuntimeError):
    """Raised until account, eligibility, contract, smoke and withdrawal gates pass."""


class StagedExecutionAdapter(ExecutionAdapter):
    name = "staged"

    @property
    def adapter_name(self) -> str:
        return self.name

    @property
    def is_concrete(self) -> bool:
        return False

    async def health_check(self) -> bool:
        return False

    def _blocked(self) -> NoReturn:
        raise ExecutionNotActivatedError(f"{self.name} execution is staged and disabled")

    async def place_order(self, request: LiveOrderRequest) -> ExecutionOrderAck:
        del request
        self._blocked()

    async def cancel_order(self, order_id: str) -> CancelAck:
        del order_id
        self._blocked()

    async def cancel_all_orders(self, symbol: str | None = None) -> Sequence[CancelAck]:
        del symbol
        self._blocked()

    async def fetch_open_orders(self, symbol: str | None = None) -> Sequence[LiveOpenOrder]:
        del symbol
        self._blocked()

    async def fetch_positions(self) -> Sequence[LivePosition]:
        self._blocked()

    async def close_position(self, symbol: str) -> ExecutionOrderAck:
        del symbol
        self._blocked()


class HyperliquidExecutionAdapter(StagedExecutionAdapter):
    name = "hyperliquid"


class AsterExecutionAdapter(StagedExecutionAdapter):
    name = "aster"


class BitgetExecutionAdapter(StagedExecutionAdapter):
    name = "bitget"


class MexcExecutionAdapter(StagedExecutionAdapter):
    name = "mexc"
