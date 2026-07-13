from collections.abc import Sequence
from datetime import datetime
from typing import NoReturn

from app.adapters.exchanges.base import ExecutionAdapter
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionFill,
    ExecutionOrderAck,
    LiveOpenOrder,
    LiveOrderRequest,
    LivePosition,
    ReconciledOrder,
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

    async def fetch_recent_fills(self, symbol: str, since: datetime) -> Sequence[ExecutionFill]:
        del symbol, since
        self._blocked()

    async def lookup_order_by_client_id(self, client_order_id: str) -> ReconciledOrder | None:
        del client_order_id
        self._blocked()

    async def close_position(self, symbol: str) -> ExecutionOrderAck:
        del symbol
        self._blocked()


class StagedHyperliquidExecutionAdapter(StagedExecutionAdapter):
    name = "hyperliquid"


class StagedAsterExecutionAdapter(StagedExecutionAdapter):
    name = "aster"


class StagedBitgetExecutionAdapter(StagedExecutionAdapter):
    name = "bitget"


class StagedMexcExecutionAdapter(StagedExecutionAdapter):
    name = "mexc"


# Module-level aliases cannot warn on import by themselves. Instantiation of a legacy
# name does, while preserving import compatibility until downstream users migrate.
def _deprecated_alias(
    name: str, replacement: type[StagedExecutionAdapter]
) -> type[StagedExecutionAdapter]:
    class Deprecated(replacement):  # type: ignore[misc, valid-type]
        def __init__(self, *args: object, **kwargs: object) -> None:
            import warnings

            warnings.warn(
                f"{name} is staged; use {replacement.__name__}",
                DeprecationWarning,
                stacklevel=2,
            )
            super().__init__(*args, **kwargs)

    Deprecated.__name__ = name
    return Deprecated


HyperliquidExecutionAdapter = _deprecated_alias(
    "HyperliquidExecutionAdapter", StagedHyperliquidExecutionAdapter
)
AsterExecutionAdapter = _deprecated_alias("AsterExecutionAdapter", StagedAsterExecutionAdapter)
BitgetExecutionAdapter = _deprecated_alias("BitgetExecutionAdapter", StagedBitgetExecutionAdapter)
MexcExecutionAdapter = _deprecated_alias("MexcExecutionAdapter", StagedMexcExecutionAdapter)
