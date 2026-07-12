from dataclasses import dataclass
from decimal import Decimal

from app.adapters.exchanges.base import ExecutionAdapter


@dataclass(frozen=True, kw_only=True)
class HedgeVenueCandidate:
    venue: str
    adapter: ExecutionAdapter
    eligible: bool
    live_health: bool
    instrument_equivalent: bool
    executable_depth: Decimal
    estimated_slippage: Decimal
    latency_ms: Decimal
    available_collateral: Decimal
    withdrawals_available: bool
    chain_healthy: bool
    exposure_within_budget: bool


def select_emergency_hedge(candidates: tuple[HedgeVenueCandidate, ...]) -> HedgeVenueCandidate:
    eligible = [
        candidate
        for candidate in candidates
        if all(
            (
                candidate.eligible,
                candidate.live_health,
                candidate.instrument_equivalent,
                candidate.withdrawals_available,
                candidate.chain_healthy,
                candidate.exposure_within_budget,
                bool(getattr(candidate.adapter, "is_concrete", False)),
                candidate.available_collateral > 0,
                candidate.executable_depth > 0,
            )
        )
    ]
    if not eligible:
        raise RuntimeError("no concrete execution venue satisfies emergency hedge gates")
    return max(
        eligible,
        key=lambda c: (
            c.executable_depth,
            -c.estimated_slippage,
            -c.latency_ms,
            c.available_collateral,
        ),
    )
