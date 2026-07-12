from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from app.config.settings import VenueRiskSettings


class VenueAllocationDecision(BaseModel):
    model_config = ConfigDict(frozen=True)
    allowed: bool
    total_allocated: Decimal = Field(ge=0)
    reasons: tuple[str, ...]


class VenueRiskBudget:
    def __init__(self, limits: dict[str, VenueRiskSettings]) -> None:
        self.limits = limits

    def evaluate(
        self,
        total_equity: Decimal,
        allocations: dict[str, Decimal],
        domestic_venues: frozenset[str] = frozenset({"gmo_coin", "bitbank", "bitflyer"}),
    ) -> VenueAllocationDecision:
        if total_equity <= 0:
            raise ValueError("total equity must be positive")
        if any(amount < 0 for amount in allocations.values()):
            raise ValueError("venue allocations cannot be negative")
        reasons: list[str] = []
        total = sum(allocations.values(), Decimal("0"))
        if total > total_equity:
            reasons.append("combined venue allocation exceeds total equity")
        domestic_total = sum(
            (amount for venue, amount in allocations.items() if venue in domestic_venues),
            Decimal("0"),
        )
        for venue, amount in allocations.items():
            limit_name = "domestic_exchanges" if venue in domestic_venues else venue
            limit = self.limits.get(limit_name)
            if limit is None:
                reasons.append(f"no venue risk limit configured for {venue}")
            elif amount > total_equity * Decimal(str(limit.maximum_equity_fraction)):
                reasons.append(f"{venue} exceeds its venue equity fraction")
        domestic_limit = self.limits.get("domestic_exchanges")
        if domestic_limit and domestic_total > total_equity * Decimal(
            str(domestic_limit.maximum_equity_fraction)
        ):
            reasons.append("combined domestic allocation exceeds its equity fraction")
        return VenueAllocationDecision(
            allowed=not reasons,
            total_allocated=total,
            reasons=tuple(reasons) if reasons else ("within venue risk budgets",),
        )
