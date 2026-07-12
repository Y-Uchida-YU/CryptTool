from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, kw_only=True)
class VenueValueAttribution:
    venue: str
    opportunities_discovered: int
    opportunities_unique_to_venue: int
    gross_edge_contribution: Decimal
    net_edge_contribution: Decimal
    fees: Decimal
    slippage: Decimal
    failed_leg_cost: Decimal
    stale_data_rejection_count: int
    api_outage_count: int
    venue_exclusion_count: int
    capital_required: Decimal
    capital_efficiency: Decimal
    maximum_venue_exposure: Decimal
    risk_reduction: Decimal = Decimal("0")


def aggregate_venue_value(rows: tuple[VenueValueAttribution, ...]) -> dict[str, object]:
    return {
        "venues": [row.__dict__ for row in rows],
        "incremental_net_pnl": sum((row.net_edge_contribution for row in rows), Decimal("0")),
        "risk_reduction": sum((row.risk_reduction for row in rows), Decimal("0")),
    }
