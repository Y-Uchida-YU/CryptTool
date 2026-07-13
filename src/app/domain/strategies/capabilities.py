from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyCapabilityRequirement:
    strategy_id: str
    strategy_version: str
    required_capabilities: tuple[str, ...]


STRATEGY_CAPABILITY_REGISTRY: dict[tuple[str, str], tuple[str, ...]] = {
    ("cross_venue_funding", "1"): (
        "funding_current",
        "funding_history",
        "orderbook_snapshot",
    ),
    ("cross_venue_basis", "1"): ("orderbook_snapshot", "index_price"),
    ("liquidation_exhaustion", "1"): (
        "market_liquidation_stream",
        "open_interest",
        "trades",
    ),
}
