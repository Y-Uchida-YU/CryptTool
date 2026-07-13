from dataclasses import dataclass


@dataclass(frozen=True)
class RequiredVenueCapability:
    venue_role: str
    capability: str


@dataclass(frozen=True)
class StrategyCapabilityRequirement:
    strategy_id: str
    strategy_version: str
    required_capabilities: tuple[RequiredVenueCapability, ...]


STRATEGY_CAPABILITY_REGISTRY: dict[tuple[str, str], tuple[RequiredVenueCapability, ...]] = {
    ("cross_venue_funding", "1"): tuple(
        RequiredVenueCapability(role, capability)
        for role in ("receive_leg", "pay_leg")
        for capability in ("funding_current", "funding_history", "orderbook_snapshot")
    ),
    ("cross_venue_basis", "1"): tuple(
        RequiredVenueCapability(role, capability)
        for role in ("receive_leg", "pay_leg")
        for capability in ("orderbook_snapshot", "index_price")
    ),
    ("liquidation_exhaustion", "1"): tuple(
        RequiredVenueCapability("order_leg", capability)
        for capability in ("market_liquidation_stream", "open_interest", "trades")
    ),
}
