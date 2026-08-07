from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RequiredVenueCapability:
    venue_role: str
    capability: str


@dataclass(frozen=True)
class StrategyCapabilityRequirement:
    strategy_id: str
    strategy_version: str
    required_capabilities: tuple[RequiredVenueCapability, ...]


@dataclass(frozen=True)
class StrategyDataRequirement:
    strategy_id: str
    required_capabilities: tuple[str, ...]
    required_venues: tuple[str, ...]
    minimum_coverage_ratio: Decimal
    maximum_stale_ratio: Decimal
    minimum_history_windows: int
    required_instruments: tuple[str, ...] = ()
    non_blocking_capabilities: tuple[str, ...] = ()


FUNDING_CARRY_STRATEGY_ID = "funding_carry"
FUNDING_CARRY_REQUIRED_CAPABILITIES = ("funding_current", "orderbook_snapshot")
FUNDING_CARRY_NON_BLOCKING_CAPABILITIES = (
    "funding_history",
    "mark_price",
    "index_price",
    "open_interest",
    "trade",
    "ohlcv",
)
FUNDING_CARRY_REQUIRED_VENUES = ("hyperliquid", "bitget")
FUNDING_CARRY_REQUIRED_INSTRUMENTS = ("BTC",)

FUNDING_CARRY_REQUIREMENT = StrategyDataRequirement(
    strategy_id=FUNDING_CARRY_STRATEGY_ID,
    required_capabilities=FUNDING_CARRY_REQUIRED_CAPABILITIES,
    required_venues=FUNDING_CARRY_REQUIRED_VENUES,
    required_instruments=FUNDING_CARRY_REQUIRED_INSTRUMENTS,
    non_blocking_capabilities=FUNDING_CARRY_NON_BLOCKING_CAPABILITIES,
    minimum_coverage_ratio=Decimal("0.80"),
    maximum_stale_ratio=Decimal("0.05"),
    minimum_history_windows=0,
)


STRATEGY_CAPABILITY_REGISTRY: dict[tuple[str, str], tuple[RequiredVenueCapability, ...]] = {
    (FUNDING_CARRY_STRATEGY_ID, "1"): tuple(
        RequiredVenueCapability(role, capability)
        for role in ("receive_leg", "pay_leg")
        for capability in FUNDING_CARRY_REQUIRED_CAPABILITIES
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
