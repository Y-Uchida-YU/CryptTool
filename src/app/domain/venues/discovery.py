from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum


class VenueCategory(StrEnum):
    CEX = "cex"
    DEX = "dex"


class DiscoveryDecision(StrEnum):
    OBSERVE = "observe"
    ADD_DATA_ADAPTER = "add_data_adapter"
    REJECT_LOW_VALUE = "reject_low_value"
    REJECT_INELIGIBLE = "reject_ineligible"
    PENDING_OPERATOR_VERIFICATION = "pending_operator_verification"


@dataclass(frozen=True, kw_only=True)
class VenueDiscoveryCandidate:
    venue: str
    category: VenueCategory
    official_docs_url: str
    terms_url: str | None
    operator_account_verified: bool
    public_api_reachable: bool
    unique_instrument_count: int | None
    funding_observation_count: int
    incremental_opportunity_count: int
    incremental_expected_net_pnl: Decimal
    api_uptime: float | None
    median_executable_depth_usd: Decimal | None
    evaluated_from: datetime | None
    evaluated_to: datetime | None
    decision: DiscoveryDecision

    def eligible_for_adapter(
        self,
        *,
        minimum_uptime: float,
        minimum_depth_usd: Decimal,
        withdrawals_verified: bool,
        requires_location_evasion: bool = False,
        emergency_hedge_value: bool = False,
    ) -> bool:
        if self.evaluated_from is None or self.evaluated_to is None:
            return False
        start, end = self.evaluated_from.astimezone(UTC), self.evaluated_to.astimezone(UTC)
        return all(
            (
                end - start >= timedelta(days=30),
                self.operator_account_verified,
                self.public_api_reachable,
                not requires_location_evasion,
                self.incremental_expected_net_pnl > 0,
                self.incremental_opportunity_count > 0 or emergency_hedge_value,
                self.api_uptime is not None and self.api_uptime >= minimum_uptime,
                self.median_executable_depth_usd is not None
                and self.median_executable_depth_usd >= minimum_depth_usd,
                withdrawals_verified,
            )
        )


DISCOVERY_REGISTRY = (
    VenueDiscoveryCandidate(
        venue="btcc",
        category=VenueCategory.CEX,
        official_docs_url="https://www.btcc.com/en-US/api-documentation",
        terms_url="https://www.btcc.com/en-US/terms",
        operator_account_verified=False,
        public_api_reachable=False,
        unique_instrument_count=None,
        funding_observation_count=0,
        incremental_opportunity_count=0,
        incremental_expected_net_pnl=Decimal("0"),
        api_uptime=None,
        median_executable_depth_usd=None,
        evaluated_from=None,
        evaluated_to=None,
        decision=DiscoveryDecision.PENDING_OPERATOR_VERIFICATION,
    ),
    VenueDiscoveryCandidate(
        venue="gateio",
        category=VenueCategory.CEX,
        official_docs_url="https://www.gate.com/docs/developers/apiv4/en/",
        terms_url="https://www.gate.com/legal/user-agreement",
        operator_account_verified=False,
        public_api_reachable=False,
        unique_instrument_count=None,
        funding_observation_count=0,
        incremental_opportunity_count=0,
        incremental_expected_net_pnl=Decimal("0"),
        api_uptime=None,
        median_executable_depth_usd=None,
        evaluated_from=None,
        evaluated_to=None,
        decision=DiscoveryDecision.PENDING_OPERATOR_VERIFICATION,
    ),
)
