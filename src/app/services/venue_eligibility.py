from datetime import datetime

from app.config.settings import Settings
from app.domain.venues.models import VenueEligibility


def eligibility_from_settings(settings: Settings, venue: str) -> VenueEligibility:
    configured = settings.venues.get(venue)
    if configured is None:
        raise KeyError(f"venue eligibility is not configured: {venue}")
    return VenueEligibility(
        venue=venue,
        status=configured.eligibility_status,
        jurisdiction=configured.jurisdiction,
        terms_checked_at=configured.terms_checked_at,
        terms_version=configured.terms_version,
        operator_account_verified=configured.operator_account_verified,
        api_market_data_available=configured.api_market_data_available,
        api_execution_available=configured.api_execution_available,
        deposits_available=configured.deposits_available,
        withdrawals_available=configured.withdrawals_available,
        execution_smoke_test_passed=configured.execution_smoke_test_passed,
        requires_location_evasion=configured.requires_location_evasion,
        reason=configured.reason,
    )


def execution_eligibility_reason(
    settings: Settings, venue: str, now: datetime, *, reduce_only: bool
) -> str | None:
    eligibility = eligibility_from_settings(settings, venue)
    allowed, reason = (
        eligibility.permits_reduction(now) if reduce_only else eligibility.permits_new_orders(now)
    )
    return None if allowed else reason
