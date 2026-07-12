from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class VenueEligibilityStatus(StrEnum):
    ENABLED = "enabled"
    DATA_ONLY = "data_only"
    EXIT_ONLY = "exit_only"
    DISABLED = "disabled"
    PENDING_VERIFICATION = "pending_verification"


@dataclass(frozen=True, kw_only=True)
class VenueEligibility:
    venue: str
    status: VenueEligibilityStatus
    jurisdiction: str
    terms_checked_at: datetime
    terms_version: str | None = None
    operator_account_verified: bool
    api_market_data_available: bool
    api_execution_available: bool
    deposits_available: bool
    withdrawals_available: bool
    execution_smoke_test_passed: bool
    requires_location_evasion: bool
    reason: str

    def __post_init__(self) -> None:
        if isinstance(self.status, str):
            object.__setattr__(self, "status", VenueEligibilityStatus(self.status))
        if self.terms_checked_at.tzinfo is None:
            raise ValueError("terms_checked_at must be timezone-aware")
        object.__setattr__(self, "terms_checked_at", self.terms_checked_at.astimezone(UTC))

    def permits_new_orders(self, now: datetime) -> tuple[bool, str]:
        now = _utc(now)
        failures: list[str] = []
        if self.requires_location_evasion:
            failures.append("location evasion would be required")
        if self.status != VenueEligibilityStatus.ENABLED:
            failures.append(f"status is {self.status.value}")
        if not self.operator_account_verified:
            failures.append("operator account is not verified")
        if not self.api_execution_available:
            failures.append("execution API is unavailable or unverified")
        if not self.withdrawals_available:
            failures.append("withdrawals are unavailable or unverified")
        if not self.execution_smoke_test_passed:
            failures.append("minimum execution smoke test has not passed")
        if now - self.terms_checked_at >= timedelta(days=30) or now < self.terms_checked_at:
            failures.append("terms verification is stale or future-dated")
        return not failures, "; ".join(failures) if failures else "eligible"

    def permits_reduction(self, now: datetime) -> tuple[bool, str]:
        now = _utc(now)
        if self.requires_location_evasion:
            return False, "location evasion is forbidden even for automated exit"
        if now < self.terms_checked_at:
            return False, "terms verification is future-dated"
        if self.status in {VenueEligibilityStatus.ENABLED, VenueEligibilityStatus.EXIT_ONLY}:
            return True, "reduction permitted"
        return False, f"status {self.status.value} does not permit execution"


class VenueCapabilityMatrix(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    detected_at: datetime
    source_version: str
    spot: bool = False
    perpetual: bool = False
    dated_futures: bool = False
    funding_current: bool = False
    funding_history: bool = False
    predicted_funding: bool = False
    open_interest: bool = False
    liquidations: bool = False
    orderbook_snapshot: bool = False
    orderbook_delta: bool = False
    trades: bool = False
    mark_price: bool = False
    index_price: bool = False
    long_short_ratio: bool = False
    wallet_positions: bool = False
    wallet_transfers: bool = False
    private_websocket: bool = False
    post_only: bool = False
    reduce_only: bool = False
    ioc: bool = False
    fok: bool = False
    batch_orders: bool = False
    subaccounts: bool = False

    @field_validator("detected_at")
    @classmethod
    def detected_at_must_be_aware(cls, value: datetime) -> datetime:
        return _utc(value)

    def require(self, capability: str) -> None:
        from app.adapters.exchanges.base import CapabilityUnavailableError

        if capability not in type(self).model_fields:
            raise ValueError(f"unknown capability: {capability}")
        if not bool(getattr(self, capability)):
            raise CapabilityUnavailableError(f"{self.venue} does not provide {capability}")


class InstrumentKind(StrEnum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    DATED_FUTURE = "dated_future"


class CanonicalAsset(BaseModel):
    model_config = ConfigDict(frozen=True)
    asset_id: str
    symbol: str
    network: str | None = None
    contract_address: str | None = None


class CanonicalInstrument(BaseModel):
    model_config = ConfigDict(frozen=True)
    instrument_id: str
    base_asset_id: str
    quote_asset_id: str
    settlement_asset_id: str
    kind: InstrumentKind
    inverse: bool = False
    contract_multiplier: Decimal = Field(default=Decimal("1"), gt=0)
    index_composition_id: str
    funding_interval_minutes: int | None = Field(default=None, gt=0)
    expiry: datetime | None = None

    @model_validator(mode="after")
    def validate_kind(self) -> CanonicalInstrument:
        if self.kind == InstrumentKind.PERPETUAL and self.funding_interval_minutes is None:
            raise ValueError("perpetual instruments require a funding interval")
        if self.kind == InstrumentKind.DATED_FUTURE and self.expiry is None:
            raise ValueError("dated futures require expiry")
        if self.expiry is not None:
            _utc(self.expiry)
        return self


class VenueInstrumentMapping(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: str
    venue_symbol: str
    canonical_instrument_id: str
    base: str
    quote: str
    settlement: str
    kind: InstrumentKind
    inverse: bool
    contract_multiplier: Decimal = Field(gt=0)
    index_composition_id: str
    funding_interval_minutes: int | None = Field(default=None, gt=0)
    verified_at: datetime

    @field_validator("verified_at")
    @classmethod
    def verified_at_must_be_aware(cls, value: datetime) -> datetime:
        return _utc(value)

    def matches(self, instrument: CanonicalInstrument) -> bool:
        return all(
            (
                self.canonical_instrument_id == instrument.instrument_id,
                self.base == instrument.base_asset_id,
                self.quote == instrument.quote_asset_id,
                self.settlement == instrument.settlement_asset_id,
                self.kind == instrument.kind,
                self.inverse == instrument.inverse,
                self.contract_multiplier == instrument.contract_multiplier,
                self.index_composition_id == instrument.index_composition_id,
                self.funding_interval_minutes == instrument.funding_interval_minutes,
            )
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
