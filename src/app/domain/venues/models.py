from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

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


class CapabilitySupport(StrEnum):
    UNAVAILABLE = "unavailable"
    DOCUMENTED = "documented"
    IMPLEMENTED = "implemented"
    LIVE_VERIFIED = "live_verified"
    DEGRADED = "degraded"


class CapabilityUseCase(StrEnum):
    RESEARCH_COLLECTION = "research_collection"
    SIGNAL_GENERATION = "signal_generation"
    NEW_EXPOSURE = "new_exposure"
    EMERGENCY_EXIT = "emergency_exit"


@dataclass(frozen=True, kw_only=True)
class VenueCapability:
    name: str
    support: CapabilitySupport
    documented_at: datetime | None = None
    implemented_at: datetime | None = None
    live_verified_at: datetime | None = None
    source_url: str | None = None
    verification_run_id: str | None = None
    failure_reason: str | None = None
    emergency_exit_approved: bool = False

    def __post_init__(self) -> None:
        for field_name in ("documented_at", "implemented_at", "live_verified_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _utc(value))
        if self.support == CapabilitySupport.DEGRADED and not self.failure_reason:
            raise ValueError("degraded capabilities require a failure_reason")
        if self.support == CapabilitySupport.LIVE_VERIFIED and self.live_verified_at is None:
            raise ValueError("live-verified capabilities require live_verified_at")

    def supports(
        self,
        use_case: CapabilityUseCase,
        *,
        now: datetime,
        maximum_age: timedelta,
    ) -> bool:
        if maximum_age < timedelta(0):
            raise ValueError("maximum_age cannot be negative")
        live_is_fresh = (
            self.support == CapabilitySupport.LIVE_VERIFIED
            and self.live_verified_at is not None
            and timedelta(0) <= _utc(now) - self.live_verified_at <= maximum_age
        )
        if use_case == CapabilityUseCase.RESEARCH_COLLECTION:
            return self.support in {
                CapabilitySupport.IMPLEMENTED,
                CapabilitySupport.LIVE_VERIFIED,
            }
        if use_case in {
            CapabilityUseCase.SIGNAL_GENERATION,
            CapabilityUseCase.NEW_EXPOSURE,
        }:
            return live_is_fresh
        if use_case == CapabilityUseCase.EMERGENCY_EXIT:
            return live_is_fresh or (
                self.support == CapabilitySupport.DEGRADED and self.emergency_exit_approved
            )
        raise ValueError(f"unsupported capability use case: {use_case}")

    def transition(
        self,
        support: CapabilitySupport,
        *,
        at: datetime,
        source_url: str | None = None,
        verification_run_id: str | None = None,
        failure_reason: str | None = None,
    ) -> VenueCapability:
        allowed = {
            CapabilitySupport.UNAVAILABLE: {CapabilitySupport.DOCUMENTED},
            CapabilitySupport.DOCUMENTED: {CapabilitySupport.IMPLEMENTED},
            CapabilitySupport.IMPLEMENTED: {
                CapabilitySupport.LIVE_VERIFIED,
                CapabilitySupport.DEGRADED,
            },
            CapabilitySupport.LIVE_VERIFIED: {CapabilitySupport.DEGRADED},
            CapabilitySupport.DEGRADED: {
                CapabilitySupport.IMPLEMENTED,
                CapabilitySupport.LIVE_VERIFIED,
            },
        }
        if support not in allowed[self.support]:
            raise ValueError(f"invalid capability transition {self.support} -> {support}")
        when = _utc(at)
        return VenueCapability(
            name=self.name,
            support=support,
            documented_at=self.documented_at
            or (when if support != CapabilitySupport.UNAVAILABLE else None),
            implemented_at=self.implemented_at
            or (
                when
                if support
                in {
                    CapabilitySupport.IMPLEMENTED,
                    CapabilitySupport.LIVE_VERIFIED,
                    CapabilitySupport.DEGRADED,
                }
                else None
            ),
            live_verified_at=when
            if support == CapabilitySupport.LIVE_VERIFIED
            else self.live_verified_at,
            source_url=source_url or self.source_url,
            verification_run_id=verification_run_id,
            failure_reason=failure_reason if support == CapabilitySupport.DEGRADED else None,
            emergency_exit_approved=False,
        )

    def __bool__(self) -> bool:
        raise TypeError(
            "VenueCapability cannot be evaluated implicitly; "
            "use supports_collection(), supports_signal(), or supports_execution()"
        )


CAPABILITY_NAMES = (
    "spot",
    "perpetual",
    "dated_futures",
    "funding_current",
    "funding_history",
    "predicted_funding",
    "open_interest",
    "liquidations",
    "orderbook_snapshot",
    "orderbook_delta",
    "trades",
    "mark_price",
    "index_price",
    "long_short_ratio",
    "wallet_positions",
    "wallet_transfers",
    "private_websocket",
    "post_only",
    "reduce_only",
    "ioc",
    "fok",
    "batch_orders",
    "subaccounts",
)


class VenueCapabilityMatrix(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    venue: str
    detected_at: datetime
    source_version: str
    capabilities: dict[str, VenueCapability] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    @model_validator(mode="before")
    @classmethod
    def collect_capabilities(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        result = dict(value)
        detected_at = result.get("detected_at")
        capabilities = dict(result.pop("capabilities", {}))
        # Accept the old constructor shape while ensuring no bool survives in the model.
        for name in CAPABILITY_NAMES:
            if name not in result:
                continue
            raw = result.pop(name)
            if isinstance(raw, VenueCapability):
                capabilities[name] = raw
            elif isinstance(raw, (CapabilitySupport, str)) and not isinstance(raw, bool):
                support = CapabilitySupport(raw)
                capabilities[name] = VenueCapability(
                    name=name,
                    support=support,
                    documented_at=detected_at if support != CapabilitySupport.UNAVAILABLE else None,
                    implemented_at=detected_at
                    if support
                    in {
                        CapabilitySupport.IMPLEMENTED,
                        CapabilitySupport.LIVE_VERIFIED,
                        CapabilitySupport.DEGRADED,
                    }
                    else None,
                    live_verified_at=detected_at
                    if support == CapabilitySupport.LIVE_VERIFIED
                    else None,
                    failure_reason="legacy degraded declaration"
                    if support == CapabilitySupport.DEGRADED
                    else None,
                )
            else:
                support = CapabilitySupport.IMPLEMENTED if raw else CapabilitySupport.UNAVAILABLE
                capabilities[name] = VenueCapability(
                    name=name,
                    support=support,
                    documented_at=detected_at if raw else None,
                    implemented_at=detected_at if raw else None,
                )
        for name in CAPABILITY_NAMES:
            capabilities.setdefault(
                name, VenueCapability(name=name, support=CapabilitySupport.UNAVAILABLE)
            )
        result["capabilities"] = capabilities
        return result

    @field_validator("detected_at")
    @classmethod
    def detected_at_must_be_aware(cls, value: datetime) -> datetime:
        return _utc(value)

    def require(
        self,
        capability: str,
        use_case: CapabilityUseCase,
        now: datetime,
        maximum_age: timedelta,
    ) -> None:
        from app.adapters.exchanges.base import CapabilityUnavailableError

        if capability not in self.capabilities:
            raise ValueError(f"unknown capability: {capability}")
        if not self.capabilities[capability].supports(use_case, now=now, maximum_age=maximum_age):
            raise CapabilityUnavailableError(
                f"{self.venue} {capability} does not support {use_case.value}"
            )

    def __getattr__(self, name: str) -> VenueCapability:
        capabilities = object.__getattribute__(self, "capabilities")
        if name in capabilities:
            return capabilities[name]  # type: ignore[no-any-return]
        raise AttributeError(name)


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
