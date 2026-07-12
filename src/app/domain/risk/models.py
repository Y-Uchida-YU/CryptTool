from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SizingMethod(StrEnum):
    CONSERVATIVE_MINIMUM = "conservative_minimum"
    FIXED_FRACTIONAL = "fixed_fractional"
    VOLATILITY_TARGETING = "volatility_targeting"
    ATR_BASED = "atr_based"
    MAXIMUM_LOSS = "maximum_loss"


class RiskHaltReason(StrEnum):
    MANUAL_KILL_SWITCH = "manual_kill_switch"
    DAILY_LOSS = "daily_loss_limit"
    WEEKLY_LOSS = "weekly_loss_limit"
    MAX_DRAWDOWN = "maximum_drawdown"
    CONSECUTIVE_LOSSES = "consecutive_losses"
    DATA_ANOMALY = "data_anomaly"
    API_UNHEALTHY = "api_unhealthy"
    WEBSOCKET_DISCONNECTED = "websocket_disconnected"
    PRICE_DIVERGENCE = "price_divergence"
    SPREAD_ANOMALY = "spread_anomaly"
    COOLDOWN = "cooldown"


class RiskLimits(BaseModel):
    model_config = ConfigDict(frozen=True)

    maximum_leverage: float = Field(default=1.0, gt=0, le=1.0)
    maximum_risk_per_trade: float = Field(default=0.0025, gt=0, le=0.01)
    maximum_daily_loss: float = Field(default=0.01, gt=0, le=0.05)
    maximum_weekly_loss: float = Field(default=0.03, gt=0, le=0.10)
    maximum_drawdown: float = Field(default=0.08, gt=0, le=0.20)
    maximum_gross_exposure: float = Field(default=1.0, gt=0, le=1.0)
    maximum_symbol_exposure: float = Field(default=0.35, gt=0, le=1.0)
    maximum_exchange_exposure: float = Field(default=0.50, gt=0, le=1.0)
    maximum_positions: int = Field(default=3, ge=1, le=20)
    maximum_consecutive_losses: int = Field(default=5, ge=1, le=20)
    minimum_data_quality: float = Field(default=0.80, ge=0, le=1)
    minimum_signal_confidence: float = Field(default=0.60, ge=0, le=1)
    maximum_spread_fraction: float = Field(default=0.005, gt=0, le=0.05)
    maximum_price_divergence_fraction: float = Field(default=0.02, gt=0, le=0.20)
    cooldown_seconds: int = Field(default=1800, ge=1, le=86_400)
    annual_volatility_target: float = Field(default=0.10, gt=0, le=0.50)
    atr_multiple: float = Field(default=2.0, ge=1, le=10)
    maximum_loss_floor_fraction: float = Field(default=0.01, gt=0, le=0.10)
    sizing_method: SizingMethod = SizingMethod.CONSERVATIVE_MINIMUM

    @model_validator(mode="after")
    def exposure_limits_must_be_consistent(self) -> RiskLimits:
        if self.maximum_symbol_exposure > self.maximum_gross_exposure:
            raise ValueError("symbol exposure cannot exceed gross exposure")
        if self.maximum_exchange_exposure > self.maximum_gross_exposure:
            raise ValueError("exchange exposure cannot exceed gross exposure")
        if self.maximum_weekly_loss < self.maximum_daily_loss:
            raise ValueError("weekly loss limit cannot be below the daily limit")
        return self


class RiskState(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    equity: Decimal = Field(gt=0)
    peak_equity: Decimal = Field(gt=0)
    day_start_equity: Decimal = Field(gt=0)
    week_start_equity: Decimal = Field(gt=0)
    realized_pnl_today: Decimal = Decimal("0")
    realized_pnl_week: Decimal = Decimal("0")
    gross_exposure: Decimal = Field(default=Decimal("0"), ge=0)
    symbol_exposures: dict[str, Decimal] = Field(default_factory=dict)
    exchange_exposures: dict[str, Decimal] = Field(default_factory=dict)
    open_positions: int = Field(default=0, ge=0)
    consecutive_losses: int = Field(default=0, ge=0)
    data_quality_score: float = Field(default=1.0, ge=0, le=1)
    data_healthy: bool = True
    api_healthy: bool = True
    websocket_connected: bool = True
    spread_fraction: float = Field(default=0, ge=0)
    price_divergence_fraction: float = Field(default=0, ge=0)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("symbol_exposures", "exchange_exposures")
    @classmethod
    def exposures_must_be_non_negative(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        if any(exposure < 0 for exposure in value.values()):
            raise ValueError("exposures must be non-negative absolute notionals")
        return value

    @model_validator(mode="after")
    def peak_and_period_equity_must_be_plausible(self) -> RiskState:
        if self.peak_equity < self.equity:
            raise ValueError("peak_equity cannot be below current equity")
        return self

    @property
    def daily_loss_fraction(self) -> Decimal:
        return max(-self.realized_pnl_today / self.day_start_equity, Decimal("0"))

    @property
    def weekly_loss_fraction(self) -> Decimal:
        return max(-self.realized_pnl_week / self.week_start_equity, Decimal("0"))

    @property
    def drawdown_fraction(self) -> Decimal:
        return max((self.peak_equity - self.equity) / self.peak_equity, Decimal("0"))


class PositionSizingResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    accepted: bool
    quantity: Decimal = Field(ge=0)
    notional: Decimal = Field(ge=0)
    risk_amount: Decimal = Field(ge=0)
    binding_constraint: str
    candidate_notionals: dict[str, Decimal] = Field(default_factory=dict)
    reason: str

    @model_validator(mode="after")
    def rejected_sizes_must_be_zero(self) -> PositionSizingResult:
        if not self.accepted and (self.quantity != 0 or self.notional != 0):
            raise ValueError("rejected sizing results must have zero quantity and notional")
        return self


class RiskDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    allowed: bool
    reasons: tuple[str, ...] = Field(min_length=1)
    sizing: PositionSizingResult | None = None
    evaluated_at: datetime
    breaker_reasons: tuple[RiskHaltReason, ...] = ()

    @field_validator("evaluated_at")
    @classmethod
    def evaluated_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("evaluated_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def allowed_entry_requires_sizing(self) -> RiskDecision:
        if self.allowed and self.sizing is not None and not self.sizing.accepted:
            raise ValueError("allowed risk decision cannot contain rejected sizing")
        return self


class CircuitBreakerStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    active: bool
    reasons: tuple[RiskHaltReason, ...] = ()
    details: dict[RiskHaltReason, str] = Field(default_factory=dict)
    tripped_at: datetime | None = None
    resume_at: datetime | None = None
    requires_manual_reset: bool = False
