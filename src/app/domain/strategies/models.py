from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.regimes.models import Regime


class StrategyName(StrEnum):
    TREND_FOLLOWING = "trend_following"
    MEAN_REVERSION = "mean_reversion"
    FLASH_CRASH_REVERSAL = "flash_crash_reversal"
    FUNDING_EXTREME = "funding_extreme"
    RELATIVE_STRENGTH = "relative_strength"


class SignalSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    FLAT = "flat"


class SignalIntent(StrEnum):
    ENTRY = "entry"
    EXIT = "exit"
    REDUCE = "reduce"


class Signal(BaseModel):
    """Auditable strategy output; confidence is setup confidence, not win probability."""

    model_config = ConfigDict(frozen=True)

    signal_id: str = Field(min_length=16, max_length=64)
    timestamp: datetime
    strategy: StrategyName
    symbol: str = Field(min_length=1)
    exchange: str | None = None
    side: SignalSide
    intent: SignalIntent = SignalIntent.ENTRY
    strength: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    suggested_risk_fraction: float = Field(default=0.0025, gt=0, le=0.01)
    primary_regime: Regime
    secondary_regimes: tuple[Regime, ...] = ()
    data_quality_score: float = Field(ge=0, le=1)
    valid_until: datetime
    evidence: tuple[str, ...] = Field(min_length=1)
    feature_snapshot: dict[str, float | None]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", "valid_until")
    @classmethod
    def timestamps_must_be_aware_and_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("signal timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("feature_snapshot")
    @classmethod
    def snapshot_must_not_contain_non_finite_values(
        cls, value: dict[str, float | None]
    ) -> dict[str, float | None]:
        invalid = [key for key, item in value.items() if item is not None and not isfinite(item)]
        if invalid:
            raise ValueError(f"non-finite feature values: {', '.join(sorted(invalid))}")
        return value

    @model_validator(mode="after")
    def validate_signal_semantics(self) -> Signal:
        if self.valid_until <= self.timestamp:
            raise ValueError("valid_until must be later than timestamp")
        if self.side == SignalSide.FLAT:
            if self.strength != 0 or self.intent == SignalIntent.ENTRY:
                raise ValueError("flat signals must be non-entry signals with zero strength")
        elif self.strength <= 0:
            raise ValueError("actionable signals require positive strength")
        return self


class StrategyEvaluation(BaseModel):
    """A signal or an explicit, auditable reason why no signal was emitted."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    strategy: StrategyName
    symbol: str
    gate_passed: bool
    signal: Signal | None = None
    reasons: tuple[str, ...] = Field(min_length=1)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def signal_requires_gate(self) -> StrategyEvaluation:
        if self.signal is not None and not self.gate_passed:
            raise ValueError("a signal cannot be emitted through a closed regime gate")
        return self


class StrategyBatchEvaluation(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    strategy: StrategyName
    gate_passed: bool
    signals: tuple[Signal, ...] = ()
    reasons: tuple[str, ...] = Field(min_length=1)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def signals_require_gate(self) -> StrategyBatchEvaluation:
        if self.signals and not self.gate_passed:
            raise ValueError("signals cannot be emitted through a closed regime gate")
        return self
