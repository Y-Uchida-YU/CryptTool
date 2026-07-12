from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Regime(StrEnum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"
    LONG_SQUEEZE = "LONG_SQUEEZE"
    SHORT_SQUEEZE = "SHORT_SQUEEZE"
    LIQUIDATION_CASCADE = "LIQUIDATION_CASCADE"
    FLASH_CRASH = "FLASH_CRASH"
    FUNDING_EXTREME_POSITIVE = "FUNDING_EXTREME_POSITIVE"
    FUNDING_EXTREME_NEGATIVE = "FUNDING_EXTREME_NEGATIVE"
    OI_EXPANSION = "OI_EXPANSION"
    OI_CONTRACTION = "OI_CONTRACTION"
    SPOT_LED_MOVE = "SPOT_LED_MOVE"
    PERP_LED_MOVE = "PERP_LED_MOVE"
    RISK_OFF = "RISK_OFF"
    UNKNOWN = "UNKNOWN"


class RegimeResult(BaseModel):
    primary_regime: Regime
    secondary_regimes: tuple[Regime, ...] = ()
    confidence: float = Field(ge=0, le=1)
    evidence: tuple[str, ...]
    feature_snapshot: dict[str, float | None]
    regime_started_at: datetime
    regime_duration_seconds: float = Field(ge=0)
    model_version: str
    data_quality_score: float = Field(ge=0, le=1)
    metadata: dict[str, Any] = {}
