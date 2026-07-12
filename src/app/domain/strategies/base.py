from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from math import isfinite

from app.domain.regimes.models import Regime, RegimeResult
from app.domain.strategies.models import SignalSide, StrategyEvaluation, StrategyName


@dataclass(frozen=True)
class StrategyContext:
    timestamp: datetime
    symbol: str
    features: Mapping[str, float | None]
    regime: RegimeResult
    exchange: str | None = None


class BaseStrategy(ABC):
    name: StrategyName

    @abstractmethod
    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        raise NotImplementedError


def finite_features(features: Mapping[str, float | None]) -> dict[str, float]:
    return {
        key: float(value)
        for key, value in features.items()
        if value is not None and isfinite(value)
    }


def feature_snapshot(
    features: Mapping[str, float | None], names: tuple[str, ...]
) -> dict[str, float | None]:
    finite = finite_features(features)
    return {name: finite.get(name) for name in names}


def missing_features(features: Mapping[str, float], required: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(name for name in required if name not in features)


def active_regimes(result: RegimeResult) -> frozenset[Regime]:
    return frozenset((result.primary_regime, *result.secondary_regimes))


def regime_gate(
    result: RegimeResult,
    *,
    allowed: frozenset[Regime],
    blocked: frozenset[Regime],
    minimum_confidence: float,
    minimum_quality: float,
) -> tuple[bool, tuple[str, ...]]:
    active = active_regimes(result)
    reasons: list[str] = []
    if result.primary_regime == Regime.UNKNOWN:
        reasons.append("primary regime is UNKNOWN")
    if active.isdisjoint(allowed):
        allowed_names = ",".join(sorted(item.value for item in allowed))
        reasons.append(f"required regime absent: {allowed_names}")
    present_blocks = active & blocked
    if present_blocks:
        reasons.append(
            f"blocking regime present: {','.join(sorted(item.value for item in present_blocks))}"
        )
    if result.confidence < minimum_confidence:
        reasons.append(f"regime confidence {result.confidence:.3f} below {minimum_confidence:.3f}")
    if result.data_quality_score < minimum_quality:
        reasons.append(f"data quality {result.data_quality_score:.3f} below {minimum_quality:.3f}")
    return not reasons, tuple(reasons)


def setup_confidence(regime: RegimeResult, setup_score: float) -> float:
    """Conservative evidence aggregation, intentionally not a calibrated P(win)."""

    bounded = min(max(setup_score, 0.0), 1.0)
    return min(regime.confidence, bounded) * regime.data_quality_score


def deterministic_signal_id(
    strategy: StrategyName,
    symbol: str,
    timestamp: datetime,
    side: SignalSide,
    discriminator: str = "",
) -> str:
    payload = "|".join(
        (strategy.value, symbol, timestamp.isoformat(), side.value, discriminator)
    ).encode()
    return sha256(payload).hexdigest()
