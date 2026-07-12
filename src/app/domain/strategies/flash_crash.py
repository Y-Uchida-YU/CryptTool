from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from app.domain.regimes.models import Regime
from app.domain.strategies.base import (
    BaseStrategy,
    StrategyContext,
    deterministic_signal_id,
    feature_snapshot,
    finite_features,
    missing_features,
    regime_gate,
    setup_confidence,
)
from app.domain.strategies.models import Signal, SignalSide, StrategyEvaluation, StrategyName


@dataclass(frozen=True)
class FlashCrashReversalConfig:
    minimum_regime_confidence: float = 0.65
    minimum_data_quality: float = 0.85
    maximum_return_z: float = -3.0
    minimum_volume_z: float = 2.0
    minimum_liquidation_z: float = 2.326
    maximum_oi_z: float = -2.0
    minimum_spread_z: float = 2.0
    minimum_recovery_confirmations: int = 2
    signal_valid_minutes: int = 1
    suggested_risk_fraction: float = 0.0010

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_regime_confidence <= 1:
            raise ValueError("minimum_regime_confidence must be in [0, 1]")
        if not 0 <= self.minimum_data_quality <= 1:
            raise ValueError("minimum_data_quality must be in [0, 1]")
        if self.maximum_return_z >= 0 or self.maximum_oi_z >= 0:
            raise ValueError("crash return and OI thresholds must be negative")
        if min(self.minimum_volume_z, self.minimum_liquidation_z, self.minimum_spread_z) <= 0:
            raise ValueError("shock thresholds must be positive")
        if self.minimum_recovery_confirmations < 2:
            raise ValueError("flash-crash reversal requires at least two recovery confirmations")
        if self.signal_valid_minutes < 1 or not 0 < self.suggested_risk_fraction <= 0.0025:
            raise ValueError("flash-crash risk and lifetime must remain conservative")


class FlashCrashReversalStrategy(BaseStrategy):
    name = StrategyName.FLASH_CRASH_REVERSAL
    _snapshot_fields = (
        "return_zscore",
        "volume_zscore",
        "long_liquidation_zscore",
        "liquidation_zscore",
        "oi_zscore",
        "spread_zscore",
        "liquidity_recovery",
        "sell_pressure_change",
        "cvd_momentum",
        "book_imbalance",
        "spread_recovery_ratio",
    )

    def __init__(self, config: FlashCrashReversalConfig | None = None) -> None:
        self.config = config or FlashCrashReversalConfig()

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        gate_open, gate_reasons = regime_gate(
            context.regime,
            allowed=frozenset((Regime.FLASH_CRASH,)),
            blocked=frozenset((Regime.UNKNOWN, Regime.RISK_OFF)),
            minimum_confidence=self.config.minimum_regime_confidence,
            minimum_quality=self.config.minimum_data_quality,
        )
        if not gate_open:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=False,
                reasons=gate_reasons,
            )
        features = finite_features(context.features)
        liquidation_key = (
            "long_liquidation_zscore"
            if "long_liquidation_zscore" in features
            else "liquidation_zscore"
        )
        required = (
            "return_zscore",
            "volume_zscore",
            liquidation_key,
            "oi_zscore",
            "spread_zscore",
        )
        missing = missing_features(features, required)
        if missing:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=(f"missing shock confirmation features: {','.join(missing)}",),
            )

        shock_checks = (
            (
                features["return_zscore"] <= self.config.maximum_return_z,
                f"return_zscore={features['return_zscore']:.3f}",
            ),
            (
                features["volume_zscore"] >= self.config.minimum_volume_z,
                f"volume_zscore={features['volume_zscore']:.3f}",
            ),
            (
                features[liquidation_key] >= self.config.minimum_liquidation_z,
                f"{liquidation_key}={features[liquidation_key]:.3f}",
            ),
            (
                features["oi_zscore"] <= self.config.maximum_oi_z,
                f"oi_zscore={features['oi_zscore']:.3f}",
            ),
            (
                features["spread_zscore"] >= self.config.minimum_spread_z,
                f"spread expansion z={features['spread_zscore']:.3f}",
            ),
        )
        failed_shocks = tuple(
            f"shock condition failed: {reason}" for passed, reason in shock_checks if not passed
        )
        if failed_shocks:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=failed_shocks,
            )

        recovery: list[str] = []
        liquidity_recovery = features.get("liquidity_recovery")
        if liquidity_recovery is not None and liquidity_recovery > 0:
            recovery.append(f"liquidity recovering ({liquidity_recovery:.3f})")
        sell_pressure_change = features.get("sell_pressure_change")
        if sell_pressure_change is not None and sell_pressure_change < 0:
            recovery.append(f"sell pressure decelerating ({sell_pressure_change:.3f})")
        cvd_momentum = features.get("cvd_momentum")
        if cvd_momentum is not None and cvd_momentum > 0:
            recovery.append(f"CVD improving ({cvd_momentum:.3f})")
        book_imbalance = features.get("book_imbalance")
        if book_imbalance is not None and book_imbalance > 0:
            recovery.append(f"book imbalance turned bid-heavy ({book_imbalance:.3f})")
        spread_recovery = features.get("spread_recovery_ratio")
        if spread_recovery is not None and spread_recovery > 0:
            recovery.append(f"spread recovery ratio={spread_recovery:.3f}")
        if len(recovery) < self.config.minimum_recovery_confirmations:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=(
                    f"only {len(recovery)} recovery confirmations; requires "
                    f"{self.config.minimum_recovery_confirmations}",
                ),
            )

        shock_score = min(
            (
                abs(features["return_zscore"]) / abs(self.config.maximum_return_z)
                + features["volume_zscore"] / self.config.minimum_volume_z
                + features[liquidation_key] / self.config.minimum_liquidation_z
                + abs(features["oi_zscore"]) / abs(self.config.maximum_oi_z)
            )
            / 8,
            1.0,
        )
        recovery_score = min(len(recovery) / 4, 1.0)
        setup_score = 0.60 * shock_score + 0.40 * recovery_score
        side = SignalSide.BUY
        evidence = (
            "FLASH_CRASH regime gate passed",
            *(reason for _, reason in shock_checks),
            *recovery,
        )
        signal = Signal(
            signal_id=deterministic_signal_id(self.name, context.symbol, context.timestamp, side),
            timestamp=context.timestamp,
            strategy=self.name,
            symbol=context.symbol,
            exchange=context.exchange,
            side=side,
            strength=setup_score,
            confidence=setup_confidence(context.regime, setup_score),
            suggested_risk_fraction=self.config.suggested_risk_fraction,
            primary_regime=context.regime.primary_regime,
            secondary_regimes=context.regime.secondary_regimes,
            data_quality_score=context.regime.data_quality_score,
            valid_until=context.timestamp + timedelta(minutes=self.config.signal_valid_minutes),
            evidence=evidence,
            feature_snapshot=feature_snapshot(context.features, self._snapshot_fields),
            metadata={
                "stages": ("shock", "liquidation_and_oi", "liquidity_recovery"),
                "confidence_semantics": "evidence_strength_not_win_probability",
            },
        )
        return StrategyEvaluation(
            timestamp=context.timestamp,
            strategy=self.name,
            symbol=context.symbol,
            gate_passed=True,
            signal=signal,
            reasons=("multi-stage flash-crash reversal setup satisfied",),
        )
