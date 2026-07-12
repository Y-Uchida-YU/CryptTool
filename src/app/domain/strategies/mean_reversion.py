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
class MeanReversionConfig:
    minimum_regime_confidence: float = 0.60
    minimum_data_quality: float = 0.80
    entry_return_z: float = 2.0
    full_strength_z: float = 3.5
    minimum_price_deviation: float = 0.005
    maximum_spread_z: float = 2.0
    maximum_abs_slope_z: float = 0.50
    signal_valid_minutes: int = 3
    suggested_risk_fraction: float = 0.0015

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_regime_confidence <= 1:
            raise ValueError("minimum_regime_confidence must be in [0, 1]")
        if not 0 <= self.minimum_data_quality <= 1:
            raise ValueError("minimum_data_quality must be in [0, 1]")
        if self.entry_return_z <= 0 or self.full_strength_z < self.entry_return_z:
            raise ValueError("z-score thresholds are inconsistent")
        if self.minimum_price_deviation <= 0 or self.maximum_spread_z <= 0:
            raise ValueError("deviation and spread thresholds must be positive")
        if self.signal_valid_minutes < 1 or not 0 < self.suggested_risk_fraction <= 0.01:
            raise ValueError("unsafe mean-reversion risk or lifetime")


class MeanReversionStrategy(BaseStrategy):
    name = StrategyName.MEAN_REVERSION
    _snapshot_fields = (
        "return_zscore",
        "bollinger_position",
        "ema_distance",
        "vwap_distance",
        "liquidity_recovery",
        "spread_zscore",
        "ma_slope_z",
    )

    def __init__(self, config: MeanReversionConfig | None = None) -> None:
        self.config = config or MeanReversionConfig()

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        gate_open, gate_reasons = regime_gate(
            context.regime,
            allowed=frozenset((Regime.RANGE,)),
            blocked=frozenset(
                (
                    Regime.UNKNOWN,
                    Regime.TREND_UP,
                    Regime.TREND_DOWN,
                    Regime.HIGH_VOLATILITY,
                    Regime.FLASH_CRASH,
                    Regime.LIQUIDATION_CASCADE,
                    Regime.RISK_OFF,
                )
            ),
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
        missing = missing_features(features, ("return_zscore",))
        if missing:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=("missing required feature: return_zscore",),
            )
        return_z = features["return_zscore"]
        failures: list[str] = []
        if abs(return_z) < self.config.entry_return_z:
            failures.append(
                f"abs(return_zscore)={abs(return_z):.3f} below {self.config.entry_return_z:.3f}"
            )
        slope = features.get("ma_slope_z")
        if slope is not None and abs(slope) > self.config.maximum_abs_slope_z:
            failures.append(f"ma_slope_z={slope:.3f} indicates a non-flat market")
        spread_z = features.get("spread_zscore")
        if spread_z is not None and spread_z > self.config.maximum_spread_z:
            failures.append(f"spread_zscore={spread_z:.3f} indicates impaired execution")

        side = SignalSide.BUY if return_z < 0 else SignalSide.SELL
        direction = 1.0 if side == SignalSide.BUY else -1.0
        deviations: list[str] = []
        bollinger = features.get("bollinger_position")
        if bollinger is not None and (
            (side == SignalSide.BUY and bollinger <= 0.05)
            or (side == SignalSide.SELL and bollinger >= 0.95)
        ):
            deviations.append(f"Bollinger position is extreme ({bollinger:.3f})")
        for feature_name in ("ema_distance", "vwap_distance"):
            deviation = features.get(feature_name)
            if (
                deviation is not None
                and deviation * direction <= -self.config.minimum_price_deviation
            ):
                deviations.append(f"{feature_name} confirms deviation ({deviation:.5f})")
        if not deviations:
            failures.append("no Bollinger, EMA, or VWAP deviation confirmation")

        liquidity_evidence: list[str] = []
        recovery = features.get("liquidity_recovery")
        if recovery is not None and recovery > 0:
            liquidity_evidence.append(f"liquidity recovery is positive ({recovery:.3f})")
        if spread_z is not None and spread_z <= 1.0:
            liquidity_evidence.append(f"spread has normalized (z={spread_z:.3f})")
        if not liquidity_evidence:
            failures.append("no observable liquidity recovery or normalized spread")
        if failures:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=tuple(failures),
            )

        z_score = min(
            (abs(return_z) - self.config.entry_return_z)
            / (self.config.full_strength_z - self.config.entry_return_z),
            1.0,
        )
        setup_score = max(0.25, 0.65 * z_score + 0.20 * min(len(deviations) / 2, 1) + 0.15)
        evidence = (
            "RANGE regime gate passed and trend/cascade gates absent",
            f"return_zscore={return_z:.3f}",
            *deviations,
            *liquidity_evidence,
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
            metadata={"confidence_semantics": "evidence_strength_not_win_probability"},
        )
        return StrategyEvaluation(
            timestamp=context.timestamp,
            strategy=self.name,
            symbol=context.symbol,
            gate_passed=True,
            signal=signal,
            reasons=("range reversion setup satisfied",),
        )
