from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from app.domain.regimes.models import Regime
from app.domain.strategies.base import (
    BaseStrategy,
    StrategyContext,
    active_regimes,
    deterministic_signal_id,
    feature_snapshot,
    finite_features,
    missing_features,
    regime_gate,
    setup_confidence,
)
from app.domain.strategies.models import Signal, SignalSide, StrategyEvaluation, StrategyName


@dataclass(frozen=True)
class TrendFollowingConfig:
    minimum_regime_confidence: float = 0.60
    minimum_data_quality: float = 0.80
    minimum_slope_z: float = 1.0
    minimum_breakout_distance: float = 0.001
    breakout_full_strength: float = 0.01
    minimum_adx: float = 20.0
    minimum_confirmations: int = 1
    signal_valid_minutes: int = 5
    suggested_risk_fraction: float = 0.0025

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_regime_confidence <= 1:
            raise ValueError("minimum_regime_confidence must be in [0, 1]")
        if not 0 <= self.minimum_data_quality <= 1:
            raise ValueError("minimum_data_quality must be in [0, 1]")
        if self.minimum_slope_z <= 0 or self.minimum_breakout_distance <= 0:
            raise ValueError("trend thresholds must be positive")
        if self.breakout_full_strength < self.minimum_breakout_distance:
            raise ValueError("breakout_full_strength must exceed the entry threshold")
        if self.minimum_confirmations < 1 or self.signal_valid_minutes < 1:
            raise ValueError("confirmation count and signal lifetime must be positive")
        if not 0 < self.suggested_risk_fraction <= 0.01:
            raise ValueError("suggested_risk_fraction must be in (0, 0.01]")


class TrendFollowingStrategy(BaseStrategy):
    name = StrategyName.TREND_FOLLOWING
    _snapshot_fields = (
        "ma_slope_z",
        "breakout_distance",
        "adx",
        "volume_zscore",
        "oi_zscore",
        "realized_volatility",
    )

    def __init__(self, config: TrendFollowingConfig | None = None) -> None:
        self.config = config or TrendFollowingConfig()

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        gate_open, gate_reasons = regime_gate(
            context.regime,
            allowed=frozenset((Regime.TREND_UP, Regime.TREND_DOWN)),
            blocked=frozenset(
                (Regime.UNKNOWN, Regime.RISK_OFF, Regime.FLASH_CRASH, Regime.LIQUIDATION_CASCADE)
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
        missing = missing_features(features, ("ma_slope_z", "breakout_distance"))
        if missing:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=(f"missing required features: {','.join(missing)}",),
            )

        regimes = active_regimes(context.regime)
        if {Regime.TREND_UP, Regime.TREND_DOWN} <= regimes:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=("contradictory TREND_UP and TREND_DOWN classifications",),
            )
        active_up = Regime.TREND_UP in regimes
        side = SignalSide.BUY if active_up else SignalSide.SELL
        direction = 1.0 if side == SignalSide.BUY else -1.0
        slope = features["ma_slope_z"] * direction
        breakout = features["breakout_distance"] * direction
        failures: list[str] = []
        if slope < self.config.minimum_slope_z:
            failures.append(
                f"directional ma_slope_z {slope:.3f} below {self.config.minimum_slope_z:.3f}"
            )
        if breakout < self.config.minimum_breakout_distance:
            failures.append(
                "directional breakout_distance "
                f"{breakout:.6f} below {self.config.minimum_breakout_distance:.6f}"
            )

        confirmations: list[str] = []
        if "adx" in features and features["adx"] >= self.config.minimum_adx:
            confirmations.append(f"ADX confirms trend ({features['adx']:.2f})")
        if "volume_zscore" in features and features["volume_zscore"] >= 0:
            volume_z = features["volume_zscore"]
            confirmations.append(f"volume is not below baseline (z={volume_z:.3f})")
        if "oi_zscore" in features and features["oi_zscore"] >= 0:
            confirmations.append(f"OI is not contracting (z={features['oi_zscore']:.3f})")
        if len(confirmations) < self.config.minimum_confirmations:
            failures.append(
                f"only {len(confirmations)} independent confirmations; "
                f"requires {self.config.minimum_confirmations}"
            )
        if failures:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=tuple(failures),
            )

        slope_score = min(slope / 2.326, 1.0)
        breakout_score = min(breakout / self.config.breakout_full_strength, 1.0)
        confirmation_score = min(len(confirmations) / 3, 1.0)
        setup_score = 0.45 * slope_score + 0.35 * breakout_score + 0.20 * confirmation_score
        evidence = (
            f"{context.regime.primary_regime.value} regime gate passed",
            f"directional ma_slope_z={slope:.3f}",
            f"directional breakout_distance={breakout:.6f}",
            *confirmations,
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
            reasons=("trend setup satisfied",),
        )
