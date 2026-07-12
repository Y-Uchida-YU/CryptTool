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
class FundingExtremeConfig:
    minimum_regime_confidence: float = 0.65
    minimum_data_quality: float = 0.85
    minimum_abs_funding_z: float = 2.326
    minimum_crowding_confirmations: int = 3
    minimum_timing_confirmations: int = 2
    signal_valid_minutes: int = 5
    suggested_risk_fraction: float = 0.0015

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_regime_confidence <= 1:
            raise ValueError("minimum_regime_confidence must be in [0, 1]")
        if not 0 <= self.minimum_data_quality <= 1:
            raise ValueError("minimum_data_quality must be in [0, 1]")
        if self.minimum_abs_funding_z < 1.5:
            raise ValueError("funding extreme threshold is too permissive")
        if self.minimum_crowding_confirmations < 2 or self.minimum_timing_confirmations < 2:
            raise ValueError("funding strategy needs multiple independent confirmations")
        if self.signal_valid_minutes < 1 or not 0 < self.suggested_risk_fraction <= 0.0025:
            raise ValueError("funding strategy risk and lifetime must remain conservative")


class FundingExtremeStrategy(BaseStrategy):
    name = StrategyName.FUNDING_EXTREME
    _snapshot_fields = (
        "funding_rate",
        "predicted_funding_rate",
        "funding_zscore",
        "funding_percentile",
        "funding_momentum",
        "ma_slope_z",
        "oi_zscore",
        "basis_zscore",
        "basis_momentum",
        "spot_perp_premium_zscore",
        "cvd_momentum",
        "liquidation_zscore",
        "long_short_ratio_zscore",
    )

    def __init__(self, config: FundingExtremeConfig | None = None) -> None:
        self.config = config or FundingExtremeConfig()

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        gate_open, gate_reasons = regime_gate(
            context.regime,
            allowed=frozenset((Regime.FUNDING_EXTREME_POSITIVE, Regime.FUNDING_EXTREME_NEGATIVE)),
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
        missing = missing_features(
            features, ("funding_rate", "predicted_funding_rate", "funding_zscore")
        )
        if missing:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=(f"missing funding timing features: {','.join(missing)}",),
            )

        regimes = active_regimes(context.regime)
        if {
            Regime.FUNDING_EXTREME_POSITIVE,
            Regime.FUNDING_EXTREME_NEGATIVE,
        } <= regimes:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=("contradictory positive and negative funding regimes",),
            )
        positive = Regime.FUNDING_EXTREME_POSITIVE in regimes
        crowd_sign = 1.0 if positive else -1.0
        side = SignalSide.SELL if positive else SignalSide.BUY
        failures: list[str] = []
        funding_z = features["funding_zscore"] * crowd_sign
        if funding_z < self.config.minimum_abs_funding_z:
            failures.append(f"directional funding_zscore={funding_z:.3f} is not extreme")
        if features["funding_rate"] * crowd_sign <= 0:
            failures.append("current funding sign conflicts with the regime")
        if features["predicted_funding_rate"] * crowd_sign <= 0:
            failures.append("predicted funding does not confirm persistence")

        crowding: list[str] = []
        percentile = features.get("funding_percentile")
        if percentile is not None and (
            (positive and percentile >= 0.98) or (not positive and percentile <= 0.02)
        ):
            crowding.append(f"funding percentile is extreme ({percentile:.3f})")
        for name, threshold in (
            ("basis_zscore", 1.0),
            ("spot_perp_premium_zscore", 1.0),
            ("long_short_ratio_zscore", 1.0),
            ("oi_zscore", 1.0),
        ):
            value = features.get(name)
            if value is not None and value * crowd_sign >= threshold:
                crowding.append(f"{name} confirms crowding ({value:.3f})")
        if len(crowding) < self.config.minimum_crowding_confirmations:
            failures.append(
                f"only {len(crowding)} crowding confirmations; requires "
                f"{self.config.minimum_crowding_confirmations}"
            )

        timing: list[str] = []
        slope = features.get("ma_slope_z")
        if slope is not None and slope * crowd_sign <= 0.50:
            timing.append(f"crowded-side price trend has faded ({slope:.3f})")
        oi_z = features.get("oi_zscore")
        if oi_z is not None and oi_z <= -0.5:
            timing.append(f"OI is contracting ({oi_z:.3f})")
        cvd = features.get("cvd_momentum")
        if cvd is not None and cvd * crowd_sign < 0:
            timing.append(f"CVD opposes crowded side ({cvd:.3f})")
        basis_momentum = features.get("basis_momentum")
        if basis_momentum is not None and basis_momentum * crowd_sign < 0:
            timing.append(f"basis is reversing ({basis_momentum:.6f})")
        funding_momentum = features.get("funding_momentum")
        if funding_momentum is not None and funding_momentum * crowd_sign < 0:
            timing.append(f"funding momentum is reversing ({funding_momentum:.6f})")
        liquidation_z = features.get("liquidation_zscore")
        if liquidation_z is not None and liquidation_z >= 2.0:
            timing.append(f"liquidations indicate unwind ({liquidation_z:.3f})")
        if len(timing) < self.config.minimum_timing_confirmations:
            failures.append(
                f"only {len(timing)} reversal timing confirmations; requires "
                f"{self.config.minimum_timing_confirmations}"
            )
        if failures:
            return StrategyEvaluation(
                timestamp=context.timestamp,
                strategy=self.name,
                symbol=context.symbol,
                gate_passed=True,
                reasons=tuple(failures),
            )

        extremity_score = min(funding_z / 3.5, 1.0)
        crowding_score = min(len(crowding) / 4, 1.0)
        timing_score = min(len(timing) / 4, 1.0)
        setup_score = 0.30 * extremity_score + 0.35 * crowding_score + 0.35 * timing_score
        evidence = (
            f"{context.regime.primary_regime.value} regime gate passed",
            f"current and predicted funding signs agree; directional z={funding_z:.3f}",
            *crowding,
            *timing,
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
            reasons=("funding crowding and reversal timing setup satisfied",),
        )
