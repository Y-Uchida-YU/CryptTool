from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from math import isfinite

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.regimes.models import Regime, RegimeResult
from app.domain.strategies.base import (
    deterministic_signal_id,
    feature_snapshot,
    finite_features,
    regime_gate,
    setup_confidence,
)
from app.domain.strategies.models import (
    Signal,
    SignalSide,
    StrategyBatchEvaluation,
    StrategyName,
)


class PairStatistics(BaseModel):
    """Statistics known at `estimated_at`; spread is log(A) - beta * log(B)."""

    model_config = ConfigDict(frozen=True)

    symbol_a: str
    symbol_b: str
    estimated_at: datetime
    beta: float = Field(gt=0)
    correlation: float = Field(ge=-1, le=1)
    cointegration_pvalue: float = Field(ge=0, le=1)
    spread_zscore: float
    observations: int = Field(ge=30)

    @field_validator("estimated_at")
    @classmethod
    def estimated_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("estimated_at must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("beta", "correlation", "cointegration_pvalue", "spread_zscore")
    @classmethod
    def statistics_must_be_finite(cls, value: float) -> float:
        if not isfinite(value):
            raise ValueError("pair statistics must be finite")
        return value


@dataclass(frozen=True)
class RelativeStrengthConfig:
    minimum_regime_confidence: float = 0.60
    minimum_data_quality: float = 0.85
    minimum_symbols: int = 3
    minimum_score_gap: float = 0.50
    minimum_correlation: float = 0.50
    maximum_cointegration_pvalue: float = 0.10
    minimum_spread_zscore: float = 0.50
    maximum_pair_age_minutes: int = 1440
    minimum_beta: float = 0.25
    maximum_beta: float = 4.0
    vol_adjusted_return_weight: float = 0.45
    momentum_weight: float = 0.25
    drawdown_weight: float = 0.10
    liquidity_weight: float = 0.10
    funding_cost_weight: float = 0.10
    funding_cost_scale: float = 10_000.0
    signal_valid_minutes: int = 15
    suggested_pair_risk_fraction: float = 0.0025

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_regime_confidence <= 1:
            raise ValueError("minimum_regime_confidence must be in [0, 1]")
        if not 0 <= self.minimum_data_quality <= 1:
            raise ValueError("minimum_data_quality must be in [0, 1]")
        if self.minimum_symbols < 2:
            raise ValueError("relative strength requires at least two symbols")
        if self.minimum_score_gap <= 0 or self.minimum_correlation < 0:
            raise ValueError("pair thresholds must be positive")
        if self.minimum_beta <= 0 or self.maximum_beta <= self.minimum_beta:
            raise ValueError("beta bounds are invalid")
        weights = (
            self.vol_adjusted_return_weight,
            self.momentum_weight,
            self.drawdown_weight,
            self.liquidity_weight,
            self.funding_cost_weight,
        )
        if any(weight < 0 for weight in weights) or abs(sum(weights) - 1) > 1e-9:
            raise ValueError("relative-strength weights must be non-negative and sum to one")
        if self.maximum_pair_age_minutes < 1 or self.signal_valid_minutes < 1:
            raise ValueError("pair age and signal lifetime must be positive")
        if not 0 < self.suggested_pair_risk_fraction <= 0.005:
            raise ValueError("pair risk must remain conservative")


class RelativeStrengthStrategy:
    name = StrategyName.RELATIVE_STRENGTH
    _required_features = (
        "vol_adjusted_return",
        "momentum",
        "drawdown",
        "liquidity_score",
        "funding_cost",
    )

    def __init__(self, config: RelativeStrengthConfig | None = None) -> None:
        self.config = config or RelativeStrengthConfig()

    def evaluate_universe(
        self,
        timestamp: datetime,
        features_by_symbol: Mapping[str, Mapping[str, float | None]],
        regimes_by_symbol: Mapping[str, RegimeResult],
        pair_statistics: Mapping[tuple[str, str], PairStatistics],
        exchange_by_symbol: Mapping[str, str] | None = None,
    ) -> StrategyBatchEvaluation:
        if timestamp.tzinfo is None:
            raise ValueError("evaluation timestamp must be timezone-aware")
        timestamp = timestamp.astimezone(UTC)
        eligible: dict[str, dict[str, float]] = {}
        exclusions: list[str] = []
        allowed = frozenset(
            regime
            for regime in Regime
            if regime
            not in {
                Regime.UNKNOWN,
                Regime.RISK_OFF,
                Regime.FLASH_CRASH,
                Regime.LIQUIDATION_CASCADE,
            }
        )
        blocked = frozenset(
            (Regime.UNKNOWN, Regime.RISK_OFF, Regime.FLASH_CRASH, Regime.LIQUIDATION_CASCADE)
        )
        for symbol, raw_features in features_by_symbol.items():
            regime = regimes_by_symbol.get(symbol)
            if regime is None:
                exclusions.append(f"{symbol}: missing regime")
                continue
            gate_open, reasons = regime_gate(
                regime,
                allowed=allowed,
                blocked=blocked,
                minimum_confidence=self.config.minimum_regime_confidence,
                minimum_quality=self.config.minimum_data_quality,
            )
            if not gate_open:
                exclusions.append(f"{symbol}: {'; '.join(reasons)}")
                continue
            features = finite_features(raw_features)
            missing = tuple(name for name in self._required_features if name not in features)
            if missing:
                exclusions.append(f"{symbol}: missing {','.join(missing)}")
                continue
            liquidity = features["liquidity_score"]
            if not 0 <= liquidity <= 1:
                exclusions.append(f"{symbol}: liquidity_score outside [0,1]")
                continue
            eligible[symbol] = features

        if len(eligible) < self.config.minimum_symbols:
            reason = (
                f"only {len(eligible)} eligible symbols; requires {self.config.minimum_symbols}"
            )
            if exclusions:
                reason = f"{reason}; exclusions: {' | '.join(exclusions)}"
            return StrategyBatchEvaluation(
                timestamp=timestamp,
                strategy=self.name,
                gate_passed=False,
                reasons=(reason,),
            )

        scores = {symbol: self._score(features) for symbol, features in eligible.items()}
        long_symbol = max(scores, key=scores.__getitem__)
        short_symbol = min(scores, key=scores.__getitem__)
        score_gap = scores[long_symbol] - scores[short_symbol]
        failures: list[str] = []
        if score_gap < self.config.minimum_score_gap:
            failures.append(
                f"cross-sectional score gap {score_gap:.3f} below "
                f"{self.config.minimum_score_gap:.3f}"
            )
        statistics = pair_statistics.get((long_symbol, short_symbol))
        if statistics is None:
            failures.append(f"missing causal pair statistics for {long_symbol}/{short_symbol}")
        else:
            if (statistics.symbol_a, statistics.symbol_b) != (long_symbol, short_symbol):
                failures.append("pair-statistics symbols do not match the selected pair")
            if statistics.estimated_at > timestamp:
                failures.append("pair statistics are future-dated")
            age = timestamp - statistics.estimated_at
            if age > timedelta(minutes=self.config.maximum_pair_age_minutes):
                failures.append(f"pair statistics are stale ({age.total_seconds():.0f}s)")
            if statistics.correlation < self.config.minimum_correlation:
                failures.append(f"correlation {statistics.correlation:.3f} is too low")
            if statistics.cointegration_pvalue > self.config.maximum_cointegration_pvalue:
                failures.append(
                    f"cointegration p={statistics.cointegration_pvalue:.3f} exceeds limit"
                )
            if not self.config.minimum_beta <= statistics.beta <= self.config.maximum_beta:
                failures.append(f"beta {statistics.beta:.3f} is outside safe bounds")
            if statistics.spread_zscore < self.config.minimum_spread_zscore:
                failures.append(
                    f"pair spread z={statistics.spread_zscore:.3f} does not confirm ranking"
                )
        if failures or statistics is None:
            return StrategyBatchEvaluation(
                timestamp=timestamp,
                strategy=self.name,
                gate_passed=True,
                reasons=tuple(failures),
            )

        long_regime = regimes_by_symbol[long_symbol]
        short_regime = regimes_by_symbol[short_symbol]
        spread_score = min(
            statistics.spread_zscore / max(self.config.minimum_spread_zscore * 4, 1), 1.0
        )
        gap_score = min(score_gap / (self.config.minimum_score_gap * 3), 1.0)
        statistical_score = min(
            (statistics.correlation + (1 - statistics.cointegration_pvalue) + spread_score) / 3,
            1.0,
        )
        setup_score = 0.55 * gap_score + 0.45 * statistical_score
        group_id = sha256(
            f"{self.name.value}|{long_symbol}|{short_symbol}|{timestamp.isoformat()}".encode()
        ).hexdigest()
        long_weight = 1 / (1 + statistics.beta)
        short_weight = statistics.beta / (1 + statistics.beta)
        common_evidence = (
            f"score gap={score_gap:.3f} ({long_symbol} strong, {short_symbol} weak)",
            f"correlation={statistics.correlation:.3f}",
            f"beta={statistics.beta:.3f}",
            f"cointegration p={statistics.cointegration_pvalue:.3f}",
            f"pair spread z={statistics.spread_zscore:.3f}",
        )
        signals = (
            self._signal(
                timestamp,
                long_symbol,
                SignalSide.BUY,
                long_regime,
                features_by_symbol[long_symbol],
                setup_score,
                group_id,
                long_weight,
                common_evidence,
                exchange_by_symbol,
            ),
            self._signal(
                timestamp,
                short_symbol,
                SignalSide.SELL,
                short_regime,
                features_by_symbol[short_symbol],
                setup_score,
                group_id,
                short_weight,
                common_evidence,
                exchange_by_symbol,
            ),
        )
        return StrategyBatchEvaluation(
            timestamp=timestamp,
            strategy=self.name,
            gate_passed=True,
            signals=signals,
            reasons=("beta-aware relative-strength pair setup satisfied",),
        )

    def _score(self, features: Mapping[str, float]) -> float:
        return (
            self.config.vol_adjusted_return_weight * features["vol_adjusted_return"]
            + self.config.momentum_weight * features["momentum"]
            + self.config.drawdown_weight * features["drawdown"]
            + self.config.liquidity_weight * features["liquidity_score"]
            - self.config.funding_cost_weight
            * features["funding_cost"]
            * self.config.funding_cost_scale
        )

    def _signal(
        self,
        timestamp: datetime,
        symbol: str,
        side: SignalSide,
        regime: RegimeResult,
        raw_features: Mapping[str, float | None],
        setup_score: float,
        group_id: str,
        notional_weight: float,
        evidence: tuple[str, ...],
        exchange_by_symbol: Mapping[str, str] | None,
    ) -> Signal:
        return Signal(
            signal_id=deterministic_signal_id(
                self.name, symbol, timestamp, side, discriminator=group_id
            ),
            timestamp=timestamp,
            strategy=self.name,
            symbol=symbol,
            exchange=exchange_by_symbol.get(symbol) if exchange_by_symbol else None,
            side=side,
            strength=setup_score,
            confidence=setup_confidence(regime, setup_score),
            suggested_risk_fraction=self.config.suggested_pair_risk_fraction / 2,
            primary_regime=regime.primary_regime,
            secondary_regimes=regime.secondary_regimes,
            data_quality_score=regime.data_quality_score,
            valid_until=timestamp + timedelta(minutes=self.config.signal_valid_minutes),
            evidence=evidence,
            feature_snapshot=feature_snapshot(raw_features, self._required_features),
            metadata={
                "pair_group_id": group_id,
                "target_notional_weight": notional_weight,
                "confidence_semantics": "evidence_strength_not_win_probability",
            },
        )
