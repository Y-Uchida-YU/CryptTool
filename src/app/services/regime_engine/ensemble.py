from datetime import UTC, datetime

from app.domain.regimes.models import Regime, RegimeResult
from app.services.regime_engine.rules import DeterministicRuleEngine
from app.services.regime_engine.statistical import StatisticalPrediction


class EnsembleRegimeEngine:
    def __init__(
        self, rules: DeterministicRuleEngine, model_version: str = "rules-1.0+gmm-1.0"
    ) -> None:
        self.rules, self.model_version = rules, model_version
        self._last_primary = Regime.UNKNOWN
        self._started_at: datetime | None = None

    def detect(
        self,
        features: dict[str, float | None],
        timestamp: datetime,
        data_quality_score: float,
        statistical: StatisticalPrediction | None = None,
    ) -> RegimeResult:
        timestamp = timestamp.astimezone(UTC)
        evidence = sorted(
            self.rules.evaluate(features, timestamp), key=lambda item: item.score, reverse=True
        )
        reasons: tuple[str, ...]
        if data_quality_score < self.rules.settings.minimum_quality or not evidence:
            primary, confidence, reasons = (
                Regime.UNKNOWN,
                0.0,
                ("insufficient data quality or evidence",),
            )
            secondary: tuple[Regime, ...] = ()
        else:
            primary = evidence[0].regime
            secondary = tuple(dict.fromkeys(item.regime for item in evidence[1:]))
            rule_confidence = sum(item.score for item in evidence[:3]) / min(3, len(evidence))
            if statistical is None:
                confidence = rule_confidence * data_quality_score * 0.8
            else:
                confidence = (
                    0.7 * rule_confidence + 0.3 * statistical.probability
                ) * data_quality_score
            confidence = min(1.0, max(0.0, confidence))
            reasons = tuple(item.reason for item in evidence)
        if primary != self._last_primary or self._started_at is None:
            self._started_at = timestamp
            self._last_primary = primary
        duration = max(0.0, (timestamp - self._started_at).total_seconds())
        return RegimeResult(
            primary_regime=primary,
            secondary_regimes=secondary,
            confidence=confidence,
            evidence=reasons,
            feature_snapshot=features,
            regime_started_at=self._started_at,
            regime_duration_seconds=duration,
            model_version=self.model_version,
            data_quality_score=data_quality_score,
            metadata={"statistical_state": statistical.state if statistical else None},
        )
