from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime

from app.domain.regimes.models import RegimeResult
from app.domain.strategies.base import BaseStrategy, StrategyContext
from app.domain.strategies.models import StrategyBatchEvaluation, StrategyEvaluation
from app.domain.strategies.relative_strength import PairStatistics, RelativeStrengthStrategy


class StrategyEngine:
    """Runs independent strategies without hiding rejected or gated decisions."""

    def __init__(
        self,
        strategies: Iterable[BaseStrategy],
        relative_strength: RelativeStrengthStrategy | None = None,
    ) -> None:
        self._strategies = tuple(strategies)
        self._relative_strength = relative_strength
        names = [strategy.name for strategy in self._strategies]
        if len(set(names)) != len(names):
            raise ValueError("strategy names must be unique")

    def evaluate(self, context: StrategyContext) -> tuple[StrategyEvaluation, ...]:
        return tuple(strategy.evaluate(context) for strategy in self._strategies)

    def evaluate_relative_strength(
        self,
        timestamp: datetime,
        features_by_symbol: Mapping[str, Mapping[str, float | None]],
        regimes_by_symbol: Mapping[str, RegimeResult],
        pair_statistics: Mapping[tuple[str, str], PairStatistics],
        exchange_by_symbol: Mapping[str, str] | None = None,
    ) -> StrategyBatchEvaluation:
        if self._relative_strength is None:
            raise RuntimeError("relative-strength strategy is not configured")
        return self._relative_strength.evaluate_universe(
            timestamp,
            features_by_symbol,
            regimes_by_symbol,
            pair_statistics,
            exchange_by_symbol,
        )
