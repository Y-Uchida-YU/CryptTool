"""Leakage-resistant validation and resampling utilities."""

from app.services.validation.overfitting import (
    DeflatedSharpeResult,
    ParameterStabilityResult,
    PBOResult,
    RealityCheckResult,
    analyze_parameter_stability,
    deflated_sharpe_ratio,
    probability_of_backtest_overfitting,
    whites_reality_check,
)
from app.services.validation.resampling import (
    DEFAULT_RANDOM_SEED,
    BootstrapResult,
    MonteCarloResult,
    block_bootstrap,
    bootstrap_metric,
    monte_carlo_paths,
)
from app.services.validation.splits import (
    DatasetSplit,
    PurgedFold,
    WalkForwardWindow,
    anchored_walk_forward,
    chronological_split,
    purged_kfold,
    rolling_walk_forward,
    walk_forward_splits,
)

__all__ = [
    "DEFAULT_RANDOM_SEED",
    "BootstrapResult",
    "DatasetSplit",
    "DeflatedSharpeResult",
    "MonteCarloResult",
    "PBOResult",
    "ParameterStabilityResult",
    "PurgedFold",
    "RealityCheckResult",
    "WalkForwardWindow",
    "analyze_parameter_stability",
    "anchored_walk_forward",
    "block_bootstrap",
    "bootstrap_metric",
    "chronological_split",
    "deflated_sharpe_ratio",
    "monte_carlo_paths",
    "probability_of_backtest_overfitting",
    "purged_kfold",
    "rolling_walk_forward",
    "walk_forward_splits",
    "whites_reality_check",
]
