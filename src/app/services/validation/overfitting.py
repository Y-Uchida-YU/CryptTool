"""Diagnostics for selection bias, multiple trials and parameter cliffs."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations
from math import e, sqrt

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from scipy.stats import kurtosis, norm, skew  # type: ignore[import-untyped]

from app.services.validation.resampling import DEFAULT_RANDOM_SEED

FloatArray = NDArray[np.float64]


@dataclass(frozen=True)
class PBOResult:
    """Combinatorially-symmetric cross-validation PBO approximation."""

    probability: float
    logits: tuple[float, ...]
    selected_configuration_indices: tuple[int, ...]
    median_out_of_sample_percentile: float
    combinations_evaluated: int
    partitions: int
    seed: int


@dataclass(frozen=True)
class DeflatedSharpeResult:
    """Sharpe ratio after accounting for non-normality and multiple trials."""

    observed_sharpe: float
    expected_maximum_sharpe: float
    deflated_sharpe: float
    probability_sharpe_is_genuine: float
    skewness: float
    kurtosis: float
    trials: int
    observations: int
    passes: bool


@dataclass(frozen=True)
class ParameterStabilityResult:
    """Local plateau diagnostics around one explicitly identified parameter set."""

    selected_parameters: dict[str, float]
    selected_score: float
    rank_percentile: float
    neighbor_count: int
    neighbor_median_score: float | None
    neighbor_worst_score: float | None
    stability_score: float
    cliff_fraction: float
    is_stable: bool
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class RealityCheckResult:
    """Block-bootstrap family-wise test for the best mean return."""

    observed_best_mean: float
    p_value: float
    best_configuration_index: int
    bootstrap_maxima: FloatArray
    block_size: int
    seed: int


def _finite_matrix(values: Sequence[Sequence[float]] | FloatArray, name: str) -> FloatArray:
    matrix = np.asarray(values, dtype=np.float64)
    if matrix.ndim != 2 or min(matrix.shape) < 2 or not np.isfinite(matrix).all():
        raise ValueError(f"{name} must be a finite two-dimensional matrix with both dimensions >=2")
    return matrix


def _sharpe(values: FloatArray) -> float:
    standard_deviation = float(np.std(values, ddof=1))
    mean = float(np.mean(values))
    if standard_deviation <= np.finfo(np.float64).eps:
        if abs(mean) <= np.finfo(np.float64).eps:
            return 0.0
        return float(np.sign(mean) / np.finfo(np.float64).eps)
    return mean / standard_deviation


def probability_of_backtest_overfitting(
    strategy_returns: Sequence[Sequence[float]] | FloatArray,
    *,
    partitions: int = 8,
    max_combinations: int = 2_000,
    seed: int = DEFAULT_RANDOM_SEED,
) -> PBOResult:
    """Estimate PBO with combinatorially symmetric cross-validation (CSCV).

    Rows are chronological observations and columns are parameter configurations.
    The metric selects on one half of time blocks and ranks the winner on the
    complement. A result above 0.5 is conventionally concerning, but it is not a
    universal pass/fail threshold and must be reported with the trial design.
    """

    matrix = _finite_matrix(strategy_returns, "strategy_returns")
    if (
        isinstance(partitions, bool)
        or not isinstance(partitions, int)
        or partitions < 4
        or partitions % 2
    ):
        raise ValueError("partitions must be an even integer of at least four")
    if partitions > matrix.shape[0]:
        raise ValueError("partitions cannot exceed the observation count")
    if (
        isinstance(max_combinations, bool)
        or not isinstance(max_combinations, int)
        or max_combinations < 1
    ):
        raise ValueError("max_combinations must be a positive integer")
    blocks = tuple(
        np.asarray(block, dtype=np.int64)
        for block in np.array_split(np.arange(matrix.shape[0]), partitions)
    )
    choices = list(combinations(range(partitions), partitions // 2))
    # A train/test complement is the same experiment in reverse. Retaining one
    # representative avoids presenting the effective sample count as twice its size.
    choices = [choice for choice in choices if 0 in choice]
    if len(choices) > max_combinations:
        generator = np.random.default_rng(seed)
        selected_choices = np.sort(generator.choice(len(choices), max_combinations, replace=False))
        choices = [choices[int(index)] for index in selected_choices]

    logits: list[float] = []
    selected_indices: list[int] = []
    all_blocks = set(range(partitions))
    configuration_count = matrix.shape[1]
    for choice in choices:
        training_rows = np.concatenate([blocks[index] for index in choice])
        testing_rows = np.concatenate([blocks[index] for index in sorted(all_blocks - set(choice))])
        train_scores = np.asarray(
            [_sharpe(matrix[training_rows, column]) for column in range(configuration_count)]
        )
        winner = int(np.argmax(train_scores))
        test_scores = np.asarray(
            [_sharpe(matrix[testing_rows, column]) for column in range(configuration_count)]
        )
        selected_score = test_scores[winner]
        less = int(np.count_nonzero(test_scores < selected_score))
        equal = int(np.count_nonzero(test_scores == selected_score))
        percentile = (less + 0.5 * equal) / configuration_count
        percentile = float(np.clip(percentile, 1e-12, 1 - 1e-12))
        logits.append(float(np.log(percentile / (1 - percentile))))
        selected_indices.append(winner)
    logit_array = np.asarray(logits, dtype=np.float64)
    percentiles = 1 / (1 + np.exp(-logit_array))
    return PBOResult(
        probability=float(np.mean(logit_array <= 0)),
        logits=tuple(logits),
        selected_configuration_indices=tuple(selected_indices),
        median_out_of_sample_percentile=float(np.median(percentiles)),
        combinations_evaluated=len(choices),
        partitions=partitions,
        seed=seed,
    )


def deflated_sharpe_ratio(
    returns: Sequence[float] | FloatArray,
    *,
    trials: int,
    periods_per_year: float = 365.0,
    confidence_level: float = 0.95,
) -> DeflatedSharpeResult:
    """Calculate the probabilistic Deflated Sharpe Ratio.

    ``trials`` must include every tried configuration, not only retained results.
    The calculation uses the non-normal standard error proposed by Bailey and
    López de Prado and an expected maximum Sharpe under multiple independent
    trials. Correlated trials make the effective count uncertain, so this result
    should be treated as a conservative diagnostic rather than proof of an edge.
    """

    values = np.asarray(returns, dtype=np.float64)
    if values.ndim != 1 or values.size < 3 or not np.isfinite(values).all():
        raise ValueError("returns must contain at least three finite observations")
    if isinstance(trials, bool) or not isinstance(trials, int) or trials < 1:
        raise ValueError("trials must be a positive integer")
    if not np.isfinite(periods_per_year) or periods_per_year <= 0:
        raise ValueError("periods_per_year must be finite and positive")
    if not 0 < confidence_level < 1:
        raise ValueError("confidence_level must be between zero and one")
    standard_deviation = float(np.std(values, ddof=1))
    if standard_deviation <= np.finfo(np.float64).eps:
        raise ValueError("Sharpe ratio is undefined for zero-variance returns")
    period_sharpe = float(np.mean(values)) / standard_deviation
    sample_skew = float(skew(values, bias=False))
    sample_kurtosis = float(kurtosis(values, fisher=False, bias=False))
    if not np.isfinite(sample_skew):
        sample_skew = 0.0
    if not np.isfinite(sample_kurtosis):
        sample_kurtosis = 3.0
    variance_numerator = (
        1 - sample_skew * period_sharpe + (sample_kurtosis - 1) * period_sharpe**2 / 4
    )
    sharpe_standard_error = sqrt(max(variance_numerator / (values.size - 1), 1e-18))
    if trials == 1:
        expected_maximum = 0.0
    else:
        euler_gamma = 0.5772156649015329
        expected_standard_normal_maximum = (1 - euler_gamma) * norm.ppf(
            1 - 1 / trials
        ) + euler_gamma * norm.ppf(1 - 1 / (trials * e))
        expected_maximum = float(sharpe_standard_error * expected_standard_normal_maximum)
    probability = float(norm.cdf((period_sharpe - expected_maximum) / sharpe_standard_error))
    annualizer = sqrt(periods_per_year)
    return DeflatedSharpeResult(
        observed_sharpe=period_sharpe * annualizer,
        expected_maximum_sharpe=expected_maximum * annualizer,
        deflated_sharpe=(period_sharpe - expected_maximum) * annualizer,
        probability_sharpe_is_genuine=probability,
        skewness=sample_skew,
        kurtosis=sample_kurtosis,
        trials=trials,
        observations=int(values.size),
        passes=probability >= confidence_level,
    )


def analyze_parameter_stability(
    results: pd.DataFrame,
    *,
    parameter_columns: Sequence[str],
    score_column: str,
    selected_parameters: Mapping[str, float] | None = None,
    neighborhood_radius: float = 0.35,
) -> ParameterStabilityResult:
    """Assess whether a score lies on a local plateau instead of an isolated peak.

    Distance is Euclidean after each parameter is scaled by its tested range. If
    ``selected_parameters`` is omitted the highest row is inspected and a warning
    records that this was an in-sample selection.
    """

    if results.empty:
        raise ValueError("results cannot be empty")
    columns = tuple(parameter_columns)
    if not columns or len(set(columns)) != len(columns):
        raise ValueError("parameter_columns must be unique and non-empty")
    missing = [name for name in (*columns, score_column) if name not in results.columns]
    if missing:
        raise ValueError(f"missing result columns: {', '.join(missing)}")
    if not np.isfinite(neighborhood_radius) or neighborhood_radius <= 0:
        raise ValueError("neighborhood_radius must be finite and positive")
    numeric = results.loc[:, [*columns, score_column]].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy(dtype=np.float64)).all():
        raise ValueError("parameter and score columns must contain only finite numeric values")

    warnings: list[str] = []
    if selected_parameters is None:
        selected_position = int(np.argmax(numeric[score_column].to_numpy(dtype=np.float64)))
        selected_row = numeric.iloc[selected_position]
        warnings.append("selected_parameters omitted; inspected the in-sample maximum")
    else:
        if set(selected_parameters) != set(columns):
            raise ValueError("selected_parameters keys must exactly match parameter_columns")
        target = np.asarray([selected_parameters[name] for name in columns], dtype=np.float64)
        if not np.isfinite(target).all():
            raise ValueError("selected_parameters must be finite")
        candidates = numeric.loc[:, list(columns)].to_numpy(dtype=np.float64)
        exact = np.all(np.isclose(candidates, target, rtol=1e-12, atol=1e-12), axis=1)
        matches = np.flatnonzero(exact)
        if matches.size != 1:
            raise ValueError("selected_parameters must identify exactly one tested row")
        selected_position = int(matches[0])
        selected_row = numeric.iloc[selected_position]

    parameter_values = numeric.loc[:, list(columns)].to_numpy(dtype=np.float64)
    ranges = np.ptp(parameter_values, axis=0)
    fixed_parameters = ranges <= np.finfo(np.float64).eps
    if np.any(fixed_parameters):
        names = [columns[index] for index in np.flatnonzero(fixed_parameters)]
        warnings.append(f"parameters were not varied: {', '.join(names)}")
    safe_ranges = np.where(fixed_parameters, 1.0, ranges)
    selected_vector = parameter_values[selected_position]
    distances = np.sqrt(np.sum(((parameter_values - selected_vector) / safe_ranges) ** 2, axis=1))
    neighbor_mask = (distances > 1e-12) & (distances <= neighborhood_radius)
    neighbor_scores = numeric.loc[neighbor_mask, score_column].to_numpy(dtype=np.float64)
    selected_score = float(selected_row[score_column])
    all_scores = numeric[score_column].to_numpy(dtype=np.float64)
    rank_percentile = float(
        (
            np.count_nonzero(all_scores < selected_score)
            + 0.5 * np.count_nonzero(all_scores == selected_score)
        )
        / all_scores.size
    )
    if not neighbor_scores.size:
        warnings.append("no tested neighboring parameter sets were inside the requested radius")
        return ParameterStabilityResult(
            selected_parameters={name: float(selected_row[name]) for name in columns},
            selected_score=selected_score,
            rank_percentile=rank_percentile,
            neighbor_count=0,
            neighbor_median_score=None,
            neighbor_worst_score=None,
            stability_score=0.0,
            cliff_fraction=1.0,
            is_stable=False,
            warnings=tuple(warnings),
        )

    score_scale = max(
        abs(selected_score),
        float(np.subtract(*np.quantile(all_scores, [0.75, 0.25]))),
        1e-12,
    )
    relative_deterioration = np.maximum(0.0, selected_score - neighbor_scores) / score_scale
    stability_score = float(np.exp(-np.median(relative_deterioration)))
    cliff_fraction = float(np.mean(relative_deterioration > 0.5))
    if neighbor_scores.size < 2:
        warnings.append("fewer than two neighbors make the plateau assessment weak")
    is_stable = bool(
        neighbor_scores.size >= 2 and stability_score >= 0.7 and cliff_fraction <= 0.25
    )
    if not is_stable:
        warnings.append("the selected parameters do not satisfy the conservative plateau rule")
    return ParameterStabilityResult(
        selected_parameters={name: float(selected_row[name]) for name in columns},
        selected_score=selected_score,
        rank_percentile=rank_percentile,
        neighbor_count=int(neighbor_scores.size),
        neighbor_median_score=float(np.median(neighbor_scores)),
        neighbor_worst_score=float(np.min(neighbor_scores)),
        stability_score=stability_score,
        cliff_fraction=cliff_fraction,
        is_stable=is_stable,
        warnings=tuple(warnings),
    )


def whites_reality_check(
    strategy_returns: Sequence[Sequence[float]] | FloatArray,
    *,
    n_resamples: int = 2_000,
    block_size: int | None = None,
    seed: int = DEFAULT_RANDOM_SEED,
) -> RealityCheckResult:
    """Test the best strategy mean against a centered block-bootstrap null."""

    matrix = _finite_matrix(strategy_returns, "strategy_returns")
    if isinstance(n_resamples, bool) or not isinstance(n_resamples, int) or n_resamples < 1:
        raise ValueError("n_resamples must be a positive integer")
    observations = matrix.shape[0]
    resolved_block = max(1, round(observations ** (1 / 3))) if block_size is None else block_size
    if (
        isinstance(resolved_block, bool)
        or not isinstance(resolved_block, int)
        or not 1 <= resolved_block <= observations
    ):
        raise ValueError("block_size must be between one and the observation count")
    means = np.mean(matrix, axis=0)
    best_index = int(np.argmax(means))
    observed_best = float(means[best_index])
    centered = matrix - means
    generator = np.random.default_rng(seed)
    maxima = np.empty(n_resamples, dtype=np.float64)
    block_count = int(np.ceil(observations / resolved_block))
    offsets = np.arange(resolved_block, dtype=np.int64)
    for resample in range(n_resamples):
        starts = generator.integers(0, observations, size=block_count)
        indices = ((starts[:, None] + offsets[None, :]) % observations).ravel()[:observations]
        maxima[resample] = float(np.max(np.mean(centered[indices], axis=0)))
    p_value = (int(np.count_nonzero(maxima >= observed_best)) + 1) / (n_resamples + 1)
    return RealityCheckResult(
        observed_best_mean=observed_best,
        p_value=float(p_value),
        best_configuration_index=best_index,
        bootstrap_maxima=maxima,
        block_size=resolved_block,
        seed=seed,
    )
