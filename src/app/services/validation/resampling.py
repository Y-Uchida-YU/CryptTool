"""Seeded bootstrap and Monte Carlo procedures for dependent return series."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray

DEFAULT_RANDOM_SEED = 1729
FloatArray = NDArray[np.float64]
Metric = Callable[[FloatArray], float]


@dataclass(frozen=True)
class BootstrapResult:
    """Observed statistic and its empirical block-bootstrap distribution."""

    observed: float
    standard_error: float
    confidence_interval: tuple[float, float]
    p_value_against_zero: float
    distribution: FloatArray
    block_size: int
    seed: int


@dataclass(frozen=True)
class MonteCarloResult:
    """Compounded equity paths and deliberately unfiltered loss diagnostics."""

    paths: FloatArray
    terminal_equity_percentiles: dict[str, float]
    max_drawdown_percentiles: dict[str, float]
    ruin_probability: float
    probability_of_loss: float
    initial_capital: float
    ruin_equity: float
    block_size: int
    seed: int

    def to_dict(self, *, include_paths: bool = False) -> dict[str, Any]:
        """Return a JSON-ready summary; paths are opt-in because they may be large."""

        result: dict[str, Any] = {
            "terminal_equity_percentiles": self.terminal_equity_percentiles,
            "max_drawdown_percentiles": self.max_drawdown_percentiles,
            "ruin_probability": self.ruin_probability,
            "probability_of_loss": self.probability_of_loss,
            "initial_capital": self.initial_capital,
            "ruin_equity": self.ruin_equity,
            "block_size": self.block_size,
            "seed": self.seed,
            "simulation_count": int(self.paths.shape[0]),
            "horizon": int(self.paths.shape[1] - 1),
        }
        if include_paths:
            result["paths"] = self.paths.tolist()
        return result


def _as_finite_vector(values: Sequence[float] | FloatArray, name: str) -> FloatArray:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or not array.size or not np.isfinite(array).all():
        raise ValueError(f"{name} must be a non-empty one-dimensional finite sequence")
    return array


def _resolve_block_size(length: int, block_size: int | None) -> int:
    resolved = max(1, round(length ** (1 / 3))) if block_size is None else block_size
    if isinstance(resolved, bool) or not isinstance(resolved, int) or not 1 <= resolved <= length:
        raise ValueError("block_size must be between one and the number of observations")
    return resolved


def _circular_block_sample(
    values: FloatArray,
    horizon: int,
    block_size: int,
    generator: np.random.Generator,
) -> FloatArray:
    block_count = int(np.ceil(horizon / block_size))
    starts = generator.integers(0, values.size, size=block_count)
    offsets = np.arange(block_size, dtype=np.int64)
    indices = (starts[:, None] + offsets[None, :]) % values.size
    return np.asarray(values[indices.ravel()[:horizon]], dtype=np.float64)


def block_bootstrap(
    values: Sequence[float] | FloatArray,
    *,
    n_resamples: int = 2_000,
    sample_size: int | None = None,
    block_size: int | None = None,
    seed: int = DEFAULT_RANDOM_SEED,
) -> FloatArray:
    """Draw circular moving-block samples while preserving short-run dependence."""

    array = _as_finite_vector(values, "values")
    if isinstance(n_resamples, bool) or not isinstance(n_resamples, int) or n_resamples < 1:
        raise ValueError("n_resamples must be a positive integer")
    horizon = array.size if sample_size is None else sample_size
    if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 1:
        raise ValueError("sample_size must be a positive integer")
    resolved_block = _resolve_block_size(array.size, block_size)
    generator = np.random.default_rng(seed)
    samples = np.empty((n_resamples, horizon), dtype=np.float64)
    for row in range(n_resamples):
        samples[row] = _circular_block_sample(array, horizon, resolved_block, generator)
    return samples


def bootstrap_metric(
    values: Sequence[float] | FloatArray,
    metric: Metric | None = None,
    *,
    n_resamples: int = 2_000,
    block_size: int | None = None,
    confidence_level: float = 0.95,
    seed: int = DEFAULT_RANDOM_SEED,
) -> BootstrapResult:
    """Estimate uncertainty for a statistic using a seeded block bootstrap.

    The p-value uses a centered bootstrap null distribution. It is diagnostic,
    not permission to tune repeatedly until significance is obtained.
    """

    if not 0 < confidence_level < 1:
        raise ValueError("confidence_level must be between zero and one")
    array = _as_finite_vector(values, "values")
    resolved_block = _resolve_block_size(array.size, block_size)
    function: Metric = (lambda sample: float(np.mean(sample))) if metric is None else metric
    observed = float(function(array))
    if not np.isfinite(observed):
        raise ValueError("metric returned a non-finite observed value")
    samples = block_bootstrap(
        array,
        n_resamples=n_resamples,
        block_size=resolved_block,
        seed=seed,
    )
    distribution = np.asarray([function(sample) for sample in samples], dtype=np.float64)
    if not np.isfinite(distribution).all():
        raise ValueError("metric returned a non-finite bootstrap value")
    alpha = 1 - confidence_level
    lower, upper = np.quantile(distribution, [alpha / 2, 1 - alpha / 2])
    centered = distribution - observed
    exceedances = int(np.count_nonzero(np.abs(centered) >= abs(observed)))
    p_value = (exceedances + 1) / (n_resamples + 1)
    return BootstrapResult(
        observed=observed,
        standard_error=float(np.std(distribution, ddof=1)) if n_resamples > 1 else 0.0,
        confidence_interval=(float(lower), float(upper)),
        p_value_against_zero=float(p_value),
        distribution=distribution,
        block_size=resolved_block,
        seed=seed,
    )


def monte_carlo_paths(
    returns: Sequence[float] | FloatArray,
    *,
    initial_capital: float,
    n_simulations: int = 5_000,
    horizon: int | None = None,
    block_size: int | None = None,
    ruin_fraction: float = 0.5,
    seed: int = DEFAULT_RANDOM_SEED,
) -> MonteCarloResult:
    """Resample return blocks and compound full paths, costs already included.

    Callers must pass net returns after fees, funding, slippage and market impact.
    Returns below -100% are rejected rather than silently repaired. Equity remains
    zero after bankruptcy so impossible recoveries cannot improve the result.
    """

    array = _as_finite_vector(returns, "returns")
    if np.any(array < -1):
        raise ValueError("simple returns cannot be below -100%")
    if not np.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("initial_capital must be finite and positive")
    if isinstance(n_simulations, bool) or not isinstance(n_simulations, int) or n_simulations < 1:
        raise ValueError("n_simulations must be a positive integer")
    simulation_horizon = array.size if horizon is None else horizon
    if (
        isinstance(simulation_horizon, bool)
        or not isinstance(simulation_horizon, int)
        or simulation_horizon < 1
    ):
        raise ValueError("horizon must be a positive integer")
    if not 0 < ruin_fraction < 1:
        raise ValueError("ruin_fraction must be between zero and one")
    resolved_block = _resolve_block_size(array.size, block_size)
    sampled = block_bootstrap(
        array,
        n_resamples=n_simulations,
        sample_size=simulation_horizon,
        block_size=resolved_block,
        seed=seed,
    )
    paths = np.empty((n_simulations, simulation_horizon + 1), dtype=np.float64)
    paths[:, 0] = initial_capital
    paths[:, 1:] = initial_capital * np.cumprod(1 + sampled, axis=1)
    paths = np.maximum(paths, 0.0)
    running_peaks = np.maximum.accumulate(paths, axis=1)
    drawdowns = np.divide(
        running_peaks - paths,
        running_peaks,
        out=np.zeros_like(paths),
        where=running_peaks > 0,
    )
    max_drawdowns = np.max(drawdowns, axis=1)
    terminal = paths[:, -1]
    ruin_equity = initial_capital * ruin_fraction
    ruined = np.any(paths <= ruin_equity, axis=1)
    terminal_quantiles = np.quantile(terminal, [0.01, 0.05, 0.5, 0.95, 0.99])
    drawdown_quantiles = np.quantile(max_drawdowns, [0.5, 0.9, 0.95, 0.99])
    return MonteCarloResult(
        paths=paths,
        terminal_equity_percentiles={
            "p01": float(terminal_quantiles[0]),
            "p05": float(terminal_quantiles[1]),
            "p50": float(terminal_quantiles[2]),
            "p95": float(terminal_quantiles[3]),
            "p99": float(terminal_quantiles[4]),
        },
        max_drawdown_percentiles={
            "p50": float(drawdown_quantiles[0]),
            "p90": float(drawdown_quantiles[1]),
            "p95": float(drawdown_quantiles[2]),
            "p99": float(drawdown_quantiles[3]),
        },
        ruin_probability=float(np.mean(ruined)),
        probability_of_loss=float(np.mean(terminal < initial_capital)),
        initial_capital=float(initial_capital),
        ruin_equity=float(ruin_equity),
        block_size=resolved_block,
        seed=seed,
    )
