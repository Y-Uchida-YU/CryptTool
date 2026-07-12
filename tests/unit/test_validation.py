import numpy as np
import pandas as pd
import pytest

from app.services.validation import (
    analyze_parameter_stability,
    anchored_walk_forward,
    block_bootstrap,
    bootstrap_metric,
    chronological_split,
    deflated_sharpe_ratio,
    monte_carlo_paths,
    probability_of_backtest_overfitting,
    purged_kfold,
    rolling_walk_forward,
    walk_forward_splits,
    whites_reality_check,
)


def test_chronological_split_applies_purge_and_embargo_without_reassignment() -> None:
    split = chronological_split(
        100,
        train_fraction=0.6,
        validation_fraction=0.2,
        purge_size=2,
        embargo_size=1,
    )

    assert split.train[[0, -1]].tolist() == [0, 57]
    assert split.validation[[0, -1]].tolist() == [61, 77]
    assert split.out_of_sample[[0, -1]].tolist() == [81, 99]
    used = np.concatenate([split.train, split.validation, split.out_of_sample])
    assert len(used) == len(np.unique(used))
    assert {58, 59, 60, 78, 79, 80}.isdisjoint(used)


def test_purged_kfold_excludes_label_overlap_and_embargo() -> None:
    folds = purged_kfold(20, n_splits=4, purge_size=2, embargo_size=2)
    second = folds[1]

    assert second.test.tolist() == [5, 6, 7, 8, 9]
    assert second.purged.tolist() == [3, 4]
    assert second.embargoed.tolist() == [10, 11]
    assert not set(range(3, 12)).intersection(second.train)


def test_rolling_and_anchored_walk_forward_have_expected_training_history() -> None:
    rolling = rolling_walk_forward(
        60,
        train_size=20,
        validation_size=5,
        out_of_sample_size=5,
        step_size=5,
        purge_size=1,
        embargo_size=1,
    )
    anchored = anchored_walk_forward(
        60,
        train_size=20,
        validation_size=5,
        out_of_sample_size=5,
        step_size=5,
        purge_size=1,
        embargo_size=1,
    )

    assert rolling[1].train[[0, -1]].tolist() == [5, 23]
    assert anchored[1].train[[0, -1]].tolist() == [0, 23]
    assert rolling[0].validation.tolist() == [21, 22, 23, 24]
    assert rolling[0].out_of_sample.tolist() == [27, 28, 29, 30, 31]
    assert all(window.out_of_sample.max() < 60 for window in rolling)

    without_validation = walk_forward_splits(
        30,
        train_size=10,
        validation_size=0,
        out_of_sample_size=5,
        embargo_size=1,
    )
    assert without_validation[0].validation.size == 0
    assert without_validation[0].out_of_sample[0] == 11


def test_split_configuration_rejects_empty_or_unsafe_windows() -> None:
    with pytest.raises(ValueError, match="at least three"):
        chronological_split(2)
    with pytest.raises(ValueError, match="train_fraction"):
        chronological_split(10, train_fraction=0)
    with pytest.raises(ValueError, match="out-of-sample"):
        chronological_split(10, train_fraction=0.8, validation_fraction=0.2)
    with pytest.raises(ValueError, match="non-negative"):
        chronological_split(10, purge_size=-1)
    with pytest.raises(ValueError, match="empty chronological"):
        chronological_split(10, purge_size=5)
    with pytest.raises(ValueError, match="at least two"):
        purged_kfold(1)
    with pytest.raises(ValueError, match="n_splits"):
        purged_kfold(10, n_splits=11)
    with pytest.raises(ValueError, match="empty training"):
        purged_kfold(4, n_splits=2, purge_size=4, embargo_size=4)
    with pytest.raises(ValueError, match="positive integer"):
        walk_forward_splits(0, train_size=2, validation_size=1, out_of_sample_size=1)
    with pytest.raises(ValueError, match="train_size"):
        walk_forward_splits(10, train_size=0, validation_size=1, out_of_sample_size=1)
    with pytest.raises(ValueError, match="step_size"):
        walk_forward_splits(
            10,
            train_size=3,
            validation_size=1,
            out_of_sample_size=1,
            step_size=0,
        )
    with pytest.raises(ValueError, match="leave non-empty"):
        walk_forward_splits(
            10,
            train_size=3,
            validation_size=1,
            out_of_sample_size=1,
            purge_size=1,
        )
    with pytest.raises(ValueError, match="not enough"):
        walk_forward_splits(5, train_size=4, validation_size=1, out_of_sample_size=1)


def test_seeded_block_bootstrap_and_monte_carlo_are_reproducible() -> None:
    returns = np.array([0.01, -0.02, 0.03, -0.01, 0.005])
    first = block_bootstrap(returns, n_resamples=20, block_size=2, seed=9)
    second = block_bootstrap(returns, n_resamples=20, block_size=2, seed=9)
    assert np.array_equal(first, second)

    one = monte_carlo_paths(
        returns,
        initial_capital=100,
        n_simulations=50,
        horizon=10,
        block_size=2,
        seed=11,
    )
    two = monte_carlo_paths(
        returns,
        initial_capital=100,
        n_simulations=50,
        horizon=10,
        block_size=2,
        seed=11,
    )
    assert np.array_equal(one.paths, two.paths)
    assert one.paths.shape == (50, 11)
    assert 0 <= one.ruin_probability <= 1
    assert one.to_dict()["seed"] == 11
    assert len(one.to_dict(include_paths=True)["paths"]) == 50

    bankruptcies = monte_carlo_paths(
        [-1.0, 0.5],
        initial_capital=100,
        n_simulations=20,
        horizon=6,
        block_size=1,
        seed=8,
    )
    for path in bankruptcies.paths:
        zeros = np.flatnonzero(path == 0)
        if zeros.size:
            assert np.all(path[zeros[0] :] == 0)


def test_bootstrap_reports_uncertainty_and_rejects_invalid_returns() -> None:
    result = bootstrap_metric(
        [0.01, 0.02, -0.01, 0.03, -0.02],
        n_resamples=100,
        block_size=2,
        seed=5,
    )
    assert result.observed == pytest.approx(0.006)
    assert result.confidence_interval[0] <= result.confidence_interval[1]
    assert 0 < result.p_value_against_zero <= 1
    with pytest.raises(ValueError, match="below -100%"):
        monte_carlo_paths([-1.01, 0.1], initial_capital=100)


def test_resampling_validates_sample_metric_and_simulation_controls() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        block_bootstrap([])
    with pytest.raises(ValueError, match="n_resamples"):
        block_bootstrap([0.1], n_resamples=0)
    with pytest.raises(ValueError, match="sample_size"):
        block_bootstrap([0.1], sample_size=0)
    with pytest.raises(ValueError, match="block_size"):
        block_bootstrap([0.1], block_size=2)
    with pytest.raises(ValueError, match="confidence_level"):
        bootstrap_metric([0.1, 0.2], confidence_level=1)
    with pytest.raises(ValueError, match="observed"):
        bootstrap_metric([0.1, 0.2], lambda _: float("nan"), n_resamples=2)

    calls = 0

    def finite_once(_: np.ndarray) -> float:
        nonlocal calls
        calls += 1
        return 0.0 if calls == 1 else float("nan")

    with pytest.raises(ValueError, match="bootstrap value"):
        bootstrap_metric([0.1, 0.2], finite_once, n_resamples=2)
    single = bootstrap_metric([0.1, 0.2], n_resamples=1)
    assert single.standard_error == 0
    with pytest.raises(ValueError, match="initial_capital"):
        monte_carlo_paths([0.1], initial_capital=0)
    with pytest.raises(ValueError, match="n_simulations"):
        monte_carlo_paths([0.1], initial_capital=100, n_simulations=0)
    with pytest.raises(ValueError, match="horizon"):
        monte_carlo_paths([0.1], initial_capital=100, horizon=0)
    with pytest.raises(ValueError, match="ruin_fraction"):
        monte_carlo_paths([0.1], initial_capital=100, ruin_fraction=1)


def test_pbo_deflated_sharpe_and_reality_check_disclose_selection_uncertainty() -> None:
    generator = np.random.default_rng(22)
    configurations = generator.normal(0.0002, 0.01, size=(120, 6))
    first = probability_of_backtest_overfitting(configurations, partitions=6, seed=3)
    second = probability_of_backtest_overfitting(configurations, partitions=6, seed=3)
    assert first == second
    assert 0 <= first.probability <= 1
    assert first.combinations_evaluated == 10

    candidate = generator.normal(0.001, 0.01, size=200)
    single = deflated_sharpe_ratio(candidate, trials=1)
    searched = deflated_sharpe_ratio(candidate, trials=100)
    assert searched.expected_maximum_sharpe > single.expected_maximum_sharpe
    assert searched.probability_sharpe_is_genuine < single.probability_sharpe_is_genuine

    reality = whites_reality_check(configurations, n_resamples=100, block_size=3, seed=4)
    assert 0 < reality.p_value <= 1
    assert reality.bootstrap_maxima.shape == (100,)

    limited = probability_of_backtest_overfitting(
        configurations,
        partitions=8,
        max_combinations=3,
        seed=7,
    )
    assert limited.combinations_evaluated == 3


def test_overfitting_diagnostics_reject_invalid_experiment_designs() -> None:
    matrix = np.ones((8, 2))
    with pytest.raises(ValueError, match="two-dimensional"):
        probability_of_backtest_overfitting([[1.0]])
    with pytest.raises(ValueError, match="even integer"):
        probability_of_backtest_overfitting(matrix, partitions=3)
    with pytest.raises(ValueError, match="observation count"):
        probability_of_backtest_overfitting(matrix, partitions=10)
    with pytest.raises(ValueError, match="max_combinations"):
        probability_of_backtest_overfitting(matrix, partitions=4, max_combinations=0)

    for kwargs in (
        {"trials": 0},
        {"trials": 1, "periods_per_year": 0},
        {"trials": 1, "confidence_level": 1},
    ):
        with pytest.raises(ValueError):
            deflated_sharpe_ratio([0.1, -0.1, 0.2], **kwargs)
    with pytest.raises(ValueError, match="at least three"):
        deflated_sharpe_ratio([0.1, 0.2], trials=1)
    with pytest.raises(ValueError, match="zero-variance"):
        deflated_sharpe_ratio([0.1, 0.1, 0.1], trials=1)
    with pytest.raises(ValueError, match="n_resamples"):
        whites_reality_check(matrix, n_resamples=0)
    with pytest.raises(ValueError, match="block_size"):
        whites_reality_check(matrix, block_size=9)


def test_parameter_stability_distinguishes_plateau_from_isolated_peak() -> None:
    plateau = pd.DataFrame(
        {
            "lookback": [10, 10, 20, 20, 30, 30],
            "threshold": [1.0, 2.0, 1.0, 2.0, 1.0, 2.0],
            "score": [0.90, 0.91, 0.92, 0.90, 0.89, 0.88],
        }
    )
    stable = analyze_parameter_stability(
        plateau,
        parameter_columns=["lookback", "threshold"],
        score_column="score",
        selected_parameters={"lookback": 20, "threshold": 1.0},
        neighborhood_radius=0.8,
    )
    assert stable.neighbor_count >= 2
    assert stable.is_stable

    cliff = plateau.copy()
    cliff.loc[:, "score"] = [-1.0, -1.0, 2.0, -1.0, -1.0, -1.0]
    unstable = analyze_parameter_stability(
        cliff,
        parameter_columns=["lookback", "threshold"],
        score_column="score",
        selected_parameters={"lookback": 20, "threshold": 1.0},
        neighborhood_radius=0.8,
    )
    assert not unstable.is_stable
    assert unstable.cliff_fraction > 0.25


def test_parameter_stability_reports_weak_search_design_and_invalid_tables() -> None:
    fixed = pd.DataFrame({"lookback": [10, 10], "score": [0.1, 0.2]})
    weak = analyze_parameter_stability(
        fixed,
        parameter_columns=["lookback"],
        score_column="score",
    )
    assert not weak.is_stable
    assert weak.neighbor_count == 0
    assert any("not varied" in warning for warning in weak.warnings)
    assert any("in-sample maximum" in warning for warning in weak.warnings)

    one_neighbor = analyze_parameter_stability(
        pd.DataFrame({"lookback": [10, 20], "score": [0.2, 0.19]}),
        parameter_columns=["lookback"],
        score_column="score",
        selected_parameters={"lookback": 10},
        neighborhood_radius=1.1,
    )
    assert one_neighbor.neighbor_count == 1
    assert any("fewer than two" in warning for warning in one_neighbor.warnings)

    with pytest.raises(ValueError, match="empty"):
        analyze_parameter_stability(pd.DataFrame(), parameter_columns=["x"], score_column="score")
    with pytest.raises(ValueError, match="unique"):
        analyze_parameter_stability(fixed, parameter_columns=[], score_column="score")
    with pytest.raises(ValueError, match="missing"):
        analyze_parameter_stability(fixed, parameter_columns=["missing"], score_column="score")
    with pytest.raises(ValueError, match="radius"):
        analyze_parameter_stability(
            fixed,
            parameter_columns=["lookback"],
            score_column="score",
            neighborhood_radius=0,
        )
    with pytest.raises(ValueError, match="finite numeric"):
        analyze_parameter_stability(
            pd.DataFrame({"lookback": [10, "bad"], "score": [0.1, 0.2]}),
            parameter_columns=["lookback"],
            score_column="score",
        )
    with pytest.raises(ValueError, match="keys"):
        analyze_parameter_stability(
            fixed,
            parameter_columns=["lookback"],
            score_column="score",
            selected_parameters={"wrong": 10},
        )
    with pytest.raises(ValueError, match="identify exactly one"):
        analyze_parameter_stability(
            fixed,
            parameter_columns=["lookback"],
            score_column="score",
            selected_parameters={"lookback": 20},
        )
