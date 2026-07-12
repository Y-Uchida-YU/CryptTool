import json
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pandas as pd
import pytest

from app.services.reporting import (
    aggregate_trade_performance,
    calculate_performance_metrics,
    evaluate_acceptance,
    generate_report,
    monthly_returns,
    regime_distribution,
    regime_transition_matrix,
)


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "net_pnl": 8.0,
                "gross_pnl": 9.0,
                "fee": 0.5,
                "funding_pnl": -0.2,
                "slippage_cost": 0.2,
                "market_impact_cost": 0.1,
                "notional": 100.0,
                "holding_time_seconds": 60.0,
                "regime": "TREND_UP",
                "strategy": "trend",
                "symbol": "BTC",
            },
            {
                "net_pnl": -3.0,
                "gross_pnl": -2.0,
                "fee": 0.5,
                "funding_pnl": 0.1,
                "slippage_cost": 0.3,
                "market_impact_cost": 0.2,
                "notional": 80.0,
                "holding_time_seconds": 120.0,
                "regime": "RANGE",
                "strategy": "mean_reversion",
                "symbol": "ETH",
            },
        ]
    )


def test_performance_metrics_include_risk_trade_and_cost_measures() -> None:
    equity = pd.Series([100.0, 110.0, 105.0, 120.0])
    metrics = calculate_performance_metrics(
        equity,
        trades=_trades(),
        exposure_series=[0.0, 0.5, 1.0, 0.0],
        periods_per_year=365,
    )

    assert metrics.total_return == pytest.approx(0.2)
    assert metrics.maximum_drawdown == pytest.approx(1 - 105 / 110)
    assert metrics.profit_factor == pytest.approx(8 / 3)
    assert metrics.win_rate == 0.5
    assert metrics.payoff_ratio == pytest.approx(8 / 3)
    assert metrics.average_holding_time_seconds == 90
    assert metrics.fee_cost == 1
    assert metrics.funding_pnl == pytest.approx(-0.1)
    assert metrics.slippage_cost == pytest.approx(0.5)
    assert metrics.market_impact_cost == pytest.approx(0.3)
    assert metrics.exposure == pytest.approx(0.375)
    assert metrics.time_in_market == 0.5
    assert metrics.consecutive_wins == 1
    assert metrics.consecutive_losses == 1
    assert metrics.value_at_risk >= 0
    assert metrics.conditional_value_at_risk >= metrics.value_at_risk


def test_missing_costs_are_unavailable_not_zero() -> None:
    metrics = calculate_performance_metrics(
        [100.0, 101.0, 100.5],
        trades=pd.DataFrame({"pnl": [1.0, -0.5]}),
    )
    assert metrics.fee_cost is None
    assert metrics.funding_pnl is None
    assert metrics.slippage_cost is None
    assert metrics.market_impact_cost is None

    no_trades = calculate_performance_metrics([100.0, 0.0, 0.0])
    assert no_trades.cagr == -1
    assert no_trades.trade_count == 0
    assert no_trades.profit_factor is None


def test_metrics_support_timestamp_holding_and_undefined_ratios() -> None:
    trades = pd.DataFrame(
        {
            "realized_pnl": [1.0, 2.0],
            "entry_time": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
            "exit_time": ["2024-01-01T00:01:00Z", "2024-01-02T00:02:00Z"],
        }
    )
    equity = pd.Series(
        [100.0, 100.0, 100.0],
        index=pd.date_range("2024-01-01", periods=3, freq="1D", tz="UTC"),
    )
    metrics = calculate_performance_metrics(equity, returns=[0.0], trades=trades)
    assert metrics.average_holding_time_seconds == 90
    assert metrics.profit_factor is None
    assert metrics.payoff_ratio is None
    assert metrics.annualized_volatility is None
    assert metrics.sharpe_ratio is None
    assert metrics.sortino_ratio is None
    assert metrics.calmar_ratio is None
    assert metrics.recovery_factor is None


def test_metrics_reject_corrupt_equity_returns_trades_and_exposure() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        calculate_performance_metrics([])
    with pytest.raises(ValueError, match="at least two"):
        calculate_performance_metrics([100])
    with pytest.raises(ValueError, match="cannot recover"):
        calculate_performance_metrics([100, 0, 1])
    with pytest.raises(ValueError, match="below -100%"):
        calculate_performance_metrics([100, 90], returns=[-1.1])
    with pytest.raises(ValueError, match="periods_per_year"):
        calculate_performance_metrics([100, 90], periods_per_year=0)
    with pytest.raises(ValueError, match="risk_free"):
        calculate_performance_metrics([100, 90], annual_risk_free_rate=float("nan"))
    with pytest.raises(ValueError, match="var_confidence"):
        calculate_performance_metrics([100, 90], var_confidence=0.5)
    with pytest.raises(ValueError, match="timezone-aware"):
        calculate_performance_metrics(
            pd.Series([100, 101], index=pd.to_datetime(["2024-01-01", "2024-01-02"]))
        )
    with pytest.raises(ValueError, match="one of"):
        calculate_performance_metrics([100, 101], trades=pd.DataFrame({"fee": [1]}))
    with pytest.raises(ValueError, match="finite numeric"):
        calculate_performance_metrics([100, 101], trades=pd.DataFrame({"pnl": [float("nan")]}))
    with pytest.raises(ValueError, match="duration"):
        calculate_performance_metrics(
            [100, 101],
            trades=pd.DataFrame({"pnl": [1], "holding_time_seconds": [-1]}),
        )
    with pytest.raises(ValueError, match="valid timestamps"):
        calculate_performance_metrics(
            [100, 101],
            trades=pd.DataFrame({"pnl": [1], "entry_time": ["bad"], "exit_time": ["bad"]}),
        )
    with pytest.raises(ValueError, match="cannot precede"):
        calculate_performance_metrics(
            [100, 101],
            trades=pd.DataFrame(
                {
                    "pnl": [1],
                    "entry_time": ["2024-01-02T00:00:00Z"],
                    "exit_time": ["2024-01-01T00:00:00Z"],
                }
            ),
        )
    with pytest.raises(ValueError, match="non-negative"):
        calculate_performance_metrics([100, 101], exposure_series=[0, -0.1])


def test_monthly_returns_and_transition_matrix_use_true_chronology() -> None:
    equity = pd.Series(
        [100.0, 110.0, 121.0],
        index=pd.to_datetime(["2024-01-01", "2024-01-31", "2024-02-29"], utc=True),
    )
    monthly = monthly_returns(equity)
    assert monthly["return"].tolist() == pytest.approx([0.1, 0.1])

    matrix = regime_transition_matrix(["UP", "UP", "RANGE", "RANGE", "UP"])
    assert matrix.loc["UP", "UP"] == pytest.approx(0.5)
    assert matrix.loc["UP", "RANGE"] == pytest.approx(0.5)
    assert matrix.loc["RANGE", "RANGE"] == pytest.approx(0.5)
    assert matrix.loc["RANGE", "UP"] == pytest.approx(0.5)
    counts = regime_transition_matrix(["UP", "RANGE", "UP"], normalize=False)
    assert counts.loc["UP", "RANGE"] == 1


def test_calendar_and_regime_summaries_validate_missing_or_unordered_data() -> None:
    with pytest.raises(ValueError, match="DatetimeIndex"):
        monthly_returns(pd.Series([100, 101]))
    with pytest.raises(ValueError, match="timezone-aware"):
        monthly_returns(pd.Series([100, 101], index=pd.date_range("2024-01-01", periods=2)))
    with pytest.raises(ValueError, match="unique and chronological"):
        monthly_returns(
            pd.Series(
                [100, 101],
                index=pd.to_datetime(["2024-01-02", "2024-01-01"], utc=True),
            )
        )
    with pytest.raises(ValueError, match="non-empty"):
        regime_distribution([])
    with pytest.raises(ValueError, match="missing"):
        regime_transition_matrix(["UP", None])  # type: ignore[list-item]
    distribution = regime_distribution(["UNKNOWN", "UP", "UNKNOWN"])
    assert distribution.set_index("regime").loc["UNKNOWN", "count"] == 2


def test_group_aggregation_retains_losing_groups() -> None:
    tables = aggregate_trade_performance(_trades())
    regimes = tables["regime"].set_index("regime")

    assert regimes.loc["TREND_UP", "total_pnl"] == 8
    assert regimes.loc["RANGE", "total_pnl"] == -3
    assert set(tables) == {"regime", "strategy", "symbol"}
    missing = aggregate_trade_performance(pd.DataFrame({"pnl": [1.0]}))
    assert all(table.empty for table in missing.values())
    empty = aggregate_trade_performance(pd.DataFrame(), dimensions=("symbol",))
    assert empty["symbol"].empty


def test_acceptance_never_promotes_missing_or_failed_evidence() -> None:
    missing = evaluate_acceptance(oos_return=0.1)
    assert missing.overall == "INSUFFICIENT_EVIDENCE"

    failed = evaluate_acceptance(
        oos_return=-0.01,
        walk_forward_returns=[0.1, -0.1, -0.2],
    )
    assert failed.overall == "FAIL"

    passed = evaluate_acceptance(
        oos_return=0.1,
        walk_forward_returns=[0.1, 0.2, -0.01],
        fee_stress_return=0.03,
        slippage_stress_return=0.01,
        leave_one_period_out_returns=[0.05, 0.04, 0.02],
        symbol_returns={"BTC": 0.05, "ETH": 0.04, "SOL": -0.01},
        maximum_drawdown=0.07,
        parameters_stable=True,
        ruin_probability=0.001,
        minimum_order_feasible=True,
        pbo=0.2,
        deflated_sharpe_passes=True,
    )
    assert passed.overall == "PASS"


def test_acceptance_validates_limits_and_symbol_evidence() -> None:
    with pytest.raises(ValueError, match="drawdown_limit"):
        evaluate_acceptance(maximum_drawdown_limit=1)
    with pytest.raises(ValueError, match="ruin_probability"):
        evaluate_acceptance(maximum_ruin_probability=1)
    with pytest.raises(ValueError, match="walk_forward"):
        evaluate_acceptance(walk_forward_returns=[])
    with pytest.raises(ValueError, match="leave_one"):
        evaluate_acceptance(leave_one_period_out_returns=[])
    with pytest.raises(ValueError, match="at least two"):
        evaluate_acceptance(symbol_returns={"BTC": 0.1})


def test_generate_report_writes_full_markdown_csv_and_json(tmp_path: Path) -> None:
    equity = pd.Series(
        [100.0, 110.0, 105.0, 108.0],
        index=pd.date_range("2024-01-01", periods=4, freq="15D", tz="UTC"),
        name="equity",
    )
    artifacts = generate_report(
        tmp_path,
        equity_curve=equity,
        trades=_trades(),
        regimes=["TREND_UP", "TREND_UP", "RANGE", "RANGE"],
        metadata={"model_version": "test-1"},
        walk_forward_results=pd.DataFrame({"window": [0, 1], "return": [0.1, -0.2]}),
        parameter_sensitivity=pd.DataFrame({"lookback": [10, 20], "score": [0.1, -0.1]}),
        monte_carlo_summary={"ruin_probability": 0.2, "seed": 1729},
    )

    assert artifacts.markdown.exists() and artifacts.json.exists()
    assert {
        "trades",
        "equity_curve",
        "drawdown_curve",
        "monthly_returns",
        "regime_performance",
        "strategy_performance",
        "symbol_performance",
        "parameter_sensitivity",
        "walk_forward_results",
    } <= set(artifacts.csv)
    payload = json.loads(artifacts.json.read_text(encoding="utf-8"))
    regime_rows = payload["tables"]["regime_performance"]
    assert any(row["regime"] == "RANGE" and row["total_pnl"] < 0 for row in regime_rows)
    assert payload["acceptance"]["overall"] == "INSUFFICIENT_EVIDENCE"
    assert "Losing regimes: RANGE" in artifacts.markdown.read_text(encoding="utf-8")


class _MetadataState(StrEnum):
    COMPLETE = "complete"


def test_generate_report_handles_no_trades_extra_tables_and_json_types(tmp_path: Path) -> None:
    assessment = evaluate_acceptance(oos_return=-0.1)
    equity = pd.Series([100.0, 99.0, 98.0], name="equity")
    artifacts = generate_report(
        tmp_path,
        equity_curve=equity,
        assessment=assessment,
        metadata={
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "state": _MetadataState.COMPLETE,
            "not_finite": float("nan"),
            "path": Path("research"),
        },
        extra_tables={"all_trials": pd.DataFrame({"score": [1.0, -2.0]})},
    )
    payload = json.loads(artifacts.json.read_text(encoding="utf-8"))
    assert payload["acceptance"]["overall"] == "FAIL"
    assert payload["metadata"]["not_finite"] is None
    assert payload["metadata"]["state"] == "complete"
    assert artifacts.csv["all_trials"].exists()
    markdown = artifacts.markdown.read_text(encoding="utf-8")
    assert "Execution-cost fields were unavailable" in markdown
    assert "Funding PnL was unavailable" in markdown

    with pytest.raises(ValueError, match="unsafe"):
        generate_report(
            tmp_path / "unsafe",
            equity_curve=equity,
            extra_tables={"../escape": pd.DataFrame()},
        )
    with pytest.raises(ValueError, match="overwrite"):
        generate_report(
            tmp_path / "collision",
            equity_curve=equity,
            extra_tables={"trades": pd.DataFrame()},
        )
