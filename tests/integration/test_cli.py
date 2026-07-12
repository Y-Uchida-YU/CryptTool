import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from app.cli.main import app

FIXTURES = Path(__file__).parents[1] / "fixtures"
runner = CliRunner()


def test_config_backfill_and_health_commands(tmp_path: Path) -> None:
    environment = {"APP_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'health.db'}"}
    assert runner.invoke(app, ["validate-config"], env=environment).exit_code == 0
    backfill = runner.invoke(app, ["backfill"], env=environment)
    assert backfill.exit_code == 2 and "no market-data adapter" in backfill.output
    assert runner.invoke(app, ["health-check"], env=environment).exit_code == 0
    preflight = runner.invoke(app, ["live-preflight"], env=environment)
    assert preflight.exit_code == 2
    assert "LIVE EXECUTION REFUSED" in preflight.output
    assert "FAIL concrete_adapter" in preflight.output


def test_backtest_walk_forward_monte_carlo_and_paper_cli(tmp_path: Path) -> None:
    backtest_output = tmp_path / "backtest.json"
    backtest = runner.invoke(
        app,
        [
            "run-backtest",
            str(FIXTURES / "backtest_events.json"),
            "--output-path",
            str(backtest_output),
        ],
    )
    assert backtest.exit_code == 0, backtest.output
    payload = json.loads(backtest_output.read_text())
    assert len(payload["fills"]) == 2
    assert payload["fills"][0]["filled_at"] > payload["fills"][0]["submitted_at"]

    walk = runner.invoke(
        app,
        [
            "run-walk-forward",
            "--observations",
            "100",
            "--train",
            "40",
            "--validation",
            "10",
            "--test",
            "10",
            "--purge",
            "2",
            "--embargo",
            "2",
            "--output-path",
            str(tmp_path / "walk.json"),
        ],
    )
    assert walk.exit_code == 0, walk.output
    assert "rolling=" in walk.output

    monte = runner.invoke(
        app,
        [
            "run-monte-carlo",
            str(FIXTURES / "net_returns.csv"),
            "--simulations",
            "100",
            "--output-path",
            str(tmp_path / "monte.json"),
        ],
    )
    assert monte.exit_code == 0, monte.output
    assert json.loads((tmp_path / "monte.json").read_text())["simulation_count"] == 100

    paper = runner.invoke(
        app,
        ["paper-trade", str(FIXTURES / "paper_quotes.csv"), "--quantity", "0.1"],
    )
    assert paper.exit_code == 0 and "live_orders=0" in paper.output


def test_feature_regime_validation_and_report_cli(tmp_path: Path) -> None:
    rows = 80
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=rows, freq="min", tz="UTC"),
            "exchange": "simulation",
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "open": [100 + index for index in range(rows)],
            "high": [101 + index for index in range(rows)],
            "low": [99 + index for index in range(rows)],
            "close": [100.5 + index for index in range(rows)],
            "volume": [10 + index for index in range(rows)],
        }
    )
    market_path = tmp_path / "market.csv"
    frame.to_csv(market_path, index=False)
    assert runner.invoke(app, ["validate-data", str(market_path)]).exit_code == 0

    features_path = tmp_path / "features.csv"
    built = runner.invoke(
        app,
        ["build-features", str(market_path), "--output-path", str(features_path), "--window", "10"],
    )
    assert built.exit_code == 0, built.output
    regime_path = tmp_path / "regime.json"
    detected = runner.invoke(
        app,
        ["detect-regimes", str(features_path), "--output-path", str(regime_path)],
    )
    assert detected.exit_code == 0, detected.output
    assert "primary_regime" in json.loads(regime_path.read_text())

    report_directory = tmp_path / "report"
    report = runner.invoke(
        app,
        [
            "generate-report",
            str(FIXTURES / "equity.csv"),
            "--trades-path",
            str(FIXTURES / "trades.csv"),
            "--output-directory",
            str(report_directory),
        ],
    )
    assert report.exit_code == 0, report.output
    assert "INSUFFICIENT_EVIDENCE" in report.output
    assert (report_directory / "report.md").exists()
