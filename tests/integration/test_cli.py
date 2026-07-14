import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from typer.testing import CliRunner

import app.cli.main as cli
from app.adapters.exchanges.websocket import ReconciliationState
from app.cli.main import app
from app.config.settings import Settings
from app.infrastructure.database.models import Base
from app.services.research.models import RawMarketEvent
from app.services.research.repository import PostgreSQLResearchRepository

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
            "generate-walk-forward-windows",
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


def test_collector_health_report_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = "collector-soak-test"
    monkeypatch.chdir(tmp_path)
    directory = Path("artifacts/collector-soak") / run_id
    directory.mkdir(parents=True)
    (directory / "health.json").write_text(
        json.dumps(
            {
                "live_execution": "OFF",
                "production_counts": {"hyperliquid": 10},
                "experimental_counts": {},
                "quarantine_count": 0,
                "health": {
                    "events_by_venue_type_instrument": {"hyperliquid:trade:BTC": 10},
                    "disconnect_count": 0,
                    "reconnect_count": 0,
                    "sequence_gaps": 0,
                    "snapshot_recoveries": 1,
                    "stale_duration_seconds": 0,
                    "checkpoint_lag_seconds": 0.1,
                    "duplicate_ratio": 0,
                    "memory_usage_bytes": 1024,
                    "task_count": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["generate-collector-health-report", "--run-id", run_id])
    assert result.exit_code == 0, result.output
    summary = (directory / "summary.md").read_text(encoding="utf-8")
    assert "Live execution: OFF" in summary
    assert "Checkpoint lag seconds: 0.1" in summary


def test_start_paper_operation_binds_r2_collector_and_r3_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'operation.db'}")
    Base.metadata.create_all(engine)
    research_repository = PostgreSQLResearchRepository(engine)
    for venue, payload in (
        (
            "hyperliquid",
            {"bids": [{"px": "99", "sz": "2"}], "asks": [{"px": "100", "sz": "3"}]},
        ),
        ("bitget", {"levels": [[["99", "2"]], [["100", "3"]]]}),
    ):
        raw_payload = json.dumps(payload)
        now = datetime.now(UTC)
        research_repository.add_raw_event(
            RawMarketEvent(
                event_id=f"{venue}-book",
                venue=venue,
                canonical_instrument_id="BTC",
                venue_symbol="BTC",
                event_type="orderbook_snapshot",
                exchange_timestamp=now,
                received_at=now,
                available_at=now,
                sequence=1,
                connection_id=uuid4(),
                reconciliation_state=ReconciliationState.SYNCHRONIZED,
                payload_sha256=hashlib.sha256(raw_payload.encode()).hexdigest(),
                raw_payload=raw_payload,
                normalizer_version="test",
                capability_verification_run_id="verified",
                created_at=now,
            )
        )
    config = tmp_path / "operation.yaml"
    config.write_text("continuous_paper: {enabled: true}\n", encoding="utf-8")
    token = tmp_path / "run.token"
    token.write_text("opaque", encoding="utf-8")
    configured = Settings(
        database_url="postgresql+psycopg://localhost/cryptbot",
        paper_trading=True,
        live_trading=False,
        paper={"enabled": True},
        live={"enabled": False},
        continuous_paper={"enabled": True, "observation_only": True},
        research_collection={
            "collection_enabled": True,
            "venues": ("hyperliquid", "bitget"),
            "instruments": ("BTC", "ETH", "SOL", "HYPE"),
            "event_types": ("orderbook_snapshot",),
            "require_complete_instrument_rules": False,
        },
    )

    class Adapter:
        def __init__(self, venue: str) -> None:
            self.venue = venue
            self.capabilities = ()

    captured: dict[str, object] = {}

    async def run_once(**kwargs: object) -> None:
        service = kwargs["service"]
        captured["service"] = service
        snapshot_id = service.snapshot_action(datetime.now(UTC))
        assert snapshot_id.startswith("snapshot-")
        captured["snapshot_id"] = snapshot_id

    class Capital:
        evidence_complete = True

        @staticmethod
        def feasible_at(_amount: object) -> bool:
            return True

    class Pipeline:
        def __init__(self, _repository: object) -> None:
            pass

        @staticmethod
        def run(config: dict[str, object]) -> SimpleNamespace:
            return SimpleNamespace(
                identity=SimpleNamespace(run_id=f"research-{config['strategy_id']}"),
                acceptance_result=SimpleNamespace(
                    verdict=SimpleNamespace(value="PASS"), capital_feasibility=Capital()
                ),
                data_quality=SimpleNamespace(passed=True),
                cost_stress_result=SimpleNamespace(evidence_complete=True),
                overfitting_result=SimpleNamespace(evidence_complete=True),
            )

    monkeypatch.setattr(cli, "_settings_from_yaml", lambda _: configured)
    monkeypatch.setattr(cli, "build_engine", lambda _: engine)
    monkeypatch.setattr(cli, "_research_data_adapter", lambda venue: Adapter(venue))
    monkeypatch.setattr(
        cli.TrustedCapabilityRegistry, "from_artifacts", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(cli, "_database_identity", lambda _: ("db", "main"))
    monkeypatch.setattr(cli, "_process_identity", lambda _: (datetime.now(UTC), "c" * 64))
    monkeypatch.setattr(cli, "_create_collector_run_token", lambda _: (token, "d" * 64))
    monkeypatch.setattr(cli, "_current_commit_sha", lambda: "a" * 40)
    monkeypatch.setattr(cli, "_run_continuous_operation", run_once)
    monkeypatch.setattr(cli, "ResearchPipeline", Pipeline)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        ["start-paper-operation", "--config", str(config), "--run-id", "operation-r3"],
    )

    assert result.exit_code == 0, result.output
    assert "live_execution=false" in result.output
    live_events = captured["service"].market_event_action()
    assert len(live_events) == 2
    assert all(item.reconciliation_state == "synchronized" for item in live_events)
    outcomes = captured["service"].research_action(captured["snapshot_id"])
    assert len(outcomes) == 3 and all(item.research_verdict == "PASS" for item in outcomes)
    assert not token.exists()


def test_yaml_settings_honor_runtime_database_and_live_safety_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "settings.yaml"
    config.write_text(
        "database_url: sqlite:///ignored.db\nlive_trading: false\nlive: {enabled: false}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("APP_DATABASE_URL", "postgresql+psycopg://runtime/cryptbot")
    monkeypatch.setenv("APP_LIVE_TRADING", "true")
    monkeypatch.setenv("APP_LIVE__ENABLED", "true")

    with pytest.raises(ValidationError, match="live mode requires"):
        cli._settings_from_yaml(config)
