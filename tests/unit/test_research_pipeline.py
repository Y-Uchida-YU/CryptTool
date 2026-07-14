from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from typer.testing import CliRunner

from app.cli.main import app
from app.infrastructure.database.models import Base
from app.services.research.data_operations import DataSnapshotService
from app.services.research.dataset import PointInTimeDatasetBuilder, raw_event_from_dict
from app.services.research.models import FrozenHypothesis, InstrumentRuleSnapshot
from app.services.research.pipeline import (
    FrozenHypothesisRegistry,
    ResearchPipeline,
    evaluate_acceptance,
)
from app.services.research.report import ResearchArtifactWriter
from app.services.research.repository import (
    InMemoryResearchRepository,
    PostgreSQLResearchRepository,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)
RUNNER = CliRunner()


def raw_event(
    event_id: str,
    *,
    venue: str = "hyperliquid",
    instrument: str = "BTC-USD-PERP",
    event_type: str = "orderbook_snapshot",
    available_at: datetime = BASE,
    exchange_timestamp: datetime | None = None,
    payload: object | None = None,
) -> dict[str, object]:
    raw_payload = payload or {
        "bid": "100",
        "ask": "100.2",
        "bid_depth": "10",
        "ask_depth": "10",
        "last_price": "100.1",
        "trade_quantity": "10",
    }
    return {
        "event_id": event_id,
        "venue": venue,
        "canonical_instrument_id": instrument,
        "venue_symbol": "SOLUSDT" if instrument.startswith("SOL") else "BTCUSDT",
        "event_type": event_type,
        "reconciliation_state": ("synchronized" if event_type.startswith("orderbook_") else None),
        "exchange_timestamp": (exchange_timestamp or available_at).isoformat(),
        "received_at": available_at.isoformat(),
        "available_at": available_at.isoformat(),
        "created_at": available_at.isoformat(),
        "raw_payload": raw_payload,
    }


def pipeline_config(*, minimum_notional: str = "5", maker: bool = False) -> dict[str, object]:
    events: list[dict[str, object]] = []
    for index in range(60):
        timestamp = BASE + timedelta(minutes=index)
        wave = Decimal(index % 7) / Decimal("10")
        for venue in ("hyperliquid", "bitget"):
            offset = (
                Decimal(0)
                if venue == "hyperliquid"
                else Decimal("3") - Decimal(index) * Decimal("0.02")
            )
            mid = Decimal("100") + Decimal(index) / Decimal("10") + wave + offset
            events.append(
                raw_event(
                    f"{venue}-{index}",
                    venue=venue,
                    available_at=timestamp,
                    payload={
                        "bid": str(mid - Decimal("0.1")),
                        "ask": str(mid + Decimal("0.1")),
                        "bid_depth": "20",
                        "ask_depth": "20",
                        "last_price": str(mid + (Decimal("0.2") if index % 2 else Decimal("-0.2"))),
                        "trade_quantity": "20",
                    },
                )
            )
    return {
        "commit_sha": "9ba27ffe7305f2bdf35c37e88162d7fb58f2372f",
        "data_snapshot_id": "snapshot-r1-sample",
        "hypothesis_version": "basis-r1-v1",
        "strategy_id": "cross_venue_basis",
        "strategy_version": "1",
        "created_at": (BASE + timedelta(hours=2)).isoformat(),
        "cutoff_at": (BASE + timedelta(hours=2)).isoformat(),
        "venues": ["hyperliquid", "bitget"],
        "instruments": ["BTC-USD-PERP"],
        "event_types": ["orderbook_snapshot"],
        "raw_events": events,
        "live_execution": False,
        "hypothesis": {
            "parameter_grid": {"threshold": [0, "0.5", "1.0"]},
            "primary_metric": "net_pnl",
            "secondary_metrics": ["sharpe", "drawdown"],
            "acceptance_thresholds": {},
            "frozen_at": (BASE - timedelta(days=1)).isoformat(),
        },
        "walk_forward": {
            "mode": "rolling",
            "train_size": 30,
            "validation_size": 10,
            "oos_size": 10,
            "purge": 2,
            "embargo": 2,
        },
        "minimum_coverage": "1",
        "maximum_stale_ratio": "1",
        "order_quantity": "0.1",
        "initial_cash": "1000",
        "use_maker_orders": maker,
        "instrument_rules": {
            "default": {
                "tick_size": "0.01",
                "lot_size": "0.001",
                "minimum_notional": minimum_notional,
                "maker_fee_rate": "-0.0001",
                "taker_fee_rate": "0.0006",
            }
        },
        "bootstrap_trials": 20,
        "monte_carlo_trials": 100,
        "random_seed": 17,
    }


def run_pipeline(tmp_path: Path, **kwargs: object):
    config = pipeline_config(**kwargs)
    return ResearchPipeline(InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts").run(
        config
    )


def test_future_data_leakage_available_cutoff_and_funding_lookahead_rejected() -> None:
    repository = InMemoryResearchRepository()
    before = raw_event_from_dict(raw_event("before", available_at=BASE))
    future = raw_event_from_dict(raw_event("future", available_at=BASE + timedelta(seconds=1)))
    funding = raw_event_from_dict(
        raw_event(
            "future-funding",
            event_type="funding_rate",
            exchange_timestamp=BASE - timedelta(hours=8),
            available_at=BASE + timedelta(seconds=1),
            payload={"rate": "0.001", "mark_price": "100"},
        )
    )
    for event in (before, future, funding):
        repository.add_raw_event(event)
    dataset = PointInTimeDatasetBuilder(repository).build(
        snapshot_id="pit-cutoff",
        cutoff_at=BASE,
        instruments=("BTC-USD-PERP",),
        venues=("hyperliquid",),
        event_types=("orderbook_snapshot", "funding_rate"),
    )
    assert tuple(item.event_id for item in dataset.values) == ("before",)
    assert set(dataset.excluded_future_event_ids) == {"future", "future-funding"}


def test_delisted_outage_and_quarantine_are_preserved() -> None:
    repository = InMemoryResearchRepository()
    repository.add_raw_event(
        raw_event_from_dict(
            raw_event(
                "delisted",
                payload={
                    "delisted": True,
                    "bid": "100",
                    "ask": "101",
                    "bid_depth": "1",
                    "ask_depth": "1",
                },
            )
        )
    )
    repository.add_raw_event(
        raw_event_from_dict(
            raw_event(
                "outage",
                event_type="venue_outage",
                payload={"outage": True, "duration_seconds": 300},
            )
        )
    )
    malformed = raw_event_from_dict(raw_event("bad", payload={"bid": 1}))
    malformed = replace(
        malformed,
        raw_payload="{",
        payload_sha256=hashlib.sha256(b"{").hexdigest(),
    )
    repository.add_raw_event(malformed)
    dataset = PointInTimeDatasetBuilder(repository).build(
        snapshot_id="retention",
        cutoff_at=BASE,
        instruments=("BTC-USD-PERP",),
        venues=("hyperliquid",),
        event_types=("orderbook_snapshot", "venue_outage"),
    )
    assert dataset.retained_delisted_event_ids == ("delisted",)
    assert dataset.retained_outage_event_ids == ("outage",)
    assert repository.quarantine_count() == 1
    assert repository.quarantined[0][0].raw_payload == "{"


def test_missing_required_funding_field_is_quarantined_instead_of_crashing() -> None:
    repository = InMemoryResearchRepository()
    repository.add_raw_event(
        raw_event_from_dict(
            raw_event(
                "funding-without-mark",
                event_type="funding_history",
                payload={"rate": "0.001"},
            )
        )
    )
    DataSnapshotService(repository).finalize(
        cutoff_at=BASE,
        snapshot_id="missing-funding-field",
        finalized_at=BASE,
    )
    dataset = PointInTimeDatasetBuilder(repository).build(
        snapshot_id="missing-funding-field",
        cutoff_at=BASE,
        instruments=("BTC-USD-PERP",),
        venues=("hyperliquid",),
        event_types=("funding_history",),
    )
    assert dataset.values == ()
    assert repository.quarantine_count() == 1
    assert "normalization failure" in repository.quarantined[0][1]


def test_train_only_normalization_purge_embargo_and_real_strategy(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path)
    rows = result.feature_artifact.rows
    train = rows[: int(len(rows) * 0.6)]
    expected = sum(Decimal(str(item["return"])) for item in train) / Decimal(len(train))
    assert result.feature_artifact.train_normalization["return"][0] == expected
    assert result.backtest_result.fills
    assert result.walk_forward_result.windows
    for window in result.walk_forward_result.windows:
        assert max(window.train_indices) + 2 < min(window.validation_indices)
        assert max(window.validation_indices) + 2 < min(window.oos_indices)


def test_frozen_hypothesis_is_immutable_without_new_version() -> None:
    registry = FrozenHypothesisRegistry()
    original = FrozenHypothesis.freeze(
        hypothesis_version="h1",
        strategy_id="cross_venue_basis",
        parameter_grid={"threshold": (0, 1)},
        primary_metric="net_pnl",
        secondary_metrics=("sharpe",),
        acceptance_thresholds={},
        frozen_at=BASE,
    )
    assert registry.freeze(original) is original
    registry.freeze(original)
    changed = FrozenHypothesis.freeze(
        hypothesis_version="h1",
        strategy_id="cross_venue_basis",
        parameter_grid={"threshold": (0, 2)},
        primary_metric="net_pnl",
        secondary_metrics=("sharpe",),
        acceptance_thresholds={},
        frozen_at=BASE,
    )
    with pytest.raises(ValueError, match="new hypothesis_version"):
        registry.freeze(changed)


def test_walk_forward_and_entire_run_are_reproducible(tmp_path: Path) -> None:
    first = run_pipeline(tmp_path / "first")
    second = run_pipeline(tmp_path / "second")
    assert first.identity == second.identity
    assert first.feature_artifact.content_sha256 == second.feature_artifact.content_sha256
    assert first.walk_forward_result.content_sha256 == second.walk_forward_result.content_sha256
    assert first.cost_stress_result.content_sha256 == second.cost_stress_result.content_sha256
    first_manifest = json.loads(Path(first.artifact_manifest_path).read_text())
    second_manifest = json.loads(Path(second.artifact_manifest_path).read_text())
    assert first_manifest == second_manifest


def test_anchored_walk_forward_keeps_training_origin(tmp_path: Path) -> None:
    config = pipeline_config()
    walk_forward = dict(config["walk_forward"])
    walk_forward["mode"] = "anchored"
    config["walk_forward"] = walk_forward
    result = ResearchPipeline(
        InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts"
    ).run(config)
    assert len(result.walk_forward_result.windows) > 1
    assert all(window.train_indices[0] == 0 for window in result.walk_forward_result.windows)


def test_cost_stress_deteriorates_and_one_leg_failure_is_measured(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path)
    base = result.cost_stress_result.scenario("base")
    fees = result.cost_stress_result.scenario("fees_x2.0")
    failure = result.cost_stress_result.scenario("one_leg_failure")
    assert base is not None and fees is not None and failure is not None
    assert fees.metrics.net_pnl <= base.metrics.net_pnl
    assert failure.failed_leg_cost > 0
    assert failure.naked_exposure_duration_ms > 0


def test_maker_rebate_removal_deteriorates_maker_run(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, maker=True)
    base = result.cost_stress_result.scenario("base")
    removed = result.cost_stress_result.scenario("maker_rebate_removal")
    assert base is not None and removed is not None
    assert base.rebate > 0
    assert removed.rebate == 0
    assert removed.metrics.net_pnl < base.metrics.net_pnl


def test_capital_infeasible_at_100_usd(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, minimum_notional="60")
    assert not result.acceptance_result.capital_feasibility.feasible_at(Decimal("100"))
    assert result.acceptance_result.capital_feasibility.feasible_at(Decimal("300"))


def test_acceptance_requires_real_complete_evidence(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path)
    incomplete = replace(
        result.overfitting_result,
        pbo=None,
        deflated_sharpe=None,
        evidence_complete=False,
    )
    acceptance = evaluate_acceptance(
        data_quality=result.data_quality,
        walk_forward=result.walk_forward_result,
        cost_stress=result.cost_stress_result,
        overfitting=incomplete,
        capital_feasibility=result.acceptance_result.capital_feasibility,
    )
    assert acceptance.verdict.value == "INSUFFICIENT_EVIDENCE"
    assert result.acceptance_result.run_id == result.identity.run_id
    assert result.acceptance_result.data_snapshot_id == result.identity.data_snapshot_id
    with pytest.raises(TypeError):
        evaluate_acceptance()  # type: ignore[call-arg]


def test_unknown_instrument_rule_fields_produce_insufficient_evidence(
    tmp_path: Path,
) -> None:
    repository = InMemoryResearchRepository()
    config = pipeline_config()
    rule_ids: list[str] = []
    for venue in ("hyperliquid", "bitget"):
        rule_id = f"unknown-{venue}-btc"
        rule_ids.append(rule_id)
        repository.save_instrument_rule(
            InstrumentRuleSnapshot(
                rule_snapshot_id=rule_id,
                venue=venue,
                canonical_instrument_id="BTC-USD-PERP",
                venue_symbol="BTCUSDT",
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.001"),
                minimum_quantity=None,
                minimum_notional=None,
                maker_fee=None,
                taker_fee=None,
                maker_rebate=None,
                funding_interval=None,
                margin_asset=None,
                source_endpoint="public-adapter:fetch_markets",
                source_payload_sha256="a" * 64,
                retrieved_at=BASE,
                valid_from=BASE,
                valid_until=None,
                field_evidence={
                    name: {"verification_status": "unknown"}
                    for name in (
                        "minimum_quantity",
                        "minimum_notional",
                        "maker_fee",
                        "taker_fee",
                        "maker_rebate",
                        "funding_interval",
                        "margin_asset",
                    )
                },
            )
        )
    config["rule_snapshot_ids"] = rule_ids
    result = ResearchPipeline(repository, artifact_root=tmp_path / "artifacts").run(config)
    assert result.acceptance_result.verdict.value == "INSUFFICIENT_EVIDENCE"
    assert not result.cost_stress_result.evidence_complete
    assert not result.acceptance_result.capital_feasibility.evidence_complete


def test_data_quality_failure_prevents_strategy_execution(tmp_path: Path) -> None:
    config = pipeline_config()
    raw_events = list(config["raw_events"])
    raw_events.append(raw_events[0])
    config["raw_events"] = raw_events
    result = ResearchPipeline(
        InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts"
    ).run(config)
    assert not result.data_quality.passed
    assert result.backtest_result.orders == ()
    assert result.walk_forward_result.combined_oos_returns == ()
    assert not result.overfitting_result.evidence_complete
    assert result.acceptance_result.verdict.value != "PASS"


def test_hypothesis_must_be_frozen_before_first_oos(tmp_path: Path) -> None:
    config = pipeline_config()
    hypothesis = dict(config["hypothesis"])
    hypothesis["frozen_at"] = (BASE + timedelta(minutes=50)).isoformat()
    config["hypothesis"] = hypothesis
    with pytest.raises(ValueError, match="before the first OOS"):
        ResearchPipeline(InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts").run(
            config
        )


def test_non_r1_strategy_is_explicitly_deferred(tmp_path: Path) -> None:
    config = pipeline_config()
    config["strategy_id"] = "liquidation_exhaustion"
    with pytest.raises(ValueError, match="DEFERRED"):
        ResearchPipeline(InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts").run(
            config
        )


def test_funding_carry_strategy_executes_with_causal_funding(tmp_path: Path) -> None:
    config = pipeline_config()
    config["strategy_id"] = "funding_carry"
    config["data_snapshot_id"] = "funding-snapshot"
    config["event_types"] = ["funding_rate", "orderbook_snapshot"]
    events = list(config["raw_events"])
    for index in range(60):
        timestamp = BASE + timedelta(minutes=index)
        for venue, rate in (("hyperliquid", "0.0001"), ("bitget", "0.001")):
            events.append(
                raw_event(
                    f"funding-{venue}-{index}",
                    venue=venue,
                    event_type="funding_rate",
                    available_at=timestamp,
                    payload={"rate": rate, "mark_price": "100"},
                )
            )
    config["raw_events"] = events
    result = ResearchPipeline(
        InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts"
    ).run(config)
    assert result.data_quality.passed
    assert result.backtest_result.fills
    assert result.identity.strategy_id == "funding_carry"


def test_btc_sol_relative_strength_executes_both_assets(tmp_path: Path) -> None:
    config = pipeline_config()
    events: list[dict[str, object]] = []
    for index in range(60):
        timestamp = BASE + timedelta(minutes=index)
        for instrument, base_price, trend in (
            ("BTC-USD-PERP", Decimal("100"), Decimal("0.2")),
            ("SOL-USD-PERP", Decimal("50"), Decimal("0.05")),
        ):
            mid = base_price + Decimal(index) * trend
            events.append(
                raw_event(
                    f"{instrument}-{index}",
                    instrument=instrument,
                    available_at=timestamp,
                    payload={
                        "bid": str(mid - Decimal("0.1")),
                        "ask": str(mid + Decimal("0.1")),
                        "bid_depth": "20",
                        "ask_depth": "20",
                        "last_price": str(mid),
                        "trade_quantity": "20",
                    },
                )
            )
    config.update(
        {
            "strategy_id": "btc_sol_relative_strength",
            "data_snapshot_id": "relative-snapshot",
            "venues": ["hyperliquid"],
            "instruments": ["BTC-USD-PERP", "SOL-USD-PERP"],
            "raw_events": events,
        }
    )
    result = ResearchPipeline(
        InMemoryResearchRepository(), artifact_root=tmp_path / "artifacts"
    ).run(config)
    assert result.data_quality.passed
    assert {order.symbol for order in result.backtest_result.orders} == {
        "BTCUSDT",
        "SOLUSDT",
    }


def test_artifact_manifest_hash_verification(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path)
    manifest = Path(result.artifact_manifest_path)
    loaded = ResearchArtifactWriter.verify(manifest)
    assert loaded["run_id"] == result.identity.run_id
    required = {
        "summary.md",
        "metrics.json",
        "walk-forward.csv",
        "cost-stress.csv",
        "trades.parquet",
        "rejections.parquet",
    }
    assert set(loaded["files"]) == required
    (manifest.parent / "summary.md").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        ResearchArtifactWriter.verify(manifest)


def test_raw_repository_preserves_payload_and_snapshot_is_immutable(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'research.db'}")
    Base.metadata.create_all(engine)
    repository = PostgreSQLResearchRepository(engine)
    payload = '{"b":2,"a":1}'
    event = raw_event_from_dict(raw_event("stored", payload={"b": 2, "a": 1}))
    event = replace(
        event,
        raw_payload=payload,
        payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
    )
    assert repository.add_raw_event(event)
    assert not repository.add_raw_event(event)
    assert repository.raw_events()[0].raw_payload == payload
    repository.save_snapshot("immutable", BASE, 1, "a" * 64)
    with pytest.raises(ValueError, match="different data"):
        repository.save_snapshot("immutable", BASE, 2, "b" * 64)
    expected = {
        "raw_market_events",
        "market_data_quarantine",
        "market_data_checkpoints",
        "data_snapshots",
        "research_runs",
        "research_artifacts",
        "frozen_hypotheses",
    }
    assert expected <= set(Base.metadata.tables)


def test_hypothesis_freeze_survives_repository_restart(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'freeze.db'}")
    Base.metadata.create_all(engine)
    original = FrozenHypothesis.freeze(
        hypothesis_version="durable-v1",
        strategy_id="funding_carry",
        parameter_grid={"threshold": ("0.1",)},
        primary_metric="net_pnl",
        secondary_metrics=("sharpe",),
        acceptance_thresholds={},
        frozen_at=BASE,
    )
    PostgreSQLResearchRepository(engine).freeze_hypothesis(original)
    changed = replace(
        original,
        parameter_grid={"threshold": ("0.2",)},
        content_sha256="f" * 64,
    )
    with pytest.raises(ValueError, match="new hypothesis_version"):
        PostgreSQLResearchRepository(engine).freeze_hypothesis(changed)


def test_research_cli_commands_use_durable_run_and_verify_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "cli.db"
    engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(engine)
    config_path = tmp_path / "research.json"
    config_path.write_text(json.dumps(pipeline_config()), encoding="utf-8")
    monkeypatch.setenv("APP_DATABASE_URL", f"sqlite+pysqlite:///{database_path}")
    monkeypatch.chdir(tmp_path)
    pipeline = RUNNER.invoke(app, ["run-research-pipeline", "--config", str(config_path)])
    assert pipeline.exit_code == 0, pipeline.output
    run_id = pipeline.output.split("run_id=", 1)[1].split()[0]
    walk_forward = RUNNER.invoke(app, ["run-walk-forward-backtest", "--config", str(config_path)])
    assert walk_forward.exit_code == 0, walk_forward.output
    report = RUNNER.invoke(app, ["generate-research-report", "--run-id", run_id])
    assert report.exit_code == 0, report.output
    assert "hashes=verified" in report.output
