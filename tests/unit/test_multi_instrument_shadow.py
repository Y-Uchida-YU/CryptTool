from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from app.adapters.exchanges.websocket import ReconciliationState
from app.config.settings import Settings
from app.domain.strategies.capabilities import validate_shadow_instruments
from app.domain.strategies.funding_carry import FundingCarryRejectCode
from app.infrastructure.database.models import Base
from app.services.operations.models import OperationMode
from app.services.operations.repository import (
    InMemoryOperationalRepository,
    PostgreSQLOperationalRepository,
)
from app.services.operations.service import ContinuousResearchPaperService
from app.services.operations.shadow_artifacts import ShadowRunArtifactWriter
from app.services.operations.shadow_runtime import (
    CanonicalOrderBookStateBuilder,
    FundingCarryShadowInputSource,
    FundingCarryShadowRuntime,
)
from app.services.research.models import RawMarketEvent
from app.services.research.repository import (
    InMemoryResearchRepository,
    PostgreSQLResearchRepository,
)

NOW = datetime(2026, 9, 26, tzinfo=UTC)


def event(
    instrument: str,
    venue: str,
    kind: str,
    *,
    ordinal: int = 0,
    at: datetime = NOW,
    sequence: int = 10,
) -> RawMarketEvent:
    payload = (
        {
            "rate": "0.001" if venue == "hyperliquid" else "0.0004",
            "funding_interval_seconds": 3600 if venue == "hyperliquid" else 28800,
        }
        if kind == "funding_current"
        else {"bids": [["99", "3"]], "asks": [["100", "3"]]}
    )
    encoded = json.dumps(payload)
    return RawMarketEvent(
        event_id=f"{instrument}-{venue}-{kind}-{ordinal:06d}",
        venue=venue,
        canonical_instrument_id=instrument,
        venue_symbol=instrument,
        event_type=kind,
        exchange_timestamp=at,
        received_at=at,
        available_at=at,
        sequence=sequence,
        connection_id=None,
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        payload_sha256=hashlib.sha256(encoded.encode()).hexdigest(),
        raw_payload=encoded,
        normalizer_version="r10-fixture",
        capability_verification_run_id="experimental",
        created_at=at,
        connection_epoch=1,
        channel="book" if kind.startswith("orderbook") else "rest",
    )


def seed(repository: InMemoryResearchRepository | PostgreSQLResearchRepository) -> None:
    for instrument in ("BTC", "SOL"):
        for venue in ("hyperliquid", "bitget"):
            for kind in ("funding_current", "orderbook_snapshot"):
                assert repository.add_experimental_event(
                    event(instrument, venue, kind), "experimental"
                )


@pytest.fixture(params=["memory", "sql"])
def repository(
    request: pytest.FixtureRequest,
) -> InMemoryResearchRepository | PostgreSQLResearchRepository:
    if request.param == "memory":
        return InMemoryResearchRepository()
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return PostgreSQLResearchRepository(engine)


def settings() -> Settings:
    return Settings(
        database_url="sqlite+pysqlite:///:memory:",
        paper_trading=True,
        live_trading=False,
        exchange_api_key=None,
        exchange_api_secret=None,
        paper={"enabled": True},
        live={"enabled": False, "adapter_name": "disabled"},
        continuous_paper={
            "enabled": True,
            "mode": "shadow",
            "observation_only": True,
            "instruments": ("BTC", "SOL"),
            "venues": ("hyperliquid", "bitget"),
            "strategies": ("funding_carry",),
        },
    )


def service(
    repository: InMemoryOperationalRepository | PostgreSQLOperationalRepository,
    source: FundingCarryShadowRuntime,
) -> ContinuousResearchPaperService:
    operation = ContinuousResearchPaperService(
        repository=repository,
        settings=settings(),
        run_id="multi",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
        market_event_action=lambda: source.read(now=NOW),
    )
    operation.set_collector_health(True)
    return operation


@pytest.mark.parametrize("instruments", [(), ("HYPE",), ("ETH",), ("BTC", "BTC"), ("BTC", "HYPE")])
def test_shadow_allowlist_rejects_unconfigured_instruments(instruments: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="BTC/SOL"):
        validate_shadow_instruments(instruments)


def test_eight_independent_cursors_books_and_instrument_pair_identities(repository) -> None:
    seed(repository)
    runtime = FundingCarryShadowRuntime(repository, instruments=("BTC", "SOL"), run_id="multi")
    batches = runtime.read(now=NOW).batches
    assert len(batches) == 2 and all(batch.matched for batch in batches)
    assert len({batch.pair_identity for batch in batches}) == 2
    for batch in batches:
        assert {item.instrument for item in batch.events} == {batch.instrument}
        source = runtime.instrument_sources[batch.instrument]
        assert source._latest_books["bitget"].instrument == batch.instrument
        cursors, checkpoint = repository.shadow_input_checkpoint(
            "multi", instrument=batch.instrument
        )
        assert len(cursors) == 4 and checkpoint.instrument == batch.instrument
        assert {cursor.instrument for cursor in cursors} == {batch.instrument}
        assert all(cursor.last_event_id.startswith(batch.instrument) for cursor in cursors)
    assert runtime.source_event_count == 8


@pytest.mark.parametrize("target,foreign", [("BTC", "SOL"), ("SOL", "BTC")])
def test_foreign_event_cannot_mutate_instrument_state(target: str, foreign: str) -> None:
    source = FundingCarryShadowInputSource(InMemoryResearchRepository(), instrument=target)
    before = source._state_json()
    with pytest.raises(ValueError, match="cross-instrument"):
        source._process_event(event(foreign, "bitget", "funding_current"), now=NOW)
    assert source._state_json() == before


@pytest.mark.parametrize("broken,healthy", [("BTC", "SOL"), ("SOL", "BTC")])
def test_sequence_gap_and_reconnect_are_isolated(broken: str, healthy: str) -> None:
    builder = CanonicalOrderBookStateBuilder()
    for instrument in (broken, healthy):
        assert builder.apply(event(instrument, "bitget", "orderbook_snapshot"), now=NOW)
    assert builder.apply(event(broken, "bitget", "orderbook_delta", sequence=12), now=NOW) is None
    assert ("bitget", healthy) not in builder.last_invalid_reason
    book = builder.apply(event(healthy, "bitget", "orderbook_delta", sequence=11), now=NOW)
    assert book is not None and book.instrument == healthy and book.last_sequence == 11
    reconnect = replace(event(broken, "bitget", "orderbook_delta", sequence=13), connection_epoch=2)
    assert builder.apply(reconnect, now=NOW) is None
    assert builder.last_invalid_reason[("bitget", broken)] == (
        FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_NOT_INITIALIZED
    )
    assert builder.apply(event(healthy, "bitget", "orderbook_delta", sequence=12), now=NOW)


@pytest.mark.parametrize("funding_instrument,book_instrument", [("BTC", "SOL"), ("SOL", "BTC")])
def test_cross_instrument_funding_and_books_never_pair(funding_instrument, book_instrument) -> None:
    repository = InMemoryResearchRepository()
    for venue in ("hyperliquid", "bitget"):
        repository.add_experimental_event(
            event(funding_instrument, venue, "funding_current"), "experimental"
        )
        repository.add_experimental_event(
            event(book_instrument, venue, "orderbook_snapshot"), "experimental"
        )
    runtime = FundingCarryShadowRuntime(repository, instruments=("BTC", "SOL"))
    assert not any(batch.matched for batch in runtime.read(now=NOW).batches)


@pytest.mark.parametrize("busy,other", [("BTC", "SOL"), ("SOL", "BTC")])
def test_large_book_backlog_does_not_starve_other_instrument_or_stream(
    repository, busy, other
) -> None:
    seed(repository)
    for ordinal in range(1, 1101):
        repository.add_experimental_event(
            event(
                busy,
                "bitget",
                "orderbook_delta",
                ordinal=ordinal,
                sequence=10 + ordinal,
                at=NOW + timedelta(microseconds=ordinal),
            ),
            "experimental",
        )
    runtime = FundingCarryShadowRuntime(repository, instruments=("BTC", "SOL"), batch_size=1000)
    batches = runtime.read(now=NOW + timedelta(seconds=1)).batches
    assert all(batch.matched for batch in batches)
    assert runtime.instrument_sources[other].source_event_count == 4
    metrics = runtime.instrument_sources[busy].runtime_metrics(now=NOW + timedelta(seconds=1))
    assert metrics["shadow_input_events_processed_total"] <= 1000
    assert metrics["shadow_input_backlog_event_count"] == 851
    assert (
        metrics["shadow_input_backlog_stream_count"]
        == metrics["shadow_input_backlog_estimate"]
        == 1
    )
    assert len(runtime.instrument_sources[busy]._latest_funding) == 2


def test_processing_failure_does_not_rollback_other_instrument(repository, monkeypatch) -> None:
    seed(repository)
    runtime = FundingCarryShadowRuntime(repository, instruments=("BTC", "SOL"), run_id="multi")
    broken = runtime.instrument_sources["BTC"]

    def fail(*args, **kwargs):
        raise RuntimeError("fixture input failure")

    monkeypatch.setattr(broken, "_process_event", fail)
    batches = runtime.read(now=NOW).batches
    assert [batch.instrument for batch in batches] == ["SOL"]
    assert runtime.runtime_status == "degraded"
    btc, failed = repository.shadow_input_checkpoint("multi", instrument="BTC")
    sol, succeeded = repository.shadow_input_checkpoint("multi", instrument="SOL")
    assert all(item.last_event_id is None for item in btc)
    assert all(item.last_event_id is not None for item in sol)
    assert failed.events_processed_total == 0 and failed.events_failed_total == 4
    assert succeeded.events_processed_total == 4


def test_restart_restores_both_snapshot_backed_books_and_cursor_identities(repository) -> None:
    seed(repository)
    options = {
        "instruments": ("BTC", "SOL"),
        "run_id": "multi",
        "initial_available_at": NOW - timedelta(seconds=1),
    }
    original = FundingCarryShadowRuntime(repository, **options)
    original.read(now=NOW)
    for instrument in ("BTC", "SOL"):
        repository.add_experimental_event(
            event(
                instrument, "bitget", "orderbook_delta", sequence=11, at=NOW + timedelta(seconds=1)
            ),
            "experimental",
        )
    restarted = FundingCarryShadowRuntime(repository, **options)
    batches = restarted.read(now=NOW + timedelta(seconds=1)).batches
    assert all(batch.matched for batch in batches)
    for instrument, source in restarted.instrument_sources.items():
        assert source._latest_books["bitget"].sequence == 11
        assert source._latest_books["bitget"].source_event_id.startswith(instrument)
        assert source.book_builder.metrics.sequence_gap_count == 0
        assert source._started_at == NOW - timedelta(seconds=1)
    again = FundingCarryShadowRuntime(repository, **options)
    assert all(
        batch.source_pair_duplicate for batch in again.read(now=NOW + timedelta(seconds=2)).batches
    )


def test_lag_coverage_and_stall_are_per_instrument_and_global_is_worst(repository) -> None:
    seed(repository)
    runtime = FundingCarryShadowRuntime(
        repository,
        instruments=("BTC", "SOL"),
        initial_available_at=NOW - timedelta(seconds=1),
        maximum_shadow_input_stall_seconds=5,
    )
    runtime.read(now=NOW)
    btc, sol = runtime.instrument_sources["BTC"], runtime.instrument_sources["SOL"]
    for venue in ("hyperliquid", "bitget"):
        for kind in ("funding_current", "orderbook_snapshot"):
            repository.add_experimental_event(
                event("BTC", venue, kind, ordinal=1, at=NOW + timedelta(seconds=10)), "experimental"
            )
    repository.add_experimental_event(
        event("SOL", "bitget", "funding_current", ordinal=1, at=NOW + timedelta(seconds=9)),
        "experimental",
    )
    btc.read(now=NOW + timedelta(seconds=10))
    sol._refresh_lag(now=NOW + timedelta(seconds=10))
    assert btc.runtime_status == "healthy" and sol.runtime_status == "degraded"
    metrics = runtime.runtime_metrics(now=NOW + timedelta(seconds=10))
    assert metrics["shadow_runtime_status"] == "degraded"
    assert metrics["by_instrument"]["BTC"]["shadow_input_lag_seconds"] == 0
    assert metrics["by_instrument"]["SOL"]["shadow_input_lag_seconds"] == 9
    assert (
        metrics["by_instrument"]["BTC"]["strategy_observation_coverage_ratio"] == Decimal(10) / 11
    )
    assert metrics["by_instrument"]["SOL"]["strategy_observation_coverage_ratio"] == 0
    assert metrics["strategy_observation_coverage_ratio"] == 0
    sol.read(now=NOW + timedelta(seconds=11))
    assert runtime.runtime_status == "healthy"
    # Idle streams with no pending input must not cause a false stall.
    btc._refresh_lag(now=NOW + timedelta(seconds=500))
    assert btc.runtime_status == "healthy"


@pytest.mark.asyncio
async def test_evaluator_isolation_determinism_dedup_artifacts_and_safety(
    tmp_path, monkeypatch
) -> None:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    research = PostgreSQLResearchRepository(engine)
    seed(research)
    source = FundingCarryShadowRuntime(research, instruments=("BTC", "SOL"), run_id="multi")
    operational = PostgreSQLOperationalRepository(engine)
    operation = service(operational, source)
    await operation._collector_tick()
    await operation._signal_tick()
    first = operational.shadow_candidates("multi")
    assert {candidate.instrument for candidate in first} == {"BTC", "SOL"}
    assert all(candidate.economics_calculated for candidate in first)
    await operation._signal_tick()
    assert operational.shadow_candidates("multi") == first
    for candidate in first:
        metrics = operational.shadow_metrics("multi", instrument=candidate.instrument)
        assert metrics.candidate_generation_attempt_count == 2
        assert metrics.candidate_inserted_count == metrics.candidate_duplicate_suppressed_count == 1
    # BTC data and source IDs through single-instrument R9-compatible path are identical.
    single = FundingCarryShadowInputSource(research, run_id="r9-compat")
    btc_batch = single.read(now=NOW)
    again = operation.generate_funding_carry_shadow_candidate(
        events=btc_batch.events, batch=btc_batch, decision_time=NOW
    )
    assert again == next(candidate for candidate in first if candidate.instrument == "BTC")
    restarted = FundingCarryShadowRuntime(research, instruments=("BTC", "SOL"), run_id="multi")
    for batch in restarted.read(now=NOW).batches:
        candidate = operation.generate_funding_carry_shadow_candidate(
            events=batch.events, batch=batch, decision_time=NOW
        )
        # Batch-only metadata can differ on replay; economic fields and IDs must not.
        old = next(item for item in first if item.instrument == batch.instrument)
        assert candidate.candidate_id == old.candidate_id
        assert candidate.expected_net_edge == old.expected_net_edge
        assert candidate.round_trip_cost == old.round_trip_cost
    assert (
        operational.signals("multi")
        == operational.orders("multi")
        == operational.fills("multi")
        == ()
    )
    assert not hasattr(operation, "execution_adapter")
    assert {worker.name for worker in operation.workers} == {
        "collector",
        "funding_carry_shadow_evaluator",
    }

    monkeypatch.setenv("CRYPTTOOL_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(
        "app.services.operations.shadow_artifacts.tempfile.gettempdir", lambda: "/os-temp"
    )
    config = tmp_path / "source.yaml"
    config.write_text("mode: shadow\n")
    writer = ShadowRunArtifactWriter(
        run_id="multi",
        commit_sha="a" * 40,
        config_path=config,
        settings=operation.settings,
        engine=engine,
        repository=operational,
        input_source=source,
    )
    assert writer.finalize(exit_code=0) == 0
    for filename in (
        "pairing-metrics.json",
        "dedup-metrics.json",
        "collector-metrics.json",
        "orderbook-state-metrics.json",
    ):
        data = json.loads((writer.directory / filename).read_text())
        assert set(data["by_instrument"]) == {"BTC", "SOL"}
    for filename in ("funding-edge-timeseries.csv", "economics-timeseries.csv"):
        with (writer.directory / filename).open() as handle:
            rows = list(csv.DictReader(handle))
        assert {row["instrument"] for row in rows} == {"BTC", "SOL"}
        assert all(row["gross_funding_edge_per_hour"] for row in rows)
    safety = json.loads((writer.directory / "safety-counters.json").read_text())
    assert all(value == 0 for key, value in safety.items() if key.endswith("_count"))
    assert safety["strict_paper_state"] == "NOT_READY"
    assert safety["live_execution_state"] == "OFF"
    assert safety["live_credentials_loaded"] is False


def test_example_config_preserves_baseline_thresholds() -> None:
    import yaml

    payload = yaml.safe_load(
        (
            Path(__file__).parents[2] / "configs/funding-carry-cross-sectional-shadow.yaml"
        ).read_text()
    )
    config = Settings(**payload)
    assert (
        config.continuous_paper.instruments
        == config.research_collection.instruments
        == ("BTC", "SOL")
    )
    operation = service(
        InMemoryOperationalRepository(),
        FundingCarryShadowRuntime(InMemoryResearchRepository(), instruments=("BTC", "SOL")),
    )
    for evaluator in operation._shadow_evaluators.values():
        assert evaluator.config.evaluation_horizon_seconds == 3600
        assert evaluator.config.minimum_net_edge == 0
        assert evaluator.config.shadow_notional == 10
        assert evaluator.config.venue_taker_fee_rates == (
            ("bitget", Decimal("0.0006")),
            ("hyperliquid", Decimal("0.0006")),
        )


def test_migration_preserves_r9_rows_and_adds_independent_sol_checkpoint(monkeypatch) -> None:
    import importlib.util

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import text

    path = Path(__file__).parents[2] / "migrations/versions/0021_shadow_instruments.py"
    spec = importlib.util.spec_from_file_location("r10_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE shadow_input_checkpoints "
                "(run_id VARCHAR(160) PRIMARY KEY, state_json TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE funding_carry_shadow_metrics (run_id VARCHAR(160) PRIMARY KEY, "
                "candidate_generation_attempt_count BIGINT)"
            )
        )
        connection.execute(text("INSERT INTO shadow_input_checkpoints VALUES ('old-run', '{}')"))
        connection.execute(text("INSERT INTO funding_carry_shadow_metrics VALUES ('old-run', 29)"))
        monkeypatch.setattr(module, "op", Operations(MigrationContext.configure(connection)))
        module.upgrade()
        assert connection.execute(
            text("SELECT instrument, state_json FROM shadow_input_checkpoints")
        ).all() == [("BTC", "{}")]
        assert sorted(
            connection.execute(
                text(
                    "SELECT instrument, candidate_generation_attempt_count "
                    "FROM funding_carry_shadow_metrics"
                )
            ).all()
        ) == [("", 29), ("BTC", 29)]
        connection.execute(
            text(
                "INSERT INTO shadow_input_checkpoints (run_id, instrument, state_json) "
                "VALUES ('old-run', 'SOL', :payload)"
            ),
            {"payload": '{"isolated":true}'},
        )
        assert connection.scalar(text("SELECT count(*) FROM shadow_input_checkpoints")) == 2
        with pytest.raises(RuntimeError, match="archival"):
            module.downgrade()


def test_failed_checkpoint_does_not_commit_processed_count_or_skip_events(
    repository, monkeypatch
) -> None:
    seed(repository)
    source = FundingCarryShadowInputSource(repository, run_id="commit-failure")
    commit = repository.commit_shadow_input_checkpoint
    calls = 0

    def fail_once(cursors, checkpoint):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("commit failed")
        commit(cursors, checkpoint)

    monkeypatch.setattr(repository, "commit_shadow_input_checkpoint", fail_once)
    with pytest.raises(RuntimeError, match="commit failed"):
        source.read(now=NOW)
    cursors, checkpoint = repository.shadow_input_checkpoint("commit-failure")
    assert all(cursor.last_available_at is None for cursor in cursors)
    assert checkpoint.events_processed_total == 0
    assert checkpoint.strategy_observation_last_at is None
    assert source.read(now=NOW).matched
    assert source.source_event_count == 4


def test_pair_identity_includes_instrument_even_for_identical_source_ids() -> None:
    repository = InMemoryResearchRepository()
    seed(repository)
    runtime = FundingCarryShadowRuntime(repository, instruments=("BTC", "SOL"))
    runtime.read(now=NOW)
    btc, sol = runtime.instrument_sources["BTC"], runtime.instrument_sources["SOL"]
    sol._latest_books = {
        venue: replace(book, instrument="SOL") for venue, book in btc._latest_books.items()
    }
    sol._latest_funding = {
        venue: replace(item, instrument="SOL") for venue, item in btc._latest_funding.items()
    }
    assert btc._source_event_ids(btc._available_events()) == sol._source_event_ids(
        sol._available_events()
    )
    assert btc._evaluate(now=NOW).pair_identity != sol._evaluate(now=NOW).pair_identity


def test_processing_rollback_preserves_applied_delta_source_identities(
    repository, monkeypatch
) -> None:
    seed(repository)
    source = FundingCarryShadowInputSource(repository, run_id="delta-rollback")
    source.read(now=NOW)
    delta = event("BTC", "bitget", "orderbook_delta", sequence=11, at=NOW + timedelta(seconds=1))
    repository.add_experimental_event(delta, "experimental")
    source.read(now=NOW + timedelta(seconds=1))
    prior_ids = source._latest_books["bitget"].applied_source_event_ids
    assert prior_ids == (delta.event_id,)
    repository.add_experimental_event(
        event(
            "BTC",
            "bitget",
            "orderbook_delta",
            ordinal=1,
            sequence=12,
            at=NOW + timedelta(seconds=2),
        ),
        "experimental",
    )
    original = source._process_event

    def fail(*args, **kwargs):
        raise RuntimeError("rollback fixture")

    monkeypatch.setattr(source, "_process_event", fail)
    with pytest.raises(RuntimeError, match="rollback fixture"):
        source.read(now=NOW + timedelta(seconds=2))
    assert source._latest_books["bitget"].applied_source_event_ids == prior_ids
    monkeypatch.setattr(source, "_process_event", original)
    assert source.read(now=NOW + timedelta(seconds=2)).matched
    assert source._latest_books["bitget"].applied_source_event_ids == (
        delta.event_id,
        "BTC-bitget-orderbook_delta-000001",
    )


@pytest.mark.parametrize("instruments", [("BTC", "SOL"), ("SOL",)])
def test_cli_wires_scoped_collector_and_evaluators_without_external_api(
    tmp_path, monkeypatch, instruments
) -> None:
    from types import SimpleNamespace

    from typer.testing import CliRunner

    from app.cli import main as cli

    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'cli.db'}")
    Base.metadata.create_all(engine)
    configured = settings()
    configured.database_url = "postgresql+psycopg://localhost/fixture-only"
    configured.continuous_paper.instruments = instruments
    configured.research_collection.collection_enabled = True
    configured.research_collection.instruments = instruments
    configured.research_collection.venues = ("hyperliquid", "bitget")
    configured.research_collection.event_types = (
        "funding_current",
        "orderbook_snapshot",
        "orderbook_delta",
    )
    config = tmp_path / "config.yaml"
    config.write_text("mode: shadow\n")
    token = tmp_path / "run.token"
    token.write_text("fixture-token")
    monkeypatch.setenv("CRYPTTOOL_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: "/os-temp")
    monkeypatch.setattr(cli, "_settings_from_yaml", lambda _: configured)
    monkeypatch.setattr(cli, "build_engine", lambda _: engine)
    monkeypatch.setattr(
        cli, "_research_data_adapter", lambda venue: SimpleNamespace(venue=venue, capabilities=())
    )
    monkeypatch.setattr(cli.TrustedCapabilityRegistry, "from_artifacts", lambda *args: object())
    monkeypatch.setattr(cli, "_database_identity", lambda _: ("fixture-db", "main"))
    monkeypatch.setattr(cli, "_process_identity", lambda _: (NOW, "c" * 64))
    monkeypatch.setattr(cli, "_create_collector_run_token", lambda _: (token, "d" * 64))
    monkeypatch.setattr(cli, "_current_commit_sha", lambda: "a" * 40)
    called = []

    async def fixture_only(**kwargs):
        operation = kwargs["service"]
        collector = kwargs["collector"]
        assert collector.instruments == instruments
        assert operation.snapshot_action is operation.research_action is None
        assert set(operation._shadow_evaluators) == set(instruments)
        assert {worker.name for worker in operation.workers} == {
            "collector",
            "funding_carry_shadow_evaluator",
        }
        research = PostgreSQLResearchRepository(engine)
        at = datetime.now(UTC)
        for instrument in instruments:
            for venue in ("hyperliquid", "bitget"):
                for kind in ("funding_current", "orderbook_snapshot"):
                    research.add_experimental_event(
                        event(instrument, venue, kind, at=at), "experimental"
                    )
        result = operation.market_event_action()
        assert {batch.instrument for batch in result.batches} == set(instruments)
        assert all(batch.matched for batch in result.batches)
        called.append(True)

    monkeypatch.setattr(cli, "_run_continuous_operation", fixture_only)
    result = CliRunner().invoke(
        cli.app, ["start-paper-operation", "--config", str(config), "--run-id", "cli-multi"]
    )
    assert result.exit_code == 0, result.exception
    assert called == [True]
    assert not token.exists()
