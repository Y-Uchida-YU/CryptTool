from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import socket
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine
from typer.testing import CliRunner

from app.adapters.exchanges.websocket import ReconciliationState
from app.cli.main import (
    _collector_token_path,
    _process_identity,
    _require_nonproduction_database_isolation,
    _run_collector_with_lease,
    _verify_collector_process_identity,
    app,
)
from app.config.settings import Settings
from app.infrastructure.database.models import Base
from app.services.research.collector_runs import (
    CollectorLeaseConflict,
    CollectorRunRecord,
    CollectorRunStatus,
    InMemoryCollectorLeaseRepository,
    SQLCollectorLeaseRepository,
    collector_group_key,
)
from app.services.research.data_operations import DataSnapshotService
from app.services.research.models import CollectionCheckpoint, RawMarketEvent
from app.services.research.repository import (
    InMemoryResearchRepository,
    NamespacedResearchRepository,
    PostgreSQLResearchRepository,
)

NOW = datetime(2026, 7, 14, tzinfo=UTC)
RUNNER = CliRunner()


def group() -> str:
    return collector_group_key(
        database_identity="sqlite:////tmp/research.db",
        schema_name="main",
        venue="hyperliquid",
        instrument="BTC",
        event_type="trade",
        channel="trades",
    )


def checkpoint(sequence: int) -> CollectionCheckpoint:
    return CollectionCheckpoint(
        venue="hyperliquid",
        stream_key="hyperliquid:trade:BTC:trades",
        connection_id=UUID("00000000-0000-0000-0000-000000000001"),
        last_sequence=sequence,
        last_event_id=f"event-{sequence}",
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        checkpointed_at=NOW,
    )


def run_record() -> CollectorRunRecord:
    return CollectorRunRecord(
        run_id="soak-1",
        collector_group="soak",
        owner_id="host:1",
        commit_sha="a" * 40,
        config_path="/tmp/config.yaml",
        database_identity="sqlite:////tmp/research.db",
        schema_name="main",
        checkpoint_namespace="soak:soak-1",
        artifact_namespace="artifacts/collector-soak/soak-1",
        venues=("hyperliquid", "bitget"),
        instruments=("BTC", "ETH"),
        event_types=("trade",),
        duration_seconds=3600,
        pid=1,
        process_started_at=NOW,
        hostname="host",
        command_sha256="b" * 64,
        run_token_sha256="c" * 64,
        status=CollectorRunStatus.RUNNING,
        started_at=NOW,
        heartbeat_at=NOW,
    )


def test_duplicate_lease_fails_closed_and_expired_lease_can_be_recovered() -> None:
    repository = InMemoryCollectorLeaseRepository()
    repository.acquire(group(), "run-a", "owner-a", datetime.now(UTC) + timedelta(minutes=1))
    with pytest.raises(CollectorLeaseConflict, match="held"):
        repository.acquire(group(), "run-b", "owner-b", datetime.now(UTC) + timedelta(minutes=1))
    repository.leases[group()] = replace(
        repository.leases[group()], expires_at=datetime.now(UTC) - timedelta(seconds=1)
    )
    assert (
        repository.acquire(
            group(), "run-b", "owner-b", datetime.now(UTC) + timedelta(minutes=1)
        ).run_id
        == "run-b"
    )


def test_sql_lease_renew_release_and_run_registry(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'leases.db'}")
    Base.metadata.create_all(engine)
    repository = SQLCollectorLeaseRepository(engine)
    expiry = datetime.now(UTC) + timedelta(minutes=1)
    repository.acquire(group(), "run-a", "owner-a", expiry)
    with pytest.raises(CollectorLeaseConflict):
        SQLCollectorLeaseRepository(engine).acquire(group(), "run-b", "owner-b", expiry)
    renewed = repository.renew(group(), "run-a", "owner-a", expiry + timedelta(minutes=1))
    assert renewed.expires_at == expiry + timedelta(minutes=1)
    repository.release(group(), "run-a", "owner-a")
    assert repository.acquire(group(), "run-b", "owner-b", expiry).run_id == "run-b"
    repository.save_run(run_record())
    assert repository.get_run("soak-1") == run_record()
    assert repository.list_runs() == (run_record(),)


@pytest.mark.parametrize("durable", [False, True])
def test_checkpoint_namespaces_are_isolated(tmp_path: Path, durable: bool) -> None:
    if durable:
        engine = create_engine(f"sqlite:///{tmp_path / 'checkpoints.db'}")
        Base.metadata.create_all(engine)
        base = PostgreSQLResearchRepository(engine)
    else:
        base = InMemoryResearchRepository()
    first = NamespacedResearchRepository(base, "soak:one")
    second = NamespacedResearchRepository(base, "soak:two")
    first.save_checkpoint(checkpoint(1))
    second.save_checkpoint(checkpoint(2))
    assert first.get_checkpoint("hyperliquid", "hyperliquid:trade:BTC:trades") == replace(
        checkpoint(1), checkpoint_namespace="soak:one"
    )
    assert second.get_checkpoint("hyperliquid", "hyperliquid:trade:BTC:trades") == replace(
        checkpoint(2), checkpoint_namespace="soak:two"
    )
    assert base.get_checkpoint("hyperliquid", "hyperliquid:trade:BTC:trades") is None


def test_checkpoint_namespace_does_not_imply_raw_event_isolation() -> None:
    base = InMemoryResearchRepository()
    first = NamespacedResearchRepository(base, "soak:one")
    second = NamespacedResearchRepository(base, "soak:two")
    event = raw_event("soak-event")
    assert first.add_raw_event(event)
    assert second.raw_events() == (event,)


@pytest.mark.parametrize("run_mode", ["soak", "accelerated_validation", "clean_replay"])
def test_nonproduction_mode_rejects_production_database(tmp_path: Path, run_mode: str) -> None:
    database_url = f"sqlite:///{tmp_path / 'production.db'}"
    settings = Settings(
        database_url=database_url,
        production_database_url=database_url,
    )
    with pytest.raises(Exception, match="must be isolated"):
        _require_nonproduction_database_isolation(settings, run_mode=run_mode)


def test_nonproduction_mode_requires_explicit_production_database_identity(
    tmp_path: Path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'soak.db'}",
        production_database_url=None,
    )
    with pytest.raises(Exception, match="explicit production_database_url"):
        _require_nonproduction_database_isolation(settings, run_mode="soak")


def test_soak_raw_event_is_excluded_from_production_snapshot() -> None:
    production = InMemoryResearchRepository()
    soak = InMemoryResearchRepository()
    production_event = raw_event("production-event")
    soak_event = raw_event("soak-event")
    production.add_raw_event(production_event)
    soak.add_raw_event(soak_event)
    manifest = DataSnapshotService(production).finalize(
        cutoff_at=NOW,
        snapshot_id="production-only",
        finalized_at=NOW,
    )
    assert tuple(item[1] for item in manifest.events) == (production_event.event_id,)


def raw_event(event_id: str) -> RawMarketEvent:
    payload = "{}"
    return RawMarketEvent(
        event_id=event_id,
        venue="hyperliquid",
        canonical_instrument_id="BTC",
        venue_symbol="BTC",
        event_type="venue_health",
        exchange_timestamp=None,
        received_at=NOW,
        available_at=NOW,
        sequence=None,
        connection_id=None,
        reconciliation_state=None,
        payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        raw_payload=payload,
        normalizer_version="test",
        capability_verification_run_id="test",
        created_at=NOW,
    )


class FakeCollector:
    def __init__(self) -> None:
        self.shutdown_requested = asyncio.Event()
        self.checkpoint_flushed = False

    async def run(self) -> None:
        await self.shutdown_requested.wait()
        await asyncio.sleep(0)
        self.checkpoint_flushed = True

    def shutdown(self) -> None:
        self.shutdown_requested.set()


class RenewalFailureRepository(InMemoryCollectorLeaseRepository):
    def renew(self, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        del args, kwargs
        raise ConnectionError("injected renewal database failure")


async def run_until_renewal_failure(
    repository: InMemoryCollectorLeaseRepository,
    *,
    expiry_seconds: float = 1,
) -> tuple[FakeCollector, CollectorRunRecord]:
    run = replace(
        run_record(),
        started_at=datetime.now(UTC),
        heartbeat_at=datetime.now(UTC),
    )
    repository.save_run(run)
    repository.acquire(
        group(), run.run_id, run.owner_id, datetime.now(UTC) + timedelta(seconds=expiry_seconds)
    )
    collector = FakeCollector()
    with pytest.raises(RuntimeError, match="lease renewal failed"):
        await _run_collector_with_lease(  # type: ignore[arg-type]
            collector=collector,
            duration_seconds=60,
            lease_repository=repository,
            groups=(group(),),
            run=run,
            lease_ttl_seconds=0.03,
            renewal_timeout_seconds=0.2,
        )
    return collector, run


@pytest.mark.asyncio
async def test_renewal_db_failure_stops_collector() -> None:
    collector, _ = await run_until_renewal_failure(RenewalFailureRepository())
    assert collector.shutdown_requested.is_set()


@pytest.mark.asyncio
async def test_renewal_failure_flushes_checkpoint() -> None:
    collector, _ = await run_until_renewal_failure(RenewalFailureRepository())
    assert collector.checkpoint_flushed


@pytest.mark.asyncio
async def test_renewal_failure_marks_run_failed() -> None:
    repository = RenewalFailureRepository()
    _, run = await run_until_renewal_failure(repository)
    stored = repository.get_run(run.run_id)
    assert stored is not None
    assert stored.status == CollectorRunStatus.FAILED
    assert "lease renewal failed" in (stored.failure_reason or "")
    assert not repository.leases


@pytest.mark.asyncio
async def test_lease_ownership_change_stops_collector() -> None:
    repository = InMemoryCollectorLeaseRepository()
    run = replace(run_record(), started_at=datetime.now(UTC), heartbeat_at=datetime.now(UTC))
    repository.save_run(run)
    repository.acquire(group(), run.run_id, run.owner_id, datetime.now(UTC) + timedelta(seconds=1))
    collector = FakeCollector()

    async def steal_lease() -> None:
        await asyncio.sleep(0.005)
        repository.leases[group()] = replace(repository.leases[group()], owner_id="other")

    thief = asyncio.create_task(steal_lease())
    with pytest.raises(RuntimeError, match="ownership changed"):
        await _run_collector_with_lease(  # type: ignore[arg-type]
            collector=collector,
            duration_seconds=60,
            lease_repository=repository,
            groups=(group(),),
            run=run,
            lease_ttl_seconds=0.03,
            renewal_timeout_seconds=0.2,
        )
    await thief
    assert collector.checkpoint_flushed


@pytest.mark.asyncio
async def test_expired_lease_cannot_coexist_with_still_running_collector() -> None:
    repository = InMemoryCollectorLeaseRepository()
    collector, _ = await run_until_renewal_failure(repository, expiry_seconds=0.001)
    assert collector.checkpoint_flushed
    assert not repository.leases


def test_collector_run_list_status_and_graceful_stop_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "runs.db"
    database_url = f"sqlite:///{database}"
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    repository = SQLCollectorLeaseRepository(engine)
    process_started_at, command_sha256 = _process_identity(os.getpid())
    token = "collector-run-test-token"
    token_path = _collector_token_path(run_record().run_id)
    token_path.write_text(token, encoding="utf-8")
    running = replace(
        run_record(),
        heartbeat_at=datetime.now(UTC),
        started_at=datetime.now(UTC),
        pid=os.getpid(),
        process_started_at=process_started_at,
        hostname=socket.gethostname(),
        command_sha256=command_sha256,
        run_token_sha256=hashlib.sha256(token.encode()).hexdigest(),
    )
    repository.save_run(running)

    listed = RUNNER.invoke(app, ["list-collector-runs", "--database-url", database_url])
    assert listed.exit_code == 0
    assert json.loads(listed.stdout)[0]["run_id"] == running.run_id
    status = RUNNER.invoke(
        app,
        [
            "collector-run-status",
            "--run-id",
            running.run_id,
            "--database-url",
            database_url,
        ],
    )
    assert status.exit_code == 0
    assert json.loads(status.stdout)["status"] == "RUNNING"

    def graceful_signal(pid: int, requested_signal: int) -> None:
        assert pid == running.pid
        assert requested_signal == signal.SIGINT
        current = repository.get_run(running.run_id)
        assert current is not None
        repository.save_run(
            replace(
                current,
                status=CollectorRunStatus.COMPLETED,
                stopped_at=datetime.now(UTC),
            )
        )

    monkeypatch.setattr("app.cli.main.os.kill", graceful_signal)
    stopped = RUNNER.invoke(
        app,
        [
            "stop-collector-run",
            "--run-id",
            running.run_id,
            "--database-url",
            database_url,
        ],
    )
    assert stopped.exit_code == 0
    assert "checkpoint_flushed=true" in stopped.stdout
    token_path.unlink(missing_ok=True)


def test_pid_identity_rejects_command_hash_mismatch() -> None:
    process_started_at, _ = _process_identity(os.getpid())
    token = "collector-run-identity-token"
    token_path = _collector_token_path(run_record().run_id)
    token_path.write_text(token, encoding="utf-8")
    run = replace(
        run_record(),
        pid=os.getpid(),
        process_started_at=process_started_at,
        hostname=socket.gethostname(),
        command_sha256="0" * 64,
        run_token_sha256=hashlib.sha256(token.encode()).hexdigest(),
    )
    with pytest.raises(Exception, match="command hash mismatch"):
        _verify_collector_process_identity(run)
    token_path.unlink(missing_ok=True)
