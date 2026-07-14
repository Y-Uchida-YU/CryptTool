from __future__ import annotations

import json
import os
import signal
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine
from typer.testing import CliRunner

from app.adapters.exchanges.websocket import ReconciliationState
from app.cli.main import app
from app.infrastructure.database.models import Base
from app.services.research.collector_runs import (
    CollectorLeaseConflict,
    CollectorRunRecord,
    CollectorRunStatus,
    InMemoryCollectorLeaseRepository,
    SQLCollectorLeaseRepository,
    collector_group_key,
)
from app.services.research.models import CollectionCheckpoint
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


def test_collector_run_list_status_and_graceful_stop_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "runs.db"
    database_url = f"sqlite:///{database}"
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    repository = SQLCollectorLeaseRepository(engine)
    running = replace(
        run_record(),
        heartbeat_at=datetime.now(UTC),
        started_at=datetime.now(UTC),
        pid=os.getpid(),
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
