from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.infrastructure.database.models import CollectorLeaseRow, CollectorRunRow


class CollectorLeaseConflict(RuntimeError):
    pass


class CollectorRunStatus(StrEnum):
    RUNNING = "RUNNING"
    STOP_REQUESTED = "STOP_REQUESTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED_DUE_TO_OVERLAP = "CANCELED_DUE_TO_OVERLAP"


@dataclass(frozen=True)
class CollectorLease:
    collector_group: str
    run_id: str
    owner_id: str
    expires_at: datetime
    acquired_at: datetime
    renewed_at: datetime


@dataclass(frozen=True)
class CollectorRunRecord:
    run_id: str
    collector_group: str
    owner_id: str
    commit_sha: str
    config_path: str
    database_identity: str
    schema_name: str
    checkpoint_namespace: str
    artifact_namespace: str
    venues: tuple[str, ...]
    instruments: tuple[str, ...]
    event_types: tuple[str, ...]
    duration_seconds: float | None
    pid: int
    status: CollectorRunStatus
    started_at: datetime
    heartbeat_at: datetime
    stop_requested_at: datetime | None = None
    stopped_at: datetime | None = None
    artifact_directory: str | None = None
    failure_reason: str | None = None


class CollectorLeaseRepository:
    def acquire(
        self,
        collector_group: str,
        run_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> CollectorLease:
        raise NotImplementedError

    def renew(
        self,
        collector_group: str,
        run_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> CollectorLease:
        raise NotImplementedError

    def release(self, collector_group: str, run_id: str, owner_id: str) -> None:
        raise NotImplementedError


def collector_group_key(
    *,
    database_identity: str,
    schema_name: str,
    venue: str,
    instrument: str,
    event_type: str,
    channel: str,
) -> str:
    payload = json.dumps(
        {
            "channel": channel,
            "database_identity": database_identity,
            "event_type": event_type,
            "instrument": instrument,
            "schema": schema_name,
            "venue": venue,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"collector-{hashlib.sha256(payload.encode()).hexdigest()}"


class InMemoryCollectorLeaseRepository(CollectorLeaseRepository):
    def __init__(self) -> None:
        self.leases: dict[str, CollectorLease] = {}
        self.runs: dict[str, CollectorRunRecord] = {}
        self._lock = Lock()

    def acquire(
        self,
        collector_group: str,
        run_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> CollectorLease:
        now = datetime.now(UTC)
        with self._lock:
            current = self.leases.get(collector_group)
            if (
                current is not None
                and current.expires_at > now
                and (current.run_id != run_id or current.owner_id != owner_id)
            ):
                raise CollectorLeaseConflict(f"collector lease is held by {current.run_id}")
            lease = CollectorLease(collector_group, run_id, owner_id, expires_at, now, now)
            self.leases[collector_group] = lease
            return lease

    def renew(
        self,
        collector_group: str,
        run_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> CollectorLease:
        now = datetime.now(UTC)
        with self._lock:
            current = self.leases.get(collector_group)
            if current is None or current.run_id != run_id or current.owner_id != owner_id:
                raise CollectorLeaseConflict("collector lease ownership changed")
            lease = replace(current, expires_at=expires_at, renewed_at=now)
            self.leases[collector_group] = lease
            return lease

    def release(self, collector_group: str, run_id: str, owner_id: str) -> None:
        with self._lock:
            current = self.leases.get(collector_group)
            if current is not None and current.run_id == run_id and current.owner_id == owner_id:
                del self.leases[collector_group]

    def save_run(self, run: CollectorRunRecord) -> None:
        self.runs[run.run_id] = run

    def get_run(self, run_id: str) -> CollectorRunRecord | None:
        return self.runs.get(run_id)

    def list_runs(self) -> tuple[CollectorRunRecord, ...]:
        return tuple(sorted(self.runs.values(), key=lambda item: item.started_at))


class SQLCollectorLeaseRepository(CollectorLeaseRepository):
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def acquire(
        self,
        collector_group: str,
        run_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> CollectorLease:
        now = datetime.now(UTC)
        try:
            with Session(self.engine) as session, session.begin():
                row = session.get(CollectorLeaseRow, collector_group, with_for_update=True)
                if (
                    row is not None
                    and self._aware(row.expires_at) > now
                    and (row.run_id != run_id or row.owner_id != owner_id)
                ):
                    raise CollectorLeaseConflict(f"collector lease is held by {row.run_id}")
                if row is None:
                    row = CollectorLeaseRow(
                        collector_group=collector_group,
                        run_id=run_id,
                        owner_id=owner_id,
                        acquired_at=now,
                        renewed_at=now,
                        expires_at=expires_at,
                    )
                    session.add(row)
                else:
                    row.run_id = run_id
                    row.owner_id = owner_id
                    row.acquired_at = now
                    row.renewed_at = now
                    row.expires_at = expires_at
            return CollectorLease(collector_group, run_id, owner_id, expires_at, now, now)
        except IntegrityError as exc:
            raise CollectorLeaseConflict("collector lease acquisition raced") from exc

    def renew(
        self,
        collector_group: str,
        run_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> CollectorLease:
        now = datetime.now(UTC)
        with Session(self.engine) as session, session.begin():
            row = session.get(CollectorLeaseRow, collector_group, with_for_update=True)
            if row is None or row.run_id != run_id or row.owner_id != owner_id:
                raise CollectorLeaseConflict("collector lease ownership changed")
            acquired_at = self._aware(row.acquired_at)
            row.renewed_at = now
            row.expires_at = expires_at
        return CollectorLease(collector_group, run_id, owner_id, expires_at, acquired_at, now)

    def release(self, collector_group: str, run_id: str, owner_id: str) -> None:
        with Session(self.engine) as session, session.begin():
            row = session.get(CollectorLeaseRow, collector_group, with_for_update=True)
            if row is not None and row.run_id == run_id and row.owner_id == owner_id:
                session.delete(row)

    def save_run(self, run: CollectorRunRecord) -> None:
        with Session(self.engine) as session, session.begin():
            row = session.get(CollectorRunRow, run.run_id)
            values = self._run_values(run)
            if row is None:
                session.add(CollectorRunRow(run_id=run.run_id, **values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)

    def get_run(self, run_id: str) -> CollectorRunRecord | None:
        with Session(self.engine) as session:
            row = session.get(CollectorRunRow, run_id)
            return self._run(row) if row is not None else None

    def list_runs(self) -> tuple[CollectorRunRecord, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(CollectorRunRow).order_by(CollectorRunRow.started_at)
            ).all()
            return tuple(self._run(row) for row in rows)

    def has_leases(self, run_id: str) -> bool:
        with Session(self.engine) as session:
            count = session.scalar(
                select(func.count())
                .select_from(CollectorLeaseRow)
                .where(CollectorLeaseRow.run_id == run_id)
            )
            return bool(count)

    @staticmethod
    def _run_values(run: CollectorRunRecord) -> dict[str, object]:
        return {
            "collector_group": run.collector_group,
            "owner_id": run.owner_id,
            "commit_sha": run.commit_sha,
            "config_path": run.config_path,
            "database_identity": run.database_identity,
            "schema_name": run.schema_name,
            "checkpoint_namespace": run.checkpoint_namespace,
            "artifact_namespace": run.artifact_namespace,
            "venues_json": json.dumps(run.venues),
            "instruments_json": json.dumps(run.instruments),
            "event_types_json": json.dumps(run.event_types),
            "duration_seconds": run.duration_seconds,
            "pid": run.pid,
            "status": run.status.value,
            "started_at": run.started_at,
            "heartbeat_at": run.heartbeat_at,
            "stop_requested_at": run.stop_requested_at,
            "stopped_at": run.stopped_at,
            "artifact_directory": run.artifact_directory,
            "failure_reason": run.failure_reason,
        }

    @classmethod
    def _run(cls, row: CollectorRunRow) -> CollectorRunRecord:
        return CollectorRunRecord(
            run_id=row.run_id,
            collector_group=row.collector_group,
            owner_id=row.owner_id,
            commit_sha=row.commit_sha,
            config_path=row.config_path,
            database_identity=row.database_identity,
            schema_name=row.schema_name,
            checkpoint_namespace=row.checkpoint_namespace,
            artifact_namespace=row.artifact_namespace,
            venues=tuple(json.loads(row.venues_json)),
            instruments=tuple(json.loads(row.instruments_json)),
            event_types=tuple(json.loads(row.event_types_json)),
            duration_seconds=row.duration_seconds,
            pid=row.pid,
            status=CollectorRunStatus(row.status),
            started_at=cls._aware(row.started_at),
            heartbeat_at=cls._aware(row.heartbeat_at),
            stop_requested_at=(
                cls._aware(row.stop_requested_at) if row.stop_requested_at else None
            ),
            stopped_at=cls._aware(row.stopped_at) if row.stopped_at else None,
            artifact_directory=row.artifact_directory,
            failure_reason=row.failure_reason,
        )

    @staticmethod
    def _aware(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
