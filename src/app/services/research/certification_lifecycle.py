from __future__ import annotations

import json
import logging
import os
import signal
import traceback
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.infrastructure.database.models import CertificationRunRow


class CertificationRunStatus(StrEnum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class CertificationStage(StrEnum):
    PROCESS_STARTED = "PROCESS_STARTED"
    CONFIG_LOADED = "CONFIG_LOADED"
    DB_CONNECTION_VERIFIED = "DB_CONNECTION_VERIFIED"
    MIGRATION_VERIFIED = "MIGRATION_VERIFIED"
    AUDIT_ARTIFACT_VERIFIED = "AUDIT_ARTIFACT_VERIFIED"
    ADAPTERS_CREATED = "ADAPTERS_CREATED"
    LEASE_ACQUIRED = "LEASE_ACQUIRED"
    COLLECTOR_STARTED = "COLLECTOR_STARTED"
    FIRST_EVENT_RECEIVED = "FIRST_EVENT_RECEIVED"
    CERTIFICATION_WINDOW_STARTED = "CERTIFICATION_WINDOW_STARTED"
    FINALIZATION_STARTED = "FINALIZATION_STARTED"
    ARTIFACT_WRITTEN = "ARTIFACT_WRITTEN"
    PROCESS_COMPLETED = "PROCESS_COMPLETED"


class CertificationCanceled(BaseException):
    def __init__(self, signal_number: int) -> None:
        super().__init__(f"normal shutdown signal {signal_number}")
        self.signal_number = signal_number


@dataclass(frozen=True)
class CertificationRunRecord:
    run_id: str
    status: CertificationRunStatus
    last_stage: str
    failure_reason: str | None
    commit_sha: str
    config_path: str
    database_identity: str
    artifact_directory: str
    pid: int
    parent_pid: int
    signal_number: int | None
    exit_code: int | None
    exception_type: str | None
    started_at: datetime
    updated_at: datetime


class CertificationRunRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save(self, record: CertificationRunRecord) -> None:
        with Session(self.engine) as session, session.begin():
            row = session.get(CertificationRunRow, record.run_id)
            values = asdict(record)
            values["status"] = record.status.value
            if row is None:
                session.add(CertificationRunRow(**values))
            else:
                for key, value in values.items():
                    if key != "run_id":
                        setattr(row, key, value)

    def get(self, run_id: str) -> CertificationRunRecord | None:
        with Session(self.engine) as session:
            row = session.get(CertificationRunRow, run_id)
            if row is None:
                return None
            return CertificationRunRecord(
                run_id=row.run_id,
                status=CertificationRunStatus(row.status),
                last_stage=row.last_stage,
                failure_reason=row.failure_reason,
                commit_sha=row.commit_sha,
                config_path=row.config_path,
                database_identity=row.database_identity,
                artifact_directory=row.artifact_directory,
                pid=row.pid,
                parent_pid=row.parent_pid,
                signal_number=row.signal_number,
                exit_code=row.exit_code,
                exception_type=row.exception_type,
                started_at=_aware(row.started_at),
                updated_at=_aware(row.updated_at),
            )


class CertificationLifecycle:
    def __init__(
        self,
        *,
        run_id: str,
        commit_sha: str,
        config_path: Path,
        database_identity: str,
        artifact_directory: Path,
        repository: CertificationRunRepository,
        now: datetime | None = None,
    ) -> None:
        started = now or datetime.now(UTC)
        self.artifact_directory = artifact_directory
        self.artifact_directory.mkdir(parents=True, exist_ok=True)
        self.log_path = artifact_directory / "lifecycle.jsonl"
        self.repository = repository
        self.record = CertificationRunRecord(
            run_id=run_id,
            status=CertificationRunStatus.STARTING,
            last_stage=CertificationStage.PROCESS_STARTED.value,
            failure_reason=None,
            commit_sha=commit_sha,
            config_path=str(config_path.resolve()),
            database_identity=database_identity,
            artifact_directory=str(artifact_directory.resolve()),
            pid=os.getpid(),
            parent_pid=os.getppid(),
            signal_number=None,
            exit_code=None,
            exception_type=None,
            started_at=started,
            updated_at=started,
        )
        self.repository.save(self.record)
        self.stage(CertificationStage.PROCESS_STARTED)

    def stage(self, stage: CertificationStage) -> None:
        now = datetime.now(UTC)
        status = (
            CertificationRunStatus.RUNNING
            if stage
            not in {CertificationStage.PROCESS_STARTED, CertificationStage.PROCESS_COMPLETED}
            else self.record.status
        )
        if stage is CertificationStage.PROCESS_COMPLETED:
            status = CertificationRunStatus.COMPLETED
        self.record = replace(
            self.record,
            status=status,
            last_stage=stage.value,
            updated_at=now,
            exit_code=0 if stage is CertificationStage.PROCESS_COMPLETED else self.record.exit_code,
        )
        self.repository.save(self.record)
        self._write({"stage": stage.value, "timestamp": now.isoformat()})

    def fail(
        self,
        exc: BaseException,
        *,
        exit_code: int = 1,
        signal_number: int | None = None,
        canceled: bool = False,
    ) -> Path:
        now = datetime.now(UTC)
        status = CertificationRunStatus.CANCELED if canceled else CertificationRunStatus.FAILED
        message = str(exc) or type(exc).__name__
        self.record = replace(
            self.record,
            status=status,
            failure_reason=message,
            signal_number=signal_number,
            exit_code=exit_code,
            exception_type=type(exc).__name__,
            updated_at=now,
        )
        self.repository.save(self.record)
        crash = {
            **asdict(self.record),
            "status": status.value,
            "traceback": "".join(traceback.format_exception(exc)),
        }
        path = self.artifact_directory / "crash.json"
        path.write_text(json.dumps(crash, default=str, indent=2, sort_keys=True) + "\n")
        self._write(
            {
                "stage": self.record.last_stage,
                "timestamp": now.isoformat(),
                "exception_type": type(exc).__name__,
                "exception_message": message,
                "signal_number": signal_number,
                "exit_code": exit_code,
                "status": status.value,
            }
        )
        return path

    def _write(self, values: dict[str, object]) -> None:
        payload = {
            "run_id": self.record.run_id,
            "pid": self.record.pid,
            "commit_sha": self.record.commit_sha,
            "config_path": self.record.config_path,
            "database_identity": self.record.database_identity,
            **values,
        }
        line = json.dumps(payload, sort_keys=True)
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        logging.getLogger("certification.lifecycle").info(line)
        for handler in logging.getLogger().handlers:
            handler.flush()
        print(line, flush=True)


def install_shutdown_signal_handlers() -> dict[int, object]:
    previous: dict[int, object] = {}

    def handle(number: int, _: object) -> None:
        raise CertificationCanceled(number)

    for number in (signal.SIGINT, signal.SIGTERM):
        previous[number] = signal.getsignal(number)
        signal.signal(number, handle)
    return previous


def restore_signal_handlers(previous: dict[int, object]) -> None:
    for number, handler in previous.items():
        signal.signal(number, handler)  # type: ignore[arg-type]


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
