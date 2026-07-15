from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine

from app.infrastructure.database.models import Base
from app.services.research.certification_lifecycle import (
    CertificationLifecycle,
    CertificationRunRepository,
    CertificationRunStatus,
    CertificationStage,
)

COMMIT = "a" * 40


def lifecycle(tmp_path: Path) -> tuple[CertificationLifecycle, CertificationRunRepository]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    repository = CertificationRunRepository(engine)
    value = CertificationLifecycle(
        run_id="certification-test",
        commit_sha=COMMIT,
        config_path=tmp_path / "config.yaml",
        database_identity="sqlite:test",
        artifact_directory=tmp_path / "artifacts",
        repository=repository,
        now=datetime(2026, 7, 15, tzinfo=UTC),
    )
    return value, repository


def test_run_registry_exists_before_adapter_startup(tmp_path: Path) -> None:
    value, repository = lifecycle(tmp_path)
    record = repository.get("certification-test")
    assert record is not None
    assert record.status is CertificationRunStatus.STARTING
    assert record.last_stage == CertificationStage.PROCESS_STARTED
    assert value.log_path.is_file()


def test_startup_exception_creates_failed_record_and_crash_artifact(tmp_path: Path) -> None:
    value, repository = lifecycle(tmp_path)
    value.stage(CertificationStage.DB_CONNECTION_VERIFIED)
    crash = value.fail(RuntimeError("adapter startup failed"))
    record = repository.get("certification-test")
    assert record is not None and record.status is CertificationRunStatus.FAILED
    assert record.last_stage == CertificationStage.DB_CONNECTION_VERIFIED
    payload = json.loads(crash.read_text())
    assert payload["exception_type"] == "RuntimeError"
    assert "adapter startup failed" in payload["traceback"]


def test_failure_reason_is_bounded_for_registry_storage(tmp_path: Path) -> None:
    value, repository = lifecycle(tmp_path)
    value.fail(RuntimeError("x" * 3000))
    record = repository.get("certification-test")
    assert record is not None
    assert record.status is CertificationRunStatus.FAILED
    assert record.failure_reason == "x" * 2000


def test_normal_sigterm_marks_canceled(tmp_path: Path) -> None:
    value, repository = lifecycle(tmp_path)
    value.fail(
        RuntimeError("normal shutdown signal 15"),
        exit_code=143,
        signal_number=15,
        canceled=True,
    )
    record = repository.get("certification-test")
    assert record is not None and record.status is CertificationRunStatus.CANCELED
    assert record.signal_number == 15


def test_unexpected_signal_marks_failed(tmp_path: Path) -> None:
    value, repository = lifecycle(tmp_path)
    value.fail(RuntimeError("unexpected signal"), exit_code=138, signal_number=10)
    record = repository.get("certification-test")
    assert record is not None and record.status is CertificationRunStatus.FAILED
    assert record.signal_number == 10


def test_zero_event_run_still_emits_artifact(tmp_path: Path) -> None:
    value, repository = lifecycle(tmp_path)
    value.stage(CertificationStage.FINALIZATION_STARTED)
    manifest = value.artifact_directory / "manifest.json"
    manifest.write_text('{"event_count":0}\n')
    value.stage(CertificationStage.ARTIFACT_WRITTEN)
    value.stage(CertificationStage.PROCESS_COMPLETED)
    record = repository.get("certification-test")
    assert manifest.is_file()
    assert record is not None and record.status is CertificationRunStatus.COMPLETED


def test_detached_runner_survives_parent_shell_exit(tmp_path: Path) -> None:
    marker = tmp_path / "detached.txt"
    child = f"import pathlib,time;time.sleep(0.2);pathlib.Path({str(marker)!r}).write_text('alive')"
    parent = (
        "import subprocess,sys;"
        f"subprocess.Popen([sys.executable,'-c',{child!r}],start_new_session=True,"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)"
    )
    subprocess.run([sys.executable, "-c", parent], check=True)
    for _ in range(10):
        if marker.is_file():
            break
        time.sleep(0.05)
    assert marker.read_text() == "alive"
