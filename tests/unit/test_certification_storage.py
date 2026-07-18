from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.infrastructure.database.models import Base, MarketDataCertificationRow
from app.services.research import certification_storage as storage
from app.services.research.certification_lifecycle import (
    CertificationLifecycle,
    CertificationRunRepository,
    CertificationStage,
)


@pytest.fixture(autouse=True)
def allow_test_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(storage.TEST_STORAGE_OVERRIDE_ENV, "1")


def completed_run(
    tmp_path: Path, *, count: int = 2
) -> tuple[object, storage.CertificationRunWorkspace]:
    state = tmp_path / "state"
    workspace = storage.CertificationRunWorkspace(state, "durable-run")
    workspace.initialize()
    source = tmp_path / "source.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "database_url": f"sqlite+pysqlite:///{tmp_path / 'certification.db'}",
                "market_data_certification": {"enabled": True},
            }
        ),
        encoding="utf-8",
    )
    workspace.persist_resolved_config(source)
    workspace.persist_environment({"HOME": str(Path.home()), "API_KEY": "secret"})
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'certification.db'}")
    Base.metadata.create_all(engine)
    lifecycle = CertificationLifecycle(
        run_id=workspace.run_id,
        commit_sha="a" * 40,
        config_path=workspace.config_path,
        database_identity="sqlite:durable-test",
        artifact_directory=workspace.root,
        repository=CertificationRunRepository(engine),
        now=datetime(2026, 7, 16, tzinfo=UTC),
    )
    lifecycle.stage(CertificationStage.ARTIFACT_WRITTEN)
    lifecycle.stage(CertificationStage.PROCESS_COMPLETED)
    now = datetime.now(UTC)
    with Session(engine) as session, session.begin():
        for index in range(count):
            certification_id = f"{workspace.run_id}-venue-BTC-capability{index}"
            certification = {
                "certification_id": certification_id,
                "verdict": "pass",
                "reasons": [],
            }
            evidence = {
                "certification_id": certification_id,
                "metrics": {"live_out_of_order_count": 0},
                "capability_contract": {"schema_valid": True},
                "historical_research_usability": {"verdict": "pass"},
                "funding_interval": None,
                "audit_passed": False,
                "audit_artifact_sha256": "",
            }
            session.add(
                MarketDataCertificationRow(
                    certification_id=certification_id,
                    venue="hyperliquid",
                    capability=f"capability{index}",
                    canonical_instrument_id="BTC",
                    verdict="pass",
                    commit_sha="a" * 40,
                    adapter_version="adapter-v1",
                    verified_at=now,
                    expires_at=now + timedelta(hours=1),
                    evidence_manifest_sha256=str(index).zfill(64),
                    payload_json=json.dumps(certification),
                    evidence_json=json.dumps(evidence),
                    created_at=now,
                )
            )
    return engine, workspace


def finalize(engine: object, workspace: storage.CertificationRunWorkspace) -> dict[str, object]:
    return storage.export_completed_run(
        engine=engine,  # type: ignore[arg-type]
        workspace=workspace,
        adapter_version="adapter-v1",
        database_identity="sqlite:durable-test",
    )


def test_temporary_state_directory_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv(storage.TEST_STORAGE_OVERRIDE_ENV)
    monkeypatch.setenv(storage.STATE_DIR_ENV, str(tmp_path / "state"))
    with pytest.raises(storage.DurableStorageError, match="temporary"):
        storage.configured_state_dir()


def test_temporary_clone_executable_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv(storage.TEST_STORAGE_OVERRIDE_ENV)
    monkeypatch.setattr(storage, "is_temporary_path", lambda _path: False)
    checkout = tmp_path / "checkout"
    executable = checkout / ".venv/bin/python"
    executable.parent.mkdir(parents=True)
    executable.write_text("python", encoding="utf-8")
    (checkout / ".git").write_text("gitdir: ephemeral", encoding="utf-8")
    with pytest.raises(storage.DurableStorageError, match="worktree clone"):
        storage.require_durable_path(
            executable, purpose="Python executable", executable_checkout=True
        )


def test_durable_config_survives_process_exit(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path)
    engine.dispose()  # type: ignore[union-attr]
    assert yaml.safe_load(workspace.config_path.read_text())["market_data_certification"][
        "artifact_root"
    ].startswith(str(workspace.state_dir))


def test_durable_artifact_survives_service_restart(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path)
    finalize(engine, workspace)
    engine.dispose()  # type: ignore[union-attr]
    reopened = storage.CertificationRunWorkspace(workspace.state_dir, workspace.run_id)
    assert storage.reconstruct_from_artifacts(reopened)["artifact_only_reconstruction"] == "PASS"


def test_named_postgresql_volume_is_required(tmp_path: Path) -> None:
    compose = tmp_path / "compose.yaml"
    compose.write_text("services:\n  db:\n    volumes: []\nvolumes: {}\n", encoding="utf-8")
    assert not storage.named_postgres_volume_configured(compose)
    compose.write_text(
        "services:\n  db:\n    volumes:\n"
        "      - crypttool_certification_pgdata:/var/lib/postgresql/data\n"
        "volumes:\n  crypttool_certification_pgdata:\n    name: crypttool_certification_pgdata\n",
        encoding="utf-8",
    )
    assert storage.named_postgres_volume_configured(compose)


def test_completed_run_exports_all_certification_records(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path, count=3)
    result = finalize(engine, workspace)
    records = json.loads((workspace.root / "certification-records.json").read_text())
    assert result["certification_record_count"] == len(records) == 3
    assert all("evidence" in item and "certification" in item for item in records)


def test_db_and_artifact_verdict_mismatch_fails_verification(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path)
    finalize(engine, workspace)
    with Session(engine) as session, session.begin():  # type: ignore[arg-type]
        row = session.get(MarketDataCertificationRow, f"{workspace.run_id}-venue-BTC-capability0")
        assert row is not None
        row.verdict = "fail"
    with pytest.raises(storage.DurableStorageError, match="db_artifact_verdicts"):
        storage.verify_run(engine, workspace)  # type: ignore[arg-type]


def test_missing_manifest_hash_fails_verification(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path)
    finalize(engine, workspace)
    (workspace.root / "COMPLETED").write_text("", encoding="utf-8")
    with pytest.raises(storage.DurableStorageError, match="manifest SHA-256 mismatch"):
        storage.verify_run(engine, workspace)  # type: ignore[arg-type]


def test_verification_succeeds_after_db_restart(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path)
    finalize(engine, workspace)
    url = engine.url  # type: ignore[union-attr]
    engine.dispose()  # type: ignore[union-attr]
    restarted = create_engine(url)
    assert storage.verify_run(restarted, workspace)["status"] == "PASS"
    restarted.dispose()


def test_run_result_can_be_reconstructed_from_artifacts_only(tmp_path: Path) -> None:
    engine, workspace = completed_run(tmp_path)
    finalize(engine, workspace)
    engine.dispose()  # type: ignore[union-attr]
    result = storage.reconstruct_from_artifacts(workspace)
    assert result["artifact_only_reconstruction"] == "PASS"
    assert result["certification_record_count"] == 2
    assert result["verdicts"] == ["pass", "pass"]


def test_environment_export_excludes_secret_values(tmp_path: Path) -> None:
    _engine, workspace = completed_run(tmp_path)
    environment = json.loads((workspace.root / "environment.json").read_text())
    assert environment == {"HOME": str(Path.home())}
