"""Durable, restart-safe storage for certification run evidence."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.infrastructure.database.models import (
    CapabilityPromotionRow,
    CertificationRunRow,
    CollectorLeaseRow,
    MarketDataCertificationRow,
    RawMarketEventRow,
)

STATE_DIR_ENV = "CRYPTTOOL_STATE_DIR"
TEST_STORAGE_OVERRIDE_ENV = "CRYPTTOOL_ALLOW_TEST_STORAGE"
REQUIRED_RUN_FILES = (
    "run.json",
    "config.resolved.yaml",
    "environment.json",
    "stdout.log",
    "stderr.log",
    "lifecycle.jsonl",
    "manifest.json",
    "metrics.json",
    "failures.csv",
    "crash.json",
)


class DurableStorageError(RuntimeError):
    """Raised when evidence cannot be proved durable and internally consistent."""


def _test_storage_override() -> bool:
    return (
        bool(os.environ.get("PYTEST_CURRENT_TEST"))
        and os.environ.get(TEST_STORAGE_OVERRIDE_ENV) == "1"
    )


def is_temporary_path(path: Path) -> bool:
    resolved = path.expanduser().resolve()
    forbidden = {
        Path("/tmp").resolve(),
        Path("/private/tmp").resolve(),
        Path(tempfile.gettempdir()).resolve(),
    }
    if any(resolved == root or root in resolved.parents for root in forbidden):
        return True
    return Path("/var/folders") == resolved or Path("/var/folders") in resolved.parents


def require_durable_path(path: Path, *, purpose: str, executable_checkout: bool = False) -> Path:
    resolved = path.expanduser().resolve()
    if is_temporary_path(resolved) and not _test_storage_override():
        raise DurableStorageError(f"{purpose} must not use an OS temporary directory: {resolved}")
    if executable_checkout:
        checkout = resolved if resolved.is_dir() else resolved.parent
        for parent in (checkout, *checkout.parents):
            git = parent / ".git"
            if git.is_file() and not _test_storage_override():
                raise DurableStorageError(
                    f"{purpose} must not use a temporary/worktree clone: {resolved}"
                )
            if git.is_dir():
                break
    return resolved


def configured_state_dir() -> Path:
    raw = os.environ.get(STATE_DIR_ENV)
    if not raw:
        raise DurableStorageError(f"{STATE_DIR_ENV} is required for certification runs")
    path = require_durable_path(Path(raw), purpose="certification state directory")
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_bytes(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    return str(value)


def atomic_write_json(path: Path, payload: object) -> None:
    encoded = (json.dumps(payload, default=_json_default, indent=2, sort_keys=True) + "\n").encode()
    atomic_write_bytes(path, encoded)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class CertificationRunWorkspace:
    state_dir: Path
    run_id: str

    @property
    def root(self) -> Path:
        return self.state_dir / "certification-runs" / self.run_id

    @property
    def certifications(self) -> Path:
        return self.root / "certifications"

    @property
    def config_path(self) -> Path:
        return self.root / "config.resolved.yaml"

    @property
    def stdout_path(self) -> Path:
        return self.root / "stdout.log"

    @property
    def stderr_path(self) -> Path:
        return self.root / "stderr.log"

    @property
    def plist_path(self) -> Path:
        return self.state_dir / "launch-agents" / f"com.crypttool.{self.run_id}.plist"

    def initialize(self) -> None:
        require_durable_path(self.root, purpose="certification run workspace")
        self.certifications.mkdir(parents=True, exist_ok=True)
        for name in ("stdout.log", "stderr.log", "lifecycle.jsonl", "failures.csv", "crash.json"):
            path = self.root / name
            if not path.exists():
                atomic_write_bytes(path, b"")

    def persist_resolved_config(
        self, source: Path, *, resolved_payload: dict[str, Any] | None = None
    ) -> Path:
        payload = resolved_payload or yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise DurableStorageError("certification config must be a YAML mapping")
        certification = dict(payload.get("market_data_certification") or {})
        certification["artifact_root"] = str(self.state_dir / "certification-runs")
        payload["market_data_certification"] = certification
        atomic_write_bytes(self.config_path, yaml.safe_dump(payload, sort_keys=True).encode())
        return self.config_path

    def persist_environment(self, environment: dict[str, str]) -> None:
        safe = {
            key: value
            for key, value in environment.items()
            if not any(token in key.upper() for token in ("KEY", "SECRET", "TOKEN", "PASSWORD"))
        }
        atomic_write_json(self.root / "environment.json", safe)


def workspace_for(run_id: str, state_dir: Path | None = None) -> CertificationRunWorkspace:
    root = state_dir or configured_state_dir()
    workspace = CertificationRunWorkspace(root, run_id)
    return workspace


def _record_payload(row: MarketDataCertificationRow) -> dict[str, Any]:
    return {
        "certification": json.loads(row.payload_json),
        "evidence": json.loads(row.evidence_json),
    }


def _run_rows(engine: Engine, run_id: str) -> tuple[Any, list[Any], list[Any], int, int]:
    with Session(engine) as session:
        run = session.get(CertificationRunRow, run_id)
        certifications = list(
            session.scalars(
                select(MarketDataCertificationRow)
                .where(MarketDataCertificationRow.certification_id.like(f"{run_id}-%"))
                .order_by(MarketDataCertificationRow.certification_id)
            ).all()
        )
        promotions = list(
            session.scalars(
                select(CapabilityPromotionRow)
                .where(CapabilityPromotionRow.certification_id.like(f"{run_id}-%"))
                .order_by(CapabilityPromotionRow.certification_id)
            ).all()
        )
        lease_count = int(
            session.scalar(
                select(func.count())
                .select_from(CollectorLeaseRow)
                .where(CollectorLeaseRow.run_id == run_id)
            )
            or 0
        )
        production_count = int(
            session.scalar(
                select(func.count())
                .select_from(RawMarketEventRow)
                .where(RawMarketEventRow.capability_verification_run_id.like(f"{run_id}-%"))
            )
            or 0
        )
    return run, certifications, promotions, lease_count, production_count


def export_completed_run(
    *,
    engine: Engine,
    workspace: CertificationRunWorkspace,
    adapter_version: str,
    database_identity: str,
) -> dict[str, Any]:
    """Export every result and atomically seal a completed run."""
    run, rows, promotions, lease_count, production_count = _run_rows(engine, workspace.run_id)
    if run is None or run.status != "COMPLETED" or run.exit_code != 0:
        raise DurableStorageError("Run Registry is not COMPLETED with exit code 0")
    if lease_count:
        raise DurableStorageError(f"cannot finalize with {lease_count} remaining lease(s)")
    records = [_record_payload(row) for row in rows]
    promotion_payloads = [json.loads(row.payload_json) for row in promotions]
    atomic_write_json(workspace.root / "certification-records.json", records)
    atomic_write_json(workspace.root / "promotions.json", promotion_payloads)
    metrics = {
        "certification_record_count": len(records),
        "verdicts": [item["certification"]["verdict"] for item in records],
        "capability_verdicts": [item["certification"] for item in records],
        "contract_verdicts": [item["evidence"].get("capability_contract") for item in records],
        "historical_research_usability": [
            item["evidence"].get("historical_research_usability") for item in records
        ],
        "timing_metrics": [item["evidence"].get("metrics") for item in records],
        "funding_metrics": [item["evidence"].get("funding_interval") for item in records],
        "trade_ordering_metrics": [
            {
                "certification_id": item["certification"]["certification_id"],
                "live_out_of_order_count": item["evidence"]
                .get("metrics", {})
                .get("live_out_of_order_count", 0),
            }
            for item in records
        ],
        "audit_binding": [
            {
                "certification_id": item["certification"]["certification_id"],
                "audit_passed": item["evidence"].get("audit_passed"),
                "audit_artifact_sha256": item["evidence"].get("audit_artifact_sha256"),
            }
            for item in records
        ],
        "promotion_count": len(promotions),
        "production_market_event_count": production_count,
        "remaining_lease_count": lease_count,
        "live_execution": "OFF",
    }
    atomic_write_json(workspace.root / "metrics.json", metrics)
    failures = StringIO()
    writer = csv.writer(failures)
    writer.writerow(("certification_id", "verdict", "reason"))
    for item in records:
        certification = item["certification"]
        for reason in certification.get("reasons", []):
            writer.writerow((certification["certification_id"], certification["verdict"], reason))
    atomic_write_bytes(workspace.root / "failures.csv", failures.getvalue().encode())
    config_sha = sha256_file(workspace.config_path)
    audit_hashes = sorted(
        {
            str(item["evidence"].get("audit_artifact_sha256", ""))
            for item in records
            if item["evidence"].get("audit_artifact_sha256")
        }
    )
    run_payload = {
        "run_id": workspace.run_id,
        "commit_sha": run.commit_sha,
        "config_sha256": config_sha,
        "adapter_version": adapter_version,
        "audit_artifact_sha256": audit_hashes[0] if len(audit_hashes) == 1 else audit_hashes,
        "started_at": run.started_at,
        "completed_at": run.updated_at,
        "status": run.status,
        "exit_code": run.exit_code,
        "database_identity": database_identity,
        "artifact_manifest_sha256": None,
    }
    evidence_files = (
        "config.resolved.yaml",
        "environment.json",
        "lifecycle.jsonl",
        "metrics.json",
        "certification-records.json",
        "promotions.json",
        "failures.csv",
    )
    manifest = {
        "format": "crypttool-certification-run-v1",
        "run": run_payload,
        "files": {name: sha256_file(workspace.root / name) for name in evidence_files},
        "certification_record_count": len(records),
        "verdicts": [item["certification"]["verdict"] for item in records],
        "live_execution": "OFF",
    }
    atomic_write_json(workspace.root / "manifest.json", manifest)
    manifest_sha = sha256_file(workspace.root / "manifest.json")
    run_payload["artifact_manifest_sha256"] = manifest_sha
    atomic_write_json(workspace.root / "run.json", run_payload)
    atomic_write_bytes(workspace.root / "COMPLETED", (manifest_sha + "\n").encode())
    return {**run_payload, "certification_record_count": len(records), "verified": True}


def reconstruct_from_artifacts(workspace: CertificationRunWorkspace) -> dict[str, Any]:
    manifest_path = workspace.root / "manifest.json"
    marker_path = workspace.root / "COMPLETED"
    run_path = workspace.root / "run.json"
    if not (manifest_path.is_file() and marker_path.is_file() and run_path.is_file()):
        raise DurableStorageError("completed marker, manifest, or run record is missing")
    expected = marker_path.read_text(encoding="utf-8").strip()
    actual = sha256_file(manifest_path)
    run = json.loads(run_path.read_text(encoding="utf-8"))
    if expected != actual or run.get("artifact_manifest_sha256") != actual:
        raise DurableStorageError("manifest SHA-256 mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for relative, digest in manifest["files"].items():
        path = workspace.root / relative
        if not path.is_file() or sha256_file(path) != digest:
            raise DurableStorageError(f"artifact hash mismatch: {relative}")
    records = json.loads((workspace.root / "certification-records.json").read_text())
    if len(records) != manifest["certification_record_count"]:
        raise DurableStorageError("artifact certification count mismatch")
    return {
        "run": run,
        "certification_record_count": len(records),
        "verdicts": [item["certification"]["verdict"] for item in records],
        "artifact_only_reconstruction": "PASS",
        "live_execution": manifest["live_execution"],
    }


def verify_run(engine: Engine, workspace: CertificationRunWorkspace) -> dict[str, Any]:
    artifact = reconstruct_from_artifacts(workspace)
    run, rows, _promotions, lease_count, _production_count = _run_rows(engine, workspace.run_id)
    if run is None:
        raise DurableStorageError("Run Registry record is missing")
    checks = {
        "run_registry": run.status == "COMPLETED",
        "exit_code": run.exit_code == 0,
        "lifecycle_complete": run.last_stage == "PROCESS_COMPLETED",
        "commit_sha": run.commit_sha == artifact["run"]["commit_sha"],
        "adapter_version": all(
            row.adapter_version == artifact["run"]["adapter_version"] for row in rows
        ),
        "lease_zero": lease_count == 0,
        "db_certification_count": len(rows) == artifact["certification_record_count"],
    }
    artifact_records = json.loads(
        (workspace.root / "certification-records.json").read_text(encoding="utf-8")
    )
    db_verdicts = {row.certification_id: row.verdict for row in rows}
    artifact_verdicts = {
        item["certification"]["certification_id"]: item["certification"]["verdict"]
        for item in artifact_records
    }
    checks["db_artifact_verdicts"] = db_verdicts == artifact_verdicts
    checks["audit_binding"] = all(
        item["evidence"].get("audit_passed") is False
        or len(str(item["evidence"].get("audit_artifact_sha256", ""))) == 64
        for item in artifact_records
    )
    failures = sorted(name for name, passed in checks.items() if not passed)
    if failures:
        raise DurableStorageError("certification verification failed: " + ", ".join(failures))
    return {
        "run_id": workspace.run_id,
        "status": "PASS",
        "checks": checks,
        "artifact_only_reconstruction": "PASS",
        "live_execution": "OFF",
    }


def named_postgres_volume_configured(compose_path: Path) -> bool:
    payload = yaml.safe_load(compose_path.read_text(encoding="utf-8")) or {}
    services = payload.get("services") or {}
    database = services.get("db") or {}
    mounts = database.get("volumes") or []
    volumes = payload.get("volumes") or {}
    return (
        any(
            isinstance(mount, str) and mount.startswith("crypttool_certification_pgdata:")
            for mount in mounts
        )
        and "crypttool_certification_pgdata" in volumes
    )
