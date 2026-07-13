from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml  # type: ignore[import-untyped]

from app.domain.venues.models import CapabilitySupport, CapabilityUseCase, VenueCapabilityMatrix


@dataclass(frozen=True)
class TrustedCapabilityRecord:
    venue: str
    capability: str
    support: CapabilitySupport
    verification_run_id: str
    verified_at: datetime
    expires_at: datetime
    adapter_version: str
    source_version: str
    contract_fixture_sha256: str
    audit_run_id: str


class TrustedCapabilityRegistry:
    def __init__(self, records: tuple[TrustedCapabilityRecord, ...]) -> None:
        self._records = {(item.venue, item.capability): item for item in records}
        if len(self._records) != len(records):
            raise ValueError("duplicate trusted capability records")

    def __len__(self) -> int:
        return len(self._records)

    def require(
        self,
        *,
        venue: str,
        capability: str,
        verification_run_id: str,
        verified_at: datetime,
        use_case: CapabilityUseCase,
        now: datetime,
    ) -> TrustedCapabilityRecord:
        record = self._records.get((venue, capability))
        if record is None:
            raise ValueError("trusted capability record is missing")
        if record.support != CapabilitySupport.LIVE_VERIFIED:
            raise ValueError("trusted capability is not LIVE_VERIFIED")
        if record.verification_run_id != verification_run_id:
            raise ValueError("verification run id is not trusted")
        if record.verified_at != verified_at:
            raise ValueError("capability verification timestamp does not match trusted record")
        current = now.astimezone(UTC)
        if not record.verified_at <= current <= record.expires_at:
            raise ValueError("trusted capability record is expired or future-dated")
        if use_case not in {CapabilityUseCase.NEW_EXPOSURE, CapabilityUseCase.EMERGENCY_EXIT}:
            raise ValueError("capability use case is not executable")
        return record

    def require_live_verified(
        self, *, venue: str, capability: str, now: datetime
    ) -> TrustedCapabilityRecord:
        record = self._records.get((venue, capability))
        if record is None:
            raise ValueError("trusted capability record is missing")
        current = now.astimezone(UTC)
        if record.support != CapabilitySupport.LIVE_VERIFIED:
            raise ValueError("trusted capability is not LIVE_VERIFIED")
        if not record.verified_at <= current <= record.expires_at:
            raise ValueError("trusted capability record is expired or future-dated")
        return record

    @classmethod
    def from_artifacts(
        cls,
        root: Path,
        matrices: tuple[VenueCapabilityMatrix, ...],
    ) -> TrustedCapabilityRegistry:
        manifest = yaml.safe_load(
            (root / "tests/contracts/capability-manifest.yaml").read_text(encoding="utf-8")
        )
        entries = {
            (item["venue"], item["capability"]): item for item in manifest.get("entries", [])
        }
        smoke = json.loads(
            (root / "artifacts/venue-verification/live-smoke-manifest.json").read_text(
                encoding="utf-8"
            )
        )
        smoke_runs = {item["verification_run_id"]: item for item in smoke.get("runs", [])}
        audit_path = root / "artifacts/capability-audit/report.json"
        audit = json.loads(audit_path.read_text(encoding="utf-8")) if audit_path.is_file() else {}
        passed = {
            (item["venue"], item["capability"]): item
            for item in audit.get("findings", [])
            if item.get("passed") is True and item.get("test_result") == "passed"
        }
        records: list[TrustedCapabilityRecord] = []
        for matrix in matrices:
            for capability, value in matrix.capabilities.items():
                run = smoke_runs.get(value.verification_run_id or "")
                entry = entries.get((matrix.venue, capability))
                finding = passed.get((matrix.venue, capability))
                if not run or not entry or not finding or value.live_verified_at is None:
                    continue
                records.append(
                    TrustedCapabilityRecord(
                        venue=matrix.venue,
                        capability=capability,
                        support=value.support,
                        verification_run_id=value.verification_run_id or "",
                        verified_at=value.live_verified_at,
                        expires_at=datetime.fromisoformat(run["expires_at"]).astimezone(UTC),
                        adapter_version=str(run["adapter_version"]),
                        source_version=matrix.source_version,
                        contract_fixture_sha256=str(entry["contract_fixture"]["sha256"]),
                        audit_run_id=str(finding["audit_run_id"]),
                    )
                )
        return cls(tuple(records))
