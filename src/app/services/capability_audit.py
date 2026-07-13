from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from app.adapters.exchanges.base import MarketDataAdapter
from app.adapters.exchanges.public import PublicRestAdapter
from app.domain.venues.models import CapabilitySupport, VenueCapabilityMatrix


@dataclass(frozen=True)
class CapabilityAuditFinding:
    capability: str
    passed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CapabilityAuditReport:
    venue: str
    audited_at: datetime
    findings: tuple[CapabilityAuditFinding, ...]

    @property
    def passed(self) -> bool:
        return bool(self.findings) and all(item.passed for item in self.findings)


class CapabilityContractAuditor:
    def __init__(
        self,
        repository_root: Path,
        *,
        maximum_age: timedelta = timedelta(days=7),
        now: datetime | None = None,
    ) -> None:
        self.root = repository_root.resolve()
        manifest = yaml.safe_load(
            (self.root / "tests/contracts/capability-manifest.yaml").read_text()
        )
        self.entries = {
            (item["venue"], item["capability"]): item for item in manifest.get("entries", [])
        }
        smoke = json.loads(
            (self.root / "artifacts/venue-verification/live-smoke-manifest.json").read_text()
        )
        self.smoke_runs = {item["verification_run_id"]: item for item in smoke.get("runs", [])}
        self.maximum_age = maximum_age
        self.now = (now or datetime.now(UTC)).astimezone(UTC)

    def audit(
        self, adapter: MarketDataAdapter, matrix: VenueCapabilityMatrix
    ) -> CapabilityAuditReport:
        findings: list[CapabilityAuditFinding] = []
        for name, capability in matrix.capabilities.items():
            if capability.support not in {
                CapabilitySupport.IMPLEMENTED,
                CapabilitySupport.LIVE_VERIFIED,
            }:
                continue
            reasons: list[str] = []
            entry = self.entries.get((matrix.venue, name))
            if entry is None:
                reasons.append("evidence manifest entry is missing")
            else:
                method_name = str(entry.get("adapter_method", ""))
                owner = self._implementation_owner(type(adapter), method_name)
                if owner in {None, MarketDataAdapter, PublicRestAdapter}:
                    reasons.append("adapter method resolves to an unavailable fallback owner")
                fixture = self.root / str(entry.get("contract_fixture", {}).get("path", ""))
                if not fixture.is_file():
                    reasons.append("contract fixture is missing")
                elif hashlib.sha256(fixture.read_bytes()).hexdigest() != entry[
                    "contract_fixture"
                ].get("sha256"):
                    reasons.append("contract fixture sha256 does not match")
                test_id = str(entry.get("normalization_test", {}).get("test_id", ""))
                test_path = self.root / test_id.partition("::")[0]
                if "::" not in test_id or not test_path.is_file():
                    reasons.append("normalization test evidence is missing")
            if capability.support == CapabilitySupport.LIVE_VERIFIED:
                run_id = capability.verification_run_id
                run = self.smoke_runs.get(run_id or "")
                if not run or capability.live_verified_at is None:
                    reasons.append("live smoke evidence is missing")
                elif not timedelta(0) <= self.now - capability.live_verified_at <= self.maximum_age:
                    reasons.append("live verification is stale or future-dated")
            findings.append(CapabilityAuditFinding(name, not reasons, tuple(reasons)))
        return CapabilityAuditReport(matrix.venue, self.now, tuple(findings))

    @staticmethod
    def _implementation_owner(adapter_type: type[Any], method_name: str) -> type[Any] | None:
        for owner in adapter_type.__mro__:
            if method_name in owner.__dict__:
                return owner
        return None
