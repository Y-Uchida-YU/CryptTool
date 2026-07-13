from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
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
    test_node_id: str = ""
    test_file_sha256: str = ""
    test_result: str = "not_run"
    ci_run_id: str = ""
    audit_run_id: str = ""
    audited_at: datetime | None = None


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
        test_runner: Callable[[list[str]], int] | None = None,
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
        self.ci_run_id = os.getenv("GITHUB_RUN_ID", "local")
        self.audit_run_id = f"capability-audit-{self.ci_run_id}-{int(self.now.timestamp())}"
        self._test_results: dict[str, tuple[str, str]] = {}
        self._test_runner = test_runner or self._run_pytest

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
            test_id = ""
            test_file_sha256 = ""
            test_result = "not_run"
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
                else:
                    test_file_sha256 = hashlib.sha256(test_path.read_bytes()).hexdigest()
                    test_result, test_failure = self._verify_test_node(test_id)
                    if test_failure:
                        reasons.append(test_failure)
            if capability.support == CapabilitySupport.LIVE_VERIFIED:
                run_id = capability.verification_run_id
                run = self.smoke_runs.get(run_id or "")
                if not run or capability.live_verified_at is None:
                    reasons.append("live smoke evidence is missing")
                elif not timedelta(0) <= self.now - capability.live_verified_at <= self.maximum_age:
                    reasons.append("live verification is stale or future-dated")
            findings.append(
                CapabilityAuditFinding(
                    name,
                    not reasons,
                    tuple(reasons),
                    test_id,
                    test_file_sha256,
                    test_result,
                    self.ci_run_id,
                    self.audit_run_id,
                    self.now,
                )
            )
        return CapabilityAuditReport(matrix.venue, self.now, tuple(findings))

    def _verify_test_node(self, test_id: str) -> tuple[str, str | None]:
        cached = self._test_results.get(test_id)
        if cached is not None:
            return cached[0], cached[1] or None
        collect_result = self._test_runner(["--collect-only", "-q", test_id])
        if collect_result != 0:
            result = ("not_collected", "normalization test node id is not collectable")
        else:
            executed_result = self._test_runner(["-q", test_id])
            result = (
                "passed" if executed_result == 0 else "failed",
                "" if executed_result == 0 else "normalization test did not pass",
            )
        self._test_results[test_id] = result
        return result[0], result[1] or None

    def _run_pytest(self, arguments: list[str]) -> int:
        import pytest

        previous = Path.cwd()
        try:
            os.chdir(self.root)
            return int(pytest.main(arguments))
        finally:
            os.chdir(previous)

    def write_artifact(self, reports: tuple[CapabilityAuditReport, ...]) -> Path:
        path = self.root / "artifacts/capability-audit/report.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        findings = [
            {"venue": report.venue, **asdict(finding)}
            for report in reports
            for finding in report.findings
        ]
        path.write_text(
            json.dumps(
                {"audit_run_id": self.audit_run_id, "audited_at": self.now, "findings": findings},
                default=str,
                sort_keys=True,
                indent=2,
            ),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _implementation_owner(adapter_type: type[Any], method_name: str) -> type[Any] | None:
        for owner in adapter_type.__mro__:
            if method_name in owner.__dict__:
                return owner
        return None
