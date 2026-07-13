from __future__ import annotations

import hashlib
import json
from pathlib import Path


def package_capability_audit(report: Path, manifest: Path, commit_sha: str) -> None:
    if not commit_sha.strip():
        raise ValueError("commit SHA is required")
    report_bytes = report.read_bytes()
    manifest.write_text(
        json.dumps(
            {
                "commit_sha": commit_sha,
                "report_file": report.name,
                "report_sha256": hashlib.sha256(report_bytes).hexdigest(),
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def verify_capability_audit(directory: Path, expected_commit_sha: str) -> None:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("commit_sha") != expected_commit_sha:
        raise ValueError("capability audit artifact commit SHA mismatch")
    report = directory / str(manifest.get("report_file", ""))
    actual_sha256 = hashlib.sha256(report.read_bytes()).hexdigest()
    if actual_sha256 != manifest.get("report_sha256"):
        raise ValueError("capability audit report SHA-256 mismatch")
