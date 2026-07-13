import json

import pytest

from app.services.capability_audit_artifact import (
    package_capability_audit,
    verify_capability_audit,
)


def test_capability_audit_artifact_binds_commit_and_report_hash(tmp_path) -> None:
    report = tmp_path / "report.json"
    manifest = tmp_path / "manifest.json"
    report.write_text('{"result":"pass"}\n', encoding="utf-8")
    package_capability_audit(report, manifest, "commit-123")

    verify_capability_audit(tmp_path, "commit-123")
    assert json.loads(manifest.read_text(encoding="utf-8"))["commit_sha"] == "commit-123"

    with pytest.raises(ValueError, match="commit SHA"):
        verify_capability_audit(tmp_path, "other-commit")
    report.write_text('{"result":"tampered"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="SHA-256"):
        verify_capability_audit(tmp_path, "commit-123")
