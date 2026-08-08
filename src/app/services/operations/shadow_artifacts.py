from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url

from app.config.settings import Settings
from app.services.operations.repository import OperationalRepository
from app.services.operations.shadow_runtime import FundingCarryShadowInputSource


class ArtifactFinalizationError(RuntimeError):
    exit_code = 1


class ShadowRunArtifactWriter:
    REQUIRED_FILES = (
        "run-manifest.json",
        "resolved-config.json",
        "source-config.yaml",
        "stdout.log",
        "stderr.log",
        "collector-metrics.json",
        "orderbook-state-metrics.json",
        "pairing-metrics.json",
        "candidate-export.json",
        "rejection-summary.json",
        "dedup-metrics.json",
        "safety-counters.json",
        "summary.json",
        "git-head.txt",
    )
    SUCCESS_FILES = (
        *REQUIRED_FILES,
        "lifecycle.jsonl",
        "manifest.sha256",
        "COMPLETED",
    )

    def __init__(
        self,
        *,
        run_id: str,
        commit_sha: str,
        config_path: Path,
        settings: Settings,
        engine: Engine,
        repository: OperationalRepository,
        input_source: FundingCarryShadowInputSource,
    ) -> None:
        raw_state = os.environ.get("CRYPTTOOL_STATE_DIR")
        if not raw_state:
            raise RuntimeError("CRYPTTOOL_STATE_DIR is required for shadow runs")
        state_dir = Path(raw_state).expanduser().resolve()
        temporary = Path(tempfile.gettempdir()).resolve()
        if state_dir == temporary or temporary in state_dir.parents:
            raise RuntimeError("shadow artifacts cannot use the OS temporary directory")
        self.directory = state_dir / "shadow-runs" / run_id
        self.directory.mkdir(parents=True, exist_ok=False)
        self.run_id = run_id
        self.commit_sha = commit_sha
        self.settings = settings
        self.engine = engine
        self.repository = repository
        self.input_source = input_source
        self.started_at = datetime.now(UTC)
        self._active_artifact_name: str | None = None
        self._baseline_counts = self._database_counts()
        self._append_lifecycle("INITIALIZED", exit_code=None)
        self._write_text(
            "source-config.yaml",
            "\n".join(
                (
                    "source_table: experimental_market_events",
                    "strategy: funding_carry",
                    "instrument: BTC",
                    "venues:",
                    "  - hyperliquid",
                    "  - bitget",
                    "event_types:",
                    "  - funding_current",
                    "  - canonical_orderbook_snapshot",
                    f"config_path: {config_path.resolve()}",
                    "",
                )
            ),
        )
        self._write_text("git-head.txt", f"{commit_sha}\n")
        self._write_text("stdout.log", "")
        self._write_text("stderr.log", "")
        resolved = settings.model_dump(mode="json")
        resolved["database_url"] = make_url(settings.database_url).render_as_string(
            hide_password=True
        )
        resolved["production_database_url"] = None
        resolved["exchange_api_key"] = None
        resolved["exchange_api_secret"] = None
        resolved["live_confirmation"] = None
        if isinstance(resolved.get("continuous_paper"), dict):
            resolved["continuous_paper"]["discord_webhook_url"] = None
        if isinstance(resolved.get("paper"), dict):
            resolved["paper"]["discord_webhook_url"] = None
        self._write_json("resolved-config.json", resolved)

    def startup(self, message: str) -> None:
        self._append("stdout.log", message + "\n")
        self._append_lifecycle("WORKLOAD_STARTED", exit_code=None)

    def failure(self, error: BaseException) -> None:
        self._append("stderr.log", f"{type(error).__name__}: {error}\n")

    def finalize(self, *, exit_code: int) -> int:
        """Write all payloads before committing the final manifest and marker."""
        try:
            payloads, manifest = self._build_payloads(exit_code=exit_code)
            for name, payload in payloads.items():
                self._active_artifact_name = name
                self._write_json(name, payload)
            self._active_artifact_name = None
            self._validate_payload_artifacts()
            if not manifest["provenance_consistent"]:
                raise RuntimeError("shadow run commit provenance mismatch")
            if not manifest["safety_gate_pass"]:
                manifest["process_status"] = "COMPLETED_WITH_GATE_VIOLATION"
                manifest["exit_code"] = 1
            elif exit_code != 0:
                manifest["process_status"] = "FAILED"
                manifest["exit_code"] = exit_code
            self._write_final_manifest(manifest)
            if manifest["exit_code"] != 0:
                self._append_lifecycle("FAILED", exit_code=manifest["exit_code"])
                return int(manifest["exit_code"])
            self._append_lifecycle("COMPLETED", exit_code=0)
            self._write_text("COMPLETED", f"{manifest['completed_at']}\n")
            missing = [name for name in self.SUCCESS_FILES if not (self.directory / name).is_file()]
            if missing:
                raise RuntimeError(f"missing shadow artifacts: {','.join(missing)}")
            return 0
        except BaseException as error:
            self._write_failure_evidence(error)
            raise ArtifactFinalizationError(
                f"shadow artifact finalization failed: {type(error).__name__}: {error}"
            ) from error
        finally:
            self._active_artifact_name = None

    def _build_payloads(self, *, exit_code: int) -> tuple[dict[str, Any], dict[str, Any]]:
        completed_at = datetime.now(UTC)
        candidates = self.repository.shadow_candidates(self.run_id)
        metrics = self.repository.shadow_metrics(self.run_id)
        run = self.repository.get_run(self.run_id)
        rejection_summary: dict[str, int] = {}
        for candidate in candidates:
            if candidate.rejection_reason is not None:
                reason = candidate.rejection_reason.value
                rejection_summary[reason] = rejection_summary.get(reason, 0) + 1
        current_counts = self._database_counts()
        counts = {
            name: current_counts[name] - self._baseline_counts[name] for name in current_counts
        }
        candidate_export = [asdict(candidate) for candidate in candidates]
        orderbook_metrics = asdict(self.input_source.book_builder.metrics)
        timing_observations = [
            {
                key: value
                for key, value in asdict(candidate).items()
                if key
                in {
                    "candidate_id",
                    "hyperliquid_funding_age_seconds",
                    "bitget_funding_age_seconds",
                    "funding_observation_skew_seconds",
                    "hyperliquid_orderbook_age_seconds",
                    "bitget_orderbook_age_seconds",
                    "orderbook_venue_skew_seconds",
                    "funding_freshness_pass",
                    "orderbook_freshness_pass",
                    "orderbook_synchronization_pass",
                }
            }
            for candidate in candidates
        ]
        pairing_metrics = {
            "matched_source_pair_count": metrics.matched_source_pair_count,
            "source_pair_duplicate_count": metrics.source_pair_duplicate_count,
            "source_event_count": self.input_source.source_event_count,
            "last_seen_inputs": self._input_timestamp_records(self.input_source.last_seen),
            "last_valid_inputs": self._input_timestamp_records(self.input_source.last_valid),
            "timing_policy": {
                "funding_max_age_seconds": self.input_source.funding_max_age_seconds,
                "funding_max_observation_skew_seconds": (
                    self.input_source.funding_max_observation_skew_seconds
                ),
                "maximum_orderbook_venue_skew_seconds": (
                    self.input_source.maximum_orderbook_venue_skew_seconds
                ),
                "legacy_mixed_timestamp_skew": False,
            },
            "timing_observations": timing_observations,
        }
        disposition_candidate_count = sum(
            item.disposition.value == "candidate" for item in candidates
        )
        disposition_rejected_count = sum(
            item.disposition.value == "rejected" for item in candidates
        )
        dedup_metrics = {
            "candidate_evaluation_attempt_count": metrics.candidate_generation_attempt_count,
            "candidate_record_inserted_count": metrics.candidate_inserted_count,
            "candidate_disposition_candidate_count": disposition_candidate_count,
            "candidate_disposition_rejected_count": disposition_rejected_count,
            "candidate_duplicate_suppressed_count": (metrics.candidate_duplicate_suppressed_count),
            "matched_source_pair_count": metrics.matched_source_pair_count,
            "source_pair_duplicate_count": metrics.source_pair_duplicate_count,
        }
        safety = {
            "snapshot_count": counts["data_snapshots"],
            "research_run_count": counts["research_runs"],
            "paper_signal_count": counts["paper_signals"],
            "paper_order_count": counts["paper_orders"],
            "paper_fill_count": counts["paper_fills"],
            "production_order_count": 0,
            "execution_adapter_call_count": 0,
            "capability_promotion_count": counts["capability_promotions"],
            "strict_paper_state": "NOT_READY",
            "live_execution_state": "OFF",
            "live_credentials_loaded": False,
        }
        safety_gate_pass = all(
            safety[name] == 0
            for name in (
                "snapshot_count",
                "research_run_count",
                "paper_signal_count",
                "paper_order_count",
                "paper_fill_count",
                "production_order_count",
                "execution_adapter_call_count",
                "capability_promotion_count",
            )
        )
        candidate_shas = {candidate.code_commit_sha for candidate in candidates}
        provenance_consistent = bool(
            run is not None
            and run.commit_sha == self.commit_sha
            and candidate_shas.issubset({self.commit_sha})
        )
        manifest = {
            "run_id": self.run_id,
            "commit_sha": self.commit_sha,
            "started_at": self.started_at,
            "completed_at": completed_at,
            "exit_code": exit_code,
            "process_status": "COMPLETED" if exit_code == 0 else "FAILED",
            "provenance_consistent": provenance_consistent,
            "safety_gate_pass": safety_gate_pass,
            "artifact_directory": str(self.directory),
        }
        collector_metrics = {
            "source_table": "experimental_market_events",
            "source_event_count": self.input_source.source_event_count,
        }
        summary = {
            "collector_metrics": collector_metrics,
            "orderbook_state_metrics": orderbook_metrics,
            "pairing_metrics": pairing_metrics,
            "dedup_metrics": dedup_metrics,
            "candidate_count": disposition_candidate_count,
            "rejected_count": disposition_rejected_count,
            "rejection_summary": rejection_summary,
            "safety_counters": safety,
        }
        payloads = {
            "collector-metrics.json": collector_metrics,
            "orderbook-state-metrics.json": orderbook_metrics,
            "pairing-metrics.json": pairing_metrics,
            "candidate-export.json": candidate_export,
            "rejection-summary.json": rejection_summary,
            "dedup-metrics.json": dedup_metrics,
            "safety-counters.json": safety,
            "summary.json": summary,
        }
        return payloads, manifest

    @staticmethod
    def _input_timestamp_records(
        values: dict[tuple[str, str], datetime],
    ) -> list[dict[str, Any]]:
        return [
            {"venue": venue, "capability": capability, "timestamp": timestamp}
            for (venue, capability), timestamp in sorted(values.items())
        ]

    def _validate_payload_artifacts(self) -> None:
        required = tuple(name for name in self.REQUIRED_FILES if name != "run-manifest.json")
        missing = [name for name in required if not (self.directory / name).is_file()]
        if missing:
            raise RuntimeError(f"missing shadow artifacts: {','.join(missing)}")
        for name in required:
            if name.endswith(".json"):
                json.loads((self.directory / name).read_text(encoding="utf-8"))

    def _write_final_manifest(self, manifest: dict[str, Any]) -> None:
        self._active_artifact_name = "run-manifest.json"
        self._write_json("run-manifest.json", manifest)
        manifest_bytes = (self.directory / "run-manifest.json").read_bytes()
        self._active_artifact_name = "manifest.sha256"
        self._write_text("manifest.sha256", f"{hashlib.sha256(manifest_bytes).hexdigest()}\n")
        self._active_artifact_name = None

    def _write_failure_evidence(self, error: BaseException) -> None:
        failed_name = self._active_artifact_name
        actual_exit_code = 1
        existing = sorted(path.name for path in self.directory.iterdir() if path.is_file())
        missing = sorted(name for name in self.SUCCESS_FILES if name not in existing)
        detail = {
            "run_id": self.run_id,
            "failed_artifact_filename": failed_name,
            "exception_type": type(error).__name__,
            "exception_message": str(error),
            "completed_artifact_files": existing,
            "missing_artifact_files": missing,
            "actual_exit_code": actual_exit_code,
            "failed_at": datetime.now(UTC),
        }
        self._write_json_unchecked("artifact-error.json", detail)
        self._write_json_unchecked(
            "failure.json",
            {**detail, "run_status": "FAILED", "process_status": "FAILED"},
        )
        self._append("stderr.log", f"{type(error).__name__}: {error}\n")
        self._append_lifecycle("FAILED", exit_code=actual_exit_code)
        failure_manifest = {
            "run_id": self.run_id,
            "commit_sha": self.commit_sha,
            "started_at": self.started_at,
            "completed_at": datetime.now(UTC),
            "exit_code": actual_exit_code,
            "process_status": "FAILED",
            "provenance_consistent": False,
            "safety_gate_pass": False,  # nosec B105 -- boolean gate status, not a password
            "artifact_directory": str(self.directory),
            "artifact_error": detail,
        }
        self._write_json_unchecked("run-manifest.json", failure_manifest)
        manifest_bytes = (self.directory / "run-manifest.json").read_bytes()
        self._write_text("manifest.sha256", f"{hashlib.sha256(manifest_bytes).hexdigest()}\n")
        (self.directory / "COMPLETED").unlink(missing_ok=True)

    def _database_counts(self) -> dict[str, int]:
        tables = (
            "data_snapshots",
            "research_runs",
            "paper_signals",
            "paper_orders",
            "paper_fills",
            "capability_promotions",
        )
        with self.engine.connect() as connection:
            return {
                table: int(
                    connection.scalar(
                        text(
                            f"SELECT count(*) FROM {table}"  # nosec B608
                            + (
                                " WHERE run_id = :run_id"
                                if table in {"paper_signals", "paper_orders", "paper_fills"}
                                else ""
                            )
                        ),
                        {"run_id": self.run_id},
                    )
                    or 0
                )
                for table in tables
            }

    def _write_json(self, name: str, value: Any) -> None:
        self._write_text(name, json.dumps(value, default=str, indent=2, sort_keys=True) + "\n")

    def _write_json_unchecked(self, name: str, value: Any) -> None:
        self._write_text(name, json.dumps(value, default=str, indent=2, sort_keys=True) + "\n")

    def _write_text(self, name: str, value: str) -> None:
        path = self.directory / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        self._fsync_directory()

    def _append(self, name: str, value: str) -> None:
        with (self.directory / name).open("a", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        self._fsync_directory()

    def _append_lifecycle(self, status: str, *, exit_code: int | None) -> None:
        self._append(
            "lifecycle.jsonl",
            json.dumps(
                {
                    "run_id": self.run_id,
                    "status": status,
                    "recorded_at": datetime.now(UTC),
                    "exit_code": exit_code,
                },
                default=str,
                sort_keys=True,
            )
            + "\n",
        )

    def _fsync_directory(self) -> None:
        descriptor = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
