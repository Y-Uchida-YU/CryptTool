from __future__ import annotations

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
        self._baseline_counts = self._database_counts()
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

    def failure(self, error: BaseException) -> None:
        self._append("stderr.log", f"{type(error).__name__}: {error}\n")

    def finalize(self, *, exit_code: int) -> None:
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
        pairing_metrics = {
            "matched_source_pair_count": metrics.matched_source_pair_count,
            "source_pair_duplicate_count": metrics.source_pair_duplicate_count,
            "source_event_count": self.input_source.source_event_count,
            "last_seen": self.input_source.last_seen,
            "last_valid": self.input_source.last_valid,
        }
        dedup_metrics = {
            "candidate_generation_attempt_count": metrics.candidate_generation_attempt_count,
            "candidate_inserted_count": metrics.candidate_inserted_count,
            "candidate_rejected_count": metrics.candidate_rejected_count,
            "candidate_duplicate_suppressed_count": (metrics.candidate_duplicate_suppressed_count),
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
            "artifact_directory": str(self.directory),
        }
        collector_metrics = {
            "source_table": "experimental_market_events",
            "source_event_count": self.input_source.source_event_count,
        }
        summary = {
            "run_manifest": manifest,
            "collector_metrics": collector_metrics,
            "orderbook_state_metrics": orderbook_metrics,
            "pairing_metrics": pairing_metrics,
            "dedup_metrics": dedup_metrics,
            "candidate_count": sum(item.disposition.value == "candidate" for item in candidates),
            "rejected_count": sum(item.disposition.value == "rejected" for item in candidates),
            "rejection_summary": rejection_summary,
            "safety_counters": safety,
        }
        self._write_json("run-manifest.json", manifest)
        self._write_json("collector-metrics.json", collector_metrics)
        self._write_json("orderbook-state-metrics.json", orderbook_metrics)
        self._write_json("pairing-metrics.json", pairing_metrics)
        self._write_json("candidate-export.json", candidate_export)
        self._write_json("rejection-summary.json", rejection_summary)
        self._write_json("dedup-metrics.json", dedup_metrics)
        self._write_json("safety-counters.json", safety)
        self._write_json("summary.json", summary)
        missing = [name for name in self.REQUIRED_FILES if not (self.directory / name).is_file()]
        if missing:
            raise RuntimeError(f"missing shadow artifacts: {','.join(missing)}")
        if not provenance_consistent:
            raise RuntimeError("shadow run commit provenance mismatch")

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
        self._write_text(
            name,
            json.dumps(value, default=str, indent=2, sort_keys=True) + "\n",
        )

    def _write_text(self, name: str, value: str) -> None:
        path = self.directory / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(value, encoding="utf-8")
        temporary.replace(path)

    def _append(self, name: str, value: str) -> None:
        with (self.directory / name).open("a", encoding="utf-8") as handle:
            handle.write(value)
