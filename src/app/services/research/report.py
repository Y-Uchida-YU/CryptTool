from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

from app.services.backtest.engine import BacktestResult
from app.services.research.models import (
    AcceptanceResult,
    CostStressResult,
    DataQualityResult,
    FeatureArtifact,
    PointInTimeDataset,
    RegimeArtifact,
    ResearchRunIdentity,
    WalkForwardResult,
)
from app.services.research.repository import ResearchRepository


def _json_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: _json_value(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(_json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ResearchArtifactWriter:
    REQUIRED_FILES = (
        "summary.md",
        "metrics.json",
        "walk-forward.csv",
        "cost-stress.csv",
        "trades.parquet",
        "rejections.parquet",
    )

    def __init__(self, root: Path, repository: ResearchRepository) -> None:
        self.root = root
        self.repository = repository

    def write(
        self,
        *,
        identity: ResearchRunIdentity,
        dataset: PointInTimeDataset,
        quality: DataQualityResult,
        features: FeatureArtifact,
        regimes: RegimeArtifact,
        backtest: BacktestResult,
        walk_forward: WalkForwardResult,
        stress: CostStressResult,
        overfitting: object,
        acceptance: AcceptanceResult,
        deferred_strategies: tuple[str, ...],
        config: dict[str, Any],
    ) -> Path:
        directory = self.root / identity.run_id
        directory.mkdir(parents=True, exist_ok=True)
        metrics = {
            "identity": identity,
            "data_quality": quality,
            "feature_artifact": features,
            "regime_artifact": regimes,
            "backtest": backtest,
            "walk_forward": walk_forward,
            "cost_stress": stress,
            "overfitting": overfitting,
            "acceptance": acceptance,
            "deferred_strategies": deferred_strategies,
            "data_snapshot_content_sha256": dataset.content_sha256,
            "config": config,
        }
        (directory / "metrics.json").write_bytes(_json_bytes(metrics))
        self._write_walk_forward(directory / "walk-forward.csv", walk_forward)
        self._write_cost_stress(directory / "cost-stress.csv", stress)
        self._write_trades(directory / "trades.parquet", backtest)
        self._write_rejections(directory / "rejections.parquet", backtest)
        (directory / "summary.md").write_text(
            self._summary(
                identity,
                backtest,
                walk_forward,
                stress,
                overfitting,
                acceptance,
                Decimal(str(config.get("initial_cash", "1000"))),
            ),
            encoding="utf-8",
        )
        file_entries = {
            name: {"sha256": _sha256(directory / name), "bytes": (directory / name).stat().st_size}
            for name in self.REQUIRED_FILES
        }
        manifest = {
            "run_id": identity.run_id,
            "data_snapshot_id": identity.data_snapshot_id,
            "data_snapshot_sha256": dataset.content_sha256,
            "commit_sha": identity.commit_sha,
            "config_sha256": identity.config_sha256,
            "hypothesis_version": identity.hypothesis_version,
            "strategy_id": identity.strategy_id,
            "strategy_version": identity.strategy_version,
            "created_at": identity.created_at,
            "files": file_entries,
        }
        manifest_path = directory / "manifest.json"
        manifest_path.write_bytes(_json_bytes(manifest))
        for artifact_type, detail in file_entries.items():
            self.repository.save_artifact(
                identity.run_id,
                identity.data_snapshot_id,
                artifact_type,
                str(directory / artifact_type),
                str(detail["sha256"]),
                identity.created_at,
            )
        self.repository.save_artifact(
            identity.run_id,
            identity.data_snapshot_id,
            "manifest.json",
            str(manifest_path),
            _sha256(manifest_path),
            identity.created_at,
        )
        self.verify(manifest_path)
        return manifest_path

    @staticmethod
    def verify(manifest_path: Path) -> dict[str, object]:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
            raise ValueError("invalid research artifact manifest")
        for name, detail in manifest["files"].items():
            if not isinstance(name, str) or not isinstance(detail, dict):
                raise ValueError("invalid research artifact entry")
            path = manifest_path.parent / name
            if not path.is_file() or _sha256(path) != detail.get("sha256"):
                raise ValueError(f"research artifact hash mismatch: {name}")
        return manifest

    @staticmethod
    def _write_walk_forward(path: Path, result: WalkForwardResult) -> None:
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=(
                    "run_id",
                    "data_snapshot_id",
                    "window_id",
                    "train_indices",
                    "validation_indices",
                    "oos_indices",
                    "selected_parameters",
                    "oos_returns",
                ),
            )
            writer.writeheader()
            for item in result.windows:
                writer.writerow(
                    {
                        "run_id": result.run_id,
                        "data_snapshot_id": result.data_snapshot_id,
                        "window_id": item.number,
                        "train_indices": json.dumps(item.train_indices),
                        "validation_indices": json.dumps(item.validation_indices),
                        "oos_indices": json.dumps(item.oos_indices),
                        "selected_parameters": json.dumps(
                            _json_value(item.selected_parameters), sort_keys=True
                        ),
                        "oos_returns": json.dumps(_json_value(item.oos_returns)),
                    }
                )

    @staticmethod
    def _write_cost_stress(path: Path, result: CostStressResult) -> None:
        rows: list[dict[str, object]] = []
        for item in result.scenarios:
            row = {
                "run_id": result.run_id,
                "data_snapshot_id": result.data_snapshot_id,
                "scenario": item.scenario,
                **asdict(item.metrics),
                **{
                    key: value
                    for key, value in asdict(item).items()
                    if key not in {"scenario", "metrics"}
                },
            }
            rows.append(row)
        pd.DataFrame(rows).to_csv(path, index=False)

    @staticmethod
    def _write_trades(path: Path, backtest: BacktestResult) -> None:
        rows = [
            {
                "run_id": backtest.run_id,
                "data_snapshot_id": backtest.data_snapshot_id,
                **asdict(fill),
            }
            for fill in backtest.fills
        ]
        pd.DataFrame(rows, columns=None if rows else ["run_id", "data_snapshot_id"]).to_parquet(
            path, index=False
        )

    @staticmethod
    def _write_rejections(path: Path, backtest: BacktestResult) -> None:
        rows = [
            {
                "run_id": backtest.run_id,
                "data_snapshot_id": backtest.data_snapshot_id,
                "rejection_type": "signal",
                **asdict(item),
            }
            for item in backtest.rejected_signals
        ]
        rows.extend(
            {
                "run_id": backtest.run_id,
                "data_snapshot_id": backtest.data_snapshot_id,
                "rejection_type": "order",
                "signal_id": order.signal_id,
                "order_id": order.order_id,
                "timestamp": order.submitted_at,
                "status": order.status.value,
                "remaining_quantity": order.remaining_quantity,
                "reason": order.rejection_reason or "unfilled or partially filled",
            }
            for order in backtest.orders
            if order.remaining_quantity > 0
        )
        pd.DataFrame(rows, columns=None if rows else ["run_id", "data_snapshot_id"]).to_parquet(
            path, index=False
        )

    @staticmethod
    def _summary(
        identity: ResearchRunIdentity,
        backtest: BacktestResult,
        walk_forward: WalkForwardResult,
        stress: CostStressResult,
        overfitting: object,
        acceptance: AcceptanceResult,
        initial_cash: Decimal,
    ) -> str:
        base = stress.scenario("base")
        fees = sum((fill.fee for fill in backtest.fills if fill.fee >= 0), Decimal(0))
        rebates = -sum((fill.fee for fill in backtest.fills if fill.fee < 0), Decimal(0))
        funding = sum((item.amount for item in backtest.funding), Decimal(0))
        gross = backtest.final_equity - initial_cash + fees - rebates - funding
        venue_attribution: dict[str, Decimal] = {}
        asset_attribution: dict[str, Decimal] = {}
        for fill in backtest.fills:
            signed = fill.quantity * fill.price * (Decimal(1) if fill.side.value == "sell" else -1)
            venue_attribution[fill.exchange] = (
                venue_attribution.get(fill.exchange, Decimal(0)) + signed
            )
            asset_attribution[fill.symbol] = asset_attribution.get(fill.symbol, Decimal(0)) + signed
        capital = ", ".join(
            f"{item.capital} USD={'PASS' if item.feasible else 'FAIL'}"
            for item in acceptance.capital_feasibility.scenarios
        )
        oos_metrics = _performance_from_returns(walk_forward.combined_oos_returns)
        stress_lines = [
            (
                f"  - {item.scenario}: net={item.metrics.net_pnl}, "
                f"sharpe={item.metrics.sharpe}, sortino={item.metrics.sortino}, "
                f"drawdown={item.metrics.maximum_drawdown}, "
                f"ruin={item.metrics.ruin_probability}"
            )
            for item in stress.scenarios
        ]
        lines = [
            f"# Research Run {identity.run_id}",
            "",
            f"- Verdict: **{acceptance.verdict.value}**",
            f"- Data snapshot: `{identity.data_snapshot_id}`",
            f"- Gross PnL: {gross}",
            f"- Net PnL: {backtest.final_equity - initial_cash}",
            f"- Fee: {fees}",
            f"- Rebate: {rebates}",
            f"- Funding: {funding}",
            f"- Slippage: {sum((fill.slippage_cost for fill in backtest.fills), Decimal(0))}",
            f"- Impact: {sum((fill.market_impact_cost for fill in backtest.fills), Decimal(0))}",
            f"- Failed-leg cost: {base.failed_leg_cost if base else 'n/a'}",
            f"- Venue outage loss: {base.venue_outage_loss if base else 'n/a'}",
            f"- OOS metrics: {_json_value(oos_metrics)}",
            "- Stress metrics:",
            *stress_lines,
            f"- PBO: {getattr(overfitting, 'pbo', None)}",
            f"- Deflated Sharpe: {getattr(overfitting, 'deflated_sharpe', None)}",
            f"- Capital feasibility: {capital}",
            f"- Venue attribution: {_json_value(venue_attribution)}",
            f"- Period attribution: {_json_value(walk_forward.leave_one_period_out)}",
            f"- Asset attribution: {_json_value(asset_attribution)}",
        ]
        return "\n".join(lines) + "\n"


def _performance_from_returns(returns: tuple[Decimal, ...]) -> dict[str, Decimal | None]:
    if not returns:
        return {"net_pnl": None, "sharpe": None, "maximum_drawdown": None}
    total = sum(returns, start=Decimal(0))
    mean = total / Decimal(len(returns))
    variance = sum(((item - mean) ** 2 for item in returns), start=Decimal(0)) / Decimal(
        len(returns)
    )
    sharpe = mean / variance.sqrt() if variance > 0 else Decimal(0)
    equity = Decimal(1)
    peak = Decimal(1)
    maximum_drawdown = Decimal(0)
    for item in returns:
        equity *= 1 + item
        peak = max(peak, equity)
        maximum_drawdown = max(maximum_drawdown, (peak - equity) / peak)
    return {
        "net_pnl": equity - 1,
        "sharpe": sharpe,
        "maximum_drawdown": maximum_drawdown,
    }
