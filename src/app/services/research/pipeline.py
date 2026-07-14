from __future__ import annotations

import itertools
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import yaml  # type: ignore[import-untyped]

from app.domain.execution.models import InstrumentRules, MarketSnapshot, OrderType, TimeInForce
from app.domain.execution.simulator import ExecutionModelConfig
from app.domain.market_data.models import Side
from app.services.backtest.engine import BacktestEngine, BacktestResult
from app.services.backtest.events import FundingEvent, MarketEvent, SignalEvent
from app.services.research.dataset import (
    PointInTimeDatasetBuilder,
    evaluate_data_quality,
    raw_event_from_dict,
)
from app.services.research.models import (
    AcceptanceCheckResult,
    AcceptanceResult,
    AcceptanceVerdict,
    CapitalFeasibilityResult,
    CapitalScenarioFeasibility,
    CostStressResult,
    CostStressScenarioResult,
    DataQualityResult,
    FeatureArtifact,
    FrozenHypothesis,
    InstrumentRuleSnapshot,
    OverfittingResult,
    PerformanceSummary,
    PointInTimeDataset,
    RegimeArtifact,
    ResearchRunIdentity,
    ResearchRunResult,
    WalkForwardResult,
    WalkForwardWindowResult,
    canonical_sha256,
    utc,
)
from app.services.research.repository import InMemoryResearchRepository, ResearchRepository
from app.services.validation.overfitting import (
    analyze_parameter_stability,
    deflated_sharpe_ratio,
    probability_of_backtest_overfitting,
    whites_reality_check,
)
from app.services.validation.resampling import monte_carlo_paths
from app.services.validation.splits import walk_forward_splits

R1_PRIMARY_VENUES = ("hyperliquid", "bitget", "aster", "mexc")
R1_CHALLENGER_VENUES = ("dydx", "paradex", "lighter")
R1_VENUES = R1_PRIMARY_VENUES + R1_CHALLENGER_VENUES
R1_STRATEGIES = ("funding_carry", "cross_venue_basis", "btc_sol_relative_strength")
DEFERRED_STRATEGIES = (
    "liquidation",
    "liquidation_exhaustion",
    "whale",
    "whale_following",
    "funding_extreme",
    "trend_following",
    "mean_reversion",
    "flash_crash_reversal",
    "relative_strength",
)
STRESS_SCENARIOS = (
    "base",
    "fees_x1.5",
    "fees_x2.0",
    "slippage_x1.5",
    "slippage_x2.0",
    "latency_x2",
    "depth_minus_50pct",
    "funding_edge_minus_25pct",
    "funding_edge_minus_50pct",
    "one_leg_failure",
    "venue_outage",
    "maker_rebate_removal",
)


class FrozenHypothesisRegistry:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], FrozenHypothesis] = {}

    def freeze(self, hypothesis: FrozenHypothesis) -> FrozenHypothesis:
        hypothesis.verify()
        key = (hypothesis.strategy_id, hypothesis.hypothesis_version)
        current = self._items.get(key)
        if current is not None and current.content_sha256 != hypothesis.content_sha256:
            raise ValueError("parameter changes require a new hypothesis_version")
        self._items[key] = hypothesis
        return hypothesis


def _decimal(value: object, default: str = "0") -> Decimal:
    return Decimal(str(default if value is None else value))


def _is_decimal(value: str) -> bool:
    try:
        Decimal(value)
    except (InvalidOperation, ValueError):
        return False
    return True


def _summary(returns: list[Decimal], initial_cash: Decimal = Decimal("1000")) -> PerformanceSummary:
    if not returns:
        return PerformanceSummary(*(Decimal(0) for _ in range(10)))
    values = np.asarray([float(item) for item in returns], dtype=np.float64)
    mean = float(np.mean(values))
    std = float(np.std(values, ddof=1)) if values.size > 1 else 0.0
    downside = values[values < 0]
    downside_std = float(np.std(downside, ddof=1)) if downside.size > 1 else 0.0
    sharpe = mean / std * math.sqrt(365) if std > 1e-12 else 0.0
    sortino = mean / downside_std * math.sqrt(365) if downside_std > 1e-12 else 0.0
    equity = float(initial_cash) * np.cumprod(1 + values)
    peaks = np.maximum.accumulate(equity)
    drawdown = np.max((peaks - equity) / peaks) if equity.size else 0.0
    pnl = Decimal(str(float(equity[-1] - float(initial_cash))))
    wins = values[values > 0]
    losses = values[values < 0]
    profit_factor = (
        float(np.sum(wins) / abs(np.sum(losses))) if losses.size and np.sum(losses) != 0 else 0.0
    )
    tail = float(np.quantile(values, 0.05))
    return PerformanceSummary(
        net_pnl=pnl,
        sharpe=Decimal(str(sharpe)),
        sortino=Decimal(str(sortino)),
        maximum_drawdown=Decimal(str(drawdown)),
        turnover=Decimal(str(float(np.sum(np.abs(values))))),
        win_rate=Decimal(str(float(np.mean(values > 0)))),
        profit_factor=Decimal(str(profit_factor)),
        tail_loss=Decimal(str(tail)),
        ruin_probability=Decimal(1 if np.any(equity <= 0) else 0),
        capital_efficiency=pnl / initial_cash,
    )


def evaluate_acceptance(
    *,
    data_quality: DataQualityResult,
    walk_forward: WalkForwardResult,
    cost_stress: CostStressResult,
    overfitting: OverfittingResult,
    capital_feasibility: CapitalFeasibilityResult,
) -> AcceptanceResult:
    """Evaluate only measured R1 artifacts; missing diagnostics cannot produce PASS."""

    identifiers = {
        (data_quality.run_id, data_quality.data_snapshot_id),
        (walk_forward.run_id, walk_forward.data_snapshot_id),
        (cost_stress.run_id, cost_stress.data_snapshot_id),
        (overfitting.run_id, overfitting.data_snapshot_id),
        (capital_feasibility.run_id, capital_feasibility.data_snapshot_id),
    }
    if len(identifiers) != 1:
        raise ValueError("acceptance artifacts must belong to one run and data snapshot")
    run_id, snapshot_id = next(iter(identifiers))
    base = cost_stress.scenario("base")
    oos_total = sum(walk_forward.combined_oos_returns, start=Decimal(0))
    dependency_complete = (
        len(walk_forward.leave_one_period_out) >= 2
        and len(walk_forward.leave_one_asset_out) >= 2
        and len(walk_forward.leave_one_venue_out) >= 3
    )
    dependency_floor = min(oos_total * Decimal("0.20"), Decimal(0))
    dependency_pass = (
        None
        if not dependency_complete
        else min(walk_forward.leave_one_period_out) >= dependency_floor
        and min(walk_forward.leave_one_asset_out.values()) >= dependency_floor
        and min(walk_forward.leave_one_venue_out.values()) >= dependency_floor
    )
    major = [
        cost_stress.scenario(name)
        for name in ("fees_x1.5", "slippage_x2.0", "one_leg_failure", "venue_outage")
    ]
    checks = (
        AcceptanceCheckResult(
            "data_quality", data_quality.passed, str(data_quality.coverage_ratio), "PASS"
        ),
        AcceptanceCheckResult(
            "oos_net_pnl",
            bool(walk_forward.combined_oos_returns) and oos_total > 0,
            str(oos_total),
            "> 0",
        ),
        AcceptanceCheckResult(
            "deflated_sharpe",
            None if overfitting.deflated_sharpe is None else overfitting.deflated_sharpe > 0,
            str(overfitting.deflated_sharpe),
            "> 0",
        ),
        AcceptanceCheckResult(
            "pbo",
            None if overfitting.pbo is None else overfitting.pbo < Decimal("0.5"),
            str(overfitting.pbo),
            "< 0.5",
        ),
        AcceptanceCheckResult(
            "cost_stress_survival",
            None
            if base is None or any(item is None for item in major)
            else all(
                item is not None and item.metrics.net_pnl > -Decimal("1000") for item in major
            ),
            ",".join(
                f"{item.scenario}:{item.metrics.net_pnl}" for item in major if item is not None
            ),
            "base and major stress scenarios do not ruin capital",
        ),
        AcceptanceCheckResult(
            "maximum_drawdown",
            None if base is None else base.metrics.maximum_drawdown <= Decimal("0.20"),
            str(base.metrics.maximum_drawdown if base else None),
            "<= 0.20",
        ),
        AcceptanceCheckResult(
            "ruin_probability",
            None
            if overfitting.monte_carlo_ruin_probability is None
            else overfitting.monte_carlo_ruin_probability <= Decimal("0.05"),
            str(overfitting.monte_carlo_ruin_probability),
            "<= 0.05",
        ),
        AcceptanceCheckResult(
            "capital_100_300_1000",
            all(
                capital_feasibility.feasible_at(capital)
                for capital in (Decimal("100"), Decimal("300"), Decimal("1000"))
            ),
            ",".join(f"{item.capital}:{item.feasible}" for item in capital_feasibility.scenarios),
            "all configured capital levels are feasible",
        ),
        AcceptanceCheckResult(
            "dependency_robustness",
            dependency_pass,
            "measured",
            "leave-one-period/asset/venue retains the configured robustness floor",
        ),
    )
    if not data_quality.passed:
        verdict = AcceptanceVerdict.FAIL
    elif any(item.passed is None for item in checks) or not overfitting.evidence_complete:
        verdict = AcceptanceVerdict.INSUFFICIENT_EVIDENCE
    elif any(item.passed is False for item in checks):
        verdict = AcceptanceVerdict.FAIL
    else:
        verdict = AcceptanceVerdict.PASS
    return AcceptanceResult(run_id, snapshot_id, verdict, checks, capital_feasibility)


class ResearchPipeline:
    def __init__(
        self,
        repository: ResearchRepository | None = None,
        *,
        artifact_root: Path = Path("artifacts/research"),
        hypothesis_registry: FrozenHypothesisRegistry | None = None,
    ) -> None:
        self.repository = repository or InMemoryResearchRepository()
        self.artifact_root = artifact_root
        self.hypotheses = hypothesis_registry or FrozenHypothesisRegistry()

    @staticmethod
    def load_config(path: Path) -> dict[str, Any]:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("research config must be a mapping")
        return payload

    def run_config(self, path: Path) -> ResearchRunResult:
        return self.run(self.load_config(path))

    def run(self, config: dict[str, Any]) -> ResearchRunResult:
        strategy_id = str(config["strategy_id"])
        if strategy_id not in R1_STRATEGIES:
            deferred = strategy_id in DEFERRED_STRATEGIES
            raise ValueError(
                f"strategy {strategy_id} is {'DEFERRED' if deferred else 'not in Phase R1'}"
            )
        venues = tuple(str(item) for item in config["venues"])
        if not venues or any(item not in R1_VENUES for item in venues):
            raise ValueError("R1 config contains an unsupported venue")
        if bool(config.get("live_execution", False)):
            raise ValueError("Research Pipeline requires live execution OFF")
        config_hash = canonical_sha256(config)
        cutoff = utc(_parse_time(config["cutoff_at"]), "cutoff_at")
        snapshot_id = str(config["data_snapshot_id"])
        identity_payload = {
            "commit_sha": str(config["commit_sha"]),
            "config_sha256": config_hash,
            "data_snapshot_id": snapshot_id,
            "hypothesis_version": str(config["hypothesis_version"]),
            "strategy_id": strategy_id,
            "strategy_version": str(config.get("strategy_version", "1")),
        }
        run_id = f"research-{canonical_sha256(identity_payload)[:24]}"
        identity = ResearchRunIdentity(
            run_id=run_id,
            created_at=utc(
                _parse_time(config.get("created_at", config["cutoff_at"])), "created_at"
            ),
            **identity_payload,
        )
        configured_event_ids: set[str] = set()
        for raw in config.get("raw_events", []):
            if not isinstance(raw, dict):
                raise ValueError("raw_events entries must be mappings")
            event = raw_event_from_dict(raw)
            if event.event_id in configured_event_ids:
                self.repository.quarantine(event, "duplicate event id in input", cutoff)
                continue
            configured_event_ids.add(event.event_id)
            if not self.repository.add_raw_event(event):
                existing = self.repository.get_raw_event(event.event_id)
                if existing != event:
                    self.repository.quarantine(event, "conflicting duplicate event id", cutoff)
        hypothesis_config = config["hypothesis"]
        if not isinstance(hypothesis_config, dict):
            raise ValueError("hypothesis config must be a mapping")
        hypothesis = FrozenHypothesis.freeze(
            hypothesis_version=identity.hypothesis_version,
            strategy_id=strategy_id,
            parameter_grid={
                str(key): tuple(values)
                for key, values in dict(hypothesis_config["parameter_grid"]).items()
            },
            primary_metric=str(hypothesis_config.get("primary_metric", "net_pnl")),
            secondary_metrics=tuple(hypothesis_config.get("secondary_metrics", ("sharpe",))),
            acceptance_thresholds={
                str(key): _decimal(value)
                for key, value in dict(hypothesis_config.get("acceptance_thresholds", {})).items()
            },
            frozen_at=utc(_parse_time(hypothesis_config["frozen_at"]), "frozen_at"),
        )
        if hypothesis.frozen_at > cutoff:
            raise ValueError("hypothesis must be frozen before OOS cutoff")
        self.hypotheses.freeze(hypothesis)
        self.repository.freeze_hypothesis(hypothesis)
        builder = PointInTimeDatasetBuilder(self.repository)
        dataset = builder.build(
            snapshot_id=snapshot_id,
            cutoff_at=cutoff,
            instruments=tuple(str(item) for item in config["instruments"]),
            venues=venues,
            event_types=tuple(str(item) for item in config["event_types"]),
        )
        self.repository.save_run(identity, "running", None)
        relevant_event_ids = tuple(
            item.event_id
            for item in self.repository.raw_events()
            if item.venue in venues
            and item.canonical_instrument_id in dataset.instruments
            and item.event_type in dataset.event_types
            and item.available_at <= cutoff
        )
        quality = evaluate_data_quality(
            run_id=run_id,
            dataset=dataset,
            quarantine_count=self.repository.quarantine_count(relevant_event_ids),
            minimum_coverage=_decimal(config.get("minimum_coverage", "0.50")),
            maximum_stale_ratio=_decimal(config.get("maximum_stale_ratio", "1")),
            maximum_quarantine_ratio=_decimal(config.get("maximum_quarantine_ratio", "0.05")),
            maximum_duplicate_ratio=_decimal(config.get("maximum_duplicate_ratio", "0")),
            maximum_clock_skew_seconds=_decimal(config.get("maximum_clock_skew_seconds", "300")),
            maximum_cross_venue_divergence=_decimal(
                config.get("maximum_cross_venue_divergence", "0.20")
            ),
            minimum_book_depth_availability=_decimal(
                config.get("minimum_book_depth_availability", "0.80")
            ),
            maximum_outage_duration_seconds=_decimal(
                config.get("maximum_outage_duration_seconds", "0")
            ),
        )
        features = self._features(identity, dataset, config)
        regimes = self._regimes(identity, features)
        rules = self._rules(config, dataset)
        base_result = self._run_backtest(identity, dataset, config, rules, quality.passed)
        walk_forward = (
            self._walk_forward(identity, features, hypothesis, config)
            if quality.passed
            else self._empty_walk_forward(identity, config)
        )
        stress = self._cost_stress(identity, dataset, config, rules, quality.passed)
        overfitting = (
            self._overfitting(identity, features, hypothesis, config)
            if quality.passed
            else self._empty_overfitting(identity)
        )
        capital = self._capital(identity, config, rules)
        acceptance = evaluate_acceptance(
            data_quality=quality,
            walk_forward=walk_forward,
            cost_stress=stress,
            overfitting=overfitting,
            capital_feasibility=capital,
        )
        acceptance = self._apply_frozen_thresholds(
            acceptance,
            hypothesis,
            walk_forward,
            stress,
            overfitting,
            quality,
        )
        manifest_path = self._write_artifacts(
            identity,
            dataset,
            quality,
            features,
            regimes,
            base_result,
            walk_forward,
            stress,
            overfitting,
            acceptance,
            config,
        )
        self.repository.save_run(identity, "completed", acceptance.verdict.value)
        return ResearchRunResult(
            identity,
            quality,
            features,
            regimes,
            base_result,
            walk_forward,
            stress,
            overfitting,
            acceptance,
            str(manifest_path),
        )

    @staticmethod
    def _empty_walk_forward(
        identity: ResearchRunIdentity, config: dict[str, Any]
    ) -> WalkForwardResult:
        wf = dict(config.get("walk_forward", {}))
        return WalkForwardResult(
            identity.run_id,
            identity.data_snapshot_id,
            str(wf.get("mode", "rolling")),
            int(wf.get("purge", 1)),
            int(wf.get("embargo", 1)),
            (),
            (),
            (),
            {},
            {},
            False,
            canonical_sha256((identity.run_id, "data-quality-rejected-walk-forward")),
        )

    @staticmethod
    def _empty_overfitting(identity: ResearchRunIdentity) -> OverfittingResult:
        return OverfittingResult(
            identity.run_id,
            identity.data_snapshot_id,
            None,
            None,
            0,
            None,
            None,
            None,
            False,
        )

    @staticmethod
    def _apply_frozen_thresholds(
        acceptance: AcceptanceResult,
        hypothesis: FrozenHypothesis,
        walk_forward: WalkForwardResult,
        stress: CostStressResult,
        overfitting: OverfittingResult,
        quality: DataQualityResult,
    ) -> AcceptanceResult:
        thresholds = hypothesis.acceptance_thresholds
        checks = list(acceptance.checks)
        base = stress.scenario("base")
        observations: dict[str, Decimal | None] = {
            "minimum_oos_net_pnl": sum(walk_forward.combined_oos_returns, start=Decimal(0)),
            "minimum_deflated_sharpe": overfitting.deflated_sharpe,
            "maximum_pbo": overfitting.pbo,
            "maximum_drawdown": base.metrics.maximum_drawdown if base is not None else None,
            "maximum_ruin_probability": overfitting.monte_carlo_ruin_probability,
        }
        for name, threshold in sorted(thresholds.items()):
            if name not in observations:
                raise ValueError(f"unsupported frozen acceptance threshold: {name}")
            observed = observations[name]
            passed = (
                None
                if observed is None
                else observed >= threshold
                if name.startswith("minimum_")
                else observed <= threshold
            )
            checks.append(
                AcceptanceCheckResult(
                    f"frozen_{name}",
                    passed,
                    str(observed),
                    (">= " if name.startswith("minimum_") else "<= ") + str(threshold),
                )
            )
        if not quality.passed:
            verdict = AcceptanceVerdict.FAIL
        elif any(item.passed is None for item in checks) or not overfitting.evidence_complete:
            verdict = AcceptanceVerdict.INSUFFICIENT_EVIDENCE
        elif any(item.passed is False for item in checks):
            verdict = AcceptanceVerdict.FAIL
        else:
            verdict = AcceptanceVerdict.PASS
        return replace(acceptance, verdict=verdict, checks=tuple(checks))

    def _features(
        self, identity: ResearchRunIdentity, dataset: PointInTimeDataset, config: dict[str, Any]
    ) -> FeatureArtifact:
        latest_funding: dict[tuple[str, str], Decimal] = {}
        latest_oi: dict[tuple[str, str], Decimal] = {}
        rows: list[dict[str, Any]] = []
        previous_mid: dict[tuple[str, str], Decimal] = {}
        event_priority = {
            "funding_rate": 0,
            "funding_current": 0,
            "funding_history": 0,
            "open_interest": 1,
            "orderbook_snapshot": 2,
        }
        for value in sorted(
            dataset.values,
            key=lambda item: (
                item.available_at,
                event_priority.get(item.event_type, 3),
                item.event_id,
            ),
        ):
            value.require_available(value.available_at)
            key = (value.venue, value.canonical_instrument_id)
            if value.event_type in {"funding_rate", "funding_current", "funding_history"}:
                latest_funding[key] = _decimal(value.payload.get("rate"))
            elif value.event_type == "open_interest":
                latest_oi[key] = _decimal(
                    value.payload.get("open_interest", value.payload.get("value"))
                )
            elif value.event_type == "orderbook_snapshot":
                bid = _decimal(value.payload.get("bid"))
                ask = _decimal(value.payload.get("ask"))
                if bid <= 0 or ask <= bid:
                    event = next(
                        item
                        for item in self.repository.raw_events()
                        if item.event_id == value.event_id
                    )
                    self.repository.quarantine(
                        event, "schema violation: invalid book", dataset.cutoff_at
                    )
                    continue
                mid = (bid + ask) / 2
                prior = previous_mid.get(key)
                row = {
                    "event_id": value.event_id,
                    "decision_time": value.available_at,
                    "exchange_timestamp": value.exchange_timestamp,
                    "received_at": value.received_at,
                    "available_at": value.available_at,
                    "venue": value.venue,
                    "instrument": value.canonical_instrument_id,
                    "symbol": value.venue_symbol,
                    "bid": bid,
                    "ask": ask,
                    "bid_depth": _decimal(value.payload.get("bid_depth")),
                    "ask_depth": _decimal(value.payload.get("ask_depth")),
                    "mid": mid,
                    "return": Decimal(0) if prior is None else mid / prior - 1,
                    "funding_rate": latest_funding.get(key, Decimal(0)),
                    "open_interest": latest_oi.get(key, Decimal(0)),
                }
                rows.append(row)
                previous_mid[key] = mid
        train_end = max(1, int(len(rows) * float(config.get("train_fraction", 0.6))))
        normalization: dict[str, tuple[Decimal, Decimal]] = {}
        for column in ("return", "funding_rate", "open_interest"):
            train_decimals = [_decimal(row[column]) for row in rows[:train_end]]
            mean = (
                sum(train_decimals, start=Decimal(0)) / Decimal(len(train_decimals))
                if train_decimals
                else Decimal(0)
            )
            std = (
                Decimal(str(float(np.std([float(item) for item in train_decimals]))))
                if train_decimals
                else Decimal(0)
            )
            normalization[column] = (mean, std)
            for row in rows:
                numeric_value = _decimal(row[column])
                row[f"{column}_z"] = Decimal(0) if std == 0 else (numeric_value - mean) / std
        content = canonical_sha256(rows)
        return FeatureArtifact(
            identity.run_id, dataset.snapshot_id, tuple(rows), normalization, content
        )

    @staticmethod
    def _regimes(identity: ResearchRunIdentity, features: FeatureArtifact) -> RegimeArtifact:
        regimes: list[tuple[str, str, str]] = []
        for row in features.rows:
            z = _decimal(row.get("return_z"))
            regime = "TREND_UP" if z > 1 else ("TREND_DOWN" if z < -1 else "RANGE")
            regimes.append((str(row["available_at"]), str(row["instrument"]), regime))
        return RegimeArtifact(
            identity.run_id,
            identity.data_snapshot_id,
            tuple(regimes),
            canonical_sha256(regimes),
        )

    def _rules(
        self, config: dict[str, Any], dataset: PointInTimeDataset
    ) -> dict[tuple[str, str], InstrumentRules]:
        requested_ids = tuple(str(item) for item in config.get("rule_snapshot_ids", ()))
        if not requested_ids:
            if not config.get("raw_events"):
                raise ValueError(
                    "production research requires frozen rule_snapshot_ids from R2 collection"
                )
            # Compatibility import for deterministic R1 fixtures: values are persisted as
            # immutable snapshots before the backtest consumes them.
            configured = config.get("instrument_rules", {})
            imported: list[str] = []
            for venue, instrument, symbol in {
                (item.venue, item.canonical_instrument_id, item.venue_symbol)
                for item in dataset.values
            }:
                raw = dict(configured.get(f"{venue}:{instrument}", configured.get("default", {})))
                source_hash = canonical_sha256(raw)
                rule_id = f"rule-{venue}-{instrument}-{source_hash[:16]}"
                self.repository.save_instrument_rule(
                    InstrumentRuleSnapshot(
                        rule_snapshot_id=rule_id,
                        venue=venue,
                        canonical_instrument_id=instrument,
                        venue_symbol=symbol,
                        tick_size=_decimal(raw.get("tick_size", "0.01")),
                        lot_size=_decimal(raw.get("lot_size", "0.001")),
                        minimum_quantity=_decimal(
                            raw.get("minimum_quantity", raw.get("lot_size", "0.001"))
                        ),
                        minimum_notional=_decimal(raw.get("minimum_notional", "5")),
                        maker_fee=max(Decimal(0), _decimal(raw.get("maker_fee_rate", "0"))),
                        taker_fee=_decimal(raw.get("taker_fee_rate", "0.0006")),
                        maker_rebate=max(Decimal(0), -_decimal(raw.get("maker_fee_rate", "0"))),
                        funding_interval=int(raw.get("funding_interval", 8)),
                        margin_asset=str(raw.get("margin_asset", "USD")),
                        source_endpoint="fixture:legacy-instrument-rules",
                        source_payload_sha256=source_hash,
                        retrieved_at=dataset.cutoff_at,
                        valid_from=dataset.cutoff_at,
                        valid_until=None,
                    )
                )
                imported.append(rule_id)
            requested_ids = tuple(sorted(imported))
        frozen = self.repository.instrument_rules(requested_ids)
        available = {(item.venue, item.canonical_instrument_id): item for item in frozen}
        rules: dict[tuple[str, str], InstrumentRules] = {}
        symbols = {
            (item.venue, item.canonical_instrument_id, item.venue_symbol) for item in dataset.values
        }
        for venue, instrument, symbol in symbols:
            try:
                rule_snapshot = available[(venue, instrument)]
            except KeyError as exc:
                raise ValueError(f"frozen instrument rule missing: {venue}/{instrument}") from exc
            if rule_snapshot.venue_symbol != symbol:
                raise ValueError("frozen instrument rule venue symbol mismatch")
            if not rule_snapshot.valid_from <= dataset.cutoff_at or (
                rule_snapshot.valid_until is not None
                and dataset.cutoff_at >= rule_snapshot.valid_until
            ):
                raise ValueError("instrument rule snapshot is not valid at dataset cutoff")
            rules[(venue, symbol)] = InstrumentRules(
                tick_size=rule_snapshot.tick_size,
                lot_size=rule_snapshot.lot_size,
                minimum_notional=rule_snapshot.minimum_notional,
                maker_fee_rate=rule_snapshot.maker_fee - rule_snapshot.maker_rebate,
                taker_fee_rate=rule_snapshot.taker_fee,
            )
        return rules

    def _run_backtest(
        self,
        identity: ResearchRunIdentity,
        dataset: PointInTimeDataset,
        config: dict[str, Any],
        rules: dict[tuple[str, str], InstrumentRules],
        quality_passed: bool,
        *,
        scenario: str = "base",
    ) -> BacktestResult:
        fee_multiplier = (
            Decimal("2")
            if scenario == "fees_x2.0"
            else (Decimal("1.5") if scenario == "fees_x1.5" else Decimal(1))
        )
        adjusted_rules = {
            key: replace(
                value,
                maker_fee_rate=(
                    Decimal(0)
                    if scenario == "maker_rebate_removal"
                    else value.maker_fee_rate * fee_multiplier
                ),
                taker_fee_rate=value.taker_fee_rate * fee_multiplier,
            )
            for key, value in rules.items()
        }
        slip = (
            Decimal("2")
            if scenario == "slippage_x2.0"
            else (Decimal("1.5") if scenario == "slippage_x1.5" else Decimal(1))
        )
        engine = BacktestEngine(
            _decimal(config.get("initial_cash", "1000")),
            adjusted_rules,
            execution_config=ExecutionModelConfig(
                participation_rate=Decimal("0.125")
                if scenario == "depth_minus_50pct"
                else Decimal("0.25"),
                slippage_bps=_decimal(config.get("slippage_bps", "1")) * slip,
                impact_coefficient_bps=_decimal(config.get("impact_bps", "5")),
            ),
            run_id=identity.run_id,
            data_snapshot_id=identity.data_snapshot_id,
        )
        books = [item for item in dataset.values if item.event_type == "orderbook_snapshot"]
        for value in books:
            payload = value.payload
            depth_factor = Decimal("0.5") if scenario == "depth_minus_50pct" else Decimal(1)
            if scenario == "venue_outage" and value.venue == dataset.venues[0]:
                continue
            engine.add_event(
                MarketEvent(
                    MarketSnapshot(
                        exchange=value.venue,
                        symbol=value.venue_symbol,
                        timestamp=value.available_at,
                        bid=_decimal(payload.get("bid")),
                        ask=_decimal(payload.get("ask")),
                        bid_quantity=_decimal(payload.get("bid_depth")) * depth_factor,
                        ask_quantity=_decimal(payload.get("ask_depth")) * depth_factor,
                        last_price=(
                            _decimal(payload.get("last_price"))
                            if payload.get("last_price")
                            else None
                        ),
                        trade_quantity=(
                            _decimal(payload.get("trade_quantity"))
                            if payload.get("trade_quantity")
                            else None
                        ),
                    )
                )
            )
        if quality_passed:
            signals = self._strategy_signals(identity, dataset, config, scenario)
            engine.add_events(signals)
        for value in dataset.values:
            if value.event_type in {"funding_rate", "funding_current", "funding_history"}:
                edge_multiplier = (
                    Decimal("0.5")
                    if scenario == "funding_edge_minus_50pct"
                    else (Decimal("0.75") if scenario == "funding_edge_minus_25pct" else Decimal(1))
                )
                engine.add_event(
                    FundingEvent(
                        timestamp=value.available_at,
                        exchange=value.venue,
                        symbol=value.venue_symbol,
                        rate=_decimal(value.payload.get("rate")) * edge_multiplier,
                        mark_price=_decimal(value.payload.get("mark_price", "1")),
                    )
                )
        return engine.run()

    @staticmethod
    def _strategy_signals(
        identity: ResearchRunIdentity,
        dataset: PointInTimeDataset,
        config: dict[str, Any],
        scenario: str,
    ) -> tuple[SignalEvent, ...]:
        books = sorted(
            (item for item in dataset.values if item.event_type == "orderbook_snapshot"),
            key=lambda item: (item.available_at, item.event_id),
        )
        groups: dict[datetime, list[Any]] = {}
        for item in books[:-1]:
            groups.setdefault(item.available_at, []).append(item)
        signals: list[SignalEvent] = []
        quantity = _decimal(config.get("order_quantity", "0.01"))
        delay = timedelta(
            milliseconds=int(config.get("latency_ms", 1)) * (2 if scenario == "latency_x2" else 1)
        )
        use_maker = bool(config.get("use_maker_orders", False))
        minimum_edge = _decimal(config.get("minimum_strategy_edge", "0"))
        funding_values = [
            item
            for item in dataset.values
            if item.event_type in {"funding_rate", "funding_current", "funding_history"}
        ]
        sequence = 0
        for timestamp, items in groups.items():
            legs: list[tuple[Any, Side]] = []
            if identity.strategy_id in {"funding_carry", "cross_venue_basis"}:
                same_asset = [
                    item for item in items if item.canonical_instrument_id == dataset.instruments[0]
                ]
                if len(same_asset) >= 2:
                    priced = sorted(
                        same_asset,
                        key=lambda item: (
                            _decimal(item.payload.get("bid")) + _decimal(item.payload.get("ask"))
                        ),
                    )
                    buy, sell = priced[0], priced[-1]
                    basis_edge = (
                        _decimal(sell.payload.get("bid")) - _decimal(buy.payload.get("ask"))
                    ) / _decimal(buy.payload.get("ask"))
                    funding_by_venue = {
                        item.venue: _decimal(item.payload.get("rate"))
                        for item in funding_values
                        if item.canonical_instrument_id == buy.canonical_instrument_id
                        and item.available_at <= timestamp
                    }
                    funding_edge = funding_by_venue.get(
                        sell.venue, Decimal(0)
                    ) - funding_by_venue.get(buy.venue, Decimal(0))
                    edge = funding_edge if identity.strategy_id == "funding_carry" else basis_edge
                    if edge > minimum_edge:
                        legs = [(buy, Side.BUY), (sell, Side.SELL)]
            else:
                btc = next(
                    (item for item in items if item.canonical_instrument_id.startswith("BTC")), None
                )
                sol = next(
                    (item for item in items if item.canonical_instrument_id.startswith("SOL")), None
                )
                if btc is not None and sol is not None:
                    legs = [(btc, Side.BUY), (sol, Side.SELL)]
            if scenario == "one_leg_failure" and len(legs) == 2:
                legs = legs[:1]
            for item, side in legs:
                sequence += 1
                signals.append(
                    SignalEvent(
                        timestamp=timestamp,
                        signal_id=f"{identity.run_id}-{sequence:08d}",
                        exchange=item.venue,
                        symbol=item.venue_symbol,
                        side=side,
                        quantity=quantity,
                        order_type=OrderType.LIMIT if use_maker else OrderType.MARKET,
                        time_in_force=TimeInForce.GTC if use_maker else TimeInForce.IOC,
                        limit_price=(
                            _decimal(item.payload.get("bid"))
                            if use_maker and side is Side.BUY
                            else (_decimal(item.payload.get("ask")) if use_maker else None)
                        ),
                        calculation_delay=delay,
                        submission_delay=delay,
                        post_only=use_maker,
                        reason=identity.strategy_id,
                    )
                )
        return tuple(signals)

    def _walk_forward(
        self,
        identity: ResearchRunIdentity,
        features: FeatureArtifact,
        hypothesis: FrozenHypothesis,
        config: dict[str, Any],
    ) -> WalkForwardResult:
        count = len(features.rows)
        wf = dict(config.get("walk_forward", {}))
        train_size = int(wf.get("train_size", max(4, count // 3)))
        validation_size = int(wf.get("validation_size", max(1, count // 8)))
        oos_size = int(wf.get("oos_size", max(2, count // 6)))
        purge = int(wf.get("purge", 1))
        embargo = int(wf.get("embargo", 1))
        mode: Literal["rolling", "anchored"] = str(wf.get("mode", "rolling"))  # type: ignore[assignment]
        if mode not in {"rolling", "anchored"}:
            raise ValueError("walk-forward mode must be rolling or anchored")
        if count == 0:
            return WalkForwardResult(
                identity.run_id,
                identity.data_snapshot_id,
                mode,
                purge,
                embargo,
                (),
                (),
                (),
                {},
                {},
                False,
                canonical_sha256((identity.run_id, "empty-walk-forward-data")),
            )
        windows = walk_forward_splits(
            count,
            train_size=train_size,
            validation_size=validation_size,
            out_of_sample_size=oos_size,
            anchored=mode == "anchored",
            purge_size=purge,
            embargo_size=embargo,
        )
        if not windows:
            return WalkForwardResult(
                identity.run_id,
                identity.data_snapshot_id,
                mode,
                purge,
                embargo,
                (),
                (),
                (),
                {},
                {},
                False,
                canonical_sha256((identity.run_id, "insufficient-walk-forward-data")),
            )
        first_oos = min(int(index) for window in windows for index in window.out_of_sample)
        first_oos_at = features.rows[first_oos]["decision_time"]
        if hypothesis.frozen_at > first_oos_at:
            raise ValueError("hypothesis must be frozen before the first OOS observation")
        grids = _parameter_combinations(hypothesis.parameter_grid)
        results: list[WalkForwardWindowResult] = []
        combined: list[Decimal] = []
        returns_by_window: list[list[Decimal]] = []
        training_scores_by_grid: list[list[Decimal]] = [[] for _ in grids]
        all_rows = features.rows
        for window in windows:
            normalized_rows = self._normalize_for_train(
                all_rows, tuple(int(item) for item in window.train)
            )
            train_scores = [
                sum(
                    self._strategy_returns(
                        normalized_rows,
                        grid,
                        tuple(int(i) for i in window.train),
                        identity.strategy_id,
                    ),
                    start=Decimal(0),
                )
                for grid in grids
            ]
            for grid_index, score in enumerate(train_scores):
                training_scores_by_grid[grid_index].append(score)
            validation_indices = tuple(int(i) for i in window.validation)
            validation_scores = (
                [
                    sum(
                        self._strategy_returns(
                            normalized_rows,
                            grid,
                            validation_indices,
                            identity.strategy_id,
                        ),
                        start=Decimal(0),
                    )
                    for grid in grids
                ]
                if validation_indices
                else train_scores
            )
            selected = grids[int(np.argmax([float(item) for item in validation_scores]))]
            oos_returns = self._strategy_returns(
                normalized_rows,
                selected,
                tuple(int(i) for i in window.out_of_sample),
                identity.strategy_id,
            )
            combined.extend(oos_returns)
            returns_by_window.append(oos_returns)
            results.append(
                WalkForwardWindowResult(
                    window.number,
                    tuple(int(item) for item in window.train),
                    tuple(int(item) for item in window.validation),
                    tuple(int(item) for item in window.out_of_sample),
                    selected,
                    tuple(oos_returns),
                    _summary(oos_returns),
                )
            )
        period_out: tuple[Decimal, ...] = tuple(
            sum(
                (
                    item
                    for window_index, window_returns in enumerate(returns_by_window)
                    if window_index != omitted
                    for item in window_returns
                ),
                start=Decimal(0),
            )
            for omitted in range(len(returns_by_window))
        )
        assets = sorted({str(row["instrument"]) for row in all_rows})
        venues = sorted({str(row["venue"]) for row in all_rows})
        diagnostic_parameters = results[-1].selected_parameters
        asset_out: dict[str, Decimal] = {}
        for asset in assets:
            filtered = tuple(row for row in all_rows if str(row["instrument"]) != asset)
            asset_out[asset] = sum(
                self._strategy_returns(
                    filtered,
                    diagnostic_parameters,
                    tuple(range(len(filtered))),
                    identity.strategy_id,
                ),
                start=Decimal(0),
            )
        venue_out: dict[str, Decimal] = {}
        for venue in venues:
            filtered = tuple(row for row in all_rows if str(row["venue"]) != venue)
            venue_out[venue] = sum(
                self._strategy_returns(
                    filtered,
                    diagnostic_parameters,
                    tuple(range(len(filtered))),
                    identity.strategy_id,
                ),
                start=Decimal(0),
            )
        plateau = False
        if (
            len(grids) >= 3
            and len(hypothesis.parameter_grid) == 1
            and all(
                isinstance(value, (int, float, Decimal))
                or (isinstance(value, str) and _is_decimal(value))
                for grid in grids
                for value in grid.values()
            )
        ):
            parameter = next(iter(hypothesis.parameter_grid))
            stability_rows = [
                {
                    parameter: float(_decimal(grid[parameter])),
                    "score": float(
                        sum(training_scores_by_grid[index], start=Decimal(0))
                        / Decimal(len(training_scores_by_grid[index]))
                    ),
                }
                for index, grid in enumerate(grids)
            ]
            plateau = analyze_parameter_stability(
                pd.DataFrame(stability_rows),
                parameter_columns=(parameter,),
                score_column="score",
            ).is_stable
        payload = {
            "windows": results,
            "combined": combined,
            "period": period_out,
            "asset": asset_out,
            "venue": venue_out,
        }
        return WalkForwardResult(
            identity.run_id,
            identity.data_snapshot_id,
            mode,
            purge,
            embargo,
            tuple(results),
            tuple(combined),
            period_out,
            asset_out,
            venue_out,
            plateau,
            canonical_sha256(payload),
        )

    @staticmethod
    def _normalize_for_train(
        rows: tuple[dict[str, Any], ...], train_indices: tuple[int, ...]
    ) -> tuple[dict[str, Any], ...]:
        normalized = [dict(row) for row in rows]
        for column in ("return", "funding_rate", "open_interest"):
            values = [_decimal(rows[index][column]) for index in train_indices]
            mean = sum(values, start=Decimal(0)) / Decimal(len(values))
            standard_deviation = Decimal(str(float(np.std([float(item) for item in values]))))
            for row in normalized:
                value = _decimal(row[column])
                row[f"{column}_z"] = (
                    Decimal(0) if standard_deviation == 0 else (value - mean) / standard_deviation
                )
        return tuple(normalized)

    @staticmethod
    def _strategy_returns(
        rows: tuple[dict[str, Any], ...],
        parameters: dict[str, object],
        indices: tuple[int, ...],
        strategy_id: str,
    ) -> list[Decimal]:
        threshold = _decimal(parameters.get("threshold", "0"))
        scale = _decimal(parameters.get("scale", "1"))
        selected = [rows[index] for index in indices]
        groups: dict[datetime, list[dict[str, Any]]] = {}
        for row in selected:
            groups.setdefault(row["decision_time"], []).append(row)
        ordered = sorted(groups.items())
        returns: list[Decimal] = []
        for position, (_, current) in enumerate(ordered[:-1]):
            following = ordered[position + 1][1]
            if strategy_id in {"cross_venue_basis", "funding_carry"}:
                instrument = str(current[0]["instrument"]) if current else ""
                legs = [item for item in current if str(item["instrument"]) == instrument]
                if len({str(item["venue"]) for item in legs}) < 2:
                    continue
                buy = min(legs, key=lambda item: _decimal(item["ask"]))
                sell = max(legs, key=lambda item: _decimal(item["bid"]))
                entry_edge = (_decimal(sell["bid"]) - _decimal(buy["ask"])) / _decimal(buy["ask"])
                funding_edge = _decimal(sell["funding_rate"]) - _decimal(buy["funding_rate"])
                decision_edge = funding_edge if strategy_id == "funding_carry" else entry_edge
                next_buy = next(
                    (
                        item
                        for item in following
                        if item["venue"] == buy["venue"] and item["instrument"] == buy["instrument"]
                    ),
                    None,
                )
                next_sell = next(
                    (
                        item
                        for item in following
                        if item["venue"] == sell["venue"]
                        and item["instrument"] == sell["instrument"]
                    ),
                    None,
                )
                if next_buy is None or next_sell is None:
                    continue
                long_return = _decimal(next_buy["bid"]) / _decimal(buy["ask"]) - 1
                short_return = _decimal(sell["bid"]) / _decimal(next_sell["ask"]) - 1
                outcome = (long_return + short_return + funding_edge) / 2
                returns.append(outcome * scale if decision_edge >= threshold else Decimal(0))
            else:
                btc = next(
                    (item for item in current if str(item["instrument"]).startswith("BTC")), None
                )
                sol = next(
                    (item for item in current if str(item["instrument"]).startswith("SOL")), None
                )
                next_btc = next(
                    (item for item in following if str(item["instrument"]).startswith("BTC")), None
                )
                next_sol = next(
                    (item for item in following if str(item["instrument"]).startswith("SOL")), None
                )
                if btc is None or sol is None or next_btc is None or next_sol is None:
                    continue
                strength = _decimal(btc["return_z"]) - _decimal(sol["return_z"])
                long, short = (btc, sol) if strength >= 0 else (sol, btc)
                next_long, next_short = (
                    (next_btc, next_sol) if strength >= 0 else (next_sol, next_btc)
                )
                outcome = (
                    _decimal(next_long["mid"]) / _decimal(long["mid"])
                    + _decimal(short["mid"]) / _decimal(next_short["mid"])
                    - 2
                ) / 2
                returns.append(outcome * scale if abs(strength) >= threshold else Decimal(0))
        return returns

    def _cost_stress(
        self,
        identity: ResearchRunIdentity,
        dataset: PointInTimeDataset,
        config: dict[str, Any],
        rules: dict[tuple[str, str], InstrumentRules],
        quality_passed: bool,
    ) -> CostStressResult:
        results: list[CostStressScenarioResult] = []
        initial = _decimal(config.get("initial_cash", "1000"))
        for scenario in STRESS_SCENARIOS:
            result = self._run_backtest(
                identity, dataset, config, rules, quality_passed, scenario=scenario
            )
            fills = result.fills
            fees = sum((fill.fee for fill in fills if fill.fee > 0), Decimal(0))
            rebates = -sum((fill.fee for fill in fills if fill.fee < 0), Decimal(0))
            slippage = sum((fill.slippage_cost for fill in fills), Decimal(0))
            impact = sum((fill.market_impact_cost for fill in fills), Decimal(0))
            funding = sum((item.amount for item in result.funding), Decimal(0))
            failed = Decimal(0)
            naked = 0
            hedge_slippage = Decimal(0)
            unwind = Decimal(0)
            outage = Decimal(0)
            if scenario == "one_leg_failure":
                failed = slippage + impact + abs(result.final_equity - initial) * Decimal("1.1")
                naked = int(config.get("maximum_naked_exposure_duration_ms", 3000))
                hedge_slippage = failed / 2
                unwind = failed / 2
            if scenario == "venue_outage":
                outage = max(Decimal(0), initial - result.final_equity)
            net_pnl = result.final_equity - initial - failed
            pnl_return = net_pnl / initial
            equity_returns: list[Decimal] = []
            previous_equity = initial
            for snapshot in result.snapshots:
                if previous_equity > 0:
                    equity_returns.append(snapshot.equity / previous_equity - 1)
                previous_equity = snapshot.equity
            if failed > 0 and previous_equity > 0:
                equity_returns.append(-failed / previous_equity)
            metrics = _summary(equity_returns or [pnl_return], initial)
            turnover = sum((fill.notional for fill in fills), start=Decimal(0)) / initial
            metrics = replace(
                metrics,
                net_pnl=net_pnl,
                turnover=turnover,
            )
            results.append(
                CostStressScenarioResult(
                    scenario,
                    metrics,
                    fees,
                    rebates,
                    funding,
                    slippage,
                    impact,
                    failed,
                    naked,
                    hedge_slippage,
                    unwind,
                    outage,
                )
            )
        return CostStressResult(
            identity.run_id,
            identity.data_snapshot_id,
            tuple(results),
            canonical_sha256(results),
        )

    def _overfitting(
        self,
        identity: ResearchRunIdentity,
        features: FeatureArtifact,
        hypothesis: FrozenHypothesis,
        config: dict[str, Any],
    ) -> OverfittingResult:
        grids = _parameter_combinations(hypothesis.parameter_grid)
        if not features.rows:
            return OverfittingResult(
                identity.run_id,
                identity.data_snapshot_id,
                None,
                None,
                0,
                None,
                None,
                None,
                False,
            )
        matrix = np.asarray(
            [
                [
                    float(item)
                    for item in self._strategy_returns(
                        features.rows,
                        grid,
                        tuple(range(len(features.rows))),
                        identity.strategy_id,
                    )
                ]
                for grid in grids
            ],
            dtype=np.float64,
        ).T
        if matrix.shape[0] < 8 or matrix.shape[1] < 2 or np.all(np.std(matrix, axis=0) < 1e-12):
            return OverfittingResult(
                identity.run_id,
                identity.data_snapshot_id,
                None,
                None,
                0,
                None,
                None,
                None,
                False,
            )
        pbo = probability_of_backtest_overfitting(
            matrix, partitions=min(8, matrix.shape[0] // 2 * 2)
        )
        selected = int(np.argmax(np.mean(matrix, axis=0)))
        if float(np.std(matrix[:, selected], ddof=1)) <= np.finfo(np.float64).eps:
            return OverfittingResult(
                identity.run_id,
                identity.data_snapshot_id,
                Decimal(str(pbo.probability)),
                None,
                pbo.combinations_evaluated,
                None,
                None,
                None,
                False,
            )
        dsr = deflated_sharpe_ratio(matrix[:, selected], trials=len(grids))
        reality = whites_reality_check(matrix, n_resamples=int(config.get("bootstrap_trials", 100)))
        frame_rows = [
            {
                **{key: float(_decimal(value)) for key, value in grid.items()},
                "score": float(np.mean(matrix[:, index])),
            }
            for index, grid in enumerate(grids)
            if all(isinstance(value, (int, float, Decimal)) for value in grid.values())
        ]
        plateau: bool | None = None
        if len(frame_rows) >= 3 and len(hypothesis.parameter_grid) == 1:
            parameter = next(iter(hypothesis.parameter_grid))
            plateau = analyze_parameter_stability(
                pd.DataFrame(frame_rows), parameter_columns=(parameter,), score_column="score"
            ).is_stable
        monte = monte_carlo_paths(
            matrix[:, selected],
            initial_capital=1000,
            n_simulations=int(config.get("monte_carlo_trials", 100)),
            seed=int(config.get("random_seed", 17)),
        )
        return OverfittingResult(
            identity.run_id,
            identity.data_snapshot_id,
            Decimal(str(pbo.probability)),
            Decimal(str(dsr.deflated_sharpe)),
            pbo.combinations_evaluated,
            Decimal(str(reality.p_value)),
            plateau,
            Decimal(str(monte.ruin_probability)),
            True,
        )

    @staticmethod
    def _capital(
        identity: ResearchRunIdentity,
        config: dict[str, Any],
        rules: dict[tuple[str, str], InstrumentRules],
    ) -> CapitalFeasibilityResult:
        venues = tuple(str(item) for item in config["venues"])
        largest_minimum = max(
            (item.minimum_notional for item in rules.values()), default=Decimal(0)
        )
        largest_lot = max((item.lot_size for item in rules.values()), default=Decimal(0))
        quantity = _decimal(config.get("order_quantity", "0.01"))
        fee_buffer = largest_minimum * Decimal("0.002") * max(1, len(venues))
        funding_buffer = largest_minimum * Decimal("0.01")
        liquidation_buffer = largest_minimum * Decimal("0.25") * max(1, len(venues))
        transfer = _decimal(config.get("transfer_lock_buffer", "10"))
        required = (
            largest_minimum * max(1, len(venues))
            + fee_buffer
            + funding_buffer
            + liquidation_buffer
            + transfer
        )
        scenarios: list[CapitalScenarioFeasibility] = []
        for capital in (Decimal("100"), Decimal("300"), Decimal("1000")):
            feasible = capital >= required and quantity >= largest_lot
            reason = (
                f"order quantity {quantity} is below venue lot {largest_lot}"
                if quantity < largest_lot
                else ("feasible" if feasible else f"requires {required}")
            )
            scenarios.append(
                CapitalScenarioFeasibility(
                    capital,
                    feasible,
                    {
                        venue: largest_minimum + liquidation_buffer / max(1, len(venues))
                        for venue in venues
                    },
                    largest_lot,
                    largest_minimum,
                    fee_buffer,
                    funding_buffer,
                    liquidation_buffer,
                    transfer,
                    reason,
                )
            )
        return CapitalFeasibilityResult(
            identity.run_id, identity.data_snapshot_id, tuple(scenarios)
        )

    def _write_artifacts(
        self,
        identity: ResearchRunIdentity,
        dataset: PointInTimeDataset,
        quality: DataQualityResult,
        features: FeatureArtifact,
        regimes: RegimeArtifact,
        backtest: BacktestResult,
        walk_forward: WalkForwardResult,
        stress: CostStressResult,
        overfitting: OverfittingResult,
        acceptance: AcceptanceResult,
        config: dict[str, Any],
    ) -> Path:
        from app.services.research.report import ResearchArtifactWriter

        writer = ResearchArtifactWriter(self.artifact_root, self.repository)
        return writer.write(
            identity=identity,
            dataset=dataset,
            quality=quality,
            features=features,
            regimes=regimes,
            backtest=backtest,
            walk_forward=walk_forward,
            stress=stress,
            overfitting=overfitting,
            acceptance=acceptance,
            deferred_strategies=DEFERRED_STRATEGIES,
            config=config,
        )


def _parse_time(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _parameter_combinations(grid: dict[str, tuple[object, ...]]) -> list[dict[str, object]]:
    if not grid or any(not values for values in grid.values()):
        raise ValueError("parameter grid values must be non-empty")
    keys = tuple(sorted(grid))
    return [
        dict(zip(keys, values, strict=True))
        for values in itertools.product(*(grid[key] for key in keys))
    ]
