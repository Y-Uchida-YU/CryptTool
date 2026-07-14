from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from app.adapters.exchanges.websocket import ReconciliationState
from app.services.backtest.engine import BacktestResult


def utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def canonical_sha256(value: object) -> str:
    def default(item: object) -> object:
        if is_dataclass(item) and not isinstance(item, type):
            return asdict(item)
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, datetime):
            return item.astimezone(UTC).isoformat()
        if isinstance(item, UUID):
            return str(item)
        if isinstance(item, StrEnum):
            return item.value
        if isinstance(item, bytes):
            return item.decode("utf-8")
        raise TypeError(type(item).__name__)

    payload = json.dumps(value, default=default, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class ResearchRunIdentity:
    run_id: str
    commit_sha: str
    config_sha256: str
    data_snapshot_id: str
    hypothesis_version: str
    strategy_id: str
    strategy_version: str
    created_at: datetime

    def __post_init__(self) -> None:
        required = (
            self.run_id,
            self.commit_sha,
            self.config_sha256,
            self.data_snapshot_id,
            self.hypothesis_version,
            self.strategy_id,
            self.strategy_version,
        )
        if not all(required):
            raise ValueError("research run identity fields are required")
        if len(self.commit_sha) != 40 or any(
            character not in "0123456789abcdef" for character in self.commit_sha.lower()
        ):
            raise ValueError("commit_sha must be a 40-character hexadecimal SHA")
        if len(self.config_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.config_sha256.lower()
        ):
            raise ValueError("config_sha256 must be a SHA-256 digest")
        object.__setattr__(self, "created_at", utc(self.created_at, "created_at"))


@dataclass(frozen=True)
class RawMarketEvent:
    event_id: str
    venue: str
    canonical_instrument_id: str
    venue_symbol: str
    event_type: str
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    sequence: int | None
    connection_id: UUID | None
    reconciliation_state: ReconciliationState | None
    payload_sha256: str
    raw_payload: str
    normalizer_version: str
    capability_verification_run_id: str
    created_at: datetime
    raw_payload_id: str | None = None
    source_payload_sha256: str | None = None
    channel: str = "unknown"
    snapshot_sequence: int | None = None
    delta_sequence: int | None = None
    connection_epoch: int | None = None

    def __post_init__(self) -> None:
        if not all(
            (
                self.event_id,
                self.venue,
                self.canonical_instrument_id,
                self.venue_symbol,
                self.event_type,
                self.normalizer_version,
                self.capability_verification_run_id,
            )
        ):
            raise ValueError("raw market event identities are required")
        if hashlib.sha256(self.raw_payload.encode()).hexdigest() != self.payload_sha256:
            raise ValueError("raw payload hash mismatch")
        for name in ("received_at", "available_at", "created_at"):
            object.__setattr__(self, name, utc(getattr(self, name), name))
        if self.exchange_timestamp is not None:
            object.__setattr__(
                self,
                "exchange_timestamp",
                utc(self.exchange_timestamp, "exchange_timestamp"),
            )

    def payload(self) -> dict[str, Any]:
        value = json.loads(self.raw_payload)
        if not isinstance(value, dict):
            raise ValueError("raw payload must be a JSON object")
        return value


@dataclass(frozen=True)
class QuarantinedMarketEvent:
    event: RawMarketEvent
    reason: str
    quarantined_at: datetime


@dataclass(frozen=True)
class DataSnapshotManifest:
    snapshot_id: str
    cutoff_at: datetime
    events: tuple[tuple[int, str, str], ...]
    quarantine_count: int
    quarantine_reasons: tuple[tuple[str, int], ...]
    outage_event_ids: tuple[str, ...]
    degraded_event_ids: tuple[str, ...]
    content_sha256: str
    manifest_sha256: str
    finalized_at: datetime
    eligibility_status: str = "FINALIZED_NOT_ELIGIBLE"
    eligibility_reasons: tuple[str, ...] = ()


class RuleVerificationStatus(StrEnum):
    UNKNOWN = "unknown"
    OBSERVED = "observed"
    VERIFIED = "verified"


class FeeTierKind(StrEnum):
    UNKNOWN = "unknown"
    DEFAULT_PUBLIC = "default_public_tier"
    OPERATOR_ACCOUNT = "operator_account_tier"
    HISTORICAL = "historical_tier"


@dataclass(frozen=True)
class InstrumentRuleSnapshot:
    rule_snapshot_id: str
    venue: str
    canonical_instrument_id: str
    venue_symbol: str
    tick_size: Decimal | None
    lot_size: Decimal | None
    minimum_quantity: Decimal | None
    minimum_notional: Decimal | None
    maker_fee: Decimal | None
    taker_fee: Decimal | None
    maker_rebate: Decimal | None
    funding_interval: int | None
    margin_asset: str | None
    source_endpoint: str
    source_payload_sha256: str
    retrieved_at: datetime
    valid_from: datetime
    valid_until: datetime | None
    field_evidence: dict[str, dict[str, str | None]]
    fee_tier: FeeTierKind = FeeTierKind.UNKNOWN
    verification_status: RuleVerificationStatus = RuleVerificationStatus.UNKNOWN

    def __post_init__(self) -> None:
        if not all(
            (
                self.rule_snapshot_id,
                self.venue,
                self.canonical_instrument_id,
                self.venue_symbol,
                self.source_endpoint,
            )
        ):
            raise ValueError("instrument rule snapshot identities are required")
        if len(self.source_payload_sha256) != 64:
            raise ValueError("source_payload_sha256 must be a SHA-256 digest")
        for name in ("retrieved_at", "valid_from"):
            object.__setattr__(self, name, utc(getattr(self, name), name))
        if self.valid_until is not None:
            object.__setattr__(self, "valid_until", utc(self.valid_until, "valid_until"))


@dataclass(frozen=True)
class CollectionCheckpoint:
    venue: str
    stream_key: str
    connection_id: UUID
    last_sequence: int | None
    last_event_id: str | None
    reconciliation_state: ReconciliationState
    checkpointed_at: datetime
    canonical_instrument_id: str = "SYSTEM"
    venue_symbol: str = "SYSTEM"
    event_type: str = "unknown"
    channel: str = "unknown"
    last_available_at: datetime | None = None
    last_funding_at: datetime | None = None
    last_trade_id: str | None = None
    snapshot_sequence: int | None = None
    delta_sequence: int | None = None
    connection_epoch: int = 0
    recovery_required: bool = False
    checkpoint_namespace: str = "production"

    def __post_init__(self) -> None:
        if not self.checkpoint_namespace:
            raise ValueError("checkpoint_namespace is required")


@dataclass(frozen=True)
class CollectionFailureEvent:
    venue: str
    stream_key: str
    instrument: str
    event_type: str
    endpoint: str
    error_type: str
    error_message: str
    occurred_at: datetime
    retry_count: int


@dataclass(frozen=True)
class PointInTimeValue:
    event_id: str
    venue: str
    canonical_instrument_id: str
    venue_symbol: str
    event_type: str
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    payload: dict[str, Any]
    sequence: int | None

    def require_available(self, decision_time: datetime) -> None:
        if self.available_at > utc(decision_time, "decision_time"):
            raise ValueError("future data leakage: value was not available at decision time")


@dataclass(frozen=True)
class PointInTimeDataset:
    snapshot_id: str
    cutoff_at: datetime
    instruments: tuple[str, ...]
    venues: tuple[str, ...]
    event_types: tuple[str, ...]
    values: tuple[PointInTimeValue, ...]
    excluded_future_event_ids: tuple[str, ...]
    retained_delisted_event_ids: tuple[str, ...]
    retained_outage_event_ids: tuple[str, ...]
    content_sha256: str


@dataclass(frozen=True)
class DataQualityResult:
    run_id: str
    data_snapshot_id: str
    passed: bool
    coverage_ratio: Decimal
    stale_ratio: Decimal
    duplicate_ratio: Decimal
    sequence_gap_count: int
    out_of_order_count: int
    quarantine_ratio: Decimal
    venue_outage_duration_seconds: Decimal
    maximum_clock_skew_seconds: Decimal
    cross_venue_divergence: Decimal
    missing_funding_windows: int
    missing_oi_windows: int
    book_depth_availability: Decimal
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class FeatureArtifact:
    run_id: str
    data_snapshot_id: str
    rows: tuple[dict[str, Any], ...]
    train_normalization: dict[str, tuple[Decimal, Decimal]]
    content_sha256: str


@dataclass(frozen=True)
class RegimeArtifact:
    run_id: str
    data_snapshot_id: str
    regimes: tuple[tuple[str, str, str], ...]
    content_sha256: str


@dataclass(frozen=True)
class FrozenHypothesis:
    hypothesis_version: str
    strategy_id: str
    parameter_grid: dict[str, tuple[object, ...]]
    primary_metric: str
    secondary_metrics: tuple[str, ...]
    acceptance_thresholds: dict[str, Decimal]
    frozen_at: datetime
    content_sha256: str

    @classmethod
    def freeze(
        cls,
        *,
        hypothesis_version: str,
        strategy_id: str,
        parameter_grid: dict[str, tuple[object, ...]],
        primary_metric: str,
        secondary_metrics: tuple[str, ...],
        acceptance_thresholds: dict[str, Decimal],
        frozen_at: datetime,
    ) -> FrozenHypothesis:
        payload = {
            "hypothesis_version": hypothesis_version,
            "strategy_id": strategy_id,
            "parameter_grid": parameter_grid,
            "primary_metric": primary_metric,
            "secondary_metrics": secondary_metrics,
            "acceptance_thresholds": acceptance_thresholds,
            "frozen_at": utc(frozen_at, "frozen_at"),
        }
        return cls(
            hypothesis_version=hypothesis_version,
            strategy_id=strategy_id,
            parameter_grid=parameter_grid,
            primary_metric=primary_metric,
            secondary_metrics=secondary_metrics,
            acceptance_thresholds=acceptance_thresholds,
            frozen_at=utc(frozen_at, "frozen_at"),
            content_sha256=canonical_sha256(payload),
        )

    def verify(self) -> None:
        payload = asdict(self)
        claimed = str(payload.pop("content_sha256"))
        if canonical_sha256(payload) != claimed:
            raise ValueError("frozen hypothesis content changed")


@dataclass(frozen=True)
class PerformanceSummary:
    net_pnl: Decimal
    sharpe: Decimal
    sortino: Decimal
    maximum_drawdown: Decimal
    turnover: Decimal
    win_rate: Decimal
    profit_factor: Decimal
    tail_loss: Decimal
    ruin_probability: Decimal
    capital_efficiency: Decimal


@dataclass(frozen=True)
class WalkForwardWindowResult:
    number: int
    train_indices: tuple[int, ...]
    validation_indices: tuple[int, ...]
    oos_indices: tuple[int, ...]
    selected_parameters: dict[str, object]
    oos_returns: tuple[Decimal, ...]
    oos: PerformanceSummary


@dataclass(frozen=True)
class WalkForwardResult:
    run_id: str
    data_snapshot_id: str
    mode: str
    purge_size: int
    embargo_size: int
    windows: tuple[WalkForwardWindowResult, ...]
    combined_oos_returns: tuple[Decimal, ...]
    leave_one_period_out: tuple[Decimal, ...]
    leave_one_asset_out: dict[str, Decimal]
    leave_one_venue_out: dict[str, Decimal]
    parameter_plateau: bool
    content_sha256: str


@dataclass(frozen=True)
class CostStressScenarioResult:
    scenario: str
    metrics: PerformanceSummary
    fee: Decimal
    rebate: Decimal
    funding: Decimal
    slippage: Decimal
    impact: Decimal
    failed_leg_cost: Decimal
    naked_exposure_duration_ms: int
    hedge_slippage: Decimal
    unwind_cost: Decimal
    venue_outage_loss: Decimal


@dataclass(frozen=True)
class CostStressResult:
    run_id: str
    data_snapshot_id: str
    scenarios: tuple[CostStressScenarioResult, ...]
    content_sha256: str
    evidence_complete: bool = True

    def scenario(self, name: str) -> CostStressScenarioResult | None:
        return next((item for item in self.scenarios if item.scenario == name), None)


@dataclass(frozen=True)
class OverfittingResult:
    run_id: str
    data_snapshot_id: str
    pbo: Decimal | None
    deflated_sharpe: Decimal | None
    cscv_combinations: int
    reality_check_p_value: Decimal | None
    parameter_plateau: bool | None
    monte_carlo_ruin_probability: Decimal | None
    evidence_complete: bool


@dataclass(frozen=True)
class CapitalScenarioFeasibility:
    capital: Decimal
    feasible: bool
    required_collateral_by_venue: dict[str, Decimal]
    minimum_order_size: Decimal
    minimum_notional: Decimal
    fee_buffer: Decimal
    funding_buffer: Decimal
    liquidation_buffer: Decimal
    transfer_lock_buffer: Decimal
    reason: str


@dataclass(frozen=True)
class CapitalFeasibilityResult:
    run_id: str
    data_snapshot_id: str
    scenarios: tuple[CapitalScenarioFeasibility, ...]
    evidence_complete: bool = True

    def feasible_at(self, capital: Decimal) -> bool:
        match = next((item for item in self.scenarios if item.capital == capital), None)
        return bool(match and match.feasible)


class AcceptanceVerdict(StrEnum):
    PASS = "PASS"  # nosec B105
    FAIL = "FAIL"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass(frozen=True)
class AcceptanceCheckResult:
    name: str
    passed: bool | None
    observed: str
    criterion: str


@dataclass(frozen=True)
class AcceptanceResult:
    run_id: str
    data_snapshot_id: str
    verdict: AcceptanceVerdict
    checks: tuple[AcceptanceCheckResult, ...]
    capital_feasibility: CapitalFeasibilityResult

    @property
    def overall(self) -> str:
        return self.verdict.value


@dataclass(frozen=True)
class ResearchRunResult:
    identity: ResearchRunIdentity
    data_quality: DataQualityResult
    feature_artifact: FeatureArtifact
    regime_artifact: RegimeArtifact
    backtest_result: BacktestResult
    walk_forward_result: WalkForwardResult
    cost_stress_result: CostStressResult
    overfitting_result: OverfittingResult
    acceptance_result: AcceptanceResult
    artifact_manifest_path: str
