from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from app.domain.market_data.models import Side


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


class OperationMode(StrEnum):
    OBSERVATION_ONLY = "observation_only"
    STRICT_PAPER = "strict_paper"


class OperationalRunStatus(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOP_REQUESTED = "stop_requested"
    STOPPED = "stopped"
    FAILED = "failed"


class StrategyEligibilityStatus(StrEnum):
    ELIGIBLE = "eligible"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    DATA_QUALITY_FAILED = "data_quality_failed"
    RESEARCH_FAILED = "research_failed"
    CAPITAL_INFEASIBLE = "capital_infeasible"
    SUSPENDED = "suspended"


class PaperPromotionVerdict(StrEnum):
    NOT_READY = "not_ready"
    CONTINUE_OBSERVATION = "continue_observation"
    ELIGIBLE_FOR_MICRO_LIVE_REVIEW = "eligible_for_micro_live_review"


class CollectorHealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class SignalDisposition(StrEnum):
    CANDIDATE = "candidate"
    REJECTED_CAPABILITY = "rejected_capability"
    REJECTED_DATA_QUALITY = "rejected_data_quality"
    REJECTED_RESEARCH = "rejected_research"
    REJECTED_CAPITAL = "rejected_capital"
    REJECTED_RISK = "rejected_risk"
    ELIGIBLE = "eligible"


class ResearchExecutionStatus(StrEnum):
    NOT_SCHEDULED = "not_scheduled"
    SKIPPED_SNAPSHOT_INELIGIBLE = "skipped_snapshot_ineligible"
    FAILED = "failed"
    COMPLETED = "completed"


class CapitalFeasibilityStatus(StrEnum):
    NOT_EVALUATED = "not_evaluated"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    FEASIBLE = "feasible"
    INFEASIBLE = "infeasible"


@dataclass(frozen=True)
class CollectorHealthSummary:
    status: CollectorHealthStatus
    total_failures: int
    fatal_failures: int
    degraded_failures: int
    expected_skips: int
    failures_by_venue: dict[str, int]
    failures_by_instrument: dict[str, int]
    failures_by_event_type: dict[str, int]
    failures_by_error_type: dict[str, int]
    permanently_degraded_streams: int
    recovery_required_streams: int
    checkpoint_lag_max_seconds: Decimal | None
    last_healthy_at: datetime | None
    reasons: tuple[str, ...]
    production_market_event_count: int = 0
    production_control_event_count: int = 0
    experimental_market_event_count: int = 0


@dataclass(frozen=True)
class OperationalIdentity:
    run_id: str
    strategy_id: str
    strategy_version: str
    data_snapshot_id: str
    research_run_id: str
    created_at: datetime
    commit_sha: str
    config_sha256: str

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("operational timestamps must be timezone-aware")
        if not all(
            (
                self.run_id,
                self.strategy_id,
                self.strategy_version,
                self.data_snapshot_id,
                self.research_run_id,
                self.commit_sha,
                self.config_sha256,
            )
        ):
            raise ValueError("operational identity fields cannot be empty")


@dataclass(frozen=True)
class OperationalRun:
    run_id: str
    commit_sha: str
    config_sha256: str
    mode: OperationMode
    status: OperationalRunStatus
    started_at: datetime
    updated_at: datetime
    last_snapshot_id: str | None = None
    last_research_run_ids: tuple[str, ...] = ()
    collector_healthy: bool = False
    signals_paused_reason: str | None = None
    failure_reason: str | None = None
    research_status: ResearchExecutionStatus = ResearchExecutionStatus.NOT_SCHEDULED
    research_skip_reason: str | None = None


@dataclass(frozen=True)
class StrategyEligibilityRecord:
    strategy_id: str
    strategy_version: str
    status: StrategyEligibilityStatus
    research_run_id: str
    data_snapshot_id: str
    evaluated_at: datetime
    expires_at: datetime
    reasons: tuple[str, ...]
    capital_feasibility_status: CapitalFeasibilityStatus = CapitalFeasibilityStatus.NOT_EVALUATED

    def valid_at(self, now: datetime) -> bool:
        return (
            self.status is StrategyEligibilityStatus.ELIGIBLE
            and self.evaluated_at <= now <= self.expires_at
        )


@dataclass(frozen=True)
class LiveSignalInput:
    event_id: str
    venue: str
    instrument: str
    event_type: str
    available_at: datetime
    data_quality_score: float
    capability_support: str
    reconciliation_state: str | None
    bid: Decimal | None = None
    ask: Decimal | None = None
    bid_size: Decimal | None = None
    ask_size: Decimal | None = None
    funding_rate: Decimal | None = None


@dataclass(frozen=True)
class PaperSignal:
    identity: OperationalIdentity
    signal_id: str
    decision_time: datetime
    venue_legs: tuple[str, ...]
    instrument: str
    side: Side
    quantity: Decimal
    expected_gross_edge: Decimal
    expected_net_edge: Decimal
    expected_fee: Decimal
    expected_rebate: Decimal
    expected_funding: Decimal
    expected_slippage: Decimal
    expected_impact: Decimal
    required_capabilities: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    disposition: SignalDisposition = SignalDisposition.CANDIDATE
    rejection_reason: str | None = None
    full_signal_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("signal quantity must be positive")
        object.__setattr__(
            self,
            "full_signal_hash",
            canonical_sha256(
                {
                    "identity": asdict(self.identity),
                    "signal_id": self.signal_id,
                    "decision_time": self.decision_time,
                    "venue_legs": self.venue_legs,
                    "instrument": self.instrument,
                    "side": self.side,
                    "quantity": self.quantity,
                    "expected_gross_edge": self.expected_gross_edge,
                    "expected_net_edge": self.expected_net_edge,
                    "expected_fee": self.expected_fee,
                    "expected_rebate": self.expected_rebate,
                    "expected_funding": self.expected_funding,
                    "expected_slippage": self.expected_slippage,
                    "expected_impact": self.expected_impact,
                    "required_capabilities": self.required_capabilities,
                    "source_event_ids": self.source_event_ids,
                    "disposition": self.disposition,
                    "rejection_reason": self.rejection_reason,
                }
            ),
        )


@dataclass(frozen=True)
class PaperOrderRecord:
    identity: OperationalIdentity
    order_id: str
    signal_id: str
    portfolio_id: str
    venue: str
    instrument: str
    side: Side
    requested_quantity: Decimal
    filled_quantity: Decimal
    status: str
    submitted_at: datetime
    updated_at: datetime
    leg_role: str | None = None
    rejection_reason: str | None = None


@dataclass(frozen=True)
class PaperFillRecord:
    identity: OperationalIdentity
    fill_id: str
    order_id: str
    portfolio_id: str
    venue: str
    instrument: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee_paid: Decimal
    rebate_received: Decimal
    slippage_cost: Decimal
    impact_cost: Decimal
    executed_at: datetime
    latency_ms: int
    leg_role: str | None = None


@dataclass(frozen=True)
class PaperPositionRecord:
    identity: OperationalIdentity
    portfolio_id: str
    venue: str
    instrument: str
    quantity: Decimal
    average_entry: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    funding_pnl: Decimal
    updated_at: datetime


@dataclass(frozen=True)
class PaperCashLedgerEntry:
    identity: OperationalIdentity
    entry_id: str
    portfolio_id: str
    amount: Decimal
    balance_after: Decimal
    entry_type: str
    occurred_at: datetime
    reference_id: str | None = None


@dataclass(frozen=True)
class PaperFundingLedgerEntry:
    identity: OperationalIdentity
    entry_id: str
    portfolio_id: str
    venue: str
    instrument: str
    rate: Decimal
    amount: Decimal
    occurred_at: datetime


@dataclass(frozen=True)
class PaperRiskEvent:
    identity: OperationalIdentity
    event_id: str
    portfolio_id: str
    event_type: str
    reason: str
    occurred_at: datetime
    blocks_new_signals: bool = True


@dataclass(frozen=True)
class PaperDailyMetric:
    identity: OperationalIdentity
    portfolio_id: str
    metric_date: date
    starting_equity: Decimal
    ending_equity: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    fees: Decimal
    rebates: Decimal
    funding: Decimal
    slippage: Decimal
    impact: Decimal
    failed_leg_cost: Decimal
    maximum_drawdown: Decimal
    capital_usage: Decimal


@dataclass(frozen=True)
class PaperAttribution:
    identity: OperationalIdentity
    portfolio_id: str
    attribution_date: date
    expected_gross_pnl: Decimal
    actual_paper_gross_pnl: Decimal
    expected_net_pnl: Decimal
    actual_paper_net_pnl: Decimal
    fee_difference: Decimal
    rebate_difference: Decimal
    funding_difference: Decimal
    slippage_difference: Decimal
    impact_difference: Decimal
    fill_rate_difference: Decimal
    latency_difference: Decimal
    failed_leg_difference: Decimal
    outage_difference: Decimal
    implementation_shortfall: Decimal
    edge_decay: Decimal
    signal_to_fill_latency_ms: Decimal
    fill_ratio: Decimal
    paper_backtest_pnl_ratio: Decimal | None
    paper_backtest_sharpe_ratio: Decimal | None


@dataclass(frozen=True)
class PortfolioState:
    portfolio_id: str
    initial_capital: Decimal
    cash: Decimal
    peak_equity: Decimal
    current_equity: Decimal
    daily_start_equity: Decimal
    halted: bool = False
    halt_reason: str | None = None


@dataclass(frozen=True)
class DailyOperationReport:
    run_id: str
    report_date: date
    snapshot_id: str
    research_run_ids: tuple[str, ...]
    eligibility: tuple[StrategyEligibilityRecord, ...]
    signal_count: int
    rejected_signal_count: int
    paper_order_count: int
    paper_fill_count: int
    metrics: tuple[PaperDailyMetric, ...]
    attribution: tuple[PaperAttribution, ...]
    risk_events: tuple[PaperRiskEvent, ...]
    promotion_verdict: PaperPromotionVerdict
    collector_health: CollectorHealthSummary
    research_status: ResearchExecutionStatus
    research_skip_reason: str | None
    capital_feasibility: dict[str, CapitalFeasibilityStatus]
