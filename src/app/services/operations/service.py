from __future__ import annotations

import asyncio
import csv
import json
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from app.adapters.notifications.base import NotificationAdapter, NullNotificationAdapter
from app.config.settings import ContinuousPaperSettings, Settings
from app.domain.market_data.models import Side
from app.services.operations.models import (
    DailyOperationReport,
    LiveSignalInput,
    OperationalIdentity,
    OperationalRun,
    OperationalRunStatus,
    OperationMode,
    PaperAttribution,
    PaperCashLedgerEntry,
    PaperDailyMetric,
    PaperFillRecord,
    PaperFundingLedgerEntry,
    PaperOrderRecord,
    PaperPositionRecord,
    PaperPromotionVerdict,
    PaperRiskEvent,
    PaperSignal,
    PortfolioState,
    StrategyEligibilityRecord,
    StrategyEligibilityStatus,
    canonical_sha256,
)
from app.services.operations.repository import OperationalRepository


class Worker(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...


class ScheduledWorker:
    def __init__(
        self, name: str, interval_seconds: float, action: Callable[[], Awaitable[None]]
    ) -> None:
        self.name = name
        self.interval_seconds = interval_seconds
        self.action = action

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.action()
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.interval_seconds)


class CollectorSupervisor(ScheduledWorker):
    pass


class SnapshotFinalizer(ScheduledWorker):
    pass


class ResearchScheduler(ScheduledWorker):
    pass


class SignalScheduler(ScheduledWorker):
    pass


class PaperExecutionWorker(ScheduledWorker):
    pass


class PaperRiskWorker(ScheduledWorker):
    pass


class ReportingWorker(ScheduledWorker):
    pass


@dataclass(frozen=True)
class ScheduledResearchOutcome:
    strategy_id: str
    strategy_version: str
    research_run_id: str
    research_verdict: str
    data_quality_passed: bool
    capital_feasible: bool
    evidence_complete: bool


class ContinuousResearchPaperService:
    """Durable orchestration for collection-to-paper operation.

    It deliberately has no dependency on an execution adapter. All fills are local simulations.
    """

    def __init__(
        self,
        *,
        repository: OperationalRepository,
        settings: Settings,
        run_id: str,
        commit_sha: str,
        config_sha256: str,
        notifier: NotificationAdapter | None = None,
        mode: OperationMode | None = None,
        local_smoke: bool = False,
        artifact_root: Path = Path("artifacts/operations"),
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        snapshot_action: Callable[[datetime], str] | None = None,
        research_action: Callable[[str], Sequence[ScheduledResearchOutcome]] | None = None,
        market_event_action: Callable[[], Sequence[LiveSignalInput]] | None = None,
    ) -> None:
        self.repository = repository
        self.settings = settings
        self.operation_settings: ContinuousPaperSettings = settings.continuous_paper
        self.run_id = run_id
        self.commit_sha = commit_sha
        self.config_sha256 = config_sha256
        self.notifier = notifier or NullNotificationAdapter()
        self.mode = mode or (
            OperationMode.OBSERVATION_ONLY
            if self.operation_settings.observation_only
            else OperationMode.STRICT_PAPER
        )
        self.local_smoke = local_smoke
        self.artifact_root = artifact_root
        self.now = now
        self.snapshot_action = snapshot_action
        self.research_action = research_action
        self.market_event_action = market_event_action
        self.stop_event = asyncio.Event()
        self._latest_events: dict[tuple[str, str, str], LiveSignalInput] = {}
        self._portfolio_states: dict[str, PortfolioState] = {}
        self._validate_startup()
        self._restore_or_initialize()
        self.workers: tuple[Worker, ...] = (
            CollectorSupervisor("collector", 10, self._collector_tick),
            SnapshotFinalizer(
                "snapshot",
                self.operation_settings.operational_snapshot_interval_seconds,
                self._snapshot_tick,
            ),
            ResearchScheduler("research", 60, self._research_tick),
            SignalScheduler(
                "signals", self.operation_settings.signal_interval_seconds, self._signal_tick
            ),
            PaperExecutionWorker("paper_execution", 1, self._paper_execution_tick),
            PaperRiskWorker(
                "paper_risk", self.operation_settings.risk_interval_seconds, self._risk_tick
            ),
            ReportingWorker("reporting", 60, self._reporting_tick),
        )

    def _validate_startup(self) -> None:
        if self.settings.live_trading or self.settings.live.enabled:
            raise ValueError(
                "continuous paper operation fails closed when live execution is enabled"
            )
        if not self.settings.paper_trading or not self.settings.paper.enabled:
            raise ValueError("continuous paper operation requires paper trading enabled")
        if (
            self.mode is OperationMode.OBSERVATION_ONLY
            and not self.operation_settings.observation_only
        ):
            raise ValueError("observation_only mode must be explicitly configured")
        if self.mode is OperationMode.STRICT_PAPER and self.operation_settings.observation_only:
            raise ValueError("strict_paper cannot start while observation_only is configured")
        if not self.local_smoke and not self.repository.durable:
            raise ValueError("continuous paper operation requires a durable PostgreSQL repository")
        if not self.local_smoke and self.settings.database_url.startswith("sqlite"):
            raise ValueError("SQLite is limited to tests and local smoke")
        if set(self.operation_settings.venues) != {"hyperliquid", "bitget"}:
            raise ValueError("R3 supports only Hyperliquid and Bitget")
        allowed_strategies = {
            "funding_carry",
            "cross_venue_basis",
            "btc_sol_relative_strength",
        }
        if not set(self.operation_settings.strategies) <= allowed_strategies:
            raise ValueError("R3 cannot enable deferred or new strategies")

    def _restore_or_initialize(self) -> None:
        existing = self.repository.get_run(self.run_id)
        now = self.now()
        if existing is None:
            run = OperationalRun(
                run_id=self.run_id,
                commit_sha=self.commit_sha,
                config_sha256=self.config_sha256,
                mode=self.mode,
                status=OperationalRunStatus.STARTING,
                started_at=now,
                updated_at=now,
            )
            self.repository.save_run(run)
        elif existing.commit_sha != self.commit_sha or existing.config_sha256 != self.config_sha256:
            raise ValueError("restart identity does not match persisted operational run")
        cash_entries = self.repository.cash_entries(self.run_id)
        positions = self.repository.positions(self.run_id)
        for capital in self.operation_settings.initial_capitals:
            portfolio_id = f"usd-{capital}"
            portfolio_entries = [item for item in cash_entries if item.portfolio_id == portfolio_id]
            cash = portfolio_entries[-1].balance_after if portfolio_entries else capital
            state = PortfolioState(
                portfolio_id=portfolio_id,
                initial_capital=capital,
                cash=cash,
                peak_equity=max(capital, cash),
                current_equity=cash,
                daily_start_equity=capital,
            )
            self._portfolio_states[portfolio_id] = state
            if not portfolio_entries:
                identity = self._identity("SYSTEM", "0", "UNASSIGNED", "UNASSIGNED", now)
                self.repository.add_cash_entry(
                    PaperCashLedgerEntry(
                        identity=identity,
                        entry_id=f"{self.run_id}:{portfolio_id}:initial",
                        portfolio_id=portfolio_id,
                        amount=capital,
                        balance_after=capital,
                        entry_type="initial_capital",
                        occurred_at=now,
                    )
                )
        for position in positions:
            state = self._portfolio_states[position.portfolio_id]
            self._portfolio_states[position.portfolio_id] = replace(
                state,
                current_equity=state.cash + position.unrealized_pnl,
                peak_equity=max(state.peak_equity, state.cash + position.unrealized_pnl),
            )

    async def run(self) -> None:
        current = self._require_run()
        self.repository.save_run(
            replace(current, status=OperationalRunStatus.RUNNING, updated_at=self.now())
        )
        try:
            async with asyncio.TaskGroup() as group:
                for worker in self.workers:
                    group.create_task(worker.run(self.stop_event), name=type(worker).__name__)
        except BaseException as exc:
            run = self._require_run()
            self.repository.save_run(
                replace(
                    run,
                    status=OperationalRunStatus.FAILED,
                    updated_at=self.now(),
                    failure_reason=f"{type(exc).__name__}: {exc}",
                )
            )
            await self.notifier.send("Paper operation failed", type(exc).__name__, "error")
            raise
        run = self._require_run()
        self.repository.save_run(
            replace(run, status=OperationalRunStatus.STOPPED, updated_at=self.now())
        )

    def request_stop(self) -> None:
        run = self._require_run()
        self.repository.save_run(
            replace(run, status=OperationalRunStatus.STOP_REQUESTED, updated_at=self.now())
        )
        self.stop_event.set()

    def set_collector_health(self, healthy: bool, reason: str | None = None) -> None:
        run = self._require_run()
        pause = None if healthy else reason or "collector_unhealthy"
        self.repository.save_run(
            replace(
                run,
                collector_healthy=healthy,
                signals_paused_reason=pause,
                updated_at=self.now(),
            )
        )

    def bind_snapshot(self, snapshot_id: str, research_run_ids: Sequence[str] = ()) -> None:
        if not snapshot_id:
            raise ValueError("snapshot_id cannot be empty")
        run = self._require_run()
        self.repository.save_run(
            replace(
                run,
                last_snapshot_id=snapshot_id,
                last_research_run_ids=tuple(research_run_ids),
                signals_paused_reason=None if run.collector_healthy else run.signals_paused_reason,
                updated_at=self.now(),
            )
        )

    def snapshot_failed(self, reason: str) -> None:
        run = self._require_run()
        self.repository.save_run(
            replace(run, signals_paused_reason=f"snapshot_failed:{reason}", updated_at=self.now())
        )

    def refresh_eligibility(
        self,
        *,
        strategy_id: str,
        strategy_version: str,
        research_run_id: str,
        data_snapshot_id: str,
        research_verdict: str,
        data_quality_passed: bool,
        capital_feasible: bool,
        evidence_complete: bool = True,
    ) -> StrategyEligibilityRecord:
        now = self.now()
        reasons: list[str] = []
        if not evidence_complete:
            status = StrategyEligibilityStatus.INSUFFICIENT_EVIDENCE
            reasons.append("research evidence is incomplete")
        elif not data_quality_passed:
            status = StrategyEligibilityStatus.DATA_QUALITY_FAILED
            reasons.append("data quality gate failed")
        elif research_verdict != "PASS":
            status = StrategyEligibilityStatus.RESEARCH_FAILED
            reasons.append(f"research verdict={research_verdict}")
        elif not capital_feasible:
            status = StrategyEligibilityStatus.CAPITAL_INFEASIBLE
            reasons.append("capital feasibility gate failed")
        else:
            status = StrategyEligibilityStatus.ELIGIBLE
        record = StrategyEligibilityRecord(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            status=status,
            research_run_id=research_run_id,
            data_snapshot_id=data_snapshot_id,
            evaluated_at=now,
            expires_at=now + timedelta(seconds=self.operation_settings.eligibility_ttl_seconds),
            reasons=tuple(reasons),
        )
        self.repository.save_eligibility(
            self.run_id,
            record,
            commit_sha=self.commit_sha,
            config_sha256=self.config_sha256,
        )
        return record

    def record_market_event(self, event: LiveSignalInput) -> None:
        self._latest_events[(event.venue, event.instrument, event.event_type)] = event

    def generate_signal(
        self,
        *,
        strategy_id: str,
        strategy_version: str,
        instrument: str,
        side: Side,
        quantity: Decimal,
        events: Sequence[LiveSignalInput],
        expected_gross_edge: Decimal,
        expected_fee: Decimal = Decimal("0"),
        expected_rebate: Decimal = Decimal("0"),
        expected_funding: Decimal = Decimal("0"),
        expected_slippage: Decimal = Decimal("0"),
        expected_impact: Decimal = Decimal("0"),
        required_capabilities: tuple[str, ...] = (),
        decision_time: datetime | None = None,
    ) -> PaperSignal:
        decision_time = decision_time or self.now()
        run = self._require_run()
        eligibility = next(
            (
                item
                for item in self.repository.eligibility(self.run_id)
                if item.strategy_id == strategy_id and item.strategy_version == strategy_version
            ),
            None,
        )
        rejection = self._signal_rejection(run, eligibility, events, instrument, decision_time)
        snapshot_id = (
            eligibility.data_snapshot_id if eligibility else run.last_snapshot_id or "UNASSIGNED"
        )
        research_run_id = eligibility.research_run_id if eligibility else "UNASSIGNED"
        identity = self._identity(
            strategy_id,
            strategy_version,
            snapshot_id,
            research_run_id,
            decision_time,
        )
        signal_id = canonical_sha256(
            {
                "run_id": self.run_id,
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "instrument": instrument,
                "decision_time": decision_time.isoformat(),
                "event_ids": sorted(item.event_id for item in events),
            }
        )
        signal = PaperSignal(
            identity=identity,
            signal_id=signal_id,
            decision_time=decision_time,
            venue_legs=tuple(dict.fromkeys(item.venue for item in events)),
            instrument=instrument,
            side=side,
            quantity=quantity,
            expected_gross_edge=expected_gross_edge,
            expected_net_edge=(
                expected_gross_edge
                - expected_fee
                + expected_rebate
                + expected_funding
                - expected_slippage
                - expected_impact
            ),
            expected_fee=expected_fee,
            expected_rebate=expected_rebate,
            expected_funding=expected_funding,
            expected_slippage=expected_slippage,
            expected_impact=expected_impact,
            required_capabilities=required_capabilities,
            source_event_ids=tuple(item.event_id for item in events),
            rejection_reason=rejection,
        )
        self.repository.add_signal(signal)
        return signal

    def _signal_rejection(
        self,
        run: OperationalRun,
        eligibility: StrategyEligibilityRecord | None,
        events: Sequence[LiveSignalInput],
        instrument: str,
        decision_time: datetime,
    ) -> str | None:
        if not run.collector_healthy:
            return run.signals_paused_reason or "collector_unhealthy"
        if run.signals_paused_reason:
            return run.signals_paused_reason
        if run.last_snapshot_id is None:
            return "no_current_snapshot"
        if eligibility is None:
            return "missing_strategy_eligibility"
        if eligibility.data_snapshot_id != run.last_snapshot_id:
            return "eligibility_snapshot_mismatch"
        if not eligibility.valid_at(decision_time):
            return f"eligibility_{eligibility.status.value}_or_expired"
        if not events:
            return "missing_source_events"
        if len({event.event_id for event in events}) != len(events):
            return "duplicate_source_event"
        maximum_age = timedelta(seconds=self.operation_settings.source_event_max_age_seconds)
        for event in events:
            if event.instrument != instrument:
                return "wrong_instrument_source_event"
            age = decision_time - event.available_at
            if not timedelta(0) <= age <= maximum_age:
                return "stale_or_future_source_event"
            if event.capability_support != "live_verified":
                return "capability_not_live_verified"
            if event.data_quality_score < self.operation_settings.minimum_data_quality:
                return "source_event_data_quality_failed"
            if (
                event.event_type.startswith("orderbook")
                and event.reconciliation_state != "synchronized"
            ):
                return "orderbook_unsynchronized"
        return None

    def execute_signal(
        self,
        signal: PaperSignal,
        *,
        quotes: Sequence[LiveSignalInput],
        maker: bool = False,
        maker_fee_rate: Decimal = Decimal("0.0002"),
        taker_fee_rate: Decimal = Decimal("0.0006"),
        maker_rebate_rate: Decimal = Decimal("0"),
        maximum_participation: Decimal = Decimal("0.10"),
        impact_bps: Decimal = Decimal("1"),
        latency_ms: int = 200,
        minimum_quantity: Decimal = Decimal("0"),
        minimum_notional: Decimal = Decimal("5"),
        fail_leg_role: str | None = None,
    ) -> tuple[PaperOrderRecord, ...]:
        if self.mode is OperationMode.OBSERVATION_ONLY:
            return ()
        if signal.rejection_reason is not None:
            return ()
        eligibility = next(
            (
                item
                for item in self.repository.eligibility(self.run_id)
                if item.strategy_id == signal.identity.strategy_id
            ),
            None,
        )
        if eligibility is None or not eligibility.valid_at(self.now()):
            return ()
        by_venue = {item.venue: item for item in quotes}
        legs = signal.venue_legs or tuple(by_venue)
        results: list[PaperOrderRecord] = []
        for state in tuple(self._portfolio_states.values()):
            if state.halted:
                continue
            first_fill: PaperFillRecord | None = None
            for index, venue in enumerate(legs):
                quote = by_venue.get(venue)
                leg_role = "receive_leg" if index == 0 else "pay_leg"
                order_id = f"{signal.signal_id}:{state.portfolio_id}:{leg_role}"
                submitted_at = self.now()
                if quote is None or leg_role == fail_leg_role:
                    order = PaperOrderRecord(
                        identity=signal.identity,
                        order_id=order_id,
                        signal_id=signal.signal_id,
                        portfolio_id=state.portfolio_id,
                        venue=venue,
                        instrument=signal.instrument,
                        side=signal.side,
                        requested_quantity=signal.quantity,
                        filled_quantity=Decimal("0"),
                        status="rejected",
                        submitted_at=submitted_at,
                        updated_at=submitted_at,
                        leg_role=leg_role,
                        rejection_reason="injected_one_leg_failure"
                        if leg_role == fail_leg_role
                        else "quote_unavailable",
                    )
                    self.repository.save_order(order)
                    results.append(order)
                    if first_fill is not None:
                        self._unwind_failed_leg(signal, state, first_fill, quote or quotes[0])
                    break
                price = quote.ask if signal.side is Side.BUY else quote.bid
                depth = quote.ask_size if signal.side is Side.BUY else quote.bid_size
                if price is None or depth is None:
                    continue
                notional = signal.quantity * price
                projected_notional = notional * Decimal(len(legs))
                estimated_trade_risk = (
                    signal.expected_fee + signal.expected_slippage + signal.expected_impact
                )
                if signal.quantity < minimum_quantity or notional < minimum_notional:
                    rejection = "instrument_minimum_not_met"
                    filled = Decimal("0")
                elif estimated_trade_risk > (
                    state.current_equity * self.operation_settings.maximum_single_trade_risk
                ):
                    rejection = "single_trade_risk_exceeded"
                    filled = Decimal("0")
                elif projected_notional > state.current_equity:
                    rejection = "maximum_leverage_exceeded"
                    filled = Decimal("0")
                elif (
                    notional > state.current_equity * self.operation_settings.maximum_venue_exposure
                    or notional
                    > state.current_equity * self.operation_settings.maximum_instrument_exposure
                ):
                    rejection = "capital_or_venue_exposure_infeasible"
                    filled = Decimal("0")
                else:
                    rejection = None
                    filled = min(signal.quantity, depth * maximum_participation)
                status = (
                    "rejected"
                    if rejection
                    else (
                        "filled"
                        if filled == signal.quantity
                        else ("open" if filled == 0 else "partially_filled")
                    )
                )
                order = PaperOrderRecord(
                    identity=signal.identity,
                    order_id=order_id,
                    signal_id=signal.signal_id,
                    portfolio_id=state.portfolio_id,
                    venue=venue,
                    instrument=signal.instrument,
                    side=signal.side,
                    requested_quantity=signal.quantity,
                    filled_quantity=filled,
                    status=status,
                    submitted_at=submitted_at,
                    updated_at=submitted_at + timedelta(milliseconds=latency_ms),
                    leg_role=leg_role,
                    rejection_reason=rejection,
                )
                self.repository.save_order(order)
                results.append(order)
                if filled <= 0:
                    continue
                touch = price
                impact = touch * impact_bps / Decimal("10000")
                fill_price = touch + impact if signal.side is Side.BUY else touch - impact
                fee = fill_price * filled * (maker_fee_rate if maker else taker_fee_rate)
                rebate = fill_price * filled * maker_rebate_rate if maker else Decimal("0")
                fill = PaperFillRecord(
                    identity=signal.identity,
                    fill_id=f"fill:{order_id}",
                    order_id=order_id,
                    portfolio_id=state.portfolio_id,
                    venue=venue,
                    instrument=signal.instrument,
                    side=signal.side,
                    quantity=filled,
                    price=fill_price,
                    fee_paid=fee,
                    rebate_received=rebate,
                    slippage_cost=abs(fill_price - touch) * filled,
                    impact_cost=abs(impact) * filled,
                    executed_at=order.updated_at,
                    latency_ms=latency_ms,
                    leg_role=leg_role,
                )
                self.repository.save_fill(fill)
                self._apply_fill(signal, state, fill)
                if index == 0:
                    first_fill = fill
        return tuple(results)

    def _apply_fill(
        self, signal: PaperSignal, state: PortfolioState, fill: PaperFillRecord
    ) -> None:
        existing = next(
            (
                item
                for item in self.repository.positions(self.run_id)
                if item.portfolio_id == state.portfolio_id
                and item.venue == fill.venue
                and item.instrument == fill.instrument
            ),
            None,
        )
        signed = fill.quantity if fill.side is Side.BUY else -fill.quantity
        quantity = (existing.quantity if existing else Decimal("0")) + signed
        position = PaperPositionRecord(
            identity=signal.identity,
            portfolio_id=state.portfolio_id,
            venue=fill.venue,
            instrument=fill.instrument,
            quantity=quantity,
            average_entry=fill.price if quantity else Decimal("0"),
            realized_pnl=existing.realized_pnl if existing else Decimal("0"),
            unrealized_pnl=Decimal("0"),
            funding_pnl=existing.funding_pnl if existing else Decimal("0"),
            updated_at=fill.executed_at,
        )
        self.repository.save_position(position)
        cash_delta = fill.rebate_received - fill.fee_paid
        new_cash = state.cash + cash_delta
        self.repository.add_cash_entry(
            PaperCashLedgerEntry(
                identity=signal.identity,
                entry_id=f"cash:{fill.fill_id}",
                portfolio_id=state.portfolio_id,
                amount=cash_delta,
                balance_after=new_cash,
                entry_type="paper_fill_cost",
                occurred_at=fill.executed_at,
                reference_id=fill.fill_id,
            )
        )
        updated = replace(
            state,
            cash=new_cash,
            current_equity=new_cash,
            peak_equity=max(state.peak_equity, new_cash),
        )
        self._portfolio_states[state.portfolio_id] = updated
        self._evaluate_portfolio_risk(signal.identity, updated)

    def _unwind_failed_leg(
        self,
        signal: PaperSignal,
        state: PortfolioState,
        first_fill: PaperFillRecord,
        quote: LiveSignalInput,
    ) -> None:
        unwind_side = Side.SELL if first_fill.side is Side.BUY else Side.BUY
        price = quote.bid if unwind_side is Side.SELL else quote.ask
        if price is None:
            cost = first_fill.price * first_fill.quantity
        else:
            cost = abs(first_fill.price - price) * first_fill.quantity
        event = PaperRiskEvent(
            identity=signal.identity,
            event_id=f"risk:{first_fill.fill_id}:one-leg",
            portfolio_id=state.portfolio_id,
            event_type="one_leg_failure_unwind",
            reason=f"failed_leg_cost={cost}",
            occurred_at=self.now(),
        )
        self.repository.add_risk_event(event)

    def settle_funding(
        self,
        *,
        portfolio_id: str,
        venue: str,
        instrument: str,
        rate: Decimal,
        mark_price: Decimal,
        at: datetime | None = None,
    ) -> Decimal:
        at = at or self.now()
        position = next(
            (
                item
                for item in self.repository.positions(self.run_id)
                if item.portfolio_id == portfolio_id
                and item.venue == venue
                and item.instrument == instrument
            ),
            None,
        )
        if position is None:
            return Decimal("0")
        amount = -(position.quantity * mark_price * rate)
        state = self._portfolio_states[portfolio_id]
        identity = position.identity
        self.repository.add_funding_entry(
            PaperFundingLedgerEntry(
                identity=identity,
                entry_id=f"funding:{portfolio_id}:{venue}:{instrument}:{at.isoformat()}",
                portfolio_id=portfolio_id,
                venue=venue,
                instrument=instrument,
                rate=rate,
                amount=amount,
                occurred_at=at,
            )
        )
        self.repository.add_cash_entry(
            PaperCashLedgerEntry(
                identity=identity,
                entry_id=f"cash:funding:{portfolio_id}:{venue}:{instrument}:{at.isoformat()}",
                portfolio_id=portfolio_id,
                amount=amount,
                balance_after=state.cash + amount,
                entry_type="funding",
                occurred_at=at,
            )
        )
        self._portfolio_states[portfolio_id] = replace(
            state, cash=state.cash + amount, current_equity=state.current_equity + amount
        )
        self.repository.save_position(
            replace(position, funding_pnl=position.funding_pnl + amount, updated_at=at)
        )
        return amount

    def _evaluate_portfolio_risk(
        self, identity: OperationalIdentity, state: PortfolioState
    ) -> None:
        daily_loss = (state.daily_start_equity - state.current_equity) / state.daily_start_equity
        drawdown = (state.peak_equity - state.current_equity) / state.peak_equity
        reason = None
        event_type = None
        if daily_loss >= self.operation_settings.maximum_daily_loss:
            event_type, reason = "daily_loss_halt", f"daily_loss={daily_loss}"
        elif drawdown >= self.operation_settings.maximum_drawdown:
            event_type, reason = "maximum_drawdown_halt", f"drawdown={drawdown}"
        if reason:
            self._portfolio_states[state.portfolio_id] = replace(
                state, halted=True, halt_reason=reason
            )
            self.repository.add_risk_event(
                PaperRiskEvent(
                    identity=identity,
                    event_id=f"risk:{self.run_id}:{state.portfolio_id}:{event_type}",
                    portfolio_id=state.portfolio_id,
                    event_type=event_type or "risk_halt",
                    reason=reason,
                    occurred_at=self.now(),
                )
            )

    def force_portfolio_equity(
        self, portfolio_id: str, equity: Decimal, identity: OperationalIdentity
    ) -> None:
        state = self._portfolio_states[portfolio_id]
        updated = replace(state, current_equity=equity)
        self._portfolio_states[portfolio_id] = updated
        self._evaluate_portfolio_risk(identity, updated)

    def portfolio_states(self) -> tuple[PortfolioState, ...]:
        return tuple(self._portfolio_states.values())

    def compute_attribution(
        self,
        report_date: date,
        *,
        expected_backtest_net_pnl: Decimal = Decimal("0"),
        expected_backtest_gross_pnl: Decimal = Decimal("0"),
        expected_fill_ratio: Decimal = Decimal("1"),
        expected_latency_ms: Decimal = Decimal("0"),
    ) -> tuple[PaperAttribution, ...]:
        signals = self.repository.signals(self.run_id)
        fills = self.repository.fills(self.run_id)
        output: list[PaperAttribution] = []
        for state in self._portfolio_states.values():
            portfolio_fills = [item for item in fills if item.portfolio_id == state.portfolio_id]
            fees = sum((item.fee_paid for item in portfolio_fills), Decimal("0"))
            rebates = sum((item.rebate_received for item in portfolio_fills), Decimal("0"))
            slippage = sum((item.slippage_cost for item in portfolio_fills), Decimal("0"))
            impact = sum((item.impact_cost for item in portfolio_fills), Decimal("0"))
            actual_net = state.current_equity - state.initial_capital
            actual_gross = actual_net + fees - rebates + slippage + impact
            fill_ratio = (
                Decimal(len(portfolio_fills)) / Decimal(len(signals)) if signals else Decimal("0")
            )
            latency = (
                sum((Decimal(item.latency_ms) for item in portfolio_fills), Decimal("0"))
                / Decimal(len(portfolio_fills))
                if portfolio_fills
                else Decimal("0")
            )
            identity = self._report_identity(report_date)
            item = PaperAttribution(
                identity=identity,
                portfolio_id=state.portfolio_id,
                attribution_date=report_date,
                expected_gross_pnl=expected_backtest_gross_pnl,
                actual_paper_gross_pnl=actual_gross,
                expected_net_pnl=expected_backtest_net_pnl,
                actual_paper_net_pnl=actual_net,
                fee_difference=fees,
                rebate_difference=rebates,
                funding_difference=sum(
                    (
                        entry.amount
                        for entry in self.repository.funding_entries(self.run_id)
                        if entry.portfolio_id == state.portfolio_id
                    ),
                    Decimal("0"),
                ),
                slippage_difference=slippage,
                impact_difference=impact,
                fill_rate_difference=fill_ratio - expected_fill_ratio,
                latency_difference=latency - expected_latency_ms,
                failed_leg_difference=Decimal(
                    len(
                        [
                            event
                            for event in self.repository.risk_events(self.run_id)
                            if event.portfolio_id == state.portfolio_id
                            and event.event_type == "one_leg_failure_unwind"
                        ]
                    )
                ),
                outage_difference=Decimal("0"),
                implementation_shortfall=expected_backtest_net_pnl - actual_net,
                edge_decay=expected_backtest_gross_pnl - actual_gross,
                signal_to_fill_latency_ms=latency,
                fill_ratio=fill_ratio,
                paper_backtest_pnl_ratio=(
                    actual_net / expected_backtest_net_pnl if expected_backtest_net_pnl else None
                ),
                paper_backtest_sharpe_ratio=None,
            )
            self.repository.save_attribution(item)
            output.append(item)
        return tuple(output)

    def promotion_verdict(self, *, minimum_signals: int = 30) -> PaperPromotionVerdict:
        signals = [
            item for item in self.repository.signals(self.run_id) if item.rejection_reason is None
        ]
        if len(signals) < minimum_signals:
            return PaperPromotionVerdict.NOT_READY
        if self.mode is OperationMode.OBSERVATION_ONLY or not self.repository.fills(self.run_id):
            return PaperPromotionVerdict.CONTINUE_OBSERVATION
        metrics = self.repository.daily_metrics(self.run_id)
        if not metrics or any(
            item.net_pnl <= 0 or item.maximum_drawdown > self.operation_settings.maximum_drawdown
            for item in metrics
        ):
            return PaperPromotionVerdict.CONTINUE_OBSERVATION
        return PaperPromotionVerdict.ELIGIBLE_FOR_MICRO_LIVE_REVIEW

    def generate_daily_report(self, report_date: date) -> DailyOperationReport:
        run = self._require_run()
        if run.last_snapshot_id is None:
            raise ValueError("daily report requires a finalized snapshot")
        fills = self.repository.fills(self.run_id)
        signals = self.repository.signals(self.run_id)
        metrics: list[PaperDailyMetric] = []
        for state in self._portfolio_states.values():
            portfolio_fills = [item for item in fills if item.portfolio_id == state.portfolio_id]
            fees = sum((item.fee_paid for item in portfolio_fills), Decimal("0"))
            rebates = sum((item.rebate_received for item in portfolio_fills), Decimal("0"))
            slippage = sum((item.slippage_cost for item in portfolio_fills), Decimal("0"))
            impact = sum((item.impact_cost for item in portfolio_fills), Decimal("0"))
            net = state.current_equity - state.initial_capital
            drawdown = max(
                Decimal("0"), (state.peak_equity - state.current_equity) / state.peak_equity
            )
            metric = PaperDailyMetric(
                identity=self._report_identity(report_date),
                portfolio_id=state.portfolio_id,
                metric_date=report_date,
                starting_equity=state.initial_capital,
                ending_equity=state.current_equity,
                gross_pnl=net + fees - rebates + slippage + impact,
                net_pnl=net,
                fees=fees,
                rebates=rebates,
                funding=sum(
                    (
                        item.amount
                        for item in self.repository.funding_entries(self.run_id)
                        if item.portfolio_id == state.portfolio_id
                    ),
                    Decimal("0"),
                ),
                slippage=slippage,
                impact=impact,
                failed_leg_cost=Decimal(
                    len(
                        [
                            event
                            for event in self.repository.risk_events(self.run_id)
                            if event.portfolio_id == state.portfolio_id
                            and event.event_type == "one_leg_failure_unwind"
                        ]
                    )
                ),
                maximum_drawdown=drawdown,
                capital_usage=Decimal("0"),
            )
            self.repository.save_daily_metric(metric)
            metrics.append(metric)
        attribution = self.compute_attribution(report_date)
        report = DailyOperationReport(
            run_id=self.run_id,
            report_date=report_date,
            snapshot_id=run.last_snapshot_id,
            research_run_ids=run.last_research_run_ids,
            eligibility=self.repository.eligibility(self.run_id),
            signal_count=len(signals),
            rejected_signal_count=sum(item.rejection_reason is not None for item in signals),
            paper_order_count=len(self.repository.orders(self.run_id)),
            paper_fill_count=len(fills),
            metrics=tuple(metrics),
            attribution=attribution,
            risk_events=self.repository.risk_events(self.run_id),
            promotion_verdict=self.promotion_verdict(),
            collector_healthy=run.collector_healthy,
        )
        self._write_report(report)
        return report

    def _write_report(self, report: DailyOperationReport) -> None:
        directory = self.artifact_root / report.report_date.isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        fills = self.repository.fills(self.run_id)
        pnl_by_strategy: dict[str, Decimal] = {}
        cost_by_venue: dict[str, Decimal] = {}
        cost_by_instrument: dict[str, Decimal] = {}
        for fill in fills:
            net_cost = fill.fee_paid - fill.rebate_received + fill.slippage_cost + fill.impact_cost
            pnl_by_strategy[fill.identity.strategy_id] = (
                pnl_by_strategy.get(fill.identity.strategy_id, Decimal("0")) - net_cost
            )
            cost_by_venue[fill.venue] = cost_by_venue.get(fill.venue, Decimal("0")) + net_cost
            cost_by_instrument[fill.instrument] = (
                cost_by_instrument.get(fill.instrument, Decimal("0")) + net_cost
            )
        payloads = {
            "collector-health.json": {"run_id": self.run_id, "healthy": report.collector_healthy},
            "snapshot-summary.json": {"run_id": self.run_id, "snapshot_id": report.snapshot_id},
            "research-summary.json": {
                "run_id": self.run_id,
                "research_run_ids": report.research_run_ids,
            },
            "strategy-eligibility.json": [asdict(item) for item in report.eligibility],
            "paper-performance.json": {
                "run_id": self.run_id,
                "signal_count": report.signal_count,
                "rejected_signal_count": report.rejected_signal_count,
                "paper_order_count": report.paper_order_count,
                "paper_fill_count": report.paper_fill_count,
                "metrics": [asdict(item) for item in report.metrics],
                "pnl_by_strategy": pnl_by_strategy,
                "cost_by_venue": cost_by_venue,
                "cost_by_instrument": cost_by_instrument,
                "capital_feasibility": {
                    item.portfolio_id: not item.portfolio_id.endswith("100")
                    or item.ending_equity >= Decimal("100")
                    for item in report.metrics
                },
                "promotion_verdict": report.promotion_verdict.value,
            },
        }
        for name, payload in payloads.items():
            (directory / name).write_text(
                json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n", encoding="utf-8"
            )
        self._write_csv(
            directory / "paper-attribution.csv", [asdict(item) for item in report.attribution]
        )
        self._write_csv(
            directory / "risk-events.csv", [asdict(item) for item in report.risk_events]
        )
        summary = (
            f"# Daily Paper Operation — {report.report_date.isoformat()}\n\n"
            f"- Run ID: `{report.run_id}`\n"
            f"- Snapshot ID: `{report.snapshot_id}`\n"
            f"- Collector health: {'PASS' if report.collector_healthy else 'FAIL'}\n"
            f"- Signals: {report.signal_count} ({report.rejected_signal_count} rejected)\n"
            f"- Paper orders/fills: {report.paper_order_count}/{report.paper_fill_count}\n"
            f"- Promotion verdict: **{report.promotion_verdict.value}**\n"
            "- Live execution: **OFF**\n"
        )
        (directory / "summary.md").write_text(summary, encoding="utf-8")

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
        if not rows:
            path.write_text("\n", encoding="utf-8")
            return
        flattened = [
            {
                key: json.dumps(value, default=str)
                if isinstance(value, (dict, list, tuple))
                else value
                for key, value in row.items()
            }
            for row in rows
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(flattened[0]))
            writer.writeheader()
            writer.writerows(flattened)

    def _identity(
        self,
        strategy_id: str,
        strategy_version: str,
        snapshot_id: str,
        research_run_id: str,
        at: datetime,
    ) -> OperationalIdentity:
        return OperationalIdentity(
            run_id=self.run_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            data_snapshot_id=snapshot_id,
            research_run_id=research_run_id,
            created_at=at,
            commit_sha=self.commit_sha,
            config_sha256=self.config_sha256,
        )

    def _report_identity(self, report_date: date) -> OperationalIdentity:
        run = self._require_run()
        return self._identity(
            "SYSTEM",
            "0",
            run.last_snapshot_id or "UNASSIGNED",
            run.last_research_run_ids[0] if run.last_research_run_ids else "UNASSIGNED",
            datetime.combine(report_date, datetime.min.time(), tzinfo=UTC),
        )

    def _require_run(self) -> OperationalRun:
        run = self.repository.get_run(self.run_id)
        if run is None:
            raise RuntimeError("operational run is missing")
        return run

    async def _collector_tick(self) -> None:
        run = self._require_run()
        if run.status is OperationalRunStatus.STOP_REQUESTED:
            self.stop_event.set()
            return
        if not run.collector_healthy:
            await self.notifier.send("Collector stopped", self.run_id, "error")
            return
        if self.market_event_action is not None:
            for event in await asyncio.to_thread(self.market_event_action):
                self.record_market_event(event)

    async def _snapshot_tick(self) -> None:
        if self.snapshot_action is not None:
            try:
                snapshot_id = await asyncio.to_thread(self.snapshot_action, self.now())
                self.bind_snapshot(snapshot_id)
            except Exception as exc:
                self.snapshot_failed(f"{type(exc).__name__}: {exc}")
                await self.notifier.send(
                    "Snapshot finalization failed", type(exc).__name__, "error"
                )
            return
        if self._require_run().last_snapshot_id is None:
            self.snapshot_failed("no_finalized_snapshot")

    async def _research_tick(self) -> None:
        run = self._require_run()
        if run.last_snapshot_id is None:
            return
        if self.research_action is not None:
            current = {
                (item.strategy_id, item.data_snapshot_id)
                for item in self.repository.eligibility(self.run_id)
            }
            if all(
                (strategy_id, run.last_snapshot_id) in current
                for strategy_id in self.operation_settings.strategies
            ):
                return
            try:
                outcomes = await asyncio.to_thread(self.research_action, run.last_snapshot_id)
                for outcome in outcomes:
                    self.refresh_eligibility(
                        strategy_id=outcome.strategy_id,
                        strategy_version=outcome.strategy_version,
                        research_run_id=outcome.research_run_id,
                        data_snapshot_id=run.last_snapshot_id,
                        research_verdict=outcome.research_verdict,
                        data_quality_passed=outcome.data_quality_passed,
                        capital_feasible=outcome.capital_feasible,
                        evidence_complete=outcome.evidence_complete,
                    )
            except Exception as exc:
                await self.notifier.send("Research Pipeline failed", type(exc).__name__, "error")
                outcomes = ()
            if outcomes:
                return
        existing = {item.strategy_id for item in self.repository.eligibility(self.run_id)}
        for strategy_id in self.operation_settings.strategies:
            if strategy_id in existing:
                continue
            self.refresh_eligibility(
                strategy_id=strategy_id,
                strategy_version="1",
                research_run_id="UNAVAILABLE",
                data_snapshot_id=run.last_snapshot_id,
                research_verdict="INSUFFICIENT_EVIDENCE",
                data_quality_passed=False,
                capital_feasible=False,
                evidence_complete=False,
            )

    async def _signal_tick(self) -> None:
        now = self.now()
        for instrument in self.operation_settings.instruments:
            books = tuple(
                event
                for (venue, saved_instrument, event_type), event in self._latest_events.items()
                if venue in self.operation_settings.venues
                and saved_instrument == instrument
                and event_type.startswith("orderbook")
            )
            if len({item.venue for item in books}) < 2:
                continue
            for strategy_id in ("funding_carry", "cross_venue_basis"):
                if strategy_id not in self.operation_settings.strategies:
                    continue
                self.generate_signal(
                    strategy_id=strategy_id,
                    strategy_version="1",
                    instrument=instrument,
                    side=Side.BUY,
                    quantity=Decimal("0.001"),
                    events=books,
                    expected_gross_edge=Decimal("0"),
                    required_capabilities=("orderbook_snapshot",),
                    decision_time=now,
                )

    async def _paper_execution_tick(self) -> None:
        if self.mode is OperationMode.OBSERVATION_ONLY:
            return
        submitted = {item.signal_id for item in self.repository.orders(self.run_id)}
        for signal in self.repository.signals(self.run_id):
            if signal.rejection_reason is not None or signal.signal_id in submitted:
                continue
            quotes = tuple(
                item
                for item in self._latest_events.values()
                if item.instrument == signal.instrument and item.venue in signal.venue_legs
            )
            self.execute_signal(signal, quotes=quotes)

    async def _risk_tick(self) -> None:
        identity = self._report_identity(self.now().date())
        for state in tuple(self._portfolio_states.values()):
            self._evaluate_portfolio_risk(identity, state)

    async def _reporting_tick(self) -> None:
        now = self.now()
        if (
            now.hour == self.operation_settings.daily_report_hour_utc
            and now.minute == self.operation_settings.daily_report_minute_utc
            and self._require_run().last_snapshot_id is not None
        ):
            self.generate_daily_report(now.date())
            await self.notifier.send("Daily report completed", now.date().isoformat(), "info")
