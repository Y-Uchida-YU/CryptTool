from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.domain.market_data.models import Side
from app.infrastructure.database.models import (
    OperationalRunRow,
    PaperAttributionRow,
    PaperCashLedgerRow,
    PaperDailyMetricRow,
    PaperFillRow,
    PaperFundingLedgerRow,
    PaperOrderRow,
    PaperPositionRow,
    PaperRiskEventRow,
    PaperSignalRow,
    StrategyEligibilityRow,
)
from app.services.operations.models import (
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
    PaperRiskEvent,
    PaperSignal,
    StrategyEligibilityRecord,
    StrategyEligibilityStatus,
)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class OperationalRepository(Protocol):
    durable: bool

    def save_run(self, run: OperationalRun) -> None: ...

    def get_run(self, run_id: str) -> OperationalRun | None: ...

    def list_runs(self) -> tuple[OperationalRun, ...]: ...

    def save_eligibility(
        self, run_id: str, record: StrategyEligibilityRecord, *, commit_sha: str, config_sha256: str
    ) -> None: ...

    def eligibility(self, run_id: str) -> tuple[StrategyEligibilityRecord, ...]: ...

    def add_signal(self, signal: PaperSignal) -> bool: ...

    def signals(self, run_id: str) -> tuple[PaperSignal, ...]: ...

    def save_order(self, order: PaperOrderRecord) -> None: ...

    def orders(self, run_id: str) -> tuple[PaperOrderRecord, ...]: ...

    def save_fill(self, fill: PaperFillRecord) -> None: ...

    def fills(self, run_id: str) -> tuple[PaperFillRecord, ...]: ...

    def save_position(self, position: PaperPositionRecord) -> None: ...

    def positions(self, run_id: str) -> tuple[PaperPositionRecord, ...]: ...

    def add_cash_entry(self, entry: PaperCashLedgerEntry) -> None: ...

    def cash_entries(self, run_id: str) -> tuple[PaperCashLedgerEntry, ...]: ...

    def add_funding_entry(self, entry: PaperFundingLedgerEntry) -> None: ...

    def funding_entries(self, run_id: str) -> tuple[PaperFundingLedgerEntry, ...]: ...

    def add_risk_event(self, event: PaperRiskEvent) -> None: ...

    def risk_events(self, run_id: str) -> tuple[PaperRiskEvent, ...]: ...

    def save_daily_metric(self, metric: PaperDailyMetric) -> None: ...

    def daily_metrics(self, run_id: str) -> tuple[PaperDailyMetric, ...]: ...

    def save_attribution(self, attribution: PaperAttribution) -> None: ...

    def attributions(self, run_id: str) -> tuple[PaperAttribution, ...]: ...


class InMemoryOperationalRepository:
    durable = False

    def __init__(self) -> None:
        self._runs: dict[str, OperationalRun] = {}
        self._eligibility: dict[tuple[str, str, str], StrategyEligibilityRecord] = {}
        self._signals: dict[str, PaperSignal] = {}
        self._orders: dict[str, PaperOrderRecord] = {}
        self._fills: dict[str, PaperFillRecord] = {}
        self._positions: dict[tuple[str, str, str, str], PaperPositionRecord] = {}
        self._cash: dict[str, PaperCashLedgerEntry] = {}
        self._funding: dict[str, PaperFundingLedgerEntry] = {}
        self._risk: dict[str, PaperRiskEvent] = {}
        self._metrics: dict[tuple[str, str, date], PaperDailyMetric] = {}
        self._attribution: dict[tuple[str, str, date], PaperAttribution] = {}

    def save_run(self, run: OperationalRun) -> None:
        self._runs[run.run_id] = run

    def get_run(self, run_id: str) -> OperationalRun | None:
        return self._runs.get(run_id)

    def list_runs(self) -> tuple[OperationalRun, ...]:
        return tuple(sorted(self._runs.values(), key=lambda item: item.started_at))

    def save_eligibility(
        self,
        run_id: str,
        record: StrategyEligibilityRecord,
        *,
        commit_sha: str,
        config_sha256: str,
    ) -> None:
        del commit_sha, config_sha256
        self._eligibility[(run_id, record.strategy_id, record.strategy_version)] = record

    def eligibility(self, run_id: str) -> tuple[StrategyEligibilityRecord, ...]:
        return tuple(
            item
            for (saved_run_id, _, _), item in self._eligibility.items()
            if saved_run_id == run_id
        )

    def add_signal(self, signal: PaperSignal) -> bool:
        if signal.signal_id in self._signals:
            return False
        self._signals[signal.signal_id] = signal
        return True

    def signals(self, run_id: str) -> tuple[PaperSignal, ...]:
        return tuple(item for item in self._signals.values() if item.identity.run_id == run_id)

    def save_order(self, order: PaperOrderRecord) -> None:
        self._orders[order.order_id] = order

    def orders(self, run_id: str) -> tuple[PaperOrderRecord, ...]:
        return tuple(item for item in self._orders.values() if item.identity.run_id == run_id)

    def save_fill(self, fill: PaperFillRecord) -> None:
        self._fills[fill.fill_id] = fill

    def fills(self, run_id: str) -> tuple[PaperFillRecord, ...]:
        return tuple(item for item in self._fills.values() if item.identity.run_id == run_id)

    def save_position(self, position: PaperPositionRecord) -> None:
        key = (position.identity.run_id, position.portfolio_id, position.venue, position.instrument)
        self._positions[key] = position

    def positions(self, run_id: str) -> tuple[PaperPositionRecord, ...]:
        return tuple(item for key, item in self._positions.items() if key[0] == run_id)

    def add_cash_entry(self, entry: PaperCashLedgerEntry) -> None:
        self._cash.setdefault(entry.entry_id, entry)

    def cash_entries(self, run_id: str) -> tuple[PaperCashLedgerEntry, ...]:
        return tuple(item for item in self._cash.values() if item.identity.run_id == run_id)

    def add_funding_entry(self, entry: PaperFundingLedgerEntry) -> None:
        self._funding.setdefault(entry.entry_id, entry)

    def funding_entries(self, run_id: str) -> tuple[PaperFundingLedgerEntry, ...]:
        return tuple(item for item in self._funding.values() if item.identity.run_id == run_id)

    def add_risk_event(self, event: PaperRiskEvent) -> None:
        self._risk.setdefault(event.event_id, event)

    def risk_events(self, run_id: str) -> tuple[PaperRiskEvent, ...]:
        return tuple(item for item in self._risk.values() if item.identity.run_id == run_id)

    def save_daily_metric(self, metric: PaperDailyMetric) -> None:
        self._metrics[(metric.identity.run_id, metric.portfolio_id, metric.metric_date)] = metric

    def daily_metrics(self, run_id: str) -> tuple[PaperDailyMetric, ...]:
        return tuple(item for key, item in self._metrics.items() if key[0] == run_id)

    def save_attribution(self, attribution: PaperAttribution) -> None:
        key = (attribution.identity.run_id, attribution.portfolio_id, attribution.attribution_date)
        self._attribution[key] = attribution

    def attributions(self, run_id: str) -> tuple[PaperAttribution, ...]:
        return tuple(item for key, item in self._attribution.items() if key[0] == run_id)


class PostgreSQLOperationalRepository:
    durable = True

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_run(self, run: OperationalRun) -> None:
        with Session(self.engine) as session, session.begin():
            row = session.get(OperationalRunRow, run.run_id)
            values = {
                "commit_sha": run.commit_sha,
                "config_sha256": run.config_sha256,
                "mode": run.mode.value,
                "status": run.status.value,
                "started_at": run.started_at,
                "updated_at": run.updated_at,
                "collector_healthy": run.collector_healthy,
                "last_snapshot_id": run.last_snapshot_id,
                "last_research_run_ids_json": _json(run.last_research_run_ids),
                "signals_paused_reason": run.signals_paused_reason,
                "failure_reason": run.failure_reason,
                "created_at": run.started_at,
            }
            if row is None:
                session.add(OperationalRunRow(run_id=run.run_id, **values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)

    def get_run(self, run_id: str) -> OperationalRun | None:
        with Session(self.engine) as session:
            row = session.get(OperationalRunRow, run_id)
            return self._run(row) if row is not None else None

    def list_runs(self) -> tuple[OperationalRun, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(OperationalRunRow).order_by(OperationalRunRow.started_at))
            return tuple(self._run(row) for row in rows)

    @staticmethod
    def _run(row: OperationalRunRow) -> OperationalRun:
        return OperationalRun(
            run_id=row.run_id,
            commit_sha=row.commit_sha,
            config_sha256=row.config_sha256,
            mode=OperationMode(row.mode),
            status=OperationalRunStatus(row.status),
            started_at=_utc(row.started_at),
            updated_at=_utc(row.updated_at),
            last_snapshot_id=row.last_snapshot_id,
            last_research_run_ids=tuple(json.loads(row.last_research_run_ids_json)),
            collector_healthy=row.collector_healthy,
            signals_paused_reason=row.signals_paused_reason,
            failure_reason=row.failure_reason,
        )

    def save_eligibility(
        self,
        run_id: str,
        record: StrategyEligibilityRecord,
        *,
        commit_sha: str,
        config_sha256: str,
    ) -> None:
        with Session(self.engine) as session, session.begin():
            row = session.scalar(
                select(StrategyEligibilityRow).where(
                    StrategyEligibilityRow.run_id == run_id,
                    StrategyEligibilityRow.strategy_id == record.strategy_id,
                    StrategyEligibilityRow.strategy_version == record.strategy_version,
                )
            )
            values = dict(
                data_snapshot_id=record.data_snapshot_id,
                research_run_id=record.research_run_id,
                status=record.status.value,
                evaluated_at=record.evaluated_at,
                expires_at=record.expires_at,
                reasons_json=_json(record.reasons),
                commit_sha=commit_sha,
                config_sha256=config_sha256,
                created_at=record.evaluated_at,
            )
            if row is None:
                session.add(
                    StrategyEligibilityRow(
                        run_id=run_id,
                        strategy_id=record.strategy_id,
                        strategy_version=record.strategy_version,
                        **values,
                    )
                )
            else:
                for key, value in values.items():
                    setattr(row, key, value)

    def eligibility(self, run_id: str) -> tuple[StrategyEligibilityRecord, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(StrategyEligibilityRow).where(StrategyEligibilityRow.run_id == run_id)
            )
            return tuple(
                StrategyEligibilityRecord(
                    strategy_id=row.strategy_id,
                    strategy_version=row.strategy_version,
                    status=StrategyEligibilityStatus(row.status),
                    research_run_id=row.research_run_id,
                    data_snapshot_id=row.data_snapshot_id,
                    evaluated_at=_utc(row.evaluated_at),
                    expires_at=_utc(row.expires_at),
                    reasons=tuple(json.loads(row.reasons_json)),
                )
                for row in rows
            )

    def add_signal(self, signal: PaperSignal) -> bool:
        with Session(self.engine) as session, session.begin():
            if session.get(PaperSignalRow, signal.signal_id) is not None:
                return False
            identity = signal.identity
            session.add(
                PaperSignalRow(
                    signal_id=signal.signal_id,
                    run_id=identity.run_id,
                    strategy_id=identity.strategy_id,
                    strategy_version=identity.strategy_version,
                    data_snapshot_id=identity.data_snapshot_id,
                    research_run_id=identity.research_run_id,
                    decision_time=signal.decision_time,
                    instrument=signal.instrument,
                    payload_json=_json(asdict(signal)),
                    rejection_reason=signal.rejection_reason,
                    signal_hash=signal.full_signal_hash,
                    commit_sha=identity.commit_sha,
                    config_sha256=identity.config_sha256,
                    created_at=identity.created_at,
                )
            )
            return True

    def signals(self, run_id: str) -> tuple[PaperSignal, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(PaperSignalRow).where(PaperSignalRow.run_id == run_id))
            return tuple(self._signal(row) for row in rows)

    @staticmethod
    def _identity(row: Any) -> OperationalIdentity:
        return OperationalIdentity(
            run_id=str(row.run_id),
            strategy_id=str(row.strategy_id),
            strategy_version=str(row.strategy_version),
            data_snapshot_id=str(row.data_snapshot_id),
            research_run_id=str(row.research_run_id),
            created_at=_utc(row.created_at),
            commit_sha=str(row.commit_sha),
            config_sha256=str(row.config_sha256),
        )

    @classmethod
    def _signal(cls, row: PaperSignalRow) -> PaperSignal:
        data = json.loads(row.payload_json)
        return PaperSignal(
            identity=cls._identity(row),
            signal_id=row.signal_id,
            decision_time=_utc(row.decision_time),
            venue_legs=tuple(data["venue_legs"]),
            instrument=row.instrument,
            side=Side(data["side"]),
            quantity=Decimal(data["quantity"]),
            expected_gross_edge=Decimal(data["expected_gross_edge"]),
            expected_net_edge=Decimal(data["expected_net_edge"]),
            expected_fee=Decimal(data["expected_fee"]),
            expected_rebate=Decimal(data["expected_rebate"]),
            expected_funding=Decimal(data["expected_funding"]),
            expected_slippage=Decimal(data["expected_slippage"]),
            expected_impact=Decimal(data["expected_impact"]),
            required_capabilities=tuple(data["required_capabilities"]),
            source_event_ids=tuple(data["source_event_ids"]),
            rejection_reason=row.rejection_reason,
        )

    def save_order(self, order: PaperOrderRecord) -> None:
        self._save_payload(
            PaperOrderRow,
            "order_id",
            order.order_id,
            order,
            status=order.status,
            updated_at=order.updated_at,
            signal_id=order.signal_id,
            portfolio_id=order.portfolio_id,
            venue=order.venue,
            instrument=order.instrument,
        )

    def orders(self, run_id: str) -> tuple[PaperOrderRecord, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(PaperOrderRow).where(PaperOrderRow.run_id == run_id))
            return tuple(self._order(row) for row in rows)

    @classmethod
    def _order(cls, row: PaperOrderRow) -> PaperOrderRecord:
        data = json.loads(row.payload_json)
        return PaperOrderRecord(
            identity=cls._identity(row),
            order_id=row.order_id,
            signal_id=row.signal_id,
            portfolio_id=row.portfolio_id,
            venue=row.venue,
            instrument=row.instrument,
            side=Side(data["side"]),
            requested_quantity=Decimal(data["requested_quantity"]),
            filled_quantity=Decimal(data["filled_quantity"]),
            status=row.status,
            submitted_at=_utc(datetime.fromisoformat(data["submitted_at"])),
            updated_at=_utc(row.updated_at),
            leg_role=data.get("leg_role"),
            rejection_reason=data.get("rejection_reason"),
        )

    def save_fill(self, fill: PaperFillRecord) -> None:
        self._save_payload(
            PaperFillRow,
            "fill_id",
            fill.fill_id,
            fill,
            order_id=fill.order_id,
            portfolio_id=fill.portfolio_id,
            venue=fill.venue,
            instrument=fill.instrument,
            executed_at=fill.executed_at,
        )

    def fills(self, run_id: str) -> tuple[PaperFillRecord, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(PaperFillRow).where(PaperFillRow.run_id == run_id))
            return tuple(self._fill(row) for row in rows)

    @classmethod
    def _fill(cls, row: PaperFillRow) -> PaperFillRecord:
        data = json.loads(row.payload_json)
        return PaperFillRecord(
            identity=cls._identity(row),
            fill_id=row.fill_id,
            order_id=row.order_id,
            portfolio_id=row.portfolio_id,
            venue=row.venue,
            instrument=row.instrument,
            side=Side(data["side"]),
            quantity=Decimal(data["quantity"]),
            price=Decimal(data["price"]),
            fee_paid=Decimal(data["fee_paid"]),
            rebate_received=Decimal(data["rebate_received"]),
            slippage_cost=Decimal(data["slippage_cost"]),
            impact_cost=Decimal(data["impact_cost"]),
            executed_at=_utc(row.executed_at),
            latency_ms=int(data["latency_ms"]),
            leg_role=data.get("leg_role"),
        )

    def save_position(self, position: PaperPositionRecord) -> None:
        with Session(self.engine) as session, session.begin():
            row = session.scalar(
                select(PaperPositionRow).where(
                    PaperPositionRow.run_id == position.identity.run_id,
                    PaperPositionRow.portfolio_id == position.portfolio_id,
                    PaperPositionRow.venue == position.venue,
                    PaperPositionRow.instrument == position.instrument,
                )
            )
            if row is None:
                session.add(
                    self._payload_row(
                        PaperPositionRow,
                        position,
                        portfolio_id=position.portfolio_id,
                        venue=position.venue,
                        instrument=position.instrument,
                        updated_at=position.updated_at,
                    )
                )
            else:
                row.payload_json = _json(asdict(position))
                row.updated_at = position.updated_at

    def positions(self, run_id: str) -> tuple[PaperPositionRecord, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PaperPositionRow).where(PaperPositionRow.run_id == run_id)
            )
            return tuple(self._position(row) for row in rows)

    @classmethod
    def _position(cls, row: PaperPositionRow) -> PaperPositionRecord:
        data = json.loads(row.payload_json)
        return PaperPositionRecord(
            identity=cls._identity(row),
            portfolio_id=row.portfolio_id,
            venue=row.venue,
            instrument=row.instrument,
            quantity=Decimal(data["quantity"]),
            average_entry=Decimal(data["average_entry"]),
            realized_pnl=Decimal(data["realized_pnl"]),
            unrealized_pnl=Decimal(data["unrealized_pnl"]),
            funding_pnl=Decimal(data["funding_pnl"]),
            updated_at=_utc(row.updated_at),
        )

    def add_cash_entry(self, entry: PaperCashLedgerEntry) -> None:
        with Session(self.engine) as session, session.begin():
            if session.get(PaperCashLedgerRow, entry.entry_id) is None:
                identity = entry.identity
                session.add(
                    PaperCashLedgerRow(
                        entry_id=entry.entry_id,
                        run_id=identity.run_id,
                        strategy_id=identity.strategy_id,
                        strategy_version=identity.strategy_version,
                        data_snapshot_id=identity.data_snapshot_id,
                        research_run_id=identity.research_run_id,
                        portfolio_id=entry.portfolio_id,
                        amount=entry.amount,
                        balance_after=entry.balance_after,
                        entry_type=entry.entry_type,
                        occurred_at=entry.occurred_at,
                        reference_id=entry.reference_id,
                        commit_sha=identity.commit_sha,
                        config_sha256=identity.config_sha256,
                        created_at=identity.created_at,
                    )
                )

    def cash_entries(self, run_id: str) -> tuple[PaperCashLedgerEntry, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PaperCashLedgerRow)
                .where(PaperCashLedgerRow.run_id == run_id)
                .order_by(PaperCashLedgerRow.occurred_at)
            )
            return tuple(
                PaperCashLedgerEntry(
                    identity=self._identity(row),
                    entry_id=row.entry_id,
                    portfolio_id=row.portfolio_id,
                    amount=Decimal(row.amount),
                    balance_after=Decimal(row.balance_after),
                    entry_type=row.entry_type,
                    occurred_at=_utc(row.occurred_at),
                    reference_id=row.reference_id,
                )
                for row in rows
            )

    def add_funding_entry(self, entry: PaperFundingLedgerEntry) -> None:
        with Session(self.engine) as session, session.begin():
            if session.get(PaperFundingLedgerRow, entry.entry_id) is None:
                identity = entry.identity
                session.add(
                    PaperFundingLedgerRow(
                        entry_id=entry.entry_id,
                        run_id=identity.run_id,
                        strategy_id=identity.strategy_id,
                        strategy_version=identity.strategy_version,
                        data_snapshot_id=identity.data_snapshot_id,
                        research_run_id=identity.research_run_id,
                        portfolio_id=entry.portfolio_id,
                        venue=entry.venue,
                        instrument=entry.instrument,
                        rate=entry.rate,
                        amount=entry.amount,
                        occurred_at=entry.occurred_at,
                        commit_sha=identity.commit_sha,
                        config_sha256=identity.config_sha256,
                        created_at=identity.created_at,
                    )
                )

    def funding_entries(self, run_id: str) -> tuple[PaperFundingLedgerEntry, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PaperFundingLedgerRow).where(PaperFundingLedgerRow.run_id == run_id)
            )
            return tuple(
                PaperFundingLedgerEntry(
                    identity=self._identity(row),
                    entry_id=row.entry_id,
                    portfolio_id=row.portfolio_id,
                    venue=row.venue,
                    instrument=row.instrument,
                    rate=Decimal(row.rate),
                    amount=Decimal(row.amount),
                    occurred_at=_utc(row.occurred_at),
                )
                for row in rows
            )

    def add_risk_event(self, event: PaperRiskEvent) -> None:
        with Session(self.engine) as session, session.begin():
            if session.get(PaperRiskEventRow, event.event_id) is None:
                identity = event.identity
                session.add(
                    PaperRiskEventRow(
                        event_id=event.event_id,
                        run_id=identity.run_id,
                        strategy_id=identity.strategy_id,
                        strategy_version=identity.strategy_version,
                        data_snapshot_id=identity.data_snapshot_id,
                        research_run_id=identity.research_run_id,
                        portfolio_id=event.portfolio_id,
                        event_type=event.event_type,
                        reason=event.reason,
                        blocks_new_signals=event.blocks_new_signals,
                        occurred_at=event.occurred_at,
                        commit_sha=identity.commit_sha,
                        config_sha256=identity.config_sha256,
                        created_at=identity.created_at,
                    )
                )

    def risk_events(self, run_id: str) -> tuple[PaperRiskEvent, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PaperRiskEventRow).where(PaperRiskEventRow.run_id == run_id)
            )
            return tuple(
                PaperRiskEvent(
                    identity=self._identity(row),
                    event_id=row.event_id,
                    portfolio_id=row.portfolio_id,
                    event_type=row.event_type,
                    reason=row.reason,
                    occurred_at=_utc(row.occurred_at),
                    blocks_new_signals=row.blocks_new_signals,
                )
                for row in rows
            )

    def save_daily_metric(self, metric: PaperDailyMetric) -> None:
        self._upsert_dated(
            PaperDailyMetricRow, "metric_date", metric.metric_date, metric.portfolio_id, metric
        )

    def daily_metrics(self, run_id: str) -> tuple[PaperDailyMetric, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PaperDailyMetricRow).where(PaperDailyMetricRow.run_id == run_id)
            )
            return tuple(self._metric(row) for row in rows)

    @classmethod
    def _metric(cls, row: PaperDailyMetricRow) -> PaperDailyMetric:
        data = json.loads(row.payload_json)
        fields = {
            key: Decimal(data[key])
            for key in (
                "starting_equity",
                "ending_equity",
                "gross_pnl",
                "net_pnl",
                "fees",
                "rebates",
                "funding",
                "slippage",
                "impact",
                "failed_leg_cost",
                "maximum_drawdown",
                "capital_usage",
            )
        }
        return PaperDailyMetric(
            identity=cls._identity(row),
            portfolio_id=row.portfolio_id,
            metric_date=date.fromisoformat(row.metric_date),
            **fields,
        )

    def save_attribution(self, attribution: PaperAttribution) -> None:
        self._upsert_dated(
            PaperAttributionRow,
            "attribution_date",
            attribution.attribution_date,
            attribution.portfolio_id,
            attribution,
        )

    def attributions(self, run_id: str) -> tuple[PaperAttribution, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PaperAttributionRow).where(PaperAttributionRow.run_id == run_id)
            )
            return tuple(self._attribution(row) for row in rows)

    @classmethod
    def _attribution(cls, row: PaperAttributionRow) -> PaperAttribution:
        data = json.loads(row.payload_json)
        fields = {
            key: Decimal(data[key])
            for key in (
                "expected_gross_pnl",
                "actual_paper_gross_pnl",
                "expected_net_pnl",
                "actual_paper_net_pnl",
                "fee_difference",
                "rebate_difference",
                "funding_difference",
                "slippage_difference",
                "impact_difference",
                "fill_rate_difference",
                "latency_difference",
                "failed_leg_difference",
                "outage_difference",
                "implementation_shortfall",
                "edge_decay",
                "signal_to_fill_latency_ms",
                "fill_ratio",
            )
        }
        return PaperAttribution(
            identity=cls._identity(row),
            portfolio_id=row.portfolio_id,
            attribution_date=date.fromisoformat(row.attribution_date),
            **fields,
            paper_backtest_pnl_ratio=(
                Decimal(data["paper_backtest_pnl_ratio"])
                if data.get("paper_backtest_pnl_ratio") is not None
                else None
            ),
            paper_backtest_sharpe_ratio=(
                Decimal(data["paper_backtest_sharpe_ratio"])
                if data.get("paper_backtest_sharpe_ratio") is not None
                else None
            ),
        )

    def _save_payload(
        self, row_type: Any, key_name: str, key_value: str, value: Any, **extra: object
    ) -> None:
        with Session(self.engine) as session, session.begin():
            if session.get(row_type, key_value) is None:
                session.add(self._payload_row(row_type, value, **{key_name: key_value}, **extra))

    @staticmethod
    def _payload_row(row_type: Any, value: Any, **extra: object) -> Any:
        identity = value.identity
        return row_type(
            run_id=identity.run_id,
            strategy_id=identity.strategy_id,
            strategy_version=identity.strategy_version,
            data_snapshot_id=identity.data_snapshot_id,
            research_run_id=identity.research_run_id,
            payload_json=_json(asdict(value)),
            commit_sha=identity.commit_sha,
            config_sha256=identity.config_sha256,
            created_at=identity.created_at,
            **extra,
        )

    def _upsert_dated(
        self, row_type: Any, date_column: str, day: date, portfolio_id: str, value: Any
    ) -> None:
        identity = value.identity
        with Session(self.engine) as session, session.begin():
            column = getattr(row_type, date_column)
            row = session.scalar(
                select(row_type).where(
                    row_type.run_id == identity.run_id,
                    row_type.portfolio_id == portfolio_id,
                    column == day.isoformat(),
                )
            )
            if row is None:
                session.add(
                    self._payload_row(
                        row_type, value, portfolio_id=portfolio_id, **{date_column: day.isoformat()}
                    )
                )
            else:
                row.payload_json = _json(asdict(value))
