from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from app.config.settings import Settings
from app.domain.market_data.models import Side
from app.infrastructure.database.models import Base
from app.services.operations.models import (
    LiveSignalInput,
    OperationalIdentity,
    OperationMode,
    PaperAttribution,
    PaperDailyMetric,
    PaperFundingLedgerEntry,
    PaperPromotionVerdict,
    PaperRiskEvent,
    StrategyEligibilityStatus,
)
from app.services.operations.repository import (
    InMemoryOperationalRepository,
    PostgreSQLOperationalRepository,
)
from app.services.operations.service import (
    ContinuousResearchPaperService,
    ScheduledResearchOutcome,
)

NOW = datetime(2026, 7, 15, 1, 0, tzinfo=UTC)


def settings(*, observation_only: bool = False) -> Settings:
    return Settings(
        database_url="sqlite+pysqlite:///:memory:",
        symbols=("BTC", "ETH", "SOL", "HYPE"),
        paper_trading=True,
        live_trading=False,
        paper={"enabled": True},
        live={"enabled": False, "allowed_symbols": ("BTC", "ETH", "SOL", "HYPE")},
        continuous_paper={
            "enabled": True,
            "observation_only": observation_only,
            "source_event_max_age_seconds": 30,
        },
    )


def service(
    repository: InMemoryOperationalRepository | PostgreSQLOperationalRepository | None = None,
    *,
    observation_only: bool = False,
    artifact_root: Path = Path("/tmp/operations-tests"),
    run_id: str = "operation-test",
) -> ContinuousResearchPaperService:
    result = ContinuousResearchPaperService(
        repository=repository or InMemoryOperationalRepository(),
        settings=settings(observation_only=observation_only),
        run_id=run_id,
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=(OperationMode.OBSERVATION_ONLY if observation_only else OperationMode.STRICT_PAPER),
        local_smoke=True,
        artifact_root=artifact_root,
        now=lambda: NOW,
    )
    result.set_collector_health(True)
    result.bind_snapshot("snapshot-1", ("research-1",))
    result.refresh_eligibility(
        strategy_id="cross_venue_basis",
        strategy_version="1",
        research_run_id="research-1",
        data_snapshot_id="snapshot-1",
        research_verdict="PASS",
        data_quality_passed=True,
        capital_feasible=True,
    )
    return result


def quote(
    venue: str,
    *,
    event_id: str | None = None,
    available_at: datetime = NOW,
    quality: float = 1.0,
    support: str = "live_verified",
    state: str = "synchronized",
    depth: Decimal = Decimal("10"),
) -> LiveSignalInput:
    return LiveSignalInput(
        event_id=event_id or f"{venue}-book",
        venue=venue,
        instrument="BTC",
        event_type="orderbook_snapshot",
        available_at=available_at,
        data_quality_score=quality,
        capability_support=support,
        reconciliation_state=state,
        bid=Decimal("99"),
        ask=Decimal("100"),
        bid_size=depth,
        ask_size=depth,
    )


def signal(
    operation: ContinuousResearchPaperService,
    events: tuple[LiveSignalInput, ...] | None = None,
    quantity: Decimal = Decimal("0.1"),
):
    return operation.generate_signal(
        strategy_id="cross_venue_basis",
        strategy_version="1",
        instrument="BTC",
        side=Side.BUY,
        quantity=quantity,
        events=events or (quote("hyperliquid"), quote("bitget")),
        expected_gross_edge=Decimal("1"),
        expected_fee=Decimal("0.01"),
        required_capabilities=("orderbook_snapshot",),
        decision_time=NOW,
    )


def test_restart_restores_paper_positions_and_cash_ledger() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    repository = PostgreSQLOperationalRepository(engine)
    first = service(repository)
    item = signal(first)
    first.execute_signal(item, quotes=(quote("hyperliquid"), quote("bitget")))
    before_positions = repository.positions(first.run_id)
    before_cash = repository.cash_entries(first.run_id)

    restarted = ContinuousResearchPaperService(
        repository=repository,
        settings=settings(),
        run_id=first.run_id,
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.STRICT_PAPER,
        local_smoke=True,
        now=lambda: NOW,
    )

    assert restarted.repository.positions(first.run_id) == before_positions
    assert restarted.repository.cash_entries(first.run_id) == before_cash
    restored_cash = {item.portfolio_id: item.cash for item in restarted.portfolio_states()}
    expected_cash = {
        portfolio_id: [entry for entry in before_cash if entry.portfolio_id == portfolio_id][
            -1
        ].balance_after
        for portfolio_id in restored_cash
    }
    assert restored_cash == expected_cash


@pytest.mark.asyncio
async def test_scheduled_snapshot_and_research_are_bound_to_operation() -> None:
    repository = InMemoryOperationalRepository()
    operation = ContinuousResearchPaperService(
        repository=repository,
        settings=settings(observation_only=True),
        run_id="scheduled-operation",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
        snapshot_action=lambda _: "snapshot-scheduled",
        research_action=lambda _: (
            ScheduledResearchOutcome(
                strategy_id="funding_carry",
                strategy_version="1",
                research_run_id="research-scheduled",
                research_verdict="PASS",
                data_quality_passed=True,
                capital_feasible=True,
                evidence_complete=True,
            ),
        ),
    )
    operation.set_collector_health(True)

    await operation._snapshot_tick()
    await operation._research_tick()

    run = repository.get_run(operation.run_id)
    assert run is not None and run.last_snapshot_id == "snapshot-scheduled"
    eligibility = repository.eligibility(operation.run_id)
    assert eligibility[0].status is StrategyEligibilityStatus.ELIGIBLE
    assert eligibility[0].research_run_id == "research-scheduled"


@pytest.mark.asyncio
async def test_scheduled_snapshot_failure_pauses_signals() -> None:
    def fail(_: datetime) -> str:
        raise RuntimeError("database unavailable")

    operation = ContinuousResearchPaperService(
        repository=InMemoryOperationalRepository(),
        settings=settings(observation_only=True),
        run_id="snapshot-failure",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
        snapshot_action=fail,
    )
    operation.set_collector_health(True)

    await operation._snapshot_tick()

    run = operation.repository.get_run(operation.run_id)
    assert run is not None
    assert run.signals_paused_reason == "snapshot_failed:RuntimeError: database unavailable"


@pytest.mark.asyncio
async def test_execution_worker_consumes_eligible_signal_once() -> None:
    operation = service()
    events = (quote("hyperliquid"), quote("bitget"))
    for event in events:
        operation.record_market_event(event)
    accepted = signal(operation, events)

    await operation._paper_execution_tick()
    await operation._paper_execution_tick()

    orders = operation.repository.orders(operation.run_id)
    assert orders
    assert {item.signal_id for item in orders} == {accepted.signal_id}


@pytest.mark.asyncio
async def test_collector_supervisor_imports_live_market_events() -> None:
    event = quote("hyperliquid")
    operation = ContinuousResearchPaperService(
        repository=InMemoryOperationalRepository(),
        settings=settings(observation_only=True),
        run_id="market-bridge",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
        market_event_action=lambda: (event,),
    )
    operation.set_collector_health(True)

    await operation._collector_tick()

    assert operation._latest_events[("hyperliquid", "BTC", "orderbook_snapshot")] == event


@pytest.mark.asyncio
async def test_observation_workers_remain_fail_closed_without_collector() -> None:
    operation = ContinuousResearchPaperService(
        repository=InMemoryOperationalRepository(),
        settings=settings(observation_only=True),
        run_id="worker-fail-closed",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )

    await operation._collector_tick()
    await operation._paper_execution_tick()
    await operation._risk_tick()

    assert operation.repository.orders(operation.run_id) == ()


def test_expired_eligibility_rejects_signal() -> None:
    operation = service()
    record = operation.repository.eligibility(operation.run_id)[0]
    operation.repository.save_eligibility(
        operation.run_id,
        replace(record, expires_at=NOW - timedelta(seconds=1)),
        commit_sha="a" * 40,
        config_sha256="b" * 64,
    )
    assert "expired" in (signal(operation).rejection_reason or "")


def test_failed_research_rejects_paper_order() -> None:
    operation = service()
    record = operation.refresh_eligibility(
        strategy_id="cross_venue_basis",
        strategy_version="1",
        research_run_id="research-failed",
        data_snapshot_id="snapshot-1",
        research_verdict="FAIL",
        data_quality_passed=True,
        capital_feasible=True,
    )
    item = signal(operation)
    assert record.status is StrategyEligibilityStatus.RESEARCH_FAILED
    assert item.rejection_reason is not None
    assert operation.execute_signal(item, quotes=(quote("hyperliquid"), quote("bitget"))) == ()


@pytest.mark.parametrize(
    ("events", "reason"),
    [
        ((quote("hyperliquid", state="degraded"), quote("bitget")), "unsynchronized"),
        (
            (
                quote("hyperliquid", available_at=NOW - timedelta(seconds=31)),
                quote("bitget"),
            ),
            "stale",
        ),
    ],
)
def test_market_evidence_gate_rejects_bad_book(
    events: tuple[LiveSignalInput, ...], reason: str
) -> None:
    assert reason in (signal(service(), events).rejection_reason or "")


def test_partial_fill_and_unfilled_order() -> None:
    operation = service()
    item = signal(operation, quantity=Decimal("0.8"))
    orders = operation.execute_signal(
        item,
        quotes=(quote("hyperliquid", depth=Decimal("5")), quote("bitget", depth=Decimal("0"))),
        minimum_notional=Decimal("0"),
    )
    assert any(order.status == "partially_filled" for order in orders)
    assert any(order.status == "open" for order in orders)


def test_maker_rebate_is_recorded() -> None:
    operation = service()
    item = signal(operation)
    operation.execute_signal(
        item,
        quotes=(quote("hyperliquid"), quote("bitget")),
        maker=True,
        maker_rebate_rate=Decimal("0.0001"),
    )
    assert all(fill.rebate_received > 0 for fill in operation.repository.fills(operation.run_id))


def test_funding_settlement_updates_ledger_and_cash() -> None:
    operation = service()
    item = signal(operation)
    operation.execute_signal(item, quotes=(quote("hyperliquid"), quote("bitget")))
    amount = operation.settle_funding(
        portfolio_id="usd-1000",
        venue="hyperliquid",
        instrument="BTC",
        rate=Decimal("0.001"),
        mark_price=Decimal("100"),
    )
    assert amount < 0
    assert operation.repository.funding_entries(operation.run_id)[0].amount == amount


def test_one_leg_failure_records_unwind_risk() -> None:
    operation = service()
    item = signal(operation)
    operation.execute_signal(
        item,
        quotes=(quote("hyperliquid"), quote("bitget")),
        fail_leg_role="pay_leg",
    )
    assert any(
        event.event_type == "one_leg_failure_unwind"
        for event in operation.repository.risk_events(operation.run_id)
    )


def test_paper_daily_loss_halts_portfolio() -> None:
    operation = service()
    item = signal(operation)
    operation.force_portfolio_equity("usd-100", Decimal("97"), item.identity)
    state = next(value for value in operation.portfolio_states() if value.portfolio_id == "usd-100")
    assert state.halted
    assert any(
        event.event_type == "daily_loss_halt"
        for event in operation.repository.risk_events(operation.run_id)
    )


def test_100_usd_capital_infeasible_while_larger_portfolios_run() -> None:
    operation = service()
    orders = operation.execute_signal(
        signal(operation, quantity=Decimal("1")),
        quotes=(quote("hyperliquid", depth=Decimal("20")), quote("bitget", depth=Decimal("20"))),
    )
    usd100 = [item for item in orders if item.portfolio_id == "usd-100"]
    usd300 = [item for item in orders if item.portfolio_id == "usd-300"]
    assert all(item.status == "rejected" for item in usd100)
    assert any(item.filled_quantity > 0 for item in usd300)


def test_backtest_paper_attribution_calculates_shortfall() -> None:
    operation = service()
    operation.execute_signal(signal(operation), quotes=(quote("hyperliquid"), quote("bitget")))
    result = operation.compute_attribution(
        NOW.date(),
        expected_backtest_net_pnl=Decimal("1"),
        expected_backtest_gross_pnl=Decimal("1.2"),
    )
    assert len(result) == 3
    assert all(
        item.implementation_shortfall == Decimal("1") - item.actual_paper_net_pnl for item in result
    )


def test_snapshot_and_collector_failure_pause_signals() -> None:
    operation = service()
    operation.snapshot_failed("database")
    assert "snapshot_failed" in (signal(operation).rejection_reason or "")
    operation.set_collector_health(False, "collector_stopped")
    assert (
        signal(
            operation, events=(quote("hyperliquid", event_id="h2"), quote("bitget", event_id="b2"))
        ).rejection_reason
        == "collector_stopped"
    )


def test_duplicate_signal_is_idempotent() -> None:
    operation = service()
    first = signal(operation)
    second = signal(operation)
    assert first.signal_id == second.signal_id
    assert len(operation.repository.signals(operation.run_id)) == 1


def test_observation_only_records_candidate_without_orders() -> None:
    operation = service(observation_only=True)
    item = signal(operation)
    assert item.rejection_reason is None
    assert operation.execute_signal(item, quotes=(quote("hyperliquid"), quote("bitget"))) == ()
    assert len(operation.repository.signals(operation.run_id)) == 1


def test_daily_report_is_reproducible(tmp_path: Path) -> None:
    operation = service(artifact_root=tmp_path)
    operation.generate_daily_report(date(2026, 7, 15))
    first = {path.name: path.read_bytes() for path in (tmp_path / "2026-07-15").iterdir()}
    operation.generate_daily_report(date(2026, 7, 15))
    second = {path.name: path.read_bytes() for path in (tmp_path / "2026-07-15").iterdir()}
    assert first == second
    assert set(first) == {
        "collector-health.json",
        "snapshot-summary.json",
        "research-summary.json",
        "strategy-eligibility.json",
        "paper-performance.json",
        "paper-attribution.csv",
        "risk-events.csv",
        "summary.md",
    }


def test_promotion_requires_evidence() -> None:
    assert service(observation_only=True).promotion_verdict() is PaperPromotionVerdict.NOT_READY


def test_live_mode_cannot_start() -> None:
    unsafe = settings().model_copy(
        update={
            "live_trading": True,
            "live": settings().live.model_copy(update={"enabled": True}),
        }
    )
    with pytest.raises(ValueError, match="live execution"):
        ContinuousResearchPaperService(
            repository=InMemoryOperationalRepository(),
            settings=unsafe,
            run_id="unsafe",
            commit_sha="a" * 40,
            config_sha256="b" * 64,
            mode=OperationMode.STRICT_PAPER,
            local_smoke=True,
            now=lambda: NOW,
        )


def test_sql_repository_round_trips_all_paper_records(tmp_path: Path) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    repository = PostgreSQLOperationalRepository(engine)
    operation = service(repository, artifact_root=tmp_path)
    item = signal(operation)
    operation.execute_signal(
        item,
        quotes=(quote("hyperliquid"), quote("bitget")),
        maker=True,
        maker_rebate_rate=Decimal("0.0001"),
    )
    identity = item.identity
    repository.add_funding_entry(
        PaperFundingLedgerEntry(
            identity=identity,
            entry_id="funding-extra",
            portfolio_id="usd-1000",
            venue="hyperliquid",
            instrument="BTC",
            rate=Decimal("0.001"),
            amount=Decimal("-0.01"),
            occurred_at=NOW,
        )
    )
    repository.add_risk_event(
        PaperRiskEvent(
            identity=identity,
            event_id="risk-extra",
            portfolio_id="usd-1000",
            event_type="test",
            reason="round-trip",
            occurred_at=NOW,
        )
    )
    metric = PaperDailyMetric(
        identity=identity,
        portfolio_id="usd-1000",
        metric_date=NOW.date(),
        starting_equity=Decimal("1000"),
        ending_equity=Decimal("1001"),
        gross_pnl=Decimal("1.1"),
        net_pnl=Decimal("1"),
        fees=Decimal("0.1"),
        rebates=Decimal("0"),
        funding=Decimal("0"),
        slippage=Decimal("0"),
        impact=Decimal("0"),
        failed_leg_cost=Decimal("0"),
        maximum_drawdown=Decimal("0.01"),
        capital_usage=Decimal("0.2"),
    )
    repository.save_daily_metric(metric)
    attribution = PaperAttribution(
        identity=identity,
        portfolio_id="usd-1000",
        attribution_date=NOW.date(),
        expected_gross_pnl=Decimal("1.2"),
        actual_paper_gross_pnl=Decimal("1.1"),
        expected_net_pnl=Decimal("1"),
        actual_paper_net_pnl=Decimal("0.9"),
        fee_difference=Decimal("0.1"),
        rebate_difference=Decimal("0"),
        funding_difference=Decimal("0"),
        slippage_difference=Decimal("0"),
        impact_difference=Decimal("0"),
        fill_rate_difference=Decimal("0"),
        latency_difference=Decimal("10"),
        failed_leg_difference=Decimal("0"),
        outage_difference=Decimal("0"),
        implementation_shortfall=Decimal("0.1"),
        edge_decay=Decimal("0.1"),
        signal_to_fill_latency_ms=Decimal("10"),
        fill_ratio=Decimal("1"),
        paper_backtest_pnl_ratio=Decimal("0.9"),
        paper_backtest_sharpe_ratio=None,
    )
    repository.save_attribution(attribution)

    assert repository.list_runs()[0].run_id == operation.run_id
    assert repository.signals(operation.run_id)[0].signal_id == item.signal_id
    assert repository.orders(operation.run_id)
    assert repository.fills(operation.run_id)
    assert repository.funding_entries(operation.run_id)[-1].entry_id == "funding-extra"
    assert repository.risk_events(operation.run_id)[-1].event_id == "risk-extra"
    assert repository.daily_metrics(operation.run_id)[0] == metric
    assert repository.attributions(operation.run_id)[0] == attribution


def test_service_lifecycle_gracefully_stops() -> None:
    operation = service()
    operation.request_stop()
    asyncio.run(operation.run())
    assert operation.repository.get_run(operation.run_id).status.value == "stopped"  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "event",
    [
        quote("hyperliquid", support="documented"),
        quote("hyperliquid", quality=0.1),
        replace(quote("hyperliquid"), instrument="ETH"),
        quote("hyperliquid", available_at=NOW + timedelta(seconds=1)),
    ],
)
def test_additional_signal_evidence_fail_closed(event: LiveSignalInput) -> None:
    operation = service()
    result = signal(operation, (event, quote("bitget")))
    assert result.rejection_reason is not None


def test_missing_snapshot_and_eligibility_fail_closed() -> None:
    repository = InMemoryOperationalRepository()
    operation = ContinuousResearchPaperService(
        repository=repository,
        settings=settings(),
        run_id="empty",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.STRICT_PAPER,
        local_smoke=True,
        now=lambda: NOW,
    )
    operation.set_collector_health(True)
    result = operation.generate_signal(
        strategy_id="cross_venue_basis",
        strategy_version="1",
        instrument="BTC",
        side=Side.BUY,
        quantity=Decimal("0.1"),
        events=(quote("hyperliquid"), quote("bitget")),
        expected_gross_edge=Decimal("1"),
    )
    assert result.rejection_reason == "no_current_snapshot"


def test_startup_requires_durable_postgresql_outside_smoke() -> None:
    with pytest.raises(ValueError, match="durable PostgreSQL"):
        ContinuousResearchPaperService(
            repository=InMemoryOperationalRepository(),
            settings=settings(),
            run_id="not-durable",
            commit_sha="a" * 40,
            config_sha256="b" * 64,
            mode=OperationMode.STRICT_PAPER,
            local_smoke=False,
            now=lambda: NOW,
        )


def test_operational_identity_rejects_empty_and_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        OperationalIdentity("", "s", "1", "snap", "research", NOW, "a", "b")
    with pytest.raises(ValueError, match="timezone-aware"):
        OperationalIdentity(
            "run",
            "s",
            "1",
            "snap",
            "research",
            datetime(2026, 1, 1),
            "a",
            "b",
        )
