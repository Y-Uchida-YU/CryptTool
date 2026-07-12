import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.adapters.exchanges.base import ExecutionAdapter
from app.adapters.exchanges.disabled import DisabledExecutionAdapter, LiveTradingDisabledError
from app.config.settings import Settings
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionAuditEvent,
    ExecutionOrderAck,
    LiveOpenOrder,
    LiveOrderRequest,
    LiveOrderState,
    LivePosition,
)
from app.domain.execution.models import OrderType
from app.domain.market_data.models import Side
from app.domain.risk.models import PositionSizingResult, RiskDecision
from app.services.live_trading.gateway import ExecutionAdapterError, LiveExecutionGateway
from app.services.live_trading.preflight import (
    RUNTIME_CONFIRMATION,
    LivePreflightContext,
    LivePreflightReport,
    PreflightCheck,
    evaluate_live_preflight,
)

NOW = datetime(2025, 1, 1, tzinfo=UTC)


class FakeExecutionAdapter(ExecutionAdapter):
    def __init__(self) -> None:
        self.place_calls = 0
        self.cancel_all_calls = 0
        self.healthy = True
        self.open_orders: list[LiveOpenOrder] = []
        self.positions: list[LivePosition] = []
        self.open_orders_symbol: str | None = "not-called"
        self.fail_place = False
        self.mismatch_request = False
        self.fail_cancel = False
        self.fail_cancel_all = False
        self.fail_close = False

    @property
    def adapter_name(self) -> str:
        return "sandbox"

    @property
    def is_concrete(self) -> bool:
        return True

    async def place_order(self, request: LiveOrderRequest) -> ExecutionOrderAck:
        self.place_calls += 1
        if self.fail_place:
            raise ConnectionError("private adapter detail")
        return ExecutionOrderAck(
            request_id="mismatch" if self.mismatch_request else request.request_id,
            external_order_id=f"external-{self.place_calls}",
            state=LiveOrderState.ACCEPTED,
            accepted_at=NOW,
            reason="accepted by fake adapter",
            adapter_called=True,
        )

    async def cancel_order(self, order_id: str) -> CancelAck:
        if self.fail_cancel:
            raise ConnectionError("cancel failure")
        return CancelAck(
            external_order_id=order_id,
            canceled=True,
            timestamp=NOW,
            reason="canceled",
        )

    async def cancel_all_orders(self, symbol: str | None = None) -> Sequence[CancelAck]:
        del symbol
        self.cancel_all_calls += 1
        if self.fail_cancel_all:
            raise ConnectionError("cancel-all failure")
        return ()

    async def fetch_open_orders(self, symbol: str | None = None) -> Sequence[LiveOpenOrder]:
        self.open_orders_symbol = symbol
        return tuple(self.open_orders)

    async def fetch_positions(self) -> Sequence[LivePosition]:
        return tuple(self.positions)

    async def close_position(self, symbol: str) -> ExecutionOrderAck:
        if self.fail_close:
            raise ConnectionError("close failure")
        return ExecutionOrderAck(
            request_id=f"close-{symbol}",
            external_order_id="close-external",
            state=LiveOrderState.ACCEPTED,
            accepted_at=NOW,
            reason="reduce-only close accepted",
            adapter_called=True,
        )

    async def health_check(self) -> bool:
        return self.healthy


def live_settings(**live_overrides: object) -> Settings:
    live = {
        "enabled": True,
        "adapter_name": "sandbox",
        "allowed_symbols": ("BTC",),
        "maximum_order_notional": 100,
        "maximum_open_orders": 3,
        "maximum_orders_per_minute": 5,
        "preflight_ttl_seconds": 300,
        **live_overrides,
    }
    return Settings(
        _env_file=None,
        environment="production",
        symbols=("BTC", "ETH", "SOL"),
        paper_trading=False,
        live_trading=True,
        dry_run=False,
        live_confirmation="I_ACCEPT_LIVE_TRADING_RISK",
        exchange_api_key="key-from-environment",
        exchange_api_secret="secret-from-environment",
        paper={"enabled": False},
        live=live,
        exchanges=(
            {
                "name": "sandbox",
                "data_enabled": True,
                "execution_enabled": True,
            },
        ),
    )


def approved_context(**overrides: object) -> LivePreflightContext:
    values = {
        "timestamp": NOW,
        "operator_confirmation": RUNTIME_CONFIRMATION,
        "adapter_name": "sandbox",
        "adapter_is_concrete": True,
        "adapter_healthy": True,
        "data_quality_score": 1,
        "websocket_connected": True,
        "clock_skew_seconds": 0.1,
        "kill_switch_active": False,
        "paper_validation_passed": True,
        "out_of_sample_validation_passed": True,
        **overrides,
    }
    return LivePreflightContext.model_validate(values)


def order(
    request_id: str = "request-1",
    idempotency_key: str = "idempotency-key-0001",
    **overrides: object,
) -> LiveOrderRequest:
    values = {
        "request_id": request_id,
        "idempotency_key": idempotency_key,
        "signal_id": "signal-0001",
        "risk_decision_id": "risk-decision-0001",
        "model_version": "strategy-1.0",
        "config_version": "config-1.0",
        "exchange": "sandbox",
        "symbol": "BTC",
        "side": Side.BUY,
        "quantity": Decimal("0.1"),
        "reference_price": Decimal("100"),
        "created_at": NOW,
        "expires_at": NOW + timedelta(minutes=1),
        **overrides,
    }
    return LiveOrderRequest.model_validate(values)


def risk(
    allowed: bool = True,
    timestamp: datetime = NOW,
    decision_id: str = "risk-decision-0001",
    quantity: Decimal = Decimal("0.1"),
) -> RiskDecision:
    return RiskDecision(
        decision_id=decision_id,
        allowed=allowed,
        reasons=("risk accepted" if allowed else "risk rejected",),
        sizing=(
            PositionSizingResult(
                accepted=True,
                quantity=quantity,
                notional=quantity * Decimal("100"),
                risk_amount=Decimal("0.25"),
                binding_constraint="test",
                reason="test approved size",
            )
            if allowed
            else None
        ),
        evaluated_at=timestamp,
    )


def test_default_preflight_refuses_every_live_path() -> None:
    report = evaluate_live_preflight(Settings(_env_file=None), LivePreflightContext(timestamp=NOW))
    assert not report.approved
    assert report.warning.startswith("LIVE EXECUTION REFUSED")
    assert any(not check.passed for check in report.checks)


def test_preflight_requires_every_runtime_and_configuration_gate() -> None:
    settings = live_settings()
    report = evaluate_live_preflight(settings, approved_context())
    assert report.approved and all(check.passed for check in report.checks)
    refused = evaluate_live_preflight(
        settings, approved_context(operator_confirmation="wrong", data_quality_score=0.5)
    )
    assert not refused.approved
    assert {check.name for check in refused.checks if not check.passed} == {
        "runtime_confirmation",
        "data_quality",
    }


def test_gateway_rejects_inconsistent_or_incomplete_approved_preflight() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    inconsistent = LivePreflightReport(
        timestamp=NOW,
        approved=True,
        checks=(PreflightCheck(name="only", passed=False, reason="forged"),),
        warning="forged",
    )
    with pytest.raises(ValueError, match="inconsistent"):
        LiveExecutionGateway(settings, adapter, inconsistent)
    incomplete = inconsistent.model_copy(
        update={
            "checks": (PreflightCheck(name="only", passed=True, reason="forged"),),
        }
    )
    with pytest.raises(ValueError, match="missing required"):
        LiveExecutionGateway(settings, adapter, incomplete)


def test_live_settings_reject_unsafe_combinations() -> None:
    with pytest.raises(ValidationError, match="production"):
        live_settings().model_copy(update={"environment": "development"}).model_validate(
            {**live_settings().model_dump(), "environment": "development"}
        )
    with pytest.raises(ValidationError, match="concrete execution adapter"):
        live_settings(adapter_name="disabled")
    with pytest.raises(ValidationError, match="withdrawal"):
        live_settings(withdrawal_permission_confirmed_disabled=False)
    invalid = live_settings().model_dump()
    invalid["exchange_api_key"] = None
    with pytest.raises(ValidationError, match="credentials"):
        Settings.model_validate(invalid)
    invalid = live_settings().model_dump()
    invalid["exchanges"] = ()
    with pytest.raises(ValidationError, match="execution-enabled"):
        Settings.model_validate(invalid)
    with pytest.raises(ValidationError, match="subset"):
        Settings(_env_file=None, live={"allowed_symbols": ("DOGE",)})
    duplicate = live_settings().model_dump()
    duplicate["exchanges"] = (
        {"name": "sandbox", "execution_enabled": True},
        {"name": "sandbox", "execution_enabled": True},
    )
    with pytest.raises(ValidationError, match="exactly one"):
        Settings.model_validate(duplicate)


@pytest.mark.asyncio
async def test_disabled_adapter_is_permanently_non_executable() -> None:
    adapter = DisabledExecutionAdapter()
    assert adapter.adapter_name == "disabled" and not adapter.is_concrete
    assert not await adapter.health_check()
    with pytest.raises(LiveTradingDisabledError):
        await adapter.place_order(order())
    with pytest.raises(LiveTradingDisabledError):
        await adapter.cancel_order("order")
    with pytest.raises(LiveTradingDisabledError):
        await adapter.cancel_all_orders()
    with pytest.raises(LiveTradingDisabledError):
        await adapter.fetch_open_orders()
    with pytest.raises(LiveTradingDisabledError):
        await adapter.fetch_positions()
    with pytest.raises(LiveTradingDisabledError):
        await adapter.close_position("BTC")


@pytest.mark.asyncio
async def test_gateway_calls_adapter_once_and_replays_idempotently() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    request = order()
    receipt = await gateway.place_order(request, risk(), NOW + timedelta(seconds=1))
    replay = await gateway.place_order(request, risk(), NOW + timedelta(seconds=2))
    assert receipt.state == LiveOrderState.ACCEPTED
    assert replay == receipt and adapter.place_calls == 1
    assert adapter.open_orders_symbol is None
    assert any(event.event_type == "idempotent_replay" for event in gateway.audit_events)


@pytest.mark.asyncio
async def test_concurrent_idempotent_orders_call_adapter_once() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    request = order()
    first, second = await asyncio.gather(
        gateway.place_order(request, risk(), NOW),
        gateway.place_order(request, risk(), NOW),
    )
    assert first == second
    assert adapter.place_calls == 1


@pytest.mark.asyncio
async def test_gateway_rejects_without_calling_adapter() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    cases = (
        order("expired1", "idempotency-expired", expires_at=NOW + timedelta(seconds=1)),
        order("symbol-1", "idempotency-symbol", symbol="ETH"),
        order(
            "notional",
            "idempotency-notional",
            quantity=Decimal("2"),
            reference_price=Decimal("100"),
        ),
    )
    for request in cases:
        receipt = await gateway.place_order(request, risk(), NOW + timedelta(seconds=2))
        assert receipt.state == LiveOrderState.REJECTED and not receipt.adapter_called
    risk_rejection = await gateway.place_order(
        order("risk-rej", "idempotency-risk-rejection"), risk(False), NOW
    )
    assert risk_rejection.state == LiveOrderState.REJECTED
    stale = await gateway.place_order(
        order("risk-old", "idempotency-risk-stale"),
        risk(True, NOW - timedelta(minutes=1)),
        NOW,
    )
    assert "stale" in stale.reason
    mismatched = await gateway.place_order(
        order("risk-id-1", "idempotency-risk-id"),
        risk(decision_id="different-risk-decision"),
        NOW,
    )
    assert "identity" in mismatched.reason
    oversized = await gateway.place_order(
        order("risk-size", "idempotency-risk-size", quantity=Decimal("0.2")),
        risk(quantity=Decimal("0.1")),
        NOW,
    )
    assert "risk-approved" in oversized.reason
    assert adapter.place_calls == 0


@pytest.mark.asyncio
async def test_gateway_rejects_stale_preflight_and_request() -> None:
    settings = live_settings(preflight_ttl_seconds=10)
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    stale_preflight = await gateway.place_order(
        order(expires_at=NOW + timedelta(minutes=1)),
        risk(timestamp=NOW + timedelta(seconds=11)),
        NOW + timedelta(seconds=11),
    )
    assert "preflight is stale" in stale_preflight.reason

    current_context = approved_context(timestamp=NOW + timedelta(seconds=40))
    current = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, current_context)
    )
    stale_request = await current.place_order(
        order(
            "stale-request",
            "idempotency-stale-request",
            expires_at=NOW + timedelta(minutes=2),
        ),
        risk(timestamp=NOW + timedelta(seconds=40)),
        NOW + timedelta(seconds=40),
    )
    assert "order request is stale" in stale_request.reason


@pytest.mark.asyncio
async def test_gateway_health_open_order_rate_and_adapter_failures() -> None:
    settings = live_settings(maximum_orders_per_minute=1, maximum_open_orders=1)
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    adapter.healthy = False
    unhealthy = await gateway.place_order(order(), risk(), NOW)
    assert "health" in unhealthy.reason
    adapter.healthy = True
    adapter.open_orders.append(
        LiveOpenOrder(
            external_order_id="open",
            exchange="sandbox",
            symbol="BTC",
            side=Side.BUY,
            quantity=Decimal("0.1"),
            reduce_only=False,
            created_at=NOW,
        )
    )
    full = await gateway.place_order(order("request-2", "idempotency-key-0002"), risk(), NOW)
    assert "open order" in full.reason
    adapter.open_orders.clear()
    accepted = await gateway.place_order(order("request-3", "idempotency-key-0003"), risk(), NOW)
    assert accepted.state == LiveOrderState.ACCEPTED
    limited = await gateway.place_order(
        order("request-4", "idempotency-key-0004"), risk(), NOW + timedelta(seconds=1)
    )
    assert "rate limit" in limited.reason

    second_gateway = LiveExecutionGateway(
        live_settings(), adapter, evaluate_live_preflight(live_settings(), approved_context())
    )
    adapter.fail_place = True
    with pytest.raises(ExecutionAdapterError, match="details retained"):
        await second_gateway.place_order(order("request-5", "idempotency-key-0005"), risk(), NOW)
    adapter.fail_place = False
    adapter.mismatch_request = True
    mismatch_gateway = LiveExecutionGateway(
        live_settings(), adapter, evaluate_live_preflight(live_settings(), approved_context())
    )
    with pytest.raises(ExecutionAdapterError, match="mismatched"):
        await mismatch_gateway.place_order(order("request-6", "idempotency-key-0006"), risk(), NOW)


@pytest.mark.asyncio
async def test_gateway_contingency_error_paths_are_audited() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    with pytest.raises(ValueError, match="reason"):
        await gateway.activate_kill_switch("", NOW)
    with pytest.raises(ValueError, match="not live-allowed"):
        await gateway.close_position("ETH", NOW)
    adapter.fail_cancel = True
    with pytest.raises(ExecutionAdapterError, match="cancel failed"):
        await gateway.cancel_order("external", NOW)
    adapter.fail_cancel_all = True
    with pytest.raises(ExecutionAdapterError, match="manual intervention"):
        await gateway.activate_kill_switch("emergency", NOW)
    adapter.fail_cancel_all = False
    adapter.fail_close = True
    with pytest.raises(ExecutionAdapterError, match="manual intervention"):
        await gateway.close_position("BTC", NOW)


@pytest.mark.asyncio
async def test_unapproved_gateway_never_calls_adapter() -> None:
    settings = Settings(_env_file=None)
    adapter = FakeExecutionAdapter()
    report = evaluate_live_preflight(settings, LivePreflightContext(timestamp=NOW))
    gateway = LiveExecutionGateway(settings, adapter, report)
    receipt = await gateway.place_order(order(), risk(), NOW)
    assert receipt.state == LiveOrderState.REJECTED
    assert "preflight" in receipt.reason and adapter.place_calls == 0
    with pytest.raises(RuntimeError, match="preflight"):
        await gateway.cancel_order("external", NOW)
    assert await gateway.activate_kill_switch("safe refusal", NOW) == ()


@pytest.mark.asyncio
async def test_rate_window_expires_and_audit_sink_receives_events() -> None:
    settings = live_settings(maximum_orders_per_minute=1)
    adapter = FakeExecutionAdapter()
    captured = []
    gateway = LiveExecutionGateway(
        settings,
        adapter,
        evaluate_live_preflight(settings, approved_context()),
        audit_sink=captured.append,
        model_version="model-test",
        config_version="config-test",
    )
    await gateway.place_order(order(), risk(), NOW)
    later_request = order(
        "request-later",
        "idempotency-key-later",
        created_at=NOW + timedelta(minutes=2),
        expires_at=NOW + timedelta(minutes=3),
    )
    receipt = await gateway.place_order(
        later_request, risk(timestamp=NOW + timedelta(minutes=2)), NOW + timedelta(minutes=2)
    )
    assert receipt.state == LiveOrderState.ACCEPTED and adapter.place_calls == 2
    assert captured[-1].model_version == "model-test"


@pytest.mark.asyncio
async def test_kill_switch_blocks_entries_but_preserves_contingency_actions() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    await gateway.activate_kill_switch("operator emergency", NOW)
    assert adapter.cancel_all_calls == 1
    blocked = await gateway.place_order(order(), risk(), NOW)
    assert "kill switch" in blocked.reason
    adapter.positions.append(
        LivePosition(
            exchange="sandbox",
            symbol="BTC",
            quantity=Decimal("0.1"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            observed_at=NOW,
        )
    )
    reduce = await gateway.place_order(
        order(
            "reduce-1",
            "idempotency-reduce-0001",
            side=Side.SELL,
            reduce_only=True,
        ),
        risk(False),
        NOW,
    )
    assert reduce.state == LiveOrderState.ACCEPTED
    canceled = await gateway.cancel_order("external-1", NOW)
    closed = await gateway.close_position("BTC", NOW)
    assert canceled.canceled and closed.request_id == "close-BTC"


@pytest.mark.asyncio
async def test_reduce_only_is_bounded_by_observed_position() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    missing = await gateway.place_order(
        order("reduce-none", "idempotency-reduce-none", side=Side.SELL, reduce_only=True),
        risk(False),
        NOW,
    )
    assert "no open position" in missing.reason
    adapter.positions.append(
        LivePosition(
            exchange="sandbox",
            symbol="BTC",
            quantity=Decimal("0.1"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            observed_at=NOW,
        )
    )
    wrong_side = await gateway.place_order(
        order("reduce-side", "idempotency-reduce-side", side=Side.BUY, reduce_only=True),
        risk(False),
        NOW,
    )
    assert "increase exposure" in wrong_side.reason
    too_large = await gateway.place_order(
        order(
            "reduce-large",
            "idempotency-reduce-large",
            side=Side.SELL,
            reduce_only=True,
            quantity=Decimal("0.2"),
        ),
        risk(False),
        NOW,
    )
    assert "exceeds open position" in too_large.reason
    assert adapter.place_calls == 0


def test_preview_never_calls_adapter_and_models_reject_invalid_orders() -> None:
    settings = live_settings()
    adapter = FakeExecutionAdapter()
    gateway = LiveExecutionGateway(
        settings, adapter, evaluate_live_preflight(settings, approved_context())
    )
    preview = gateway.preview_order(order(), NOW)
    assert preview.state == LiveOrderState.DRY_RUN and not preview.adapter_called
    assert adapter.place_calls == 0
    with pytest.raises(ValueError, match="execution timestamp"):
        gateway.preview_order(order(), datetime(2025, 1, 1))
    with pytest.raises(ValidationError, match="expiry"):
        order(expires_at=NOW)
    with pytest.raises(ValidationError, match="timezone"):
        order(created_at=datetime(2025, 1, 1), expires_at=NOW)
    with pytest.raises(ValidationError, match="limit_price"):
        order(order_type=OrderType.LIMIT)
    with pytest.raises(ValidationError, match="cannot specify"):
        order(order_type=OrderType.MARKET, limit_price=Decimal("100"))
    with pytest.raises(ValidationError, match="ack timestamp"):
        ExecutionOrderAck(
            request_id="request",
            state=LiveOrderState.ACCEPTED,
            accepted_at=datetime(2025, 1, 1),
            reason="invalid",
            adapter_called=True,
        )
    with pytest.raises(ValidationError, match="external_order_id"):
        ExecutionOrderAck(
            request_id="request",
            state=LiveOrderState.ACCEPTED,
            accepted_at=NOW,
            reason="invalid",
            adapter_called=True,
        )
    with pytest.raises(ValidationError, match="preflight timestamp"):
        LivePreflightContext(timestamp=datetime(2025, 1, 1))
    with pytest.raises(ValidationError, match="cancel timestamp"):
        CancelAck(
            external_order_id="order",
            canceled=True,
            timestamp=datetime(2025, 1, 1),
            reason="invalid",
        )
    with pytest.raises(ValidationError, match="open-order timestamp"):
        LiveOpenOrder(
            external_order_id="order",
            exchange="sandbox",
            symbol="BTC",
            side=Side.BUY,
            quantity=Decimal("1"),
            reduce_only=False,
            created_at=datetime(2025, 1, 1),
        )
    with pytest.raises(ValidationError, match="position timestamp"):
        LivePosition(
            exchange="sandbox",
            symbol="BTC",
            quantity=Decimal("1"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            observed_at=datetime(2025, 1, 1),
        )
    with pytest.raises(ValidationError, match="audit timestamp"):
        ExecutionAuditEvent(
            event_id="event",
            timestamp=datetime(2025, 1, 1),
            event_type="test",
            allowed=False,
            reason="invalid",
            model_version="test",
            config_version="test",
        )
    with pytest.raises(ValueError, match="does not match"):
        LiveExecutionGateway(
            settings,
            DisabledExecutionAdapter(),
            evaluate_live_preflight(settings, approved_context()),
        )
