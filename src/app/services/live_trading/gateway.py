import asyncio
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from app.adapters.exchanges.base import ExecutionAdapter
from app.config.settings import Settings
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionAuditEvent,
    ExecutionOrderAck,
    LiveOrderRequest,
    LiveOrderState,
)
from app.domain.risk.models import RiskDecision
from app.services.live_trading.preflight import LivePreflightReport


class ExecutionAdapterError(RuntimeError):
    pass


class LiveExecutionGateway:
    """Fail-closed boundary around an explicitly supplied execution adapter."""

    def __init__(
        self,
        settings: Settings,
        adapter: ExecutionAdapter,
        preflight: LivePreflightReport,
        audit_sink: Callable[[ExecutionAuditEvent], None] | None = None,
        model_version: str = "phase9-interface-1.0",
        config_version: str = "runtime",
    ) -> None:
        self.settings = settings
        self.adapter = adapter
        self.preflight = preflight
        self._validate_preflight_integrity()
        self.audit_sink = audit_sink
        self.model_version = model_version
        self.config_version = config_version
        self.kill_switch_reason: str | None = None
        self.audit_events: list[ExecutionAuditEvent] = []
        self._idempotent_receipts: dict[str, ExecutionOrderAck] = {}
        self._order_timestamps: deque[datetime] = deque()
        self._placement_lock = asyncio.Lock()

    def preview_order(self, request: LiveOrderRequest, timestamp: datetime) -> ExecutionOrderAck:
        timestamp = self._utc(timestamp)
        reason = self._validate_request(
            request,
            timestamp,
            risk_allowed=True,
            risk_evaluated_at=timestamp,
            preview=True,
        )
        accepted = reason is None
        receipt = ExecutionOrderAck(
            request_id=request.request_id,
            state=LiveOrderState.DRY_RUN if accepted else LiveOrderState.REJECTED,
            accepted_at=timestamp,
            reason="dry-run preview accepted; adapter not called"
            if accepted
            else reason or "rejected",
            adapter_called=False,
        )
        self._audit(timestamp, "order_preview", request.request_id, accepted, receipt.reason)
        return receipt

    async def place_order(
        self,
        request: LiveOrderRequest,
        risk_decision: RiskDecision,
        timestamp: datetime,
    ) -> ExecutionOrderAck:
        # Serialize the complete check-and-submit transaction. Without this lock, concurrent
        # coroutines could bypass idempotency, open-order and rate-limit checks.
        async with self._placement_lock:
            return await self._place_order_locked(request, risk_decision, timestamp)

    async def _place_order_locked(
        self,
        request: LiveOrderRequest,
        risk_decision: RiskDecision,
        timestamp: datetime,
    ) -> ExecutionOrderAck:
        timestamp = self._utc(timestamp)
        previous = self._idempotent_receipts.get(request.idempotency_key)
        if previous is not None:
            self._audit(
                timestamp,
                "idempotent_replay",
                request.request_id,
                previous.state == LiveOrderState.ACCEPTED,
                "returned prior receipt without calling adapter",
            )
            return previous
        reason = self._validate_request(
            request,
            timestamp,
            risk_allowed=risk_decision.allowed or request.reduce_only,
            risk_evaluated_at=risk_decision.evaluated_at,
            risk_decision_id=risk_decision.decision_id,
            maximum_risk_quantity=(
                risk_decision.sizing.quantity if risk_decision.sizing is not None else None
            ),
            preview=False,
        )
        if reason is not None:
            return self._reject_and_remember(request, timestamp, reason)
        try:
            if not await self.adapter.health_check():
                return self._reject_and_remember(request, timestamp, "adapter health check failed")
            open_orders = await self.adapter.fetch_open_orders()
            if len(open_orders) >= self.settings.live.maximum_open_orders:
                return self._reject_and_remember(
                    request, timestamp, "maximum open order count reached"
                )
            if request.reduce_only:
                positions = await self.adapter.fetch_positions()
                position = next(
                    (
                        item
                        for item in positions
                        if item.exchange == request.exchange and item.symbol == request.symbol
                    ),
                    None,
                )
                if position is None or position.quantity == 0:
                    return self._reject_and_remember(
                        request, timestamp, "reduce-only order has no open position"
                    )
                closes_long = position.quantity > 0 and request.side.value == "sell"
                closes_short = position.quantity < 0 and request.side.value == "buy"
                if not (closes_long or closes_short):
                    return self._reject_and_remember(
                        request, timestamp, "reduce-only side would increase exposure"
                    )
                if request.quantity > abs(position.quantity):
                    return self._reject_and_remember(
                        request, timestamp, "reduce-only quantity exceeds open position"
                    )
            receipt = await self.adapter.place_order(request)
        except Exception as exc:
            self._audit(
                timestamp,
                "adapter_error",
                request.request_id,
                False,
                type(exc).__name__,
            )
            raise ExecutionAdapterError(
                "execution adapter failed; details retained internally"
            ) from exc
        if receipt.request_id != request.request_id:
            self._audit(
                timestamp, "adapter_protocol_error", request.request_id, False, "request mismatch"
            )
            raise ExecutionAdapterError("execution adapter returned a mismatched request id")
        self._idempotent_receipts[request.idempotency_key] = receipt
        self._order_timestamps.append(timestamp)
        self._audit(
            timestamp,
            "order_acknowledged",
            request.request_id,
            receipt.state == LiveOrderState.ACCEPTED,
            receipt.reason,
            {
                "external_order_id": receipt.external_order_id or "",
                "signal_id": request.signal_id,
                "risk_decision_id": request.risk_decision_id,
                "symbol": request.symbol,
                "reference_notional": str(request.reference_notional),
            },
        )
        return receipt

    async def cancel_order(self, external_order_id: str, timestamp: datetime) -> CancelAck:
        timestamp = self._utc(timestamp)
        self._require_approved()
        try:
            result = await self.adapter.cancel_order(external_order_id)
        except Exception as exc:
            self._audit(timestamp, "cancel_error", external_order_id, False, type(exc).__name__)
            raise ExecutionAdapterError("cancel failed; details retained internally") from exc
        self._audit(timestamp, "order_canceled", external_order_id, result.canceled, result.reason)
        return result

    async def activate_kill_switch(self, reason: str, timestamp: datetime) -> tuple[CancelAck, ...]:
        timestamp = self._utc(timestamp)
        if not reason.strip():
            raise ValueError("kill-switch reason is required")
        self.kill_switch_reason = reason
        self._audit(timestamp, "kill_switch", None, False, reason)
        if not self.preflight.approved:
            return ()
        try:
            canceled = tuple(await self.adapter.cancel_all_orders())
        except Exception as exc:
            self._audit(timestamp, "cancel_all_error", None, False, type(exc).__name__)
            raise ExecutionAdapterError("cancel-all failed; manual intervention required") from exc
        return canceled

    async def close_position(self, symbol: str, timestamp: datetime) -> ExecutionOrderAck:
        timestamp = self._utc(timestamp)
        self._require_approved()
        if symbol not in self.settings.live.allowed_symbols:
            raise ValueError("position symbol is not live-allowed")
        try:
            result = await self.adapter.close_position(symbol)
        except Exception as exc:
            self._audit(timestamp, "close_error", symbol, False, type(exc).__name__)
            raise ExecutionAdapterError(
                "close-position failed; manual intervention required"
            ) from exc
        self._audit(timestamp, "position_close", result.request_id, True, result.reason)
        return result

    def _validate_request(
        self,
        request: LiveOrderRequest,
        timestamp: datetime,
        *,
        risk_allowed: bool,
        risk_evaluated_at: datetime,
        preview: bool,
        risk_decision_id: str | None = None,
        maximum_risk_quantity: Decimal | None = None,
    ) -> str | None:
        if not preview and not self.preflight.approved:
            return "live preflight is not approved"
        if not preview:
            preflight_age = timestamp - self.preflight.timestamp
            if preflight_age < timedelta(0) or preflight_age > timedelta(
                seconds=self.settings.live.preflight_ttl_seconds
            ):
                return "live preflight is stale or future-dated"
        request_age = timestamp - request.created_at
        if request_age < timedelta(0):
            return "order request is future-dated"
        if request_age > timedelta(seconds=self.settings.risk.stale_data_seconds):
            return "order request is stale"
        if request.expires_at <= timestamp:
            return "order request has expired"
        enabled_exchanges = {
            exchange.name for exchange in self.settings.exchanges if exchange.execution_enabled
        }
        if request.exchange not in enabled_exchanges:
            return "exchange is not execution-enabled"
        if request.symbol not in self.settings.live.allowed_symbols:
            return "symbol is not live-allowed"
        maximum = Decimal(str(self.settings.live.maximum_order_notional))
        if request.reference_notional > maximum:
            return "order reference notional exceeds live maximum"
        if not risk_allowed:
            return "risk manager rejected the order"
        if not preview and risk_decision_id != request.risk_decision_id:
            return "risk decision identity does not match the order request"
        if not preview and not request.reduce_only:
            if maximum_risk_quantity is None:
                return "entry risk decision has no approved position size"
            if request.quantity > maximum_risk_quantity:
                return "order quantity exceeds the risk-approved position size"
        risk_age = timestamp - risk_evaluated_at
        if (
            not preview
            and not request.reduce_only
            and (
                risk_age < timedelta(0)
                or risk_age > timedelta(seconds=self.settings.risk.stale_data_seconds)
            )
        ):
            return "risk decision is stale or future-dated"
        if self.kill_switch_reason and not request.reduce_only:
            return "kill switch blocks new exposure"
        cutoff = timestamp - timedelta(minutes=1)
        while self._order_timestamps and self._order_timestamps[0] <= cutoff:
            self._order_timestamps.popleft()
        if len(self._order_timestamps) >= self.settings.live.maximum_orders_per_minute:
            return "live order rate limit reached"
        return None

    def _reject_and_remember(
        self, request: LiveOrderRequest, timestamp: datetime, reason: str
    ) -> ExecutionOrderAck:
        receipt = ExecutionOrderAck(
            request_id=request.request_id,
            state=LiveOrderState.REJECTED,
            accepted_at=timestamp,
            reason=reason,
            adapter_called=False,
        )
        self._idempotent_receipts[request.idempotency_key] = receipt
        self._audit(
            timestamp,
            "order_rejected",
            request.request_id,
            False,
            reason,
            {
                "signal_id": request.signal_id,
                "risk_decision_id": request.risk_decision_id,
                "symbol": request.symbol,
                "reference_notional": str(request.reference_notional),
            },
        )
        return receipt

    def _require_approved(self) -> None:
        if not self.preflight.approved:
            raise RuntimeError("live preflight is not approved")

    def _validate_preflight_integrity(self) -> None:
        required_checks = {
            "production_environment",
            "live_enabled",
            "paper_disabled",
            "dry_run_disabled",
            "configuration_confirmation",
            "runtime_confirmation",
            "concrete_adapter",
            "adapter_health",
            "credentials_present",
            "withdrawal_permission_disabled",
            "data_quality",
            "websocket_connected",
            "clock_synchronized",
            "kill_switch_clear",
            "paper_validation",
            "out_of_sample_validation",
        }
        actual_checks = {check.name for check in self.preflight.checks}
        checks_passed = all(check.passed for check in self.preflight.checks)
        if self.preflight.approved != checks_passed:
            raise ValueError("preflight approval is inconsistent with its checks")
        if self.preflight.approved and actual_checks != required_checks:
            raise ValueError("approved preflight is missing required safety checks")
        if self.preflight.approved and (
            not self.adapter.is_concrete
            or self.adapter.adapter_name != self.settings.live.adapter_name
        ):
            raise ValueError("approved preflight does not match the supplied concrete adapter")

    def _audit(
        self,
        timestamp: datetime,
        event_type: str,
        request_id: str | None,
        allowed: bool,
        reason: str,
        details: dict[str, str] | None = None,
    ) -> None:
        event = ExecutionAuditEvent(
            event_id=str(uuid4()),
            timestamp=timestamp.astimezone(UTC),
            event_type=event_type,
            request_id=request_id,
            allowed=allowed,
            reason=reason,
            model_version=self.model_version,
            config_version=self.config_version,
            details=details or {},
        )
        self.audit_events.append(event)
        if self.audit_sink is not None:
            self.audit_sink(event)

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("execution timestamp must be timezone-aware")
        return value.astimezone(UTC)
