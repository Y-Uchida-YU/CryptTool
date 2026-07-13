import asyncio
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import uuid4

from app.adapters.exchanges.base import ExecutionAdapter
from app.adapters.exchanges.websocket import ReconciliationState
from app.config.settings import Settings
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionAuditEvent,
    ExecutionOrderAck,
    LiveOrderRequest,
    LiveOrderState,
)
from app.domain.market_data.evidence import CapabilityEvidence
from app.domain.market_data.source_event_repository import SourceEventRepository, StoredSourceEvent
from app.domain.risk.models import RiskDecision
from app.domain.strategies.capabilities import STRATEGY_CAPABILITY_REGISTRY
from app.domain.venues.models import CapabilitySupport, CapabilityUseCase
from app.services.live_trading.preflight import LivePreflightReport
from app.services.venue_eligibility import execution_eligibility_reason


class ExecutionAdapterError(RuntimeError):
    pass


class LiveExecutionGateway:
    """Fail-closed boundary around an explicitly supplied execution adapter."""

    def __init__(
        self,
        settings: Settings,
        adapter: ExecutionAdapter,
        preflight: LivePreflightReport,
        source_event_repository: SourceEventRepository | None = None,
        audit_sink: Callable[[ExecutionAuditEvent], None] | None = None,
        model_version: str = "phase9-interface-1.0",
        config_version: str = "runtime",
    ) -> None:
        self.settings = settings
        self.adapter = adapter
        self.preflight = preflight
        self.source_event_repository = source_event_repository or (
            cast(SourceEventRepository, adapter) if hasattr(adapter, "get") else None
        )
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
        evidence = request.cross_venue_signal_evidence
        if (
            evidence.signal_id != request.signal_id
            or not evidence.valid_hash()
            or request.cross_venue_signal_hash != evidence.evidence_hash
        ):
            return "cross-venue signal evidence identity or hash is invalid"
        if evidence.receive_leg is None:
            return "receive leg evidence is missing"
        if evidence.pay_leg is None:
            return "pay leg evidence is missing"
        if evidence.receive_leg.venue == evidence.pay_leg.venue:
            return "receive and pay venues must differ"
        if not evidence.receive_leg.valid_hash() or not evidence.pay_leg.valid_hash():
            return "cross-venue leg evidence hash is invalid"
        order_leg = evidence.leg(request.order_leg_role)
        if order_leg is None or order_leg != request.order_leg_evidence:
            return "order leg evidence does not match full signal"
        if order_leg.venue != request.exchange:
            return "order venue does not match leg role"
        registered_capabilities = STRATEGY_CAPABILITY_REGISTRY.get(
            (request.strategy_id, request.strategy_version)
        )
        if registered_capabilities is None:
            return "unknown strategy id or version"
        role_capabilities = tuple(
            item.capability
            for item in registered_capabilities
            if item.venue_role == request.order_leg_role
        )
        if request.required_capabilities != role_capabilities:
            return "required capabilities do not exactly match strategy registry"
        required_use_case = (
            CapabilityUseCase.EMERGENCY_EXIT
            if request.reduce_only
            else CapabilityUseCase.NEW_EXPOSURE
        )
        all_capabilities = evidence.receive_leg.capabilities + evidence.pay_leg.capabilities
        keys = [(item.venue, item.capability) for item in all_capabilities]
        if len(keys) != len(set(keys)):
            return "duplicate venue and capability evidence"
        available = {(item.venue, item.capability): item for item in all_capabilities}
        role_venues = {
            "receive_leg": evidence.receive_leg.venue,
            "pay_leg": evidence.pay_leg.venue,
        }
        missing = {
            requirement
            for requirement in registered_capabilities
            if (role_venues.get(requirement.venue_role, ""), requirement.capability)
            not in available
        }
        if missing:
            return "required cross-venue capability evidence is missing"
        for item in order_leg.capabilities:
            age = timestamp - item.verified_at
            if item.venue != request.exchange:
                return "capability evidence venue does not match order venue"
            if item.use_case != required_use_case:
                return "capability evidence use case does not match order intent"
            if item.support != CapabilitySupport.LIVE_VERIFIED:
                return "capability evidence is not LIVE_VERIFIED"
            if age < timedelta(0) or age > timedelta(
                seconds=self.settings.live.capability_verification_max_age_seconds
            ):
                return "capability evidence is stale or future-dated"
            source_failure = self._validate_source_events(request, item, timestamp)
            if source_failure is not None:
                return source_failure
        enabled_exchanges = {
            exchange.name for exchange in self.settings.exchanges if exchange.execution_enabled
        }
        if request.exchange not in enabled_exchanges:
            return "exchange is not execution-enabled"
        try:
            eligibility_failure = execution_eligibility_reason(
                self.settings, request.exchange, timestamp, reduce_only=request.reduce_only
            )
        except KeyError:
            return "venue eligibility is not configured"
        if eligibility_failure is not None:
            return f"venue eligibility rejected execution: {eligibility_failure}"
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

    def _validate_source_events(
        self, request: LiveOrderRequest, capability: CapabilityEvidence, timestamp: datetime
    ) -> str | None:
        import re

        event_types = {
            "funding_current": {"funding_current", "funding"},
            "funding_history": {"funding_history"},
            "orderbook_snapshot": {"orderbook_snapshot", "orderbook_delta"},
            "index_price": {"index_price"},
            "market_liquidation_stream": {"market_liquidation", "liquidation"},
            "open_interest": {"open_interest"},
            "trades": {"trade", "trades"},
        }
        for event in capability.source_events:
            if not event.event_id:
                return "source event id is empty"
            if self.source_event_repository is None:
                return "source event repository is unavailable"
            stored = self.source_event_repository.get(event.event_id)
            if stored is None:
                return "source event does not exist in repository"
            mismatch = self._stored_event_mismatch(event, stored)
            if mismatch is not None:
                return mismatch
            if stored.venue != request.exchange:
                return "stored source event venue does not match order venue"
            if stored.symbol != request.symbol:
                return "stored source event symbol does not match order symbol"
            if stored.event_type not in event_types.get(capability.capability, set()):
                return "source event type does not match capability"
            age = timestamp - stored.available_at
            if age < timedelta(0) or age > timedelta(
                seconds=self.settings.live.source_event_max_age_seconds
            ):
                return "source event is stale or future-dated"
            if re.fullmatch(r"[0-9a-f]{64}", stored.payload_sha256) is None:
                return "source event payload hash is invalid"
            if stored.data_quality_score < self.settings.live.minimum_data_quality:
                return "source event data quality is below threshold"
            snapshot_delta = stored.event_type in {"orderbook_snapshot", "orderbook_delta"}
            if snapshot_delta and stored.reconciliation_state != ReconciliationState.SYNCHRONIZED:
                return "source event is not synchronized"
        return None

    @staticmethod
    def _stored_event_mismatch(event: object, stored: StoredSourceEvent) -> str | None:
        fields = (
            "venue",
            "symbol",
            "event_type",
            "exchange_timestamp",
            "received_at",
            "available_at",
            "payload_sha256",
            "sequence",
            "connection_id",
            "reconciliation_state",
            "data_quality_score",
        )
        for field in fields:
            if getattr(event, field) != getattr(stored, field):
                return f"source event {field} does not match repository"
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
