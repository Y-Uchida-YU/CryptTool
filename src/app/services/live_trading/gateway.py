import asyncio
import hashlib
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol, cast
from uuid import uuid4

from app.adapters.exchanges.base import ExecutionAdapter
from app.adapters.exchanges.websocket import ReconciliationState
from app.config.settings import Settings
from app.domain.execution.leg_state import LegExecutionMachine, PreflightChangeAction
from app.domain.execution.live_models import (
    CancelAck,
    ExecutionAuditEvent,
    ExecutionOrderAck,
    LiveOrderRequest,
    LiveOrderState,
    PreflightRecoveryAction,
)
from app.domain.market_data.evidence import CapabilityEvidence, LegDataEvidence
from app.domain.market_data.source_event_repository import SourceEventRepository, StoredSourceEvent
from app.domain.risk.models import RiskDecision
from app.domain.strategies.capabilities import STRATEGY_CAPABILITY_REGISTRY
from app.domain.venues.models import CapabilityUseCase
from app.domain.venues.trusted_capabilities import TrustedCapabilityRegistry
from app.services.live_trading.cross_venue_preflight import (
    CrossVenuePreflightService,
    InMemoryPreflightBindingRepository,
    PreflightBinding,
    PreflightBindingRepository,
    PreflightBindingState,
    new_binding,
)
from app.services.live_trading.preflight import LivePreflightReport
from app.services.venue_eligibility import execution_eligibility_reason


class ExecutionAdapterError(RuntimeError):
    pass


class ReconciliationExecutionAdapter(Protocol):
    async def fetch_recent_fills(self, symbol: str) -> Sequence[ExecutionOrderAck]: ...

    async def lookup_order_by_client_id(self, request_id: str) -> ExecutionOrderAck | None: ...


RECONCILIATION_CAPABILITIES = {
    "lookup_order_by_client_id": "order_lookup_by_client_id",
    "fetch_recent_fills": "recent_fills",
    "fetch_open_orders": "open_orders",
    "fetch_positions": "positions",
}


class LiveExecutionGateway:
    """Fail-closed boundary around an explicitly supplied execution adapter."""

    def __init__(
        self,
        settings: Settings,
        adapter: ExecutionAdapter,
        preflight: LivePreflightReport,
        source_event_repository: SourceEventRepository | None = None,
        trusted_capability_registry: TrustedCapabilityRegistry | None = None,
        audit_sink: Callable[[ExecutionAuditEvent], None] | None = None,
        model_version: str = "phase9-interface-1.0",
        config_version: str = "runtime",
        leg_execution_machine: LegExecutionMachine | None = None,
        cross_venue_preflight_service: CrossVenuePreflightService | None = None,
        preflight_binding_repository: PreflightBindingRepository | None = None,
    ) -> None:
        self.settings = settings
        self.adapter = adapter
        self.preflight = preflight
        self.source_event_repository = source_event_repository or (
            cast(SourceEventRepository, adapter) if hasattr(adapter, "get") else None
        )
        self.trusted_capability_registry = trusted_capability_registry or getattr(
            adapter, "trusted_capability_registry", None
        )
        self.audit_sink = audit_sink
        self.model_version = model_version
        self.config_version = config_version
        self.kill_switch_reason: str | None = None
        self.audit_events: list[ExecutionAuditEvent] = []
        self._idempotent_receipts: dict[str, ExecutionOrderAck] = {}
        self._order_timestamps: deque[datetime] = deque()
        self._placement_lock = asyncio.Lock()
        self.cross_venue_preflight_service = cross_venue_preflight_service or getattr(
            adapter, "cross_venue_preflight_service", None
        )
        repository = preflight_binding_repository or getattr(
            adapter, "preflight_binding_repository", None
        )
        if self.settings.live.enabled:
            if repository is None or not repository.durable:
                raise ValueError("live execution requires a durable preflight binding repository")
            self.preflight_binding_repository = repository
            self._validate_reconciliation_startup()
        else:
            self.preflight_binding_repository = repository or InMemoryPreflightBindingRepository()
        self._validate_preflight_integrity()
        self.leg_execution_machine = leg_execution_machine
        self.last_preflight_action: PreflightChangeAction | None = None

    def _validate_reconciliation_startup(self) -> None:
        if not self.adapter.is_concrete:
            return
        if self.trusted_capability_registry is None:
            raise ValueError("live execution requires a trusted capability registry")
        for method_name, capability in RECONCILIATION_CAPABILITIES.items():
            if not callable(getattr(self.adapter, method_name, None)):
                raise ValueError(f"concrete execution adapter is missing {method_name}")
            try:
                self.trusted_capability_registry.require_live_verified(
                    venue=self.adapter.adapter_name,
                    capability=capability,
                    now=datetime.now(UTC),
                )
            except ValueError as exc:
                raise ValueError(
                    f"reconciliation capability {capability} is not LIVE_VERIFIED: {exc}"
                ) from exc

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
        *,
        cas_retry: bool = False,
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
        submission_started = False
        reserved = False
        try:
            if not await self.adapter.health_check():
                return self._reject_and_remember(request, timestamp, "adapter health check failed")
            reservation_failure = self._reserve_preflight(request, timestamp)
            if reservation_failure is not None:
                if reservation_failure == "preflight binding CAS conflict" and not cas_retry:
                    try:
                        await self._refresh_binding_and_exchange_state(request)
                    except Exception:
                        return self._reject_and_remember(
                            request,
                            timestamp,
                            "preflight binding CAS conflict and exchange refresh failed",
                        )
                    return await self._place_order_locked(
                        request, risk_decision, timestamp, cas_retry=True
                    )
                return self._reject_and_remember(request, timestamp, reservation_failure)
            reserved = True
            open_orders = await self.adapter.fetch_open_orders()
            if len(open_orders) >= self.settings.live.maximum_open_orders:
                self._abort_preflight(request, timestamp, "maximum open order count reached")
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
                    self._abort_preflight(request, timestamp, "reduce-only position is absent")
                    return self._reject_and_remember(
                        request, timestamp, "reduce-only order has no open position"
                    )
                closes_long = position.quantity > 0 and request.side.value == "sell"
                closes_short = position.quantity < 0 and request.side.value == "buy"
                if not (closes_long or closes_short):
                    self._abort_preflight(request, timestamp, "reduce-only side increases exposure")
                    return self._reject_and_remember(
                        request, timestamp, "reduce-only side would increase exposure"
                    )
                if request.quantity > abs(position.quantity):
                    self._abort_preflight(request, timestamp, "reduce-only quantity is excessive")
                    return self._reject_and_remember(
                        request, timestamp, "reduce-only quantity exceeds open position"
                    )
            submission_started = True
            receipt = await self.adapter.place_order(request)
        except Exception as exc:
            if reserved and not submission_started:
                self._abort_preflight(request, timestamp, type(exc).__name__)
            elif submission_started:
                self._mark_reconciliation_required(request, timestamp, type(exc).__name__)
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
            self._mark_reconciliation_required(request, timestamp, "adapter request id mismatch")
            self._audit(
                timestamp, "adapter_protocol_error", request.request_id, False, "request mismatch"
            )
            raise ExecutionAdapterError("execution adapter returned a mismatched request id")
        try:
            self._record_preflight_ack(request, receipt, timestamp)
        except ExecutionAdapterError:
            await self._refresh_binding_and_exchange_state(request)
            refreshed = self.preflight_binding(request.signal_id)
            expected = (
                PreflightBindingState.COMPLETED
                if request.order_leg_role != refreshed.first_leg_role
                else PreflightBindingState.FIRST_LEG_ACCEPTED
            )
            if refreshed.state != expected:
                self._mark_reconciliation_required(
                    request, timestamp, "binding conflict after adapter acknowledgement"
                )
                raise
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
        legs = (order_leg,) if request.reduce_only else (evidence.receive_leg, evidence.pay_leg)
        for leg in legs:
            expected_use_case = (
                CapabilityUseCase.EMERGENCY_EXIT
                if request.reduce_only
                else CapabilityUseCase.NEW_EXPOSURE
            )
            for item in leg.capabilities:
                failure = self._validate_capability(
                    request, item, leg.venue, expected_use_case, timestamp
                )
                if failure is not None:
                    return failure
        preflight_failure = self._validate_cross_venue_preflight(request, timestamp)
        if preflight_failure is not None:
            return preflight_failure
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

    def _validate_capability(
        self,
        request: LiveOrderRequest,
        item: CapabilityEvidence,
        expected_venue: str,
        expected_use_case: CapabilityUseCase,
        timestamp: datetime,
    ) -> str | None:
        age = timestamp - item.verified_at
        if item.venue != expected_venue:
            return "capability evidence venue does not match order venue"
        if item.use_case != expected_use_case:
            return "capability evidence use case does not match order intent"
        if age < timedelta(0) or age > timedelta(
            seconds=self.settings.live.capability_verification_max_age_seconds
        ):
            return "capability evidence is stale or future-dated"
        try:
            if self.trusted_capability_registry is None:
                return "trusted capability registry is unavailable"
            trusted = self.trusted_capability_registry.require(
                venue=item.venue,
                capability=item.capability,
                verification_run_id=item.verification_run_id,
                verified_at=item.verified_at,
                use_case=item.use_case,
                now=timestamp,
            )
        except ValueError as exc:
            return f"trusted capability rejected evidence: {exc}"
        if item.support != trusted.support:
            return "capability support does not match trusted record"
        trusted_fields = (
            (item.adapter_version, trusted.adapter_version, "adapter version"),
            (item.source_version, trusted.source_version, "source version"),
            (
                item.contract_fixture_sha256,
                trusted.contract_fixture_sha256,
                "fixture sha256",
            ),
            (item.audit_run_id, trusted.audit_run_id, "capability audit run"),
        )
        for claimed, actual, label in trusted_fields:
            if claimed != actual:
                return f"{label} does not match trusted record"
        return self._validate_source_events(request, item, timestamp, expected_venue)

    def _validate_source_events(
        self,
        request: LiveOrderRequest,
        capability: CapabilityEvidence,
        timestamp: datetime,
        expected_venue: str,
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
            if stored.venue != expected_venue:
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

    def _validate_cross_venue_preflight(
        self, request: LiveOrderRequest, timestamp: datetime
    ) -> str | None:
        evidence = request.cross_venue_signal_evidence
        if evidence.receive_leg is None or evidence.pay_leg is None:
            return "cross-venue execution preflight leg evidence is missing"
        preflight = request.cross_venue_preflight
        if self.cross_venue_preflight_service is None:
            return "trusted cross-venue preflight service is unavailable"
        try:
            self.cross_venue_preflight_service.verify(preflight, timestamp)
        except ValueError as exc:
            return f"trusted cross-venue preflight rejected snapshot: {exc}"
        if preflight.signal_id != request.signal_id:
            return "cross-venue execution preflight hash or signal is invalid"
        if (
            preflight.receive_venue != evidence.receive_leg.venue
            or preflight.pay_venue != evidence.pay_leg.venue
        ):
            return "cross-venue execution preflight venues do not match evidence"
        if not preflight.created_at <= timestamp < preflight.expires_at:
            return "cross-venue execution preflight is stale or future-dated"
        if not preflight.receive_execution_health or not preflight.pay_execution_health:
            return "cross-venue execution preflight health check failed"
        if min(preflight.receive_available_collateral, preflight.pay_available_collateral) <= 0:
            return "cross-venue execution preflight collateral is insufficient"
        if (
            min(preflight.receive_fillable_quantity, preflight.pay_fillable_quantity)
            < request.quantity
        ):
            return "cross-venue execution preflight fillable quantity is insufficient"
        binding = self.preflight_binding_repository.get(request.signal_id)
        expected = binding.preflight_hash if binding is not None else None
        if (
            binding is not None
            and binding.state
            not in {
                PreflightBindingState.UNBOUND,
                PreflightBindingState.ABORTED,
            }
            and expected != preflight.preflight_hash
        ):
            if self.leg_execution_machine is not None:
                self.last_preflight_action = self.leg_execution_machine.handle_preflight_change(
                    timestamp,
                    evidence_valid=True,
                    execution_health=preflight.pay_execution_health,
                    available_collateral=preflight.pay_available_collateral,
                    fillable_quantity=preflight.pay_fillable_quantity,
                    expected_vwap=preflight.pay_expected_vwap,
                    alternate_hedge_available=True,
                )
            return "cross-venue orders use different preflight hashes"
        if (
            preflight.receive_capability_hash != evidence.receive_leg.evidence_hash
            or preflight.pay_capability_hash != evidence.pay_leg.evidence_hash
        ):
            return "cross-venue preflight capability hashes do not match evidence"
        if preflight.receive_source_event_hash != self._source_event_hash(
            evidence.receive_leg
        ) or preflight.pay_source_event_hash != self._source_event_hash(evidence.pay_leg):
            return "cross-venue preflight source event hashes do not match evidence"
        return None

    def preflight_binding(self, signal_id: str) -> PreflightBinding:
        return self.preflight_binding_repository.get(signal_id) or PreflightBinding(
            signal_id, None, PreflightBindingState.UNBOUND
        )

    def _reserve_preflight(self, request: LiveOrderRequest, timestamp: datetime) -> str | None:
        current = self.preflight_binding(request.signal_id)
        preflight_hash = request.cross_venue_preflight.preflight_hash
        if current.state in {PreflightBindingState.UNBOUND, PreflightBindingState.ABORTED}:
            if request.preflight_recovery_action is not None:
                return "recovery action is invalid without unmatched exposure"
            binding = new_binding(
                signal_id=request.signal_id,
                preflight_hash=preflight_hash,
                state=PreflightBindingState.RESERVED,
                now=timestamp,
            )
            if current.state == PreflightBindingState.ABORTED:
                binding = replace(
                    binding,
                    version=current.version + 1,
                    created_at=current.created_at,
                )
            return None if self._cas(current, binding) else "preflight binding CAS conflict"
        if current.preflight_hash != preflight_hash:
            return "cross-venue orders use different preflight hashes"
        if current.state == PreflightBindingState.FIRST_LEG_ACCEPTED:
            if current.first_leg_role == request.order_leg_role:
                return "cross-venue second order must use the opposite leg role"
            if request.preflight_recovery_action is not None:
                return "recovery action is invalid before a hedge failure"
            binding = replace(
                current,
                state=PreflightBindingState.SECOND_LEG_SUBMITTED,
                second_order_request_id=request.request_id,
                version=current.version + 1,
                updated_at=timestamp,
                failure_reason=None,
            )
            return None if self._cas(current, binding) else "preflight binding CAS conflict"
        if current.state == PreflightBindingState.HEDGING_REQUIRED:
            allowed = self._hedging_operation_allowed(current, request)
            if not allowed:
                return "hedging-required binding only permits retry, hedge, unwind, or manual halt"
            binding = replace(
                current,
                state=PreflightBindingState.SECOND_LEG_SUBMITTED,
                second_order_request_id=request.request_id,
                version=current.version + 1,
                updated_at=timestamp,
                failure_reason=None,
            )
            return None if self._cas(current, binding) else "preflight binding CAS conflict"
        if current.state == PreflightBindingState.COMPLETED:
            return "cross-venue preflight binding is already completed"
        if current.state == PreflightBindingState.RECONCILIATION_REQUIRED:
            return "cross-venue preflight binding requires reconciliation"
        if current.state == PreflightBindingState.HALTED:
            return "cross-venue preflight binding is halted"
        return f"cross-venue preflight binding is {current.state.value}"

    def _abort_preflight(
        self, request: LiveOrderRequest, timestamp: datetime, failure_reason: str
    ) -> None:
        current = self.preflight_binding(request.signal_id)
        self._transition(
            current,
            PreflightBindingState.ABORTED,
            timestamp,
            failure_reason=failure_reason,
        )

    def _record_preflight_ack(
        self, request: LiveOrderRequest, receipt: ExecutionOrderAck, timestamp: datetime
    ) -> None:
        current = self.preflight_binding(request.signal_id)
        if receipt.state != LiveOrderState.ACCEPTED:
            target = (
                PreflightBindingState.HEDGING_REQUIRED
                if current.state == PreflightBindingState.SECOND_LEG_SUBMITTED
                else PreflightBindingState.ABORTED
            )
            self._transition(current, target, timestamp, failure_reason=receipt.reason)
            return
        if current.state == PreflightBindingState.SECOND_LEG_SUBMITTED:
            self._transition(
                current,
                PreflightBindingState.COMPLETED,
                timestamp,
                second_external_order_id=receipt.external_order_id,
            )
        else:
            self._transition(
                current,
                PreflightBindingState.FIRST_LEG_ACCEPTED,
                timestamp,
                first_leg_role=request.order_leg_role,
                first_order_request_id=request.request_id,
                first_external_order_id=receipt.external_order_id,
            )

    def _mark_reconciliation_required(
        self, request: LiveOrderRequest, timestamp: datetime, failure_reason: str
    ) -> None:
        current = self.preflight_binding(request.signal_id)
        if current.state == PreflightBindingState.RESERVED:
            self._transition(
                current,
                PreflightBindingState.RECONCILIATION_REQUIRED,
                timestamp,
                failure_reason=failure_reason,
                first_order_request_id=request.request_id,
            )
        elif current.state == PreflightBindingState.SECOND_LEG_SUBMITTED:
            self._transition(
                current,
                PreflightBindingState.RECONCILIATION_REQUIRED,
                timestamp,
                failure_reason=failure_reason,
                second_order_request_id=request.request_id,
            )

    async def reconcile_binding(
        self, request: LiveOrderRequest, timestamp: datetime
    ) -> PreflightBinding:
        timestamp = self._utc(timestamp)
        current = self.preflight_binding(request.signal_id)
        if current.state != PreflightBindingState.RECONCILIATION_REQUIRED:
            raise ValueError("preflight binding does not require reconciliation")
        try:
            reconciliation_adapter = cast(ReconciliationExecutionAdapter, self.adapter)
            recent_fills = await reconciliation_adapter.fetch_recent_fills(request.symbol)
            client_order = await reconciliation_adapter.lookup_order_by_client_id(
                request.request_id
            )
            open_orders = await self.adapter.fetch_open_orders(request.symbol)
            positions = await self.adapter.fetch_positions()
        except Exception as exc:
            self._transition(
                current,
                PreflightBindingState.HALTED,
                timestamp,
                failure_reason=f"reconciliation failed: {type(exc).__name__}",
            )
            return self.preflight_binding(request.signal_id)

        matched_fill = next(
            (fill for fill in recent_fills if fill.request_id == request.request_id), None
        )
        reconciled_ack = (
            client_order
            if client_order is not None and client_order.state == LiveOrderState.ACCEPTED
            else matched_fill
        )
        accepted = reconciled_ack is not None
        accepted = accepted or any(
            item.exchange == request.exchange and item.symbol == request.symbol
            for item in open_orders
        )
        exposed = any(
            item.exchange == request.exchange
            and item.symbol == request.symbol
            and item.quantity != 0
            for item in positions
        )
        is_second = current.second_order_request_id == request.request_id
        if accepted or exposed:
            target = (
                PreflightBindingState.COMPLETED
                if is_second
                else PreflightBindingState.FIRST_LEG_ACCEPTED
            )
        elif is_second and current.first_external_order_id is not None:
            target = PreflightBindingState.HEDGING_REQUIRED
        else:
            target = PreflightBindingState.ABORTED
        identifiers: dict[str, Any] = {}
        if reconciled_ack is not None and is_second:
            identifiers["second_external_order_id"] = reconciled_ack.external_order_id
        elif reconciled_ack is not None:
            identifiers["first_external_order_id"] = reconciled_ack.external_order_id
        self._transition(current, target, timestamp, failure_reason=None, **identifiers)
        return self.preflight_binding(request.signal_id)

    async def _refresh_binding_and_exchange_state(
        self, request: LiveOrderRequest
    ) -> PreflightBinding:
        reconciliation_adapter = cast(ReconciliationExecutionAdapter, self.adapter)
        self.preflight_binding_repository.get(request.signal_id)
        await reconciliation_adapter.fetch_recent_fills(request.symbol)
        await reconciliation_adapter.lookup_order_by_client_id(request.request_id)
        await self.adapter.fetch_open_orders(request.symbol)
        await self.adapter.fetch_positions()
        return self.preflight_binding(request.signal_id)

    def halt_binding(self, signal_id: str, reason: str, timestamp: datetime) -> PreflightBinding:
        current = self.preflight_binding(signal_id)
        if current.state not in {
            PreflightBindingState.HEDGING_REQUIRED,
            PreflightBindingState.RECONCILIATION_REQUIRED,
        }:
            raise ValueError("only recovery-required bindings may be halted")
        self._transition(current, PreflightBindingState.HALTED, self._utc(timestamp), reason)
        return self.preflight_binding(signal_id)

    @staticmethod
    def _hedging_operation_allowed(current: PreflightBinding, request: LiveOrderRequest) -> bool:
        opposite_leg = current.first_leg_role != request.order_leg_role
        action = request.preflight_recovery_action
        if action is None:
            return opposite_leg
        if action in {
            PreflightRecoveryAction.ALTERNATE_HEDGE,
            PreflightRecoveryAction.PARTIAL_HEDGE,
        }:
            return opposite_leg
        return (
            action == PreflightRecoveryAction.FIRST_LEG_UNWIND
            and request.reduce_only
            and current.first_leg_role == request.order_leg_role
        )

    def _transition(
        self,
        current: PreflightBinding,
        state: PreflightBindingState,
        timestamp: datetime,
        failure_reason: str | None = None,
        **updates: Any,
    ) -> None:
        binding = replace(
            current,
            state=state,
            version=current.version + 1,
            updated_at=timestamp,
            failure_reason=failure_reason,
            **updates,
        )
        if not self._cas(current, binding):
            raise ExecutionAdapterError("preflight binding CAS conflict")

    def _cas(self, current: PreflightBinding, binding: PreflightBinding) -> bool:
        return self.preflight_binding_repository.compare_and_set(
            binding.signal_id, current.version, binding
        )

    @staticmethod
    def _source_event_hash(leg: LegDataEvidence) -> str:
        values = sorted(
            f"{event.event_id}:{event.payload_sha256}"
            for capability in leg.capabilities
            for event in capability.source_events
        )
        return hashlib.sha256("|".join(values).encode()).hexdigest()

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
