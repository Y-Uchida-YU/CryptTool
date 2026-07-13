import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from app.adapters.exchanges.disabled import DisabledExecutionAdapter
from app.adapters.exchanges.websocket import ReconciliationState
from app.config.settings import Settings
from app.domain.execution.live_models import (
    CrossVenueExecutionPreflight,
    LiveOrderRequest,
    LiveOrderState,
)
from app.domain.market_data.evidence import (
    CapabilityEvidence,
    CrossVenueSignalEvidence,
    LegDataEvidence,
    SourceEventEvidence,
)
from app.domain.market_data.models import Side
from app.domain.market_data.source_event_repository import StoredSourceEvent
from app.domain.venues.models import CapabilitySupport, CapabilityUseCase
from app.domain.venues.trusted_capabilities import (
    TrustedCapabilityRecord,
    TrustedCapabilityRegistry,
)
from app.services.live_trading.gateway import LiveExecutionGateway
from app.services.live_trading.preflight import LivePreflightContext, evaluate_live_preflight

NOW = datetime(2025, 1, 1, tzinfo=UTC)


class EventRepository:
    def __init__(self) -> None:
        self.events: dict[str, StoredSourceEvent] = {}

    def get(self, event_id: str) -> StoredSourceEvent | None:
        return self.events.get(event_id)


def settings() -> Settings:
    return Settings(
        _env_file=None,
        environment="production",
        symbols=("BTC",),
        paper_trading=False,
        live_trading=True,
        dry_run=False,
        live_confirmation="I_ACCEPT_LIVE_TRADING_RISK",
        exchange_api_key="test-key",
        exchange_api_secret="test-secret",
        paper={"enabled": False},
        live={
            "enabled": True,
            "adapter_name": "sandbox",
            "allowed_symbols": ("BTC",),
            "maximum_order_notional": 100,
        },
        exchanges=({"name": "sandbox", "execution_enabled": True},),
        venues={
            "sandbox": {
                "data_enabled": True,
                "execution_enabled": True,
                "eligibility_status": "enabled",
                "jurisdiction": "JP",
                "terms_checked_at": NOW,
                "operator_account_verified": True,
                "api_market_data_available": True,
                "api_execution_available": True,
                "deposits_available": True,
                "withdrawals_available": True,
                "execution_smoke_test_passed": True,
                "reason": "property test",
            }
        },
    )


@given(
    quantity=st.decimals(min_value="0.001", max_value="10", places=3),
    price=st.decimals(min_value="1", max_value="1000", places=2),
)
def test_live_preview_never_calls_adapter_and_respects_notional_cap(
    quantity: Decimal, price: Decimal
) -> None:
    configured = settings()
    context = LivePreflightContext(timestamp=NOW)
    repository = EventRepository()

    def leg(role: str, venue: str) -> LegDataEvidence:
        capabilities = []
        for capability in ("orderbook_snapshot", "index_price"):
            event = SourceEventEvidence(
                f"event-{venue}-{capability}",
                venue,
                "BTC",
                capability,
                NOW,
                NOW,
                NOW,
                "a" * 64,
                1,
                None,
                ReconciliationState.SYNCHRONIZED if capability == "orderbook_snapshot" else None,
                1,
            )
            repository.events[event.event_id] = StoredSourceEvent(**event.__dict__)
            capabilities.append(
                CapabilityEvidence(
                    venue=venue,
                    capability=capability,
                    use_case=CapabilityUseCase.NEW_EXPOSURE,
                    support=CapabilitySupport.LIVE_VERIFIED,
                    verified_at=NOW,
                    verification_run_id="property-smoke",
                    source_events=(event,),
                    adapter_version="property-adapter",
                    source_version="property-source",
                    contract_fixture_sha256="f" * 64,
                    audit_run_id="property-audit",
                )
            )
        return LegDataEvidence.build(role, venue, tuple(capabilities))

    receive_leg = leg("receive_leg", "sandbox")
    pay_leg = leg("pay_leg", "counterparty")
    signal = CrossVenueSignalEvidence.build("signal-property", receive_leg, pay_leg)

    def source_hash(value: LegDataEvidence) -> str:
        items = sorted(
            f"{event.event_id}:{event.payload_sha256}"
            for capability in value.capabilities
            for event in capability.source_events
        )
        return hashlib.sha256("|".join(items).encode()).hexdigest()

    preflight = CrossVenueExecutionPreflight.build(
        signal_id="signal-property",
        receive_venue="sandbox",
        pay_venue="counterparty",
        canonical_instrument_id="BTC-PERP",
        receive_capability_hash=receive_leg.evidence_hash,
        pay_capability_hash=pay_leg.evidence_hash,
        receive_source_event_hash=source_hash(receive_leg),
        pay_source_event_hash=source_hash(pay_leg),
        receive_execution_health=True,
        pay_execution_health=True,
        receive_available_collateral=Decimal("1000"),
        pay_available_collateral=Decimal("1000"),
        receive_fillable_quantity=Decimal("10"),
        pay_fillable_quantity=Decimal("10"),
        receive_expected_vwap=Decimal("100"),
        pay_expected_vwap=Decimal("101"),
        maximum_naked_exposure_duration_ms=3000,
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )
    registry = TrustedCapabilityRegistry(
        tuple(
            TrustedCapabilityRecord(
                venue,
                capability,
                CapabilitySupport.LIVE_VERIFIED,
                "property-smoke",
                NOW,
                NOW + timedelta(days=1),
                "property-adapter",
                "property-source",
                "f" * 64,
                "property-audit",
            )
            for venue in ("sandbox", "counterparty")
            for capability in ("orderbook_snapshot", "index_price")
        )
    )
    gateway = LiveExecutionGateway(
        configured,
        DisabledExecutionAdapter(),
        evaluate_live_preflight(configured, context),
        repository,
        trusted_capability_registry=registry,
    )
    request = LiveOrderRequest(
        request_id="request-property",
        idempotency_key="idempotency-property",
        signal_id="signal-property",
        cross_venue_signal_evidence=signal,
        cross_venue_signal_hash=signal.evidence_hash,
        order_leg_role="receive_leg",
        order_leg_evidence=receive_leg,
        cross_venue_preflight=preflight,
        strategy_id="cross_venue_basis",
        strategy_version="1",
        required_capabilities=("orderbook_snapshot", "index_price"),
        risk_decision_id="risk-property",
        model_version="test",
        config_version="test",
        exchange="sandbox",
        symbol="BTC",
        side=Side.BUY,
        quantity=quantity,
        reference_price=price,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )
    receipt = gateway.preview_order(request, NOW)
    expected = (
        LiveOrderState.DRY_RUN if quantity * price <= Decimal("100") else LiveOrderState.REJECTED
    )
    assert receipt.state == expected
    assert not receipt.adapter_called
