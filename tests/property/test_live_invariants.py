from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from app.adapters.exchanges.disabled import DisabledExecutionAdapter
from app.config.settings import Settings
from app.domain.execution.live_models import LiveOrderRequest, LiveOrderState
from app.domain.market_data.models import Side
from app.services.live_trading.gateway import LiveExecutionGateway
from app.services.live_trading.preflight import LivePreflightContext, evaluate_live_preflight

NOW = datetime(2025, 1, 1, tzinfo=UTC)


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
    gateway = LiveExecutionGateway(
        configured,
        DisabledExecutionAdapter(),
        evaluate_live_preflight(configured, context),
    )
    request = LiveOrderRequest(
        request_id="request-property",
        idempotency_key="idempotency-property",
        signal_id="signal-property",
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
