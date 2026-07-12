from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.domain.regimes.models import Regime
from app.domain.risk import (
    CircuitBreaker,
    PositionSizer,
    PositionSizingResult,
    RiskDecision,
    RiskHaltReason,
    RiskLimits,
    RiskManager,
    RiskState,
    SizingMethod,
    diagonal_risk_parity_weights,
)
from app.domain.strategies.models import (
    Signal,
    SignalIntent,
    SignalSide,
    StrategyName,
)

NOW = datetime(2024, 1, 1, tzinfo=UTC)


def signal(
    *,
    confidence: float = 0.80,
    intent: SignalIntent = SignalIntent.ENTRY,
    side: SignalSide = SignalSide.BUY,
) -> Signal:
    return Signal(
        signal_id="a" * 64,
        timestamp=NOW,
        strategy=StrategyName.TREND_FOLLOWING,
        symbol="BTC",
        exchange="test_exchange",
        side=side,
        intent=intent,
        strength=0.80,
        confidence=confidence,
        suggested_risk_fraction=0.0025,
        primary_regime=Regime.TREND_UP,
        data_quality_score=0.95,
        valid_until=NOW + timedelta(hours=1),
        evidence=("auditable test signal",),
        feature_snapshot={"ma_slope_z": 2.0},
    )


def state(**updates: object) -> RiskState:
    values: dict[str, object] = {
        "timestamp": NOW,
        "equity": Decimal("1000"),
        "peak_equity": Decimal("1000"),
        "day_start_equity": Decimal("1000"),
        "week_start_equity": Decimal("1000"),
        "symbol_exposures": {},
        "exchange_exposures": {},
    }
    values.update(updates)
    return RiskState.model_validate(values)


def test_conservative_position_sizing_uses_smallest_constraint() -> None:
    manager = RiskManager()
    decision = manager.evaluate(
        signal(),
        Decimal("100"),
        Decimal("98"),
        state(),
        atr=Decimal("10"),
        annualized_volatility=0.50,
        lot_size=Decimal("0.001"),
    )
    assert decision.allowed and decision.sizing is not None
    assert decision.sizing.risk_amount == Decimal("2.000000")
    assert decision.sizing.binding_constraint == "atr_based"
    assert decision.sizing.notional == Decimal("10.000")
    assert decision.sizing.quantity == Decimal("0.100")
    assert decision.sizing.candidate_notionals["fixed_fractional"] == Decimal("100.0000")


def test_minimum_order_and_stop_validation_reject_without_increasing_risk() -> None:
    manager = RiskManager()
    no_stop = manager.evaluate(signal(), Decimal("100"), None, state())
    assert not no_stop.allowed and no_stop.sizing is not None
    assert no_stop.sizing.quantity == 0

    too_small = manager.evaluate(
        signal(),
        Decimal("100"),
        Decimal("98"),
        state(),
        minimum_notional=Decimal("101"),
    )
    assert not too_small.allowed and too_small.sizing is not None
    assert "below minimum" in too_small.reasons[0]


@pytest.mark.parametrize(
    ("updates", "expected"),
    [
        ({"realized_pnl_today": Decimal("-10")}, RiskHaltReason.DAILY_LOSS),
        ({"realized_pnl_week": Decimal("-30")}, RiskHaltReason.WEEKLY_LOSS),
        ({"consecutive_losses": 5}, RiskHaltReason.CONSECUTIVE_LOSSES),
        ({"data_healthy": False}, RiskHaltReason.DATA_ANOMALY),
        ({"api_healthy": False}, RiskHaltReason.API_UNHEALTHY),
        ({"websocket_connected": False}, RiskHaltReason.WEBSOCKET_DISCONNECTED),
        ({"spread_fraction": 0.006}, RiskHaltReason.SPREAD_ANOMALY),
        ({"price_divergence_fraction": 0.03}, RiskHaltReason.PRICE_DIVERGENCE),
    ],
)
def test_operational_and_loss_limits_trip_circuit_breaker(
    updates: dict[str, object], expected: RiskHaltReason
) -> None:
    manager = RiskManager()
    decision = manager.evaluate(signal(), Decimal("100"), Decimal("98"), state(**updates))
    assert not decision.allowed
    assert expected in decision.breaker_reasons
    assert manager.breaker.status(NOW).active


def test_drawdown_latches_until_exact_manual_reset() -> None:
    manager = RiskManager()
    drawdown_state = state(equity=Decimal("920"), peak_equity=Decimal("1000"))
    decision = manager.evaluate(signal(), Decimal("100"), Decimal("98"), drawdown_state)
    assert not decision.allowed
    status = manager.breaker.status(NOW)
    assert RiskHaltReason.MAX_DRAWDOWN in status.reasons
    assert status.requires_manual_reset and status.resume_at is None
    with pytest.raises(ValueError, match="exact"):
        manager.reset_kill_switch(NOW, "yes")
    manager.reset_kill_switch(NOW, "RESET_KILL_SWITCH")
    assert not manager.breaker.status(NOW).active


def test_transient_breaker_enforces_cooldown_after_health_recovers() -> None:
    limits = RiskLimits(cooldown_seconds=60)
    manager = RiskManager(limits)
    manager.evaluate(signal(), Decimal("100"), Decimal("98"), state(api_healthy=False))
    recovered = state(timestamp=NOW + timedelta(seconds=30))
    still_blocked = manager.evaluate(signal(), Decimal("100"), Decimal("98"), recovered)
    assert not still_blocked.allowed
    assert RiskHaltReason.API_UNHEALTHY in still_blocked.breaker_reasons

    after_cooldown = state(timestamp=NOW + timedelta(seconds=61))
    resumed = manager.evaluate(signal(), Decimal("100"), Decimal("98"), after_cooldown)
    assert resumed.allowed


def test_exposure_position_confidence_and_data_quality_limits() -> None:
    manager = RiskManager()
    constrained = state(
        gross_exposure=Decimal("1000"),
        symbol_exposures={"BTC": Decimal("350")},
        exchange_exposures={"test_exchange": Decimal("500")},
        open_positions=3,
    )
    decision = manager.evaluate(signal(), Decimal("100"), Decimal("98"), constrained)
    assert not decision.allowed
    assert any("gross exposure" in reason for reason in decision.reasons)
    assert any("simultaneous" in reason for reason in decision.reasons)

    low_confidence = manager.evaluate(
        signal(confidence=0.59), Decimal("100"), Decimal("98"), state()
    )
    assert not low_confidence.allowed
    bad_quality = manager.evaluate(
        signal(), Decimal("100"), Decimal("98"), state(data_quality_score=0.79)
    )
    assert not bad_quality.allowed
    assert RiskHaltReason.DATA_ANOMALY in bad_quality.breaker_reasons


def test_manual_kill_blocks_entries_but_not_risk_reduction() -> None:
    manager = RiskManager()
    manager.activate_kill_switch(NOW, "operator requested stop")
    entry = manager.evaluate(signal(), Decimal("100"), Decimal("98"), state())
    assert not entry.allowed
    assert RiskHaltReason.MANUAL_KILL_SWITCH in entry.breaker_reasons
    assert manager.breaker.status(NOW).details[RiskHaltReason.MANUAL_KILL_SWITCH] == (
        "operator requested stop"
    )

    exit_decision = manager.evaluate(
        signal(intent=SignalIntent.EXIT), Decimal("100"), None, state()
    )
    assert exit_decision.allowed and exit_decision.sizing is None


def test_circuit_breaker_and_risk_parity_input_guards() -> None:
    breaker = CircuitBreaker(10)
    breaker.trip(RiskHaltReason.API_UNHEALTHY, NOW)
    assert breaker.status(NOW + timedelta(seconds=9)).active
    assert not breaker.status(NOW + timedelta(seconds=10)).active

    weights = diagonal_risk_parity_weights({"BTC": 0.5, "ETH": 1.0})
    assert weights["BTC"] == pytest.approx(2 / 3)
    assert sum(weights.values()) == pytest.approx(1)
    with pytest.raises(ValueError):
        diagonal_risk_parity_weights({"BTC": 0.0})
    with pytest.raises(ValueError):
        diagonal_risk_parity_weights({})


def test_risk_configuration_rejects_inconsistent_exposure_and_loss_limits() -> None:
    with pytest.raises(ValidationError, match="symbol exposure"):
        RiskLimits(maximum_gross_exposure=0.5, maximum_symbol_exposure=0.6)
    with pytest.raises(ValidationError, match="exchange exposure"):
        RiskLimits(maximum_gross_exposure=0.5, maximum_exchange_exposure=0.6)
    with pytest.raises(ValidationError, match="weekly loss"):
        RiskLimits(maximum_daily_loss=0.04, maximum_weekly_loss=0.03)


def test_risk_state_and_decision_contract_validation() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        state(timestamp=datetime(2024, 1, 1))
    with pytest.raises(ValidationError, match="non-negative"):
        state(symbol_exposures={"BTC": Decimal("-1")})
    with pytest.raises(ValidationError, match="peak_equity"):
        state(equity=Decimal("1001"), peak_equity=Decimal("1000"))

    rejected_size = PositionSizingResult(
        accepted=False,
        quantity=Decimal("0"),
        notional=Decimal("0"),
        risk_amount=Decimal("0"),
        binding_constraint="test",
        reason="test rejection",
    )
    with pytest.raises(ValidationError, match="rejected sizing"):
        PositionSizingResult(
            accepted=False,
            quantity=Decimal("1"),
            notional=Decimal("100"),
            risk_amount=Decimal("1"),
            binding_constraint="test",
            reason="invalid rejection",
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        RiskDecision(
            decision_id="risk-test-allowed",
            allowed=False,
            reasons=("test",),
            evaluated_at=datetime(2024, 1, 1),
        )
    with pytest.raises(ValidationError, match="allowed risk decision"):
        RiskDecision(
            decision_id="risk-test-rejected",
            allowed=True,
            reasons=("test",),
            sizing=rejected_size,
            evaluated_at=NOW,
        )


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"entry_price": Decimal("0")}, "entry price"),
        ({"stop_price": Decimal("100")}, "distinct positive stop"),
        ({"stop_price": Decimal("101")}, "buy entry stop"),
        ({"lot_size": Decimal("0")}, "lot_size"),
        ({"minimum_notional": Decimal("-1")}, "minimum_notional"),
        ({"atr": Decimal("0")}, "ATR"),
        ({"annualized_volatility": 0.0}, "annualized volatility"),
    ],
)
def test_position_sizer_rejects_unsafe_inputs(kwargs: dict[str, object], reason: str) -> None:
    arguments: dict[str, object] = {
        "signal": signal(),
        "entry_price": Decimal("100"),
        "stop_price": Decimal("98"),
        "state": state(),
    }
    arguments.update(kwargs)
    result = PositionSizer(RiskLimits()).size(**arguments)  # type: ignore[arg-type]
    assert not result.accepted and reason in result.reason

    wrong_sell_stop = PositionSizer(RiskLimits()).size(
        signal(side=SignalSide.SELL), Decimal("100"), Decimal("99"), state()
    )
    assert not wrong_sell_stop.accepted and "sell entry stop" in wrong_sell_stop.reason


def test_position_sizer_enforces_rounding_and_available_exposure() -> None:
    sizer = PositionSizer(RiskLimits())
    no_capacity = sizer.size(
        signal(),
        Decimal("100"),
        Decimal("98"),
        state(gross_exposure=Decimal("1000")),
    )
    assert not no_capacity.accepted and "exhausted" in no_capacity.reason

    rounded_to_zero = sizer.size(
        signal(),
        Decimal("100"),
        Decimal("98"),
        state(),
        lot_size=Decimal("2"),
    )
    assert not rounded_to_zero.accepted and "rounds to zero" in rounded_to_zero.reason


@pytest.mark.parametrize(
    "method",
    [SizingMethod.FIXED_FRACTIONAL, SizingMethod.ATR_BASED, SizingMethod.VOLATILITY_TARGETING],
)
def test_position_sizing_methods_are_selectable_and_still_exposure_capped(
    method: SizingMethod,
) -> None:
    limits = RiskLimits(sizing_method=method)
    result = PositionSizer(limits).size(
        signal(),
        Decimal("100"),
        Decimal("98"),
        state(symbol_exposures={"BTC": Decimal("349")}),
        atr=Decimal("1"),
        annualized_volatility=0.5,
    )
    assert result.accepted
    assert result.notional == Decimal("1")
    assert result.binding_constraint == "symbol_exposure"


def test_missing_required_input_for_selected_sizer_is_a_clean_rejection() -> None:
    limits = RiskLimits(sizing_method=SizingMethod.VOLATILITY_TARGETING)
    decision = RiskManager(limits).evaluate(signal(), Decimal("100"), Decimal("98"), state())
    assert not decision.allowed and decision.sizing is not None
    assert decision.sizing.binding_constraint == "missing_sizing_input"


def test_expired_and_unknown_signals_are_rejected_before_sizing() -> None:
    manager = RiskManager()
    expired = manager.evaluate(
        signal(),
        Decimal("100"),
        Decimal("98"),
        state(timestamp=NOW + timedelta(hours=2)),
    )
    assert not expired.allowed and "expired" in expired.reasons[0]
    unknown_signal = signal().model_copy(update={"primary_regime": Regime.UNKNOWN})
    unknown = manager.evaluate(unknown_signal, Decimal("100"), Decimal("98"), state())
    assert not unknown.allowed and any("UNKNOWN" in reason for reason in unknown.reasons)


def test_circuit_breaker_validates_timestamps_cooldowns_and_kill_reason() -> None:
    with pytest.raises(ValueError, match="positive"):
        CircuitBreaker(0)
    breaker = CircuitBreaker(10)
    with pytest.raises(ValueError, match="timezone-aware"):
        breaker.trip(RiskHaltReason.API_UNHEALTHY, datetime(2024, 1, 1))
    with pytest.raises(ValueError, match="positive"):
        breaker.trip(RiskHaltReason.API_UNHEALTHY, NOW, cooldown_seconds=0)
    breaker.trip(RiskHaltReason.API_UNHEALTHY, NOW + timedelta(seconds=2))
    breaker.trip(RiskHaltReason.DATA_ANOMALY, NOW, cooldown_seconds=30)
    status = breaker.status(NOW + timedelta(seconds=15))
    assert status.active and status.tripped_at == NOW
    assert status.resume_at == NOW + timedelta(seconds=30)
    with pytest.raises(ValueError, match="reason"):
        RiskManager().activate_kill_switch(NOW, "  ")
