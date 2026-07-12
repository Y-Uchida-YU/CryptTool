from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.domain.regimes.models import Regime
from app.domain.risk.models import (
    CircuitBreakerStatus,
    PositionSizingResult,
    RiskDecision,
    RiskHaltReason,
    RiskLimits,
    RiskState,
)
from app.domain.risk.sizing import PositionSizer
from app.domain.strategies.models import Signal, SignalIntent


class CircuitBreaker:
    """Latching halt: transient faults require cooldown; critical faults require manual reset."""

    _RESET_CONFIRMATION = "RESET_KILL_SWITCH"

    def __init__(self, default_cooldown_seconds: int) -> None:
        if default_cooldown_seconds <= 0:
            raise ValueError("default cooldown must be positive")
        self.default_cooldown_seconds = default_cooldown_seconds
        self._reasons: set[RiskHaltReason] = set()
        self._details: dict[RiskHaltReason, str] = {}
        self._tripped_at: datetime | None = None
        self._resume_at: datetime | None = None
        self._manual_reset_required = False
        self.last_reset_at: datetime | None = None

    def trip(
        self,
        reason: RiskHaltReason,
        at: datetime,
        *,
        requires_manual_reset: bool = False,
        cooldown_seconds: int | None = None,
        detail: str | None = None,
    ) -> None:
        at = self._utc(at)
        cooldown = self.default_cooldown_seconds if cooldown_seconds is None else cooldown_seconds
        if cooldown <= 0:
            raise ValueError("cooldown must be positive")
        self._reasons.add(reason)
        if detail:
            self._details[reason] = detail
        if self._tripped_at is None or at < self._tripped_at:
            self._tripped_at = at
        self._manual_reset_required = self._manual_reset_required or requires_manual_reset
        if self._manual_reset_required:
            self._resume_at = None
            return
        candidate_resume = at + timedelta(seconds=cooldown)
        if self._resume_at is None or candidate_resume > self._resume_at:
            self._resume_at = candidate_resume

    def status(self, at: datetime) -> CircuitBreakerStatus:
        at = self._utc(at)
        if (
            self._reasons
            and not self._manual_reset_required
            and self._resume_at is not None
            and at >= self._resume_at
        ):
            self._clear(at)
        return CircuitBreakerStatus(
            active=bool(self._reasons),
            reasons=tuple(sorted(self._reasons, key=str)),
            details=dict(self._details),
            tripped_at=self._tripped_at,
            resume_at=self._resume_at,
            requires_manual_reset=self._manual_reset_required,
        )

    def reset(self, confirmation: str, at: datetime) -> None:
        if confirmation != self._RESET_CONFIRMATION:
            raise ValueError("exact kill-switch reset confirmation is required")
        self._clear(self._utc(at))

    def _clear(self, at: datetime) -> None:
        self._reasons.clear()
        self._details.clear()
        self._tripped_at = None
        self._resume_at = None
        self._manual_reset_required = False
        self.last_reset_at = at

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("circuit-breaker timestamps must be timezone-aware")
        return value.astimezone(UTC)


class RiskManager:
    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()
        self.sizer = PositionSizer(self.limits)
        self.breaker = CircuitBreaker(self.limits.cooldown_seconds)

    def activate_kill_switch(self, timestamp: datetime, reason: str) -> None:
        if not reason.strip():
            raise ValueError("kill-switch reason is required")
        self.breaker.trip(
            RiskHaltReason.MANUAL_KILL_SWITCH,
            timestamp,
            requires_manual_reset=True,
            detail=reason.strip(),
        )

    def reset_kill_switch(self, timestamp: datetime, confirmation: str) -> None:
        self.breaker.reset(confirmation, timestamp)

    def evaluate(
        self,
        signal: Signal,
        entry_price: Decimal,
        stop_price: Decimal | None,
        state: RiskState,
        *,
        atr: Decimal | None = None,
        annualized_volatility: float | None = None,
        lot_size: Decimal | None = None,
        minimum_notional: Decimal | None = None,
    ) -> RiskDecision:
        if signal.intent in {SignalIntent.EXIT, SignalIntent.REDUCE}:
            return RiskDecision(
                allowed=True,
                reasons=("risk-reducing action bypasses entry limits",),
                evaluated_at=state.timestamp,
                breaker_reasons=self.breaker.status(state.timestamp).reasons,
            )

        reasons: list[str] = []
        transient_halts: list[RiskHaltReason] = []
        critical_halts: list[RiskHaltReason] = []
        if signal.valid_until < state.timestamp:
            reasons.append("signal has expired")
        if signal.primary_regime == Regime.UNKNOWN:
            reasons.append("UNKNOWN regime cannot open risk")
        if signal.confidence < self.limits.minimum_signal_confidence:
            reasons.append(
                f"signal confidence {signal.confidence:.3f} below "
                f"{self.limits.minimum_signal_confidence:.3f}"
            )
        observed_quality = min(signal.data_quality_score, state.data_quality_score)
        if observed_quality < self.limits.minimum_data_quality:
            reasons.append(
                f"data quality {observed_quality:.3f} below {self.limits.minimum_data_quality:.3f}"
            )
            transient_halts.append(RiskHaltReason.DATA_ANOMALY)
        if state.daily_loss_fraction >= Decimal(str(self.limits.maximum_daily_loss)):
            reasons.append(f"daily loss limit reached ({state.daily_loss_fraction:.4%})")
            transient_halts.append(RiskHaltReason.DAILY_LOSS)
        if state.weekly_loss_fraction >= Decimal(str(self.limits.maximum_weekly_loss)):
            reasons.append(f"weekly loss limit reached ({state.weekly_loss_fraction:.4%})")
            transient_halts.append(RiskHaltReason.WEEKLY_LOSS)
        if state.drawdown_fraction >= Decimal(str(self.limits.maximum_drawdown)):
            reasons.append(f"maximum drawdown reached ({state.drawdown_fraction:.4%})")
            critical_halts.append(RiskHaltReason.MAX_DRAWDOWN)
        if state.consecutive_losses >= self.limits.maximum_consecutive_losses:
            reasons.append(f"consecutive-loss stop reached ({state.consecutive_losses})")
            transient_halts.append(RiskHaltReason.CONSECUTIVE_LOSSES)
        if not state.data_healthy:
            reasons.append("market data health check failed")
            transient_halts.append(RiskHaltReason.DATA_ANOMALY)
        if not state.api_healthy:
            reasons.append("exchange API health check failed")
            transient_halts.append(RiskHaltReason.API_UNHEALTHY)
        if not state.websocket_connected:
            reasons.append("market-data WebSocket is disconnected")
            transient_halts.append(RiskHaltReason.WEBSOCKET_DISCONNECTED)
        if state.spread_fraction > self.limits.maximum_spread_fraction:
            reasons.append(f"spread is abnormal ({state.spread_fraction:.4%})")
            transient_halts.append(RiskHaltReason.SPREAD_ANOMALY)
        if state.price_divergence_fraction > self.limits.maximum_price_divergence_fraction:
            reasons.append(
                f"cross-venue price divergence is abnormal ({state.price_divergence_fraction:.4%})"
            )
            transient_halts.append(RiskHaltReason.PRICE_DIVERGENCE)

        equity = state.equity
        gross_limit = equity * Decimal(str(self.limits.maximum_gross_exposure))
        leverage_limit = equity * Decimal(str(self.limits.maximum_leverage))
        if state.gross_exposure >= min(gross_limit, leverage_limit):
            reasons.append("gross exposure or leverage capacity is exhausted")
        symbol_exposure = state.symbol_exposures.get(signal.symbol, Decimal("0"))
        if symbol_exposure >= equity * Decimal(str(self.limits.maximum_symbol_exposure)):
            reasons.append(f"{signal.symbol} exposure capacity is exhausted")
        exchange_exposure = state.exchange_exposures.get(signal.exchange or "", Decimal("0"))
        if exchange_exposure >= equity * Decimal(str(self.limits.maximum_exchange_exposure)):
            reasons.append("exchange exposure capacity is exhausted")
        if state.open_positions >= self.limits.maximum_positions:
            reasons.append("maximum simultaneous positions reached")

        for halt in transient_halts:
            self.breaker.trip(halt, state.timestamp)
        for halt in critical_halts:
            self.breaker.trip(halt, state.timestamp, requires_manual_reset=True)
        breaker_status = self.breaker.status(state.timestamp)
        if breaker_status.active:
            breaker_text = ",".join(reason.value for reason in breaker_status.reasons)
            reasons.append(f"circuit breaker active: {breaker_text}")
        if reasons:
            return RiskDecision(
                allowed=False,
                reasons=tuple(dict.fromkeys(reasons)),
                evaluated_at=state.timestamp,
                breaker_reasons=breaker_status.reasons,
            )

        try:
            sizing = self.sizer.size(
                signal,
                entry_price,
                stop_price,
                state,
                atr=atr,
                annualized_volatility=annualized_volatility,
                lot_size=lot_size,
                minimum_notional=minimum_notional,
            )
        except ValueError as exc:
            sizing = PositionSizingResult(
                accepted=False,
                quantity=Decimal("0"),
                notional=Decimal("0"),
                risk_amount=Decimal("0"),
                binding_constraint="missing_sizing_input",
                reason=str(exc),
            )
        if not sizing.accepted:
            return RiskDecision(
                allowed=False,
                reasons=(sizing.reason,),
                sizing=sizing,
                evaluated_at=state.timestamp,
            )
        return RiskDecision(
            allowed=True,
            reasons=("all entry risk checks passed",),
            sizing=sizing,
            evaluated_at=state.timestamp,
        )
