from __future__ import annotations

from collections.abc import Mapping
from decimal import ROUND_FLOOR, Decimal

from app.domain.risk.models import PositionSizingResult, RiskLimits, RiskState, SizingMethod
from app.domain.strategies.models import Signal, SignalSide

ZERO = Decimal("0")


def diagonal_risk_parity_weights(volatilities: Mapping[str, float]) -> dict[str, float]:
    """Inverse-volatility weights: a transparent diagonal-covariance risk-parity baseline."""

    if not volatilities:
        raise ValueError("at least one volatility is required")
    if any(volatility <= 0 for volatility in volatilities.values()):
        raise ValueError("volatilities must be positive")
    inverse = {symbol: 1 / volatility for symbol, volatility in volatilities.items()}
    total = sum(inverse.values())
    return {symbol: value / total for symbol, value in inverse.items()}


class PositionSizer:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits

    def size(
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
    ) -> PositionSizingResult:
        if entry_price <= 0:
            return self._reject("entry price must be positive")
        if stop_price is None or stop_price <= 0 or stop_price == entry_price:
            return self._reject("a distinct positive stop price is required")
        if signal.side == SignalSide.BUY and stop_price >= entry_price:
            return self._reject("buy entry stop must be below entry price")
        if signal.side == SignalSide.SELL and stop_price <= entry_price:
            return self._reject("sell entry stop must be above entry price")
        if lot_size is not None and lot_size <= 0:
            return self._reject("lot_size must be positive")
        if minimum_notional is not None and minimum_notional < 0:
            return self._reject("minimum_notional cannot be negative")

        risk_fraction = min(signal.suggested_risk_fraction, self.limits.maximum_risk_per_trade)
        evidence_scale = Decimal(str(min(signal.strength, signal.confidence)))
        risk_amount = state.equity * Decimal(str(risk_fraction)) * evidence_scale
        stop_fraction = abs(entry_price - stop_price) / entry_price
        candidates: dict[str, Decimal] = {
            SizingMethod.FIXED_FRACTIONAL.value: risk_amount / stop_fraction,
            SizingMethod.MAXIMUM_LOSS.value: risk_amount
            / max(stop_fraction, Decimal(str(self.limits.maximum_loss_floor_fraction))),
        }
        if atr is not None:
            if atr <= 0:
                return self._reject("ATR must be positive when supplied")
            atr_stop_fraction = Decimal(str(self.limits.atr_multiple)) * atr / entry_price
            candidates[SizingMethod.ATR_BASED.value] = risk_amount / atr_stop_fraction
        if annualized_volatility is not None:
            if annualized_volatility <= 0:
                return self._reject("annualized volatility must be positive when supplied")
            candidates[SizingMethod.VOLATILITY_TARGETING.value] = (
                state.equity
                * Decimal(str(self.limits.annual_volatility_target))
                / Decimal(str(annualized_volatility))
            )

        exchange_exposure = state.exchange_exposures.get(signal.exchange or "", ZERO)
        candidates["gross_exposure"] = max(
            state.equity * Decimal(str(self.limits.maximum_gross_exposure)) - state.gross_exposure,
            ZERO,
        )
        candidates["leverage"] = max(
            state.equity * Decimal(str(self.limits.maximum_leverage)) - state.gross_exposure,
            ZERO,
        )
        candidates["symbol_exposure"] = max(
            state.equity * Decimal(str(self.limits.maximum_symbol_exposure))
            - state.symbol_exposures.get(signal.symbol, ZERO),
            ZERO,
        )
        candidates["exchange_exposure"] = max(
            state.equity * Decimal(str(self.limits.maximum_exchange_exposure)) - exchange_exposure,
            ZERO,
        )

        selected = self._select_candidates(candidates)
        binding_constraint, capped_notional = min(selected.items(), key=lambda item: item[1])
        if capped_notional <= 0 or risk_amount <= 0:
            return self._reject(
                "risk or exposure budget is exhausted", candidates, binding_constraint, risk_amount
            )
        quantity = capped_notional / entry_price
        if lot_size is not None:
            quantity = (quantity / lot_size).to_integral_value(rounding=ROUND_FLOOR) * lot_size
        notional = quantity * entry_price
        if quantity <= 0:
            return self._reject(
                "quantity rounds to zero at configured lot size",
                candidates,
                binding_constraint,
                risk_amount,
            )
        if minimum_notional is not None and notional < minimum_notional:
            return self._reject(
                f"notional {notional} is below minimum {minimum_notional}",
                candidates,
                binding_constraint,
                risk_amount,
            )
        return PositionSizingResult(
            accepted=True,
            quantity=quantity,
            notional=notional,
            risk_amount=risk_amount,
            binding_constraint=binding_constraint,
            candidate_notionals=candidates,
            reason="sized to the most conservative active constraint",
        )

    def _select_candidates(self, candidates: Mapping[str, Decimal]) -> dict[str, Decimal]:
        exposure_names = {
            "gross_exposure",
            "leverage",
            "symbol_exposure",
            "exchange_exposure",
        }
        always = {name: value for name, value in candidates.items() if name in exposure_names}
        if self.limits.sizing_method == SizingMethod.CONSERVATIVE_MINIMUM:
            return dict(candidates)
        requested = candidates.get(self.limits.sizing_method.value)
        if requested is None:
            raise ValueError(
                f"{self.limits.sizing_method.value} sizing requires an unavailable input"
            )
        return {self.limits.sizing_method.value: requested, **always}

    @staticmethod
    def _reject(
        reason: str,
        candidates: Mapping[str, Decimal] | None = None,
        binding_constraint: str = "invalid_input",
        risk_amount: Decimal = ZERO,
    ) -> PositionSizingResult:
        return PositionSizingResult(
            accepted=False,
            quantity=ZERO,
            notional=ZERO,
            risk_amount=risk_amount,
            binding_constraint=binding_constraint,
            candidate_notionals=dict(candidates or {}),
            reason=reason,
        )
