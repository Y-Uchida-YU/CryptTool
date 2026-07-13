from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from app.domain.market_data.clock import CrossVenueTimestamp, VenueClock
from app.domain.market_data.evidence import (
    RejectedSignal,
    SignalDataEvidence,
    require_signal_capabilities,
)


class ExecutableBook(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: str
    symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    clock: CrossVenueTimestamp

    def bid_vwap(self, quantity: Decimal) -> Decimal:
        return _vwap(self.bids, quantity)

    def ask_vwap(self, quantity: Decimal) -> Decimal:
        return _vwap(self.asks, quantity)


class FundingLeg(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: str
    symbol: str
    quote_currency: str
    settlement_currency: str
    expected_rates: tuple[Decimal, ...]
    notional: Decimal = Field(gt=0)
    fee_rate_round_trip: Decimal = Field(ge=0)
    slippage_rate_round_trip: Decimal = Field(ge=0)


class CrossVenueFundingOpportunity(BaseModel):
    model_config = ConfigDict(frozen=True)
    receive_leg: FundingLeg
    pay_leg: FundingLeg
    expected_funding_received: Decimal
    expected_funding_paid: Decimal
    entry_fees: Decimal
    exit_fees: Decimal
    entry_slippage: Decimal
    exit_slippage: Decimal
    expected_basis_convergence_loss: Decimal
    transfer_cost: Decimal
    venue_risk_premium: Decimal
    currency_risk_charge: Decimal
    expected_net_carry: Decimal
    stressed_net_carry: Decimal


class CrossVenueFundingArbitrageStrategy:
    required_capabilities = ("funding_current", "funding_history", "orderbook_snapshot")

    def validate_evidence(
        self,
        signal_id: str,
        evidence: SignalDataEvidence,
        venue: str,
        now: datetime,
        maximum_age_seconds: int,
    ) -> RejectedSignal | None:
        return require_signal_capabilities(
            signal_id, evidence, self.required_capabilities, venue, now, maximum_age_seconds
        )

    def evaluate(
        self,
        receive_leg: FundingLeg,
        pay_leg: FundingLeg,
        *,
        expected_basis_convergence_loss: Decimal,
        transfer_cost: Decimal,
        venue_risk_premium: Decimal,
        currency_risk_rate: Decimal = Decimal("0.0025"),
        funding_reversal_stress: Decimal = Decimal("1.5"),
    ) -> CrossVenueFundingOpportunity:
        notional = min(receive_leg.notional, pay_leg.notional)
        received = notional * sum(receive_leg.expected_rates, Decimal("0"))
        paid = notional * sum(pay_leg.expected_rates, Decimal("0"))
        total_fee = notional * (receive_leg.fee_rate_round_trip + pay_leg.fee_rate_round_trip)
        total_slippage = notional * (
            receive_leg.slippage_rate_round_trip + pay_leg.slippage_rate_round_trip
        )
        currency_charge = (
            notional * currency_risk_rate
            if {
                receive_leg.quote_currency,
                receive_leg.settlement_currency,
                pay_leg.quote_currency,
                pay_leg.settlement_currency,
            }
            - {receive_leg.quote_currency}
            else Decimal("0")
        )
        entry_fees = total_fee / 2
        exit_fees = total_fee - entry_fees
        entry_slippage = total_slippage / 2
        exit_slippage = total_slippage - entry_slippage
        costs = (
            entry_fees
            + exit_fees
            + entry_slippage
            + exit_slippage
            + expected_basis_convergence_loss
            + transfer_cost
            + venue_risk_premium
            + currency_charge
        )
        net = received - paid - costs
        reversal_loss = (
            notional
            * sum((abs(rate) for rate in receive_leg.expected_rates), Decimal("0"))
            * funding_reversal_stress
        )
        return CrossVenueFundingOpportunity(
            receive_leg=receive_leg,
            pay_leg=pay_leg,
            expected_funding_received=received,
            expected_funding_paid=paid,
            entry_fees=entry_fees,
            exit_fees=exit_fees,
            entry_slippage=entry_slippage,
            exit_slippage=exit_slippage,
            expected_basis_convergence_loss=expected_basis_convergence_loss,
            transfer_cost=transfer_cost,
            venue_risk_premium=venue_risk_premium,
            currency_risk_charge=currency_charge,
            expected_net_carry=net,
            stressed_net_carry=net - reversal_loss,
        )


class BasisOpportunity(BaseModel):
    model_config = ConfigDict(frozen=True)
    buy_venue: str
    sell_venue: str
    quantity: Decimal
    buy_ask_vwap: Decimal
    sell_bid_vwap: Decimal
    fees: Decimal
    expected_exit_cost: Decimal
    latency_buffer: Decimal
    risk_premium: Decimal
    executable_spread: Decimal


class CrossVenueBasisStrategy:
    required_capabilities = ("orderbook_snapshot", "index_price")

    def __init__(self, clock: VenueClock) -> None:
        self.clock = clock

    def validate_evidence(
        self,
        signal_id: str,
        evidence: SignalDataEvidence,
        venue: str,
        now: datetime,
        maximum_age_seconds: int,
    ) -> RejectedSignal | None:
        return require_signal_capabilities(
            signal_id, evidence, self.required_capabilities, venue, now, maximum_age_seconds
        )

    def evaluate(
        self,
        buy_book: ExecutableBook,
        sell_book: ExecutableBook,
        quantity: Decimal,
        *,
        fees: Decimal,
        expected_exit_cost: Decimal,
        latency_buffer: Decimal,
        risk_premium: Decimal,
    ) -> BasisOpportunity:
        if not self.clock.comparable(buy_book.clock, sell_book.clock):
            raise ValueError("cross-venue books are not clock-comparable")
        buy = buy_book.ask_vwap(quantity)
        sell = sell_book.bid_vwap(quantity)
        spread = (sell - buy) * quantity - fees - expected_exit_cost - latency_buffer - risk_premium
        return BasisOpportunity(
            buy_venue=buy_book.venue,
            sell_venue=sell_book.venue,
            quantity=quantity,
            buy_ask_vwap=buy,
            sell_bid_vwap=sell,
            fees=fees,
            expected_exit_cost=expected_exit_cost,
            latency_buffer=latency_buffer,
            risk_premium=risk_premium,
            executable_spread=spread,
        )


def _vwap(levels: tuple[tuple[Decimal, Decimal], ...], quantity: Decimal) -> Decimal:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    remaining, notional = quantity, Decimal("0")
    for price, available in levels:
        if price <= 0 or available <= 0:
            raise ValueError("book levels must be positive")
        fill = min(remaining, available)
        notional += fill * price
        remaining -= fill
        if remaining == 0:
            return notional / quantity
    raise ValueError("insufficient executable depth")
