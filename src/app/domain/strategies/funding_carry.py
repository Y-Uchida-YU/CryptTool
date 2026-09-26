from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from app.domain.strategies.capabilities import (
    FUNDING_CARRY_DEFAULT_SHADOW_INSTRUMENT,
    FUNDING_CARRY_REQUIRED_VENUES,
    FUNDING_CARRY_SHADOW_ALLOWED_INSTRUMENTS,
    FUNDING_CARRY_STRATEGY_ID,
)


class ShadowDisposition(StrEnum):
    CANDIDATE = "candidate"
    REJECTED = "rejected"


class FundingCarryEvaluationStage(StrEnum):
    INPUT_VALIDATION = "input_validation"
    FUNDING_NORMALIZATION = "funding_normalization"
    BOOK_PRICING = "book_pricing"
    ECONOMICS_CALCULATED = "economics_calculated"
    THRESHOLD_EVALUATED = "threshold_evaluated"


class FundingCarryRejectCode(StrEnum):
    MISSING_RECEIVE_LEG = "missing_receive_leg"
    MISSING_PAY_LEG = "missing_pay_leg"
    MISSING_FUNDING_CURRENT = "missing_funding_current"
    MISSING_ORDERBOOK_SNAPSHOT = "missing_orderbook_snapshot"
    STALE_DATA = "stale_data"
    FUTURE_TIMESTAMP = "future_timestamp"
    INSTRUMENT_MISMATCH = "instrument_mismatch"
    VENUE_MISMATCH = "venue_mismatch"
    UNKNOWN_FUNDING_UNIT = "unknown_funding_unit"
    UNKNOWN_FUNDING_INTERVAL = "unknown_funding_interval"
    INVALID_BID_ASK = "invalid_bid_ask"
    CROSSED_ORDERBOOK = "crossed_orderbook"
    EMPTY_USABLE_DEPTH = "empty_usable_depth"
    UNSYNCHRONIZED_VENUE_TIMESTAMPS = "unsynchronized_venue_timestamps"
    DUPLICATE_SOURCE_EVENT = "duplicate_source_event"
    NON_FINITE_NUMERIC_VALUE = "non_finite_numeric_value"
    EDGE_CALCULATION_FAILURE = "edge_calculation_failure"
    INSUFFICIENT_ORDERBOOK_DEPTH = "insufficient_orderbook_depth"
    UNKNOWN_FEE = "unknown_fee"
    EDGE_BELOW_THRESHOLD = "edge_below_threshold"
    MISSING_HYPERLIQUID_FUNDING_CURRENT = "missing_hyperliquid_funding_current"
    MISSING_BITGET_FUNDING_CURRENT = "missing_bitget_funding_current"
    MISSING_HYPERLIQUID_ORDERBOOK_SNAPSHOT = "missing_hyperliquid_orderbook_snapshot"
    MISSING_BITGET_ORDERBOOK_SNAPSHOT = "missing_bitget_orderbook_snapshot"
    BITGET_ORDERBOOK_STATE_NOT_INITIALIZED = "bitget_orderbook_state_not_initialized"
    BITGET_ORDERBOOK_SEQUENCE_GAP = "bitget_orderbook_sequence_gap"
    BITGET_ORDERBOOK_STATE_INVALID = "bitget_orderbook_state_invalid"
    HYPERLIQUID_FUNDING_STALE = "hyperliquid_funding_stale"
    BITGET_FUNDING_STALE = "bitget_funding_stale"
    FUNDING_OBSERVATION_UNSYNCHRONIZED = "funding_observation_unsynchronized"
    HYPERLIQUID_ORDERBOOK_STALE = "hyperliquid_orderbook_stale"
    BITGET_ORDERBOOK_STALE = "bitget_orderbook_stale"
    ORDERBOOK_VENUES_UNSYNCHRONIZED = "orderbook_venues_unsynchronized"


@dataclass(frozen=True)
class FundingObservation:
    event_id: str
    venue: str
    instrument: str
    raw_funding_rate: Decimal
    funding_unit: str
    funding_interval_seconds: int | None
    next_funding_at: datetime | None
    source_timestamp: datetime
    received_at: datetime
    experimental: bool = True


@dataclass(frozen=True)
class OrderBookObservation:
    event_id: str
    venue: str
    instrument: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    source_timestamp: datetime
    received_at: datetime
    experimental: bool = True
    source_event_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class CanonicalFunding:
    raw_funding_rate: Decimal
    canonical_funding_rate: Decimal
    funding_interval_seconds: int
    funding_rate_per_hour: Decimal
    next_funding_at: datetime | None
    source_timestamp: datetime
    received_at: datetime


@dataclass(frozen=True)
class FundingCarryShadowConfig:
    instrument: str = FUNDING_CARRY_DEFAULT_SHADOW_INSTRUMENT
    venues: tuple[str, str] = ("hyperliquid", "bitget")
    shadow_notional: Decimal = Decimal("10")
    minimum_net_edge: Decimal = Decimal("0")
    maximum_age_seconds: int = 30
    maximum_future_seconds: int = 1
    funding_max_age_seconds: int = 40
    funding_max_observation_skew_seconds: int = 30
    maximum_orderbook_venue_skew_seconds: int = 5
    # Legacy configuration retained for compatibility; no 4-way skew is evaluated.
    maximum_venue_timestamp_skew_seconds: int = 5
    economics_currency: str = "USD"
    evaluation_horizon_seconds: int = 3600
    venue_taker_fee_rates: tuple[tuple[str, Decimal], ...] = (
        ("hyperliquid", Decimal("0.0006")),
        ("bitget", Decimal("0.0006")),
    )

    def fee_rate(self, venue: str) -> Decimal | None:
        return dict(self.venue_taker_fee_rates).get(venue)


@dataclass(frozen=True)
class FundingCarryEconomics:
    """Immutable economics snapshot committed before the threshold decision.

    Funding edge is a fraction/hour. Cashflow, fees, slippage, basis and
    round-trip cost are quote-currency amounts. Expected net edge is the net
    quote-currency income divided by notional over evaluation_horizon_seconds.
    """

    long_venue: str
    short_venue: str
    receive_leg: str
    pay_leg: str
    funding_timestamps: tuple[tuple[str, datetime], ...]
    orderbook_timestamps: tuple[tuple[str, datetime], ...]
    raw_funding_rates: tuple[tuple[str, Decimal], ...]
    canonical_funding_rates: tuple[tuple[str, Decimal], ...]
    funding_intervals: tuple[tuple[str, int], ...]
    funding_rates_per_hour: tuple[tuple[str, Decimal], ...]
    long_entry_vwap: Decimal
    short_entry_vwap: Decimal
    long_available_quantity: Decimal
    short_available_quantity: Decimal
    long_slippage_bps: Decimal
    short_slippage_bps: Decimal
    gross_funding_edge_per_hour: Decimal
    gross_funding_cashflow_per_hour: Decimal
    expected_funding_income: Decimal
    long_entry_fee: Decimal
    short_entry_fee: Decimal
    entry_fee_total: Decimal
    estimated_exit_fee: Decimal
    long_entry_slippage: Decimal
    short_entry_slippage: Decimal
    entry_slippage_total: Decimal
    estimated_exit_slippage: Decimal
    basis_cost: Decimal
    round_trip_cost: Decimal
    expected_net_income: Decimal
    expected_net_edge: Decimal
    break_even_holding_hours: Decimal | None
    economics_currency: str
    notional: Decimal
    evaluation_horizon_seconds: int
    funding_income_horizon: str


@dataclass(frozen=True)
class FundingCarryShadowCandidate:
    candidate_id: str
    run_id: str
    strategy_id: str
    instrument: str
    long_venue: str | None
    short_venue: str | None
    receive_leg: str | None
    pay_leg: str | None
    source_event_ids: tuple[str, ...]
    funding_timestamps: tuple[tuple[str, datetime], ...]
    orderbook_timestamps: tuple[tuple[str, datetime], ...]
    raw_funding_rates: tuple[tuple[str, Decimal], ...]
    canonical_funding_rates: tuple[tuple[str, Decimal], ...]
    funding_intervals: tuple[tuple[str, int], ...]
    funding_rates_per_hour: tuple[tuple[str, Decimal], ...]
    long_entry_vwap: Decimal | None
    short_entry_vwap: Decimal | None
    long_available_quantity: Decimal | None
    short_available_quantity: Decimal | None
    long_slippage_bps: Decimal | None
    short_slippage_bps: Decimal | None
    gross_funding_edge: Decimal | None
    expected_funding_income: Decimal | None
    long_entry_fee: Decimal | None
    short_entry_fee: Decimal | None
    estimated_exit_fee: Decimal | None
    long_entry_slippage: Decimal | None
    short_entry_slippage: Decimal | None
    estimated_exit_slippage: Decimal | None
    entry_basis_cost: Decimal | None
    expected_net_edge: Decimal | None
    disposition: ShadowDisposition
    rejection_reason: FundingCarryRejectCode | None
    created_at: datetime
    code_commit_sha: str
    config_sha: str
    missing_venue: str | None = None
    missing_capability: str | None = None
    last_seen_event_at: datetime | None = None
    last_valid_event_at: datetime | None = None
    source_event_count: int = 0
    venue_timestamp_skew_seconds: Decimal | None = None
    hyperliquid_funding_age_seconds: Decimal | None = None
    bitget_funding_age_seconds: Decimal | None = None
    funding_observation_skew_seconds: Decimal | None = None
    hyperliquid_orderbook_age_seconds: Decimal | None = None
    bitget_orderbook_age_seconds: Decimal | None = None
    orderbook_venue_skew_seconds: Decimal | None = None
    funding_freshness_pass: bool | None = None
    orderbook_freshness_pass: bool | None = None
    orderbook_synchronization_pass: bool | None = None
    evaluation_stage: FundingCarryEvaluationStage = FundingCarryEvaluationStage.INPUT_VALIDATION
    economics_calculated: bool = False
    economics_currency: str = "USD"
    economics_notional: Decimal | None = None
    evaluation_horizon_seconds: int | None = None
    funding_income_horizon: str | None = None
    gross_funding_edge_per_hour: Decimal | None = None
    gross_funding_cashflow_per_hour: Decimal | None = None
    entry_fee_total: Decimal | None = None
    entry_slippage_total: Decimal | None = None
    round_trip_cost: Decimal | None = None
    expected_net_income: Decimal | None = None
    break_even_holding_hours: Decimal | None = None


def _identity(strategy_id: str, instrument: str, source_event_ids: Iterable[str]) -> str:
    value = {
        "strategy_id": strategy_id,
        "instrument": instrument,
        "source_event_ids": sorted(source_event_ids),
    }
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _finite(value: Decimal) -> bool:
    return value.is_finite() and math.isfinite(float(value))


def normalize_funding(observation: FundingObservation) -> CanonicalFunding:
    if observation.funding_unit != "fraction_per_interval":
        raise ValueError(FundingCarryRejectCode.UNKNOWN_FUNDING_UNIT)
    interval = observation.funding_interval_seconds
    if interval is None or interval <= 0:
        raise ValueError(FundingCarryRejectCode.UNKNOWN_FUNDING_INTERVAL)
    if observation.venue not in FUNDING_CARRY_REQUIRED_VENUES:
        raise ValueError(FundingCarryRejectCode.VENUE_MISMATCH)
    rate = observation.raw_funding_rate
    if not _finite(rate):
        raise ValueError(FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE)
    # Both public adapters expose positive funding as long-pays-short. Keeping the
    # mapping explicit prevents raw venue semantics from leaking into the strategy.
    sign = {"hyperliquid": Decimal("1"), "bitget": Decimal("1")}[observation.venue]
    canonical = rate * sign
    return CanonicalFunding(
        raw_funding_rate=rate,
        canonical_funding_rate=canonical,
        funding_interval_seconds=interval,
        funding_rate_per_hour=canonical * Decimal(3600) / Decimal(interval),
        next_funding_at=observation.next_funding_at,
        source_timestamp=observation.source_timestamp,
        received_at=observation.received_at,
    )


def _vwap(
    levels: tuple[tuple[Decimal, Decimal], ...],
    notional: Decimal,
    *,
    buying: bool,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    if not levels:
        raise ValueError(FundingCarryRejectCode.EMPTY_USABLE_DEPTH)
    if any(not _finite(value) for level in levels for value in level):
        raise ValueError(FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE)
    usable = tuple((price, size) for price, size in levels if price > 0 and size > 0)
    if not usable:
        raise ValueError(FundingCarryRejectCode.EMPTY_USABLE_DEPTH)
    remaining = notional
    cost = Decimal("0")
    filled = Decimal("0")
    for price, size in usable:
        take = min(size, remaining / price)
        cost += take * price
        filled += take
        remaining -= take * price
        if remaining <= Decimal("0.000000000001"):
            break
    if remaining > Decimal("0.000000000001") or filled <= 0:
        raise ValueError(FundingCarryRejectCode.INSUFFICIENT_ORDERBOOK_DEPTH)
    vwap = cost / filled
    best = usable[0][0]
    slippage_bps = ((vwap / best - 1) if buying else (best / vwap - 1)) * Decimal(10000)
    available = sum((size for _, size in usable), Decimal("0"))
    return vwap, filled, available, slippage_bps


class FundingCarryShadowEvaluator:
    def __init__(self, config: FundingCarryShadowConfig) -> None:
        self.config = config

    def evaluate(
        self,
        *,
        run_id: str,
        funding: tuple[FundingObservation, ...],
        orderbooks: tuple[OrderBookObservation, ...],
        now: datetime,
        code_commit_sha: str,
        config_sha: str,
    ) -> FundingCarryShadowCandidate:
        all_inputs: tuple[FundingObservation | OrderBookObservation, ...] = funding + orderbooks
        source_ids = tuple(
            source_id
            for item in all_inputs
            for source_id in (
                item.source_event_ids
                if isinstance(item, OrderBookObservation) and item.source_event_ids
                else (item.event_id,)
            )
        )
        candidate_id = _identity(FUNDING_CARRY_STRATEGY_ID, self.config.instrument, source_ids)
        stage = FundingCarryEvaluationStage.INPUT_VALIDATION
        try:
            self._validate_inputs(funding, orderbooks, now)
            stage = FundingCarryEvaluationStage.FUNDING_NORMALIZATION
            normalized = {item.venue: normalize_funding(item) for item in funding}
            books = {item.venue: item for item in orderbooks}
            ordered = sorted(normalized, key=lambda venue: normalized[venue].funding_rate_per_hour)
            long_venue, short_venue = ordered[0], ordered[-1]
            long_rate = normalized[long_venue]
            short_rate = normalized[short_venue]
            long_book, short_book = books[long_venue], books[short_venue]
            stage = FundingCarryEvaluationStage.BOOK_PRICING
            long_vwap, long_qty, long_available, long_bps = _vwap(
                long_book.asks, self.config.shadow_notional, buying=True
            )
            short_vwap, short_qty, short_available, short_bps = _vwap(
                short_book.bids, self.config.shadow_notional, buying=False
            )
            long_fee_rate = self.config.fee_rate(long_venue)
            short_fee_rate = self.config.fee_rate(short_venue)
            if long_fee_rate is None or short_fee_rate is None:
                raise ValueError(FundingCarryRejectCode.UNKNOWN_FEE)
            gross_edge = short_rate.funding_rate_per_hour - long_rate.funding_rate_per_hour
            gross_cashflow_per_hour = gross_edge * self.config.shadow_notional
            funding_income = (
                gross_cashflow_per_hour
                * Decimal(self.config.evaluation_horizon_seconds)
                / Decimal(3600)
            )
            long_fee = self.config.shadow_notional * long_fee_rate
            short_fee = self.config.shadow_notional * short_fee_rate
            entry_fee_total = long_fee + short_fee
            exit_fee = long_fee + short_fee
            long_slippage = long_qty * (long_vwap - long_book.asks[0][0])
            short_slippage = short_qty * (short_book.bids[0][0] - short_vwap)
            entry_slippage_total = long_slippage + short_slippage
            exit_slippage = long_slippage + short_slippage
            midpoint = (long_vwap + short_vwap) / Decimal(2)
            basis_cost = (long_vwap - short_vwap) / midpoint * self.config.shadow_notional
            round_trip_cost = (
                entry_fee_total + exit_fee + entry_slippage_total + exit_slippage + basis_cost
            )
            net_income = funding_income - round_trip_cost
            net_edge = net_income / self.config.shadow_notional
            break_even = (
                max(round_trip_cost, Decimal("0")) / gross_cashflow_per_hour
                if gross_cashflow_per_hour > 0
                else None
            )
            if not all(
                _finite(value)
                for value in (
                    gross_edge,
                    gross_cashflow_per_hour,
                    funding_income,
                    long_fee,
                    short_fee,
                    entry_fee_total,
                    exit_fee,
                    long_slippage,
                    short_slippage,
                    entry_slippage_total,
                    exit_slippage,
                    basis_cost,
                    round_trip_cost,
                    net_income,
                    net_edge,
                    *(value for value in (break_even,) if value is not None),
                )
            ):
                raise ValueError(FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE)
            economics = FundingCarryEconomics(
                long_venue=long_venue,
                short_venue=short_venue,
                receive_leg=short_venue,
                pay_leg=long_venue,
                funding_timestamps=tuple(
                    sorted((venue, value.source_timestamp) for venue, value in normalized.items())
                ),
                orderbook_timestamps=tuple(
                    sorted((venue, value.source_timestamp) for venue, value in books.items())
                ),
                raw_funding_rates=tuple(
                    sorted((venue, value.raw_funding_rate) for venue, value in normalized.items())
                ),
                canonical_funding_rates=tuple(
                    sorted(
                        (venue, value.canonical_funding_rate) for venue, value in normalized.items()
                    )
                ),
                funding_intervals=tuple(
                    sorted(
                        (venue, value.funding_interval_seconds)
                        for venue, value in normalized.items()
                    )
                ),
                funding_rates_per_hour=tuple(
                    sorted(
                        (venue, value.funding_rate_per_hour) for venue, value in normalized.items()
                    )
                ),
                long_entry_vwap=long_vwap,
                short_entry_vwap=short_vwap,
                long_available_quantity=long_available,
                short_available_quantity=short_available,
                long_slippage_bps=long_bps,
                short_slippage_bps=short_bps,
                gross_funding_edge_per_hour=gross_edge,
                gross_funding_cashflow_per_hour=gross_cashflow_per_hour,
                expected_funding_income=funding_income,
                long_entry_fee=long_fee,
                short_entry_fee=short_fee,
                entry_fee_total=entry_fee_total,
                estimated_exit_fee=exit_fee,
                long_entry_slippage=long_slippage,
                short_entry_slippage=short_slippage,
                entry_slippage_total=entry_slippage_total,
                estimated_exit_slippage=exit_slippage,
                basis_cost=basis_cost,
                round_trip_cost=round_trip_cost,
                expected_net_income=net_income,
                expected_net_edge=net_edge,
                break_even_holding_hours=break_even,
                economics_currency=self.config.economics_currency,
                notional=self.config.shadow_notional,
                evaluation_horizon_seconds=self.config.evaluation_horizon_seconds,
                funding_income_horizon="configured_evaluation_horizon",
            )
            stage = FundingCarryEvaluationStage.ECONOMICS_CALCULATED
            disposition = ShadowDisposition.CANDIDATE
            reason = None
            if economics.expected_net_edge < self.config.minimum_net_edge:
                disposition = ShadowDisposition.REJECTED
                reason = FundingCarryRejectCode.EDGE_BELOW_THRESHOLD
            stage = FundingCarryEvaluationStage.THRESHOLD_EVALUATED
            return self._candidate_from_economics(
                candidate_id=candidate_id,
                run_id=run_id,
                source_ids=source_ids,
                now=now,
                code_commit_sha=code_commit_sha,
                config_sha=config_sha,
                economics=economics,
                disposition=disposition,
                rejection_reason=reason,
            )
        except (ArithmeticError, InvalidOperation, ValueError) as exc:
            reason = (
                exc.args[0]
                if exc.args and isinstance(exc.args[0], FundingCarryRejectCode)
                else FundingCarryRejectCode.EDGE_CALCULATION_FAILURE
            )
            return self._pre_economics_rejection(
                candidate_id=candidate_id,
                run_id=run_id,
                source_ids=source_ids,
                now=now,
                code_commit_sha=code_commit_sha,
                config_sha=config_sha,
                stage=stage,
                rejection_reason=reason,
            )

    def _candidate_from_economics(
        self,
        *,
        candidate_id: str,
        run_id: str,
        source_ids: tuple[str, ...],
        now: datetime,
        code_commit_sha: str,
        config_sha: str,
        economics: FundingCarryEconomics,
        disposition: ShadowDisposition,
        rejection_reason: FundingCarryRejectCode | None,
    ) -> FundingCarryShadowCandidate:
        return FundingCarryShadowCandidate(
            candidate_id=candidate_id,
            run_id=run_id,
            strategy_id=FUNDING_CARRY_STRATEGY_ID,
            instrument=self.config.instrument,
            source_event_ids=tuple(sorted(source_ids)),
            created_at=now,
            code_commit_sha=code_commit_sha,
            config_sha=config_sha,
            long_venue=economics.long_venue,
            short_venue=economics.short_venue,
            receive_leg=economics.receive_leg,
            pay_leg=economics.pay_leg,
            funding_timestamps=economics.funding_timestamps,
            orderbook_timestamps=economics.orderbook_timestamps,
            raw_funding_rates=economics.raw_funding_rates,
            canonical_funding_rates=economics.canonical_funding_rates,
            funding_intervals=economics.funding_intervals,
            funding_rates_per_hour=economics.funding_rates_per_hour,
            long_entry_vwap=economics.long_entry_vwap,
            short_entry_vwap=economics.short_entry_vwap,
            long_available_quantity=economics.long_available_quantity,
            short_available_quantity=economics.short_available_quantity,
            long_slippage_bps=economics.long_slippage_bps,
            short_slippage_bps=economics.short_slippage_bps,
            gross_funding_edge=economics.gross_funding_edge_per_hour,
            expected_funding_income=economics.expected_funding_income,
            long_entry_fee=economics.long_entry_fee,
            short_entry_fee=economics.short_entry_fee,
            estimated_exit_fee=economics.estimated_exit_fee,
            long_entry_slippage=economics.long_entry_slippage,
            short_entry_slippage=economics.short_entry_slippage,
            estimated_exit_slippage=economics.estimated_exit_slippage,
            entry_basis_cost=economics.basis_cost,
            expected_net_edge=economics.expected_net_edge,
            disposition=disposition,
            rejection_reason=rejection_reason,
            evaluation_stage=FundingCarryEvaluationStage.THRESHOLD_EVALUATED,
            economics_calculated=True,
            economics_currency=economics.economics_currency,
            economics_notional=economics.notional,
            evaluation_horizon_seconds=economics.evaluation_horizon_seconds,
            funding_income_horizon=economics.funding_income_horizon,
            gross_funding_edge_per_hour=economics.gross_funding_edge_per_hour,
            gross_funding_cashflow_per_hour=economics.gross_funding_cashflow_per_hour,
            entry_fee_total=economics.entry_fee_total,
            entry_slippage_total=economics.entry_slippage_total,
            round_trip_cost=economics.round_trip_cost,
            expected_net_income=economics.expected_net_income,
            break_even_holding_hours=economics.break_even_holding_hours,
        )

    def _pre_economics_rejection(
        self,
        *,
        candidate_id: str,
        run_id: str,
        source_ids: tuple[str, ...],
        now: datetime,
        code_commit_sha: str,
        config_sha: str,
        stage: FundingCarryEvaluationStage,
        rejection_reason: FundingCarryRejectCode,
    ) -> FundingCarryShadowCandidate:
        return FundingCarryShadowCandidate(
            candidate_id=candidate_id,
            run_id=run_id,
            strategy_id=FUNDING_CARRY_STRATEGY_ID,
            instrument=self.config.instrument,
            source_event_ids=tuple(sorted(source_ids)),
            created_at=now,
            code_commit_sha=code_commit_sha,
            config_sha=config_sha,
            long_venue=None,
            short_venue=None,
            receive_leg=None,
            pay_leg=None,
            funding_timestamps=(),
            orderbook_timestamps=(),
            raw_funding_rates=(),
            canonical_funding_rates=(),
            funding_intervals=(),
            funding_rates_per_hour=(),
            long_entry_vwap=None,
            short_entry_vwap=None,
            long_available_quantity=None,
            short_available_quantity=None,
            long_slippage_bps=None,
            short_slippage_bps=None,
            gross_funding_edge=None,
            expected_funding_income=None,
            long_entry_fee=None,
            short_entry_fee=None,
            estimated_exit_fee=None,
            long_entry_slippage=None,
            short_entry_slippage=None,
            estimated_exit_slippage=None,
            entry_basis_cost=None,
            expected_net_edge=None,
            disposition=ShadowDisposition.REJECTED,
            rejection_reason=rejection_reason,
            evaluation_stage=stage,
            economics_calculated=False,
            economics_currency=self.config.economics_currency,
            economics_notional=self.config.shadow_notional,
            evaluation_horizon_seconds=self.config.evaluation_horizon_seconds,
            funding_income_horizon="configured_evaluation_horizon",
        )

    def _validate_inputs(
        self,
        funding: tuple[FundingObservation, ...],
        orderbooks: tuple[OrderBookObservation, ...],
        now: datetime,
    ) -> None:
        if self.config.instrument not in FUNDING_CARRY_SHADOW_ALLOWED_INSTRUMENTS:
            raise ValueError(FundingCarryRejectCode.INSTRUMENT_MISMATCH)
        if tuple(self.config.venues) != FUNDING_CARRY_REQUIRED_VENUES:
            raise ValueError(FundingCarryRejectCode.VENUE_MISMATCH)
        all_inputs: tuple[FundingObservation | OrderBookObservation, ...] = funding + orderbooks
        if len({item.event_id for item in all_inputs}) != len(all_inputs):
            raise ValueError(FundingCarryRejectCode.DUPLICATE_SOURCE_EVENT)
        if any(item.instrument != self.config.instrument for item in all_inputs):
            raise ValueError(FundingCarryRejectCode.INSTRUMENT_MISMATCH)
        if any(item.venue not in self.config.venues for item in all_inputs):
            raise ValueError(FundingCarryRejectCode.VENUE_MISMATCH)
        funding_venues = {item.venue for item in funding}
        book_venues = {item.venue for item in orderbooks}
        if not funding:
            raise ValueError(FundingCarryRejectCode.MISSING_FUNDING_CURRENT)
        if not orderbooks:
            raise ValueError(FundingCarryRejectCode.MISSING_ORDERBOOK_SNAPSHOT)
        first, second = self.config.venues
        if first not in funding_venues or first not in book_venues:
            raise ValueError(FundingCarryRejectCode.MISSING_RECEIVE_LEG)
        if second not in funding_venues or second not in book_venues:
            raise ValueError(FundingCarryRejectCode.MISSING_PAY_LEG)
        if len(funding) != 2:
            raise ValueError(FundingCarryRejectCode.MISSING_FUNDING_CURRENT)
        if len(orderbooks) != 2:
            raise ValueError(FundingCarryRejectCode.MISSING_ORDERBOOK_SNAPSHOT)
        maximum_funding_age = timedelta(seconds=self.config.funding_max_age_seconds)
        maximum_orderbook_age = timedelta(seconds=self.config.maximum_age_seconds)
        maximum_future = timedelta(seconds=self.config.maximum_future_seconds)
        for item in all_inputs:
            if item.source_timestamp.tzinfo is None or item.received_at.tzinfo is None:
                raise ValueError(FundingCarryRejectCode.FUTURE_TIMESTAMP)
            source = item.source_timestamp.astimezone(UTC)
            if source > now + maximum_future:
                raise ValueError(FundingCarryRejectCode.FUTURE_TIMESTAMP)
        funding_by_venue = {item.venue: item for item in funding}
        books_by_venue = {item.venue: item for item in orderbooks}
        for venue, reason in (
            ("hyperliquid", FundingCarryRejectCode.HYPERLIQUID_FUNDING_STALE),
            ("bitget", FundingCarryRejectCode.BITGET_FUNDING_STALE),
        ):
            if now - funding_by_venue[venue].source_timestamp > maximum_funding_age:
                raise ValueError(reason)
        if abs(
            funding_by_venue["hyperliquid"].source_timestamp
            - funding_by_venue["bitget"].source_timestamp
        ) > timedelta(seconds=self.config.funding_max_observation_skew_seconds):
            raise ValueError(FundingCarryRejectCode.FUNDING_OBSERVATION_UNSYNCHRONIZED)
        for venue, reason in (
            ("hyperliquid", FundingCarryRejectCode.HYPERLIQUID_ORDERBOOK_STALE),
            ("bitget", FundingCarryRejectCode.BITGET_ORDERBOOK_STALE),
        ):
            if now - books_by_venue[venue].source_timestamp > maximum_orderbook_age:
                raise ValueError(reason)
        if abs(
            books_by_venue["hyperliquid"].source_timestamp
            - books_by_venue["bitget"].source_timestamp
        ) > timedelta(seconds=self.config.maximum_orderbook_venue_skew_seconds):
            raise ValueError(FundingCarryRejectCode.ORDERBOOK_VENUES_UNSYNCHRONIZED)
        for book in orderbooks:
            if not book.bids or not book.asks:
                raise ValueError(FundingCarryRejectCode.EMPTY_USABLE_DEPTH)
            bid, ask = book.bids[0][0], book.asks[0][0]
            if not _finite(bid) or not _finite(ask):
                raise ValueError(FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE)
            if bid <= 0 or ask <= 0:
                raise ValueError(FundingCarryRejectCode.INVALID_BID_ASK)
            if bid >= ask:
                raise ValueError(FundingCarryRejectCode.CROSSED_ORDERBOOK)


def candidate_payload(candidate: FundingCarryShadowCandidate) -> dict[str, object]:
    return asdict(candidate)
