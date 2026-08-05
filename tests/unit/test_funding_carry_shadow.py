from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine

from app.config.settings import Settings
from app.domain.strategies.capabilities import (
    FUNDING_CARRY_NON_BLOCKING_CAPABILITIES,
    FUNDING_CARRY_REQUIRED_CAPABILITIES,
    FUNDING_CARRY_REQUIREMENT,
    STRATEGY_CAPABILITY_REGISTRY,
)
from app.domain.strategies.funding_carry import (
    FundingCarryRejectCode,
    FundingCarryShadowConfig,
    FundingCarryShadowEvaluator,
    FundingObservation,
    OrderBookObservation,
    ShadowDisposition,
    normalize_funding,
)
from app.infrastructure.database.models import Base
from app.services.operations.models import LiveSignalInput, OperationMode
from app.services.operations.repository import (
    InMemoryOperationalRepository,
    PostgreSQLOperationalRepository,
)
from app.services.operations.service import ContinuousResearchPaperService
from app.services.research.certification import (
    InMemoryCertificationRepository,
    StrictPaperReadiness,
    evaluate_strict_paper_readiness,
)

NOW = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)


def funding(
    venue: str,
    rate: str,
    *,
    interval: int | None = 3600,
    timestamp: datetime = NOW,
    event_id: str | None = None,
    instrument: str = "BTC",
    unit: str = "fraction_per_interval",
) -> FundingObservation:
    return FundingObservation(
        event_id=event_id or f"{venue}-funding",
        venue=venue,
        instrument=instrument,
        raw_funding_rate=Decimal(rate),
        funding_unit=unit,
        funding_interval_seconds=interval,
        next_funding_at=NOW + timedelta(hours=1),
        source_timestamp=timestamp,
        received_at=timestamp,
    )


def book(
    venue: str,
    *,
    bids: tuple[tuple[Decimal, Decimal], ...] | None = None,
    asks: tuple[tuple[Decimal, Decimal], ...] | None = None,
    timestamp: datetime = NOW,
    event_id: str | None = None,
    instrument: str = "BTC",
) -> OrderBookObservation:
    return OrderBookObservation(
        event_id=event_id or f"{venue}-book",
        venue=venue,
        instrument=instrument,
        bids=(
            bids
            if bids is not None
            else ((Decimal("99"), Decimal("0.05")), (Decimal("98"), Decimal("1")))
        ),
        asks=(
            asks
            if asks is not None
            else ((Decimal("100"), Decimal("0.05")), (Decimal("101"), Decimal("1")))
        ),
        source_timestamp=timestamp,
        received_at=timestamp,
    )


def evaluate(
    *,
    hyperliquid_rate: str = "0.001",
    bitget_rate: str = "0.0001",
    hyperliquid_interval: int | None = 3600,
    bitget_interval: int | None = 3600,
    books: tuple[OrderBookObservation, ...] | None = None,
    fundings: tuple[FundingObservation, ...] | None = None,
    config: FundingCarryShadowConfig | None = None,
):
    return FundingCarryShadowEvaluator(config or FundingCarryShadowConfig()).evaluate(
        run_id="shadow-run",
        funding=fundings
        if fundings is not None
        else (
            funding("hyperliquid", hyperliquid_rate, interval=hyperliquid_interval),
            funding("bitget", bitget_rate, interval=bitget_interval),
        ),
        orderbooks=books if books is not None else (book("hyperliquid"), book("bitget")),
        now=NOW,
        code_commit_sha="a" * 40,
        config_sha="b" * 64,
    )


def test_canonical_requirement_has_one_source_of_truth() -> None:
    assert FUNDING_CARRY_REQUIREMENT.strategy_id == "funding_carry"
    assert FUNDING_CARRY_REQUIREMENT.required_capabilities == (
        "funding_current",
        "orderbook_snapshot",
    )
    assert FUNDING_CARRY_REQUIREMENT.required_capabilities == FUNDING_CARRY_REQUIRED_CAPABILITIES
    assert set(FUNDING_CARRY_NON_BLOCKING_CAPABILITIES) == {
        "funding_history",
        "mark_price",
        "index_price",
        "open_interest",
        "trade",
        "ohlcv",
    }


def test_snapshot_and_continuous_operation_use_same_requirement() -> None:
    gateway = STRATEGY_CAPABILITY_REGISTRY[("funding_carry", "1")]
    assert {item.capability for item in gateway} == set(
        FUNDING_CARRY_REQUIREMENT.required_capabilities
    )


@pytest.mark.parametrize(
    ("left", "right", "expected_long", "expected_short"),
    [
        ("0.001", "0.0001", "bitget", "hyperliquid"),
        ("-0.0001", "-0.001", "bitget", "hyperliquid"),
    ],
)
def test_positive_and_negative_funding_select_correct_legs(
    left: str, right: str, expected_long: str, expected_short: str
) -> None:
    result = evaluate(hyperliquid_rate=left, bitget_rate=right)
    assert result.disposition is ShadowDisposition.CANDIDATE
    assert (result.long_venue, result.short_venue) == (expected_long, expected_short)
    assert (result.pay_leg, result.receive_leg) == (expected_long, expected_short)
    assert result.gross_funding_edge is not None and result.gross_funding_edge > 0


def test_different_funding_intervals_are_normalized_per_hour() -> None:
    result = evaluate(
        hyperliquid_rate="0.0002",
        hyperliquid_interval=3600,
        bitget_rate="0.0008",
        bitget_interval=28800,
    )
    assert dict(result.funding_rates_per_hour) == {
        "bitget": Decimal("0.0001"),
        "hyperliquid": Decimal("0.0002"),
    }
    assert result.long_venue == "bitget"


@pytest.mark.parametrize("venue", ["hyperliquid", "bitget"])
def test_raw_venue_funding_sign_is_normalized(venue: str) -> None:
    result = normalize_funding(funding(venue, "0.001"))
    assert result.raw_funding_rate == result.canonical_funding_rate
    assert result.canonical_funding_rate > 0


@pytest.mark.parametrize(
    ("fundings", "books", "reason"),
    [
        (
            (
                funding("hyperliquid", "0.001", timestamp=NOW - timedelta(seconds=31)),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.STALE_DATA,
        ),
        (
            (
                funding("hyperliquid", "0.001", timestamp=NOW + timedelta(seconds=2)),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.FUTURE_TIMESTAMP,
        ),
        (
            (funding("hyperliquid", "0.001"),),
            None,
            FundingCarryRejectCode.MISSING_PAY_LEG,
        ),
        (
            (
                funding("hyperliquid", "0.001", instrument="ETH"),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.INSTRUMENT_MISMATCH,
        ),
        (
            None,
            (
                book(
                    "hyperliquid",
                    bids=((Decimal("101"), Decimal("1")),),
                    asks=((Decimal("100"), Decimal("1")),),
                ),
                book("bitget"),
            ),
            FundingCarryRejectCode.CROSSED_ORDERBOOK,
        ),
        (
            None,
            (
                book(
                    "hyperliquid",
                    bids=((Decimal("99"), Decimal("0.001")),),
                    asks=((Decimal("100"), Decimal("0.001")),),
                ),
                book("bitget"),
            ),
            FundingCarryRejectCode.INSUFFICIENT_ORDERBOOK_DEPTH,
        ),
    ],
)
def test_runtime_validation_rejects_invalid_inputs(
    fundings: tuple[FundingObservation, ...] | None,
    books: tuple[OrderBookObservation, ...] | None,
    reason: FundingCarryRejectCode,
) -> None:
    result = evaluate(fundings=fundings, books=books)
    assert result.disposition is ShadowDisposition.REJECTED
    assert result.rejection_reason is reason


@pytest.mark.parametrize(
    ("fundings", "books", "reason"),
    [
        ((), None, FundingCarryRejectCode.MISSING_FUNDING_CURRENT),
        (None, (), FundingCarryRejectCode.MISSING_ORDERBOOK_SNAPSHOT),
        (
            (
                funding("hyperliquid", "0.001", unit="percent"),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.UNKNOWN_FUNDING_UNIT,
        ),
        (
            (
                funding("hyperliquid", "0.001", interval=None),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.UNKNOWN_FUNDING_INTERVAL,
        ),
        (
            None,
            (
                book(
                    "hyperliquid",
                    bids=((Decimal("0"), Decimal("1")),),
                    asks=((Decimal("100"), Decimal("1")),),
                ),
                book("bitget"),
            ),
            FundingCarryRejectCode.INVALID_BID_ASK,
        ),
        (
            None,
            (
                book("hyperliquid", bids=(), asks=()),
                book("bitget"),
            ),
            FundingCarryRejectCode.EMPTY_USABLE_DEPTH,
        ),
        (
            (
                funding("hyperliquid", "0.001", timestamp=NOW - timedelta(seconds=6)),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.UNSYNCHRONIZED_VENUE_TIMESTAMPS,
        ),
        (
            (
                funding("hyperliquid", "0.001", event_id="duplicate"),
                funding("bitget", "0.0001", event_id="duplicate"),
            ),
            None,
            FundingCarryRejectCode.DUPLICATE_SOURCE_EVENT,
        ),
        (
            (
                funding("hyperliquid", "NaN"),
                funding("bitget", "0.0001"),
            ),
            None,
            FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE,
        ),
    ],
)
def test_machine_readable_rejection_reason_codes_are_recorded(
    fundings: tuple[FundingObservation, ...] | None,
    books: tuple[OrderBookObservation, ...] | None,
    reason: FundingCarryRejectCode,
) -> None:
    result = evaluate(fundings=fundings, books=books)
    assert result.rejection_reason is reason
    assert result.rejection_reason.value == str(reason)


def test_vwap_uses_configured_notional_and_depth() -> None:
    result = evaluate()
    assert result.long_entry_vwap is not None
    assert result.short_entry_vwap is not None
    assert result.long_entry_vwap > Decimal("100")
    assert result.short_entry_vwap < Decimal("99")
    assert result.long_slippage_bps is not None and result.long_slippage_bps > 0
    assert result.short_slippage_bps is not None and result.short_slippage_bps > 0


def test_fees_slippage_and_basis_are_included_in_net_edge() -> None:
    result = evaluate()
    assert result.expected_funding_income is not None
    assert result.expected_net_edge is not None
    total_cost = sum(
        value
        for value in (
            result.long_entry_fee,
            result.short_entry_fee,
            result.estimated_exit_fee,
            result.long_entry_slippage,
            result.short_entry_slippage,
            result.estimated_exit_slippage,
            result.entry_basis_cost,
        )
        if value is not None
    )
    assert result.expected_net_edge == (result.expected_funding_income - total_cost) / Decimal("10")


def test_unknown_fee_is_not_treated_as_zero() -> None:
    config = replace(
        FundingCarryShadowConfig(),
        venue_taker_fee_rates=(("hyperliquid", Decimal("0.0006")),),
    )
    assert evaluate(config=config).rejection_reason is FundingCarryRejectCode.UNKNOWN_FEE


def test_edge_below_threshold_is_rejected() -> None:
    config = replace(FundingCarryShadowConfig(), minimum_net_edge=Decimal("1"))
    assert evaluate(config=config).rejection_reason is FundingCarryRejectCode.EDGE_BELOW_THRESHOLD


def test_experimental_evidence_creates_candidate_only_and_never_eligible() -> None:
    result = evaluate()
    assert result.disposition is ShadowDisposition.CANDIDATE
    assert all(value != "eligible" for value in ShadowDisposition)


def shadow_event(venue: str, event_type: str, *, rate: str | None = None) -> LiveSignalInput:
    return LiveSignalInput(
        event_id=f"{venue}-{event_type}",
        venue=venue,
        instrument="BTC",
        event_type=event_type,
        available_at=NOW,
        data_quality_score=1,
        capability_support="experimental",
        reconciliation_state="synchronized" if event_type.startswith("orderbook") else None,
        funding_rate=Decimal(rate) if rate else None,
        funding_unit="fraction_per_interval" if rate else None,
        funding_interval_seconds=3600 if rate else None,
        bids=((Decimal("99"), Decimal("1")),) if not rate else (),
        asks=((Decimal("100"), Decimal("1")),) if not rate else (),
        source_timestamp=NOW,
        received_at=NOW,
    )


def shadow_service() -> tuple[ContinuousResearchPaperService, InMemoryOperationalRepository]:
    repository = InMemoryOperationalRepository()
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        paper_trading=True,
        live_trading=False,
        paper={"enabled": True},
        live={"enabled": False, "adapter_name": "disabled", "allowed_symbols": ("BTC",)},
        continuous_paper={
            "enabled": True,
            "mode": "shadow",
            "observation_only": True,
            "venues": ("hyperliquid", "bitget"),
            "instruments": ("BTC",),
            "strategies": ("funding_carry",),
            "minimum_shadow_net_edge": -1,
        },
    )
    operation = ContinuousResearchPaperService(
        repository=repository,
        settings=settings,
        run_id="shadow-run",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )
    return operation, repository


def test_shadow_persists_deterministic_identity_without_duplicate_candidates() -> None:
    operation, repository = shadow_service()
    events = (
        shadow_event("hyperliquid", "funding_current", rate="0.001"),
        shadow_event("bitget", "funding_current", rate="0.0001"),
        shadow_event("hyperliquid", "orderbook_snapshot"),
        shadow_event("bitget", "orderbook_snapshot"),
    )
    first = operation.generate_funding_carry_shadow_candidate(events=events)
    second = operation.generate_funding_carry_shadow_candidate(events=tuple(reversed(events)))
    assert first.candidate_id == second.candidate_id
    assert len(first.candidate_id) == 64
    assert repository.shadow_candidates("shadow-run") == (first,)


def test_shadow_candidate_round_trips_through_durable_schema() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    repository = PostgreSQLOperationalRepository(engine)
    candidate = evaluate()
    assert repository.add_shadow_candidate(candidate)
    assert not repository.add_shadow_candidate(candidate)
    assert repository.shadow_candidates("shadow-run") == (candidate,)


def test_shadow_creates_zero_orders_fills_and_has_no_execution_adapter() -> None:
    operation, repository = shadow_service()
    assert not hasattr(operation, "execution_adapter")
    assert repository.orders("shadow-run") == ()
    assert repository.fills("shadow-run") == ()
    assert repository.signals("shadow-run") == ()


def test_sol_hype_gaps_do_not_block_btc_shadow_startup_and_safety_stays_off() -> None:
    operation, repository = shadow_service()
    assert operation.settings.continuous_paper.instruments == ("BTC",)
    assert operation.settings.live_trading is False
    assert operation.settings.live.enabled is False
    assert operation.settings.live.adapter_name == "disabled"
    assert repository.orders("shadow-run") == repository.fills("shadow-run") == ()
    certification_repository = InMemoryCertificationRepository()
    assert certification_repository.promotions == {}
    assert (
        evaluate_strict_paper_readiness(
            capabilities_live_verified=False,
            snapshot_eligible=False,
            research_completed=False,
            strategy_eligible=False,
            instrument_rules_complete=False,
            paper_risk_enabled=False,
            observation_candidate_exists=True,
        )
        is StrictPaperReadiness.NOT_READY
    )
