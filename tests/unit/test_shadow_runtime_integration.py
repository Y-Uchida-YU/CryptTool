from __future__ import annotations

import hashlib
import json
import zlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.adapters.exchanges.websocket import ReconciliationState
from app.config.settings import Settings
from app.domain.strategies.funding_carry import (
    FundingCarryRejectCode,
    FundingCarryShadowConfig,
    FundingCarryShadowEvaluator,
    FundingObservation,
    OrderBookObservation,
)
from app.infrastructure.database.models import (
    Base,
    DataSnapshotRow,
    PaperFillRow,
    PaperOrderRow,
    PaperSignalRow,
    ResearchRunRow,
)
from app.services.operations.models import OperationMode
from app.services.operations.provenance import resolve_commit_sha
from app.services.operations.repository import (
    InMemoryOperationalRepository,
    PostgreSQLOperationalRepository,
)
from app.services.operations.service import (
    ContinuousResearchPaperService,
    ResearchScheduler,
    SnapshotFinalizer,
)
from app.services.operations.shadow_artifacts import (
    ArtifactFinalizationError,
    ShadowRunArtifactWriter,
)
from app.services.operations.shadow_runtime import (
    BookStateStatus,
    CanonicalOrderBookStateBuilder,
    FundingCarryShadowInputSource,
)
from app.services.research.models import RawMarketEvent
from app.services.research.repository import InMemoryResearchRepository

NOW = datetime(2026, 8, 7, 3, 0, tzinfo=UTC)


def event(
    event_id: str,
    venue: str,
    event_type: str,
    payload: dict[str, object],
    *,
    sequence: int | None = None,
    epoch: int = 1,
    timestamp: datetime = NOW,
) -> RawMarketEvent:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return RawMarketEvent(
        event_id=event_id,
        venue=venue,
        canonical_instrument_id="BTC",
        venue_symbol="BTC" if venue == "hyperliquid" else "BTCUSDT",
        event_type=event_type,
        exchange_timestamp=timestamp,
        received_at=timestamp,
        available_at=timestamp,
        sequence=sequence,
        connection_id=None,
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        payload_sha256=hashlib.sha256(encoded.encode()).hexdigest(),
        raw_payload=encoded,
        normalizer_version="fixture-v1",
        capability_verification_run_id="unverified-experimental",
        created_at=timestamp,
        channel="orderbook" if event_type.startswith("orderbook") else "rest",
        connection_epoch=epoch,
    )


def book_payload(*, bid: str = "99", ask: str = "100") -> dict[str, object]:
    return {
        "bids": [[bid, "2"], [str(Decimal(bid) - 1), "3"]],
        "asks": [[ask, "2"], [str(Decimal(ask) + 1), "3"]],
    }


def funding_payload(rate: str, interval: int) -> dict[str, object]:
    return {
        "rate": rate,
        "funding_interval_seconds": interval,
        "next_funding_at": (NOW + timedelta(hours=1)).isoformat(),
    }


def complete_repository() -> InMemoryResearchRepository:
    repository = InMemoryResearchRepository()
    values = (
        event("hl-funding", "hyperliquid", "funding_current", funding_payload("0.001", 3600)),
        event("bg-funding", "bitget", "funding_current", funding_payload("0.0004", 28800)),
        event("hl-book", "hyperliquid", "orderbook_snapshot", book_payload(), sequence=10),
        event("bg-book", "bitget", "orderbook_snapshot", book_payload(), sequence=20),
    )
    for value in values:
        assert repository.add_experimental_event(value, "implemented")
    return repository


def test_shadow_input_reads_experimental_market_events_not_production_raw_events() -> None:
    repository = complete_repository()
    production = event(
        "production-funding",
        "hyperliquid",
        "funding_current",
        funding_payload("9", 3600),
    )
    assert repository.add_raw_event(production)
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert batch.matched
    assert {item.source_table for item in batch.events} == {"experimental_market_events"}
    assert "production-funding" not in {item.event_id for item in batch.events}
    assert {item.event_id for item in batch.events if item.event_type == "funding_current"} == {
        "hl-funding",
        "bg-funding",
    }


def test_hyperliquid_snapshot_becomes_valid_canonical_book() -> None:
    builder = CanonicalOrderBookStateBuilder()
    result = builder.apply(
        event("hl-book", "hyperliquid", "orderbook_snapshot", book_payload(), sequence=1),
        now=NOW,
    )
    assert result is not None
    assert result.state_status is BookStateStatus.VALID
    assert result.best_bid == Decimal("99") and result.best_ask == Decimal("100")
    assert result.source_snapshot_event_id == "hl-book"


def test_bitget_initial_snapshot_and_delta_build_canonical_state() -> None:
    builder = CanonicalOrderBookStateBuilder()
    initial = builder.apply(
        event(
            "bg-snapshot",
            "bitget",
            "orderbook_delta",
            {"_book_update": {"action": "snapshot", **book_payload()}},
            sequence=10,
        ),
        now=NOW,
    )
    assert initial is not None and initial.last_sequence == 10
    updated = builder.apply(
        event(
            "bg-delta",
            "bitget",
            "orderbook_delta",
            {
                "_book_update": {
                    "action": "update",
                    "bids": [["99", "4"], ["98", "0"]],
                    "asks": [["100", "1"]],
                }
            },
            sequence=11,
        ),
        now=NOW,
    )
    assert updated is not None
    assert updated.bids == ((Decimal("99"), Decimal("4")),)
    assert updated.applied_delta_event_ids == ("bg-delta",)


def test_bitget_predecessor_sequence_allows_nonconsecutive_venue_sequence() -> None:
    builder = CanonicalOrderBookStateBuilder()
    assert builder.apply(
        event("snapshot", "bitget", "orderbook_snapshot", book_payload(), sequence=100), now=NOW
    )
    updated = builder.apply(
        event(
            "delta",
            "bitget",
            "orderbook_delta",
            {
                "_book_update": {
                    "action": "update",
                    "bids": [["99", "4"]],
                    "asks": [],
                    "previous_sequence": 100,
                }
            },
            sequence=105,
        ),
        now=NOW,
    )
    assert updated is not None and updated.last_sequence == 105


def test_bitget_sequence_gap_invalidates_state() -> None:
    builder = CanonicalOrderBookStateBuilder()
    assert builder.apply(
        event("snapshot", "bitget", "orderbook_snapshot", book_payload(), sequence=10), now=NOW
    )
    assert (
        builder.apply(
            event(
                "gap",
                "bitget",
                "orderbook_delta",
                {"bids": [["99", "3"]], "asks": []},
                sequence=12,
            ),
            now=NOW,
        )
        is None
    )
    assert builder.metrics.sequence_gap_count == 1
    assert builder.last_invalid_reason[("bitget", "BTC")] is (
        FundingCarryRejectCode.BITGET_ORDERBOOK_SEQUENCE_GAP
    )


def test_bitget_sequence_gap_withdraws_book_from_shadow_input() -> None:
    repository = complete_repository()
    assert repository.add_experimental_event(
        event(
            "bg-gap",
            "bitget",
            "orderbook_delta",
            {"_book_update": {"action": "update", "bids": [["99", "3"]], "asks": []}},
            sequence=22,
        ),
        "implemented",
    )
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert not batch.matched
    assert batch.missing is not None
    assert batch.missing.reason is FundingCarryRejectCode.BITGET_ORDERBOOK_SEQUENCE_GAP
    assert batch.missing.venue == "bitget"
    assert batch.missing.capability == "orderbook_snapshot"
    assert not any(
        item.venue == "bitget" and item.event_type == "canonical_orderbook_snapshot"
        for item in batch.events
    )


def test_bitget_out_of_order_delta_is_rejected_without_mutating_book() -> None:
    builder = CanonicalOrderBookStateBuilder()
    initial = builder.apply(
        event("snapshot", "bitget", "orderbook_snapshot", book_payload(), sequence=10), now=NOW
    )
    assert initial is not None
    assert (
        builder.apply(
            event(
                "old",
                "bitget",
                "orderbook_delta",
                {"bids": [["99", "8"]], "asks": []},
                sequence=10,
            ),
            now=NOW,
        )
        is None
    )
    assert builder.metrics.out_of_order_count == 1


def test_bitget_reconnect_clears_state_and_delta_cannot_initialize() -> None:
    builder = CanonicalOrderBookStateBuilder()
    assert builder.apply(
        event("snapshot", "bitget", "orderbook_snapshot", book_payload(), sequence=10, epoch=1),
        now=NOW,
    )
    assert (
        builder.apply(
            event(
                "new-epoch-delta",
                "bitget",
                "orderbook_delta",
                {"bids": [["99", "8"]], "asks": []},
                sequence=11,
                epoch=2,
            ),
            now=NOW,
        )
        is None
    )
    assert builder.metrics.reconnect_reset_count == 1
    assert builder.last_invalid_reason[("bitget", "BTC")] is (
        FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_NOT_INITIALIZED
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"bids": [], "asks": [["100", "1"]]},
        {"bids": [["101", "1"]], "asks": [["100", "1"]]},
        {"bids": [["NaN", "1"]], "asks": [["100", "1"]]},
    ],
)
def test_invalid_hyperliquid_book_never_reaches_evaluator(payload: dict[str, object]) -> None:
    repository = complete_repository()
    repository.experimental_events.pop("hl-book")
    assert repository.add_experimental_event(
        event("invalid-hl", "hyperliquid", "orderbook_snapshot", payload), "implemented"
    )
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert not batch.matched
    assert batch.missing is not None
    assert batch.missing.venue == "hyperliquid"
    assert batch.missing.capability == "orderbook_snapshot"


@pytest.mark.parametrize(
    ("payload", "timestamp", "reason"),
    [
        (
            funding_payload("0.001", 3600),
            NOW + timedelta(seconds=2),
            FundingCarryRejectCode.FUTURE_TIMESTAMP,
        ),
        (
            funding_payload("0.001", 3600),
            NOW - timedelta(seconds=41),
            FundingCarryRejectCode.HYPERLIQUID_FUNDING_STALE,
        ),
        ({"funding_interval_seconds": 3600}, NOW, FundingCarryRejectCode.MISSING_FUNDING_CURRENT),
        ({"rate": "0.001"}, NOW, FundingCarryRejectCode.UNKNOWN_FUNDING_INTERVAL),
        (
            {"rate": "not-a-number", "funding_interval_seconds": 3600},
            NOW,
            FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE,
        ),
        (
            {"rate": "NaN", "funding_interval_seconds": 3600},
            NOW,
            FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE,
        ),
        (
            {"rate": "0.001", "funding_interval_seconds": 0},
            NOW,
            FundingCarryRejectCode.UNKNOWN_FUNDING_INTERVAL,
        ),
    ],
)
def test_invalid_experimental_funding_has_machine_readable_reason(
    payload: dict[str, object], timestamp: datetime, reason: FundingCarryRejectCode
) -> None:
    repository = complete_repository()
    repository.experimental_events.pop("hl-funding")
    assert repository.add_experimental_event(
        event("invalid-hl-funding", "hyperliquid", "funding_current", payload, timestamp=timestamp),
        "implemented",
    )
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert batch.missing is not None
    assert batch.missing.reason is reason
    assert batch.missing.venue == "hyperliquid"
    assert batch.missing.capability == "funding_current"


def test_bitget_checksum_is_validated_before_book_publication() -> None:
    payload = book_payload()
    values = ("99", "2", "100", "2", "98", "3", "101", "3")
    checksum = zlib.crc32(":".join(values).encode())
    valid_builder = CanonicalOrderBookStateBuilder()
    valid = valid_builder.apply(
        event(
            "valid-checksum",
            "bitget",
            "orderbook_delta",
            {"_book_update": {"action": "snapshot", **payload, "checksum": checksum}},
            sequence=1,
        ),
        now=NOW,
    )
    assert valid is not None
    invalid_builder = CanonicalOrderBookStateBuilder()
    invalid = invalid_builder.apply(
        event(
            "invalid-checksum",
            "bitget",
            "orderbook_delta",
            {"_book_update": {"action": "snapshot", **payload, "checksum": checksum + 1}},
            sequence=1,
        ),
        now=NOW,
    )
    assert invalid is None
    assert invalid_builder.metrics.checksum_failure_count == 1
    assert invalid_builder.last_invalid_reason[("bitget", "BTC")] is (
        FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_INVALID
    )


def test_funding_polling_offset_does_not_cause_orderbook_sync_rejection() -> None:
    repository = complete_repository()
    repository.experimental_events.pop("bg-funding")
    assert repository.add_experimental_event(
        event(
            "skewed-bg-funding",
            "bitget",
            "funding_current",
            funding_payload("0.0004", 28800),
            timestamp=NOW - timedelta(seconds=6),
        ),
        "implemented",
    )
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert batch.matched
    assert batch.missing is None
    assert batch.timing is not None
    assert batch.timing.funding_observation_skew_seconds == Decimal("6.0")
    assert batch.timing.orderbook_venue_skew_seconds == Decimal("0.0")


def test_timestamp_semantics_and_freshness_are_persisted_per_input_domain() -> None:
    batch = FundingCarryShadowInputSource(complete_repository()).read(now=NOW)
    assert batch.matched
    assert {(item.event_type, item.timestamp_semantic) for item in batch.events} == {
        ("funding_current", "funding_observation"),
        ("canonical_orderbook_snapshot", "orderbook_market_event"),
    }
    assert all(item.exchange_timestamp is not None for item in batch.events)
    assert all(item.freshness_age_seconds == 0 for item in batch.events)


@pytest.mark.parametrize(
    ("venue", "reason"),
    [
        ("hyperliquid", FundingCarryRejectCode.HYPERLIQUID_FUNDING_STALE),
        ("bitget", FundingCarryRejectCode.BITGET_FUNDING_STALE),
    ],
)
def test_funding_freshness_uses_funding_timing_policy(
    venue: str, reason: FundingCarryRejectCode
) -> None:
    repository = complete_repository()
    repository.experimental_events.pop("hl-funding" if venue == "hyperliquid" else "bg-funding")
    assert repository.add_experimental_event(
        event(
            f"stale-{venue}-funding",
            venue,
            "funding_current",
            funding_payload("0.001", 3600),
            timestamp=NOW - timedelta(seconds=41),
        ),
        "implemented",
    )
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert not batch.matched
    assert batch.missing is not None
    assert batch.missing.reason is reason
    assert batch.timing is not None
    assert not batch.timing.funding_freshness_pass


def test_unsynchronized_orderbooks_use_orderbook_only_timing_policy() -> None:
    repository = complete_repository()
    repository.experimental_events.pop("hl-book")
    assert repository.add_experimental_event(
        event(
            "skewed-hl-book",
            "hyperliquid",
            "orderbook_snapshot",
            book_payload(),
            sequence=10,
            timestamp=NOW - timedelta(seconds=6),
        ),
        "implemented",
    )
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert not batch.matched
    assert batch.missing is not None
    assert batch.missing.reason is FundingCarryRejectCode.ORDERBOOK_VENUES_UNSYNCHRONIZED
    assert batch.timing is not None
    assert batch.timing.funding_freshness_pass
    assert not batch.timing.orderbook_synchronization_pass


def test_all_four_inputs_create_matched_pair_and_reach_evaluator() -> None:
    batch = FundingCarryShadowInputSource(complete_repository()).read(now=NOW)
    assert batch.matched and len(batch.events) == 4
    funding = tuple(
        FundingObservation(
            event_id=item.event_id,
            venue=item.venue,
            instrument=item.instrument,
            raw_funding_rate=item.funding_rate or Decimal("0"),
            funding_unit=item.funding_unit or "",
            funding_interval_seconds=item.funding_interval_seconds,
            next_funding_at=item.next_funding_at,
            source_timestamp=item.source_timestamp or item.available_at,
            received_at=item.received_at or item.available_at,
        )
        for item in batch.events
        if item.event_type == "funding_current"
    )
    books = tuple(
        OrderBookObservation(
            event_id=item.event_id,
            venue=item.venue,
            instrument=item.instrument,
            bids=item.bids,
            asks=item.asks,
            source_timestamp=item.source_timestamp or item.available_at,
            received_at=item.received_at or item.available_at,
        )
        for item in batch.events
        if item.event_type == "canonical_orderbook_snapshot"
    )
    result = FundingCarryShadowEvaluator(
        FundingCarryShadowConfig(minimum_net_edge=Decimal("-1"))
    ).evaluate(
        run_id="pair-test",
        funding=funding,
        orderbooks=books,
        now=NOW,
        code_commit_sha="a" * 40,
        config_sha="b" * 64,
    )
    assert result.long_venue is not None and result.short_venue is not None


def test_missing_venue_is_identified_exactly() -> None:
    repository = complete_repository()
    repository.experimental_events.pop("bg-funding")
    batch = FundingCarryShadowInputSource(repository).read(now=NOW)
    assert batch.missing is not None
    assert batch.missing.reason is FundingCarryRejectCode.MISSING_BITGET_FUNDING_CURRENT
    assert batch.missing.venue == "bitget"
    assert batch.missing.capability == "funding_current"
    assert batch.missing.source_event_count == 3


@pytest.mark.asyncio
async def test_missing_venue_details_are_persisted_on_rejected_candidate() -> None:
    repository = complete_repository()
    repository.experimental_events.pop("bg-funding")
    input_source = FundingCarryShadowInputSource(repository)
    operational = InMemoryOperationalRepository()
    operation = ContinuousResearchPaperService(
        repository=operational,
        settings=shadow_settings(),
        run_id="missing-detail",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
        market_event_action=lambda: input_source.read(now=NOW),
    )
    operation.set_collector_health(True)
    await operation._collector_tick()
    await operation._signal_tick()
    candidate = operational.shadow_candidates("missing-detail")[0]
    assert candidate.rejection_reason is FundingCarryRejectCode.MISSING_BITGET_FUNDING_CURRENT
    assert candidate.missing_venue == "bitget"
    assert candidate.missing_capability == "funding_current"
    assert candidate.source_event_count == 3


def shadow_settings() -> Settings:
    return Settings(
        database_url="sqlite+pysqlite:///:memory:",
        paper_trading=True,
        live_trading=False,
        paper={"enabled": True},
        live={"enabled": False, "adapter_name": "disabled", "allowed_symbols": ("BTC",)},
        symbols=("BTC",),
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


def test_shadow_subsystem_allowlist_excludes_snapshot_research_and_paper_workers() -> None:
    operation = ContinuousResearchPaperService(
        repository=InMemoryOperationalRepository(),
        settings=shadow_settings(),
        run_id="allowlist",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )
    assert len(operation.workers) == 2
    assert not any(isinstance(worker, SnapshotFinalizer) for worker in operation.workers)
    assert not any(isinstance(worker, ResearchScheduler) for worker in operation.workers)
    assert {worker.name for worker in operation.workers} == {
        "collector",
        "funding_carry_shadow_evaluator",
    }


@pytest.mark.asyncio
async def test_shadow_evaluator_waits_for_first_experimental_input_scan() -> None:
    operational = InMemoryOperationalRepository()
    operation = ContinuousResearchPaperService(
        repository=operational,
        settings=shadow_settings(),
        run_id="await-input-scan",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )
    operation.set_collector_health(True)
    await operation._signal_tick()
    assert operational.shadow_candidates("await-input-scan") == ()
    assert operational.shadow_metrics("await-input-scan").candidate_generation_attempt_count == 0


@pytest.mark.asyncio
async def test_experimental_inputs_reach_shadow_evaluator_end_to_end() -> None:
    input_source = FundingCarryShadowInputSource(complete_repository())
    operational = InMemoryOperationalRepository()
    operation = ContinuousResearchPaperService(
        repository=operational,
        settings=shadow_settings(),
        run_id="end-to-end",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
        market_event_action=lambda: input_source.read(now=NOW),
    )
    operation.set_collector_health(True)
    await operation._collector_tick()
    await operation._signal_tick()
    candidates = operational.shadow_candidates("end-to-end")
    assert len(candidates) == 1
    assert candidates[0].source_event_count == 4
    assert candidates[0].long_venue is not None
    assert operational.signals("end-to-end") == ()
    assert operational.orders("end-to-end") == ()
    assert operational.fills("end-to-end") == ()
    assert not hasattr(operation, "execution_adapter")
    metrics = operational.shadow_metrics("end-to-end")
    assert metrics.matched_source_pair_count == 1
    assert metrics.candidate_inserted_count == 1


def test_shadow_mode_creates_zero_snapshot_research_paper_records() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    repository = PostgreSQLOperationalRepository(engine)
    ContinuousResearchPaperService(
        repository=repository,
        settings=shadow_settings(),
        run_id="zero-side-effects",
        commit_sha="a" * 40,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(DataSnapshotRow)) == 0
        assert session.scalar(select(func.count()).select_from(ResearchRunRow)) == 0
        assert session.scalar(select(func.count()).select_from(PaperSignalRow)) == 0
        assert session.scalar(select(func.count()).select_from(PaperOrderRow)) == 0
        assert session.scalar(select(func.count()).select_from(PaperFillRow)) == 0


def test_shadow_artifact_bundle_is_complete_and_records_zero_safety_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_dir = tmp_path / "durable-state"
    monkeypatch.setenv("CRYPTTOOL_STATE_DIR", str(state_dir))
    monkeypatch.setattr(
        "app.services.operations.shadow_artifacts.tempfile.gettempdir",
        lambda: "/system-temporary-directory",
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    operational = PostgreSQLOperationalRepository(engine)
    settings = shadow_settings()
    settings.continuous_paper.minimum_shadow_net_edge = Decimal("1")
    commit_sha = "a" * 40
    run_id = "artifact-test"
    operation = ContinuousResearchPaperService(
        repository=operational,
        settings=settings,
        run_id=run_id,
        commit_sha=commit_sha,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )
    source = FundingCarryShadowInputSource(complete_repository())
    batch = source.read(now=NOW)
    inserted = operation.generate_funding_carry_shadow_candidate(
        events=batch.events, batch=batch, decision_time=NOW
    )
    assert inserted.disposition.value == "rejected"
    source_config = tmp_path / "source-config.yaml"
    source_config.write_text("mode: shadow\n", encoding="utf-8")
    writer = ShadowRunArtifactWriter(
        run_id=run_id,
        commit_sha=commit_sha,
        config_path=source_config,
        settings=settings,
        engine=engine,
        repository=operational,
        input_source=source,
    )
    writer.startup(f"commit_sha={commit_sha}")
    writer.finalize(exit_code=0)
    assert {item.name for item in writer.directory.iterdir()} == set(writer.SUCCESS_FILES)
    safety = json.loads((writer.directory / "safety-counters.json").read_text())
    assert safety == {
        "capability_promotion_count": 0,
        "execution_adapter_call_count": 0,
        "live_credentials_loaded": False,
        "live_execution_state": "OFF",
        "paper_fill_count": 0,
        "paper_order_count": 0,
        "paper_signal_count": 0,
        "production_order_count": 0,
        "research_run_count": 0,
        "snapshot_count": 0,
        "strict_paper_state": "NOT_READY",
    }
    manifest = json.loads((writer.directory / "run-manifest.json").read_text())
    assert manifest["commit_sha"] == commit_sha
    assert manifest["provenance_consistent"] is True
    assert manifest["exit_code"] == 0
    pairing = json.loads((writer.directory / "pairing-metrics.json").read_text())
    assert isinstance(pairing["last_seen_inputs"], list)
    assert "('hyperliquid', 'funding_current')" not in json.dumps(pairing)
    dedup = json.loads((writer.directory / "dedup-metrics.json").read_text())
    assert set(dedup) >= {
        "candidate_evaluation_attempt_count",
        "candidate_record_inserted_count",
        "candidate_disposition_candidate_count",
        "candidate_disposition_rejected_count",
        "candidate_duplicate_suppressed_count",
        "matched_source_pair_count",
    }
    assert dedup["candidate_record_inserted_count"] == 1
    assert dedup["candidate_disposition_candidate_count"] == 0
    assert dedup["candidate_disposition_rejected_count"] == 1


def test_artifact_serialization_failure_is_nonzero_and_writes_partial_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_dir = tmp_path / "durable-state"
    monkeypatch.setenv("CRYPTTOOL_STATE_DIR", str(state_dir))
    monkeypatch.setattr(
        "app.services.operations.shadow_artifacts.tempfile.gettempdir",
        lambda: "/system-temporary-directory",
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    operational = PostgreSQLOperationalRepository(engine)
    settings = shadow_settings()
    commit_sha = "a" * 40
    run_id = "artifact-failure-test"
    ContinuousResearchPaperService(
        repository=operational,
        settings=settings,
        run_id=run_id,
        commit_sha=commit_sha,
        config_sha256="b" * 64,
        mode=OperationMode.OBSERVATION_ONLY,
        local_smoke=True,
        now=lambda: NOW,
    )
    source_config = tmp_path / "source-config.yaml"
    source_config.write_text("mode: shadow\n", encoding="utf-8")
    writer = ShadowRunArtifactWriter(
        run_id=run_id,
        commit_sha=commit_sha,
        config_path=source_config,
        settings=settings,
        engine=engine,
        repository=operational,
        input_source=FundingCarryShadowInputSource(complete_repository()),
    )
    original_write_json = writer._write_json

    def fail_pairing_metrics(name: str, value: object) -> None:
        if name == "pairing-metrics.json":
            raise TypeError("fixture serialization failure")
        original_write_json(name, value)

    monkeypatch.setattr(writer, "_write_json", fail_pairing_metrics)
    with pytest.raises(ArtifactFinalizationError):
        writer.finalize(exit_code=0)
    manifest = json.loads((writer.directory / "run-manifest.json").read_text())
    error = json.loads((writer.directory / "artifact-error.json").read_text())
    failure = json.loads((writer.directory / "failure.json").read_text())
    assert manifest["exit_code"] == error["actual_exit_code"] == failure["actual_exit_code"] == 1
    assert manifest["process_status"] == "FAILED"
    assert error["failed_artifact_filename"] == "pairing-metrics.json"
    assert error["exception_type"] == "TypeError"
    assert (writer.directory / "lifecycle.jsonl").is_file()
    assert (writer.directory / "stdout.log").is_file()
    assert (writer.directory / "stderr.log").is_file()
    assert not (writer.directory / "COMPLETED").exists()


def test_worktree_commit_resolution_and_unknown_fail_closed(tmp_path: Path) -> None:
    expected = (
        __import__("subprocess")
        .run(
            ("git", "rev-parse", "HEAD"),
            cwd=Path.cwd(),
            check=True,
            capture_output=True,
            text=True,
        )
        .stdout.strip()
    )
    assert resolve_commit_sha(cwd=Path.cwd(), environment={}) == expected
    with pytest.raises(RuntimeError, match="unable to resolve"):
        resolve_commit_sha(cwd=tmp_path, environment={})


def test_worktree_gitdir_file_resolves_common_repository_ref(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = "c" * 40
    worktree = tmp_path / "worktree"
    git_dir = tmp_path / "repository" / ".git" / "worktrees" / "worktree"
    common_dir = tmp_path / "repository" / ".git"
    (common_dir / "refs" / "heads" / "feature").mkdir(parents=True)
    git_dir.mkdir(parents=True)
    worktree.mkdir()
    (worktree / ".git").write_text(f"gitdir: {git_dir}\n", encoding="utf-8")
    (git_dir / "HEAD").write_text("ref: refs/heads/feature/runtime\n", encoding="utf-8")
    (git_dir / "commondir").write_text("../..\n", encoding="utf-8")
    (common_dir / "refs" / "heads" / "feature" / "runtime").write_text(
        expected + "\n", encoding="utf-8"
    )
    monkeypatch.setattr("app.services.operations.provenance._git", lambda *args, **kwargs: None)
    assert resolve_commit_sha(cwd=worktree, environment={}) == expected


def test_commit_resolution_accepts_ci_then_explicit_full_sha(tmp_path: Path) -> None:
    ci_sha = "d" * 40
    explicit_sha = "e" * 40
    assert resolve_commit_sha(cwd=tmp_path, environment={"GITHUB_SHA": ci_sha}) == ci_sha
    assert (
        resolve_commit_sha(cwd=tmp_path, environment={}, explicit_sha=explicit_sha) == explicit_sha
    )


def test_candidate_duplicate_suppression_is_persisted_and_identity_survives_restart() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    first = PostgreSQLOperationalRepository(engine)
    candidate = FundingCarryShadowEvaluator(
        FundingCarryShadowConfig(minimum_net_edge=Decimal("-1"))
    ).evaluate(
        run_id="dedup-run",
        funding=(
            FundingObservation(
                "hl-f",
                "hyperliquid",
                "BTC",
                Decimal("0.001"),
                "fraction_per_interval",
                3600,
                None,
                NOW,
                NOW,
            ),
            FundingObservation(
                "bg-f",
                "bitget",
                "BTC",
                Decimal("0.0001"),
                "fraction_per_interval",
                3600,
                None,
                NOW,
                NOW,
            ),
        ),
        orderbooks=(
            OrderBookObservation(
                "hl-b",
                "hyperliquid",
                "BTC",
                ((Decimal("99"), Decimal("2")),),
                ((Decimal("100"), Decimal("2")),),
                NOW,
                NOW,
            ),
            OrderBookObservation(
                "bg-b",
                "bitget",
                "BTC",
                ((Decimal("99"), Decimal("2")),),
                ((Decimal("100"), Decimal("2")),),
                NOW,
                NOW,
            ),
        ),
        now=NOW,
        code_commit_sha="a" * 40,
        config_sha="b" * 64,
    )
    assert first.record_shadow_candidate(
        candidate, matched_source_pair=True, source_pair_duplicate=False
    )
    restarted = PostgreSQLOperationalRepository(engine)
    restored = restarted.shadow_candidates("dedup-run")[0]
    assert restored.candidate_id == candidate.candidate_id
    assert not restarted.record_shadow_candidate(
        restored, matched_source_pair=True, source_pair_duplicate=True
    )
    metrics = restarted.shadow_metrics("dedup-run")
    assert metrics.candidate_generation_attempt_count == 2
    assert metrics.candidate_inserted_count == 1
    assert metrics.candidate_duplicate_suppressed_count == 1
    assert metrics.source_pair_duplicate_count == 1
    assert metrics.matched_source_pair_count == 2
