from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from sqlalchemy import create_engine

from app.adapters.exchanges.websocket import ReconciliationState
from app.infrastructure.database.models import Base
from app.services.operations.shadow_runtime import FundingCarryShadowInputSource
from app.services.research.models import (
    ExperimentalEventCursor,
    RawMarketEvent,
)
from app.services.research.repository import (
    InMemoryResearchRepository,
    PostgreSQLResearchRepository,
)

NOW = datetime(2026, 8, 9, 22, 54, 24, tzinfo=UTC)


def raw_event(
    ordinal: int,
    *,
    venue: str = "hyperliquid",
    event_type: str = "funding_current",
    timestamp: datetime | None = None,
    sequence: int | None = None,
    payload: dict[str, object] | None = None,
) -> RawMarketEvent:
    observed_at = timestamp or NOW + timedelta(microseconds=ordinal)
    body = payload or {"rate": "0.001", "funding_interval_seconds": 3600}
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return RawMarketEvent(
        event_id=f"event-{ordinal:09d}",
        venue=venue,
        canonical_instrument_id="BTC",
        venue_symbol="BTC" if venue == "hyperliquid" else "BTCUSDT",
        event_type=event_type,
        exchange_timestamp=observed_at,
        received_at=observed_at,
        available_at=observed_at,
        sequence=sequence,
        connection_id=None,
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        payload_sha256=hashlib.sha256(encoded.encode()).hexdigest(),
        raw_payload=encoded,
        normalizer_version="incremental-fixture-v1",
        capability_verification_run_id="experimental",
        created_at=observed_at,
        channel="orderbook" if event_type.startswith("orderbook") else "rest",
        connection_epoch=1,
    )


def test_incremental_query_is_deterministic_bounded_and_returns_only_unseen() -> None:
    repository = InMemoryResearchRepository()
    for ordinal in (3, 1, 2):
        assert repository.add_experimental_event(raw_event(ordinal), "implemented")
    first = repository.list_experimental_events_after(
        (),
        venues=("hyperliquid", "bitget"),
        instrument="BTC",
        event_types=("funding_current", "orderbook_snapshot", "orderbook_delta"),
        limit=2,
    )
    assert [item.event_id for item in first] == ["event-000000001", "event-000000002"]
    cursor = ExperimentalEventCursor(
        run_id="bounded",
        venue="hyperliquid",
        instrument="BTC",
        event_stream="funding_current",
        last_available_at=first[-1].available_at,
        last_event_id=first[-1].event_id,
        updated_at=NOW,
    )
    second = repository.list_experimental_events_after(
        (cursor,),
        venues=("hyperliquid",),
        instrument="BTC",
        event_types=("funding_current",),
        limit=2,
    )
    assert [item.event_id for item in second] == ["event-000000003"]


def test_cursor_advances_only_after_success_and_restart_resumes() -> None:
    repository = InMemoryResearchRepository()
    for ordinal in range(5):
        repository.add_experimental_event(raw_event(ordinal), "implemented")
    first = FundingCarryShadowInputSource(repository, run_id="resume", batch_size=2)
    first.read(now=NOW + timedelta(seconds=1))
    cursors, checkpoint = repository.shadow_input_checkpoint("resume")
    assert checkpoint is not None and checkpoint.events_processed_total == 2
    assert max(item.last_event_id or "" for item in cursors) == "event-000000001"
    restarted = FundingCarryShadowInputSource(repository, run_id="resume", batch_size=2)
    restarted.read(now=NOW + timedelta(seconds=2))
    _, resumed = repository.shadow_input_checkpoint("resume")
    assert resumed is not None and resumed.events_processed_total == 4


class FailingSource(FundingCarryShadowInputSource):
    def _process_event(self, event: RawMarketEvent, *, now: datetime) -> None:
        raise RuntimeError("fixture processing failure")


def test_cursor_does_not_advance_after_processing_failure() -> None:
    repository = InMemoryResearchRepository()
    repository.add_experimental_event(raw_event(1), "implemented")
    source = FailingSource(repository, run_id="failed", batch_size=100)
    with pytest.raises(RuntimeError, match="fixture processing failure"):
        source.read(now=NOW + timedelta(seconds=1))
    cursors, checkpoint = repository.shadow_input_checkpoint("failed")
    assert all(item.last_event_id is None for item in cursors)
    assert checkpoint is not None
    assert checkpoint.events_processed_total == 0
    assert checkpoint.events_failed_total == 1


def test_restart_restores_snapshot_backed_bitget_book_before_delta() -> None:
    repository = InMemoryResearchRepository()
    snapshot = raw_event(
        1,
        venue="bitget",
        event_type="orderbook_snapshot",
        sequence=10,
        payload={"bids": [["99", "2"]], "asks": [["100", "2"]]},
    )
    repository.add_experimental_event(snapshot, "implemented")
    source = FundingCarryShadowInputSource(repository, run_id="book-recovery", batch_size=10)
    source.read(now=NOW + timedelta(seconds=1))
    delta = raw_event(
        2,
        venue="bitget",
        event_type="orderbook_delta",
        sequence=11,
        payload={"bids": [["99", "3"]], "asks": []},
    )
    repository.add_experimental_event(delta, "implemented")
    restarted = FundingCarryShadowInputSource(repository, run_id="book-recovery", batch_size=10)
    restarted.read(now=NOW + timedelta(seconds=1))
    assert restarted.book_builder.metrics.delta_count == 1
    assert restarted.book_builder.metrics.sequence_gap_count == 0
    assert restarted._latest_books["bitget"].bid_size == 3
    expected_ids = restarted._latest_books["bitget"].applied_source_event_ids
    second_restart = FundingCarryShadowInputSource(
        repository, run_id="book-recovery", batch_size=10
    )
    second_restart.read(now=NOW + timedelta(seconds=1))
    assert second_restart._latest_books["bitget"].applied_source_event_ids == expected_ids


class SyntheticTailRepository(InMemoryResearchRepository):
    def __init__(self, count: int) -> None:
        super().__init__()
        self.count = count

    def list_experimental_events_after(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
        limit: int,
    ) -> tuple[RawMarketEvent, ...]:
        del venues, instrument, event_types
        cursor = next(
            (
                item
                for item in cursors
                if item.venue == "hyperliquid" and item.event_stream == "funding_current"
            ),
            None,
        )
        start = (
            int(cursor.last_event_id.rsplit("-", 1)[-1]) + 1
            if cursor and cursor.last_event_id
            else 0
        )
        return tuple(raw_event(ordinal) for ordinal in range(start, min(start + limit, self.count)))

    def experimental_event_backlog(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
    ) -> tuple[int, datetime | None]:
        if (
            "hyperliquid" not in venues
            or "funding_current" not in event_types
            or instrument != "BTC"
        ):
            return 0, None
        cursor = next(
            (
                item
                for item in cursors
                if item.venue == "hyperliquid" and item.event_stream == "funding_current"
            ),
            None,
        )
        consumed = (
            int(cursor.last_event_id.rsplit("-", 1)[-1]) + 1
            if cursor and cursor.last_event_id
            else 0
        )
        latest = NOW + timedelta(microseconds=self.count - 1) if self.count else None
        return max(0, self.count - consumed), latest


def test_226k_event_fixture_reaches_tail_with_bounded_batches() -> None:
    count = 226_708
    repository = SyntheticTailRepository(count)
    source = FundingCarryShadowInputSource(repository, run_id="large-tail", batch_size=1000)
    while cast(int, source.runtime_metrics()["shadow_input_backlog_estimate"]) or (
        cast(int, source.runtime_metrics()["shadow_input_events_processed_total"]) == 0
    ):
        source.read(now=NOW + timedelta(seconds=2))
    metrics = source.runtime_metrics(now=NOW + timedelta(seconds=3))
    assert metrics["shadow_input_events_processed_total"] == count
    assert metrics["shadow_input_backlog_estimate"] == 0
    cursors, _ = repository.shadow_input_checkpoint("large-tail")
    assert max(item.last_event_id or "" for item in cursors) == "event-000226707"


def test_new_events_after_large_history_are_consumed_from_start_boundary() -> None:
    repository = InMemoryResearchRepository()
    for ordinal in range(10_000):
        repository.add_experimental_event(raw_event(ordinal), "implemented")
    boundary = NOW + timedelta(seconds=1)
    source = FundingCarryShadowInputSource(
        repository,
        run_id="new-only",
        batch_size=1000,
        initial_available_at=boundary,
    )
    appended = raw_event(10_001, timestamp=boundary + timedelta(microseconds=1))
    repository.add_experimental_event(appended, "implemented")
    source.read(now=boundary + timedelta(seconds=1))
    assert source.runtime_metrics()["shadow_input_events_processed_total"] == 1


def test_collector_growth_with_stalled_cursor_is_degraded_then_recovers() -> None:
    repository = InMemoryResearchRepository()
    source = FundingCarryShadowInputSource(
        repository,
        run_id="stall",
        batch_size=1,
        maximum_shadow_input_stall_seconds=5,
    )
    source._cursors = tuple(replace(cursor, updated_at=NOW) for cursor in source._cursors)
    repository.add_experimental_event(raw_event(1), "implemented")
    source._refresh_lag(now=NOW + timedelta(seconds=6))
    assert source.runtime_status == "degraded"
    source.read(now=NOW + timedelta(seconds=7))
    assert source.runtime_status == "healthy"
    assert source.runtime_metrics()["shadow_input_lag_seconds"] == 0


def test_postgresql_incremental_query_checkpoint_and_restart_round_trip() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    repository = PostgreSQLResearchRepository(engine)
    fixtures = (
        raw_event(1, venue="hyperliquid"),
        raw_event(2, venue="bitget"),
        raw_event(
            3,
            venue="hyperliquid",
            event_type="orderbook_snapshot",
            sequence=10,
            payload={"bids": [["99", "2"]], "asks": [["100", "2"]]},
        ),
        raw_event(
            4,
            venue="bitget",
            event_type="orderbook_snapshot",
            sequence=20,
            payload={"bids": [["99", "2"]], "asks": [["100", "2"]]},
        ),
    )
    for item in fixtures:
        assert repository.add_experimental_event(item, "implemented")
    source = FundingCarryShadowInputSource(repository, run_id="postgres-round-trip")
    assert source.read(now=NOW + timedelta(seconds=1)).matched
    cursors, checkpoint = repository.shadow_input_checkpoint("postgres-round-trip")
    assert checkpoint is not None and checkpoint.events_processed_total == 4
    assert len(cursors) == 4
    restarted = FundingCarryShadowInputSource(repository, run_id="postgres-round-trip")
    assert restarted.read(now=NOW + timedelta(seconds=2)).source_pair_duplicate
