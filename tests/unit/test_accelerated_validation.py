from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from app.adapters.exchanges.websocket import ReconciliationState
from app.services.research.accelerated_validation import (
    AcceleratedValidationArtifactWriter,
    FaultKind,
    FaultSchedule,
    HistoricalMarketEventReplay,
    HistoricalPublicDatasetLoader,
    ReplayMarketEvent,
    run_start_stop_resource_test,
)
from app.services.research.data_operations import CollectedEnvelope
from app.services.research.repository import InMemoryResearchRepository

START = datetime(2026, 1, 1, tzinfo=UTC)


def replay_event(index: int, *, event_type: str = "ohlcv") -> ReplayMarketEvent:
    at = START + timedelta(hours=index * 12)
    payload = json.dumps(
        {"close": 100 + index, "index": index, "timestamp": at.isoformat()}, sort_keys=True
    )
    envelope = CollectedEnvelope(
        venue="hyperliquid",
        canonical_instrument_id="BTC",
        venue_symbol="BTC",
        event_type=event_type,
        source_endpoint="https://api.hyperliquid.xyz/info",
        raw_payload=payload,
        normalized_payload=payload,
        exchange_timestamp=at,
        received_at=at + timedelta(seconds=1),
        available_at=at + timedelta(seconds=1),
        channel="rest",
        sequence=index,
        connection_id=UUID("00000000-0000-0000-0000-000000000001"),
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        stable_event_key=f"historical-{index}",
    )
    return ReplayMarketEvent(
        envelope=envelope,
        source_endpoint=envelope.source_endpoint,
        retrieved_at=datetime(2026, 2, 2, tzinfo=UTC),
        source_payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        coverage_start=START,
        coverage_end=START + timedelta(days=30),
    )


async def event_stream(events: tuple[ReplayMarketEvent, ...]) -> AsyncIterator[ReplayMarketEvent]:
    for event in events:
        yield event


def test_fault_schedule_is_seeded_and_covers_every_fault() -> None:
    first = FaultSchedule.deterministic(event_count=80, seed=42)
    second = FaultSchedule.deterministic(event_count=80, seed=42)
    assert first == second
    assert {item.kind for item in first.faults} == set(FaultKind)


@pytest.mark.asyncio
async def test_historical_replay_uses_consumer_checkpoint_snapshot_and_restarts() -> None:
    repository = InMemoryResearchRepository()
    events = tuple(replay_event(index) for index in range(1, 61))
    schedule = FaultSchedule.deterministic(event_count=len(events), seed=1984)

    result = await HistoricalMarketEventReplay(repository=repository).replay(
        events=event_stream(events),
        speed=None,
        maximum_queue_depth=4,
        fault_schedule=schedule,
    )

    assert result.input_events == 60
    assert result.event_loss == 0
    assert result.unexpected_duplicates == 0
    assert result.checkpoint_regressions == 0
    assert len(result.restart_results) >= 3
    assert all(
        item.connection_epoch_after > item.connection_epoch_before
        for item in result.restart_results
    )
    assert repository.snapshot_manifest(result.snapshot_manifest.snapshot_id) is not None
    assert result.snapshot_manifest == repository.snapshot_manifest(
        result.snapshot_manifest.snapshot_id
    )
    assert repository.collection_failures()  # injected failures were observed and recovered
    assert result.passed


@pytest.mark.asyncio
async def test_historical_replay_realtime_modes_are_supported() -> None:
    for speed in (10.0, 100.0):
        repository = InMemoryResearchRepository()
        events = tuple(
            replace(
                replay_event(index),
                envelope=replace(
                    replay_event(index).envelope,
                    available_at=START + timedelta(milliseconds=index),
                    received_at=START + timedelta(milliseconds=index),
                    exchange_timestamp=START + timedelta(milliseconds=index),
                ),
            )
            for index in range(1, 5)
        )
        # Recompute provenance after replacing the envelope payload clocks are unchanged.
        result = await HistoricalMarketEventReplay(
            repository=repository, restart_percentages=()
        ).replay(
            events=event_stream(events),
            speed=speed,
            maximum_queue_depth=2,
            fault_schedule=None,
        )
        assert result.event_loss == 0


@pytest.mark.asyncio
async def test_clean_replay_has_no_faults_restarts_and_distinct_snapshot_namespace() -> None:
    repository = InMemoryResearchRepository()
    result = await HistoricalMarketEventReplay(
        repository=repository,
        restart_percentages=(),
        snapshot_prefix="clean",
    ).replay(
        events=event_stream(tuple(replay_event(index) for index in range(1, 5))),
        speed=None,
        maximum_queue_depth=2,
        fault_schedule=None,
    )
    assert result.snapshot_manifest.snapshot_id.startswith("clean-")
    assert result.fault_results == ()
    assert result.restart_results == ()
    assert result.event_loss == 0


@pytest.mark.asyncio
async def test_one_hundred_start_stop_cycles_do_not_leak_resources() -> None:
    async def cycle(_: int) -> None:
        task = __import__("asyncio").create_task(__import__("asyncio").sleep(0))
        await task

    samples, analysis = await run_start_stop_resource_test(iterations=100, cycle=cycle)
    assert len(samples) == 100
    assert analysis.bounded
    assert analysis.warmup_iterations == 10
    assert analysis.top_allocation_traceback
    assert samples[-1].task_count <= samples[0].task_count + 1
    assert samples[-1].open_db_connections == samples[0].open_db_connections


@pytest.mark.asyncio
async def test_accelerated_artifact_manifest_hash_verification(tmp_path: Path) -> None:
    repository = InMemoryResearchRepository()
    replay = await HistoricalMarketEventReplay(
        repository=repository, restart_percentages=()
    ).replay(
        events=event_stream(tuple(replay_event(index) for index in range(1, 4))),
        speed=None,
        maximum_queue_depth=2,
        fault_schedule=None,
    )
    samples, analysis = await run_start_stop_resource_test(iterations=2, cycle=lambda _: None)
    manifest = AcceleratedValidationArtifactWriter.write(
        root=tmp_path,
        run_id="accelerated-test",
        commit_sha="d96567d",
        replay=replay,
        resources=samples,
        resource_leak_detected=not analysis.bounded,
        resource_analysis=analysis,
        live_soak_status="INSUFFICIENT_EVIDENCE",
        research_pipeline_verdict="INSUFFICIENT_EVIDENCE",
        unresolved_items=("historical order-book delta unavailable",),
    )
    verified = AcceleratedValidationArtifactWriter.verify(manifest)
    assert verified["run_id"] == "accelerated-test"
    (manifest.parent / "metrics.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        AcceleratedValidationArtifactWriter.verify(manifest)


def test_replay_rejects_changed_source_payload() -> None:
    item = replay_event(1)
    with pytest.raises(ValueError, match="source payload hash mismatch"):
        replace(item, source_payload_sha256="0" * 64)


@pytest.mark.asyncio
async def test_historical_loader_declares_unavailable_orderbook_without_fabrication() -> None:
    class Source:
        venue = "hyperliquid"
        adapter = object()

        async def historical_events(
            self, identity: object, *, start: datetime, end: datetime, timeframe: str
        ) -> tuple[CollectedEnvelope, ...]:
            del identity, start, end, timeframe
            return (replay_event(1).envelope,)

    dataset = await HistoricalPublicDatasetLoader((Source(),)).load(  # type: ignore[arg-type]
        start=START,
        end=START + timedelta(days=30),
        instruments=("BTC",),
    )
    books = [item for item in dataset.coverage if item.event_type == "orderbook_delta"]
    assert len(dataset.events) == 2  # OHLCV and funding use the supplied public history path.
    assert books[0].status == "INSUFFICIENT_EVIDENCE"
    assert books[0].detail is not None
    assert "historical order-book delta unavailable" in books[0].detail
    assert not dataset.historical_orderbook_delta_available
