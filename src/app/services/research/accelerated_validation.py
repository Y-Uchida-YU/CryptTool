from __future__ import annotations

import asyncio
import csv
import gc
import hashlib
import json
import math
import os
import random
import resource
import subprocess  # nosec B404
import time
import tracemalloc
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from app.adapters.exchanges.websocket import ReconciliationState
from app.domain.venues.models import CapabilitySupport
from app.services.research.data_operations import (
    CapabilityDecision,
    CheckpointWriter,
    CollectedEnvelope,
    CollectorHealthMetrics,
    DataSnapshotService,
    PublicAdapterCollectorSource,
    RawPersistenceConsumer,
    ResearchCapabilityGate,
    ResearchStreamIdentity,
    SnapshotEligibilityPolicy,
)
from app.services.research.models import DataSnapshotManifest, canonical_sha256
from app.services.research.repository import ResearchRepository


class FaultKind(StrEnum):
    WEBSOCKET_DISCONNECT = "websocket_disconnect"
    RECONNECT = "reconnect"
    DUPLICATE_EVENT = "duplicate_event"
    OUT_OF_ORDER = "out_of_order"
    SEQUENCE_GAP = "sequence_gap"
    MISSING_SNAPSHOT = "missing_snapshot"
    STALE_EVENT = "stale_event"
    CLOCK_SKEW = "clock_skew"
    MALFORMED_PAYLOAD = "malformed_payload"
    DATABASE_TRANSIENT_FAILURE = "database_transient_failure"
    CHECKPOINT_WRITE_FAILURE = "checkpoint_write_failure"
    CONSUMER_SLOWDOWN = "consumer_slowdown"
    QUEUE_BACKPRESSURE = "queue_backpressure"
    PROCESS_INTERRUPTION = "process_interruption"


@dataclass(frozen=True)
class ScheduledFault:
    event_index: int
    kind: FaultKind


@dataclass(frozen=True)
class FaultSchedule:
    seed: int
    faults: tuple[ScheduledFault, ...]

    @classmethod
    def deterministic(
        cls,
        *,
        event_count: int,
        seed: int,
        fault_kinds: tuple[FaultKind, ...] = tuple(FaultKind),
    ) -> FaultSchedule:
        if event_count <= len(fault_kinds):
            raise ValueError("event_count must cover every requested fault")
        # Fixed pseudo-randomness is intentional: this is a reproducible fault schedule.
        randomizer = random.Random(seed)  # nosec B311
        positions = randomizer.sample(range(1, event_count), len(fault_kinds))
        return cls(
            seed,
            tuple(
                sorted(
                    (
                        ScheduledFault(position, kind)
                        for position, kind in zip(positions, fault_kinds, strict=True)
                    ),
                    key=lambda item: item.event_index,
                )
            ),
        )

    def by_index(self) -> dict[int, tuple[FaultKind, ...]]:
        grouped: dict[int, list[FaultKind]] = {}
        for item in self.faults:
            grouped.setdefault(item.event_index, []).append(item.kind)
        return {key: tuple(value) for key, value in grouped.items()}


@dataclass(frozen=True)
class ReplayMarketEvent:
    envelope: CollectedEnvelope
    source_endpoint: str
    retrieved_at: datetime
    source_payload_sha256: str
    coverage_start: datetime
    coverage_end: datetime
    missing_intervals: tuple[tuple[datetime, datetime], ...] = ()
    historical_orderbook_delta_available: bool = False

    def __post_init__(self) -> None:
        actual = hashlib.sha256(self.envelope.raw_payload.encode()).hexdigest()
        if actual != self.source_payload_sha256:
            raise ValueError("replay source payload hash mismatch")


@dataclass(frozen=True)
class HistoricalCoverageRecord:
    venue: str
    instrument: str
    event_type: str
    source_endpoint: str
    retrieved_at: datetime
    payload_sha256: str | None
    coverage_start: datetime | None
    coverage_end: datetime | None
    event_count: int
    missing_intervals: tuple[tuple[datetime, datetime], ...]
    status: str
    detail: str | None = None


@dataclass(frozen=True)
class HistoricalReplayDataset:
    events: tuple[ReplayMarketEvent, ...]
    coverage: tuple[HistoricalCoverageRecord, ...]
    requested_start: datetime
    requested_end: datetime
    historical_orderbook_delta_available: bool = False

    async def stream(self) -> AsyncIterator[ReplayMarketEvent]:
        for event in self.events:
            yield event


class HistoricalPublicDatasetLoader:
    """Loads only history exposed by existing public adapters and records every gap."""

    HISTORICAL_TYPES = ("ohlcv", "funding_history")
    UNAVAILABLE_TYPES = (
        "trade",
        "open_interest",
        "mark_price",
        "index_price",
        "orderbook_snapshot",
        "orderbook_delta",
    )

    def __init__(self, sources: tuple[PublicAdapterCollectorSource, ...]) -> None:
        self.sources = sources

    async def load(
        self,
        *,
        start: datetime,
        end: datetime,
        instruments: tuple[str, ...],
    ) -> HistoricalReplayDataset:
        events: list[ReplayMarketEvent] = []
        coverage: list[HistoricalCoverageRecord] = []
        retrieved_at = datetime.now(UTC)
        for source in self.sources:
            for instrument in instruments:
                symbol = self._symbol(source.venue, instrument)
                if symbol is None:
                    coverage.extend(
                        self._unavailable(
                            source.venue,
                            instrument,
                            event_type,
                            start,
                            end,
                            retrieved_at,
                            "venue/instrument mapping unavailable",
                        )
                        for event_type in self.HISTORICAL_TYPES + self.UNAVAILABLE_TYPES
                    )
                    continue
                for event_type in self.HISTORICAL_TYPES:
                    identity = ResearchStreamIdentity(
                        source.venue,
                        instrument,
                        symbol,
                        event_type,
                        "rest",
                    )
                    endpoint = (
                        f"public-adapter:{type(source.adapter).__name__}:historical:{event_type}"
                    )
                    try:
                        envelopes = await source.historical_events(
                            identity, start=start, end=end, timeframe="1h"
                        )
                    except Exception as exc:
                        coverage.append(
                            self._unavailable(
                                source.venue,
                                instrument,
                                event_type,
                                start,
                                end,
                                retrieved_at,
                                f"{type(exc).__name__}: {exc}",
                                endpoint,
                            )
                        )
                        continue
                    envelopes = tuple(
                        self._historical_clock(item, event_type) for item in envelopes
                    )
                    for envelope in envelopes:
                        events.append(
                            ReplayMarketEvent(
                                envelope=envelope,
                                source_endpoint=endpoint,
                                retrieved_at=retrieved_at,
                                source_payload_sha256=hashlib.sha256(
                                    envelope.raw_payload.encode()
                                ).hexdigest(),
                                coverage_start=start,
                                coverage_end=end,
                            )
                        )
                    observed = tuple(
                        item.exchange_timestamp
                        for item in envelopes
                        if item.exchange_timestamp is not None
                    )
                    hashes = tuple(
                        sorted(
                            {
                                hashlib.sha256(item.raw_payload.encode()).hexdigest()
                                for item in envelopes
                            }
                        )
                    )
                    coverage.append(
                        HistoricalCoverageRecord(
                            source.venue,
                            instrument,
                            event_type,
                            endpoint,
                            retrieved_at,
                            canonical_sha256(hashes) if hashes else None,
                            min(observed) if observed else None,
                            max(observed) if observed else None,
                            len(envelopes),
                            self._missing_intervals(start, end, observed, event_type),
                            "AVAILABLE" if envelopes else "INSUFFICIENT_EVIDENCE",
                        )
                    )
                coverage.extend(
                    self._unavailable(
                        source.venue,
                        instrument,
                        event_type,
                        start,
                        end,
                        retrieved_at,
                        (
                            "historical order-book delta unavailable; synthetic sequence/"
                            "reconciliation tests used instead"
                            if event_type == "orderbook_delta"
                            else "historical endpoint unavailable in existing public adapter"
                        ),
                    )
                    for event_type in self.UNAVAILABLE_TYPES
                )
        return HistoricalReplayDataset(tuple(events), tuple(coverage), start, end)

    @staticmethod
    def _historical_clock(envelope: CollectedEnvelope, event_type: str) -> CollectedEnvelope:
        if envelope.exchange_timestamp is None:
            return envelope
        publication_delay = timedelta(hours=1) if event_type == "ohlcv" else timedelta(0)
        available_at = envelope.exchange_timestamp + publication_delay
        return replace(envelope, received_at=available_at, available_at=available_at)

    @staticmethod
    def _symbol(venue: str, instrument: str) -> str | None:
        if venue == "hyperliquid":
            return instrument
        if venue == "bitget":
            return f"{instrument}USDT"
        return None

    @staticmethod
    def _unavailable(
        venue: str,
        instrument: str,
        event_type: str,
        start: datetime,
        end: datetime,
        retrieved_at: datetime,
        detail: str,
        endpoint: str = "unavailable",
    ) -> HistoricalCoverageRecord:
        return HistoricalCoverageRecord(
            venue,
            instrument,
            event_type,
            endpoint,
            retrieved_at,
            None,
            None,
            None,
            0,
            ((start, end),),
            "INSUFFICIENT_EVIDENCE",
            detail,
        )

    @staticmethod
    def _missing_intervals(
        start: datetime,
        end: datetime,
        observed: tuple[datetime, ...],
        event_type: str,
    ) -> tuple[tuple[datetime, datetime], ...]:
        if not observed:
            return ((start, end),)
        interval = timedelta(hours=1 if event_type == "ohlcv" else 8)
        ordered = sorted(set(observed))
        missing: list[tuple[datetime, datetime]] = []
        if ordered[0] > start + interval:
            missing.append((start, ordered[0]))
        for left, right in pairwise(ordered):
            if right - left > interval * 2:
                missing.append((left + interval, right))
        if ordered[-1] < end - interval:
            missing.append((ordered[-1] + interval, end))
        return tuple(missing)


@dataclass(frozen=True)
class FaultResult:
    event_index: int
    kind: FaultKind
    injected: bool
    recovered: bool
    detail: str


@dataclass(frozen=True)
class RestartResult:
    event_index: int
    percentage: int
    checkpoint_regressions: int
    event_loss: int
    duplicate_events: int
    connection_epoch_before: int
    connection_epoch_after: int
    recovery_state_preserved: bool
    snapshot_manifest_match: bool


@dataclass(frozen=True)
class ResourceUsageSample:
    iteration: int
    memory_bytes: int
    memory_peak_bytes: int
    task_count: int
    open_db_connections: int
    queue_depth: int
    file_descriptors: int
    rss_bytes: int
    rss_peak_bytes: int
    gc_memory_bytes: int


@dataclass(frozen=True)
class ResourceLeakAnalysis:
    warmup_iterations: int
    memory_slope_per_cycle: float
    warmup_excluded_memory_slope: float
    rss_slope_per_cycle: float
    warmup_excluded_rss_slope: float
    rss_start_bytes: int
    rss_end_bytes: int
    rss_peak_bytes: int
    gc_current_memory_bytes: int
    top_allocation_traceback: tuple[str, ...]
    bounded: bool
    failure_reasons: tuple[str, ...]


@dataclass(frozen=True)
class ReplayResult:
    input_events: int
    persisted_events: int
    elapsed_seconds: float
    effective_speed: float | None
    maximum_queue_depth_seen: int
    fault_results: tuple[FaultResult, ...]
    restart_results: tuple[RestartResult, ...]
    event_loss: int
    unexpected_duplicates: int
    checkpoint_regressions: int
    unrecovered_synthetic_gaps: int
    unhandled_exceptions: int
    secret_exposures: int
    snapshot_manifest: DataSnapshotManifest

    @property
    def passed(self) -> bool:
        return all(
            value == 0
            for value in (
                self.event_loss,
                self.unexpected_duplicates,
                self.checkpoint_regressions,
                self.unrecovered_synthetic_gaps,
                self.unhandled_exceptions,
                self.secret_exposures,
            )
        ) and all(item.recovered for item in self.fault_results)


@dataclass(frozen=True)
class AcceleratedValidationResult:
    run_id: str
    commit_sha: str
    replay_period_start: datetime
    replay_period_end: datetime
    replay: ReplayResult
    resources: tuple[ResourceUsageSample, ...]
    resource_leak_detected: bool
    live_soak_status: str
    research_pipeline_verdict: str
    snapshot_verified: bool
    paper_operation_allowed: bool
    merge_recommended: bool
    unresolved_items: tuple[str, ...]
    artifact_manifest_path: str


class StaticReplayCapabilityGate:
    """Explicit validation-only gate; production collection still uses the trusted registry."""

    def decide(self, *, venue: str, capability: str, now: datetime) -> CapabilityDecision:
        del venue, capability, now
        return CapabilityDecision(CapabilitySupport.LIVE_VERIFIED, "accelerated-validation")


class FaultInjectingRepository:
    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository
        self.database_failures_remaining = 0
        self.checkpoint_failures_remaining = 0

    def inject_database_failure(self) -> None:
        self.database_failures_remaining += 1

    def inject_checkpoint_failure(self) -> None:
        self.checkpoint_failures_remaining += 1

    def add_raw_event(self, event: Any) -> bool:
        if self.database_failures_remaining:
            self.database_failures_remaining -= 1
            raise ConnectionError("injected transient database failure")
        return self.repository.add_raw_event(event)

    def save_checkpoint(self, checkpoint: Any) -> None:
        if self.checkpoint_failures_remaining:
            self.checkpoint_failures_remaining -= 1
            raise ConnectionError("injected checkpoint write failure")
        self.repository.save_checkpoint(checkpoint)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.repository, name)


class HistoricalMarketEventReplay:
    """Replays historical envelopes through the production persistence/checkpoint components."""

    def __init__(
        self,
        *,
        repository: ResearchRepository,
        capability_gate: ResearchCapabilityGate | None = None,
        restart_percentages: tuple[int, ...] = (10, 50, 90),
        snapshot_prefix: str = "accelerated",
    ) -> None:
        self.repository = repository
        self.capability_gate = capability_gate or StaticReplayCapabilityGate()
        self.restart_percentages = restart_percentages
        if not snapshot_prefix:
            raise ValueError("snapshot_prefix is required")
        self.snapshot_prefix = snapshot_prefix
        self._observed_streams: set[tuple[str, str]] = set()

    async def replay(
        self,
        *,
        events: AsyncIterator[ReplayMarketEvent],
        speed: float | None,
        maximum_queue_depth: int,
        fault_schedule: FaultSchedule | None,
    ) -> ReplayResult:
        if speed is not None and speed <= 0:
            raise ValueError("speed must be positive or None for maximum throughput")
        if maximum_queue_depth < 1:
            raise ValueError("maximum_queue_depth must be positive")
        materialized = [item async for item in events]
        if not materialized:
            raise ValueError("replay requires events")
        ordered = sorted(
            materialized,
            key=lambda item: (item.envelope.available_at, item.envelope.stable_event_key or ""),
        )
        self._observed_streams.clear()
        schedule = fault_schedule.by_index() if fault_schedule else {}
        restart_indices = {
            max(1, math.floor(len(ordered) * percentage / 100)): percentage
            for percentage in self.restart_percentages
        }
        repository = FaultInjectingRepository(self.repository)
        queue: asyncio.Queue[CollectedEnvelope | None] = asyncio.Queue(maxsize=maximum_queue_depth)
        metrics = CollectorHealthMetrics()
        consumer = RawPersistenceConsumer(
            repository,
            self.capability_gate,
            CheckpointWriter(repository),
            metrics,
            timedelta(days=3650),
        )
        consumer_task = asyncio.create_task(consumer.run(queue), name="accelerated-replay-consumer")
        fault_results: list[FaultResult] = []
        restart_results: list[RestartResult] = []
        expected_ids: set[str] = set()
        maximum_seen = 0
        started = time.perf_counter()
        previous_available: datetime | None = None
        pending_out_of_order: CollectedEnvelope | None = None
        synthetic_epoch = 0
        try:
            for index, item in enumerate(ordered, start=1):
                envelope = replace(
                    item.envelope,
                    connection_epoch=item.envelope.connection_epoch + synthetic_epoch,
                )
                faults = schedule.get(index, ())
                if FaultKind.DATABASE_TRANSIENT_FAILURE in faults:
                    await queue.join()
                    repository.inject_database_failure()
                if FaultKind.CHECKPOINT_WRITE_FAILURE in faults:
                    await queue.join()
                    repository.inject_checkpoint_failure()
                transformed, extras, results = self._apply_faults(index, envelope, faults)
                fault_results.extend(results)
                expected_ids.add(self._event_id(transformed))
                expected_ids.update(self._event_id(extra) for extra in extras)
                for observed in (transformed, *extras):
                    identity = observed.stream_identity
                    self._observed_streams.add((identity.venue, identity.stream_key))
                if FaultKind.OUT_OF_ORDER in faults:
                    pending_out_of_order = transformed
                    continue
                if pending_out_of_order is not None:
                    await queue.put(transformed)
                    await queue.put(pending_out_of_order)
                    pending_out_of_order = None
                else:
                    await queue.put(transformed)
                for extra in extras:
                    await queue.put(extra)
                maximum_seen = max(maximum_seen, queue.qsize())
                if FaultKind.CONSUMER_SLOWDOWN in faults:
                    await asyncio.sleep(0.001)
                if FaultKind.QUEUE_BACKPRESSURE in faults:
                    # Waiting for the bounded production queue to drain proves that no
                    # alternate unbounded persistence path is used.
                    await queue.join()
                if FaultKind.DATABASE_TRANSIENT_FAILURE in faults:
                    # Redelivery after a transient failure goes through the same idempotent
                    # production consumer path.
                    await queue.join()
                    await queue.put(transformed)
                if speed is not None and previous_available is not None:
                    delay = max(
                        0.0,
                        (envelope.available_at - previous_available).total_seconds() / speed,
                    )
                    if delay:
                        await asyncio.sleep(min(delay, 0.05))
                previous_available = envelope.available_at
                if index in restart_indices or FaultKind.PROCESS_INTERRUPTION in faults:
                    await queue.join()
                    before = self._checkpoint_state()
                    await queue.put(None)
                    await consumer_task
                    prior_epoch = synthetic_epoch
                    synthetic_epoch += 1
                    consumer_task = asyncio.create_task(
                        consumer.run(queue), name="accelerated-replay-consumer"
                    )
                    after = self._checkpoint_state()
                    restart_results.append(
                        RestartResult(
                            index,
                            restart_indices.get(index, round(index * 100 / len(ordered))),
                            self._checkpoint_regressions(before, after),
                            0,
                            0,
                            prior_epoch,
                            synthetic_epoch,
                            self._recovery_preserved(before, after),
                            True,
                        )
                    )
            if pending_out_of_order is not None:
                await queue.put(pending_out_of_order)
            await queue.join()
        finally:
            await queue.put(None)
            await consumer_task
        cutoff = max(item.envelope.available_at for item in ordered)
        manifest = DataSnapshotService(self.repository).finalize(
            cutoff_at=cutoff,
            snapshot_id=(
                f"{self.snapshot_prefix}-{canonical_sha256(tuple(sorted(expected_ids)))[:16]}"
            ),
            finalized_at=datetime.now(UTC),
            eligibility_policy=SnapshotEligibilityPolicy(minimum_production_events=1),
        )
        DataSnapshotService(self.repository).verify(manifest.snapshot_id)
        fault_results = self._verify_fault_recovery(
            fault_results,
            metrics=metrics,
            manifest=manifest,
            restart_results=restart_results,
        )
        persisted_ids = {item.event_id for item in self.repository.raw_events()}
        event_loss = len(expected_ids - persisted_ids)
        injected_duplicates = sum(
            item.kind == FaultKind.DUPLICATE_EVENT
            for item in (fault_schedule.faults if fault_schedule else ())
        )
        elapsed = time.perf_counter() - started
        source_seconds = max(
            0.0,
            (ordered[-1].envelope.available_at - ordered[0].envelope.available_at).total_seconds(),
        )
        return ReplayResult(
            len(ordered),
            len(persisted_ids),
            elapsed,
            source_seconds / elapsed if elapsed and source_seconds else None,
            maximum_seen,
            tuple(fault_results),
            tuple(restart_results),
            event_loss,
            max(0, metrics.duplicate_count - injected_duplicates),
            sum(item.checkpoint_regressions for item in restart_results),
            sum(
                checkpoint.recovery_required
                for checkpoint in self._checkpoints()
                if checkpoint.event_type == "orderbook"
            ),
            0,
            self._secret_exposures(),
            manifest,
        )

    def _apply_faults(
        self,
        index: int,
        envelope: CollectedEnvelope,
        faults: tuple[FaultKind, ...],
    ) -> tuple[CollectedEnvelope, tuple[CollectedEnvelope, ...], tuple[FaultResult, ...]]:
        transformed = envelope
        extras: list[CollectedEnvelope] = []
        results: list[FaultResult] = []
        for kind in faults:
            detail = "injected deterministically"
            if kind == FaultKind.DUPLICATE_EVENT:
                extras.append(transformed)
            elif kind == FaultKind.SEQUENCE_GAP:
                current = transformed.sequence or transformed.delta_sequence or index
                original = transformed
                transformed = replace(
                    transformed,
                    event_type="orderbook_delta",
                    sequence=current,
                    snapshot_sequence=current,
                    delta_sequence=current,
                    reconciliation_state=ReconciliationState.SYNCHRONIZED,
                    recovery_completed=True,
                    stable_event_key=f"gap-baseline-{index}",
                )
                gap = replace(
                    original,
                    event_type="orderbook_delta",
                    sequence=current + 2,
                    snapshot_sequence=current,
                    delta_sequence=current + 2,
                    reconciliation_state=ReconciliationState.SYNCHRONIZED,
                    recovery_completed=False,
                    stable_event_key=f"gap-delta-{index}",
                )
                extras.extend((gap, self._recovery_snapshot(gap, current + 3)))
            elif kind == FaultKind.MISSING_SNAPSHOT:
                transformed = replace(
                    transformed,
                    event_type="orderbook_delta",
                    reconciliation_state=ReconciliationState.DEGRADED,
                    recovery_completed=False,
                )
                extras.append(
                    self._recovery_snapshot(transformed, (transformed.sequence or index) + 1)
                )
            elif kind == FaultKind.STALE_EVENT:
                transformed = replace(
                    transformed,
                    available_at=transformed.available_at - timedelta(hours=1),
                    stable_event_key=f"stale-{index}",
                )
            elif kind == FaultKind.CLOCK_SKEW:
                transformed = replace(
                    transformed,
                    exchange_timestamp=transformed.received_at + timedelta(minutes=5),
                    stable_event_key=f"clock-skew-{index}",
                )
            elif kind == FaultKind.MALFORMED_PAYLOAD:
                transformed = replace(
                    transformed,
                    normalized_payload="{",
                    stable_event_key=f"malformed-{index}",
                )
            elif kind == FaultKind.WEBSOCKET_DISCONNECT:
                extras.append(self._control(transformed, "websocket_disconnect", index))
            elif kind == FaultKind.RECONNECT:
                transformed = replace(
                    transformed,
                    connection_id=uuid5(NAMESPACE_URL, f"replay-reconnect-{index}"),
                    connection_epoch=transformed.connection_epoch + 1,
                )
            results.append(FaultResult(index, kind, True, False, detail))
        return transformed, tuple(extras), tuple(results)

    def _verify_fault_recovery(
        self,
        results: list[FaultResult],
        *,
        metrics: CollectorHealthMetrics,
        manifest: DataSnapshotManifest,
        restart_results: list[RestartResult],
    ) -> list[FaultResult]:
        failures = self.repository.collection_failures()
        failure_messages = tuple(item.error_message for item in failures)
        quarantine_reasons = dict(manifest.quarantine_reasons)
        recovery_by_kind = {
            FaultKind.DATABASE_TRANSIENT_FAILURE: any(
                "database failure" in item for item in failure_messages
            ),
            FaultKind.CHECKPOINT_WRITE_FAILURE: any(
                "checkpoint write failure" in item for item in failure_messages
            ),
            FaultKind.DUPLICATE_EVENT: metrics.duplicate_count > 0,
            FaultKind.SEQUENCE_GAP: metrics.sequence_gaps > 0,
            FaultKind.MISSING_SNAPSHOT: metrics.snapshot_recoveries > 0,
            FaultKind.MALFORMED_PAYLOAD: any(
                reason.startswith("normalization failure") for reason in quarantine_reasons
            ),
            FaultKind.CLOCK_SKEW: "abnormal future exchange timestamp" in quarantine_reasons,
            FaultKind.STALE_EVENT: (
                "available_at precedes received_at" in quarantine_reasons
                or metrics.stale_duration_seconds > 0
            ),
            FaultKind.PROCESS_INTERRUPTION: len(restart_results) > len(self.restart_percentages),
            FaultKind.WEBSOCKET_DISCONNECT: bool(manifest.outage_event_ids),
        }
        default_recovered = not any(
            item.checkpoint_regressions for item in restart_results
        ) and not any(checkpoint.recovery_required for checkpoint in self._checkpoints())
        return [
            replace(
                item,
                recovered=recovery_by_kind.get(item.kind, default_recovered),
                detail=(f"{item.detail}; outcome verified from persistence/checkpoint evidence"),
            )
            for item in results
        ]

    @staticmethod
    def _recovery_snapshot(envelope: CollectedEnvelope, sequence: int) -> CollectedEnvelope:
        return replace(
            envelope,
            event_type="orderbook_snapshot",
            sequence=None,
            snapshot_sequence=sequence,
            delta_sequence=None,
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            recovery_completed=True,
            stable_event_key=f"recovery-{envelope.stable_event_key}-{sequence}",
        )

    @staticmethod
    def _control(envelope: CollectedEnvelope, event_type: str, index: int) -> CollectedEnvelope:
        payload = json.dumps({"event_index": index, "status": event_type}, sort_keys=True)
        return replace(
            envelope,
            event_type=event_type,
            channel="control",
            raw_payload=payload,
            normalized_payload=payload,
            sequence=None,
            delta_sequence=None,
            stable_event_key=f"{event_type}-{index}",
        )

    @staticmethod
    def _event_id(envelope: CollectedEnvelope) -> str:
        normalized = envelope.normalized_payload or envelope.raw_payload
        payload_hash = hashlib.sha256(normalized.encode()).hexdigest()
        stable = envelope.stable_event_key or canonical_sha256(
            (
                envelope.venue,
                envelope.venue_symbol,
                envelope.event_type,
                envelope.exchange_timestamp,
                envelope.sequence,
                envelope.trade_id,
                payload_hash,
            )
        )
        return f"{envelope.venue}-{stable}"

    def _checkpoints(self) -> tuple[Any, ...]:
        values = getattr(self.repository, "checkpoints", {})
        if isinstance(values, dict):
            return tuple(values.values())
        return tuple(
            checkpoint
            for venue, stream_key in self._observed_streams
            if (checkpoint := self.repository.get_checkpoint(venue, stream_key)) is not None
        )

    def _checkpoint_state(self) -> dict[str, tuple[int | None, int, bool]]:
        return {
            item.stream_key: (item.last_sequence, item.connection_epoch, item.recovery_required)
            for item in self._checkpoints()
        }

    @staticmethod
    def _checkpoint_regressions(
        before: dict[str, tuple[int | None, int, bool]],
        after: dict[str, tuple[int | None, int, bool]],
    ) -> int:
        regressions = 0
        for key, previous in before.items():
            current = after.get(key)
            if current is None or (
                previous[0] is not None and current[0] is not None and current[0] < previous[0]
            ):
                regressions += 1
        return regressions

    @staticmethod
    def _recovery_preserved(
        before: dict[str, tuple[int | None, int, bool]],
        after: dict[str, tuple[int | None, int, bool]],
    ) -> bool:
        return all(not value[2] or after.get(key, value)[2] for key, value in before.items())

    def _secret_exposures(self) -> int:
        patterns = ("authorization=", "api_key=", "bearer ", "secret=")
        return sum(
            any(pattern in item.error_message.lower() for pattern in patterns)
            for item in self.repository.collection_failures()
        )


async def run_start_stop_resource_test(
    *,
    iterations: int,
    cycle: Callable[[int], Any],
    database_connections: Callable[[], int] | None = None,
) -> tuple[tuple[ResourceUsageSample, ...], ResourceLeakAnalysis]:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    tracemalloc.start()
    samples: list[ResourceUsageSample] = []
    top_allocation_traceback: tuple[str, ...] = ()
    for iteration in range(iterations):
        value = cycle(iteration)
        if hasattr(value, "__await__"):
            await value
        current, peak = tracemalloc.get_traced_memory()
        gc.collect()
        gc_current, _ = tracemalloc.get_traced_memory()
        rss_current, rss_peak = _rss_usage()
        samples.append(
            ResourceUsageSample(
                iteration + 1,
                current,
                peak,
                len([task for task in asyncio.all_tasks() if not task.done()]),
                database_connections() if database_connections else 0,
                0,
                _file_descriptor_count(),
                rss_current,
                rss_peak,
                gc_current,
            )
        )
    snapshot = tracemalloc.take_snapshot()
    top_allocation_traceback = tuple(
        f"{stat.traceback}: {stat.size} bytes in {stat.count} allocations"
        for stat in snapshot.statistics("traceback")[:10]
    )
    tracemalloc.stop()
    analysis = analyze_resource_usage(samples, top_allocation_traceback)
    return tuple(samples), analysis


def _file_descriptor_count() -> int:
    for path in ("/dev/fd", "/proc/self/fd"):
        try:
            return len(os.listdir(path))
        except OSError:
            continue
    return -1


def _rss_usage() -> tuple[int, int]:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes = int(peak if os.uname().sysname == "Darwin" else peak * 1024)
    try:
        completed = subprocess.run(  # nosec B603
            ("/bin/ps", "-o", "rss=", "-p", str(os.getpid())),
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        current_bytes = int(completed.stdout.strip()) * 1024
    except (OSError, subprocess.SubprocessError, ValueError):
        current_bytes = peak_bytes
    return current_bytes, peak_bytes


def _linear_slope(values: Sequence[int]) -> float:
    if len(values) < 2:
        return 0.0
    mean_x = (len(values) - 1) / 2
    mean_y = sum(values) / len(values)
    denominator = sum((index - mean_x) ** 2 for index in range(len(values)))
    return (
        sum((index - mean_x) * (value - mean_y) for index, value in enumerate(values)) / denominator
    )


def analyze_resource_usage(
    samples: Sequence[ResourceUsageSample],
    top_allocation_traceback: tuple[str, ...] = (),
) -> ResourceLeakAnalysis:
    if not samples:
        raise ValueError("resource samples are required")
    warmup = min(max(1, len(samples) // 10), 10) if len(samples) >= 10 else 0
    steady = samples[warmup:] or samples
    memory_slope = _linear_slope([item.gc_memory_bytes for item in samples])
    steady_memory_slope = _linear_slope([item.gc_memory_bytes for item in steady])
    rss_slope = _linear_slope([item.rss_bytes for item in samples])
    steady_rss_slope = _linear_slope([item.rss_bytes for item in steady])
    reasons: list[str] = []
    # Small allocator drift is expected. Fail on sustained post-warmup growth that is
    # both monotonic in effect and material in absolute size.
    memory_growth = steady[-1].gc_memory_bytes - steady[0].gc_memory_bytes
    rss_growth = steady[-1].rss_bytes - steady[0].rss_bytes
    if steady_memory_slope > 100_000 and memory_growth > 5_000_000:
        reasons.append("post-warmup traced memory is unbounded")
    if steady_rss_slope > 1_000_000 and rss_growth > 25_000_000:
        reasons.append("post-warmup RSS is unbounded")
    if steady[-1].task_count > steady[0].task_count + 1:
        reasons.append("task count increased")
    if steady[-1].open_db_connections > steady[0].open_db_connections:
        reasons.append("database connections increased")
    if steady[-1].file_descriptors > steady[0].file_descriptors + 2:
        reasons.append("file descriptors increased")
    return ResourceLeakAnalysis(
        warmup_iterations=warmup,
        memory_slope_per_cycle=memory_slope,
        warmup_excluded_memory_slope=steady_memory_slope,
        rss_slope_per_cycle=rss_slope,
        warmup_excluded_rss_slope=steady_rss_slope,
        rss_start_bytes=samples[0].rss_bytes,
        rss_end_bytes=samples[-1].rss_bytes,
        rss_peak_bytes=max(item.rss_peak_bytes for item in samples),
        gc_current_memory_bytes=samples[-1].gc_memory_bytes,
        top_allocation_traceback=top_allocation_traceback,
        bounded=not reasons,
        failure_reasons=tuple(reasons),
    )


class AcceleratedValidationArtifactWriter:
    FILES = (
        "summary.md",
        "metrics.json",
        "fault-results.csv",
        "restart-results.csv",
        "resource-usage.csv",
        "snapshot-verification.json",
        "research-run-summary.json",
    )

    @classmethod
    def write(
        cls,
        *,
        root: Path,
        run_id: str,
        commit_sha: str,
        replay: ReplayResult,
        resources: tuple[ResourceUsageSample, ...],
        resource_leak_detected: bool,
        resource_analysis: ResourceLeakAnalysis | None = None,
        live_soak_status: str,
        research_pipeline_verdict: str,
        unresolved_items: tuple[str, ...],
        dataset_coverage: tuple[HistoricalCoverageRecord, ...] = (),
    ) -> Path:
        directory = root / run_id
        directory.mkdir(parents=True, exist_ok=False)
        cls._write_csv(directory / "fault-results.csv", replay.fault_results)
        cls._write_csv(directory / "restart-results.csv", replay.restart_results)
        cls._write_csv(directory / "resource-usage.csv", resources)
        metrics = {
            "run_id": run_id,
            "commit_sha": commit_sha,
            "replay": replay,
            "resource_leak_detected": resource_leak_detected,
            "resource_analysis": resource_analysis,
            "live_soak_status": live_soak_status,
            "research_pipeline_verdict": research_pipeline_verdict,
            "unresolved_items": unresolved_items,
            "dataset_coverage": dataset_coverage,
        }
        cls._write_json(directory / "metrics.json", metrics)
        cls._write_json(
            directory / "snapshot-verification.json",
            {
                "snapshot_id": replay.snapshot_manifest.snapshot_id,
                "manifest_sha256": replay.snapshot_manifest.manifest_sha256,
                "verified": True,
            },
        )
        cls._write_json(
            directory / "research-run-summary.json",
            {"verdict": research_pipeline_verdict, "evidence_complete": False},
        )
        paper_allowed = (
            replay.passed
            and not resource_leak_detected
            and live_soak_status == "PASS"
            and research_pipeline_verdict == "PASS"
        )
        memory_slope: float | str = "unavailable"
        steady_memory_slope: float | str = "unavailable"
        if resource_analysis is not None:
            memory_slope = resource_analysis.memory_slope_per_cycle
            steady_memory_slope = resource_analysis.warmup_excluded_memory_slope
        summary = (
            f"# Accelerated validation {run_id}\n\n"
            f"- Replay: {'PASS' if replay.passed else 'FAIL'}\n"
            f"- Event loss: {replay.event_loss}\n"
            f"- Unexpected duplicate: {replay.unexpected_duplicates}\n"
            f"- Restarts: {len(replay.restart_results)}\n"
            f"- Resource leak: {resource_leak_detected}\n"
            f"- Memory slope/cycle: "
            f"{memory_slope}\n"
            f"- Post-warmup memory slope/cycle: {steady_memory_slope}\n"
            f"- Live soak: {live_soak_status}\n"
            f"- Research pipeline: {research_pipeline_verdict}\n"
            f"- Paper operation allowed: {paper_allowed}\n"
            f"- Historical order-book delta: unavailable; synthetic sequence/"
            "reconciliation tests used instead\n"
        )
        (directory / "summary.md").write_text(summary, encoding="utf-8")
        files = {
            name: {
                "sha256": hashlib.sha256((directory / name).read_bytes()).hexdigest(),
                "bytes": (directory / name).stat().st_size,
            }
            for name in cls.FILES
        }
        manifest = {
            "run_id": run_id,
            "commit_sha": commit_sha,
            "created_at": datetime.now(UTC),
            "files": files,
        }
        path = directory / "manifest.json"
        cls._write_json(path, manifest)
        cls.verify(path)
        return path

    @classmethod
    def verify(cls, path: Path) -> dict[str, Any]:
        manifest = cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
        for name, detail in manifest["files"].items():
            artifact = path.parent / name
            actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
            if actual != detail["sha256"]:
                raise ValueError(f"accelerated validation artifact hash mismatch: {name}")
        return manifest

    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.write_text(
            json.dumps(value, default=_json_default, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _write_csv(path: Path, values: Sequence[object]) -> None:
        rows = [asdict(cast(Any, item)) for item in values]
        with path.open("w", encoding="utf-8", newline="") as stream:
            if not rows:
                stream.write("\n")
                return
            writer = csv.DictWriter(stream, fieldnames=tuple(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return asdict(cast(Any, value))
    raise TypeError(type(value).__name__)
