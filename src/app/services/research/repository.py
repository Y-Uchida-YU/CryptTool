from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.adapters.exchanges.websocket import ReconciliationState
from app.infrastructure.database.models import (
    CollectionFailureEventRow,
    DataSnapshotEventRow,
    DataSnapshotRow,
    ExperimentalMarketEventRow,
    FrozenHypothesisRow,
    InstrumentRuleSnapshotRow,
    MarketDataCheckpointRow,
    MarketDataQuarantineRow,
    RawMarketEventRow,
    RawMarketPayloadRow,
    ResearchArtifactRow,
    ResearchRunRow,
    ShadowInputCheckpointRow,
    ShadowInputCursorRow,
)
from app.services.research.models import (
    AvailabilityProvenance,
    CollectionCheckpoint,
    CollectionFailureEvent,
    DataSnapshotManifest,
    ExperimentalEventCursor,
    FeeTierKind,
    FrozenHypothesis,
    InstrumentRuleSnapshot,
    RawMarketEvent,
    ResearchRunIdentity,
    RuleVerificationStatus,
    ShadowInputCheckpoint,
    TimestampSemantic,
)


def _shadow_event_stream(event_type: str) -> str:
    return "funding_current" if event_type == "funding_current" else "orderbook_snapshot"


def _cursor_predicates(
    cursors: tuple[ExperimentalEventCursor, ...],
    *,
    venues: tuple[str, ...],
) -> tuple[Any, ...]:
    cursor_map = {(item.venue, item.event_stream): item for item in cursors}
    predicates: list[Any] = []
    for venue in venues:
        for stream, event_types in (
            ("funding_current", ("funding_current",)),
            ("orderbook_snapshot", ("orderbook_snapshot", "orderbook_delta")),
        ):
            cursor = cursor_map.get((venue, stream))
            ordering: Any = True
            if cursor is not None and cursor.last_available_at is not None:
                ordering = or_(
                    ExperimentalMarketEventRow.available_at > cursor.last_available_at,
                    and_(
                        ExperimentalMarketEventRow.available_at == cursor.last_available_at,
                        ExperimentalMarketEventRow.event_id > (cursor.last_event_id or ""),
                    ),
                )
            predicates.append(
                and_(
                    ExperimentalMarketEventRow.venue == venue,
                    ExperimentalMarketEventRow.event_type.in_(event_types),
                    ordering,
                )
            )
    return tuple(predicates)


class ResearchRepository(Protocol):
    def add_raw_event(self, event: RawMarketEvent) -> bool: ...

    def get_raw_event(self, event_id: str) -> RawMarketEvent | None: ...

    def raw_events(self) -> tuple[RawMarketEvent, ...]: ...

    def add_experimental_event(self, event: RawMarketEvent, support: str) -> bool: ...

    def get_experimental_event(self, event_id: str) -> RawMarketEvent | None: ...

    def experimental_event_count(self) -> int: ...

    def list_experimental_events(self) -> tuple[RawMarketEvent, ...]: ...

    def list_experimental_events_after(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
        limit: int,
    ) -> tuple[RawMarketEvent, ...]: ...

    def experimental_event_backlog(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
    ) -> tuple[int, datetime | None]: ...

    def shadow_input_checkpoint(
        self, run_id: str, *, instrument: str = "BTC"
    ) -> tuple[tuple[ExperimentalEventCursor, ...], ShadowInputCheckpoint | None]: ...

    def commit_shadow_input_checkpoint(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        checkpoint: ShadowInputCheckpoint,
    ) -> None: ...

    def list_experimental_event_ids_between(
        self,
        *,
        venue: str,
        instrument: str,
        event_type: str,
        after_available_at: datetime,
        after_event_id: str,
        through_available_at: datetime,
        through_event_id: str,
    ) -> tuple[str, ...]: ...

    def save_raw_payload(
        self,
        *,
        payload_id: str,
        venue: str,
        source_endpoint: str,
        payload_sha256: str,
        raw_payload: str,
        received_at: datetime,
    ) -> bool: ...

    def purge_raw_payloads_before(self, cutoff_at: datetime) -> int: ...

    def quarantine(self, event: RawMarketEvent, reason: str, at: datetime) -> None: ...

    def quarantine_count(self, event_ids: tuple[str, ...] | None = None) -> int: ...

    def quarantine_summary(
        self, cutoff_at: datetime
    ) -> tuple[tuple[str, ...], tuple[tuple[str, int], ...]]: ...

    def save_collection_failure(self, failure: CollectionFailureEvent) -> None: ...

    def collection_failures(self) -> tuple[CollectionFailureEvent, ...]: ...

    def save_snapshot(
        self, snapshot_id: str, cutoff_at: datetime, event_count: int, content_sha256: str
    ) -> None: ...

    def finalize_snapshot(self, manifest: DataSnapshotManifest) -> None: ...

    def snapshot_manifest(self, snapshot_id: str) -> DataSnapshotManifest | None: ...

    def snapshot_events(self, snapshot_id: str) -> tuple[RawMarketEvent, ...]: ...

    def save_checkpoint(self, checkpoint: CollectionCheckpoint) -> None: ...

    def get_checkpoint(
        self, venue: str, stream_key: str, checkpoint_namespace: str = "production"
    ) -> CollectionCheckpoint | None: ...

    def list_checkpoints(
        self, checkpoint_namespace: str = "production"
    ) -> tuple[CollectionCheckpoint, ...]: ...

    def save_instrument_rule(self, rule: InstrumentRuleSnapshot) -> None: ...

    def instrument_rules(
        self, rule_snapshot_ids: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]: ...

    def instrument_rules_at(self, cutoff_at: datetime) -> tuple[InstrumentRuleSnapshot, ...]: ...

    def freeze_hypothesis(self, hypothesis: FrozenHypothesis) -> None: ...

    def save_run(self, identity: ResearchRunIdentity, status: str, verdict: str | None) -> None: ...

    def save_artifact(
        self,
        run_id: str,
        data_snapshot_id: str,
        artifact_type: str,
        path: str,
        content_sha256: str,
        created_at: datetime,
    ) -> None: ...


class NamespacedResearchRepository:
    """Shares immutable events while isolating mutable collector checkpoints by run."""

    def __init__(self, repository: ResearchRepository, checkpoint_namespace: str) -> None:
        if not checkpoint_namespace or checkpoint_namespace == "production":
            raise ValueError("test collectors require a non-production checkpoint namespace")
        self.repository = repository
        self.checkpoint_namespace = checkpoint_namespace

    def save_checkpoint(self, checkpoint: CollectionCheckpoint) -> None:
        self.repository.save_checkpoint(
            replace(checkpoint, checkpoint_namespace=self.checkpoint_namespace)
        )

    def get_checkpoint(
        self,
        venue: str,
        stream_key: str,
        checkpoint_namespace: str = "production",
    ) -> CollectionCheckpoint | None:
        del checkpoint_namespace
        return self.repository.get_checkpoint(
            venue, stream_key, checkpoint_namespace=self.checkpoint_namespace
        )

    def list_checkpoints(
        self, checkpoint_namespace: str = "production"
    ) -> tuple[CollectionCheckpoint, ...]:
        del checkpoint_namespace
        return self.repository.list_checkpoints(self.checkpoint_namespace)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.repository, name)


class InMemoryResearchRepository:
    def __init__(self) -> None:
        self.events: dict[str, RawMarketEvent] = {}
        self.quarantined: list[tuple[RawMarketEvent, str, datetime]] = []
        self.snapshots: dict[str, tuple[datetime, int, str]] = {}
        self.snapshot_manifests: dict[str, DataSnapshotManifest] = {}
        self.experimental_events: dict[str, tuple[RawMarketEvent, str]] = {}
        self.shadow_cursors: dict[tuple[str, str, str, str], ExperimentalEventCursor] = {}
        self.shadow_checkpoints: dict[tuple[str, str], ShadowInputCheckpoint] = {}
        self.raw_payloads: dict[str, tuple[str, str, str, str, datetime]] = {}
        self.checkpoints: dict[tuple[str, str, str], CollectionCheckpoint] = {}
        self.rules: dict[str, InstrumentRuleSnapshot] = {}
        self.failures: list[CollectionFailureEvent] = []
        self.runs: dict[str, tuple[ResearchRunIdentity, str, str | None]] = {}
        self.artifacts: list[tuple[str, str, str, str, str, datetime]] = []
        self.hypotheses: dict[tuple[str, str], FrozenHypothesis] = {}

    def add_raw_event(self, event: RawMarketEvent) -> bool:
        if event.event_id in self.events:
            return False
        self.events[event.event_id] = event
        return True

    def raw_events(self) -> tuple[RawMarketEvent, ...]:
        return tuple(self.events.values())

    def add_experimental_event(self, event: RawMarketEvent, support: str) -> bool:
        if event.event_id in self.experimental_events:
            return False
        self.experimental_events[event.event_id] = (event, support)
        return True

    def get_experimental_event(self, event_id: str) -> RawMarketEvent | None:
        value = self.experimental_events.get(event_id)
        return value[0] if value is not None else None

    def experimental_event_count(self) -> int:
        return len(self.experimental_events)

    def list_experimental_events(self) -> tuple[RawMarketEvent, ...]:
        return tuple(item[0] for item in self.experimental_events.values())

    def list_experimental_events_after(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
        limit: int,
    ) -> tuple[RawMarketEvent, ...]:
        cursor_map = {(item.venue, item.event_stream): item for item in cursors}

        def unseen(event: RawMarketEvent) -> bool:
            stream = _shadow_event_stream(event.event_type)
            cursor = cursor_map.get((event.venue, stream))
            return (
                cursor is None
                or cursor.last_available_at is None
                or (event.available_at, event.event_id)
                > (cursor.last_available_at, cursor.last_event_id or "")
            )

        return tuple(
            sorted(
                (
                    item[0]
                    for item in self.experimental_events.values()
                    if item[0].venue in venues
                    and item[0].canonical_instrument_id == instrument
                    and item[0].event_type in event_types
                    and unseen(item[0])
                ),
                key=lambda item: (item.available_at, item.event_id),
            )[:limit]
        )

    def experimental_event_backlog(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
    ) -> tuple[int, datetime | None]:
        events = self.list_experimental_events_after(
            cursors,
            venues=venues,
            instrument=instrument,
            event_types=event_types,
            limit=len(self.experimental_events) + 1,
        )
        latest = max(
            (
                item[0].available_at
                for item in self.experimental_events.values()
                if item[0].venue in venues
                and item[0].canonical_instrument_id == instrument
                and item[0].event_type in event_types
            ),
            default=None,
        )
        return len(events), latest

    def shadow_input_checkpoint(
        self, run_id: str, *, instrument: str = "BTC"
    ) -> tuple[tuple[ExperimentalEventCursor, ...], ShadowInputCheckpoint | None]:
        cursors = tuple(
            sorted(
                (
                    item
                    for key, item in self.shadow_cursors.items()
                    if key[0] == run_id and item.instrument == instrument
                ),
                key=lambda item: (item.venue, item.event_stream),
            )
        )
        return cursors, self.shadow_checkpoints.get((run_id, instrument))

    def commit_shadow_input_checkpoint(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        checkpoint: ShadowInputCheckpoint,
    ) -> None:
        for item in cursors:
            self.shadow_cursors[(item.run_id, item.venue, item.instrument, item.event_stream)] = (
                item
            )
        self.shadow_checkpoints[(checkpoint.run_id, checkpoint.instrument)] = checkpoint

    def list_experimental_event_ids_between(
        self,
        *,
        venue: str,
        instrument: str,
        event_type: str,
        after_available_at: datetime,
        after_event_id: str,
        through_available_at: datetime,
        through_event_id: str,
    ) -> tuple[str, ...]:
        return tuple(
            event.event_id
            for event in sorted(
                (
                    item[0]
                    for item in self.experimental_events.values()
                    if item[0].venue == venue
                    and item[0].canonical_instrument_id == instrument
                    and item[0].event_type == event_type
                    and (item[0].available_at, item[0].event_id)
                    > (after_available_at, after_event_id)
                    and (item[0].available_at, item[0].event_id)
                    <= (through_available_at, through_event_id)
                ),
                key=lambda item: (item.available_at, item.event_id),
            )
        )

    def save_raw_payload(
        self,
        *,
        payload_id: str,
        venue: str,
        source_endpoint: str,
        payload_sha256: str,
        raw_payload: str,
        received_at: datetime,
    ) -> bool:
        if payload_sha256 in self.raw_payloads:
            return False
        self.raw_payloads[payload_sha256] = (
            payload_id,
            venue,
            source_endpoint,
            raw_payload,
            received_at,
        )
        return True

    def purge_raw_payloads_before(self, cutoff_at: datetime) -> int:
        protected = {
            self.events[event_id].source_payload_sha256
            for manifest in self.snapshot_manifests.values()
            for _, event_id, _ in manifest.events
            if self.events[event_id].source_payload_sha256 is not None
        }
        removable = [
            payload_hash
            for payload_hash, (_, _, _, _, received_at) in self.raw_payloads.items()
            if received_at < cutoff_at and payload_hash not in protected
        ]
        for payload_hash in removable:
            del self.raw_payloads[payload_hash]
        return len(removable)

    def get_raw_event(self, event_id: str) -> RawMarketEvent | None:
        return self.events.get(event_id)

    def quarantine(self, event: RawMarketEvent, reason: str, at: datetime) -> None:
        self.quarantined.append((event, reason, at))

    def quarantine_count(self, event_ids: tuple[str, ...] | None = None) -> int:
        allowed = set(event_ids) if event_ids is not None else None
        return sum(
            1 for item, _, _ in self.quarantined if allowed is None or item.event_id in allowed
        )

    def quarantine_summary(
        self, cutoff_at: datetime
    ) -> tuple[tuple[str, ...], tuple[tuple[str, int], ...]]:
        records = [item for item in self.quarantined if item[2] <= cutoff_at]
        counts: dict[str, int] = {}
        for _, reason, _ in records:
            counts[reason] = counts.get(reason, 0) + 1
        return (
            tuple(sorted({event.event_id for event, _, _ in records})),
            tuple(sorted(counts.items())),
        )

    def save_collection_failure(self, failure: CollectionFailureEvent) -> None:
        self.failures.append(failure)

    def collection_failures(self) -> tuple[CollectionFailureEvent, ...]:
        return tuple(self.failures)

    def save_snapshot(
        self, snapshot_id: str, cutoff_at: datetime, event_count: int, content_sha256: str
    ) -> None:
        existing = self.snapshots.get(snapshot_id)
        value = (cutoff_at, event_count, content_sha256)
        if existing is not None and existing != value:
            raise ValueError("snapshot id is already bound to different data")
        self.snapshots[snapshot_id] = value

    def finalize_snapshot(self, manifest: DataSnapshotManifest) -> None:
        current = self.snapshot_manifests.get(manifest.snapshot_id)
        if current is not None:
            if current != manifest:
                raise ValueError("finalized snapshot is immutable")
            return
        for _, event_id, payload_hash in manifest.events:
            event = self.events.get(event_id)
            if event is None or event.payload_sha256 != payload_hash:
                raise ValueError("snapshot membership references missing or changed event")
        self.save_snapshot(
            manifest.snapshot_id,
            manifest.cutoff_at,
            len(manifest.events),
            manifest.content_sha256,
        )
        self.snapshot_manifests[manifest.snapshot_id] = manifest

    def snapshot_manifest(self, snapshot_id: str) -> DataSnapshotManifest | None:
        return self.snapshot_manifests.get(snapshot_id)

    def snapshot_events(self, snapshot_id: str) -> tuple[RawMarketEvent, ...]:
        manifest = self.snapshot_manifests.get(snapshot_id)
        if manifest is None:
            return ()
        return tuple(self.events[event_id] for _, event_id, _ in manifest.events)

    def save_checkpoint(self, checkpoint: CollectionCheckpoint) -> None:
        self.checkpoints[
            (checkpoint.checkpoint_namespace, checkpoint.venue, checkpoint.stream_key)
        ] = checkpoint

    def get_checkpoint(
        self, venue: str, stream_key: str, checkpoint_namespace: str = "production"
    ) -> CollectionCheckpoint | None:
        return self.checkpoints.get((checkpoint_namespace, venue, stream_key))

    def list_checkpoints(
        self, checkpoint_namespace: str = "production"
    ) -> tuple[CollectionCheckpoint, ...]:
        return tuple(
            checkpoint
            for (namespace, _, _), checkpoint in self.checkpoints.items()
            if namespace == checkpoint_namespace
        )

    def save_instrument_rule(self, rule: InstrumentRuleSnapshot) -> None:
        current = self.rules.get(rule.rule_snapshot_id)
        if current is not None and current != rule:
            raise ValueError("instrument rule snapshot is immutable")
        self.rules[rule.rule_snapshot_id] = rule

    def instrument_rules(
        self, rule_snapshot_ids: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]:
        return tuple(self.rules[item] for item in rule_snapshot_ids)

    def instrument_rules_at(self, cutoff_at: datetime) -> tuple[InstrumentRuleSnapshot, ...]:
        return tuple(
            item
            for item in self.rules.values()
            if item.valid_from <= cutoff_at
            and (item.valid_until is None or cutoff_at < item.valid_until)
        )

    def save_run(self, identity: ResearchRunIdentity, status: str, verdict: str | None) -> None:
        existing = self.runs.get(identity.run_id)
        if existing is not None and existing[0] != identity:
            raise ValueError("run id is already bound to a different identity")
        self.runs[identity.run_id] = (identity, status, verdict)

    def freeze_hypothesis(self, hypothesis: FrozenHypothesis) -> None:
        key = (hypothesis.strategy_id, hypothesis.hypothesis_version)
        existing = self.hypotheses.get(key)
        if existing is not None and existing.content_sha256 != hypothesis.content_sha256:
            raise ValueError("parameter changes require a new hypothesis_version")
        self.hypotheses[key] = hypothesis

    def save_artifact(
        self,
        run_id: str,
        data_snapshot_id: str,
        artifact_type: str,
        path: str,
        content_sha256: str,
        created_at: datetime,
    ) -> None:
        self.artifacts.append(
            (run_id, data_snapshot_id, artifact_type, path, content_sha256, created_at)
        )


class PostgreSQLResearchRepository:
    """SQLAlchemy implementation used with PostgreSQL in CI/deployment and SQLite in tests."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def add_raw_event(self, event: RawMarketEvent) -> bool:
        row = RawMarketEventRow(
            event_id=event.event_id,
            venue=event.venue,
            canonical_instrument_id=event.canonical_instrument_id,
            venue_symbol=event.venue_symbol,
            event_type=event.event_type,
            exchange_timestamp=event.exchange_timestamp,
            received_at=event.received_at,
            available_at=event.available_at,
            sequence=event.sequence,
            connection_id=str(event.connection_id) if event.connection_id else None,
            reconciliation_state=(
                event.reconciliation_state.value if event.reconciliation_state else None
            ),
            payload_sha256=event.payload_sha256,
            raw_payload=event.raw_payload,
            normalizer_version=event.normalizer_version,
            capability_verification_run_id=event.capability_verification_run_id,
            raw_payload_id=event.raw_payload_id,
            source_payload_sha256=event.source_payload_sha256,
            channel=event.channel,
            snapshot_sequence=event.snapshot_sequence,
            delta_sequence=event.delta_sequence,
            connection_epoch=event.connection_epoch,
            timestamp_semantic=event.timestamp_semantic.value,
            availability_provenance=event.availability_provenance.value,
            exchange_server_time=event.exchange_server_time,
            timeframe=event.timeframe,
            created_at=event.created_at,
        )
        with Session(self.engine) as session:
            try:
                session.add(row)
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

    def purge_raw_payloads_before(self, cutoff_at: datetime) -> int:
        with Session(self.engine) as session:
            protected = (
                select(RawMarketEventRow.source_payload_sha256)
                .join(
                    DataSnapshotEventRow,
                    DataSnapshotEventRow.event_id == RawMarketEventRow.event_id,
                )
                .where(RawMarketEventRow.source_payload_sha256.is_not(None))
            )
            result = session.execute(
                delete(RawMarketPayloadRow).where(
                    RawMarketPayloadRow.received_at < cutoff_at,
                    RawMarketPayloadRow.payload_sha256.not_in(protected),
                )
            )
            session.commit()
            return int(getattr(result, "rowcount", 0) or 0)

    def raw_events(self) -> tuple[RawMarketEvent, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(RawMarketEventRow)).all()
            return tuple(self._event(row) for row in rows)

    def add_experimental_event(self, event: RawMarketEvent, support: str) -> bool:
        with Session(self.engine) as session:
            try:
                session.add(
                    ExperimentalMarketEventRow(
                        event_id=event.event_id,
                        venue=event.venue,
                        canonical_instrument_id=event.canonical_instrument_id,
                        venue_symbol=event.venue_symbol,
                        event_type=event.event_type,
                        payload_sha256=event.payload_sha256,
                        raw_payload=event.raw_payload,
                        capability_support=support,
                        capability_verification_run_id=(
                            event.capability_verification_run_id or None
                        ),
                        exchange_timestamp=event.exchange_timestamp,
                        received_at=event.received_at,
                        available_at=event.available_at,
                        sequence=event.sequence,
                        connection_id=(str(event.connection_id) if event.connection_id else None),
                        reconciliation_state=(
                            event.reconciliation_state.value
                            if event.reconciliation_state is not None
                            else None
                        ),
                        normalizer_version=event.normalizer_version,
                        channel=event.channel,
                        connection_epoch=event.connection_epoch,
                        timestamp_semantic=event.timestamp_semantic.value,
                        availability_provenance=event.availability_provenance.value,
                        exchange_server_time=event.exchange_server_time,
                        timeframe=event.timeframe,
                    )
                )
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

    def get_experimental_event(self, event_id: str) -> RawMarketEvent | None:
        with Session(self.engine) as session:
            row = session.get(ExperimentalMarketEventRow, event_id)
            if row is None:
                return None
            return self._experimental_event(row)

    def experimental_event_count(self) -> int:
        with Session(self.engine) as session:
            return int(
                session.scalar(select(func.count()).select_from(ExperimentalMarketEventRow)) or 0
            )

    def list_experimental_events(self) -> tuple[RawMarketEvent, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(ExperimentalMarketEventRow)).all()
            return tuple(self._experimental_event(row) for row in rows)

    def list_experimental_events_after(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
        limit: int,
    ) -> tuple[RawMarketEvent, ...]:
        cursor_map = {(item.venue, item.event_stream): item for item in cursors}
        stream_queries = tuple(
            (venue, stream, stream_event_types)
            for venue in venues
            for stream, stream_event_types in (
                ("funding_current", ("funding_current",)),
                ("orderbook_snapshot", ("orderbook_snapshot", "orderbook_delta")),
            )
            if set(stream_event_types).intersection(event_types)
        )
        if not stream_queries:
            return ()
        per_stream_limit = max(1, math.ceil(limit / len(stream_queries)))
        collected: list[RawMarketEvent] = []
        with Session(self.engine) as session:
            for venue, stream, stream_event_types in stream_queries:
                cursor = cursor_map.get((venue, stream))
                ordering: Any = True
                if cursor is not None and cursor.last_available_at is not None:
                    ordering = or_(
                        ExperimentalMarketEventRow.available_at > cursor.last_available_at,
                        and_(
                            ExperimentalMarketEventRow.available_at == cursor.last_available_at,
                            ExperimentalMarketEventRow.event_id > (cursor.last_event_id or ""),
                        ),
                    )
                statement = (
                    select(ExperimentalMarketEventRow)
                    .where(
                        ExperimentalMarketEventRow.venue == venue,
                        ExperimentalMarketEventRow.canonical_instrument_id == instrument,
                        ExperimentalMarketEventRow.event_type.in_(stream_event_types),
                        ExperimentalMarketEventRow.event_type.in_(event_types),
                        ordering,
                    )
                    .order_by(
                        ExperimentalMarketEventRow.available_at,
                        ExperimentalMarketEventRow.event_id,
                    )
                    .limit(per_stream_limit)
                )
                collected.extend(
                    self._experimental_event(row) for row in session.scalars(statement).all()
                )
        return tuple(sorted(collected, key=lambda item: (item.available_at, item.event_id))[:limit])

    def experimental_event_backlog(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
    ) -> tuple[int, datetime | None]:
        predicates = _cursor_predicates(cursors, venues=venues)
        with Session(self.engine) as session:
            backlog_event_count = sum(
                session.scalar(
                    select(func.count())
                    .select_from(ExperimentalMarketEventRow)
                    .where(
                        ExperimentalMarketEventRow.canonical_instrument_id == instrument,
                        ExperimentalMarketEventRow.event_type.in_(event_types),
                        predicate,
                    )
                )
                or 0
                for predicate in predicates
            )
            latest_by_venue = tuple(
                session.scalar(
                    select(ExperimentalMarketEventRow.available_at)
                    .where(
                        ExperimentalMarketEventRow.canonical_instrument_id == instrument,
                        ExperimentalMarketEventRow.venue == venue,
                        ExperimentalMarketEventRow.event_type.in_(event_types),
                    )
                    .order_by(ExperimentalMarketEventRow.available_at.desc())
                    .limit(1)
                )
                for venue in venues
            )
        latest = max((item for item in latest_by_venue if item is not None), default=None)
        return backlog_event_count, self._aware(latest) if latest is not None else None

    def shadow_input_checkpoint(
        self, run_id: str, *, instrument: str = "BTC"
    ) -> tuple[tuple[ExperimentalEventCursor, ...], ShadowInputCheckpoint | None]:
        with Session(self.engine) as session:
            cursor_rows = session.scalars(
                select(ShadowInputCursorRow)
                .where(
                    ShadowInputCursorRow.run_id == run_id,
                    ShadowInputCursorRow.instrument == instrument,
                )
                .order_by(ShadowInputCursorRow.venue, ShadowInputCursorRow.event_stream)
            ).all()
            row = session.get(ShadowInputCheckpointRow, (run_id, instrument))
            cursors = tuple(
                ExperimentalEventCursor(
                    run_id=item.run_id,
                    venue=item.venue,
                    instrument=item.instrument,
                    event_stream=item.event_stream,
                    last_available_at=(
                        self._aware(item.last_available_at) if item.last_available_at else None
                    ),
                    last_event_id=item.last_event_id,
                    updated_at=self._aware(item.updated_at),
                )
                for item in cursor_rows
            )
            if row is None:
                return cursors, None
            return cursors, ShadowInputCheckpoint(
                run_id=row.run_id,
                instrument=row.instrument,
                state_json=row.state_json,
                events_fetched_total=row.events_fetched_total,
                events_processed_total=row.events_processed_total,
                events_failed_total=row.events_failed_total,
                batch_count=row.batch_count,
                last_batch_size=row.last_batch_size,
                scan_duration_ms=Decimal(row.scan_duration_ms),
                processing_duration_ms=Decimal(row.processing_duration_ms),
                latest_db_event_available_at=(
                    self._aware(row.latest_db_event_available_at)
                    if row.latest_db_event_available_at
                    else None
                ),
                shadow_input_lag_seconds=(
                    Decimal(row.shadow_input_lag_seconds)
                    if row.shadow_input_lag_seconds is not None
                    else None
                ),
                shadow_input_backlog_estimate=row.shadow_input_backlog_estimate,
                strategy_observation_first_at=(
                    self._aware(row.strategy_observation_first_at)
                    if row.strategy_observation_first_at
                    else None
                ),
                strategy_observation_last_at=(
                    self._aware(row.strategy_observation_last_at)
                    if row.strategy_observation_last_at
                    else None
                ),
                last_cursor_advanced_at=(
                    self._aware(row.last_cursor_advanced_at)
                    if row.last_cursor_advanced_at
                    else None
                ),
                runtime_status=row.runtime_status,
                updated_at=self._aware(row.updated_at),
            )

    def commit_shadow_input_checkpoint(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        checkpoint: ShadowInputCheckpoint,
    ) -> None:
        with Session(self.engine) as session:
            for cursor in cursors:
                row = session.scalar(
                    select(ShadowInputCursorRow).where(
                        ShadowInputCursorRow.run_id == cursor.run_id,
                        ShadowInputCursorRow.venue == cursor.venue,
                        ShadowInputCursorRow.instrument == cursor.instrument,
                        ShadowInputCursorRow.event_stream == cursor.event_stream,
                    )
                )
                if row is None:
                    row = ShadowInputCursorRow(
                        run_id=cursor.run_id,
                        venue=cursor.venue,
                        instrument=cursor.instrument,
                        event_stream=cursor.event_stream,
                        last_available_at=cursor.last_available_at,
                        last_event_id=cursor.last_event_id,
                        updated_at=cursor.updated_at,
                    )
                    session.add(row)
                else:
                    row.last_available_at = cursor.last_available_at
                    row.last_event_id = cursor.last_event_id
                    row.updated_at = cursor.updated_at
            saved = session.get(
                ShadowInputCheckpointRow, (checkpoint.run_id, checkpoint.instrument)
            )
            values = {
                "state_json": checkpoint.state_json,
                "events_fetched_total": checkpoint.events_fetched_total,
                "events_processed_total": checkpoint.events_processed_total,
                "events_failed_total": checkpoint.events_failed_total,
                "batch_count": checkpoint.batch_count,
                "last_batch_size": checkpoint.last_batch_size,
                "scan_duration_ms": checkpoint.scan_duration_ms,
                "processing_duration_ms": checkpoint.processing_duration_ms,
                "latest_db_event_available_at": checkpoint.latest_db_event_available_at,
                "shadow_input_lag_seconds": checkpoint.shadow_input_lag_seconds,
                "shadow_input_backlog_estimate": checkpoint.shadow_input_backlog_estimate,
                "strategy_observation_first_at": checkpoint.strategy_observation_first_at,
                "strategy_observation_last_at": checkpoint.strategy_observation_last_at,
                "last_cursor_advanced_at": checkpoint.last_cursor_advanced_at,
                "runtime_status": checkpoint.runtime_status,
                "updated_at": checkpoint.updated_at,
            }
            if saved is None:
                session.add(
                    ShadowInputCheckpointRow(
                        run_id=checkpoint.run_id, instrument=checkpoint.instrument, **values
                    )
                )
            else:
                for name, value in values.items():
                    setattr(saved, name, value)
            session.commit()

    def list_experimental_event_ids_between(
        self,
        *,
        venue: str,
        instrument: str,
        event_type: str,
        after_available_at: datetime,
        after_event_id: str,
        through_available_at: datetime,
        through_event_id: str,
    ) -> tuple[str, ...]:
        after = or_(
            ExperimentalMarketEventRow.available_at > after_available_at,
            and_(
                ExperimentalMarketEventRow.available_at == after_available_at,
                ExperimentalMarketEventRow.event_id > after_event_id,
            ),
        )
        through = or_(
            ExperimentalMarketEventRow.available_at < through_available_at,
            and_(
                ExperimentalMarketEventRow.available_at == through_available_at,
                ExperimentalMarketEventRow.event_id <= through_event_id,
            ),
        )
        statement = (
            select(ExperimentalMarketEventRow.event_id)
            .where(
                ExperimentalMarketEventRow.venue == venue,
                ExperimentalMarketEventRow.canonical_instrument_id == instrument,
                ExperimentalMarketEventRow.event_type == event_type,
                after,
                through,
            )
            .order_by(
                ExperimentalMarketEventRow.available_at,
                ExperimentalMarketEventRow.event_id,
            )
        )
        with Session(self.engine) as session:
            return tuple(session.scalars(statement).all())

    def save_raw_payload(
        self,
        *,
        payload_id: str,
        venue: str,
        source_endpoint: str,
        payload_sha256: str,
        raw_payload: str,
        received_at: datetime,
    ) -> bool:
        with Session(self.engine) as session:
            try:
                session.add(
                    RawMarketPayloadRow(
                        payload_id=payload_id,
                        venue=venue,
                        source_endpoint=source_endpoint,
                        payload_sha256=payload_sha256,
                        raw_payload=raw_payload,
                        received_at=received_at,
                    )
                )
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

    def get_raw_event(self, event_id: str) -> RawMarketEvent | None:
        with Session(self.engine) as session:
            row = session.get(RawMarketEventRow, event_id)
            return self._event(row) if row is not None else None

    def quarantine(self, event: RawMarketEvent, reason: str, at: datetime) -> None:
        with Session(self.engine) as session:
            session.add(
                MarketDataQuarantineRow(
                    event_id=event.event_id,
                    reason=reason,
                    raw_payload=event.raw_payload,
                    quarantined_at=at,
                )
            )
            session.commit()

    def quarantine_count(self, event_ids: tuple[str, ...] | None = None) -> int:
        with Session(self.engine) as session:
            statement = select(MarketDataQuarantineRow.id)
            if event_ids is not None:
                statement = statement.where(MarketDataQuarantineRow.event_id.in_(event_ids))
            return len(session.scalars(statement).all())

    def quarantine_summary(
        self, cutoff_at: datetime
    ) -> tuple[tuple[str, ...], tuple[tuple[str, int], ...]]:
        with Session(self.engine) as session:
            rows = session.execute(
                select(MarketDataQuarantineRow.event_id, MarketDataQuarantineRow.reason).where(
                    MarketDataQuarantineRow.quarantined_at <= cutoff_at
                )
            ).all()
            counts: dict[str, int] = {}
            for _, reason in rows:
                counts[reason] = counts.get(reason, 0) + 1
            return tuple(sorted({row[0] for row in rows})), tuple(sorted(counts.items()))

    def save_collection_failure(self, failure: CollectionFailureEvent) -> None:
        with Session(self.engine) as session:
            session.add(CollectionFailureEventRow(**asdict(failure)))
            session.commit()

    def collection_failures(self) -> tuple[CollectionFailureEvent, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(CollectionFailureEventRow).order_by(CollectionFailureEventRow.id)
            ).all()
            return tuple(
                CollectionFailureEvent(
                    venue=row.venue,
                    stream_key=row.stream_key,
                    instrument=row.instrument,
                    event_type=row.event_type,
                    endpoint=row.endpoint,
                    error_type=row.error_type,
                    error_message=row.error_message,
                    occurred_at=self._aware(row.occurred_at),
                    retry_count=row.retry_count,
                )
                for row in rows
            )

    def save_snapshot(
        self, snapshot_id: str, cutoff_at: datetime, event_count: int, content_sha256: str
    ) -> None:
        with Session(self.engine) as session:
            current = session.get(DataSnapshotRow, snapshot_id)
            if current is not None:
                stored_cutoff = (
                    current.cutoff_at.replace(tzinfo=UTC)
                    if current.cutoff_at.tzinfo is None
                    else current.cutoff_at.astimezone(UTC)
                )
                requested_cutoff = (
                    cutoff_at.replace(tzinfo=UTC)
                    if cutoff_at.tzinfo is None
                    else cutoff_at.astimezone(UTC)
                )
                if (
                    current.content_sha256 != content_sha256
                    or current.event_count != event_count
                    or stored_cutoff != requested_cutoff
                ):
                    raise ValueError("snapshot id is already bound to different data")
                return
            session.add(
                DataSnapshotRow(
                    snapshot_id=snapshot_id,
                    cutoff_at=cutoff_at,
                    event_count=event_count,
                    content_sha256=content_sha256,
                    created_at=cutoff_at,
                )
            )
            session.commit()

    def finalize_snapshot(self, manifest: DataSnapshotManifest) -> None:
        payload = json.dumps(asdict(manifest), default=str, sort_keys=True, separators=(",", ":"))
        with Session(self.engine) as session:
            current = session.get(DataSnapshotRow, manifest.snapshot_id)
            if current is not None and current.finalized_at is not None:
                if current.manifest_sha256 != manifest.manifest_sha256:
                    raise ValueError("finalized snapshot is immutable")
                return
            if current is None:
                current = DataSnapshotRow(
                    snapshot_id=manifest.snapshot_id,
                    cutoff_at=manifest.cutoff_at,
                    event_count=len(manifest.events),
                    content_sha256=manifest.content_sha256,
                    manifest_sha256=None,
                    manifest_json=None,
                    quarantine_count=manifest.quarantine_count,
                    finalized_at=None,
                    eligibility_status=manifest.eligibility_status,
                    eligibility_reasons_json=json.dumps(manifest.eligibility_reasons),
                    created_at=manifest.finalized_at,
                )
                session.add(current)
                session.flush()
            elif current.content_sha256 != manifest.content_sha256:
                raise ValueError("snapshot id is already bound to different data")
            for ordinal, event_id, payload_hash in manifest.events:
                event = session.get(RawMarketEventRow, event_id)
                if event is None or event.payload_sha256 != payload_hash:
                    raise ValueError("snapshot membership references missing or changed event")
                session.add(
                    DataSnapshotEventRow(
                        snapshot_id=manifest.snapshot_id,
                        event_id=event_id,
                        ordinal=ordinal,
                        event_payload_sha256=payload_hash,
                        included_at=manifest.finalized_at,
                    )
                )
            current.event_count = len(manifest.events)
            current.content_sha256 = manifest.content_sha256
            current.manifest_sha256 = manifest.manifest_sha256
            current.manifest_json = payload
            current.quarantine_count = manifest.quarantine_count
            current.finalized_at = manifest.finalized_at
            current.eligibility_status = manifest.eligibility_status
            current.eligibility_reasons_json = json.dumps(manifest.eligibility_reasons)
            session.commit()

    def snapshot_manifest(self, snapshot_id: str) -> DataSnapshotManifest | None:
        with Session(self.engine) as session:
            row = session.get(DataSnapshotRow, snapshot_id)
            if row is None or row.finalized_at is None or row.manifest_json is None:
                return None
            payload = json.loads(row.manifest_json)
            return DataSnapshotManifest(
                snapshot_id=payload["snapshot_id"],
                cutoff_at=self._aware(datetime.fromisoformat(payload["cutoff_at"])),
                events=tuple((int(a), str(b), str(c)) for a, b, c in payload["events"]),
                quarantine_count=int(payload["quarantine_count"]),
                quarantine_reasons=tuple(
                    (str(reason), int(count)) for reason, count in payload["quarantine_reasons"]
                ),
                outage_event_ids=tuple(payload["outage_event_ids"]),
                degraded_event_ids=tuple(payload["degraded_event_ids"]),
                content_sha256=payload["content_sha256"],
                manifest_sha256=payload["manifest_sha256"],
                finalized_at=self._aware(datetime.fromisoformat(payload["finalized_at"])),
                eligibility_status=payload.get("eligibility_status", "FINALIZED_NOT_ELIGIBLE"),
                eligibility_reasons=tuple(payload.get("eligibility_reasons", ())),
            )

    def snapshot_events(self, snapshot_id: str) -> tuple[RawMarketEvent, ...]:
        with Session(self.engine) as session:
            statement = (
                select(RawMarketEventRow)
                .join(
                    DataSnapshotEventRow,
                    DataSnapshotEventRow.event_id == RawMarketEventRow.event_id,
                )
                .where(DataSnapshotEventRow.snapshot_id == snapshot_id)
                .order_by(DataSnapshotEventRow.ordinal)
            )
            return tuple(self._event(row) for row in session.scalars(statement).all())

    def save_checkpoint(self, checkpoint: CollectionCheckpoint) -> None:
        storage_key = self._checkpoint_storage_key(
            checkpoint.stream_key, checkpoint.checkpoint_namespace
        )
        with Session(self.engine) as session:
            row = session.scalar(
                select(MarketDataCheckpointRow).where(
                    MarketDataCheckpointRow.venue == checkpoint.venue,
                    MarketDataCheckpointRow.stream_key == storage_key,
                )
            )
            values = {
                "connection_id": str(checkpoint.connection_id),
                "last_sequence": checkpoint.last_sequence,
                "last_event_id": checkpoint.last_event_id,
                "reconciliation_state": checkpoint.reconciliation_state.value,
                "checkpointed_at": checkpoint.checkpointed_at,
                "canonical_instrument_id": checkpoint.canonical_instrument_id,
                "venue_symbol": checkpoint.venue_symbol,
                "event_type": checkpoint.event_type,
                "channel": checkpoint.channel,
                "last_available_at": checkpoint.last_available_at,
                "last_funding_at": checkpoint.last_funding_at,
                "last_trade_id": checkpoint.last_trade_id,
                "snapshot_sequence": checkpoint.snapshot_sequence,
                "delta_sequence": checkpoint.delta_sequence,
                "connection_epoch": checkpoint.connection_epoch,
                "recovery_required": checkpoint.recovery_required,
                "bootstrap_completed": checkpoint.bootstrap_completed,
                "recovery_started_at": checkpoint.recovery_started_at,
                "recovery_completed_at": checkpoint.recovery_completed_at,
                "last_recovery_failure": checkpoint.last_recovery_failure,
                "checkpoint_namespace": checkpoint.checkpoint_namespace,
            }
            if row is None:
                session.add(
                    MarketDataCheckpointRow(
                        venue=checkpoint.venue,
                        stream_key=storage_key,
                        **values,
                    )
                )
            else:
                for key, value in values.items():
                    setattr(row, key, value)
            session.commit()

    def get_checkpoint(
        self, venue: str, stream_key: str, checkpoint_namespace: str = "production"
    ) -> CollectionCheckpoint | None:
        storage_key = self._checkpoint_storage_key(stream_key, checkpoint_namespace)
        with Session(self.engine) as session:
            row = session.scalar(
                select(MarketDataCheckpointRow).where(
                    MarketDataCheckpointRow.venue == venue,
                    MarketDataCheckpointRow.stream_key == storage_key,
                )
            )
            if row is None:
                return None
            return CollectionCheckpoint(
                venue=row.venue,
                stream_key=stream_key,
                connection_id=UUID(row.connection_id),
                last_sequence=row.last_sequence,
                last_event_id=row.last_event_id,
                reconciliation_state=ReconciliationState(row.reconciliation_state),
                checkpointed_at=self._aware(row.checkpointed_at),
                canonical_instrument_id=row.canonical_instrument_id,
                venue_symbol=row.venue_symbol,
                event_type=row.event_type,
                channel=row.channel,
                last_available_at=(
                    self._aware(row.last_available_at) if row.last_available_at else None
                ),
                last_funding_at=(self._aware(row.last_funding_at) if row.last_funding_at else None),
                last_trade_id=row.last_trade_id,
                snapshot_sequence=row.snapshot_sequence,
                delta_sequence=row.delta_sequence,
                connection_epoch=row.connection_epoch,
                recovery_required=row.recovery_required,
                bootstrap_completed=row.bootstrap_completed,
                recovery_started_at=(
                    self._aware(row.recovery_started_at) if row.recovery_started_at else None
                ),
                recovery_completed_at=(
                    self._aware(row.recovery_completed_at) if row.recovery_completed_at else None
                ),
                last_recovery_failure=row.last_recovery_failure,
                checkpoint_namespace=row.checkpoint_namespace,
            )

    def list_checkpoints(
        self, checkpoint_namespace: str = "production"
    ) -> tuple[CollectionCheckpoint, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(MarketDataCheckpointRow).where(
                    MarketDataCheckpointRow.checkpoint_namespace == checkpoint_namespace
                )
            ).all()
        output: list[CollectionCheckpoint] = []
        for row in rows:
            stream_key = row.stream_key
            if checkpoint_namespace != "production" and "::" in stream_key:
                stream_key = stream_key.split("::", 1)[1]
            checkpoint = self.get_checkpoint(row.venue, stream_key, checkpoint_namespace)
            if checkpoint is not None:
                output.append(checkpoint)
        return tuple(output)

    @staticmethod
    def _checkpoint_storage_key(stream_key: str, checkpoint_namespace: str) -> str:
        if checkpoint_namespace == "production":
            return stream_key
        namespace_hash = hashlib.sha256(checkpoint_namespace.encode()).hexdigest()[:16]
        return f"ns-{namespace_hash}::{stream_key}"

    def save_instrument_rule(self, rule: InstrumentRuleSnapshot) -> None:
        with Session(self.engine) as session:
            current = session.get(InstrumentRuleSnapshotRow, rule.rule_snapshot_id)
            values = asdict(rule)
            values["field_evidence_json"] = json.dumps(
                values.pop("field_evidence"), sort_keys=True, separators=(",", ":")
            )
            values["fee_tier"] = rule.fee_tier.value
            values["verification_status"] = rule.verification_status.value
            if current is not None:
                mismatched = False
                for key, value in values.items():
                    stored = getattr(current, key)
                    if isinstance(stored, datetime):
                        stored = self._aware(stored)
                    if stored != value:
                        mismatched = True
                        break
                if mismatched:
                    raise ValueError("instrument rule snapshot is immutable")
                return
            session.add(InstrumentRuleSnapshotRow(**values))
            session.commit()

    def instrument_rules(
        self, rule_snapshot_ids: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(InstrumentRuleSnapshotRow).where(
                    InstrumentRuleSnapshotRow.rule_snapshot_id.in_(rule_snapshot_ids)
                )
            ).all()
            by_id = {row.rule_snapshot_id: row for row in rows}
            if set(by_id) != set(rule_snapshot_ids):
                raise ValueError("instrument rule snapshot is missing")
            return tuple(
                InstrumentRuleSnapshot(
                    rule_snapshot_id=by_id[item].rule_snapshot_id,
                    venue=by_id[item].venue,
                    canonical_instrument_id=by_id[item].canonical_instrument_id,
                    venue_symbol=by_id[item].venue_symbol,
                    tick_size=by_id[item].tick_size,
                    lot_size=by_id[item].lot_size,
                    minimum_quantity=by_id[item].minimum_quantity,
                    minimum_notional=by_id[item].minimum_notional,
                    maker_fee=by_id[item].maker_fee,
                    taker_fee=by_id[item].taker_fee,
                    maker_rebate=by_id[item].maker_rebate,
                    funding_interval=by_id[item].funding_interval,
                    margin_asset=by_id[item].margin_asset,
                    source_endpoint=by_id[item].source_endpoint,
                    source_payload_sha256=by_id[item].source_payload_sha256,
                    retrieved_at=self._aware(by_id[item].retrieved_at),
                    valid_from=self._aware(by_id[item].valid_from),
                    valid_until=(
                        self._aware(cast(datetime, by_id[item].valid_until))
                        if by_id[item].valid_until is not None
                        else None
                    ),
                    field_evidence=json.loads(by_id[item].field_evidence_json),
                    fee_tier=FeeTierKind(by_id[item].fee_tier),
                    verification_status=RuleVerificationStatus(by_id[item].verification_status),
                )
                for item in rule_snapshot_ids
            )

    def instrument_rules_at(self, cutoff_at: datetime) -> tuple[InstrumentRuleSnapshot, ...]:
        with Session(self.engine) as session:
            ids = tuple(
                session.scalars(
                    select(InstrumentRuleSnapshotRow.rule_snapshot_id).where(
                        InstrumentRuleSnapshotRow.valid_from <= cutoff_at,
                        (
                            InstrumentRuleSnapshotRow.valid_until.is_(None)
                            | (InstrumentRuleSnapshotRow.valid_until > cutoff_at)
                        ),
                    )
                ).all()
            )
        return self.instrument_rules(ids)

    def save_run(self, identity: ResearchRunIdentity, status: str, verdict: str | None) -> None:
        with Session(self.engine) as session:
            row = session.get(ResearchRunRow, identity.run_id)
            values = {
                "commit_sha": identity.commit_sha,
                "config_sha256": identity.config_sha256,
                "data_snapshot_id": identity.data_snapshot_id,
                "hypothesis_version": identity.hypothesis_version,
                "strategy_id": identity.strategy_id,
                "strategy_version": identity.strategy_version,
                "status": status,
                "acceptance_verdict": verdict,
                "created_at": identity.created_at,
                "completed_at": identity.created_at if status == "completed" else None,
            }
            if row is None:
                session.add(ResearchRunRow(run_id=identity.run_id, **values))
            else:
                immutable = {
                    "commit_sha": identity.commit_sha,
                    "config_sha256": identity.config_sha256,
                    "data_snapshot_id": identity.data_snapshot_id,
                    "hypothesis_version": identity.hypothesis_version,
                    "strategy_id": identity.strategy_id,
                    "strategy_version": identity.strategy_version,
                }
                if any(getattr(row, key) != value for key, value in immutable.items()):
                    raise ValueError("run id is already bound to a different identity")
                row.status = status
                row.acceptance_verdict = verdict
                row.completed_at = identity.created_at if status == "completed" else None
            session.commit()

    def freeze_hypothesis(self, hypothesis: FrozenHypothesis) -> None:
        with Session(self.engine) as session:
            current = session.get(
                FrozenHypothesisRow,
                (hypothesis.hypothesis_version, hypothesis.strategy_id),
            )
            if current is not None:
                if current.content_sha256 != hypothesis.content_sha256:
                    raise ValueError("parameter changes require a new hypothesis_version")
                return
            session.add(
                FrozenHypothesisRow(
                    hypothesis_version=hypothesis.hypothesis_version,
                    strategy_id=hypothesis.strategy_id,
                    content_sha256=hypothesis.content_sha256,
                    content_json=json.dumps(asdict(hypothesis), default=str, sort_keys=True),
                    frozen_at=hypothesis.frozen_at,
                )
            )
            session.commit()

    def save_artifact(
        self,
        run_id: str,
        data_snapshot_id: str,
        artifact_type: str,
        path: str,
        content_sha256: str,
        created_at: datetime,
    ) -> None:
        with Session(self.engine) as session:
            session.add(
                ResearchArtifactRow(
                    run_id=run_id,
                    data_snapshot_id=data_snapshot_id,
                    artifact_type=artifact_type,
                    path=path,
                    content_sha256=content_sha256,
                    created_at=created_at,
                )
            )
            session.commit()

    @staticmethod
    def _aware(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    @staticmethod
    def _event(row: RawMarketEventRow) -> RawMarketEvent:
        def aware(value: datetime) -> datetime:
            return value.replace(tzinfo=UTC) if value.tzinfo is None else value

        return RawMarketEvent(
            event_id=row.event_id,
            venue=row.venue,
            canonical_instrument_id=row.canonical_instrument_id,
            venue_symbol=row.venue_symbol,
            event_type=row.event_type,
            exchange_timestamp=(aware(row.exchange_timestamp) if row.exchange_timestamp else None),
            received_at=aware(row.received_at),
            available_at=aware(row.available_at),
            sequence=row.sequence,
            connection_id=UUID(row.connection_id) if row.connection_id else None,
            reconciliation_state=(
                ReconciliationState(row.reconciliation_state) if row.reconciliation_state else None
            ),
            payload_sha256=row.payload_sha256,
            raw_payload=row.raw_payload,
            normalizer_version=row.normalizer_version,
            capability_verification_run_id=row.capability_verification_run_id,
            created_at=aware(row.created_at),
            raw_payload_id=row.raw_payload_id,
            source_payload_sha256=row.source_payload_sha256,
            channel=row.channel,
            snapshot_sequence=row.snapshot_sequence,
            delta_sequence=row.delta_sequence,
            connection_epoch=row.connection_epoch,
            timestamp_semantic=TimestampSemantic(row.timestamp_semantic),
            availability_provenance=AvailabilityProvenance(row.availability_provenance),
            exchange_server_time=(
                aware(row.exchange_server_time) if row.exchange_server_time else None
            ),
            timeframe=row.timeframe,
        )

    @classmethod
    def _experimental_event(cls, row: ExperimentalMarketEventRow) -> RawMarketEvent:
        return RawMarketEvent(
            event_id=row.event_id,
            venue=row.venue,
            canonical_instrument_id=row.canonical_instrument_id,
            venue_symbol=row.venue_symbol,
            event_type=row.event_type,
            exchange_timestamp=(
                cls._aware(row.exchange_timestamp) if row.exchange_timestamp is not None else None
            ),
            received_at=cls._aware(row.received_at),
            available_at=cls._aware(row.available_at),
            sequence=row.sequence,
            connection_id=UUID(row.connection_id) if row.connection_id else None,
            reconciliation_state=(
                ReconciliationState(row.reconciliation_state) if row.reconciliation_state else None
            ),
            payload_sha256=row.payload_sha256,
            raw_payload=row.raw_payload,
            normalizer_version=row.normalizer_version,
            capability_verification_run_id=(
                row.capability_verification_run_id or "unverified-experimental"
            ),
            created_at=cls._aware(row.received_at),
            channel=row.channel,
            connection_epoch=row.connection_epoch,
            timestamp_semantic=TimestampSemantic(row.timestamp_semantic),
            availability_provenance=AvailabilityProvenance(row.availability_provenance),
            exchange_server_time=(
                cls._aware(row.exchange_server_time) if row.exchange_server_time else None
            ),
            timeframe=row.timeframe,
        )
