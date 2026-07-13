from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.adapters.exchanges.websocket import ReconciliationState
from app.infrastructure.database.models import (
    DataSnapshotRow,
    FrozenHypothesisRow,
    MarketDataQuarantineRow,
    RawMarketEventRow,
    ResearchArtifactRow,
    ResearchRunRow,
)
from app.services.research.models import FrozenHypothesis, RawMarketEvent, ResearchRunIdentity


class ResearchRepository(Protocol):
    def add_raw_event(self, event: RawMarketEvent) -> bool: ...

    def get_raw_event(self, event_id: str) -> RawMarketEvent | None: ...

    def raw_events(self) -> tuple[RawMarketEvent, ...]: ...

    def quarantine(self, event: RawMarketEvent, reason: str, at: datetime) -> None: ...

    def quarantine_count(self, event_ids: tuple[str, ...] | None = None) -> int: ...

    def save_snapshot(
        self, snapshot_id: str, cutoff_at: datetime, event_count: int, content_sha256: str
    ) -> None: ...

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


class InMemoryResearchRepository:
    def __init__(self) -> None:
        self.events: dict[str, RawMarketEvent] = {}
        self.quarantined: list[tuple[RawMarketEvent, str, datetime]] = []
        self.snapshots: dict[str, tuple[datetime, int, str]] = {}
        self.runs: dict[str, tuple[ResearchRunIdentity, str, str | None]] = {}
        self.artifacts: list[tuple[str, str, str, str, str, datetime]] = []
        self.hypotheses: dict[str, FrozenHypothesis] = {}

    def add_raw_event(self, event: RawMarketEvent) -> bool:
        if event.event_id in self.events:
            return False
        self.events[event.event_id] = event
        return True

    def raw_events(self) -> tuple[RawMarketEvent, ...]:
        return tuple(self.events.values())

    def get_raw_event(self, event_id: str) -> RawMarketEvent | None:
        return self.events.get(event_id)

    def quarantine(self, event: RawMarketEvent, reason: str, at: datetime) -> None:
        self.quarantined.append((event, reason, at))

    def quarantine_count(self, event_ids: tuple[str, ...] | None = None) -> int:
        allowed = set(event_ids) if event_ids is not None else None
        return sum(
            1 for item, _, _ in self.quarantined if allowed is None or item.event_id in allowed
        )

    def save_snapshot(
        self, snapshot_id: str, cutoff_at: datetime, event_count: int, content_sha256: str
    ) -> None:
        existing = self.snapshots.get(snapshot_id)
        value = (cutoff_at, event_count, content_sha256)
        if existing is not None and existing != value:
            raise ValueError("snapshot id is already bound to different data")
        self.snapshots[snapshot_id] = value

    def save_run(self, identity: ResearchRunIdentity, status: str, verdict: str | None) -> None:
        existing = self.runs.get(identity.run_id)
        if existing is not None and existing[0] != identity:
            raise ValueError("run id is already bound to a different identity")
        self.runs[identity.run_id] = (identity, status, verdict)

    def freeze_hypothesis(self, hypothesis: FrozenHypothesis) -> None:
        existing = self.hypotheses.get(hypothesis.hypothesis_version)
        if existing is not None and existing.content_sha256 != hypothesis.content_sha256:
            raise ValueError("parameter changes require a new hypothesis_version")
        self.hypotheses[hypothesis.hypothesis_version] = hypothesis

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

    def raw_events(self) -> tuple[RawMarketEvent, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(select(RawMarketEventRow)).all()
            return tuple(self._event(row) for row in rows)

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
            current = session.get(FrozenHypothesisRow, hypothesis.hypothesis_version)
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
        )
