from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.adapters.exchanges.websocket import ReconciliationState
from app.domain.venues.models import CapabilitySupport
from app.infrastructure.database.models import Base, ResearchArtifactRow
from app.infrastructure.database.session import build_engine
from app.services.research.data_operations import (
    CapabilityDecision,
    CollectedEnvelope,
    DataSnapshotService,
    ResearchMarketDataCollector,
    ResearchRetentionService,
)
from app.services.research.models import (
    CollectionCheckpoint,
    FrozenHypothesis,
    InstrumentRuleSnapshot,
    RawMarketEvent,
)
from app.services.research.repository import (
    InMemoryResearchRepository,
    PostgreSQLResearchRepository,
)

BASE = datetime(2026, 7, 13, tzinfo=UTC)


def event(
    event_id: str,
    *,
    available_at: datetime = BASE,
    event_type: str = "trade",
    payload: dict[str, object] | None = None,
) -> RawMarketEvent:
    raw = json.dumps(payload or {"price": "100", "quantity": "1"}, sort_keys=True)
    return RawMarketEvent(
        event_id=event_id,
        venue="hyperliquid",
        canonical_instrument_id="BTC",
        venue_symbol="BTC",
        event_type=event_type,
        exchange_timestamp=available_at,
        received_at=available_at,
        available_at=available_at,
        sequence=None,
        connection_id=None,
        reconciliation_state=None,
        payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),
        raw_payload=raw,
        normalizer_version="r2-test",
        capability_verification_run_id="verified-run",
        created_at=available_at,
    )


class Gate:
    def __init__(self, support: CapabilitySupport) -> None:
        self.support = support

    def decide(self, *, venue: str, capability: str, now: datetime) -> CapabilityDecision:
        del venue, capability, now
        return CapabilityDecision(
            self.support, "verified-run" if self.support == CapabilitySupport.LIVE_VERIFIED else ""
        )


class Source:
    venue = "hyperliquid"

    def __init__(self, envelopes: tuple[CollectedEnvelope, ...]) -> None:
        self.envelopes = envelopes
        self.seen_checkpoint: CollectionCheckpoint | None = None
        self.closed = False

    async def events(
        self,
        *,
        instruments: tuple[str, ...],
        event_types: tuple[str, ...],
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]:
        del instruments, event_types
        self.seen_checkpoint = checkpoint
        for item in self.envelopes:
            yield item

    async def close(self) -> None:
        self.closed = True


def envelope(
    *,
    sequence: int | None = None,
    raw_payload: str = '{"price":"100","quantity":"1"}',
    stable_event_key: str | None = None,
) -> CollectedEnvelope:
    return CollectedEnvelope(
        venue="hyperliquid",
        canonical_instrument_id="BTC",
        venue_symbol="BTC",
        event_type="trade",
        source_endpoint="fake-public-adapter",
        raw_payload=raw_payload,
        exchange_timestamp=BASE,
        received_at=BASE,
        available_at=BASE,
        sequence=sequence,
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        stable_event_key=stable_event_key,
    )


@pytest.mark.asyncio
async def test_same_raw_payload_is_idempotent_and_checkpoint_resumes() -> None:
    repository = InMemoryResearchRepository()
    first = Source((envelope(sequence=1),))
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(first,),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("trade",),
        collection_enabled=True,
        maximum_cycles=1,
        stale_after_seconds=999999,
    )
    await collector.run()
    assert collector.result.production_counts == {"hyperliquid": 2}  # trade + health
    second = Source((envelope(sequence=1),))
    restarted = ResearchMarketDataCollector(
        repository=repository,
        sources=(second,),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("trade",),
        collection_enabled=True,
        maximum_cycles=1,
        stale_after_seconds=999999,
    )
    await restarted.run()
    assert second.seen_checkpoint is not None
    assert second.seen_checkpoint.last_sequence == 1
    assert len(repository.raw_payloads) == 1
    assert len([item for item in repository.events.values() if item.event_type == "trade"]) == 1


@pytest.mark.asyncio
async def test_different_payload_with_same_event_id_is_quarantined() -> None:
    repository = InMemoryResearchRepository()
    source = Source(
        (
            envelope(raw_payload='{"value":1}', stable_event_key="same"),
            envelope(raw_payload='{"value":2}', stable_event_key="same"),
        )
    )
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(source,),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("trade",),
        collection_enabled=True,
        maximum_cycles=1,
    )
    await collector.run()
    assert any("payload conflict" in reason for _, reason, _ in repository.quarantined)


@pytest.mark.asyncio
async def test_unverified_capability_is_experimental_and_not_production() -> None:
    repository = InMemoryResearchRepository()
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(Source((envelope(),)),),
        capability_gate=Gate(CapabilitySupport.IMPLEMENTED),
        instruments=("BTC",),
        event_types=("trade",),
        collection_enabled=True,
        maximum_cycles=1,
    )
    await collector.run()
    assert collector.result.experimental_counts == {"hyperliquid": 1}
    assert not [item for item in repository.events.values() if item.event_type == "trade"]


@pytest.mark.asyncio
async def test_sequence_gap_enters_quarantine_and_degraded_checkpoint() -> None:
    repository = InMemoryResearchRepository()
    repository.save_checkpoint(
        CollectionCheckpoint(
            venue="hyperliquid",
            stream_key="public-rest-and-websocket",
            connection_id=uuid4(),
            last_sequence=4,
            last_event_id="prior",
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            checkpointed_at=BASE - timedelta(seconds=1),
        )
    )
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(Source((envelope(sequence=7),)),),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("trade",),
        collection_enabled=True,
        maximum_cycles=1,
    )
    await collector.run()
    checkpoint = repository.get_checkpoint("hyperliquid", "public-rest-and-websocket")
    assert (
        checkpoint is not None and checkpoint.reconciliation_state == ReconciliationState.DEGRADED
    )
    assert any(item.event_type == "sequence_gap" for item in repository.events.values())
    assert repository.quarantine_count() == 1


def test_snapshot_membership_exact_reproduction_cutoff_and_quarantine_visibility() -> None:
    repository = InMemoryResearchRepository()
    before = event("before")
    outage = event("outage", event_type="websocket_disconnect", payload={"outage": True})
    quarantined = event("bad")
    after = event("after", available_at=BASE + timedelta(seconds=1))
    for item in (before, outage, quarantined, after):
        assert repository.add_raw_event(item)
    repository.quarantine(quarantined, "schema violation", BASE)
    service = DataSnapshotService(repository)
    manifest = service.finalize(cutoff_at=BASE, finalized_at=BASE)
    assert [item[1] for item in manifest.events] == ["before", "outage"]
    assert manifest.quarantine_count == 1
    assert manifest.quarantine_reasons == (("schema violation", 1),)
    assert repository.quarantined[0][0] == quarantined
    assert manifest.outage_event_ids == ("outage",)
    assert service.verify(manifest.snapshot_id) == manifest
    with pytest.raises(ValueError, match="immutable"):
        repository.finalize_snapshot(replace(manifest, manifest_sha256="0" * 64))
    repository.snapshot_manifests[manifest.snapshot_id] = replace(manifest, content_sha256="0" * 64)
    with pytest.raises(ValueError, match="content hash"):
        service.verify(manifest.snapshot_id)


def test_event_received_before_but_available_after_cutoff_is_excluded() -> None:
    item = event("late-availability", available_at=BASE + timedelta(seconds=1))
    item = replace(item, received_at=BASE)
    repository = InMemoryResearchRepository()
    repository.add_raw_event(item)
    manifest = DataSnapshotService(repository).finalize(cutoff_at=BASE, finalized_at=BASE)
    assert manifest.events == ()


def test_instrument_rule_is_frozen_and_hypothesis_version_is_strategy_scoped() -> None:
    repository = InMemoryResearchRepository()
    rule = InstrumentRuleSnapshot(
        rule_snapshot_id="rule-1",
        venue="hyperliquid",
        canonical_instrument_id="BTC",
        venue_symbol="BTC",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.001"),
        minimum_quantity=Decimal("0.001"),
        minimum_notional=Decimal("5"),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0.0005"),
        maker_rebate=Decimal("0.0001"),
        funding_interval=8,
        margin_asset="USDC",
        source_endpoint="/meta",
        source_payload_sha256="a" * 64,
        retrieved_at=BASE,
        valid_from=BASE,
        valid_until=None,
    )
    repository.save_instrument_rule(rule)
    with pytest.raises(ValueError, match="immutable"):
        repository.save_instrument_rule(replace(rule, tick_size=Decimal("1")))
    for strategy in ("funding_carry", "cross_venue_basis"):
        repository.freeze_hypothesis(
            FrozenHypothesis.freeze(
                hypothesis_version="v1",
                strategy_id=strategy,
                parameter_grid={"x": (1,)},
                primary_metric="net_pnl",
                secondary_metrics=(),
                acceptance_thresholds={},
                frozen_at=BASE,
            )
        )
    assert len(repository.hypotheses) == 2


def test_sql_foreign_key_rejects_orphan_artifact(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite+pysqlite:///{tmp_path}/r2.db")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            ResearchArtifactRow(
                run_id="missing-run",
                data_snapshot_id="missing-snapshot",
                artifact_type="metrics",
                path="missing.json",
                content_sha256="a" * 64,
                created_at=BASE,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
    engine.dispose()


def test_sql_snapshot_membership_is_reproduced_in_ordinal_order(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite+pysqlite:///{tmp_path}/snapshot.db")
    Base.metadata.create_all(engine)
    repository = PostgreSQLResearchRepository(engine)
    for item in (event("b"), event("a")):
        assert repository.add_raw_event(item)
    service = DataSnapshotService(repository)
    manifest = service.finalize(cutoff_at=BASE, finalized_at=BASE)
    assert tuple(item[1] for item in manifest.events) == ("a", "b")
    assert service.verify(manifest.snapshot_id).manifest_sha256 == manifest.manifest_sha256
    with pytest.raises(ValueError, match="immutable"):
        repository.finalize_snapshot(replace(manifest, manifest_sha256="f" * 64))
    engine.dispose()


def test_raw_payload_retention_protects_finalized_snapshot_membership() -> None:
    repository = InMemoryResearchRepository()
    protected_hash = "a" * 64
    removable_hash = "b" * 64
    repository.save_raw_payload(
        payload_id="protected",
        venue="hyperliquid",
        source_endpoint="/trades",
        payload_sha256=protected_hash,
        raw_payload="protected raw response",
        received_at=BASE,
    )
    repository.save_raw_payload(
        payload_id="removable",
        venue="hyperliquid",
        source_endpoint="/trades",
        payload_sha256=removable_hash,
        raw_payload="removable raw response",
        received_at=BASE,
    )
    repository.add_raw_event(
        replace(
            event("protected-event"),
            raw_payload_id="protected",
            source_payload_sha256=protected_hash,
        )
    )
    DataSnapshotService(repository).finalize(cutoff_at=BASE, finalized_at=BASE)
    removed = ResearchRetentionService(repository).purge_raw_payloads_before(
        BASE + timedelta(seconds=1)
    )
    assert removed == 1
    assert protected_hash in repository.raw_payloads
    assert removable_hash not in repository.raw_payloads
