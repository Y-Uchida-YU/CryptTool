from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session

from app.adapters.exchanges.websocket import OrderBookStreamSemantics, ReconciliationState
from app.domain.market_data.models import Market, Side, Trade
from app.domain.venues.models import CapabilitySupport
from app.infrastructure.database.models import Base, ResearchArtifactRow
from app.infrastructure.database.session import build_engine
from app.services.research.data_operations import (
    CapabilityDecision,
    CheckpointWriter,
    CollectedEnvelope,
    CollectorHealthMetrics,
    DataSnapshotService,
    PublicAdapterCollectorSource,
    RawPersistenceConsumer,
    ResearchMarketDataCollector,
    ResearchRetentionService,
    ResearchStreamIdentity,
    SnapshotEligibilityPolicy,
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


class SplitSource:
    venue = "hyperliquid"

    def __init__(self, websocket_values: tuple[CollectedEnvelope, ...] = ()) -> None:
        self.websocket_values = websocket_values
        self.rest_checkpoints: list[CollectionCheckpoint | None] = []
        self.websocket_checkpoints: list[CollectionCheckpoint | None] = []
        self.calls: list[str] = []
        self.closed = False

    async def rest_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]:
        del identity
        self.calls.append("rest")
        self.rest_checkpoints.append(checkpoint)
        if False:
            yield envelope()

    async def websocket_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
        shutdown: asyncio.Event,
    ) -> AsyncIterator[CollectedEnvelope]:
        del identity, shutdown
        self.calls.append("websocket")
        self.websocket_checkpoints.append(checkpoint)
        for item in self.websocket_values:
            yield item

    async def instrument_rules(
        self, instruments: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]:
        del instruments
        return ()

    async def close(self) -> None:
        self.closed = True


def persistence_consumer(repository: InMemoryResearchRepository) -> RawPersistenceConsumer:
    return RawPersistenceConsumer(
        repository,
        Gate(CapabilitySupport.LIVE_VERIFIED),
        CheckpointWriter(repository),
        CollectorHealthMetrics(),
        timedelta(seconds=999999),
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
    assert len(repository.raw_payloads) == 2  # trade plus collector health payload
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
            stream_key="hyperliquid:trade:BTC:unknown",
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
    checkpoint = repository.get_checkpoint("hyperliquid", "hyperliquid:trade:BTC:unknown")
    assert (
        checkpoint is not None and checkpoint.reconciliation_state == ReconciliationState.DEGRADED
    )
    assert any(item.event_type == "sequence_gap" for item in repository.events.values())
    assert repository.quarantine_count() == 1


def test_stream_identity_isolates_instrument_event_type_and_channel() -> None:
    keys = {
        ResearchStreamIdentity("hyperliquid", instrument, symbol, event_type, channel).stream_key
        for instrument, symbol, event_type, channel in (
            ("BTC", "BTC", "trade", "trades"),
            ("ETH", "ETH", "trade", "trades"),
            ("BTC", "BTC", "ohlcv", "rest"),
            ("BTC", "BTC", "trade", "rest"),
        )
    }
    assert keys == {
        "hyperliquid:trade:BTC:trades",
        "hyperliquid:trade:ETH:trades",
        "hyperliquid:ohlcv:BTC:rest",
        "hyperliquid:trade:BTC:rest",
    }


def test_rest_without_sequence_does_not_overwrite_websocket_checkpoint() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    consumer.persist(
        replace(
            envelope(sequence=9),
            channel="trades",
            trade_id="trade-9",
        )
    )
    consumer.persist(
        replace(
            envelope(sequence=None, stable_event_key="rest-trade"),
            channel="rest",
        )
    )
    websocket = repository.get_checkpoint("hyperliquid", "hyperliquid:trade:BTC:trades")
    rest = repository.get_checkpoint("hyperliquid", "hyperliquid:trade:BTC:rest")
    assert websocket is not None and websocket.last_sequence == 9
    assert websocket.last_trade_id == "trade-9"
    assert rest is not None and rest.last_sequence is None


@pytest.mark.asyncio
async def test_restart_passes_rest_timestamp_and_websocket_sequence_cursors() -> None:
    repository = InMemoryResearchRepository()
    repository.save_checkpoint(
        CollectionCheckpoint(
            venue="hyperliquid",
            stream_key="hyperliquid:ohlcv:BTC:rest",
            connection_id=uuid4(),
            last_sequence=None,
            last_event_id="ohlcv-prior",
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            checkpointed_at=BASE,
            canonical_instrument_id="BTC",
            venue_symbol="BTC",
            event_type="ohlcv",
            channel="rest",
            last_available_at=BASE,
        )
    )
    repository.save_checkpoint(
        CollectionCheckpoint(
            venue="hyperliquid",
            stream_key="hyperliquid:trade:BTC:trades",
            connection_id=uuid4(),
            last_sequence=41,
            last_event_id="trade-prior",
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            checkpointed_at=BASE,
            canonical_instrument_id="BTC",
            venue_symbol="BTC",
            event_type="trade",
            channel="trades",
            last_trade_id="trade-41",
        )
    )
    source = SplitSource((replace(envelope(sequence=42), channel="trades"),))
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(source,),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("ohlcv", "trade"),
        collection_enabled=True,
        maximum_cycles=1,
        poll_interval_seconds=0.001,
    )
    await collector.run()
    assert source.rest_checkpoints[0] is not None
    assert source.rest_checkpoints[0].last_available_at == BASE
    assert source.websocket_checkpoints[0] is not None
    assert source.websocket_checkpoints[0].last_sequence == 41
    assert source.websocket_checkpoints[0].last_trade_id == "trade-41"
    assert source.calls.index("rest") < source.calls.index("websocket")


@pytest.mark.asyncio
async def test_websocket_worker_keeps_connection_for_multiple_messages() -> None:
    source = SplitSource(
        tuple(
            replace(
                envelope(sequence=number, stable_event_key=f"trade-{number}"),
                channel="trades",
            )
            for number in (1, 2, 3)
        )
    )
    repository = InMemoryResearchRepository()
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(source,),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("trade",),
        collection_enabled=True,
        maximum_cycles=3,
    )
    await collector.run()
    assert len(source.websocket_checkpoints) == 1
    assert len([item for item in repository.raw_events() if item.event_type == "trade"]) == 3


@pytest.mark.asyncio
async def test_orderbook_restart_restores_sequence_epoch_and_recovery_requirement() -> None:
    repository = InMemoryResearchRepository()
    connection_id = uuid4()
    repository.save_checkpoint(
        CollectionCheckpoint(
            venue="hyperliquid",
            stream_key="hyperliquid:orderbook:BTC:orderbook",
            connection_id=connection_id,
            last_sequence=102,
            last_event_id="book-102",
            reconciliation_state=ReconciliationState.DEGRADED,
            checkpointed_at=BASE,
            canonical_instrument_id="BTC",
            venue_symbol="BTC",
            event_type="orderbook",
            channel="orderbook",
            snapshot_sequence=100,
            delta_sequence=102,
            connection_epoch=7,
            recovery_required=True,
        )
    )
    source = SplitSource(
        (
            replace(
                envelope(stable_event_key="book-recovered"),
                event_type="orderbook_snapshot",
                channel="orderbook",
                sequence=None,
                snapshot_sequence=110,
                connection_id=uuid4(),
                connection_epoch=8,
                reconciliation_state=ReconciliationState.SYNCHRONIZED,
                recovery_completed=True,
            ),
        )
    )
    collector = ResearchMarketDataCollector(
        repository=repository,
        sources=(source,),
        capability_gate=Gate(CapabilitySupport.LIVE_VERIFIED),
        instruments=("BTC",),
        event_types=("orderbook_snapshot",),
        collection_enabled=True,
        maximum_cycles=1,
    )
    await collector.run()
    resumed = source.websocket_checkpoints[0]
    assert resumed is not None
    assert resumed.snapshot_sequence == 100
    assert resumed.delta_sequence == 102
    assert resumed.connection_epoch == 7
    assert resumed.recovery_required
    final = repository.get_checkpoint("hyperliquid", "hyperliquid:orderbook:BTC:orderbook")
    assert final is not None and not final.recovery_required
    assert final.snapshot_sequence == 110
    assert final.connection_epoch == 8


def test_orderbook_gap_requires_snapshot_recovery_before_synchronized() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    connection_id = uuid4()
    base = replace(
        envelope(stable_event_key="book-10"),
        event_type="orderbook_delta",
        channel="orderbook",
        connection_id=connection_id,
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        sequence=10,
        delta_sequence=10,
        snapshot_sequence=8,
        connection_epoch=1,
    )
    consumer.persist(base)
    consumer.persist(replace(base, stable_event_key="book-12", sequence=12, delta_sequence=12))
    key = "hyperliquid:orderbook:BTC:orderbook"
    gap = repository.get_checkpoint("hyperliquid", key)
    assert gap is not None and gap.recovery_required
    assert gap.reconciliation_state == ReconciliationState.DEGRADED
    consumer.persist(
        replace(
            base,
            stable_event_key="book-delta-after-gap",
            sequence=13,
            delta_sequence=13,
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            recovery_completed=False,
        )
    )
    unrecovered = repository.get_checkpoint("hyperliquid", key)
    assert unrecovered is not None and unrecovered.recovery_required
    consumer.persist(
        replace(
            base,
            stable_event_key="book-recovery-snapshot",
            event_type="orderbook_snapshot",
            sequence=None,
            delta_sequence=None,
            snapshot_sequence=20,
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            recovery_completed=True,
        )
    )
    recovered = repository.get_checkpoint("hyperliquid", key)
    assert recovered is not None and not recovered.recovery_required
    assert recovered.reconciliation_state == ReconciliationState.SYNCHRONIZED


def test_bitget_gap_without_snapshot_remains_degraded() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    connection_id = uuid4()
    snapshot = replace(
        envelope(stable_event_key="bitget-snapshot"),
        venue="bitget",
        canonical_instrument_id="BTC",
        venue_symbol="BTCUSDT",
        event_type="orderbook_snapshot",
        channel="orderbook",
        connection_id=connection_id,
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        snapshot_sequence=100,
        connection_epoch=1,
        stream_semantics=OrderBookStreamSemantics.SNAPSHOT_AND_DELTA,
        bootstrap_completed=True,
        recovery_completed=True,
    )
    consumer.persist(snapshot)
    consumer.persist(
        replace(
            snapshot,
            stable_event_key="bitget-gap",
            event_type="orderbook_delta",
            sequence=110,
            delta_sequence=110,
            previous_delta_sequence=99,
        )
    )
    checkpoint = repository.get_checkpoint("bitget", "bitget:orderbook:BTCUSDT:orderbook")
    assert checkpoint is not None
    assert checkpoint.reconciliation_state == ReconciliationState.DEGRADED
    assert checkpoint.recovery_required


def test_old_epoch_orderbook_delta_is_rejected() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    repository.save_checkpoint(
        CollectionCheckpoint(
            venue="bitget",
            stream_key="bitget:orderbook:BTCUSDT:orderbook",
            connection_id=uuid4(),
            last_sequence=200,
            last_event_id="current",
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            checkpointed_at=BASE,
            canonical_instrument_id="BTC",
            venue_symbol="BTCUSDT",
            event_type="orderbook",
            channel="orderbook",
            snapshot_sequence=190,
            delta_sequence=200,
            connection_epoch=4,
            recovery_required=False,
            bootstrap_completed=True,
        )
    )
    consumer.persist(
        replace(
            envelope(stable_event_key="old-epoch"),
            venue="bitget",
            canonical_instrument_id="BTC",
            venue_symbol="BTCUSDT",
            event_type="orderbook_delta",
            channel="orderbook",
            connection_id=uuid4(),
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            sequence=201,
            delta_sequence=201,
            previous_delta_sequence=200,
            connection_epoch=3,
            stream_semantics=OrderBookStreamSemantics.SNAPSHOT_AND_DELTA,
            bootstrap_completed=True,
            recovery_completed=True,
        )
    )
    checkpoint = repository.get_checkpoint("bitget", "bitget:orderbook:BTCUSDT:orderbook")
    assert checkpoint is not None and checkpoint.last_event_id == "current"
    assert any("old connection epoch" in reason for _, reason, _ in repository.quarantined)


def test_hyperliquid_snapshot_recovery_clears_requirement_without_sequence() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    repository.save_checkpoint(
        CollectionCheckpoint(
            venue="hyperliquid",
            stream_key="hyperliquid:orderbook:BTC:orderbook",
            connection_id=uuid4(),
            last_sequence=None,
            last_event_id="disconnect",
            reconciliation_state=ReconciliationState.DISCONNECTED,
            checkpointed_at=BASE,
            canonical_instrument_id="BTC",
            venue_symbol="BTC",
            event_type="orderbook",
            channel="orderbook",
            connection_epoch=2,
            recovery_required=True,
            recovery_started_at=BASE,
        )
    )
    completed = BASE + timedelta(seconds=1)
    consumer.persist(
        replace(
            envelope(stable_event_key="hl-new-snapshot"),
            event_type="orderbook_snapshot",
            channel="orderbook",
            connection_id=uuid4(),
            reconciliation_state=ReconciliationState.SYNCHRONIZED,
            connection_epoch=3,
            stream_semantics=OrderBookStreamSemantics.SNAPSHOT_ONLY,
            bootstrap_completed=True,
            recovery_completed=True,
            recovery_started_at=BASE,
            recovery_completed_at=completed,
        )
    )
    checkpoint = repository.get_checkpoint("hyperliquid", "hyperliquid:orderbook:BTC:orderbook")
    assert checkpoint is not None
    assert checkpoint.reconciliation_state == ReconciliationState.SYNCHRONIZED
    assert not checkpoint.recovery_required
    assert checkpoint.bootstrap_completed
    assert checkpoint.snapshot_sequence is None
    assert checkpoint.recovery_completed_at == completed


def test_shutdown_disconnect_is_not_a_recovery_failure() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    key = "hyperliquid:orderbook:BTC:orderbook"
    current = CollectionCheckpoint(
        venue="hyperliquid",
        stream_key=key,
        connection_id=uuid4(),
        last_sequence=None,
        last_event_id="book",
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        checkpointed_at=BASE,
        canonical_instrument_id="BTC",
        venue_symbol="BTC",
        event_type="orderbook",
        channel="orderbook",
        connection_epoch=1,
        recovery_required=False,
        bootstrap_completed=True,
    )
    repository.save_checkpoint(current)
    payload = json.dumps({"client_initiated_close": True, "reason": "client_shutdown"})
    consumer.persist(
        replace(
            envelope(raw_payload=payload, stable_event_key="shutdown"),
            event_type="websocket_disconnect",
            channel="control",
            logical_stream_event_type="orderbook",
            connection_id=current.connection_id,
            connection_epoch=1,
        )
    )
    checkpoint = repository.get_checkpoint("hyperliquid", key)
    assert checkpoint == current
    assert consumer.metrics.disconnect_count == 0


def test_unsynchronized_book_is_excluded_and_control_only_snapshot_is_not_eligible() -> None:
    repository = InMemoryResearchRepository()
    book = replace(
        event("unsynchronized-book", event_type="orderbook_delta"),
        reconciliation_state=ReconciliationState.DEGRADED,
    )
    health = event("health", event_type="venue_health", payload={"status": "up"})
    repository.add_raw_event(book)
    repository.add_raw_event(health)
    manifest = DataSnapshotService(repository).finalize(
        cutoff_at=BASE,
        finalized_at=BASE,
        eligibility_policy=SnapshotEligibilityPolicy(minimum_production_events=1),
    )
    assert tuple(item[1] for item in manifest.events) == ("health",)
    assert manifest.eligibility_status == "FINALIZED_NOT_ELIGIBLE"
    assert "unsynchronized order-book events present" in manifest.eligibility_reasons


def test_collection_failure_is_persisted_with_sanitized_endpoint_and_secret() -> None:
    repository = InMemoryResearchRepository()
    consumer = persistence_consumer(repository)
    identity = ResearchStreamIdentity("hyperliquid", "BTC", "BTC", "trade", "trades")
    consumer.persist_failure(
        identity,
        "https://user:password@example.test/ws?api_key=visible",
        RuntimeError('headers={"Authorization": "Bearer visible-token"}; socket failed'),
        2,
    )
    failure = repository.collection_failures()[0]
    assert failure.endpoint == "https://example.test/ws"
    assert "visible" not in failure.error_message
    assert "password" not in failure.endpoint
    assert failure.retry_count == 2


def test_websocket_raw_frame_is_preserved_separately_from_normalized_event() -> None:
    source = PublicAdapterCollectorSource(object(), "hyperliquid")  # type: ignore[arg-type]
    raw_frame = '{"channel":"trades","data":[{"px":"100","sz":"1"}]}'
    trade = Trade(
        exchange="hyperliquid",
        symbol="BTC",
        exchange_timestamp=BASE,
        received_at=BASE,
        available_at=BASE,
        trade_id="trade-1",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=Side.BUY,
        source_raw_payload=raw_frame,
        source_payload_sha256=hashlib.sha256(raw_frame.encode()).hexdigest(),
    )
    identity = ResearchStreamIdentity("hyperliquid", "BTC", "BTC", "trade", "trades")
    collected = source._envelope(identity, trade)
    assert collected.raw_payload == raw_frame
    assert collected.normalized_payload is not None
    assert "source_raw_payload" not in collected.normalized_payload


@pytest.mark.asyncio
async def test_public_rest_ohlcv_uses_checkpoint_cursor_and_explicit_end() -> None:
    class OhlcvAdapter:
        client = None

        def __init__(self) -> None:
            self.calls: list[tuple[datetime | None, datetime | None]] = []

        async def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            start: datetime | None = None,
            end: datetime | None = None,
            limit: int = 1000,
        ) -> tuple[object, ...]:
            del symbol, timeframe, limit
            self.calls.append((start, end))
            return ()

    adapter = OhlcvAdapter()
    source = PublicAdapterCollectorSource(
        adapter,
        "hyperliquid",  # type: ignore[arg-type]
    )
    checkpoint = CollectionCheckpoint(
        venue="hyperliquid",
        stream_key="hyperliquid:ohlcv:BTC:rest",
        connection_id=uuid4(),
        last_sequence=None,
        last_event_id="last-candle",
        reconciliation_state=ReconciliationState.SYNCHRONIZED,
        checkpointed_at=BASE,
        last_available_at=BASE,
    )
    identity = ResearchStreamIdentity("hyperliquid", "BTC", "BTC", "ohlcv", "rest")
    assert [item async for item in source.rest_events(identity, checkpoint)] == []
    assert adapter.calls[0][0] == BASE
    assert adapter.calls[0][1] is not None and adapter.calls[0][1] > BASE


@pytest.mark.asyncio
async def test_unknown_instrument_fees_and_limits_remain_none_not_zero() -> None:
    class RuleAdapter:
        client = None

        async def fetch_markets(self) -> list[Market]:
            return [
                Market(
                    exchange="hyperliquid",
                    symbol="BTC",
                    base="BTC",
                    quote="USDC",
                    market_type="perpetual",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("0.001"),
                    minimum_notional=None,
                )
            ]

    source = PublicAdapterCollectorSource(
        RuleAdapter(),
        "hyperliquid",  # type: ignore[arg-type]
    )
    rule = (await source.instrument_rules(("BTC",)))[0]
    assert rule.minimum_quantity is None
    assert rule.minimum_notional is None
    assert rule.maker_fee is None
    assert rule.taker_fee is None
    assert rule.maker_rebate is None
    assert rule.funding_interval is None
    assert rule.field_evidence["maker_fee"]["verification_status"] == "unknown"
    assert rule.fee_tier.value == "unknown"


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
        field_evidence={
            name: {"verification_status": "verified"}
            for name in (
                "tick_size",
                "lot_size",
                "minimum_quantity",
                "minimum_notional",
                "maker_fee",
                "taker_fee",
                "maker_rebate",
                "funding_interval",
                "margin_asset",
            )
        },
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


def test_database_trigger_rejects_finalized_snapshot_row_update_and_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "immutable.db"
    url = f"sqlite+pysqlite:///{database}"
    monkeypatch.setenv("APP_DATABASE_URL", url)
    command.upgrade(Config("alembic.ini"), "head")
    engine = build_engine(url)
    repository = PostgreSQLResearchRepository(engine)
    repository.add_raw_event(event("immutable-event"))
    manifest = DataSnapshotService(repository).finalize(cutoff_at=BASE, finalized_at=BASE)
    with engine.begin() as connection, pytest.raises(DBAPIError, match="immutable"):
        connection.execute(
            text(
                "UPDATE data_snapshots SET content_sha256 = :digest "
                "WHERE snapshot_id = :snapshot_id"
            ),
            {"digest": "f" * 64, "snapshot_id": manifest.snapshot_id},
        )
    with engine.begin() as connection, pytest.raises(DBAPIError, match="immutable"):
        connection.execute(
            text("DELETE FROM data_snapshots WHERE snapshot_id = :snapshot_id"),
            {"snapshot_id": manifest.snapshot_id},
        )
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


def test_collector_health_report_is_available_after_event_loop_shutdown() -> None:
    assert CollectorHealthMetrics().report()["task_count"] == 0
