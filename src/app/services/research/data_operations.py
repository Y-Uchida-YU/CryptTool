from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from app.adapters.exchanges.base import CapabilityUnavailableError, MarketDataAdapter
from app.adapters.exchanges.websocket import ReconciliationState
from app.domain.venues.models import CapabilitySupport
from app.domain.venues.trusted_capabilities import TrustedCapabilityRegistry
from app.services.research.models import (
    CollectionCheckpoint,
    DataSnapshotManifest,
    InstrumentRuleSnapshot,
    RawMarketEvent,
    canonical_sha256,
    utc,
)
from app.services.research.repository import ResearchRepository

SUPPORTED_VENUES = (
    "hyperliquid",
    "bitget",
    "aster",
    "mexc",
    "dydx",
    "paradex",
    "lighter",
)
SUPPORTED_INSTRUMENTS = ("BTC", "ETH", "SOL", "HYPE")

VENUE_SYMBOLS: dict[str, dict[str, str]] = {
    "hyperliquid": {item: item for item in SUPPORTED_INSTRUMENTS},
    "bitget": {item: f"{item}USDT" for item in SUPPORTED_INSTRUMENTS},
    "aster": {item: f"{item}USDT" for item in SUPPORTED_INSTRUMENTS},
    "mexc": {item: f"{item}_USDT" for item in SUPPORTED_INSTRUMENTS},
    "dydx": {item: f"{item}-USD" for item in SUPPORTED_INSTRUMENTS},
    "paradex": {item: f"{item}-USD-PERP" for item in SUPPORTED_INSTRUMENTS},
    "lighter": {item: item for item in SUPPORTED_INSTRUMENTS},
}

EVENT_CAPABILITIES = {
    "ohlcv": "trades",
    "trade": "trades",
    "orderbook_snapshot": "orderbook_snapshot",
    "orderbook_delta": "orderbook_delta",
    "funding_current": "funding_current",
    "funding_history": "funding_history",
    "open_interest": "open_interest",
    "mark_price": "mark_price",
    "index_price": "index_price",
}


@dataclass(frozen=True)
class CapabilityDecision:
    support: CapabilitySupport
    verification_run_id: str

    @property
    def production_eligible(self) -> bool:
        return self.support == CapabilitySupport.LIVE_VERIFIED


class ResearchCapabilityGate(Protocol):
    def decide(self, *, venue: str, capability: str, now: datetime) -> CapabilityDecision: ...


class TrustedResearchCapabilityGate:
    def __init__(self, registry: TrustedCapabilityRegistry) -> None:
        self.registry = registry

    def decide(self, *, venue: str, capability: str, now: datetime) -> CapabilityDecision:
        try:
            record = self.registry.require_live_verified(
                venue=venue, capability=capability, now=now
            )
        except ValueError:
            return CapabilityDecision(CapabilitySupport.IMPLEMENTED, "")
        return CapabilityDecision(record.support, record.verification_run_id)


@dataclass(frozen=True)
class CollectedEnvelope:
    venue: str
    canonical_instrument_id: str
    venue_symbol: str
    event_type: str
    source_endpoint: str
    raw_payload: str
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    sequence: int | None = None
    connection_id: UUID | None = None
    reconciliation_state: ReconciliationState | None = None
    stable_event_key: str | None = None
    normalized_payload: str | None = None


class CollectorSource(Protocol):
    venue: str

    def events(
        self,
        *,
        instruments: tuple[str, ...],
        event_types: tuple[str, ...],
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]: ...

    async def close(self) -> None: ...


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (Decimal, datetime, UUID)):
        return str(value)
    return value


class PublicAdapterCollectorSource:
    """Connects the existing public adapter contract to immutable research envelopes."""

    def __init__(self, adapter: MarketDataAdapter, venue: str) -> None:
        self.adapter = adapter
        self.venue = venue
        self._raw_responses: list[str] = []
        client = getattr(adapter, "client", None)
        if client is not None:
            client.event_hooks.setdefault("response", []).append(self._capture_response)

    async def _capture_response(self, response: Any) -> None:
        await response.aread()
        self._raw_responses.append(response.text)

    async def events(
        self,
        *,
        instruments: tuple[str, ...],
        event_types: tuple[str, ...],
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]:
        del checkpoint
        for instrument in instruments:
            symbol = canonical_symbol(self.venue, instrument)
            for event_type in event_types:
                self._raw_responses.clear()
                try:
                    values = await self._fetch(event_type, symbol)
                except (CapabilityUnavailableError, KeyError, ValueError):
                    continue
                for value in values:
                    now = datetime.now(UTC)
                    payload = json.dumps(_json_value(value), sort_keys=True, separators=(",", ":"))
                    source_payload = self._raw_responses[-1] if self._raw_responses else payload
                    exchange_at = getattr(value, "exchange_timestamp", None)
                    received_at = getattr(value, "received_at", None) or now
                    available_at = getattr(value, "available_at", None) or received_at
                    yield CollectedEnvelope(
                        venue=self.venue,
                        canonical_instrument_id=instrument,
                        venue_symbol=symbol,
                        event_type=event_type,
                        source_endpoint=f"public-adapter:{type(self.adapter).__name__}:{event_type}",
                        raw_payload=source_payload,
                        normalized_payload=payload,
                        exchange_timestamp=exchange_at,
                        received_at=received_at,
                        available_at=available_at,
                        sequence=getattr(value, "sequence", None),
                        reconciliation_state=(
                            ReconciliationState.SYNCHRONIZED
                            if event_type == "orderbook_delta"
                            else None
                        ),
                    )

    async def instrument_rules(
        self, instruments: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]:
        markets = await self.adapter.fetch_markets()
        now = datetime.now(UTC)
        requested = {canonical_symbol(self.venue, item): item for item in instruments}
        result: list[InstrumentRuleSnapshot] = []
        for market in markets:
            canonical = requested.get(market.symbol)
            if canonical is None or market.tick_size is None or market.lot_size is None:
                continue
            payload = json.dumps(
                market.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
            )
            source_hash = hashlib.sha256(payload.encode()).hexdigest()
            result.append(
                InstrumentRuleSnapshot(
                    rule_snapshot_id=(
                        f"rule-{self.venue}-{canonical}-"
                        f"{now.strftime('%Y%m%dT%H%M%S%fZ')}-{source_hash[:12]}"
                    ),
                    venue=self.venue,
                    canonical_instrument_id=canonical,
                    venue_symbol=market.symbol,
                    tick_size=market.tick_size,
                    lot_size=market.lot_size,
                    minimum_quantity=market.lot_size,
                    minimum_notional=market.minimum_notional or Decimal(0),
                    maker_fee=Decimal(0),
                    taker_fee=Decimal(0),
                    maker_rebate=Decimal(0),
                    funding_interval=8,
                    margin_asset=market.quote,
                    source_endpoint=f"public-adapter:{type(self.adapter).__name__}:fetch_markets",
                    source_payload_sha256=source_hash,
                    retrieved_at=now,
                    valid_from=now,
                    valid_until=None,
                )
            )
        return tuple(result)

    async def _fetch(self, event_type: str, symbol: str) -> Sequence[Any]:
        if event_type == "ohlcv":
            return await self.adapter.fetch_ohlcv(symbol, "1m", limit=100)
        if event_type == "trade":
            return await self.adapter.fetch_recent_trades(symbol, limit=100)
        if event_type == "orderbook_snapshot":
            return (await self.adapter.fetch_order_book(symbol, depth=50),)
        if event_type == "orderbook_delta":
            book_iterator = self.adapter.stream_order_book(symbol).__aiter__()
            return (await asyncio.wait_for(anext(book_iterator), timeout=10),)
        if event_type in {"funding_current", "funding_history"}:
            values = await self.adapter.fetch_funding_rates(symbol)
            return values[-1:] if event_type == "funding_current" else values
        if event_type == "open_interest":
            return await self.adapter.fetch_open_interest(symbol)
        if event_type in {"mark_price", "index_price"}:
            ticker_iterator = self.adapter.stream_ticker(symbol).__aiter__()
            value = await asyncio.wait_for(anext(ticker_iterator), timeout=10)
            return ({"price_type": event_type, **value},)
        raise CapabilityUnavailableError(event_type)

    async def close(self) -> None:
        close = getattr(self.adapter, "close", None)
        if close is not None:
            await close()


def canonical_symbol(venue: str, instrument: str) -> str:
    try:
        return VENUE_SYMBOLS[venue][instrument]
    except KeyError as exc:
        raise ValueError(f"unsupported venue/instrument mapping: {venue}/{instrument}") from exc


@dataclass(frozen=True)
class CollectorResult:
    production_counts: dict[str, int]
    experimental_counts: dict[str, int]
    quarantine_count: int


class ResearchMarketDataCollector:
    def __init__(
        self,
        *,
        repository: ResearchRepository,
        sources: tuple[CollectorSource, ...],
        capability_gate: ResearchCapabilityGate,
        instruments: tuple[str, ...] = SUPPORTED_INSTRUMENTS,
        event_types: tuple[str, ...] = tuple(EVENT_CAPABILITIES),
        collection_enabled: bool = False,
        poll_interval_seconds: float = 30,
        maximum_cycles: int | None = None,
        stale_after_seconds: int = 120,
    ) -> None:
        if any(item not in SUPPORTED_INSTRUMENTS for item in instruments):
            raise ValueError("collector instrument is outside the R2 allowlist")
        self.repository = repository
        self.sources = sources
        self.capability_gate = capability_gate
        self.instruments = instruments
        self.event_types = event_types
        self.collection_enabled = collection_enabled
        self.poll_interval_seconds = poll_interval_seconds
        self.maximum_cycles = maximum_cycles
        self.stale_after = timedelta(seconds=stale_after_seconds)
        self._shutdown = asyncio.Event()
        self._production_counts: dict[str, int] = {}
        self._experimental_counts: dict[str, int] = {}
        self._quarantine_count = 0

    async def run(self) -> None:
        if not self.collection_enabled:
            raise RuntimeError("research collection requires collection_enabled=true")
        cycles = 0
        try:
            while not self._shutdown.is_set():
                await asyncio.gather(*(self._collect_source(source) for source in self.sources))
                cycles += 1
                if self.maximum_cycles is not None and cycles >= self.maximum_cycles:
                    break
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._shutdown.wait(), timeout=self.poll_interval_seconds
                    )
        finally:
            await asyncio.gather(
                *(source.close() for source in self.sources), return_exceptions=True
            )

    def shutdown(self) -> None:
        self._shutdown.set()

    @property
    def result(self) -> CollectorResult:
        return CollectorResult(
            dict(self._production_counts),
            dict(self._experimental_counts),
            self._quarantine_count,
        )

    async def _collect_source(self, source: CollectorSource) -> None:
        stream_key = "public-rest-and-websocket"
        checkpoint = self.repository.get_checkpoint(source.venue, stream_key)
        connection_id = uuid4()
        try:
            rule_loader = getattr(source, "instrument_rules", None)
            if rule_loader is not None:
                for rule in await rule_loader(self.instruments):
                    self.repository.save_instrument_rule(rule)
            async for envelope in source.events(
                instruments=self.instruments,
                event_types=self.event_types,
                checkpoint=checkpoint,
            ):
                event = self._event(envelope, connection_id)
                capability = EVENT_CAPABILITIES.get(envelope.event_type)
                decision = (
                    self.capability_gate.decide(
                        venue=envelope.venue,
                        capability=capability,
                        now=envelope.received_at,
                    )
                    if capability
                    else CapabilityDecision(
                        CapabilitySupport.LIVE_VERIFIED, "collector-control-plane"
                    )
                )
                source_hash = hashlib.sha256(envelope.raw_payload.encode()).hexdigest()
                payload_id = f"payload-{source_hash}"
                event = RawMarketEvent(
                    **{
                        **event.__dict__,
                        "capability_verification_run_id": decision.verification_run_id
                        or "unverified-experimental",
                        "raw_payload_id": payload_id,
                        "source_payload_sha256": source_hash,
                    }
                )
                self.repository.save_raw_payload(
                    payload_id=payload_id,
                    venue=event.venue,
                    source_endpoint=envelope.source_endpoint,
                    payload_sha256=source_hash,
                    raw_payload=envelope.raw_payload,
                    received_at=event.received_at,
                )
                invalid_reason: str | None = None
                try:
                    event.payload()
                except (ValueError, json.JSONDecodeError) as exc:
                    invalid_reason = f"normalization failure: {exc}"
                if event.available_at < event.received_at:
                    invalid_reason = "available_at precedes received_at"
                if (
                    event.exchange_timestamp is not None
                    and event.exchange_timestamp > event.received_at
                ):
                    invalid_reason = "abnormal future exchange timestamp"
                if not decision.production_eligible:
                    self._persist_experimental(event, decision.support)
                    continue
                existing = self.repository.get_raw_event(event.event_id)
                if existing is not None:
                    if existing.payload_sha256 != event.payload_sha256:
                        self.repository.quarantine(
                            event, "event_id payload conflict", event.received_at
                        )
                        self._quarantine_count += 1
                        raise ValueError("different payload with same event_id")
                    continue
                if invalid_reason is not None:
                    self._persist_production(event)
                    self.repository.quarantine(event, invalid_reason, event.received_at)
                    self._quarantine_count += 1
                    continue
                if self._sequence_invalid(checkpoint, event):
                    self._persist_production(event)
                    self.repository.quarantine(
                        event, "sequence gap: collector entered degraded state", event.received_at
                    )
                    self._quarantine_count += 1
                    self._emit_control_event(event, "sequence_gap", connection_id)
                    state = ReconciliationState.DEGRADED
                else:
                    self._persist_production(event)
                    state = event.reconciliation_state or ReconciliationState.SYNCHRONIZED
                checkpoint = CollectionCheckpoint(
                    venue=source.venue,
                    stream_key=stream_key,
                    connection_id=connection_id,
                    last_sequence=event.sequence,
                    last_event_id=event.event_id,
                    reconciliation_state=state,
                    checkpointed_at=event.received_at,
                )
                self.repository.save_checkpoint(checkpoint)
                if datetime.now(UTC) - event.available_at > self.stale_after:
                    self._emit_control_event(event, "stale_stream", connection_id)
            self._emit_venue_health(source.venue, connection_id, True)
        except Exception:
            self._emit_venue_health(source.venue, connection_id, False)
            return

    def _event(self, envelope: CollectedEnvelope, connection_id: UUID) -> RawMarketEvent:
        normalized_payload = envelope.normalized_payload or envelope.raw_payload
        payload_hash = hashlib.sha256(normalized_payload.encode()).hexdigest()
        stable = envelope.stable_event_key or canonical_sha256(
            (
                envelope.venue,
                envelope.venue_symbol,
                envelope.event_type,
                envelope.exchange_timestamp,
                envelope.sequence,
                payload_hash,
            )
        )
        event_id = f"{envelope.venue}-{stable}"
        return RawMarketEvent(
            event_id=event_id,
            venue=envelope.venue,
            canonical_instrument_id=envelope.canonical_instrument_id,
            venue_symbol=envelope.venue_symbol,
            event_type=envelope.event_type,
            exchange_timestamp=envelope.exchange_timestamp,
            received_at=envelope.received_at,
            available_at=envelope.available_at,
            sequence=envelope.sequence,
            connection_id=envelope.connection_id or connection_id,
            reconciliation_state=envelope.reconciliation_state,
            payload_sha256=payload_hash,
            raw_payload=normalized_payload,
            normalizer_version="research-collector-r2-v1",
            capability_verification_run_id="pending",
            created_at=envelope.received_at,
        )

    def _persist_production(self, event: RawMarketEvent) -> None:
        current = self.repository.get_raw_event(event.event_id)
        if current is not None:
            if current.payload_sha256 != event.payload_sha256:
                self.repository.quarantine(event, "event_id payload conflict", event.received_at)
                self._quarantine_count += 1
                raise ValueError("different payload with same event_id")
            return
        if self.repository.add_raw_event(event):
            self._production_counts[event.venue] = self._production_counts.get(event.venue, 0) + 1

    def _persist_experimental(self, event: RawMarketEvent, support: CapabilitySupport) -> None:
        if self.repository.add_experimental_event(event, support.value):
            self._experimental_counts[event.venue] = (
                self._experimental_counts.get(event.venue, 0) + 1
            )

    @staticmethod
    def _sequence_invalid(checkpoint: CollectionCheckpoint | None, event: RawMarketEvent) -> bool:
        return (
            checkpoint is not None
            and checkpoint.last_sequence is not None
            and event.sequence is not None
            and event.sequence != checkpoint.last_sequence + 1
        )

    def _emit_control_event(
        self, source: RawMarketEvent, event_type: str, connection_id: UUID
    ) -> None:
        payload = json.dumps(
            {"source_event_id": source.event_id, "sequence": source.sequence},
            sort_keys=True,
            separators=(",", ":"),
        )
        event = self._event(
            CollectedEnvelope(
                venue=source.venue,
                canonical_instrument_id=source.canonical_instrument_id,
                venue_symbol=source.venue_symbol,
                event_type=event_type,
                source_endpoint="collector-control-plane",
                raw_payload=payload,
                exchange_timestamp=None,
                received_at=source.received_at,
                available_at=source.received_at,
                connection_id=connection_id,
                stable_event_key=f"{event_type}-{source.event_id}",
            ),
            connection_id,
        )
        self._persist_production(
            RawMarketEvent(
                **{
                    **event.__dict__,
                    "capability_verification_run_id": "collector-control-plane",
                }
            )
        )

    def _emit_venue_health(self, venue: str, connection_id: UUID, healthy: bool) -> None:
        now = datetime.now(UTC)
        payload = json.dumps({"healthy": healthy}, separators=(",", ":"))
        event = self._event(
            CollectedEnvelope(
                venue=venue,
                canonical_instrument_id="SYSTEM",
                venue_symbol="SYSTEM",
                event_type="venue_health" if healthy else "websocket_disconnect",
                source_endpoint="collector-control-plane",
                raw_payload=payload,
                exchange_timestamp=None,
                received_at=now,
                available_at=now,
                connection_id=connection_id,
            ),
            connection_id,
        )
        self._persist_production(
            RawMarketEvent(
                **{
                    **event.__dict__,
                    "capability_verification_run_id": "collector-control-plane",
                }
            )
        )


class DataSnapshotService:
    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def finalize(
        self,
        *,
        cutoff_at: datetime,
        snapshot_id: str | None = None,
        finalized_at: datetime | None = None,
    ) -> DataSnapshotManifest:
        cutoff = utc(cutoff_at, "cutoff_at")
        finalized = utc(finalized_at or datetime.now(UTC), "finalized_at")
        quarantined, reasons = self.repository.quarantine_summary(cutoff)
        quarantined_ids = set(quarantined)
        events = tuple(
            event
            for event in sorted(
                self.repository.raw_events(), key=lambda item: (item.available_at, item.event_id)
            )
            if event.available_at <= cutoff and event.event_id not in quarantined_ids
        )
        membership = tuple(
            (ordinal, event.event_id, event.payload_sha256) for ordinal, event in enumerate(events)
        )
        content_hash = canonical_sha256(membership)
        identifier = snapshot_id or (
            f"snapshot-{cutoff.strftime('%Y%m%dT%H%M%SZ')}-{content_hash[:12]}"
        )
        existing = self.repository.snapshot_manifest(identifier)
        if existing is not None:
            return existing
        outages = tuple(
            item.event_id
            for item in events
            if item.event_type in {"venue_outage", "websocket_disconnect"}
        )
        degraded = tuple(
            item.event_id
            for item in events
            if item.event_type in {"sequence_gap", "stale_stream", "clock_skew"}
        )
        unsigned: dict[str, object] = {
            "snapshot_id": identifier,
            "cutoff_at": cutoff,
            "events": membership,
            "quarantine_count": len(quarantined),
            "quarantine_reasons": reasons,
            "outage_event_ids": outages,
            "degraded_event_ids": degraded,
            "content_sha256": content_hash,
            "finalized_at": finalized,
        }
        manifest = DataSnapshotManifest(
            snapshot_id=identifier,
            cutoff_at=cutoff,
            events=membership,
            quarantine_count=len(quarantined),
            quarantine_reasons=reasons,
            outage_event_ids=outages,
            degraded_event_ids=degraded,
            content_sha256=content_hash,
            manifest_sha256=canonical_sha256(unsigned),
            finalized_at=finalized,
        )
        self.repository.finalize_snapshot(manifest)
        return manifest

    def verify(self, snapshot_id: str) -> DataSnapshotManifest:
        manifest = self.repository.snapshot_manifest(snapshot_id)
        if manifest is None:
            raise ValueError("snapshot does not exist or is not finalized")
        events = self.repository.snapshot_events(snapshot_id)
        actual_membership = tuple(
            (ordinal, event.event_id, event.payload_sha256) for ordinal, event in enumerate(events)
        )
        if actual_membership != manifest.events:
            raise ValueError("snapshot membership does not reproduce exactly")
        if canonical_sha256(actual_membership) != manifest.content_sha256:
            raise ValueError("snapshot content hash mismatch")
        unsigned = {
            key: value for key, value in manifest.__dict__.items() if key != "manifest_sha256"
        }
        if canonical_sha256(unsigned) != manifest.manifest_sha256:
            raise ValueError("snapshot manifest hash mismatch")
        return manifest


class ResearchRetentionService:
    """Separate retention domains while protecting every finalized membership."""

    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def purge_raw_payloads_before(self, cutoff_at: datetime) -> int:
        return self.repository.purge_raw_payloads_before(utc(cutoff_at, "cutoff_at"))

    def raw_event_is_protected(self, event_id: str) -> bool:
        return any(
            event_id == member_id
            for snapshot_id in self._snapshot_ids()
            for _, member_id, _ in (
                self.repository.snapshot_manifest(snapshot_id).events  # type: ignore[union-attr]
            )
        )

    def _snapshot_ids(self) -> tuple[str, ...]:
        if hasattr(self.repository, "snapshot_manifests"):
            return tuple(self.repository.snapshot_manifests)
        return ()


def write_snapshot_manifest(path: Path, manifest: DataSnapshotManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest.__dict__, default=str, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
