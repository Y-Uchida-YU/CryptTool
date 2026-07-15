from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import resource
import subprocess  # nosec B404
import time
from collections import Counter
from collections.abc import AsyncIterator, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from app.adapters.exchanges.base import CapabilityUnavailableError, MarketDataAdapter
from app.adapters.exchanges.websocket import (
    OrderBookStreamSemantics,
    ReconciliationState,
    WebSocketConnectionLifecycle,
)
from app.domain.venues.models import CapabilitySupport
from app.domain.venues.trusted_capabilities import TrustedCapabilityRegistry
from app.services.research.models import (
    CollectionCheckpoint,
    CollectionFailureEvent,
    DataSnapshotManifest,
    FeeTierKind,
    InstrumentRuleSnapshot,
    RawMarketEvent,
    RuleVerificationStatus,
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
REST_EVENT_TYPES = ("ohlcv", "funding_current", "funding_history", "open_interest")
WEBSOCKET_EVENT_TYPES = (
    "trade",
    "orderbook_snapshot",
    "orderbook_delta",
    "mark_price",
    "index_price",
)
CONTROL_EVENT_TYPES = {
    "venue_health",
    "websocket_disconnect",
    "sequence_gap",
    "stale_stream",
    "clock_skew",
    "snapshot_recovery",
}
BOOK_EVENT_TYPES = {"orderbook_snapshot", "orderbook_delta"}

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
class ResearchStreamIdentity:
    venue: str
    canonical_instrument_id: str
    venue_symbol: str
    event_type: str
    channel: str

    @property
    def stream_key(self) -> str:
        return f"{self.venue}:{self.event_type}:{self.venue_symbol}:{self.channel}"


@dataclass(frozen=True)
class CapabilityDecision:
    support: CapabilitySupport
    verification_run_id: str

    @property
    def production_eligible(self) -> bool:
        return self.support == CapabilitySupport.LIVE_VERIFIED


class ResearchCapabilityGate(Protocol):
    def decide(
        self,
        *,
        venue: str,
        capability: str,
        canonical_instrument_id: str,
        now: datetime,
    ) -> CapabilityDecision: ...


class TrustedResearchCapabilityGate:
    def __init__(self, registry: TrustedCapabilityRegistry) -> None:
        self.registry = registry

    def decide(
        self,
        *,
        venue: str,
        capability: str,
        canonical_instrument_id: str,
        now: datetime,
    ) -> CapabilityDecision:
        try:
            record = self.registry.require_live_verified(
                venue=venue,
                capability=capability,
                canonical_instrument_id=canonical_instrument_id,
                now=now,
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
    channel: str = "unknown"
    sequence: int | None = None
    connection_id: UUID | None = None
    reconciliation_state: ReconciliationState | None = None
    stable_event_key: str | None = None
    normalized_payload: str | None = None
    trade_id: str | None = None
    snapshot_sequence: int | None = None
    delta_sequence: int | None = None
    previous_delta_sequence: int | None = None
    connection_epoch: int = 0
    recovery_completed: bool = False
    stream_semantics: OrderBookStreamSemantics | None = None
    bootstrap_completed: bool = False
    recovery_started_at: datetime | None = None
    recovery_completed_at: datetime | None = None
    last_recovery_failure: str | None = None
    logical_stream_event_type: str | None = None

    @property
    def stream_identity(self) -> ResearchStreamIdentity:
        logical_type = self.logical_stream_event_type or (
            "orderbook" if self.event_type in BOOK_EVENT_TYPES else self.event_type
        )
        return ResearchStreamIdentity(
            self.venue,
            self.canonical_instrument_id,
            self.venue_symbol,
            logical_type,
            self.channel,
        )


class CollectorSource(Protocol):
    venue: str

    def rest_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]: ...

    def websocket_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
        shutdown: asyncio.Event,
    ) -> AsyncIterator[CollectedEnvelope]: ...

    async def instrument_rules(
        self, instruments: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]: ...

    async def close(self) -> None: ...


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(
            mode="json", exclude={"source_raw_payload", "source_payload_sha256"}
        )
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _json_value(item)
            for key, item in value.items()
            if not str(key).startswith("_collector_")
        }
    if isinstance(value, (Decimal, datetime, UUID)):
        return str(value)
    return value


class PublicAdapterCollectorSource:
    """Turns the existing public adapter into separate REST and continuous WS sources."""

    def __init__(self, adapter: MarketDataAdapter, venue: str) -> None:
        self.adapter = adapter
        self.venue = venue
        self._http_lock = asyncio.Lock()
        self._raw_responses: list[str] = []
        client = getattr(adapter, "client", None)
        if client is not None:
            client.event_hooks.setdefault("response", []).append(self._capture_response)

    async def _capture_response(self, response: Any) -> None:
        await response.aread()
        self._raw_responses.append(response.text)

    def rest_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]:
        return self._rest_events(identity, checkpoint)

    async def historical_events(
        self,
        identity: ResearchStreamIdentity,
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1h",
    ) -> tuple[CollectedEnvelope, ...]:
        """Read an explicit historical interval while preserving captured public payloads."""
        if end <= start:
            raise ValueError("historical end must be after start")
        async with self._http_lock:
            self._raw_responses.clear()
            if identity.event_type == "ohlcv":
                values: Sequence[Any] = await self.adapter.fetch_ohlcv(
                    identity.venue_symbol,
                    timeframe,
                    start=start,
                    end=end,
                    limit=1000,
                )
            elif identity.event_type == "funding_history":
                values = await self.adapter.fetch_funding_rates(
                    identity.venue_symbol,
                    start=start,
                    end=end,
                )
            else:
                raise CapabilityUnavailableError(
                    f"historical {identity.event_type} is unavailable through the public adapter"
                )
            return tuple(
                self._envelope(identity, value, use_captured_response=True) for value in values
            )

    async def _rest_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
    ) -> AsyncIterator[CollectedEnvelope]:
        async with self._http_lock:
            self._raw_responses.clear()
            start = checkpoint.last_available_at if checkpoint else None
            if identity.event_type == "ohlcv":
                end = datetime.now(UTC)
                start = start or end - timedelta(minutes=1000)
                values: Sequence[Any] = await self.adapter.fetch_ohlcv(
                    identity.venue_symbol,
                    "1m",
                    start=start,
                    end=end,
                    limit=1000,
                )
            elif identity.event_type in {"funding_current", "funding_history"}:
                funding_start = checkpoint.last_funding_at if checkpoint else None
                if identity.event_type == "funding_current":
                    current_loader = getattr(self.adapter, "fetch_current_funding_rate", None)
                    if current_loader is None:
                        values = await self.adapter.fetch_funding_rates(
                            identity.venue_symbol, start=funding_start
                        )
                        values = values[-1:]
                    else:
                        values = (await current_loader(identity.venue_symbol),)
                else:
                    values = await self.adapter.fetch_funding_rates(
                        identity.venue_symbol, start=funding_start
                    )
            elif identity.event_type == "open_interest":
                values = await self.adapter.fetch_open_interest(identity.venue_symbol, start=start)
            else:
                raise CapabilityUnavailableError(identity.event_type)
            envelopes = tuple(
                self._envelope(identity, value, use_captured_response=True) for value in values
            )
        for item in envelopes:
            yield item

    def websocket_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
        shutdown: asyncio.Event,
    ) -> AsyncIterator[CollectedEnvelope]:
        return self._websocket_events(identity, checkpoint, shutdown)

    async def _websocket_events(
        self,
        identity: ResearchStreamIdentity,
        checkpoint: CollectionCheckpoint | None,
        shutdown: asyncio.Event,
    ) -> AsyncIterator[CollectedEnvelope]:
        connection_epoch = (checkpoint.connection_epoch + 1) if checkpoint else 1
        if identity.event_type == "trade":
            iterator: AsyncIterator[Any] = self.adapter.stream_trades(identity.venue_symbol)
            raw_type = "trade"
        elif identity.event_type == "orderbook":
            iterator = self.adapter.stream_order_book(identity.venue_symbol)
            raw_type = "orderbook_delta"
        elif identity.event_type in {"mark_price", "index_price"}:
            iterator = self.adapter.stream_ticker(identity.venue_symbol)
            raw_type = identity.event_type
        else:
            raise CapabilityUnavailableError(identity.event_type)
        try:
            async for value in iterator:
                if shutdown.is_set():
                    break
                for lifecycle_event in self._drain_connection_lifecycle(identity):
                    yield self._connection_lifecycle_envelope(
                        identity, lifecycle_event, connection_epoch
                    )
                raw_state = getattr(value, "reconciliation_state", None)
                state = ReconciliationState(raw_state) if raw_state is not None else None
                venue_sequence = getattr(value, "sequence", None)
                snapshot_sequence = getattr(value, "snapshot_sequence", None)
                semantics_raw = getattr(value, "stream_semantics", None)
                semantics = (
                    OrderBookStreamSemantics(semantics_raw) if semantics_raw is not None else None
                )
                self_contained_snapshot = semantics in {
                    OrderBookStreamSemantics.SNAPSHOT_ONLY,
                    OrderBookStreamSemantics.LIMITED_DEPTH_SNAPSHOT,
                } or (semantics is None and snapshot_sequence is None)
                yield self._envelope(
                    identity,
                    value,
                    event_type=(
                        "orderbook_snapshot"
                        if raw_type == "orderbook_delta" and self_contained_snapshot
                        else raw_type
                    ),
                    reconciliation_state=state,
                    snapshot_sequence=(None if self_contained_snapshot else snapshot_sequence),
                    delta_sequence=None if self_contained_snapshot else venue_sequence,
                    previous_delta_sequence=(
                        None
                        if self_contained_snapshot
                        else getattr(value, "previous_delta_sequence", None)
                    ),
                    connection_id=getattr(value, "connection_id", None),
                    connection_epoch=(
                        connection_epoch - 1 + max(1, getattr(value, "connection_epoch", 0))
                    ),
                    recovery_completed=bool(getattr(value, "bootstrap_completed", False))
                    and state == ReconciliationState.SYNCHRONIZED,
                    stream_semantics=semantics,
                    bootstrap_completed=bool(getattr(value, "bootstrap_completed", False)),
                    recovery_started_at=getattr(value, "recovery_started_at", None),
                    recovery_completed_at=getattr(value, "recovery_completed_at", None),
                    last_recovery_failure=getattr(value, "last_recovery_failure", None),
                )
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()

    def _drain_connection_lifecycle(
        self,
        identity: ResearchStreamIdentity,
    ) -> tuple[WebSocketConnectionLifecycle, ...]:
        queue = getattr(self.adapter, "connection_lifecycle_events", None)
        if queue is None:
            return ()
        matched: list[WebSocketConnectionLifecycle] = []
        for _ in range(len(queue)):
            event = queue.popleft()
            if event.instrument == identity.venue_symbol:
                matched.append(event)
            else:
                queue.append(event)
        return tuple(matched)

    def _envelope(
        self,
        identity: ResearchStreamIdentity,
        value: Any,
        *,
        event_type: str | None = None,
        reconciliation_state: ReconciliationState | None = None,
        snapshot_sequence: int | None = None,
        delta_sequence: int | None = None,
        previous_delta_sequence: int | None = None,
        connection_id: UUID | None = None,
        connection_epoch: int = 0,
        recovery_completed: bool = False,
        stream_semantics: OrderBookStreamSemantics | None = None,
        bootstrap_completed: bool = False,
        recovery_started_at: datetime | None = None,
        recovery_completed_at: datetime | None = None,
        last_recovery_failure: str | None = None,
        use_captured_response: bool = False,
    ) -> CollectedEnvelope:
        now = datetime.now(UTC)
        normalized = json.dumps(_json_value(value), sort_keys=True, separators=(",", ":"))
        websocket_raw = getattr(value, "source_raw_payload", None)
        websocket_hash = getattr(value, "source_payload_sha256", None)
        if websocket_raw is None and isinstance(value, dict):
            websocket_metadata = value.get("_collector_source", {})
            websocket_raw = websocket_metadata.get("raw_payload")
            websocket_hash = websocket_metadata.get("payload_sha256")
        if websocket_raw is not None and websocket_hash is not None:
            actual = hashlib.sha256(str(websocket_raw).encode()).hexdigest()
            if actual != websocket_hash:
                raise ValueError("websocket source payload hash mismatch")
        source_payload = (
            self._raw_responses[-1]
            if use_captured_response and self._raw_responses
            else websocket_raw or normalized
        )
        return CollectedEnvelope(
            venue=identity.venue,
            canonical_instrument_id=identity.canonical_instrument_id,
            venue_symbol=identity.venue_symbol,
            event_type=event_type or identity.event_type,
            channel=identity.channel,
            source_endpoint=f"public-adapter:{type(self.adapter).__name__}:{identity.channel}",
            raw_payload=source_payload,
            normalized_payload=normalized,
            exchange_timestamp=getattr(value, "exchange_timestamp", None),
            received_at=getattr(value, "received_at", None) or now,
            available_at=getattr(value, "available_at", None) or now,
            sequence=delta_sequence or getattr(value, "sequence", None),
            trade_id=getattr(value, "trade_id", None),
            connection_id=connection_id,
            reconciliation_state=reconciliation_state,
            snapshot_sequence=snapshot_sequence,
            delta_sequence=delta_sequence,
            previous_delta_sequence=previous_delta_sequence,
            connection_epoch=connection_epoch,
            recovery_completed=recovery_completed,
            stream_semantics=stream_semantics,
            bootstrap_completed=bootstrap_completed,
            recovery_started_at=recovery_started_at,
            recovery_completed_at=recovery_completed_at,
            last_recovery_failure=last_recovery_failure,
        )

    @staticmethod
    def _connection_lifecycle_envelope(
        identity: ResearchStreamIdentity,
        event: WebSocketConnectionLifecycle,
        base_connection_epoch: int,
    ) -> CollectedEnvelope:
        persisted_epoch = base_connection_epoch - 1 + event.connection_epoch
        payload = json.dumps(
            {
                "channel": identity.channel,
                "close_code": event.close_code,
                "close_message": event.close_message,
                "connected_at": event.connected_at.isoformat(),
                "connection_epoch": persisted_epoch,
                "connection_id": str(event.connection_id),
                "disconnected_at": event.disconnected_at.isoformat(),
                "duration_ms": event.duration_ms,
                "exception_type": event.exception_type,
                "heartbeat_received": event.heartbeat_received,
                "heartbeat_sent": event.heartbeat_sent,
                "messages_received": event.messages_received,
                "reason": event.reason,
                "server_initiated_close": event.server_initiated_close,
                "client_initiated_close": event.client_initiated_close,
                "stale_timeout": event.stale_timeout,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return CollectedEnvelope(
            venue=identity.venue,
            canonical_instrument_id=identity.canonical_instrument_id,
            venue_symbol=identity.venue_symbol,
            event_type="websocket_disconnect",
            channel="control",
            source_endpoint=f"websocket:{identity.channel}",
            raw_payload=payload,
            normalized_payload=payload,
            exchange_timestamp=None,
            received_at=event.disconnected_at,
            available_at=event.disconnected_at,
            stable_event_key=(
                f"disconnect-{event.connection_id}-{event.connection_epoch}-"
                f"{event.disconnected_at.isoformat()}"
            ),
            connection_id=event.connection_id,
            connection_epoch=persisted_epoch,
            logical_stream_event_type=identity.event_type,
            recovery_started_at=event.disconnected_at,
            last_recovery_failure=event.reason,
        )

    async def instrument_rules(
        self, instruments: tuple[str, ...]
    ) -> tuple[InstrumentRuleSnapshot, ...]:
        async with self._http_lock:
            self._raw_responses.clear()
            markets = await self.adapter.fetch_markets()
            captured_response = self._raw_responses[-1] if self._raw_responses else None
        now = datetime.now(UTC)
        requested = {canonical_symbol(self.venue, item): item for item in instruments}
        result: list[InstrumentRuleSnapshot] = []
        for market in markets:
            canonical = requested.get(market.symbol)
            if canonical is None:
                continue
            payload = json.dumps(
                market.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
            )
            source_payload = captured_response or payload
            source_hash = hashlib.sha256(source_payload.encode()).hexdigest()
            endpoint = f"public-adapter:{type(self.adapter).__name__}:fetch_markets"
            observed: dict[str, str | None] = {
                "source_endpoint": endpoint,
                "source_payload_sha256": source_hash,
                "retrieved_at": now.isoformat(),
                "verification_status": RuleVerificationStatus.OBSERVED.value,
            }
            unknown: dict[str, str | None] = {
                "source_endpoint": None,
                "source_payload_sha256": None,
                "retrieved_at": None,
                "verification_status": RuleVerificationStatus.UNKNOWN.value,
            }
            field_evidence: dict[str, dict[str, str | None]] = {
                "tick_size": observed if market.tick_size is not None else unknown,
                "lot_size": observed if market.lot_size is not None else unknown,
                "minimum_notional": observed if market.minimum_notional is not None else unknown,
                "minimum_quantity": unknown,
                "maker_fee": unknown,
                "taker_fee": unknown,
                "maker_rebate": unknown,
                "funding_interval": unknown,
                "margin_asset": observed,
            }
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
                    minimum_quantity=None,
                    minimum_notional=market.minimum_notional,
                    maker_fee=None,
                    taker_fee=None,
                    maker_rebate=None,
                    funding_interval=None,
                    margin_asset=market.quote,
                    source_endpoint=endpoint,
                    source_payload_sha256=source_hash,
                    retrieved_at=now,
                    valid_from=now,
                    valid_until=None,
                    field_evidence=field_evidence,
                    fee_tier=FeeTierKind.UNKNOWN,
                    verification_status=RuleVerificationStatus.OBSERVED,
                )
            )
        return tuple(result)

    async def close(self) -> None:
        close = getattr(self.adapter, "close", None)
        if close is not None:
            await close()


def canonical_symbol(venue: str, instrument: str) -> str:
    try:
        return VENUE_SYMBOLS[venue][instrument]
    except KeyError as exc:
        raise ValueError(f"unsupported venue/instrument mapping: {venue}/{instrument}") from exc


class CheckpointWriter:
    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def write(
        self,
        *,
        envelope: CollectedEnvelope,
        event_id: str,
        state: ReconciliationState,
        recovery_required: bool,
    ) -> CollectionCheckpoint:
        identity = envelope.stream_identity
        previous = self.repository.get_checkpoint(identity.venue, identity.stream_key)
        sequence = (
            envelope.delta_sequence if envelope.delta_sequence is not None else envelope.sequence
        )
        checkpoint = CollectionCheckpoint(
            venue=identity.venue,
            stream_key=identity.stream_key,
            connection_id=envelope.connection_id
            or (previous.connection_id if previous else uuid4()),
            last_sequence=(
                sequence if sequence is not None else previous.last_sequence if previous else None
            ),
            last_event_id=event_id,
            reconciliation_state=state,
            checkpointed_at=envelope.received_at,
            canonical_instrument_id=identity.canonical_instrument_id,
            venue_symbol=identity.venue_symbol,
            event_type=identity.event_type,
            channel=identity.channel,
            last_available_at=(
                envelope.available_at
                if identity.channel == "rest"
                else previous.last_available_at
                if previous
                else None
            ),
            last_funding_at=(
                envelope.exchange_timestamp
                if envelope.event_type == "funding_history"
                else previous.last_funding_at
                if previous
                else None
            ),
            last_trade_id=(
                envelope.trade_id
                if envelope.event_type == "trade"
                else previous.last_trade_id
                if previous
                else None
            ),
            snapshot_sequence=envelope.snapshot_sequence
            if envelope.snapshot_sequence is not None
            else previous.snapshot_sequence
            if previous
            else None,
            delta_sequence=envelope.delta_sequence
            if envelope.delta_sequence is not None
            else previous.delta_sequence
            if previous
            else None,
            connection_epoch=max(
                envelope.connection_epoch,
                previous.connection_epoch if previous else 0,
            ),
            recovery_required=recovery_required,
            bootstrap_completed=(
                envelope.bootstrap_completed
                if envelope.event_type in BOOK_EVENT_TYPES
                else previous.bootstrap_completed
                if previous
                else False
            ),
            recovery_started_at=(
                envelope.recovery_started_at
                if envelope.recovery_started_at is not None
                else previous.recovery_started_at
                if previous
                else None
            ),
            recovery_completed_at=(
                envelope.recovery_completed_at
                if envelope.recovery_completed_at is not None
                else previous.recovery_completed_at
                if previous
                else None
            ),
            last_recovery_failure=(
                envelope.last_recovery_failure
                if envelope.last_recovery_failure is not None
                else None
                if envelope.bootstrap_completed
                else previous.last_recovery_failure
                if previous
                else None
            ),
        )
        self.repository.save_checkpoint(checkpoint)
        return checkpoint


@dataclass
class CollectorHealthMetrics:
    events: Counter[tuple[str, str, str]] = field(default_factory=Counter)
    disconnect_count: int = 0
    reconnect_count: int = 0
    sequence_gaps: int = 0
    snapshot_recoveries: int = 0
    stale_duration_seconds: float = 0
    quarantine_count: int = 0
    experimental_count: int = 0
    production_count: int = 0
    duplicate_count: int = 0
    checkpoint_lag_seconds: float = 0
    production_by_venue: Counter[str] = field(default_factory=Counter)
    experimental_by_venue: Counter[str] = field(default_factory=Counter)
    queue_peak: int = 0
    database_write_count: int = 0
    database_write_latency_total_seconds: float = 0
    database_write_latency_peak_seconds: float = 0
    task_count_start: int = 0
    task_count_end: int = 0
    task_count_peak: int = 0
    rss_start_bytes: int = 0
    rss_end_bytes: int = 0
    rss_peak_bytes: int = 0

    def observe_runtime(self, *, queue_depth: int) -> None:
        rss = _current_rss_bytes()
        task_count = len([task for task in asyncio.all_tasks() if not task.done()])
        if self.rss_start_bytes == 0:
            self.rss_start_bytes = rss
            self.task_count_start = task_count
        self.rss_end_bytes = rss
        self.rss_peak_bytes = max(self.rss_peak_bytes, rss)
        self.task_count_end = task_count
        self.task_count_peak = max(self.task_count_peak, task_count)
        self.queue_peak = max(self.queue_peak, queue_depth)

    def observe_database_write(self, elapsed_seconds: float) -> None:
        self.database_write_count += 1
        self.database_write_latency_total_seconds += elapsed_seconds
        self.database_write_latency_peak_seconds = max(
            self.database_write_latency_peak_seconds, elapsed_seconds
        )

    def report(self) -> dict[str, object]:
        total = self.production_count + self.experimental_count + self.duplicate_count
        average_write_latency = self.database_write_latency_total_seconds / max(
            1, self.database_write_count
        )
        return {
            "events_by_venue_type_instrument": {
                ":".join(key): value for key, value in sorted(self.events.items())
            },
            "disconnect_count": self.disconnect_count,
            "reconnect_count": self.reconnect_count,
            "sequence_gaps": self.sequence_gaps,
            "snapshot_recoveries": self.snapshot_recoveries,
            "stale_duration_seconds": self.stale_duration_seconds,
            "quarantine_count": self.quarantine_count,
            "experimental_count": self.experimental_count,
            "production_count": self.production_count,
            "checkpoint_lag_seconds": self.checkpoint_lag_seconds,
            "duplicate_ratio": self.duplicate_count / max(1, total),
            "queue_peak": self.queue_peak,
            "database_write_latency_average_seconds": average_write_latency,
            "database_write_latency_peak_seconds": self.database_write_latency_peak_seconds,
            "rss_start_bytes": self.rss_start_bytes,
            "rss_end_bytes": self.rss_end_bytes,
            "rss_peak_bytes": self.rss_peak_bytes,
            "task_count_start": self.task_count_start,
            "task_count_end": self.task_count_end,
            "task_count_peak": self.task_count_peak,
            # Retained for compatibility with the prior health artifact schema.
            "memory_usage_bytes": self.rss_end_bytes,
            "task_count": self.task_count_end,
        }


def _current_rss_bytes() -> int:
    try:
        completed = subprocess.run(  # nosec B603
            ("/bin/ps", "-o", "rss=", "-p", str(os.getpid())),
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        return int(completed.stdout.strip()) * 1024
    except (OSError, subprocess.SubprocessError, ValueError):
        maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(maximum if os.uname().sysname == "Darwin" else maximum * 1024)


def _sanitize_endpoint(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    if not parsed.scheme:
        return endpoint.split("?", 1)[0]
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _sanitize_error(value: str) -> str:
    result = re.sub(
        r"(?i)[\"']?(api[-_ ]?key|authorization|secret|signature|token)[\"']?"
        r"\s*[:=]\s*[\"']?[^,;}\n]+",
        r"\1=[REDACTED]",
        value,
    )
    result = re.sub(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", result)
    return result[:500]


class RawPersistenceConsumer:
    def __init__(
        self,
        repository: ResearchRepository,
        capability_gate: ResearchCapabilityGate,
        checkpoint_writer: CheckpointWriter,
        metrics: CollectorHealthMetrics,
        stale_after: timedelta,
    ) -> None:
        self.repository = repository
        self.capability_gate = capability_gate
        self.checkpoint_writer = checkpoint_writer
        self.metrics = metrics
        self.stale_after = stale_after

    async def run(self, queue: asyncio.Queue[CollectedEnvelope | None]) -> None:
        while True:
            envelope = await queue.get()
            try:
                if envelope is None:
                    return
                self.metrics.observe_runtime(queue_depth=queue.qsize())
                started = time.perf_counter()
                try:
                    self.persist(envelope)
                except Exception as exc:
                    self.persist_failure(
                        envelope.stream_identity,
                        envelope.source_endpoint,
                        exc,
                        0,
                    )
                finally:
                    self.metrics.observe_database_write(time.perf_counter() - started)
            finally:
                queue.task_done()

    def persist(self, envelope: CollectedEnvelope) -> None:
        event = self._event(envelope)
        identity = envelope.stream_identity
        checkpoint = self.repository.get_checkpoint(identity.venue, identity.stream_key)
        capability = EVENT_CAPABILITIES.get(envelope.event_type)
        decision = (
            self.capability_gate.decide(
                venue=envelope.venue,
                capability=capability,
                canonical_instrument_id=envelope.canonical_instrument_id,
                now=envelope.received_at,
            )
            if capability
            else CapabilityDecision(CapabilitySupport.LIVE_VERIFIED, "collector-control-plane")
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
        self.metrics.events[(event.venue, event.event_type, event.canonical_instrument_id)] += 1
        if (
            envelope.event_type in BOOK_EVENT_TYPES
            and checkpoint is not None
            and (
                envelope.connection_epoch < checkpoint.connection_epoch
                or (
                    envelope.connection_epoch == checkpoint.connection_epoch
                    and envelope.connection_id is not None
                    and envelope.connection_id != checkpoint.connection_id
                )
            )
        ):
            self._quarantine(event, "order-book event belongs to an old connection epoch")
            return
        invalid = self._invalid_reason(event)
        if not decision.production_eligible:
            if self.repository.add_experimental_event(event, decision.support.value):
                self.metrics.experimental_count += 1
                self.metrics.experimental_by_venue[event.venue] += 1
            gap = self._sequence_gap(checkpoint, envelope)
            state = (
                ReconciliationState.DEGRADED
                if gap
                else envelope.reconciliation_state
                or (
                    ReconciliationState.DEGRADED
                    if envelope.event_type in BOOK_EVENT_TYPES
                    else ReconciliationState.SYNCHRONIZED
                )
            )
            prior_recovery = bool(checkpoint and checkpoint.recovery_required)
            recovery_completed = self._recovery_completed(envelope, state)
            recovery_required = gap or (
                envelope.event_type in BOOK_EVENT_TYPES
                and (
                    state != ReconciliationState.SYNCHRONIZED
                    or (prior_recovery and not recovery_completed)
                )
            )
            if recovery_required and state == ReconciliationState.SYNCHRONIZED:
                state = ReconciliationState.DEGRADED
            if gap:
                self.metrics.sequence_gaps += 1
            self.checkpoint_writer.write(
                envelope=envelope,
                event_id=event.event_id,
                state=state,
                recovery_required=recovery_required,
            )
            self._record_checkpoint_lag(envelope)
            return
        existing = self.repository.get_raw_event(event.event_id)
        if existing is not None:
            if existing.payload_sha256 != event.payload_sha256:
                self._quarantine(event, "event_id payload conflict")
                return
            self.metrics.duplicate_count += 1
            return
        if invalid is not None:
            self._persist_raw(event)
            self._quarantine(event, invalid)
            return
        if envelope.event_type in BOOK_EVENT_TYPES:
            self._persist_book(event, envelope, checkpoint)
            return
        if (
            envelope.event_type == "websocket_disconnect"
            and envelope.logical_stream_event_type == "orderbook"
        ):
            self._persist_raw(event)
            payload = event.payload()
            if not bool(payload.get("client_initiated_close")):
                self.checkpoint_writer.write(
                    envelope=envelope,
                    event_id=event.event_id,
                    state=ReconciliationState.DISCONNECTED,
                    recovery_required=True,
                )
                self.metrics.disconnect_count += 1
                self.metrics.reconnect_count += 1
            return
        if self._sequence_gap(checkpoint, envelope):
            self._persist_raw(event)
            self._quarantine(event, "sequence gap: snapshot recovery required")
            self._record_gap_control(event, envelope)
            self.metrics.sequence_gaps += 1
            self.checkpoint_writer.write(
                envelope=envelope,
                event_id=event.event_id,
                state=ReconciliationState.DEGRADED,
                recovery_required=True,
            )
            return
        self._persist_raw(event)
        state = envelope.reconciliation_state or ReconciliationState.SYNCHRONIZED
        self.checkpoint_writer.write(
            envelope=envelope,
            event_id=event.event_id,
            state=state,
            recovery_required=False,
        )
        self._record_checkpoint_lag(envelope)
        self._record_stale(event)

    def _persist_book(
        self,
        event: RawMarketEvent,
        envelope: CollectedEnvelope,
        checkpoint: CollectionCheckpoint | None,
    ) -> None:
        recovery_required = bool(checkpoint and checkpoint.recovery_required)
        recovered = recovery_required and self._recovery_completed(
            envelope, envelope.reconciliation_state
        )
        if (
            envelope.event_type == "orderbook_delta"
            and not recovered
            and self._sequence_gap(checkpoint, envelope)
        ):
            self._persist_raw(event)
            self._quarantine(event, "order-book gap detected; snapshot recovery required")
            self._record_gap_control(event, envelope)
            self.metrics.sequence_gaps += 1
            self.checkpoint_writer.write(
                envelope=envelope,
                event_id=event.event_id,
                state=ReconciliationState.DEGRADED,
                recovery_required=True,
            )
            return
        if recovery_required and not recovered:
            self._persist_raw(event)
            self._quarantine(event, "order-book recovery has not completed")
            state = envelope.reconciliation_state or ReconciliationState.DEGRADED
            self.checkpoint_writer.write(
                envelope=envelope,
                event_id=event.event_id,
                state=state,
                recovery_required=True,
            )
            return
        if envelope.reconciliation_state != ReconciliationState.SYNCHRONIZED:
            self._persist_raw(event)
            self._quarantine(event, "order-book event is not synchronized")
            self.checkpoint_writer.write(
                envelope=envelope,
                event_id=event.event_id,
                state=envelope.reconciliation_state or ReconciliationState.DEGRADED,
                recovery_required=True,
            )
            return
        self._persist_raw(event)
        self.checkpoint_writer.write(
            envelope=envelope,
            event_id=event.event_id,
            state=ReconciliationState.SYNCHRONIZED,
            recovery_required=False,
        )
        self._record_checkpoint_lag(envelope)
        if recovered:
            self.metrics.snapshot_recoveries += 1
        self._record_stale(event)

    def persist_failure(
        self,
        identity: ResearchStreamIdentity,
        endpoint: str,
        exc: Exception,
        retry_count: int,
    ) -> None:
        self.repository.save_collection_failure(
            CollectionFailureEvent(
                venue=identity.venue,
                stream_key=identity.stream_key,
                instrument=identity.canonical_instrument_id,
                event_type=identity.event_type,
                endpoint=_sanitize_endpoint(endpoint),
                error_type=type(exc).__name__,
                error_message=_sanitize_error(str(exc)),
                occurred_at=datetime.now(UTC),
                retry_count=retry_count,
            )
        )

    def _event(self, envelope: CollectedEnvelope) -> RawMarketEvent:
        normalized = envelope.normalized_payload or envelope.raw_payload
        payload_hash = hashlib.sha256(normalized.encode()).hexdigest()
        sequence = (
            envelope.delta_sequence if envelope.delta_sequence is not None else envelope.sequence
        )
        stable = envelope.stable_event_key or canonical_sha256(
            (
                envelope.venue,
                envelope.venue_symbol,
                envelope.event_type,
                envelope.exchange_timestamp,
                sequence,
                envelope.trade_id,
                payload_hash,
            )
        )
        return RawMarketEvent(
            event_id=f"{envelope.venue}-{stable}",
            venue=envelope.venue,
            canonical_instrument_id=envelope.canonical_instrument_id,
            venue_symbol=envelope.venue_symbol,
            event_type=envelope.event_type,
            exchange_timestamp=envelope.exchange_timestamp,
            received_at=envelope.received_at,
            available_at=envelope.available_at,
            sequence=sequence,
            connection_id=envelope.connection_id,
            reconciliation_state=envelope.reconciliation_state,
            payload_sha256=payload_hash,
            raw_payload=normalized,
            normalizer_version="research-collector-r2.1-v1",
            capability_verification_run_id="pending",
            created_at=envelope.received_at,
            channel=envelope.channel,
            snapshot_sequence=envelope.snapshot_sequence,
            delta_sequence=envelope.delta_sequence,
            connection_epoch=envelope.connection_epoch,
        )

    @staticmethod
    def _invalid_reason(event: RawMarketEvent) -> str | None:
        try:
            event.payload()
        except (ValueError, json.JSONDecodeError) as exc:
            return f"normalization failure: {exc}"
        if event.available_at < event.received_at:
            return "available_at precedes received_at"
        if event.exchange_timestamp is not None and event.exchange_timestamp > event.received_at:
            return "abnormal future exchange timestamp"
        return None

    @staticmethod
    def _sequence_gap(checkpoint: CollectionCheckpoint | None, envelope: CollectedEnvelope) -> bool:
        if envelope.event_type != "orderbook_delta" and envelope.stream_semantics is not None:
            return False
        if envelope.stream_semantics in {
            OrderBookStreamSemantics.SNAPSHOT_ONLY,
            OrderBookStreamSemantics.LIMITED_DEPTH_SNAPSHOT,
        }:
            return False
        current = (
            envelope.delta_sequence if envelope.delta_sequence is not None else envelope.sequence
        )
        if envelope.previous_delta_sequence is not None:
            previous = checkpoint.delta_sequence if checkpoint else None
            if previous is None and checkpoint is not None:
                previous = checkpoint.snapshot_sequence
            return previous is not None and envelope.previous_delta_sequence != previous
        return (
            checkpoint is not None
            and checkpoint.last_sequence is not None
            and current is not None
            and current != checkpoint.last_sequence + 1
        )

    @staticmethod
    def _recovery_completed(
        envelope: CollectedEnvelope,
        state: ReconciliationState | None,
    ) -> bool:
        if (
            not envelope.recovery_completed
            or state != ReconciliationState.SYNCHRONIZED
            or (envelope.stream_semantics is not None and not envelope.bootstrap_completed)
        ):
            return False
        if envelope.stream_semantics in {
            OrderBookStreamSemantics.SNAPSHOT_ONLY,
            OrderBookStreamSemantics.LIMITED_DEPTH_SNAPSHOT,
        }:
            return envelope.event_type == "orderbook_snapshot"
        return envelope.snapshot_sequence is not None and (
            envelope.event_type == "orderbook_snapshot"
            or (
                envelope.delta_sequence is not None
                and envelope.delta_sequence >= envelope.snapshot_sequence
            )
        )

    def _persist_raw(self, event: RawMarketEvent) -> None:
        if self.repository.add_raw_event(event):
            self.metrics.production_count += 1
            self.metrics.production_by_venue[event.venue] += 1

    def _quarantine(self, event: RawMarketEvent, reason: str) -> None:
        self.repository.quarantine(event, reason, event.received_at)
        self.metrics.quarantine_count += 1

    def _record_stale(self, event: RawMarketEvent) -> None:
        age = datetime.now(UTC) - event.available_at
        if age > self.stale_after:
            self.metrics.stale_duration_seconds += age.total_seconds()

    def _record_checkpoint_lag(self, envelope: CollectedEnvelope) -> None:
        self.metrics.checkpoint_lag_seconds = max(
            0.0, (datetime.now(UTC) - envelope.available_at).total_seconds()
        )

    def _record_gap_control(self, event: RawMarketEvent, envelope: CollectedEnvelope) -> None:
        payload = json.dumps(
            {
                "affected_event_id": event.event_id,
                "delta_sequence": envelope.delta_sequence,
                "sequence": envelope.sequence,
                "stream_key": envelope.stream_identity.stream_key,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(payload.encode()).hexdigest()
        self._persist_raw(
            RawMarketEvent(
                event_id=f"{event.venue}-sequence-gap-{digest}",
                venue=event.venue,
                canonical_instrument_id=event.canonical_instrument_id,
                venue_symbol=event.venue_symbol,
                event_type="sequence_gap",
                exchange_timestamp=event.exchange_timestamp,
                received_at=event.received_at,
                available_at=event.available_at,
                sequence=event.sequence,
                connection_id=event.connection_id,
                reconciliation_state=ReconciliationState.GAP_DETECTED,
                payload_sha256=digest,
                raw_payload=payload,
                normalizer_version="research-collector-r2.1-v1",
                capability_verification_run_id="collector-control-plane",
                created_at=event.created_at,
                channel="control",
                snapshot_sequence=event.snapshot_sequence,
                delta_sequence=event.delta_sequence,
                connection_epoch=event.connection_epoch,
            )
        )


class RestPollingWorker:
    def __init__(
        self,
        source: CollectorSource,
        identities: tuple[ResearchStreamIdentity, ...],
        repository: ResearchRepository,
        output: asyncio.Queue[CollectedEnvelope | None],
        consumer: RawPersistenceConsumer,
        shutdown: asyncio.Event,
        interval_seconds: float,
        maximum_cycles: int | None,
        first_cycle_complete: asyncio.Event,
    ) -> None:
        self.source = source
        self.identities = identities
        self.repository = repository
        self.output = output
        self.consumer = consumer
        self.shutdown = shutdown
        self.interval_seconds = interval_seconds
        self.maximum_cycles = maximum_cycles
        self.first_cycle_complete = first_cycle_complete

    async def run(self) -> None:
        cycles = 0
        while not self.shutdown.is_set():
            rule_loader = getattr(self.source, "instrument_rules", None)
            if rule_loader is not None:
                try:
                    instruments = tuple(
                        dict.fromkeys(item.canonical_instrument_id for item in self.identities)
                    )
                    for rule in await rule_loader(instruments):
                        self.repository.save_instrument_rule(rule)
                except Exception as exc:
                    identity = (
                        self.identities[0]
                        if self.identities
                        else ResearchStreamIdentity(
                            self.source.venue, "SYSTEM", "SYSTEM", "instrument_rules", "rest"
                        )
                    )
                    self.consumer.persist_failure(identity, "fetch_markets", exc, cycles)
            for identity in self.identities:
                checkpoint = self.repository.get_checkpoint(identity.venue, identity.stream_key)
                try:
                    async for event in self.source.rest_events(identity, checkpoint):
                        await self.output.put(event)
                except Exception as exc:
                    self.consumer.persist_failure(identity, identity.channel, exc, cycles)
            cycles += 1
            self.first_cycle_complete.set()
            if self.maximum_cycles is not None and cycles >= self.maximum_cycles:
                return
            with suppress(TimeoutError):
                await asyncio.wait_for(self.shutdown.wait(), timeout=self.interval_seconds)


class WebSocketStreamingWorker:
    def __init__(
        self,
        source: CollectorSource,
        identity: ResearchStreamIdentity,
        repository: ResearchRepository,
        output: asyncio.Queue[CollectedEnvelope | None],
        consumer: RawPersistenceConsumer,
        shutdown: asyncio.Event,
        start_after: asyncio.Event,
        maximum_messages: int | None = None,
    ) -> None:
        self.source = source
        self.identity = identity
        self.repository = repository
        self.output = output
        self.consumer = consumer
        self.shutdown = shutdown
        self.start_after = start_after
        self.maximum_messages = maximum_messages

    async def run(self) -> None:
        await self.start_after.wait()
        retry = 0
        received = 0
        while not self.shutdown.is_set():
            checkpoint = self.repository.get_checkpoint(
                self.identity.venue, self.identity.stream_key
            )
            try:
                connected_reported = False
                async for event in self.source.websocket_events(
                    self.identity, checkpoint, self.shutdown
                ):
                    await self.output.put(event)
                    if not connected_reported:
                        await self.output.put(
                            self._control_event("venue_health", "connected", retry)
                        )
                        connected_reported = True
                    received += 1
                    if self.maximum_messages is not None and received >= self.maximum_messages:
                        return
                if self.shutdown.is_set() or self.maximum_messages is not None:
                    return
                retry += 1
            except Exception as exc:
                retry += 1
                self.consumer.metrics.disconnect_count += 1
                self.consumer.persist_failure(self.identity, self.identity.channel, exc, retry)
                control_type = (
                    "stale_stream" if isinstance(exc, TimeoutError) else "websocket_disconnect"
                )
                await self.output.put(self._control_event(control_type, type(exc).__name__, retry))
            self.consumer.metrics.reconnect_count += 1
            await asyncio.sleep(min(1.0, retry / 10))

    def _control_event(self, event_type: str, status: str, retry_count: int) -> CollectedEnvelope:
        now = datetime.now(UTC)
        payload = json.dumps(
            {
                "channel": self.identity.channel,
                "retry_count": retry_count,
                "status": status,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return CollectedEnvelope(
            venue=self.identity.venue,
            canonical_instrument_id=self.identity.canonical_instrument_id,
            venue_symbol=self.identity.venue_symbol,
            event_type=event_type,
            channel="control",
            source_endpoint=f"websocket:{self.identity.channel}",
            raw_payload=payload,
            normalized_payload=payload,
            exchange_timestamp=None,
            received_at=now,
            available_at=now,
            stable_event_key=f"{event_type}-{self.identity.stream_key}-{now.isoformat()}",
        )


@dataclass(frozen=True)
class CollectorResult:
    production_counts: dict[str, int]
    experimental_counts: dict[str, int]
    quarantine_count: int
    health: dict[str, object]


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
        self.metrics = CollectorHealthMetrics()

    async def run(self) -> None:
        if not self.collection_enabled:
            raise RuntimeError("research collection requires collection_enabled=true")
        queue: asyncio.Queue[CollectedEnvelope | None] = asyncio.Queue(maxsize=10_000)
        checkpoint_writer = CheckpointWriter(self.repository)
        consumer = RawPersistenceConsumer(
            self.repository,
            self.capability_gate,
            checkpoint_writer,
            self.metrics,
            self.stale_after,
        )
        resource_monitor_shutdown = asyncio.Event()

        async def monitor_resources() -> None:
            while not resource_monitor_shutdown.is_set():
                self.metrics.observe_runtime(queue_depth=queue.qsize())
                with suppress(TimeoutError):
                    await asyncio.wait_for(resource_monitor_shutdown.wait(), timeout=1)

        resource_monitor = asyncio.create_task(
            monitor_resources(), name="collector-resource-monitor"
        )
        consumer_task = asyncio.create_task(consumer.run(queue), name="raw-persistence-consumer")
        workers: list[asyncio.Task[None]] = []
        try:
            for source in self.sources:
                await queue.put(self._health_envelope(source.venue))
                rest_identities, websocket_identities = self._identities(source.venue)
                initial_backfill_complete = asyncio.Event()
                if hasattr(source, "rest_events"):
                    if rest_identities:
                        rest = RestPollingWorker(
                            source,
                            rest_identities,
                            self.repository,
                            queue,
                            consumer,
                            self._shutdown,
                            self.poll_interval_seconds,
                            self.maximum_cycles,
                            initial_backfill_complete,
                        )
                        workers.append(asyncio.create_task(rest.run(), name=f"rest:{source.venue}"))
                    else:
                        initial_backfill_complete.set()
                    for identity in websocket_identities:
                        worker = WebSocketStreamingWorker(
                            source,
                            identity,
                            self.repository,
                            queue,
                            consumer,
                            self._shutdown,
                            initial_backfill_complete,
                            maximum_messages=self.maximum_cycles,
                        )
                        workers.append(
                            asyncio.create_task(worker.run(), name=f"ws:{identity.stream_key}")
                        )
                else:
                    workers.append(
                        asyncio.create_task(
                            self._legacy_source(source, queue), name=f"legacy:{source.venue}"
                        )
                    )
            await asyncio.gather(*workers)
            await queue.join()
        finally:
            self._shutdown.set()
            resource_monitor_shutdown.set()
            await resource_monitor
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await queue.put(None)
            await consumer_task
            await asyncio.gather(
                *(source.close() for source in self.sources), return_exceptions=True
            )

    @staticmethod
    def _health_envelope(venue: str) -> CollectedEnvelope:
        now = datetime.now(UTC)
        payload = json.dumps(
            {"status": "collector_started", "venue": venue},
            sort_keys=True,
            separators=(",", ":"),
        )
        return CollectedEnvelope(
            venue=venue,
            canonical_instrument_id="SYSTEM",
            venue_symbol="SYSTEM",
            event_type="venue_health",
            channel="control",
            source_endpoint="collector:health",
            raw_payload=payload,
            normalized_payload=payload,
            exchange_timestamp=None,
            received_at=now,
            available_at=now,
            stable_event_key=f"collector-started-{now.isoformat()}",
        )

    async def _legacy_source(
        self,
        source: CollectorSource,
        queue: asyncio.Queue[CollectedEnvelope | None],
    ) -> None:
        instrument = self.instruments[0]
        event_type = self.event_types[0]
        identity = ResearchStreamIdentity(
            source.venue,
            instrument,
            canonical_symbol(source.venue, instrument),
            event_type,
            "unknown",
        )
        checkpoint = self.repository.get_checkpoint(source.venue, identity.stream_key)
        events = source.events  # type: ignore[attr-defined]
        async for event in events(
            instruments=self.instruments,
            event_types=self.event_types,
            checkpoint=checkpoint,
        ):
            await queue.put(event)

    def _identities(
        self, venue: str
    ) -> tuple[tuple[ResearchStreamIdentity, ...], tuple[ResearchStreamIdentity, ...]]:
        rest: list[ResearchStreamIdentity] = []
        websocket: list[ResearchStreamIdentity] = []
        for instrument in self.instruments:
            symbol = canonical_symbol(venue, instrument)
            for event_type in self.event_types:
                if event_type in REST_EVENT_TYPES:
                    rest.append(
                        ResearchStreamIdentity(venue, instrument, symbol, event_type, "rest")
                    )
                elif event_type in BOOK_EVENT_TYPES:
                    identity = ResearchStreamIdentity(
                        venue, instrument, symbol, "orderbook", "orderbook"
                    )
                    if identity not in websocket:
                        websocket.append(identity)
                elif event_type in WEBSOCKET_EVENT_TYPES:
                    channel = "trades" if event_type == "trade" else "ticker"
                    websocket.append(
                        ResearchStreamIdentity(venue, instrument, symbol, event_type, channel)
                    )
        return tuple(rest), tuple(websocket)

    def shutdown(self) -> None:
        self._shutdown.set()

    @property
    def result(self) -> CollectorResult:
        return CollectorResult(
            dict(self.metrics.production_by_venue),
            dict(self.metrics.experimental_by_venue),
            self.metrics.quarantine_count,
            self.metrics.report(),
        )


@dataclass(frozen=True)
class SnapshotEligibilityPolicy:
    required_event_types: tuple[str, ...] = ()
    required_venues: tuple[str, ...] = ()
    minimum_production_events: int = 1
    maximum_gap_ratio: Decimal = Decimal("0.01")
    maximum_stale_ratio: Decimal = Decimal("0.05")
    require_complete_instrument_rules: bool = False
    minimum_venue_event_coverage_ratio: Decimal = Decimal("0")
    minimum_history_windows_per_venue_event: int = 0


class DataSnapshotService:
    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def finalize(
        self,
        *,
        cutoff_at: datetime,
        snapshot_id: str | None = None,
        finalized_at: datetime | None = None,
        eligibility_policy: SnapshotEligibilityPolicy | None = None,
        included_event_types: tuple[str, ...] | None = None,
        included_venues: tuple[str, ...] | None = None,
    ) -> DataSnapshotManifest:
        cutoff = utc(cutoff_at, "cutoff_at")
        finalized = utc(finalized_at or datetime.now(UTC), "finalized_at")
        policy = eligibility_policy or SnapshotEligibilityPolicy()
        quarantined, reasons = self.repository.quarantine_summary(cutoff)
        quarantined_ids = set(quarantined)
        source = tuple(
            event
            for event in sorted(
                self.repository.raw_events(), key=lambda item: (item.available_at, item.event_id)
            )
            if event.available_at <= cutoff
            and (included_event_types is None or event.event_type in included_event_types)
            and (included_venues is None or event.venue in included_venues)
        )
        unsynchronized_books = {
            event.event_id
            for event in source
            if event.event_type in BOOK_EVENT_TYPES
            and event.reconciliation_state != ReconciliationState.SYNCHRONIZED
        }
        events = tuple(
            event
            for event in source
            if event.event_id not in quarantined_ids and event.event_id not in unsynchronized_books
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
        eligibility_reasons = self._eligibility_reasons(
            events, source, cutoff, policy, unsynchronized_books
        )
        eligibility_status = (
            "FINALIZED_RESEARCH_ELIGIBLE" if not eligibility_reasons else "FINALIZED_NOT_ELIGIBLE"
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
            "eligibility_status": eligibility_status,
            "eligibility_reasons": eligibility_reasons,
        }
        manifest = DataSnapshotManifest(
            **unsigned,  # type: ignore[arg-type]
            manifest_sha256=canonical_sha256(unsigned),
        )
        self.repository.finalize_snapshot(manifest)
        return manifest

    def _eligibility_reasons(
        self,
        events: tuple[RawMarketEvent, ...],
        source: tuple[RawMarketEvent, ...],
        cutoff: datetime,
        policy: SnapshotEligibilityPolicy,
        unsynchronized_books: set[str],
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        market = [item for item in events if item.event_type not in CONTROL_EVENT_TYPES]
        if len(market) < policy.minimum_production_events:
            reasons.append("production market event count below threshold")
        present_types = {item.event_type for item in market}
        missing_types = set(policy.required_event_types) - present_types
        if missing_types:
            reasons.append(f"missing required event types: {','.join(sorted(missing_types))}")
        present_venues = {item.venue for item in market}
        missing_venues = set(policy.required_venues) - present_venues
        if missing_venues:
            reasons.append(f"missing required venues: {','.join(sorted(missing_venues))}")
        required_pairs = tuple(
            (venue, event_type)
            for venue in policy.required_venues
            for event_type in policy.required_event_types
        )
        if required_pairs:
            counts = Counter((item.venue, item.event_type) for item in market)
            covered = sum(
                counts[pair] >= policy.minimum_history_windows_per_venue_event
                for pair in required_pairs
            )
            coverage = Decimal(covered) / Decimal(len(required_pairs))
            if coverage < policy.minimum_venue_event_coverage_ratio:
                reasons.append("venue/event capability coverage ratio below threshold")
            incomplete = tuple(
                f"{venue}:{event_type}"
                for venue, event_type in required_pairs
                if counts[(venue, event_type)] < policy.minimum_history_windows_per_venue_event
            )
            if incomplete:
                reasons.append(f"insufficient history windows: {','.join(incomplete)}")
        if unsynchronized_books:
            reasons.append("unsynchronized order-book events present")
        gaps = sum(item.event_type == "sequence_gap" for item in source)
        stale = sum(item.event_type == "stale_stream" for item in source)
        denominator = Decimal(max(1, len(market)))
        if Decimal(gaps) / denominator > policy.maximum_gap_ratio:
            reasons.append("sequence-gap ratio above threshold")
        if Decimal(stale) / denominator > policy.maximum_stale_ratio:
            reasons.append("stale ratio above threshold")
        if policy.require_complete_instrument_rules:
            rules = self.repository.instrument_rules_at(cutoff)
            complete = {
                (item.venue, item.canonical_instrument_id)
                for item in rules
                if _rule_is_complete(item)
            }
            required = {(item.venue, item.canonical_instrument_id) for item in market}
            if not required <= complete:
                reasons.append("instrument rules are incomplete or UNKNOWN")
        return tuple(reasons)

    def verify(self, snapshot_id: str) -> DataSnapshotManifest:
        manifest = self.repository.snapshot_manifest(snapshot_id)
        if manifest is None:
            raise ValueError("snapshot does not exist or is not finalized")
        events = self.repository.snapshot_events(snapshot_id)
        actual = tuple(
            (ordinal, event.event_id, event.payload_sha256) for ordinal, event in enumerate(events)
        )
        if actual != manifest.events:
            raise ValueError("snapshot membership does not reproduce exactly")
        if canonical_sha256(actual) != manifest.content_sha256:
            raise ValueError("snapshot content hash mismatch")
        unsigned = {
            key: value for key, value in manifest.__dict__.items() if key != "manifest_sha256"
        }
        if canonical_sha256(unsigned) != manifest.manifest_sha256:
            raise ValueError("snapshot manifest hash mismatch")
        return manifest


def _rule_is_complete(rule: InstrumentRuleSnapshot) -> bool:
    required_names = (
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
    return (
        rule.fee_tier != FeeTierKind.UNKNOWN
        and rule.verification_status != RuleVerificationStatus.UNKNOWN
        and all(getattr(rule, name) is not None for name in required_names)
        and all(
            (evidence := rule.field_evidence.get(name)) is not None
            and evidence.get("verification_status") != RuleVerificationStatus.UNKNOWN.value
            and bool(evidence.get("source_endpoint"))
            and len(evidence.get("source_payload_sha256") or "") == 64
            and bool(evidence.get("retrieved_at"))
            for name in required_names
        )
    )


class ResearchRetentionService:
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


def write_collector_health_report(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
