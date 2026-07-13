from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from time import monotonic, monotonic_ns
from typing import Any
from uuid import UUID, uuid4

import websockets


class StreamClassification(StrEnum):
    SNAPSHOT_DELTA = "snapshot_delta"
    LIMITED_DEPTH_SNAPSHOT_STREAM = "limited_depth_snapshot_stream"
    EVENTS = "events"


class ConnectionState(StrEnum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECOVERING = "recovering"
    DEGRADED = "degraded"
    CLOSED = "closed"


class ReconciliationState(StrEnum):
    DISCONNECTED = "disconnected"
    WAITING_FOR_SNAPSHOT = "waiting_for_snapshot"
    BUFFERING_DELTAS = "buffering_deltas"
    APPLYING_SNAPSHOT = "applying_snapshot"
    REPLAYING_DELTAS = "replaying_deltas"
    SYNCHRONIZED = "synchronized"
    DEGRADED = "degraded"


@dataclass
class ConnectionReconciliationContext:
    connection_id: UUID
    state: ReconciliationState
    previous_sequence: int | None
    snapshot_sequence: int | None
    buffered_deltas: list[tuple[int, Any]]
    payload_hashes: set[str]
    payload_hash_order: deque[str]


@dataclass(frozen=True)
class RawVenueMessage:
    venue: str
    connection_id: UUID
    subscription_id: str
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    monotonic_received_ns: int
    venue_sequence: int | None
    local_sequence: int
    is_snapshot: bool
    is_delta: bool
    payload: bytes
    payload_sha256: str
    normalized_payload: Any | None = None


@dataclass(frozen=True)
class ConnectionStateEvent:
    venue: str
    state: ConnectionState
    occurred_at: datetime
    reason: str | None = None


class ResilientWebSocketSession:
    """A fail-closed public-stream session with bounded reconnects and recovery."""

    def __init__(
        self,
        *,
        venue: str,
        url: str,
        subscription_id: str,
        subscribe: dict[str, object] | None,
        acknowledgement: Callable[[Any], bool] | None,
        normalize: Callable[[Any], Any] = lambda value: value,
        sequence: Callable[[Any], int | None] = lambda value: None,
        exchange_timestamp: Callable[[Any], datetime | None] = lambda value: None,
        snapshot: Callable[[Any], bool] = lambda value: False,
        delta: Callable[[Any], bool] = lambda value: False,
        heartbeat: Callable[[Any], bool] = lambda value: value in {"pong", "ping"},
        rest_snapshot: Callable[[], Awaitable[Any]] | None = None,
        snapshot_applier: Callable[[Any], int] | None = None,
        delta_applier: Callable[[Any], None] | None = None,
        maximum_buffered_deltas: int = 10_000,
        duplicate_cache_size: int = 4096,
        raw_message_sink: Callable[[RawVenueMessage], Awaitable[None]] | None = None,
        classification: StreamClassification = StreamClassification.EVENTS,
        stale_timeout: float = 30.0,
        acknowledgement_timeout: float = 10.0,
        maximum_reconnects_per_minute: int = 10,
        backoff_cap: float = 30.0,
    ) -> None:
        if classification == StreamClassification.SNAPSHOT_DELTA and any(
            callback is None for callback in (rest_snapshot, snapshot_applier, delta_applier)
        ):
            raise ValueError("snapshot/delta streams require loader and snapshot/delta appliers")
        self.venue, self.url, self.subscription_id = venue, url, subscription_id
        self.subscribe, self.acknowledgement = subscribe, acknowledgement
        self.normalize, self.sequence = normalize, sequence
        self.exchange_timestamp = exchange_timestamp
        self.snapshot, self.delta, self.heartbeat = snapshot, delta, heartbeat
        self.rest_snapshot, self.classification = rest_snapshot, classification
        self.snapshot_applier, self.delta_applier = snapshot_applier, delta_applier
        self.maximum_buffered_deltas = maximum_buffered_deltas
        self.duplicate_cache_size = duplicate_cache_size
        self.raw_message_sink = raw_message_sink
        self.stale_timeout, self.acknowledgement_timeout = stale_timeout, acknowledgement_timeout
        self.maximum_reconnects_per_minute = maximum_reconnects_per_minute
        self.backoff_cap = backoff_cap
        self.state = ConnectionState.CLOSED
        self.state_events: list[ConnectionStateEvent] = []
        self._closing = False
        self._reconnects: deque[float] = deque()
        self._local_sequence = 0
        self.reconciliation_state = ReconciliationState.DISCONNECTED
        self.connection_context: ConnectionReconciliationContext | None = None

    async def close(self) -> None:
        self._closing = True
        self._set_state(ConnectionState.CLOSED, "graceful shutdown")

    def _set_state(self, state: ConnectionState, reason: str | None = None) -> None:
        self.state = state
        self.state_events.append(ConnectionStateEvent(self.venue, state, datetime.now(UTC), reason))

    async def messages(self) -> AsyncIterator[RawVenueMessage]:
        attempt = 0
        while not self._closing:
            now = monotonic()
            while self._reconnects and now - self._reconnects[0] >= 60:
                self._reconnects.popleft()
            if len(self._reconnects) >= self.maximum_reconnects_per_minute:
                self._set_state(ConnectionState.DEGRADED, "maximum reconnect rate exceeded")
                await asyncio.sleep(min(self.stale_timeout, 60.0))
                continue
            self._reconnects.append(now)
            self._set_state(ConnectionState.CONNECTING)
            connection_id = uuid4()
            context = ConnectionReconciliationContext(
                connection_id=connection_id,
                state=ReconciliationState.DISCONNECTED,
                previous_sequence=None,
                snapshot_sequence=None,
                buffered_deltas=[],
                payload_hashes=set(),
                payload_hash_order=deque(),
            )
            self.connection_context = context
            self._set_reconciliation(context, ReconciliationState.WAITING_FOR_SNAPSHOT)
            try:
                async with websockets.connect(
                    self.url, open_timeout=10, ping_interval=20
                ) as socket:
                    if self.subscribe is not None:
                        await socket.send(json.dumps(self.subscribe))
                        raw_ack = await asyncio.wait_for(
                            socket.recv(), self.acknowledgement_timeout
                        )
                        ack = self._decode(raw_ack)
                        if self.acknowledgement is None or not self.acknowledgement(ack):
                            raise RuntimeError("subscription acknowledgement validation failed")
                    self._set_state(ConnectionState.CONNECTED)
                    message_queue: asyncio.Queue[bytes | str | BaseException] = asyncio.Queue()
                    reader_task = asyncio.create_task(self._socket_reader(socket, message_queue))
                    if self.classification == StreamClassification.SNAPSHOT_DELTA:
                        await self._bootstrap(message_queue, context)
                    else:
                        self._set_reconciliation(context, ReconciliationState.SYNCHRONIZED)
                    attempt = 0
                    while not self._closing:
                        raw = await self._next_queued(message_queue)
                        decoded = self._decode(raw)
                        if self.heartbeat(decoded):
                            continue
                        payload = raw if isinstance(raw, bytes) else raw.encode()
                        digest = hashlib.sha256(payload).hexdigest()
                        if self._seen_payload(context, digest):
                            continue
                        current = self.sequence(decoded)
                        if current is not None and context.previous_sequence is not None:
                            if current <= context.previous_sequence:
                                self._set_state(ConnectionState.DEGRADED, "out-of-order sequence")
                                continue
                            if current != context.previous_sequence + 1:
                                await self._recover(context, current, decoded)
                                continue
                        if self.classification == StreamClassification.SNAPSHOT_DELTA:
                            if self.delta(decoded):
                                if context.state != ReconciliationState.SYNCHRONIZED:
                                    self._buffer_delta(context, current, decoded)
                                    continue
                                if self.delta_applier is None:
                                    raise RuntimeError("delta applier unavailable")
                                self.delta_applier(decoded)
                            elif self.snapshot(decoded):
                                continue
                        context.previous_sequence = current
                        self._local_sequence += 1
                        received = datetime.now(UTC)
                        message = RawVenueMessage(
                            venue=self.venue,
                            connection_id=connection_id,
                            subscription_id=self.subscription_id,
                            exchange_timestamp=self.exchange_timestamp(decoded),
                            received_at=received,
                            available_at=datetime.now(UTC),
                            monotonic_received_ns=monotonic_ns(),
                            venue_sequence=current,
                            local_sequence=self._local_sequence,
                            is_snapshot=self.snapshot(decoded),
                            is_delta=self.delta(decoded)
                            and self.classification == StreamClassification.SNAPSHOT_DELTA,
                            payload=payload,
                            payload_sha256=digest,
                            normalized_payload=self.normalize(decoded),
                        )
                        if self.raw_message_sink is not None:
                            await self.raw_message_sink(message)
                        yield message
                    reader_task.cancel()
                    await asyncio.gather(reader_task, return_exceptions=True)
            except asyncio.CancelledError:
                await self.close()
                raise
            except (
                OSError,
                TimeoutError,
                RuntimeError,
                ValueError,
                websockets.WebSocketException,
            ) as exc:
                self._set_state(ConnectionState.DEGRADED, str(exc))
                attempt += 1
                await asyncio.sleep(
                    secrets.SystemRandom().uniform(0, min(self.backoff_cap, 2 ** min(attempt, 10)))
                )
        self._set_state(ConnectionState.CLOSED)

    async def _socket_messages(self, socket: Any) -> AsyncIterator[bytes | str]:
        while not self._closing:
            yield await asyncio.wait_for(socket.recv(), timeout=self.stale_timeout)

    async def _socket_reader(
        self, socket: Any, queue: asyncio.Queue[bytes | str | BaseException]
    ) -> None:
        try:
            while not self._closing:
                await queue.put(await asyncio.wait_for(socket.recv(), timeout=self.stale_timeout))
        except BaseException as exc:
            await queue.put(exc)

    @staticmethod
    async def _next_queued(queue: asyncio.Queue[bytes | str | BaseException]) -> bytes | str:
        item = await queue.get()
        if isinstance(item, BaseException):
            raise item
        return item

    def _seen_payload(self, context: ConnectionReconciliationContext, digest: str) -> bool:
        if digest in context.payload_hashes:
            return True
        if len(context.payload_hash_order) >= self.duplicate_cache_size:
            expired = context.payload_hash_order.popleft()
            context.payload_hashes.remove(expired)
        context.payload_hash_order.append(digest)
        context.payload_hashes.add(digest)
        return False

    def _set_reconciliation(
        self, context: ConnectionReconciliationContext, state: ReconciliationState
    ) -> None:
        context.state = state
        self.reconciliation_state = state

    def _buffer_delta(
        self, context: ConnectionReconciliationContext, sequence: int | None, decoded: Any
    ) -> None:
        if sequence is None:
            raise RuntimeError("delta sequence is required")
        if len(context.buffered_deltas) >= self.maximum_buffered_deltas:
            self._set_reconciliation(context, ReconciliationState.DEGRADED)
            raise RuntimeError("delta recovery buffer overflow")
        self._set_reconciliation(context, ReconciliationState.BUFFERING_DELTAS)
        context.buffered_deltas.append((sequence, decoded))

    async def _bootstrap(
        self,
        queue: asyncio.Queue[bytes | str | BaseException],
        context: ConnectionReconciliationContext,
    ) -> None:
        """Start REST bootstrap after ACK while continuing to buffer socket deltas."""
        if self.rest_snapshot is None:
            raise RuntimeError("REST snapshot recovery unavailable")
        self._set_reconciliation(context, ReconciliationState.BUFFERING_DELTAS)
        snapshot = await self.rest_snapshot()
        await asyncio.sleep(0)
        while not queue.empty():
            raw = await self._next_queued(queue)
            decoded = self._decode(raw)
            if self.heartbeat(decoded):
                continue
            if not self.delta(decoded):
                raise RuntimeError("non-delta message received during REST bootstrap")
            payload = raw if isinstance(raw, bytes) else raw.encode()
            digest = hashlib.sha256(payload).hexdigest()
            if not self._seen_payload(context, digest):
                self._buffer_delta(context, self.sequence(decoded), decoded)
        await self._apply_snapshot_and_replay(context, snapshot)

    async def _recover(
        self, context: ConnectionReconciliationContext, sequence: int, decoded: Any
    ) -> None:
        self._set_state(ConnectionState.RECOVERING, "sequence gap")
        self._buffer_delta(context, sequence, decoded)
        if (
            self.rest_snapshot is None
            or self.snapshot_applier is None
            or self.delta_applier is None
        ):
            self._set_state(ConnectionState.DEGRADED, "sequence gap without snapshot semantics")
            raise RuntimeError("REST snapshot recovery unavailable")
        self._set_reconciliation(context, ReconciliationState.WAITING_FOR_SNAPSHOT)
        snapshot = await self.rest_snapshot()
        await self._apply_snapshot_and_replay(context, snapshot)
        self._set_state(
            ConnectionState.CONNECTED,
            f"REST snapshot recovered for {context.connection_id}",
        )

    async def _apply_snapshot_and_replay(
        self, context: ConnectionReconciliationContext, snapshot: Any
    ) -> None:
        if self.snapshot_applier is None or self.delta_applier is None:
            raise RuntimeError("REST snapshot recovery unavailable")
        self._set_reconciliation(context, ReconciliationState.APPLYING_SNAPSHOT)
        snapshot_sequence = self.snapshot_applier(snapshot)
        context.snapshot_sequence = snapshot_sequence
        replay = sorted(
            (item for item in context.buffered_deltas if item[0] > snapshot_sequence),
            key=lambda item: item[0],
        )
        self._set_reconciliation(context, ReconciliationState.REPLAYING_DELTAS)
        expected = snapshot_sequence + 1
        for current, delta in replay:
            if current != expected:
                self._set_reconciliation(context, ReconciliationState.DEGRADED)
                raise RuntimeError("replayed delta sequence is not continuous")
            self.delta_applier(delta)
            expected += 1
        context.previous_sequence = expected - 1
        context.buffered_deltas.clear()
        self._set_reconciliation(context, ReconciliationState.SYNCHRONIZED)

    @staticmethod
    def _decode(raw: bytes | str) -> Any:
        if isinstance(raw, bytes):
            raw = raw.decode()
        if raw in {"pong", "ping"}:
            return raw
        return json.loads(raw)
