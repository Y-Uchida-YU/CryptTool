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
        self.raw_message_sink = raw_message_sink
        self.stale_timeout, self.acknowledgement_timeout = stale_timeout, acknowledgement_timeout
        self.maximum_reconnects_per_minute = maximum_reconnects_per_minute
        self.backoff_cap = backoff_cap
        self.state = ConnectionState.CLOSED
        self.state_events: list[ConnectionStateEvent] = []
        self._closing = False
        self._recent_hashes: deque[str] = deque(maxlen=4096)
        self._reconnects: deque[float] = deque()
        self._local_sequence = 0
        self.reconciliation_state = ReconciliationState.DISCONNECTED
        self._delta_buffer: list[tuple[int, Any]] = []

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
            previous_sequence: int | None = None
            self.reconciliation_state = ReconciliationState.WAITING_FOR_SNAPSHOT
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
                    if self.classification != StreamClassification.SNAPSHOT_DELTA:
                        self.reconciliation_state = ReconciliationState.SYNCHRONIZED
                    attempt = 0
                    async for raw in self._socket_messages(socket):
                        decoded = self._decode(raw)
                        if self.heartbeat(decoded):
                            continue
                        payload = raw if isinstance(raw, bytes) else raw.encode()
                        digest = hashlib.sha256(payload).hexdigest()
                        if digest in self._recent_hashes:
                            continue
                        self._recent_hashes.append(digest)
                        current = self.sequence(decoded)
                        if current is not None and previous_sequence is not None:
                            if current <= previous_sequence:
                                self._set_state(ConnectionState.DEGRADED, "out-of-order sequence")
                                continue
                            if current != previous_sequence + 1:
                                await self._recover(connection_id, current, decoded)
                                previous_sequence = current
                                continue
                        if self.classification == StreamClassification.SNAPSHOT_DELTA:
                            if self.snapshot(decoded):
                                if self.snapshot_applier is None:
                                    raise RuntimeError("snapshot applier unavailable")
                                previous_sequence = self.snapshot_applier(decoded)
                                self.reconciliation_state = ReconciliationState.SYNCHRONIZED
                            elif self.delta(decoded):
                                if self.reconciliation_state != ReconciliationState.SYNCHRONIZED:
                                    self._buffer_delta(current, decoded)
                                    continue
                                if self.delta_applier is None:
                                    raise RuntimeError("delta applier unavailable")
                                self.delta_applier(decoded)
                        previous_sequence = current
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

    def _buffer_delta(self, sequence: int | None, decoded: Any) -> None:
        if sequence is None:
            raise RuntimeError("delta sequence is required")
        if len(self._delta_buffer) >= self.maximum_buffered_deltas:
            self.reconciliation_state = ReconciliationState.DEGRADED
            raise RuntimeError("delta recovery buffer overflow")
        self.reconciliation_state = ReconciliationState.BUFFERING_DELTAS
        self._delta_buffer.append((sequence, decoded))

    async def _recover(self, connection_id: UUID, sequence: int, decoded: Any) -> None:
        self._set_state(ConnectionState.RECOVERING, "sequence gap")
        self._buffer_delta(sequence, decoded)
        if (
            self.rest_snapshot is None
            or self.snapshot_applier is None
            or self.delta_applier is None
        ):
            self._set_state(ConnectionState.DEGRADED, "sequence gap without snapshot semantics")
            raise RuntimeError("REST snapshot recovery unavailable")
        self.reconciliation_state = ReconciliationState.WAITING_FOR_SNAPSHOT
        snapshot = await self.rest_snapshot()
        self.reconciliation_state = ReconciliationState.APPLYING_SNAPSHOT
        snapshot_sequence = self.snapshot_applier(snapshot)
        replay = sorted(
            (item for item in self._delta_buffer if item[0] > snapshot_sequence),
            key=lambda item: item[0],
        )
        self.reconciliation_state = ReconciliationState.REPLAYING_DELTAS
        expected = snapshot_sequence + 1
        for current, delta in replay:
            if current != expected:
                self.reconciliation_state = ReconciliationState.DEGRADED
                raise RuntimeError("replayed delta sequence is not continuous")
            self.delta_applier(delta)
            expected += 1
        self._delta_buffer.clear()
        self.reconciliation_state = ReconciliationState.SYNCHRONIZED
        self._set_state(ConnectionState.CONNECTED, f"REST snapshot recovered for {connection_id}")

    @staticmethod
    def _decode(raw: bytes | str) -> Any:
        if isinstance(raw, bytes):
            raw = raw.decode()
        if raw in {"pong", "ping"}:
            return raw
        return json.loads(raw)
