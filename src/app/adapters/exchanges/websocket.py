from __future__ import annotations

import asyncio
import hashlib
import json
import random
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
        classification: StreamClassification = StreamClassification.EVENTS,
        stale_timeout: float = 30.0,
        acknowledgement_timeout: float = 10.0,
        maximum_reconnects_per_minute: int = 10,
        backoff_cap: float = 30.0,
    ) -> None:
        if classification == StreamClassification.SNAPSHOT_DELTA and rest_snapshot is None:
            raise ValueError("snapshot/delta streams require REST snapshot recovery")
        self.venue, self.url, self.subscription_id = venue, url, subscription_id
        self.subscribe, self.acknowledgement = subscribe, acknowledgement
        self.normalize, self.sequence = normalize, sequence
        self.exchange_timestamp = exchange_timestamp
        self.snapshot, self.delta, self.heartbeat = snapshot, delta, heartbeat
        self.rest_snapshot, self.classification = rest_snapshot, classification
        self.stale_timeout, self.acknowledgement_timeout = stale_timeout, acknowledgement_timeout
        self.maximum_reconnects_per_minute = maximum_reconnects_per_minute
        self.backoff_cap = backoff_cap
        self.state = ConnectionState.CLOSED
        self.state_events: list[ConnectionStateEvent] = []
        self._closing = False
        self._recent_hashes: deque[str] = deque(maxlen=4096)
        self._reconnects: deque[float] = deque()
        self._local_sequence = 0

    async def close(self) -> None:
        self._closing = True
        self._set_state(ConnectionState.CLOSED, "graceful shutdown")

    def _set_state(self, state: ConnectionState, reason: str | None = None) -> None:
        self.state = state
        self.state_events.append(ConnectionStateEvent(self.venue, state, datetime.now(UTC), reason))

    async def messages(self) -> AsyncIterator[RawVenueMessage]:
        attempt = 0
        previous_sequence: int | None = None
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
                                await self._recover(connection_id)
                        previous_sequence = current
                        self._local_sequence += 1
                        received = datetime.now(UTC)
                        yield RawVenueMessage(
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
                await asyncio.sleep(random.uniform(0, min(self.backoff_cap, 2 ** min(attempt, 10))))
        self._set_state(ConnectionState.CLOSED)

    async def _socket_messages(self, socket: Any) -> AsyncIterator[bytes | str]:
        while not self._closing:
            yield await asyncio.wait_for(socket.recv(), timeout=self.stale_timeout)

    async def _recover(self, connection_id: UUID) -> None:
        self._set_state(ConnectionState.RECOVERING, "sequence gap")
        if self.rest_snapshot is None:
            self._set_state(ConnectionState.DEGRADED, "sequence gap without snapshot semantics")
            raise RuntimeError("REST snapshot recovery unavailable")
        await self.rest_snapshot()
        self._set_state(ConnectionState.CONNECTED, f"REST snapshot recovered for {connection_id}")

    @staticmethod
    def _decode(raw: bytes | str) -> Any:
        if isinstance(raw, bytes):
            raw = raw.decode()
        if raw in {"pong", "ping"}:
            return raw
        return json.loads(raw)
