import asyncio
import json
from collections import deque
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.adapters.exchanges.websocket import (
    ConnectionReconciliationContext,
    ConnectionState,
    ReconciliationState,
    ResilientWebSocketSession,
    StreamClassification,
)


class FakeSocket:
    def __init__(self, messages: list[str | bytes | BaseException]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []

    async def send(self, value: str) -> None:
        self.sent.append(value)

    async def recv(self) -> str | bytes:
        if not self.messages:
            await asyncio.Future()
        value = self.messages.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


class FakeConnect:
    def __init__(self, socket: FakeSocket) -> None:
        self.socket = socket

    async def __aenter__(self) -> FakeSocket:
        return self.socket

    async def __aexit__(self, *_: object) -> None:
        return None


class QueueSocket(FakeSocket):
    def __init__(self) -> None:
        super().__init__([])
        self.queue: asyncio.Queue[str | bytes] = asyncio.Queue()

    async def recv(self) -> str | bytes:
        return await self.queue.get()


def session(**overrides: object) -> ResilientWebSocketSession:
    values = dict(
        venue="x",
        url="ws://fake",
        subscription_id="book",
        subscribe={"op": "subscribe"},
        acknowledgement=lambda x: x.get("ok") is True,
        normalize=lambda x: {"normalized": x["seq"]} if "seq" in x else x,
        sequence=lambda x: x.get("seq"),
        exchange_timestamp=lambda x: datetime.fromtimestamp(x["ts"], UTC) if "ts" in x else None,
        snapshot=lambda x: x.get("kind") == "snapshot",
        delta=lambda x: x.get("kind") == "delta",
        heartbeat=lambda x: (
            x.get("kind") == "heartbeat" if isinstance(x, dict) else x in {"ping", "pong"}
        ),
        stale_timeout=0.01,
        acknowledgement_timeout=0.01,
        backoff_cap=0,
    )
    values.update(overrides)
    return ResilientWebSocketSession(**values)  # type: ignore[arg-type]


def context() -> ConnectionReconciliationContext:
    return ConnectionReconciliationContext(
        uuid4(), ReconciliationState.DISCONNECTED, None, None, [], set(), deque()
    )


def raw_queue(*items: str) -> asyncio.Queue[str | bytes | BaseException]:
    queue: asyncio.Queue[str | bytes | BaseException] = asyncio.Queue()
    for item in items:
        queue.put_nowait(item)
    return queue


@pytest.mark.asyncio
async def test_ack_heartbeat_duplicate_and_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = json.dumps({"seq": 1, "ts": 1, "kind": "event"})
    socket = FakeSocket([json.dumps({"ok": True}), json.dumps({"kind": "heartbeat"}), raw, raw])
    monkeypatch.setattr(
        "app.adapters.exchanges.websocket.websockets.connect", lambda *a, **k: FakeConnect(socket)
    )
    retained = []

    async def retain(message: object) -> None:
        retained.append(message)

    item = await anext(session(raw_message_sink=retain).messages())
    assert item.venue_sequence == 1 and item.normalized_payload == {"normalized": 1}
    assert item.payload_sha256 and socket.sent and retained == [item]


@pytest.mark.asyncio
async def test_duplicate_out_of_order_and_snapshot_delta_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duplicate = json.dumps({"seq": 1, "kind": "event"})
    socket = FakeSocket(
        [json.dumps({"ok": True}), duplicate, duplicate, json.dumps({"seq": 2, "kind": "event"})]
    )
    monkeypatch.setattr(
        "app.adapters.exchanges.websocket.websockets.connect", lambda *a, **k: FakeConnect(socket)
    )
    stream = session().messages()
    assert (await anext(stream)).venue_sequence == 1
    assert (await anext(stream)).venue_sequence == 2
    await stream.aclose()

    applied: list[int] = []
    socket2 = FakeSocket([json.dumps({"seq": 6, "kind": "delta"})])
    monkeypatch.setattr(
        "app.adapters.exchanges.websocket.websockets.connect", lambda *a, **k: FakeConnect(socket2)
    )
    value = session(
        subscribe=None,
        acknowledgement=None,
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 5}),
        snapshot_applier=lambda x: int(x["seq"]),
        delta_applier=lambda x: applied.append(int(x["seq"])),
    )
    stream2 = value.messages()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(stream2), 0.02)
    assert applied == [6]
    assert value.reconciliation_state == ReconciliationState.SYNCHRONIZED


@pytest.mark.asyncio
async def test_ack_failure_and_malformed_json_degrade(monkeypatch: pytest.MonkeyPatch) -> None:
    sockets = iter(
        [FakeSocket([json.dumps({"ok": False})]), FakeSocket([json.dumps({"ok": True}), "{"])]
    )
    monkeypatch.setattr(
        "app.adapters.exchanges.websocket.websockets.connect",
        lambda *a, **k: FakeConnect(next(sockets)),
    )
    value = session(maximum_reconnects_per_minute=2)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(value.messages()), 0.05)
    assert value.state == ConnectionState.DEGRADED


@pytest.mark.asyncio
async def test_snapshot_recovery_replays_buffer_and_applies_snapshot() -> None:
    applied: list[int] = []
    value = session(
        subscribe=None,
        acknowledgement=None,
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 5}),
        snapshot_applier=lambda x: int(x["seq"]),
        delta_applier=lambda x: applied.append(int(x["seq"])),
    )
    current = context()
    await value._recover(current, 6, {"seq": 6, "kind": "delta"})
    assert applied == [6] and value.reconciliation_state == ReconciliationState.SYNCHRONIZED
    assert not current.buffered_deltas


@pytest.mark.asyncio
async def test_recovery_gap_snapshot_failure_and_buffer_overflow() -> None:
    value = session(
        subscribe=None,
        acknowledgement=None,
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 1}),
        snapshot_applier=lambda x: int(x["seq"]),
        delta_applier=lambda x: None,
        maximum_buffered_deltas=1,
    )
    current = context()
    value._buffer_delta(current, 3, {"seq": 3})
    with pytest.raises(RuntimeError, match="overflow"):
        value._buffer_delta(current, 4, {"seq": 4})
    current.buffered_deltas.clear()
    with pytest.raises(RuntimeError, match="continuous"):
        await value._recover(current, 3, {"seq": 3})
    assert value.reconciliation_state == ReconciliationState.DEGRADED

    async def failed() -> object:
        raise OSError("snapshot failed")

    broken = session(
        subscribe=None,
        acknowledgement=None,
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=failed,
        snapshot_applier=lambda x: 1,
        delta_applier=lambda x: None,
    )
    with pytest.raises(OSError):
        await broken._recover(context(), 2, {"seq": 2})


@pytest.mark.asyncio
async def test_stale_timeout_close_decode_and_constructor_guards() -> None:
    value = session(subscribe=None, acknowledgement=None)
    with pytest.raises(TimeoutError):
        await anext(value._socket_messages(FakeSocket([])))
    await value.close()
    assert value.state == ConnectionState.CLOSED
    assert value._decode(b"ping") == "ping"
    assert value._decode('{"a":1}') == {"a": 1}
    with pytest.raises(ValueError, match="appliers"):
        session(
            classification=StreamClassification.SNAPSHOT_DELTA,
            rest_snapshot=lambda: asyncio.sleep(0),
        )

    with pytest.raises(RuntimeError, match="sequence"):
        value._buffer_delta(context(), None, {})


@pytest.mark.asyncio
async def test_missing_recovery_callbacks_maximum_reconnect_and_loop_close() -> None:
    value = session(
        subscribe=None,
        acknowledgement=None,
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 1}),
        snapshot_applier=lambda x: 1,
        delta_applier=lambda x: None,
    )
    value.rest_snapshot = None
    with pytest.raises(RuntimeError, match="unavailable"):
        await value._recover(context(), 2, {"seq": 2})

    limited = session(
        subscribe=None, acknowledgement=None, maximum_reconnects_per_minute=1, stale_timeout=0.001
    )
    limited._reconnects.append(__import__("time").monotonic())
    task = asyncio.create_task(anext(limited.messages()))
    await asyncio.sleep(0.003)
    await limited.close()
    with pytest.raises(StopAsyncIteration):
        await task
    assert any(event.reason == "maximum reconnect rate exceeded" for event in limited.state_events)


@pytest.mark.asyncio
async def test_initial_rest_snapshot_bootstrap_buffers_first_delta() -> None:
    applied: list[tuple[str, int]] = []

    async def delayed_snapshot() -> object:
        await asyncio.sleep(0.001)
        return {"seq": 5}

    value = session(
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=delayed_snapshot,
        snapshot_applier=lambda item: applied.append(("snapshot", item["seq"])) or item["seq"],
        delta_applier=lambda item: applied.append(("delta", item["seq"])),
    )
    current = context()
    await value._bootstrap(raw_queue(json.dumps({"seq": 6, "kind": "delta"})), current)
    assert applied == [("snapshot", 5), ("delta", 6)]
    assert current.state == ReconciliationState.SYNCHRONIZED
    assert current.previous_sequence == 6


@pytest.mark.asyncio
async def test_reconnect_discards_buffer_and_duplicate_cache_is_connection_scoped() -> None:
    old = context()
    old.buffered_deltas.append((9, {"seq": 9}))
    old.payload_hashes.add("a" * 64)
    new = context()
    assert new.connection_id != old.connection_id
    assert not new.buffered_deltas
    assert "a" * 64 not in new.payload_hashes


@pytest.mark.asyncio
async def test_snapshot_sequence_ahead_and_behind_buffer() -> None:
    applied: list[int] = []
    value = session(
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 1}),
        snapshot_applier=lambda item: item["seq"],
        delta_applier=lambda item: applied.append(item["seq"]),
    )
    ahead = context()
    ahead.buffered_deltas.extend([(4, {"seq": 4}), (5, {"seq": 5})])
    await value._apply_snapshot_and_replay(ahead, {"seq": 5})
    assert applied == [] and ahead.previous_sequence == 5

    behind = context()
    behind.buffered_deltas.extend([(5, {"seq": 5}), (6, {"seq": 6})])
    with pytest.raises(RuntimeError, match="continuous"):
        await value._apply_snapshot_and_replay(behind, {"seq": 3})
    assert behind.state == ReconciliationState.DEGRADED


@pytest.mark.asyncio
async def test_initial_rest_snapshot_failure_fails_closed() -> None:
    async def failed() -> object:
        raise OSError("snapshot unavailable")

    value = session(
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=failed,
        snapshot_applier=lambda item: item["seq"],
        delta_applier=lambda item: None,
    )
    with pytest.raises(OSError, match="snapshot unavailable"):
        await value._bootstrap(raw_queue(), context())


@pytest.mark.asyncio
async def test_synchronized_delta_is_delivered_only_after_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = QueueSocket()
    await socket.queue.put(json.dumps({"ok": True}))
    await socket.queue.put(json.dumps({"seq": 6, "kind": "delta"}))

    async def delayed_snapshot() -> object:
        await asyncio.sleep(0.001)
        return {"seq": 5}

    applied: list[int] = []
    value = session(
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=delayed_snapshot,
        snapshot_applier=lambda item: item["seq"],
        delta_applier=lambda item: applied.append(item["seq"]),
    )
    monkeypatch.setattr(
        "app.adapters.exchanges.websocket.websockets.connect",
        lambda *args, **kwargs: FakeConnect(socket),
    )

    async def send_after_bootstrap() -> None:
        await asyncio.sleep(0.003)
        await socket.queue.put(json.dumps({"seq": 7, "kind": "delta"}))

    producer = asyncio.create_task(send_after_bootstrap())
    stream = value.messages()
    delivered = await asyncio.wait_for(anext(stream), 0.1)
    await producer
    assert delivered.venue_sequence == 7
    assert applied == [6, 7]
    assert value.reconciliation_state == ReconciliationState.SYNCHRONIZED
    await stream.aclose()


@pytest.mark.asyncio
async def test_bootstrap_rejects_missing_loader_and_non_delta_message() -> None:
    value = session(
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 1}),
        snapshot_applier=lambda item: item["seq"],
        delta_applier=lambda item: None,
    )
    value.rest_snapshot = None
    with pytest.raises(RuntimeError, match="unavailable"):
        await value._bootstrap(raw_queue(), context())

    async def delayed_snapshot() -> object:
        await asyncio.sleep(0.01)
        return {"seq": 1}

    value.rest_snapshot = delayed_snapshot
    with pytest.raises(RuntimeError, match="non-delta"):
        await value._bootstrap(raw_queue(json.dumps({"kind": "snapshot"})), context())


@pytest.mark.asyncio
async def test_snapshot_and_receive_complete_simultaneously_without_message_loss() -> None:
    applied: list[int] = []
    queue = raw_queue(
        json.dumps({"kind": "heartbeat"}),
        json.dumps({"seq": 6, "kind": "delta"}),
    )
    value = session(
        classification=StreamClassification.SNAPSHOT_DELTA,
        rest_snapshot=lambda: asyncio.sleep(0, result={"seq": 5}),
        snapshot_applier=lambda item: item["seq"],
        delta_applier=lambda item: applied.append(item["seq"]),
    )
    await value._bootstrap(queue, context())
    assert applied == [6]
    assert queue.empty()

    value.snapshot_applier = None
    with pytest.raises(RuntimeError, match="unavailable"):
        await value._apply_snapshot_and_replay(context(), {"seq": 5})


def test_duplicate_cache_remains_bounded() -> None:
    value = session(duplicate_cache_size=3)
    current = context()
    for digest in ("a", "b", "c", "d"):
        assert not value._seen_payload(current, digest)
    assert current.payload_hash_order == deque(("b", "c", "d"))
    assert current.payload_hashes == {"b", "c", "d"}
    assert value._seen_payload(current, "d")
