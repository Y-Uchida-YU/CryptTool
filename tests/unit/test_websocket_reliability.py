import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.adapters.exchanges.websocket import (
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
    socket2 = FakeSocket(
        [
            json.dumps({"seq": 5, "kind": "snapshot"}),
            json.dumps({"seq": 6, "kind": "delta"}),
            json.dumps({"seq": 5, "kind": "delta"}),
        ]
    )
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
    assert (await anext(stream2)).is_snapshot
    assert (await anext(stream2)).is_delta and applied == [6]
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(stream2), 0.02)
    assert any(event.reason == "out-of-order sequence" for event in value.state_events)


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
    await value._recover(uuid4(), 6, {"seq": 6, "kind": "delta"})
    assert applied == [6] and value.reconciliation_state == ReconciliationState.SYNCHRONIZED
    assert not value._delta_buffer


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
    value._buffer_delta(3, {"seq": 3})
    with pytest.raises(RuntimeError, match="overflow"):
        value._buffer_delta(4, {"seq": 4})
    value._delta_buffer.clear()
    with pytest.raises(RuntimeError, match="continuous"):
        await value._recover(uuid4(), 3, {"seq": 3})
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
        await broken._recover(uuid4(), 2, {"seq": 2})


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
        value._buffer_delta(None, {})


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
        await value._recover(uuid4(), 2, {"seq": 2})

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
