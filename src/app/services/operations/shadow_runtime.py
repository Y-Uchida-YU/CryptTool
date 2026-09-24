from __future__ import annotations

import hashlib
import json
import math
import zlib
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from time import perf_counter
from typing import Protocol

from app.domain.strategies.funding_carry import FundingCarryRejectCode
from app.services.operations.models import LiveSignalInput
from app.services.research.models import (
    ExperimentalEventCursor,
    RawMarketEvent,
    ShadowInputCheckpoint,
)

SHADOW_INPUT_REFRESH_INTERVAL_SECONDS = 10


class ExperimentalEventRepository(Protocol):
    def list_experimental_events_after(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
        limit: int,
    ) -> tuple[RawMarketEvent, ...]: ...

    def experimental_event_backlog(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        *,
        venues: tuple[str, ...],
        instrument: str,
        event_types: tuple[str, ...],
    ) -> tuple[int, datetime | None]: ...

    def shadow_input_checkpoint(
        self, run_id: str
    ) -> tuple[tuple[ExperimentalEventCursor, ...], ShadowInputCheckpoint | None]: ...

    def commit_shadow_input_checkpoint(
        self,
        cursors: tuple[ExperimentalEventCursor, ...],
        checkpoint: ShadowInputCheckpoint,
    ) -> None: ...

    def list_experimental_event_ids_between(
        self,
        *,
        venue: str,
        instrument: str,
        event_type: str,
        after_available_at: datetime,
        after_event_id: str,
        through_available_at: datetime,
        through_event_id: str,
    ) -> tuple[str, ...]: ...


class BookStateStatus(StrEnum):
    UNINITIALIZED = "uninitialized"
    VALID = "valid"
    INVALID = "invalid"


@dataclass(frozen=True)
class CanonicalOrderBookSnapshot:
    venue: str
    instrument: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    best_bid: Decimal
    best_ask: Decimal
    depth_level_count: int
    source_snapshot_event_id: str
    applied_delta_event_ids: tuple[str, ...]
    last_sequence: int | None
    connection_epoch: int
    exchange_timestamp: datetime
    received_at: datetime
    available_at: datetime
    state_status: BookStateStatus
    state_hash: str
    normalizer_version: str


@dataclass(frozen=True)
class MissingShadowInput:
    reason: FundingCarryRejectCode
    venue: str
    capability: str
    last_seen_event_at: datetime | None
    last_valid_event_at: datetime | None
    source_event_count: int


@dataclass(frozen=True)
class ShadowTimingMetrics:
    hyperliquid_funding_age_seconds: Decimal
    bitget_funding_age_seconds: Decimal
    funding_observation_skew_seconds: Decimal
    hyperliquid_orderbook_age_seconds: Decimal
    bitget_orderbook_age_seconds: Decimal
    orderbook_venue_skew_seconds: Decimal
    funding_freshness_pass: bool
    orderbook_freshness_pass: bool
    orderbook_synchronization_pass: bool


@dataclass(frozen=True)
class ShadowInputBatch:
    events: tuple[LiveSignalInput, ...]
    matched: bool
    pair_identity: str | None
    source_pair_duplicate: bool
    missing: MissingShadowInput | None = None
    timing: ShadowTimingMetrics | None = None
    # Legacy only. It must never mix funding and order-book timestamp domains.
    venue_timestamp_skew_seconds: Decimal | None = None


@dataclass
class OrderBookStateMetrics:
    snapshot_count: int = 0
    delta_count: int = 0
    valid_snapshot_count: int = 0
    invalid_count: int = 0
    sequence_gap_count: int = 0
    out_of_order_count: int = 0
    reconnect_reset_count: int = 0
    checksum_failure_count: int = 0


@dataclass
class _BookState:
    venue: str
    instrument: str
    channel: str
    connection_epoch: int
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    source_snapshot_event_id: str
    source_snapshot_available_at: datetime
    applied_delta_event_ids: list[str]
    last_sequence: int | None
    exchange_timestamp: datetime
    received_at: datetime
    available_at: datetime
    normalizer_version: str
    status: BookStateStatus = BookStateStatus.VALID


def _finite(value: Decimal) -> bool:
    return value.is_finite() and math.isfinite(float(value))


def _levels(payload: dict[str, object], side: str) -> tuple[tuple[Decimal, Decimal], ...]:
    raw = payload.get(side)
    levels_value = payload.get("levels")
    if raw is None and isinstance(levels_value, list):
        levels = levels_value
        raw = levels[0 if side == "bids" else 1] if len(levels) >= 2 else None
    if not isinstance(raw, list):
        return ()
    result: list[tuple[Decimal, Decimal]] = []
    for item in raw:
        if isinstance(item, dict):
            price = item.get("px") or item.get("price")
            quantity = item.get("sz") or item.get("quantity") or item.get("size")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, quantity = item[0], item[1]
        else:
            raise ValueError("invalid order-book level")
        try:
            parsed = (Decimal(str(price)), Decimal(str(quantity)))
        except (InvalidOperation, TypeError) as exc:
            raise ValueError("non-decimal order-book level") from exc
        if not all(_finite(value) for value in parsed):
            raise ValueError("non-finite order-book level")
        result.append(parsed)
    return tuple(result)


def _book_hash(
    *,
    venue: str,
    instrument: str,
    bids: tuple[tuple[Decimal, Decimal], ...],
    asks: tuple[tuple[Decimal, Decimal], ...],
    epoch: int,
    sequence: int | None,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "venue": venue,
                "instrument": instrument,
                "bids": bids,
                "asks": asks,
                "connection_epoch": epoch,
                "last_sequence": sequence,
            },
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class CanonicalOrderBookStateBuilder:
    def __init__(self, *, maximum_age_seconds: int = 30, maximum_future_seconds: int = 1) -> None:
        self.maximum_age = timedelta(seconds=maximum_age_seconds)
        self.maximum_future = timedelta(seconds=maximum_future_seconds)
        self._states: dict[tuple[str, str, str], _BookState] = {}
        self.metrics = OrderBookStateMetrics()
        self.last_invalid_reason: dict[tuple[str, str], FundingCarryRejectCode] = {}

    def export_state(self) -> dict[str, object]:
        """Return the durable replay boundary for memory-only book state."""
        return {
            "states": [
                {
                    **{
                        key: value
                        for key, value in asdict(state).items()
                        if key not in {"bids", "asks", "status", "applied_delta_event_ids"}
                    },
                    "bids": list(state.bids.items()),
                    "asks": list(state.asks.items()),
                    "status": state.status.value,
                }
                for _, state in sorted(self._states.items())
            ],
            "metrics": asdict(self.metrics),
            "last_invalid_reason": [
                {"venue": venue, "instrument": instrument, "reason": reason.value}
                for (venue, instrument), reason in sorted(self.last_invalid_reason.items())
            ],
        }

    def restore_state(self, value: dict[str, object]) -> None:
        """Restore a snapshot-backed book before consuming post-cursor deltas."""
        self._states.clear()
        states = value.get("states", [])
        if isinstance(states, list):
            for item in states:
                if not isinstance(item, dict):
                    continue
                state = _BookState(
                    venue=str(item["venue"]),
                    instrument=str(item["instrument"]),
                    channel=str(item["channel"]),
                    connection_epoch=int(item["connection_epoch"]),
                    bids={Decimal(str(price)): Decimal(str(size)) for price, size in item["bids"]},
                    asks={Decimal(str(price)): Decimal(str(size)) for price, size in item["asks"]},
                    source_snapshot_event_id=str(item["source_snapshot_event_id"]),
                    source_snapshot_available_at=datetime.fromisoformat(
                        str(item["source_snapshot_available_at"])
                    ),
                    applied_delta_event_ids=[],
                    last_sequence=(
                        int(item["last_sequence"])
                        if item.get("last_sequence") is not None
                        else None
                    ),
                    exchange_timestamp=datetime.fromisoformat(str(item["exchange_timestamp"])),
                    received_at=datetime.fromisoformat(str(item["received_at"])),
                    available_at=datetime.fromisoformat(str(item["available_at"])),
                    normalizer_version=str(item["normalizer_version"]),
                    status=BookStateStatus(str(item["status"])),
                )
                self._states[(state.venue, state.instrument, state.channel)] = state
        metrics = value.get("metrics", {})
        if isinstance(metrics, dict):
            self.metrics = OrderBookStateMetrics(
                **{
                    name: int(metrics.get(name, 0))
                    for name in OrderBookStateMetrics.__annotations__
                }
            )
        self.last_invalid_reason.clear()
        reasons = value.get("last_invalid_reason", [])
        if isinstance(reasons, list):
            for item in reasons:
                if isinstance(item, dict):
                    self.last_invalid_reason[(str(item["venue"]), str(item["instrument"]))] = (
                        FundingCarryRejectCode(str(item["reason"]))
                    )

    def recovery_boundaries(
        self,
    ) -> tuple[tuple[str, str, str, datetime, str], ...]:
        return tuple(
            (
                state.venue,
                state.instrument,
                state.channel,
                state.source_snapshot_available_at,
                state.source_snapshot_event_id,
            )
            for _, state in sorted(self._states.items())
        )

    def restore_applied_delta_event_ids(
        self,
        *,
        venue: str,
        instrument: str,
        channel: str,
        event_ids: tuple[str, ...],
    ) -> None:
        state = self._states.get((venue, instrument, channel))
        if state is not None:
            state.applied_delta_event_ids = list(event_ids)

    def apply(self, event: RawMarketEvent, *, now: datetime) -> CanonicalOrderBookSnapshot | None:
        if event.venue not in {"hyperliquid", "bitget"} or event.canonical_instrument_id != "BTC":
            return None
        if event.event_type not in {"orderbook_snapshot", "orderbook_delta"}:
            return None
        if event.venue == "hyperliquid":
            if event.event_type != "orderbook_snapshot":
                return None
            self.metrics.snapshot_count += 1
            try:
                return self._standalone_snapshot(event, now=now)
            except ValueError as exc:
                self.metrics.invalid_count += 1
                self.last_invalid_reason[(event.venue, "BTC")] = self._validation_reason(exc)
                return None
        return self._apply_bitget(event, now=now)

    def _standalone_snapshot(
        self, event: RawMarketEvent, *, now: datetime
    ) -> CanonicalOrderBookSnapshot:
        payload = event.payload()
        bids, asks = _levels(payload, "bids"), _levels(payload, "asks")
        return self._canonical(
            event=event,
            bids=bids,
            asks=asks,
            source_snapshot_event_id=event.event_id,
            applied_delta_event_ids=(),
            last_sequence=event.sequence,
            epoch=event.connection_epoch or 0,
            now=now,
        )

    def _apply_bitget(
        self, event: RawMarketEvent, *, now: datetime
    ) -> CanonicalOrderBookSnapshot | None:
        channel = event.channel or "orderbook"
        key = (event.venue, event.canonical_instrument_id, channel)
        epoch = event.connection_epoch or 0
        current = self._states.get(key)
        if current is not None and current.connection_epoch != epoch:
            self.metrics.reconnect_reset_count += 1
            self._states.pop(key, None)
            current = None
        payload = event.payload()
        update_payload = payload.get("_book_update")
        update = update_payload if isinstance(update_payload, dict) else payload
        is_snapshot = event.event_type == "orderbook_snapshot" or update.get("action") == "snapshot"
        if is_snapshot:
            self.metrics.snapshot_count += 1
            try:
                bids = dict(_levels(update, "bids"))
                asks = dict(_levels(update, "asks"))
                current = _BookState(
                    venue=event.venue,
                    instrument=event.canonical_instrument_id,
                    channel=channel,
                    connection_epoch=epoch,
                    bids=bids,
                    asks=asks,
                    source_snapshot_event_id=event.event_id,
                    source_snapshot_available_at=event.available_at,
                    applied_delta_event_ids=[],
                    last_sequence=event.sequence,
                    exchange_timestamp=event.exchange_timestamp or event.available_at,
                    received_at=event.received_at,
                    available_at=event.available_at,
                    normalizer_version=event.normalizer_version,
                )
                self._states[key] = current
                snapshot = self._from_state(current, now=now)
                self._validate_checksum(update, snapshot)
                return snapshot
            except ValueError as exc:
                self.metrics.invalid_count += 1
                self.last_invalid_reason[("bitget", "BTC")] = self._validation_reason(exc)
                if current is not None:
                    current.status = BookStateStatus.INVALID
                return None
        self.metrics.delta_count += 1
        if current is None:
            self.last_invalid_reason[("bitget", "BTC")] = (
                FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_NOT_INITIALIZED
            )
            return None
        if current.status is BookStateStatus.INVALID:
            return None
        sequence = event.sequence
        previous_sequence_value = update.get("previous_sequence")
        try:
            previous_sequence = (
                int(str(previous_sequence_value)) if previous_sequence_value is not None else None
            )
        except (TypeError, ValueError):
            current.status = BookStateStatus.INVALID
            self.metrics.sequence_gap_count += 1
            self.metrics.invalid_count += 1
            self.last_invalid_reason[("bitget", "BTC")] = (
                FundingCarryRejectCode.BITGET_ORDERBOOK_SEQUENCE_GAP
            )
            return None
        if sequence is not None and current.last_sequence is not None:
            if sequence <= current.last_sequence:
                self.metrics.out_of_order_count += 1
                return None
            has_gap = (
                previous_sequence != current.last_sequence
                if previous_sequence is not None
                else sequence != current.last_sequence + 1
            )
            if has_gap:
                current.status = BookStateStatus.INVALID
                self.metrics.sequence_gap_count += 1
                self.metrics.invalid_count += 1
                self.last_invalid_reason[("bitget", "BTC")] = (
                    FundingCarryRejectCode.BITGET_ORDERBOOK_SEQUENCE_GAP
                )
                return None
        try:
            self._update_side(current.bids, _levels(update, "bids"))
            self._update_side(current.asks, _levels(update, "asks"))
            current.applied_delta_event_ids.append(event.event_id)
            current.last_sequence = sequence if sequence is not None else current.last_sequence
            current.exchange_timestamp = event.exchange_timestamp or event.available_at
            current.received_at = event.received_at
            current.available_at = event.available_at
            snapshot = self._from_state(current, now=now)
            self._validate_checksum(update, snapshot)
            return snapshot
        except ValueError as exc:
            current.status = BookStateStatus.INVALID
            self.metrics.invalid_count += 1
            self.last_invalid_reason[("bitget", "BTC")] = self._validation_reason(exc)
            return None

    @staticmethod
    def _update_side(
        state: dict[Decimal, Decimal], updates: tuple[tuple[Decimal, Decimal], ...]
    ) -> None:
        for price, quantity in updates:
            if price <= 0 or quantity < 0:
                raise ValueError("invalid order-book delta")
            if quantity == 0:
                state.pop(price, None)
            else:
                state[price] = quantity

    def _from_state(self, state: _BookState, *, now: datetime) -> CanonicalOrderBookSnapshot:
        return self._canonical(
            event=None,
            venue=state.venue,
            instrument=state.instrument,
            bids=tuple(sorted(state.bids.items(), reverse=True)),
            asks=tuple(sorted(state.asks.items())),
            source_snapshot_event_id=state.source_snapshot_event_id,
            applied_delta_event_ids=tuple(state.applied_delta_event_ids),
            last_sequence=state.last_sequence,
            epoch=state.connection_epoch,
            exchange_timestamp=state.exchange_timestamp,
            received_at=state.received_at,
            available_at=state.available_at,
            normalizer_version=state.normalizer_version,
            now=now,
        )

    def _canonical(
        self,
        *,
        event: RawMarketEvent | None,
        bids: tuple[tuple[Decimal, Decimal], ...],
        asks: tuple[tuple[Decimal, Decimal], ...],
        source_snapshot_event_id: str,
        applied_delta_event_ids: tuple[str, ...],
        last_sequence: int | None,
        epoch: int,
        now: datetime,
        venue: str | None = None,
        instrument: str | None = None,
        exchange_timestamp: datetime | None = None,
        received_at: datetime | None = None,
        available_at: datetime | None = None,
        normalizer_version: str | None = None,
    ) -> CanonicalOrderBookSnapshot:
        if event is not None:
            venue = event.venue
            instrument = event.canonical_instrument_id
            exchange_timestamp = event.exchange_timestamp or event.available_at
            received_at = event.received_at
            available_at = event.available_at
            normalizer_version = event.normalizer_version
        if venue is None or instrument is None:
            raise ValueError("missing canonical order-book identity")
        if exchange_timestamp is None or received_at is None or available_at is None:
            raise ValueError("missing canonical order-book timestamp")
        if not bids or not asks:
            raise ValueError("empty order book")
        if any(
            price <= 0 or quantity <= 0 for level in (*bids, *asks) for price, quantity in (level,)
        ):
            raise ValueError("non-positive order-book level")
        if any(not _finite(value) for level in (*bids, *asks) for value in level):
            raise ValueError("non-finite order-book level")
        sorted_bids = tuple(sorted(bids, reverse=True))
        sorted_asks = tuple(sorted(asks))
        if sorted_bids[0][0] >= sorted_asks[0][0]:
            raise ValueError("crossed order book")
        source_timestamp = exchange_timestamp.astimezone(UTC)
        if source_timestamp > now + self.maximum_future:
            raise ValueError("future order book")
        if now - source_timestamp > self.maximum_age:
            raise ValueError("stale order book")
        state_hash = _book_hash(
            venue=venue,
            instrument=instrument,
            bids=sorted_bids,
            asks=sorted_asks,
            epoch=epoch,
            sequence=last_sequence,
        )
        self.metrics.valid_snapshot_count += 1
        return CanonicalOrderBookSnapshot(
            venue=venue,
            instrument=instrument,
            bids=sorted_bids,
            asks=sorted_asks,
            best_bid=sorted_bids[0][0],
            best_ask=sorted_asks[0][0],
            depth_level_count=len(sorted_bids) + len(sorted_asks),
            source_snapshot_event_id=source_snapshot_event_id,
            applied_delta_event_ids=applied_delta_event_ids,
            last_sequence=last_sequence,
            connection_epoch=epoch,
            exchange_timestamp=exchange_timestamp,
            received_at=received_at,
            available_at=available_at,
            state_status=BookStateStatus.VALID,
            state_hash=state_hash,
            normalizer_version=normalizer_version or "unknown",
        )

    def _validate_checksum(
        self, payload: dict[str, object], snapshot: CanonicalOrderBookSnapshot
    ) -> None:
        provided = payload.get("checksum")
        if provided is None:
            return
        values: list[str] = []
        for index in range(max(len(snapshot.bids[:25]), len(snapshot.asks[:25]))):
            if index < len(snapshot.bids[:25]):
                values.extend(map(str, snapshot.bids[index]))
            if index < len(snapshot.asks[:25]):
                values.extend(map(str, snapshot.asks[index]))
        computed = zlib.crc32(":".join(values).encode())
        signed = computed if computed < 2**31 else computed - 2**32
        if int(str(provided)) not in {computed, signed}:
            self.metrics.checksum_failure_count += 1
            raise ValueError("order-book checksum mismatch")

    @staticmethod
    def _validation_reason(error: ValueError) -> FundingCarryRejectCode:
        message = str(error)
        if "future" in message:
            return FundingCarryRejectCode.FUTURE_TIMESTAMP
        if "stale" in message:
            return FundingCarryRejectCode.STALE_DATA
        if "crossed" in message:
            return FundingCarryRejectCode.CROSSED_ORDERBOOK
        return FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_INVALID


class FundingCarryShadowInputSource:
    SOURCE_TABLE = "experimental_market_events"
    FUNDING_TIMESTAMP_SEMANTIC = "funding_observation"
    ORDERBOOK_TIMESTAMP_SEMANTIC = "orderbook_market_event"

    def __init__(
        self,
        repository: ExperimentalEventRepository,
        *,
        run_id: str = "shadow-input-test",
        batch_size: int = 1000,
        maximum_shadow_input_stall_seconds: int | None = None,
        initial_available_at: datetime | None = None,
        maximum_age_seconds: int = 30,
        maximum_future_seconds: int = 1,
        collector_poll_interval_seconds: float = 30,
        input_refresh_interval_seconds: float = SHADOW_INPUT_REFRESH_INTERVAL_SECONDS,
        funding_max_age_seconds: int | None = None,
        funding_max_observation_skew_seconds: int | None = None,
        maximum_orderbook_venue_skew_seconds: int = 5,
        maximum_venue_timestamp_skew_seconds: int | None = None,
    ) -> None:
        self.repository = repository
        self.run_id = run_id
        self.batch_size = batch_size
        if batch_size <= 0:
            raise ValueError("shadow input batch size must be positive")
        self.maximum_shadow_input_stall_seconds = (
            maximum_shadow_input_stall_seconds
            if maximum_shadow_input_stall_seconds is not None
            else math.ceil(2 * (collector_poll_interval_seconds + input_refresh_interval_seconds))
        )
        self.maximum_orderbook_age = timedelta(seconds=maximum_age_seconds)
        self.maximum_future = timedelta(seconds=maximum_future_seconds)
        derived_funding_age = math.ceil(
            collector_poll_interval_seconds + input_refresh_interval_seconds
        )
        derived_funding_skew = math.ceil(collector_poll_interval_seconds)
        self.funding_max_age_seconds = (
            funding_max_age_seconds if funding_max_age_seconds is not None else derived_funding_age
        )
        self.funding_max_observation_skew_seconds = (
            funding_max_observation_skew_seconds
            if funding_max_observation_skew_seconds is not None
            else derived_funding_skew
        )
        self.maximum_orderbook_venue_skew_seconds = (
            maximum_orderbook_venue_skew_seconds
            if maximum_venue_timestamp_skew_seconds is None
            else maximum_venue_timestamp_skew_seconds
        )
        self.maximum_funding_age = timedelta(seconds=self.funding_max_age_seconds)
        self.maximum_funding_skew = Decimal(self.funding_max_observation_skew_seconds)
        self.maximum_orderbook_skew = Decimal(self.maximum_orderbook_venue_skew_seconds)
        self.book_builder = CanonicalOrderBookStateBuilder(
            maximum_age_seconds=maximum_age_seconds,
            maximum_future_seconds=maximum_future_seconds,
        )
        self._latest_funding: dict[str, LiveSignalInput] = {}
        self._latest_books: dict[str, LiveSignalInput] = {}
        self._last_pair_identity: str | None = None
        self.source_event_count = 0
        self.last_seen: dict[tuple[str, str], datetime] = {}
        self.last_valid: dict[tuple[str, str], datetime] = {}
        self._funding_invalid_reason: dict[str, FundingCarryRejectCode] = {}
        self._events_fetched_total = 0
        self._events_processed_total = 0
        self._events_failed_total = 0
        self._batch_count = 0
        self._last_batch_size = 0
        self._scan_duration_ms = Decimal("0")
        self._processing_duration_ms = Decimal("0")
        self._latest_db_event_available_at: datetime | None = None
        self._lag_seconds: Decimal | None = None
        self._backlog_estimate = 0
        self._strategy_observation_first_at: datetime | None = None
        self._strategy_observation_last_at: datetime | None = None
        self._last_cursor_advanced_at: datetime | None = None
        self.runtime_status = "healthy"
        self._processed_by_stream: dict[tuple[str, str], int] = {}
        saved_cursors, checkpoint = repository.shadow_input_checkpoint(run_id)
        cursor_map = {(item.venue, item.event_stream): item for item in saved_cursors}
        initialized_at = datetime.now(UTC)
        self._started_at = initialized_at
        self._cursors = tuple(
            cursor_map.get((venue, stream))
            or ExperimentalEventCursor(
                run_id=run_id,
                venue=venue,
                instrument="BTC",
                event_stream=stream,
                last_available_at=initial_available_at,
                last_event_id="" if initial_available_at is not None else None,
                updated_at=initialized_at,
            )
            for venue in ("hyperliquid", "bitget")
            for stream in ("funding_current", "orderbook_snapshot")
        )
        if checkpoint is not None:
            self._restore_checkpoint(checkpoint)
            self._restore_delta_identities()
        elif self._last_cursor_advanced_at is None:
            self._last_cursor_advanced_at = initialized_at

    def read(self, *, now: datetime) -> ShadowInputBatch:
        before = self._state_json()
        cursors_before = self._cursors
        scan_started = perf_counter()
        events = self.repository.list_experimental_events_after(
            self._cursors,
            venues=("hyperliquid", "bitget"),
            instrument="BTC",
            event_types=("funding_current", "orderbook_snapshot", "orderbook_delta"),
            limit=self.batch_size,
        )
        self._scan_duration_ms = Decimal(str((perf_counter() - scan_started) * 1000))
        self._events_fetched_total += len(events)
        self._last_batch_size = len(events)
        self._batch_count += 1
        processing_started = perf_counter()
        try:
            for event in events:
                self._process_event(event, now=now)
            self._processing_duration_ms = Decimal(
                str((perf_counter() - processing_started) * 1000)
            )
            self._events_processed_total += len(events)
            self.source_event_count = self._events_processed_total
            if events:
                self._advance_cursors(events, now=now)
                self._last_cursor_advanced_at = now
            result = self._evaluate(now=now)
            if result.matched:
                self._strategy_observation_first_at = self._strategy_observation_first_at or now
                self._strategy_observation_last_at = now
            self._refresh_lag(now=now)
            self.repository.commit_shadow_input_checkpoint(self._cursors, self._checkpoint(now=now))
            return result
        except Exception:
            self._events_failed_total += len(events)
            self._cursors = cursors_before
            self._restore_state_json(before)
            self._refresh_lag(now=now)
            self.repository.commit_shadow_input_checkpoint(self._cursors, self._checkpoint(now=now))
            raise

    def _process_event(self, event: RawMarketEvent, *, now: datetime) -> None:
        capability = (
            "funding_current" if event.event_type == "funding_current" else "orderbook_snapshot"
        )
        key = (event.venue, capability)
        self._processed_by_stream[key] = self._processed_by_stream.get(key, 0) + 1
        self.last_seen[key] = event.available_at
        if event.event_type == "funding_current":
            parsed = self._funding(event, now=now)
            if parsed is not None and (
                event.venue not in self._latest_funding
                or parsed.available_at >= self._latest_funding[event.venue].available_at
            ):
                self._latest_funding[event.venue] = parsed
                self.last_valid[key] = event.available_at
            elif parsed is None:
                self._latest_funding.pop(event.venue, None)
            return
        snapshot = self.book_builder.apply(event, now=now)
        if snapshot is not None:
            self._latest_books[event.venue] = self._book(snapshot, now=now)
            self.last_valid[key] = event.available_at
        elif event.venue == "hyperliquid" or self.book_builder.last_invalid_reason.get(
            (event.venue, "BTC")
        ) in {
            FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_NOT_INITIALIZED,
            FundingCarryRejectCode.BITGET_ORDERBOOK_SEQUENCE_GAP,
            FundingCarryRejectCode.BITGET_ORDERBOOK_STATE_INVALID,
            FundingCarryRejectCode.STALE_DATA,
            FundingCarryRejectCode.FUTURE_TIMESTAMP,
            FundingCarryRejectCode.CROSSED_ORDERBOOK,
        }:
            self._latest_books.pop(event.venue, None)

    def _evaluate(self, *, now: datetime) -> ShadowInputBatch:
        missing = self._missing()
        if missing is not None:
            return ShadowInputBatch(self._available_events(), False, None, False, missing)
        paired = (
            self._latest_funding["hyperliquid"],
            self._latest_books["hyperliquid"],
            self._latest_funding["bitget"],
            self._latest_books["bitget"],
        )
        paired = (
            self._with_freshness(paired[0], now=now),
            self._with_freshness(paired[1], now=now),
            self._with_freshness(paired[2], now=now),
            self._with_freshness(paired[3], now=now),
        )
        for item in paired:
            timestamp = item.exchange_timestamp or item.available_at
            if timestamp > now + self.maximum_future:
                capability = (
                    "funding_current"
                    if item.timestamp_semantic == self.FUNDING_TIMESTAMP_SEMANTIC
                    else "orderbook_snapshot"
                )
                return self._invalid_batch(
                    paired,
                    item,
                    capability,
                    FundingCarryRejectCode.FUTURE_TIMESTAMP,
                )
        timing = self._timing_metrics(paired)
        rejection = self._timing_rejection(paired, timing)
        if rejection is not None:
            item, capability, reason = rejection
            return self._invalid_batch(
                paired,
                item,
                capability,
                reason,
                timing=timing,
            )
        pair_identity = hashlib.sha256(
            json.dumps(self._source_event_ids(paired), separators=(",", ":")).encode()
        ).hexdigest()
        duplicate = pair_identity == self._last_pair_identity
        self._last_pair_identity = pair_identity
        return ShadowInputBatch(
            paired,
            True,
            pair_identity,
            duplicate,
            timing=timing,
        )

    @staticmethod
    def _stream(event_type: str) -> str:
        return "funding_current" if event_type == "funding_current" else "orderbook_snapshot"

    def _advance_cursors(self, events: tuple[RawMarketEvent, ...], *, now: datetime) -> None:
        latest: dict[tuple[str, str], RawMarketEvent] = {}
        for event in events:
            key = (event.venue, self._stream(event.event_type))
            previous = latest.get(key)
            if previous is None or (event.available_at, event.event_id) > (
                previous.available_at,
                previous.event_id,
            ):
                latest[key] = event
        self._cursors = tuple(
            replace(
                cursor,
                last_available_at=latest[(cursor.venue, cursor.event_stream)].available_at,
                last_event_id=latest[(cursor.venue, cursor.event_stream)].event_id,
                updated_at=now,
            )
            if (cursor.venue, cursor.event_stream) in latest
            else cursor
            for cursor in self._cursors
        )

    def _refresh_lag(self, *, now: datetime) -> None:
        backlog, latest = self.repository.experimental_event_backlog(
            self._cursors,
            venues=("hyperliquid", "bitget"),
            instrument="BTC",
            event_types=("funding_current", "orderbook_snapshot", "orderbook_delta"),
        )
        self._backlog_estimate = backlog
        self._latest_db_event_available_at = latest
        cursor_times = [
            item.last_available_at for item in self._cursors if item.last_available_at is not None
        ]
        newest_cursor = max(cursor_times) if cursor_times else None
        self._lag_seconds = (
            Decimal(str(max(0.0, (latest - newest_cursor).total_seconds())))
            if latest is not None and newest_cursor is not None
            else None
        )
        stalled_for = max(
            (max(0.0, (now - cursor.updated_at).total_seconds()) for cursor in self._cursors),
            default=0,
        )
        self.runtime_status = (
            "degraded"
            if backlog > 0 and stalled_for > self.maximum_shadow_input_stall_seconds
            else "healthy"
        )

    @staticmethod
    def _input_payload(item: LiveSignalInput) -> dict[str, object]:
        payload = asdict(item)
        if item.event_type == "canonical_orderbook_snapshot":
            payload["applied_source_event_ids"] = ()
        return payload

    @staticmethod
    def _restore_input(value: dict[str, object]) -> LiveSignalInput:
        date_fields = {
            "available_at",
            "next_funding_at",
            "source_timestamp",
            "received_at",
            "exchange_timestamp",
        }
        decimal_fields = {
            "bid",
            "ask",
            "bid_size",
            "ask_size",
            "funding_rate",
            "freshness_age_seconds",
        }
        restored = dict(value)
        for name in date_fields:
            if restored.get(name) is not None:
                restored[name] = datetime.fromisoformat(str(restored[name]))
        for name in decimal_fields:
            if restored.get(name) is not None:
                restored[name] = Decimal(str(restored[name]))
        for name in ("bids", "asks"):
            levels = restored.get(name, [])
            if not isinstance(levels, list):
                levels = []
            restored[name] = tuple(
                (Decimal(str(price)), Decimal(str(quantity))) for price, quantity in levels
            )
        applied_ids = restored.get("applied_source_event_ids", [])
        if not isinstance(applied_ids, list):
            applied_ids = []
        restored["applied_source_event_ids"] = tuple(str(item) for item in applied_ids)
        return LiveSignalInput(**restored)  # type: ignore[arg-type]

    def _state_json(self) -> str:
        payload = {
            "latest_funding": {
                venue: self._input_payload(item) for venue, item in self._latest_funding.items()
            },
            "latest_books": {
                venue: self._input_payload(item) for venue, item in self._latest_books.items()
            },
            "last_pair_identity": self._last_pair_identity,
            "last_seen": [
                [venue, capability, timestamp]
                for (venue, capability), timestamp in sorted(self.last_seen.items())
            ],
            "last_valid": [
                [venue, capability, timestamp]
                for (venue, capability), timestamp in sorted(self.last_valid.items())
            ],
            "funding_invalid_reason": {
                venue: reason.value for venue, reason in self._funding_invalid_reason.items()
            },
            "processed_by_stream": [
                [venue, capability, count]
                for (venue, capability), count in sorted(self._processed_by_stream.items())
            ],
            "book_builder": self.book_builder.export_state(),
        }
        return json.dumps(payload, default=str, sort_keys=True, separators=(",", ":"))

    def _restore_state_json(self, value: str) -> None:
        payload = json.loads(value) if value else {}
        self._latest_funding = {
            venue: self._restore_input(item)
            for venue, item in payload.get("latest_funding", {}).items()
        }
        self._latest_books = {
            venue: self._restore_input(item)
            for venue, item in payload.get("latest_books", {}).items()
        }
        self._last_pair_identity = payload.get("last_pair_identity")
        self.last_seen = {
            (str(venue), str(capability)): datetime.fromisoformat(str(timestamp))
            for venue, capability, timestamp in payload.get("last_seen", [])
        }
        self.last_valid = {
            (str(venue), str(capability)): datetime.fromisoformat(str(timestamp))
            for venue, capability, timestamp in payload.get("last_valid", [])
        }
        self._funding_invalid_reason = {
            venue: FundingCarryRejectCode(reason)
            for venue, reason in payload.get("funding_invalid_reason", {}).items()
        }
        self._processed_by_stream = {
            (str(venue), str(capability)): int(count)
            for venue, capability, count in payload.get("processed_by_stream", [])
        }
        builder = payload.get("book_builder", {})
        if isinstance(builder, dict):
            self.book_builder.restore_state(builder)

    def _restore_checkpoint(self, checkpoint: ShadowInputCheckpoint) -> None:
        self._restore_state_json(checkpoint.state_json)
        self._events_fetched_total = checkpoint.events_fetched_total
        self._events_processed_total = checkpoint.events_processed_total
        self._events_failed_total = checkpoint.events_failed_total
        self._batch_count = checkpoint.batch_count
        self._last_batch_size = checkpoint.last_batch_size
        self._scan_duration_ms = checkpoint.scan_duration_ms
        self._processing_duration_ms = checkpoint.processing_duration_ms
        self._latest_db_event_available_at = checkpoint.latest_db_event_available_at
        self._lag_seconds = checkpoint.shadow_input_lag_seconds
        self._backlog_estimate = checkpoint.shadow_input_backlog_estimate
        self._strategy_observation_first_at = checkpoint.strategy_observation_first_at
        self._strategy_observation_last_at = checkpoint.strategy_observation_last_at
        self._last_cursor_advanced_at = checkpoint.last_cursor_advanced_at
        self.runtime_status = checkpoint.runtime_status
        self.source_event_count = self._events_processed_total

    def _restore_delta_identities(self) -> None:
        cursor_map = {(item.venue, item.event_stream): item for item in self._cursors}
        for (
            venue,
            instrument,
            channel,
            snapshot_at,
            snapshot_id,
        ) in self.book_builder.recovery_boundaries():
            cursor = cursor_map.get((venue, "orderbook_snapshot"))
            if (
                venue != "bitget"
                or cursor is None
                or cursor.last_available_at is None
                or cursor.last_event_id is None
            ):
                continue
            identities = self.repository.list_experimental_event_ids_between(
                venue=venue,
                instrument=instrument,
                event_type="orderbook_delta",
                after_available_at=snapshot_at,
                after_event_id=snapshot_id,
                through_available_at=cursor.last_available_at,
                through_event_id=cursor.last_event_id,
            )
            self.book_builder.restore_applied_delta_event_ids(
                venue=venue,
                instrument=instrument,
                channel=channel,
                event_ids=identities,
            )
            book = self._latest_books.get(venue)
            if book is not None:
                self._latest_books[venue] = replace(
                    book,
                    applied_source_event_ids=identities,
                )

    def _checkpoint(self, *, now: datetime) -> ShadowInputCheckpoint:
        return ShadowInputCheckpoint(
            run_id=self.run_id,
            state_json=self._state_json(),
            events_fetched_total=self._events_fetched_total,
            events_processed_total=self._events_processed_total,
            events_failed_total=self._events_failed_total,
            batch_count=self._batch_count,
            last_batch_size=self._last_batch_size,
            scan_duration_ms=self._scan_duration_ms,
            processing_duration_ms=self._processing_duration_ms,
            latest_db_event_available_at=self._latest_db_event_available_at,
            shadow_input_lag_seconds=self._lag_seconds,
            shadow_input_backlog_estimate=self._backlog_estimate,
            strategy_observation_first_at=self._strategy_observation_first_at,
            strategy_observation_last_at=self._strategy_observation_last_at,
            last_cursor_advanced_at=self._last_cursor_advanced_at,
            runtime_status=self.runtime_status,
            updated_at=now,
        )

    def runtime_metrics(self, *, now: datetime | None = None) -> dict[str, object]:
        evaluated_at = now or datetime.now(UTC)
        coverage = Decimal("0")
        if self._strategy_observation_first_at and self._strategy_observation_last_at:
            coverage = Decimal(
                str(
                    max(
                        0.0,
                        (
                            self._strategy_observation_last_at - self._strategy_observation_first_at
                        ).total_seconds(),
                    )
                )
            )
        run_age = max(
            0.0,
            (evaluated_at - self._started_at).total_seconds(),
        )
        return {
            "shadow_input_events_fetched_total": self._events_fetched_total,
            "shadow_input_events_processed_total": self._events_processed_total,
            "shadow_input_events_failed_total": self._events_failed_total,
            "shadow_input_batch_count": self._batch_count,
            "shadow_input_batch_size": self._last_batch_size,
            "shadow_input_configured_batch_size": self.batch_size,
            "shadow_input_scan_duration_ms": self._scan_duration_ms,
            "shadow_input_processing_duration_ms": self._processing_duration_ms,
            "shadow_input_cursor_available_at": max(
                (item.last_available_at for item in self._cursors if item.last_available_at),
                default=None,
            ),
            "latest_db_event_available_at": self._latest_db_event_available_at,
            "shadow_input_lag_seconds": self._lag_seconds,
            "shadow_input_backlog_estimate": self._backlog_estimate,
            "collector_uptime_seconds": Decimal(str(run_age)),
            "strategy_observation_first_at": self._strategy_observation_first_at,
            "strategy_observation_last_at": self._strategy_observation_last_at,
            "strategy_observation_coverage_seconds": coverage,
            "strategy_observation_coverage_ratio": (
                coverage / Decimal(str(run_age)) if run_age > 0 else Decimal("0")
            ),
            "maximum_shadow_input_stall_seconds": self.maximum_shadow_input_stall_seconds,
            "shadow_runtime_status": self.runtime_status,
            "processed_by_venue_capability": [
                {"venue": venue, "capability": capability, "count": count}
                for (venue, capability), count in sorted(self._processed_by_stream.items())
            ],
            "cursor_positions": [
                {
                    "venue": cursor.venue,
                    "instrument": cursor.instrument,
                    "event_stream": cursor.event_stream,
                    "last_available_at": cursor.last_available_at,
                    "last_event_id": cursor.last_event_id,
                    "updated_at": cursor.updated_at,
                }
                for cursor in self._cursors
            ],
        }

    def _funding(self, event: RawMarketEvent, *, now: datetime) -> LiveSignalInput | None:
        timestamp = event.exchange_timestamp or event.available_at
        payload = event.payload()
        rate = payload.get("rate") or payload.get("funding_rate")
        interval = payload.get("funding_interval_seconds")
        if rate is None:
            self._funding_invalid_reason[event.venue] = (
                FundingCarryRejectCode.MISSING_FUNDING_CURRENT
            )
            return None
        if interval is None:
            self._funding_invalid_reason[event.venue] = (
                FundingCarryRejectCode.UNKNOWN_FUNDING_INTERVAL
            )
            return None
        try:
            parsed_rate = Decimal(str(rate))
            parsed_interval = int(str(interval))
        except (InvalidOperation, TypeError, ValueError):
            self._funding_invalid_reason[event.venue] = (
                FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE
            )
            return None
        if not _finite(parsed_rate):
            self._funding_invalid_reason[event.venue] = (
                FundingCarryRejectCode.NON_FINITE_NUMERIC_VALUE
            )
            return None
        if parsed_interval <= 0:
            self._funding_invalid_reason[event.venue] = (
                FundingCarryRejectCode.UNKNOWN_FUNDING_INTERVAL
            )
            return None
        self._funding_invalid_reason.pop(event.venue, None)
        next_funding = payload.get("next_funding_at") or payload.get("next_funding_time")
        return LiveSignalInput(
            event_id=event.event_id,
            venue=event.venue,
            instrument=event.canonical_instrument_id,
            event_type="funding_current",
            available_at=event.available_at,
            data_quality_score=1.0,
            capability_support="experimental",
            reconciliation_state=None,
            funding_rate=parsed_rate,
            funding_unit="fraction_per_interval",
            funding_interval_seconds=parsed_interval,
            next_funding_at=(datetime.fromisoformat(str(next_funding)) if next_funding else None),
            source_timestamp=timestamp,
            received_at=event.received_at,
            source_table=self.SOURCE_TABLE,
            normalizer_version=event.normalizer_version,
            connection_epoch=event.connection_epoch,
            sequence=event.sequence,
            source_event_id=event.event_id,
            timestamp_semantic=self.FUNDING_TIMESTAMP_SEMANTIC,
            exchange_timestamp=timestamp,
            freshness_age_seconds=self._age_seconds(now, timestamp),
        )

    def _book(self, snapshot: CanonicalOrderBookSnapshot, *, now: datetime) -> LiveSignalInput:
        return LiveSignalInput(
            event_id=f"canonical-book-{snapshot.state_hash}",
            venue=snapshot.venue,
            instrument=snapshot.instrument,
            event_type="canonical_orderbook_snapshot",
            available_at=snapshot.available_at,
            data_quality_score=1.0,
            capability_support="experimental",
            reconciliation_state="synchronized",
            bid=snapshot.best_bid,
            ask=snapshot.best_ask,
            bid_size=snapshot.bids[0][1],
            ask_size=snapshot.asks[0][1],
            bids=snapshot.bids,
            asks=snapshot.asks,
            source_timestamp=snapshot.exchange_timestamp,
            received_at=snapshot.received_at,
            source_table=self.SOURCE_TABLE,
            normalizer_version=snapshot.normalizer_version,
            connection_epoch=snapshot.connection_epoch,
            sequence=snapshot.last_sequence,
            state_hash=snapshot.state_hash,
            source_event_id=snapshot.source_snapshot_event_id,
            applied_source_event_ids=snapshot.applied_delta_event_ids,
            timestamp_semantic=self.ORDERBOOK_TIMESTAMP_SEMANTIC,
            exchange_timestamp=snapshot.exchange_timestamp,
            freshness_age_seconds=self._age_seconds(now, snapshot.exchange_timestamp),
        )

    def _missing(self) -> MissingShadowInput | None:
        expectations = (
            (
                "hyperliquid",
                "funding_current",
                FundingCarryRejectCode.MISSING_HYPERLIQUID_FUNDING_CURRENT,
            ),
            ("bitget", "funding_current", FundingCarryRejectCode.MISSING_BITGET_FUNDING_CURRENT),
            (
                "hyperliquid",
                "orderbook_snapshot",
                FundingCarryRejectCode.MISSING_HYPERLIQUID_ORDERBOOK_SNAPSHOT,
            ),
            (
                "bitget",
                "orderbook_snapshot",
                FundingCarryRejectCode.MISSING_BITGET_ORDERBOOK_SNAPSHOT,
            ),
        )
        for venue, capability, reason in expectations:
            collection = (
                self._latest_funding if capability == "funding_current" else self._latest_books
            )
            if venue in collection:
                continue
            if capability == "funding_current":
                reason = self._funding_invalid_reason.get(venue, reason)
            if venue == "bitget" and capability == "orderbook_snapshot":
                reason = self.book_builder.last_invalid_reason.get((venue, "BTC"), reason)
            if venue == "hyperliquid" and capability == "orderbook_snapshot":
                reason = self.book_builder.last_invalid_reason.get((venue, "BTC"), reason)
            if reason is FundingCarryRejectCode.STALE_DATA and capability == "orderbook_snapshot":
                reason = (
                    FundingCarryRejectCode.HYPERLIQUID_ORDERBOOK_STALE
                    if venue == "hyperliquid"
                    else FundingCarryRejectCode.BITGET_ORDERBOOK_STALE
                )
            return MissingShadowInput(
                reason=reason,
                venue=venue,
                capability=capability,
                last_seen_event_at=self.last_seen.get((venue, capability)),
                last_valid_event_at=self.last_valid.get((venue, capability)),
                source_event_count=self.source_event_count,
            )
        return None

    def _available_events(self) -> tuple[LiveSignalInput, ...]:
        return tuple(
            item
            for venue in ("hyperliquid", "bitget")
            for item in (self._latest_funding.get(venue), self._latest_books.get(venue))
            if item is not None
        )

    @staticmethod
    def _source_event_ids(events: tuple[LiveSignalInput, ...]) -> tuple[str, ...]:
        return tuple(
            sorted(
                source_id
                for item in events
                for source_id in (
                    item.source_event_id or item.event_id,
                    *item.applied_source_event_ids,
                )
            )
        )

    def _invalid_batch(
        self,
        events: tuple[LiveSignalInput, ...],
        item: LiveSignalInput,
        capability: str,
        reason: FundingCarryRejectCode,
        *,
        timing: ShadowTimingMetrics | None = None,
    ) -> ShadowInputBatch:
        missing = MissingShadowInput(
            reason=reason,
            venue=item.venue,
            capability=capability,
            last_seen_event_at=self.last_seen.get((item.venue, capability)),
            last_valid_event_at=self.last_valid.get((item.venue, capability)),
            source_event_count=self.source_event_count,
        )
        return ShadowInputBatch(events, False, None, False, missing, timing=timing)

    @staticmethod
    def _age_seconds(now: datetime, timestamp: datetime) -> Decimal:
        return Decimal(str((now - timestamp).total_seconds()))

    def _with_freshness(self, item: LiveSignalInput, *, now: datetime) -> LiveSignalInput:
        timestamp = item.exchange_timestamp or item.available_at
        return replace(
            item,
            exchange_timestamp=timestamp,
            freshness_age_seconds=self._age_seconds(now, timestamp),
        )

    @staticmethod
    def _semantic_event(
        events: tuple[LiveSignalInput, ...], venue: str, semantic: str
    ) -> LiveSignalInput:
        return next(
            item for item in events if item.venue == venue and item.timestamp_semantic == semantic
        )

    def _timing_metrics(self, events: tuple[LiveSignalInput, ...]) -> ShadowTimingMetrics:
        hl_funding = self._semantic_event(events, "hyperliquid", self.FUNDING_TIMESTAMP_SEMANTIC)
        bg_funding = self._semantic_event(events, "bitget", self.FUNDING_TIMESTAMP_SEMANTIC)
        hl_book = self._semantic_event(events, "hyperliquid", self.ORDERBOOK_TIMESTAMP_SEMANTIC)
        bg_book = self._semantic_event(events, "bitget", self.ORDERBOOK_TIMESTAMP_SEMANTIC)
        hl_funding_age = hl_funding.freshness_age_seconds or Decimal("0")
        bg_funding_age = bg_funding.freshness_age_seconds or Decimal("0")
        hl_book_age = hl_book.freshness_age_seconds or Decimal("0")
        bg_book_age = bg_book.freshness_age_seconds or Decimal("0")
        funding_skew = abs(
            Decimal(
                str(
                    (
                        (hl_funding.exchange_timestamp or hl_funding.available_at)
                        - (bg_funding.exchange_timestamp or bg_funding.available_at)
                    ).total_seconds()
                )
            )
        )
        book_skew = abs(
            Decimal(
                str(
                    (
                        (hl_book.exchange_timestamp or hl_book.available_at)
                        - (bg_book.exchange_timestamp or bg_book.available_at)
                    ).total_seconds()
                )
            )
        )
        return ShadowTimingMetrics(
            hyperliquid_funding_age_seconds=hl_funding_age,
            bitget_funding_age_seconds=bg_funding_age,
            funding_observation_skew_seconds=funding_skew,
            hyperliquid_orderbook_age_seconds=hl_book_age,
            bitget_orderbook_age_seconds=bg_book_age,
            orderbook_venue_skew_seconds=book_skew,
            funding_freshness_pass=(
                hl_funding_age <= Decimal(self.funding_max_age_seconds)
                and bg_funding_age <= Decimal(self.funding_max_age_seconds)
                and funding_skew <= self.maximum_funding_skew
            ),
            orderbook_freshness_pass=(
                hl_book_age <= Decimal(str(self.maximum_orderbook_age.total_seconds()))
                and bg_book_age <= Decimal(str(self.maximum_orderbook_age.total_seconds()))
            ),
            orderbook_synchronization_pass=book_skew <= self.maximum_orderbook_skew,
        )

    def _timing_rejection(
        self,
        events: tuple[LiveSignalInput, ...],
        timing: ShadowTimingMetrics,
    ) -> tuple[LiveSignalInput, str, FundingCarryRejectCode] | None:
        hl_funding = self._semantic_event(events, "hyperliquid", self.FUNDING_TIMESTAMP_SEMANTIC)
        bg_funding = self._semantic_event(events, "bitget", self.FUNDING_TIMESTAMP_SEMANTIC)
        hl_book = self._semantic_event(events, "hyperliquid", self.ORDERBOOK_TIMESTAMP_SEMANTIC)
        bg_book = self._semantic_event(events, "bitget", self.ORDERBOOK_TIMESTAMP_SEMANTIC)
        maximum_funding_age = Decimal(self.funding_max_age_seconds)
        maximum_book_age = Decimal(str(self.maximum_orderbook_age.total_seconds()))
        if timing.hyperliquid_funding_age_seconds > maximum_funding_age:
            return (
                hl_funding,
                "funding_current",
                FundingCarryRejectCode.HYPERLIQUID_FUNDING_STALE,
            )
        if timing.bitget_funding_age_seconds > maximum_funding_age:
            return (
                bg_funding,
                "funding_current",
                FundingCarryRejectCode.BITGET_FUNDING_STALE,
            )
        if timing.funding_observation_skew_seconds > self.maximum_funding_skew:
            return (
                hl_funding,
                "funding_observation_synchronization",
                FundingCarryRejectCode.FUNDING_OBSERVATION_UNSYNCHRONIZED,
            )
        if timing.hyperliquid_orderbook_age_seconds > maximum_book_age:
            return (
                hl_book,
                "orderbook_snapshot",
                FundingCarryRejectCode.HYPERLIQUID_ORDERBOOK_STALE,
            )
        if timing.bitget_orderbook_age_seconds > maximum_book_age:
            return (
                bg_book,
                "orderbook_snapshot",
                FundingCarryRejectCode.BITGET_ORDERBOOK_STALE,
            )
        if timing.orderbook_venue_skew_seconds > self.maximum_orderbook_skew:
            return (
                hl_book,
                "orderbook_synchronization",
                FundingCarryRejectCode.ORDERBOOK_VENUES_UNSYNCHRONIZED,
            )
        return None
