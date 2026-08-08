from __future__ import annotations

import hashlib
import json
import math
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol

from app.domain.strategies.funding_carry import FundingCarryRejectCode
from app.services.operations.models import LiveSignalInput
from app.services.research.models import RawMarketEvent


class ExperimentalEventRepository(Protocol):
    def list_experimental_events(self) -> tuple[RawMarketEvent, ...]: ...


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
class ShadowInputBatch:
    events: tuple[LiveSignalInput, ...]
    matched: bool
    pair_identity: str | None
    source_pair_duplicate: bool
    missing: MissingShadowInput | None = None
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

    def __init__(
        self,
        repository: ExperimentalEventRepository,
        *,
        maximum_age_seconds: int = 30,
        maximum_future_seconds: int = 1,
        maximum_venue_timestamp_skew_seconds: int = 5,
    ) -> None:
        self.repository = repository
        self.maximum_age = timedelta(seconds=maximum_age_seconds)
        self.maximum_future = timedelta(seconds=maximum_future_seconds)
        self.maximum_skew = Decimal(maximum_venue_timestamp_skew_seconds)
        self.book_builder = CanonicalOrderBookStateBuilder(
            maximum_age_seconds=maximum_age_seconds,
            maximum_future_seconds=maximum_future_seconds,
        )
        self._processed_book_events: set[str] = set()
        self._latest_funding: dict[str, LiveSignalInput] = {}
        self._latest_books: dict[str, LiveSignalInput] = {}
        self._last_pair_identity: str | None = None
        self.source_event_count = 0
        self.last_seen: dict[tuple[str, str], datetime] = {}
        self.last_valid: dict[tuple[str, str], datetime] = {}
        self._funding_invalid_reason: dict[str, FundingCarryRejectCode] = {}

    def read(self, *, now: datetime) -> ShadowInputBatch:
        events = tuple(
            event
            for event in self.repository.list_experimental_events()
            if event.venue in {"hyperliquid", "bitget"}
            and event.canonical_instrument_id == "BTC"
            and event.event_type in {"funding_current", "orderbook_snapshot", "orderbook_delta"}
        )
        self.source_event_count = len(events)
        for event in sorted(events, key=lambda item: (item.available_at, item.event_id)):
            capability = (
                "funding_current" if event.event_type == "funding_current" else "orderbook_snapshot"
            )
            self.last_seen[(event.venue, capability)] = event.available_at
            if event.event_type == "funding_current":
                parsed = self._funding(event, now=now)
                if parsed is not None and (
                    event.venue not in self._latest_funding
                    or parsed.available_at >= self._latest_funding[event.venue].available_at
                ):
                    self._latest_funding[event.venue] = parsed
                    self.last_valid[(event.venue, capability)] = event.available_at
                elif parsed is None:
                    self._latest_funding.pop(event.venue, None)
                continue
            if event.event_id in self._processed_book_events:
                continue
            self._processed_book_events.add(event.event_id)
            snapshot = self.book_builder.apply(event, now=now)
            if snapshot is not None:
                parsed_book = self._book(snapshot)
                self._latest_books[event.venue] = parsed_book
                self.last_valid[(event.venue, capability)] = event.available_at
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

        missing = self._missing()
        if missing is not None:
            return ShadowInputBatch(self._available_events(), False, None, False, missing)
        paired = (
            self._latest_funding["hyperliquid"],
            self._latest_books["hyperliquid"],
            self._latest_funding["bitget"],
            self._latest_books["bitget"],
        )
        timestamps = tuple((item.source_timestamp or item.available_at) for item in paired)
        for item, timestamp in zip(paired, timestamps, strict=True):
            capability = (
                "funding_current" if item.event_type == "funding_current" else "orderbook_snapshot"
            )
            if timestamp > now + self.maximum_future:
                return self._invalid_batch(
                    paired, item, capability, FundingCarryRejectCode.FUTURE_TIMESTAMP
                )
            if now - timestamp > self.maximum_age:
                return self._invalid_batch(
                    paired, item, capability, FundingCarryRejectCode.STALE_DATA
                )
        skew = Decimal(str((max(timestamps) - min(timestamps)).total_seconds()))
        if skew > self.maximum_skew:
            missing = MissingShadowInput(
                reason=FundingCarryRejectCode.UNSYNCHRONIZED_VENUE_TIMESTAMPS,
                venue="cross_venue",
                capability="timestamp_synchronization",
                last_seen_event_at=max(timestamps),
                last_valid_event_at=None,
                source_event_count=self.source_event_count,
            )
            return ShadowInputBatch(
                paired, False, None, False, missing, venue_timestamp_skew_seconds=skew
            )
        pair_identity = hashlib.sha256(
            json.dumps(self._source_event_ids(paired), separators=(",", ":")).encode()
        ).hexdigest()
        duplicate = pair_identity == self._last_pair_identity
        self._last_pair_identity = pair_identity
        return ShadowInputBatch(
            paired, True, pair_identity, duplicate, venue_timestamp_skew_seconds=skew
        )

    def _funding(self, event: RawMarketEvent, *, now: datetime) -> LiveSignalInput | None:
        timestamp = event.exchange_timestamp or event.available_at
        if timestamp > now + self.maximum_future:
            self._funding_invalid_reason[event.venue] = FundingCarryRejectCode.FUTURE_TIMESTAMP
            return None
        if now - timestamp > self.maximum_age:
            self._funding_invalid_reason[event.venue] = FundingCarryRejectCode.STALE_DATA
            return None
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
        )

    def _book(self, snapshot: CanonicalOrderBookSnapshot) -> LiveSignalInput:
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
    ) -> ShadowInputBatch:
        missing = MissingShadowInput(
            reason=reason,
            venue=item.venue,
            capability=capability,
            last_seen_event_at=self.last_seen.get((item.venue, capability)),
            last_valid_event_at=self.last_valid.get((item.venue, capability)),
            source_event_count=self.source_event_count,
        )
        return ShadowInputBatch(events, False, None, False, missing)
