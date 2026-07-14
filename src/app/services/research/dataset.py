from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from itertools import pairwise
from typing import Any
from uuid import UUID

from app.adapters.exchanges.websocket import ReconciliationState
from app.services.research.data_operations import DataSnapshotService
from app.services.research.models import (
    DataQualityResult,
    PointInTimeDataset,
    PointInTimeValue,
    RawMarketEvent,
    canonical_sha256,
    utc,
)
from app.services.research.repository import ResearchRepository


class PointInTimeDatasetBuilder:
    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def build(
        self,
        *,
        snapshot_id: str,
        cutoff_at: datetime,
        instruments: tuple[str, ...],
        venues: tuple[str, ...],
        event_types: tuple[str, ...],
    ) -> PointInTimeDataset:
        cutoff = utc(cutoff_at, "cutoff_at")
        selected: list[PointInTimeValue] = []
        excluded: list[str] = []
        delisted: list[str] = []
        outages: list[str] = []
        hash_records: list[tuple[object, ...]] = []
        previous_sequences: dict[tuple[str, str, str, object], int] = {}
        finalized = self.repository.snapshot_manifest(snapshot_id)
        if finalized is not None and finalized.cutoff_at != cutoff:
            raise ValueError("requested cutoff does not match finalized snapshot")
        if finalized is not None and finalized.eligibility_status != "FINALIZED_RESEARCH_ELIGIBLE":
            raise ValueError("data snapshot is finalized but not research eligible")
        source_events = (
            self.repository.snapshot_events(snapshot_id)
            if finalized is not None
            else self.repository.raw_events()
        )
        for event in sorted(
            source_events,
            key=lambda item: (item.available_at, item.event_id),
        ):
            if (
                event.canonical_instrument_id not in instruments
                or event.venue not in venues
                or event.event_type not in event_types
            ):
                continue
            if event.available_at > cutoff:
                excluded.append(event.event_id)
                continue
            try:
                payload = event.payload()
                _validate_payload(event.event_type, payload)
            except (ValueError, TypeError) as exc:
                self.repository.quarantine(event, f"normalization failure: {exc}", cutoff)
                continue
            if (
                event.exchange_timestamp is not None
                and event.exchange_timestamp > event.received_at
            ):
                self.repository.quarantine(event, "abnormal future exchange timestamp", cutoff)
                continue
            if event.received_at > event.available_at:
                self.repository.quarantine(event, "available_at precedes received_at", cutoff)
                continue
            if event.sequence is not None:
                sequence_key = (
                    event.venue,
                    event.canonical_instrument_id,
                    event.event_type,
                    event.connection_id,
                )
                previous = previous_sequences.get(sequence_key)
                previous_sequences[sequence_key] = event.sequence
                if previous is not None and event.sequence != previous + 1:
                    reason = (
                        "sequence gap" if event.sequence > previous + 1 else "out-of-order sequence"
                    )
                    self.repository.quarantine(event, reason, cutoff)
                    continue
            value = PointInTimeValue(
                event_id=event.event_id,
                venue=event.venue,
                canonical_instrument_id=event.canonical_instrument_id,
                venue_symbol=event.venue_symbol,
                event_type=event.event_type,
                exchange_timestamp=event.exchange_timestamp,
                received_at=event.received_at,
                available_at=event.available_at,
                payload=payload,
                sequence=event.sequence,
            )
            value.require_available(cutoff)
            selected.append(value)
            hash_records.append(
                (
                    event.event_id,
                    event.venue,
                    event.canonical_instrument_id,
                    event.event_type,
                    event.exchange_timestamp,
                    event.received_at,
                    event.available_at,
                    event.sequence,
                    event.connection_id,
                    event.reconciliation_state,
                    event.payload_sha256,
                    event.normalizer_version,
                    event.capability_verification_run_id,
                )
            )
            if bool(payload.get("delisted")):
                delisted.append(event.event_id)
            if event.event_type in {"venue_outage", "websocket_disconnect"} or bool(
                payload.get("outage")
            ):
                outages.append(event.event_id)
        content_hash = canonical_sha256(hash_records)
        if finalized is None:
            finalized = DataSnapshotService(self.repository).finalize(
                cutoff_at=cutoff,
                snapshot_id=snapshot_id,
                finalized_at=cutoff,
            )
            if finalized.eligibility_status != "FINALIZED_RESEARCH_ELIGIBLE":
                raise ValueError("data snapshot is finalized but not research eligible")
        return PointInTimeDataset(
            snapshot_id=snapshot_id,
            cutoff_at=cutoff,
            instruments=instruments,
            venues=venues,
            event_types=event_types,
            values=tuple(selected),
            excluded_future_event_ids=tuple(excluded),
            retained_delisted_event_ids=tuple(delisted),
            retained_outage_event_ids=tuple(outages),
            content_sha256=content_hash,
        )


def evaluate_data_quality(
    *,
    run_id: str,
    dataset: PointInTimeDataset,
    quarantine_count: int,
    minimum_coverage: Decimal = Decimal("0.80"),
    maximum_stale_ratio: Decimal = Decimal("0.20"),
    maximum_quarantine_ratio: Decimal = Decimal("0.05"),
    maximum_duplicate_ratio: Decimal = Decimal("0"),
    maximum_clock_skew_seconds: Decimal = Decimal("300"),
    maximum_cross_venue_divergence: Decimal = Decimal("0.20"),
    minimum_book_depth_availability: Decimal = Decimal("0.80"),
    maximum_outage_duration_seconds: Decimal = Decimal("0"),
) -> DataQualityResult:
    values = dataset.values
    expected = max(1, len(dataset.instruments) * len(dataset.venues) * len(dataset.event_types))
    keys = {(item.venue, item.canonical_instrument_id, item.event_type) for item in values}
    coverage = min(Decimal(1), Decimal(len(keys)) / Decimal(expected))
    event_ids = [item.event_id for item in values]
    duplicate_ratio = Decimal(len(event_ids) - len(set(event_ids))) / Decimal(
        max(1, len(event_ids))
    )
    stale_count = sum(
        1 for item in values if (dataset.cutoff_at - item.available_at).total_seconds() > 3600
    )
    stale_ratio = Decimal(stale_count) / Decimal(max(1, len(values)))
    sequences: dict[tuple[str, str], list[int]] = {}
    out_of_order = 0
    for item in values:
        if item.sequence is not None:
            sequences.setdefault((item.venue, item.canonical_instrument_id), []).append(
                item.sequence
            )
    gap_count = 0
    for sequence_values in sequences.values():
        gap_count += sum(
            max(0, current - previous - 1) for previous, current in pairwise(sequence_values)
        )
        out_of_order += sum(current <= previous for previous, current in pairwise(sequence_values))
    quarantine_ratio = Decimal(quarantine_count) / Decimal(max(1, len(values) + quarantine_count))
    outage_seconds = sum(
        (
            Decimal(str(item.payload.get("duration_seconds", 0)))
            for item in values
            if item.event_type in {"venue_outage", "websocket_disconnect"}
            or bool(item.payload.get("outage"))
        ),
        start=Decimal(0),
    )
    skews = [
        Decimal(str(abs((item.received_at - item.exchange_timestamp).total_seconds())))
        for item in values
        if item.exchange_timestamp is not None
    ]
    maximum_skew = max(skews, default=Decimal(0))
    book_values = [item for item in values if item.event_type == "orderbook_snapshot"]
    book_available = Decimal(
        sum(
            bool(item.payload.get("bid_depth")) and bool(item.payload.get("ask_depth"))
            for item in book_values
        )
    ) / Decimal(max(1, len(book_values)))
    funding_keys = {
        (item.venue, item.canonical_instrument_id)
        for item in values
        if item.event_type in {"funding_rate", "funding_current", "funding_history"}
    }
    oi_keys = {
        (item.venue, item.canonical_instrument_id)
        for item in values
        if item.event_type == "open_interest"
    }
    expected_windows = len(dataset.venues) * len(dataset.instruments)
    prices: dict[tuple[str, datetime], list[Decimal]] = {}
    for item in book_values:
        bid = Decimal(str(item.payload.get("bid", 0)))
        ask = Decimal(str(item.payload.get("ask", 0)))
        if bid > 0 and ask > bid:
            prices.setdefault((item.canonical_instrument_id, item.available_at), []).append(
                (bid + ask) / 2
            )
    divergences = [
        (max(items) - min(items)) / min(items)
        for items in prices.values()
        if len(items) > 1 and min(items) > 0
    ]
    divergence = max(divergences, default=Decimal(0))
    reasons: list[str] = []
    if coverage < minimum_coverage:
        reasons.append("coverage below threshold")
    if stale_ratio > maximum_stale_ratio:
        reasons.append("stale ratio above threshold")
    if quarantine_ratio > maximum_quarantine_ratio:
        reasons.append("quarantine ratio above threshold")
    if duplicate_ratio > maximum_duplicate_ratio:
        reasons.append("duplicate ratio above threshold")
    if gap_count:
        reasons.append("sequence gaps detected")
    if out_of_order:
        reasons.append("out-of-order events detected")
    if quarantine_count:
        reasons.append("quarantined events present")
    if outage_seconds > maximum_outage_duration_seconds:
        reasons.append("venue outage duration above threshold")
    if maximum_skew > maximum_clock_skew_seconds:
        reasons.append("clock skew above threshold")
    if divergence > maximum_cross_venue_divergence:
        reasons.append("cross-venue divergence above threshold")
    if (
        any(
            name in dataset.event_types
            for name in ("funding_rate", "funding_current", "funding_history")
        )
        and len(funding_keys) < expected_windows
    ):
        reasons.append("missing funding windows")
    if "open_interest" in dataset.event_types and len(oi_keys) < expected_windows:
        reasons.append("missing open-interest windows")
    if (
        "orderbook_snapshot" in dataset.event_types
        and book_available < minimum_book_depth_availability
    ):
        reasons.append("book depth availability below threshold")
    if not values:
        reasons.append("dataset is empty")
    return DataQualityResult(
        run_id=run_id,
        data_snapshot_id=dataset.snapshot_id,
        passed=not reasons,
        coverage_ratio=coverage,
        stale_ratio=stale_ratio,
        duplicate_ratio=duplicate_ratio,
        sequence_gap_count=gap_count,
        out_of_order_count=out_of_order,
        quarantine_ratio=quarantine_ratio,
        venue_outage_duration_seconds=outage_seconds,
        maximum_clock_skew_seconds=maximum_skew,
        cross_venue_divergence=divergence,
        missing_funding_windows=max(0, expected_windows - len(funding_keys)),
        missing_oi_windows=max(0, expected_windows - len(oi_keys)),
        book_depth_availability=book_available,
        reasons=tuple(reasons),
    )


def _validate_payload(event_type: str, payload: dict[str, Any]) -> None:
    if event_type == "orderbook_snapshot":
        bid = Decimal(str(payload["bid"]))
        ask = Decimal(str(payload["ask"]))
        bid_depth = Decimal(str(payload["bid_depth"]))
        ask_depth = Decimal(str(payload["ask_depth"]))
        if bid <= 0 or ask <= bid or min(bid_depth, ask_depth) < 0:
            raise ValueError("invalid orderbook schema")
    elif event_type in {"funding_rate", "funding_current", "funding_history"}:
        Decimal(str(payload["rate"]))
        if Decimal(str(payload["mark_price"])) <= 0:
            raise ValueError("invalid funding schema")
    elif event_type == "open_interest" and Decimal(str(payload["value"])) < 0:
        raise ValueError("invalid open-interest schema")


def raw_event_from_dict(value: dict[str, Any]) -> RawMarketEvent:
    def timestamp(name: str, optional: bool = False) -> datetime | None:
        raw = value.get(name)
        if raw is None and optional:
            return None
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return parsed.astimezone(UTC)

    raw_payload = value.get("raw_payload")
    if not isinstance(raw_payload, str):
        import json

        raw_payload = json.dumps(raw_payload or {}, sort_keys=True, separators=(",", ":"))
    import hashlib

    return RawMarketEvent(
        event_id=str(value["event_id"]),
        venue=str(value["venue"]),
        canonical_instrument_id=str(value["canonical_instrument_id"]),
        venue_symbol=str(value.get("venue_symbol", value["canonical_instrument_id"])),
        event_type=str(value["event_type"]),
        exchange_timestamp=timestamp("exchange_timestamp", optional=True),
        received_at=timestamp("received_at") or datetime.now(UTC),
        available_at=timestamp("available_at") or datetime.now(UTC),
        sequence=int(value["sequence"]) if value.get("sequence") is not None else None,
        connection_id=UUID(str(value["connection_id"])) if value.get("connection_id") else None,
        reconciliation_state=(
            ReconciliationState(str(value["reconciliation_state"]))
            if value.get("reconciliation_state")
            else None
        ),
        payload_sha256=hashlib.sha256(raw_payload.encode()).hexdigest(),
        raw_payload=raw_payload,
        normalizer_version=str(value.get("normalizer_version", "r1")),
        capability_verification_run_id=str(
            value.get("capability_verification_run_id", "research-fixture")
        ),
        created_at=timestamp("created_at", optional=True)
        or timestamp("available_at")
        or datetime.now(UTC),
    )
