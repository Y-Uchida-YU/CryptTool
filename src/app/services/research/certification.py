from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from statistics import median
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.adapters.exchanges.websocket import ReconciliationState
from app.domain.venues.models import CapabilitySupport
from app.domain.venues.trusted_capabilities import TrustedCapabilityRecord
from app.infrastructure.database.models import CapabilityPromotionRow, MarketDataCertificationRow
from app.services.research.data_operations import DataSnapshotService, SnapshotEligibilityPolicy
from app.services.research.models import DataSnapshotManifest, RawMarketEvent, canonical_sha256, utc
from app.services.research.repository import ResearchRepository

TIER_ONE_CAPABILITIES = (
    "funding_current",
    "funding_history",
    "mark_price",
    "index_price",
    "open_interest",
    "trade",
    "ohlcv",
)
SUPPORTED_CERTIFICATION_VENUES = ("hyperliquid", "bitget")
SUPPORTED_CERTIFICATION_INSTRUMENTS = ("BTC", "ETH", "SOL", "HYPE")
EVENT_CAPABILITY = {
    "funding_current": "funding_current",
    "funding_history": "funding_history",
    "mark_price": "mark_price",
    "index_price": "index_price",
    "open_interest": "open_interest",
    "trade": "trade",
    "ohlcv": "ohlcv",
    "orderbook_snapshot": "orderbook_snapshot",
    "orderbook_delta": "orderbook_delta",
}


class CertificationVerdict(StrEnum):
    PASS = "pass"  # nosec B105
    FAIL = "fail"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class StrictPaperReadiness(StrEnum):
    NOT_READY = "not_ready"
    READY_FOR_OPERATOR_APPROVAL = "ready_for_operator_approval"


@dataclass(frozen=True)
class MarketDataCertification:
    certification_id: str
    venue: str
    capability: str
    canonical_instrument_id: str
    adapter_version: str
    commit_sha: str
    source_version: str
    contract_fixture_sha256: str
    sample_start: datetime
    sample_end: datetime
    event_count: int
    verdict: CertificationVerdict
    verified_at: datetime
    expires_at: datetime
    evidence_manifest_sha256: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.venue not in SUPPORTED_CERTIFICATION_VENUES:
            raise ValueError("certification venue is outside the R4 allowlist")
        if self.canonical_instrument_id not in SUPPORTED_CERTIFICATION_INSTRUMENTS:
            raise ValueError("certification instrument is outside the R4 allowlist")
        if len(self.commit_sha) != 40 or len(self.contract_fixture_sha256) != 64:
            raise ValueError("certification source hashes are invalid")
        if len(self.evidence_manifest_sha256) != 64:
            raise ValueError("evidence manifest hash is invalid")
        for name in ("sample_start", "sample_end", "verified_at", "expires_at"):
            object.__setattr__(self, name, utc(getattr(self, name), name))
        if not self.sample_start <= self.sample_end <= self.verified_at <= self.expires_at:
            raise ValueError("certification timestamps are not monotonic")


@dataclass(frozen=True)
class CertificationMetrics:
    event_count: int
    coverage_ratio: Decimal
    missing_interval_count: int
    duplicate_ratio: Decimal
    stale_ratio: Decimal
    out_of_order_count: int
    sequence_gap_count: int
    median_latency_ms: Decimal | None
    p95_latency_ms: Decimal | None
    maximum_latency_ms: Decimal | None
    maximum_clock_skew_ms: Decimal | None
    cross_source_absolute_error: Decimal | None
    cross_source_relative_error: Decimal | None


@dataclass(frozen=True)
class ContractValidationSpec:
    venue: str
    capability: str
    canonical_instrument_id: str
    source_endpoint: str
    request_parameters: tuple[str, ...]
    response_fields: tuple[str, ...]
    symbol: str
    price_unit: str
    quantity_unit: str
    funding_unit: str | None
    funding_interval_seconds: int | None
    timestamp_unit: str
    timestamp_timezone: str
    sequence_semantics: str
    snapshot_delta_semantics: str
    null_behavior: str
    rate_limit_behavior: str
    error_behavior: str
    fixture_path: str
    fixture_sha256: str
    normalization_test_node_id: str
    normalization_test_passed: bool
    minimum_event_count: int = 1
    minimum_coverage_ratio: Decimal = Decimal("0.80")
    maximum_stale_ratio: Decimal = Decimal("0.05")
    maximum_clock_skew_ms: Decimal = Decimal("5000")
    maximum_relative_error: Decimal = Decimal("0.01")
    maximum_absolute_error: Decimal | None = None

    def validate(self, root: Path) -> tuple[str, ...]:
        reasons: list[str] = []
        required_text = (
            self.source_endpoint,
            self.symbol,
            self.price_unit,
            self.quantity_unit,
            self.timestamp_unit,
            self.timestamp_timezone,
            self.sequence_semantics,
            self.snapshot_delta_semantics,
            self.null_behavior,
            self.rate_limit_behavior,
            self.error_behavior,
            self.normalization_test_node_id,
        )
        if not all(required_text) or not self.request_parameters or not self.response_fields:
            reasons.append("contract metadata is incomplete")
        fixture = root / self.fixture_path
        if not fixture.is_file():
            reasons.append("contract fixture is missing")
        elif hashlib.sha256(fixture.read_bytes()).hexdigest() != self.fixture_sha256:
            reasons.append("contract fixture SHA-256 mismatch")
        if not self.normalization_test_passed:
            reasons.append("normalization test did not pass")
        return tuple(reasons)


@dataclass(frozen=True)
class CertificationEvidence:
    certification_id: str
    metrics: CertificationMetrics
    event_manifest: tuple[tuple[str, str], ...]
    contract_passed: bool
    normalization_passed: bool
    live_smoke_passed: bool
    reconciliation_passed: bool
    audit_passed: bool
    audit_run_id: str
    ci_run_id: str
    manifest_sha256: str


@dataclass(frozen=True)
class StrategyDataRequirement:
    strategy_id: str
    required_capabilities: tuple[str, ...]
    required_venues: tuple[str, ...]
    minimum_coverage_ratio: Decimal
    maximum_stale_ratio: Decimal
    minimum_history_windows: int


FUNDING_CARRY_REQUIREMENT = StrategyDataRequirement(
    strategy_id="funding_carry",
    required_capabilities=TIER_ONE_CAPABILITIES,
    required_venues=SUPPORTED_CERTIFICATION_VENUES,
    minimum_coverage_ratio=Decimal("0.80"),
    maximum_stale_ratio=Decimal("0.05"),
    minimum_history_windows=2,
)


class CertificationRepository(Protocol):
    def save(
        self, certification: MarketDataCertification, evidence: CertificationEvidence
    ) -> None: ...

    def get(
        self, certification_id: str
    ) -> tuple[MarketDataCertification, CertificationEvidence] | None: ...

    def list(self) -> tuple[MarketDataCertification, ...]: ...

    def save_promotion(
        self, certification_id: str, instrument: str, record: TrustedCapabilityRecord
    ) -> None: ...


class InMemoryCertificationRepository:
    def __init__(self) -> None:
        self.records: dict[str, tuple[MarketDataCertification, CertificationEvidence]] = {}
        self.promotions: dict[str, TrustedCapabilityRecord] = {}

    def save(self, certification: MarketDataCertification, evidence: CertificationEvidence) -> None:
        if evidence.certification_id != certification.certification_id:
            raise ValueError("certification evidence identity mismatch")
        current = self.records.get(certification.certification_id)
        value = (certification, evidence)
        if current is not None and current != value:
            raise ValueError("certification is immutable")
        self.records[certification.certification_id] = value

    def get(
        self, certification_id: str
    ) -> tuple[MarketDataCertification, CertificationEvidence] | None:
        return self.records.get(certification_id)

    def list(self) -> tuple[MarketDataCertification, ...]:
        return tuple(item[0] for item in self.records.values())

    def save_promotion(
        self, certification_id: str, instrument: str, record: TrustedCapabilityRecord
    ) -> None:
        if record.canonical_instrument_id != instrument:
            raise ValueError("promotion instrument mismatch")
        current = self.promotions.get(certification_id)
        if current is not None and current != record:
            raise ValueError("promotion is immutable")
        self.promotions[certification_id] = record


class SQLCertificationRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save(self, certification: MarketDataCertification, evidence: CertificationEvidence) -> None:
        if evidence.certification_id != certification.certification_id:
            raise ValueError("certification evidence identity mismatch")
        with Session(self.engine) as session:
            current = session.get(MarketDataCertificationRow, certification.certification_id)
            payload = _json(asdict(certification))
            evidence_payload = _json(asdict(evidence))
            if current is not None:
                if current.payload_json != payload or current.evidence_json != evidence_payload:
                    raise ValueError("certification is immutable")
                return
            session.add(
                MarketDataCertificationRow(
                    certification_id=certification.certification_id,
                    venue=certification.venue,
                    capability=certification.capability,
                    canonical_instrument_id=certification.canonical_instrument_id,
                    verdict=certification.verdict.value,
                    commit_sha=certification.commit_sha,
                    adapter_version=certification.adapter_version,
                    verified_at=certification.verified_at,
                    expires_at=certification.expires_at,
                    evidence_manifest_sha256=certification.evidence_manifest_sha256,
                    payload_json=payload,
                    evidence_json=evidence_payload,
                    created_at=certification.verified_at,
                )
            )
            session.commit()

    def get(
        self, certification_id: str
    ) -> tuple[MarketDataCertification, CertificationEvidence] | None:
        with Session(self.engine) as session:
            row = session.get(MarketDataCertificationRow, certification_id)
            if row is None:
                return None
            return _certification(row.payload_json), _evidence(row.evidence_json)

    def list(self) -> tuple[MarketDataCertification, ...]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(MarketDataCertificationRow).order_by(
                    MarketDataCertificationRow.venue,
                    MarketDataCertificationRow.canonical_instrument_id,
                    MarketDataCertificationRow.capability,
                )
            ).all()
            return tuple(_certification(item.payload_json) for item in rows)

    def save_promotion(
        self, certification_id: str, instrument: str, record: TrustedCapabilityRecord
    ) -> None:
        if record.canonical_instrument_id != instrument:
            raise ValueError("promotion instrument mismatch")
        with Session(self.engine) as session:
            current = session.get(CapabilityPromotionRow, certification_id)
            payload = _json(asdict(record))
            if current is not None:
                if current.payload_json != payload:
                    raise ValueError("promotion is immutable")
                return
            session.add(
                CapabilityPromotionRow(
                    certification_id=certification_id,
                    venue=record.venue,
                    capability=record.capability,
                    canonical_instrument_id=instrument,
                    verification_run_id=record.verification_run_id,
                    expires_at=record.expires_at,
                    payload_json=payload,
                    created_at=record.verified_at,
                )
            )
            session.commit()


class MarketDataCertificationService:
    def __init__(
        self,
        repository: CertificationRepository,
        *,
        root: Path,
        commit_sha: str,
        adapter_version: str,
        source_version: str,
        certification_ttl: timedelta = timedelta(hours=24),
    ) -> None:
        self.repository = repository
        self.root = root
        self.commit_sha = commit_sha
        self.adapter_version = adapter_version
        self.source_version = source_version
        self.certification_ttl = certification_ttl

    def certify(
        self,
        *,
        certification_id: str,
        spec: ContractValidationSpec,
        events: Sequence[RawMarketEvent],
        sample_start: datetime,
        sample_end: datetime,
        cross_source_pairs: Sequence[tuple[Decimal, Decimal]] = (),
        audit_passed: bool = False,
        audit_run_id: str = "",
        ci_run_id: str = "",
    ) -> MarketDataCertification:
        start = utc(sample_start, "sample_start")
        end = utc(sample_end, "sample_end")
        selected = tuple(
            item
            for item in events
            if item.venue == spec.venue
            and item.canonical_instrument_id == spec.canonical_instrument_id
            and EVENT_CAPABILITY.get(item.event_type) == spec.capability
            and start <= item.received_at <= end
        )
        reasons = list(spec.validate(self.root))
        event_reasons = self._validate_events(selected, spec, end)
        reasons.extend(event_reasons)
        metrics = certification_metrics(selected, spec, start, end, cross_source_pairs)
        reconciliation_required = spec.capability in {
            "funding_current",
            "mark_price",
            "ohlcv",
        }
        reconciliation_missing = reconciliation_required and not cross_source_pairs
        if reconciliation_missing:
            reasons.append("cross-source reconciliation evidence is missing")
        if metrics.event_count < spec.minimum_event_count:
            reasons.append("minimum event count not reached")
        if metrics.coverage_ratio < spec.minimum_coverage_ratio:
            reasons.append("coverage ratio below threshold")
        if metrics.stale_ratio > spec.maximum_stale_ratio:
            reasons.append("stale ratio above threshold")
        if (
            metrics.maximum_clock_skew_ms is not None
            and metrics.maximum_clock_skew_ms > spec.maximum_clock_skew_ms
        ):
            reasons.append("clock skew above threshold")
        if (
            metrics.cross_source_relative_error is not None
            and metrics.cross_source_relative_error > spec.maximum_relative_error
            and spec.maximum_absolute_error is None
        ):
            reasons.append("cross-source relative error above threshold")
        if (
            metrics.cross_source_absolute_error is not None
            and spec.maximum_absolute_error is not None
            and metrics.cross_source_absolute_error > spec.maximum_absolute_error
        ):
            reasons.append("cross-source absolute error above threshold")
        if spec.capability == "funding_current":
            reasons.extend(validate_funding_current(selected, spec))
        if spec.capability.startswith("funding"):
            reasons.extend(validate_funding_series(selected, spec))
        if spec.capability in {"orderbook_snapshot", "orderbook_delta"}:
            reasons.extend(validate_order_book_events(selected, spec))
        manifest = tuple(sorted((item.event_id, item.payload_sha256) for item in selected))
        manifest_hash = canonical_sha256(
            {
                "certification_id": certification_id,
                "spec": asdict(spec),
                "events": manifest,
                "metrics": asdict(metrics),
                "commit_sha": self.commit_sha,
                "adapter_version": self.adapter_version,
            }
        )
        contract_passed = not spec.validate(self.root)
        reconciliation_passed = not reconciliation_missing and not any(
            "cross-source" in item for item in reasons
        )
        live_passed = bool(selected) and not event_reasons
        if not selected or metrics.event_count < spec.minimum_event_count or reconciliation_missing:
            verdict = CertificationVerdict.INSUFFICIENT_EVIDENCE
        elif reasons:
            verdict = CertificationVerdict.FAIL
        else:
            verdict = CertificationVerdict.PASS
        now = end
        certification = MarketDataCertification(
            certification_id=certification_id,
            venue=spec.venue,
            capability=spec.capability,
            canonical_instrument_id=spec.canonical_instrument_id,
            adapter_version=self.adapter_version,
            commit_sha=self.commit_sha,
            source_version=self.source_version,
            contract_fixture_sha256=spec.fixture_sha256,
            sample_start=start,
            sample_end=end,
            event_count=len(selected),
            verdict=verdict,
            verified_at=now,
            expires_at=now + self.certification_ttl,
            evidence_manifest_sha256=manifest_hash,
            reasons=tuple(dict.fromkeys(reasons)),
        )
        evidence = CertificationEvidence(
            certification_id=certification_id,
            metrics=metrics,
            event_manifest=manifest,
            contract_passed=contract_passed,
            normalization_passed=spec.normalization_test_passed,
            live_smoke_passed=live_passed,
            reconciliation_passed=reconciliation_passed,
            audit_passed=audit_passed,
            audit_run_id=audit_run_id,
            ci_run_id=ci_run_id,
            manifest_sha256=manifest_hash,
        )
        self.repository.save(certification, evidence)
        return certification

    @staticmethod
    def _validate_events(
        events: Sequence[RawMarketEvent], spec: ContractValidationSpec, sample_end: datetime
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        previous_exchange: datetime | None = None
        for event in events:
            if event.available_at < event.received_at:
                reasons.append("available_at precedes received_at")
            if event.exchange_timestamp is not None:
                if event.exchange_timestamp > event.received_at + timedelta(seconds=5):
                    reasons.append("future exchange timestamp")
                if previous_exchange is not None and event.exchange_timestamp < previous_exchange:
                    reasons.append("out-of-order exchange timestamp")
                previous_exchange = event.exchange_timestamp
            if event.available_at > sample_end:
                reasons.append("event became available after sample end")
            payload = event.payload()
            missing = [name for name in spec.response_fields if name not in payload]
            if missing:
                reasons.append(f"response fields missing: {','.join(missing)}")
        return tuple(reasons)


class CapabilityPromotionService:
    def __init__(
        self,
        repository: CertificationRepository,
        *,
        commit_sha: str,
        adapter_version: str,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.repository = repository
        self.commit_sha = commit_sha
        self.adapter_version = adapter_version
        self.now = now

    def promote(self, certification: MarketDataCertification) -> TrustedCapabilityRecord:
        stored = self.repository.get(certification.certification_id)
        if stored is None or stored[0] != certification:
            raise ValueError("certification is not repository-trusted")
        _, evidence = stored
        current = utc(self.now(), "now")
        if certification.verdict is not CertificationVerdict.PASS:
            raise ValueError("failed or insufficient certification cannot be promoted")
        if certification.commit_sha != self.commit_sha:
            raise ValueError("certification commit SHA mismatch")
        if certification.adapter_version != self.adapter_version:
            raise ValueError("certification adapter version mismatch")
        if not certification.verified_at <= current <= certification.expires_at:
            raise ValueError("certification is expired or future-dated")
        if evidence.manifest_sha256 != certification.evidence_manifest_sha256:
            raise ValueError("evidence manifest hash mismatch")
        required = (
            evidence.contract_passed,
            evidence.normalization_passed,
            evidence.live_smoke_passed,
            evidence.reconciliation_passed,
            evidence.audit_passed,
            bool(evidence.audit_run_id),
            bool(evidence.ci_run_id),
        )
        if not all(required):
            raise ValueError("certification evidence gates are incomplete")
        record = TrustedCapabilityRecord(
            venue=certification.venue,
            capability=certification.capability,
            support=CapabilitySupport.LIVE_VERIFIED,
            verification_run_id=certification.certification_id,
            verified_at=certification.verified_at,
            expires_at=certification.expires_at,
            adapter_version=certification.adapter_version,
            source_version=certification.source_version,
            contract_fixture_sha256=certification.contract_fixture_sha256,
            audit_run_id=evidence.audit_run_id,
            canonical_instrument_id=certification.canonical_instrument_id,
        )
        self.repository.save_promotion(
            certification.certification_id, certification.canonical_instrument_id, record
        )
        return record


class ProductionEventCertificationGate:
    def require(
        self,
        *,
        event: RawMarketEvent,
        certification: MarketDataCertification,
        trusted_record: TrustedCapabilityRecord,
        adapter_version: str,
        now: datetime,
    ) -> None:
        capability = EVENT_CAPABILITY.get(event.event_type)
        exact = (
            event.venue == certification.venue == trusted_record.venue
            and capability == certification.capability == trusted_record.capability
            and event.canonical_instrument_id
            == certification.canonical_instrument_id
            == trusted_record.canonical_instrument_id
            and certification.certification_id == trusted_record.verification_run_id
            and certification.adapter_version == adapter_version == trusted_record.adapter_version
        )
        if not exact:
            raise ValueError("production event does not exactly match certified identity")
        current = utc(now, "now")
        if certification.verdict is not CertificationVerdict.PASS:
            raise ValueError("production event certification is not PASS")
        if not certification.verified_at <= current <= certification.expires_at:
            raise ValueError("production event certification is expired")


class StrategySnapshotService:
    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def finalize(
        self,
        *,
        requirement: StrategyDataRequirement,
        cutoff_at: datetime,
        snapshot_id: str | None = None,
    ) -> DataSnapshotManifest:
        event_types = tuple(
            event_type
            for event_type, capability in EVENT_CAPABILITY.items()
            if capability in requirement.required_capabilities
        )
        manifest = DataSnapshotService(self.repository).finalize(
            cutoff_at=cutoff_at,
            snapshot_id=snapshot_id,
            eligibility_policy=SnapshotEligibilityPolicy(
                required_event_types=event_types,
                required_venues=requirement.required_venues,
                minimum_production_events=max(1, len(requirement.required_venues)),
                maximum_stale_ratio=requirement.maximum_stale_ratio,
                minimum_venue_event_coverage_ratio=requirement.minimum_coverage_ratio,
                minimum_history_windows_per_venue_event=requirement.minimum_history_windows,
            ),
            included_event_types=event_types,
            included_venues=requirement.required_venues,
        )
        return manifest


def evaluate_strict_paper_readiness(
    *,
    capabilities_live_verified: bool,
    snapshot_eligible: bool,
    research_completed: bool,
    strategy_eligible: bool,
    instrument_rules_complete: bool,
    paper_risk_enabled: bool,
    observation_candidate_exists: bool,
) -> StrictPaperReadiness:
    if all(
        (
            capabilities_live_verified,
            snapshot_eligible,
            research_completed,
            strategy_eligible,
            instrument_rules_complete,
            paper_risk_enabled,
            observation_candidate_exists,
        )
    ):
        return StrictPaperReadiness.READY_FOR_OPERATOR_APPROVAL
    return StrictPaperReadiness.NOT_READY


def require_operator_approval(readiness: StrictPaperReadiness, *, operator_approved: bool) -> None:
    if readiness is not StrictPaperReadiness.READY_FOR_OPERATOR_APPROVAL:
        raise ValueError("strict paper readiness gate has not passed")
    if not operator_approved:
        raise ValueError("strict paper requires explicit operator approval")


def normalize_funding_rate(value: object, *, unit: str) -> Decimal:
    try:
        raw = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("funding rate is not decimal") from exc
    if unit == "decimal":
        return raw
    if unit == "percent":
        return raw / Decimal("100")
    if unit == "basis_points":
        return raw / Decimal("10000")
    raise ValueError("unknown funding rate unit")


def funding_payment_direction(rate: Decimal, *, position_quantity: Decimal) -> str:
    payment = rate * position_quantity
    if payment > 0:
        return "long_pays"
    if payment < 0:
        return "long_receives"
    return "neutral"


def normalize_exchange_timestamp(
    value: int | float | str, *, unit: str, received_at: datetime
) -> datetime:
    scale = {
        "seconds": Decimal("1"),
        "milliseconds": Decimal("1000"),
        "microseconds": Decimal("1000000"),
    }
    if unit not in scale:
        raise ValueError("unsupported timestamp unit")
    seconds = Decimal(str(value)) / scale[unit]
    try:
        result = datetime.fromtimestamp(float(seconds), tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("timestamp unit mismatch") from exc
    received = utc(received_at, "received_at")
    if result > received + timedelta(seconds=5):
        raise ValueError("future exchange timestamp")
    if abs((received - result).total_seconds()) > 10 * 365 * 24 * 3600:
        raise ValueError("timestamp unit mismatch")
    return result


def validate_funding_interval(
    previous_at: datetime,
    current_at: datetime,
    *,
    expected_seconds: int,
    tolerance_seconds: int = 60,
) -> None:
    actual = (utc(current_at, "current_at") - utc(previous_at, "previous_at")).total_seconds()
    if abs(actual - expected_seconds) > tolerance_seconds:
        raise ValueError("funding interval mismatch")


def reconcile_funding_current_history(
    current_rate: Decimal, history_rate: Decimal, *, tolerance: Decimal = Decimal("0.00000001")
) -> Decimal:
    error = abs(current_rate - history_rate)
    if error > tolerance:
        raise ValueError("funding current/history mismatch")
    return error


def reconcile_values(
    left: Decimal, right: Decimal, *, maximum_relative_error: Decimal
) -> tuple[Decimal, Decimal]:
    absolute = abs(left - right)
    relative = absolute / max(abs(left), abs(right), Decimal("0.000000000001"))
    if relative > maximum_relative_error:
        raise ValueError("cross-source reconciliation mismatch")
    return absolute, relative


def certification_metrics(
    events: Sequence[RawMarketEvent],
    spec: ContractValidationSpec,
    sample_start: datetime,
    sample_end: datetime,
    cross_source_pairs: Sequence[tuple[Decimal, Decimal]],
) -> CertificationMetrics:
    interval = spec.funding_interval_seconds or max(
        1, int((sample_end - sample_start).total_seconds() / max(1, spec.minimum_event_count))
    )
    expected = max(1, int((sample_end - sample_start).total_seconds() / interval) + 1)
    unique = {(item.event_id, item.payload_sha256) for item in events}
    duplicate_count = len(events) - len(unique)
    stale_count = sum(
        (sample_end - item.available_at).total_seconds() > interval * 2 for item in events
    )
    ordered = sorted(events, key=lambda item: item.received_at)
    out_of_order = sum(
        previous.exchange_timestamp is not None
        and current.exchange_timestamp is not None
        and current.exchange_timestamp < previous.exchange_timestamp
        for previous, current in pairwise(ordered)
    )
    sequences = [item.sequence for item in ordered if item.sequence is not None]
    gaps = sum(current != previous + 1 for previous, current in pairwise(sequences))
    latencies = sorted(
        Decimal(str((item.received_at - item.exchange_timestamp).total_seconds() * 1000))
        for item in events
        if item.exchange_timestamp is not None
    )
    absolute_errors = [abs(left - right) for left, right in cross_source_pairs]
    relative_errors = [
        value / max(abs(left), abs(right), Decimal("0.000000000001"))
        for value, (left, right) in zip(absolute_errors, cross_source_pairs, strict=True)
    ]
    return CertificationMetrics(
        event_count=len(events),
        coverage_ratio=min(Decimal("1"), Decimal(len(unique)) / Decimal(expected)),
        missing_interval_count=max(0, expected - len(unique)),
        duplicate_ratio=Decimal(duplicate_count) / Decimal(max(1, len(events))),
        stale_ratio=Decimal(stale_count) / Decimal(max(1, len(events))),
        out_of_order_count=out_of_order,
        sequence_gap_count=gaps,
        median_latency_ms=Decimal(str(median(latencies))) if latencies else None,
        p95_latency_ms=(
            latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))] if latencies else None
        ),
        maximum_latency_ms=max(latencies) if latencies else None,
        maximum_clock_skew_ms=max((abs(item) for item in latencies), default=None),
        cross_source_absolute_error=max(absolute_errors, default=None),
        cross_source_relative_error=max(relative_errors, default=None),
    )


def validate_funding_current(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> tuple[str, ...]:
    reasons: list[str] = []
    for event in events:
        payload = event.payload()
        try:
            rate = normalize_funding_rate(
                payload["rate"] if "rate" in payload else payload["funding_rate"],
                unit=spec.funding_unit or "",
            )
        except (KeyError, ValueError):
            reasons.append("funding rate unit validation failed")
            continue
        if abs(rate) > Decimal("0.1"):
            reasons.append("funding rate magnitude is implausible")
        if "next_funding_at" not in payload and "next_funding_time" not in payload:
            reasons.append("next funding time is missing")
        observed_interval = payload.get("funding_interval_seconds")
        if observed_interval is None:
            reasons.append("funding interval evidence is missing")
        elif spec.funding_interval_seconds != int(observed_interval):
            reasons.append("funding interval mismatch")
        if not payload.get("funding_schedule_source"):
            reasons.append("funding schedule source is missing")
    return tuple(reasons)


def validate_funding_series(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> tuple[str, ...]:
    if spec.funding_interval_seconds is None:
        return ("funding interval is not declared",)
    timestamps = sorted(
        item.exchange_timestamp for item in events if item.exchange_timestamp is not None
    )
    reasons: list[str] = []
    for previous, current in pairwise(timestamps):
        try:
            validate_funding_interval(
                previous,
                current,
                expected_seconds=spec.funding_interval_seconds,
                tolerance_seconds=300,
            )
        except ValueError:
            reasons.append("funding interval mismatch")
    return tuple(reasons)


def validate_order_book_events(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> tuple[str, ...]:
    reasons: list[str] = []
    for event in events:
        payload = event.payload()
        try:
            bids = tuple(
                (Decimal(str(item["price"])), Decimal(str(item["quantity"])))
                for item in payload["bids"]
            )
            asks = tuple(
                (Decimal(str(item["price"])), Decimal(str(item["quantity"])))
                for item in payload["asks"]
            )
        except (KeyError, InvalidOperation, TypeError):
            reasons.append("order-book schema is invalid")
            continue
        if not bids or not asks:
            reasons.append("order-book side is empty")
            continue
        if any(quantity <= 0 for _, quantity in bids + asks):
            reasons.append("order-book quantity is non-positive")
        if tuple(price for price, _ in bids) != tuple(
            sorted((price for price, _ in bids), reverse=True)
        ):
            reasons.append("bids are not descending")
        if tuple(price for price, _ in asks) != tuple(sorted(price for price, _ in asks)):
            reasons.append("asks are not ascending")
        if bids[0][0] >= asks[0][0]:
            reasons.append("order-book is crossed or locked")
        if len({price for price, _ in bids}) != len(bids) or len(
            {price for price, _ in asks}
        ) != len(asks):
            reasons.append("duplicate order-book price level")
        if event.reconciliation_state is not ReconciliationState.SYNCHRONIZED:
            reasons.append("order-book is not synchronized")
        if spec.venue == "hyperliquid" and spec.snapshot_delta_semantics != "snapshot_only":
            reasons.append("Hyperliquid order-book semantics must be snapshot_only")
        if spec.venue == "bitget" and spec.snapshot_delta_semantics != "snapshot_and_delta":
            reasons.append("Bitget order-book semantics must be snapshot_and_delta")
    return tuple(reasons)


def write_certification_artifacts(
    *,
    root: Path,
    certification: MarketDataCertification,
    evidence: CertificationEvidence,
    contract: ContractValidationSpec,
    events: Sequence[RawMarketEvent],
    promotion: TrustedCapabilityRecord | None = None,
) -> Path:
    directory = root / certification.certification_id
    fixture_dir = directory / "contract-fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    (fixture_dir / "contract.json").write_text(_json(asdict(contract)) + "\n", encoding="utf-8")
    selected = tuple(
        item
        for item in events
        if (item.event_id, item.payload_sha256) in set(evidence.event_manifest)
    )
    for index, event in enumerate(selected[:20]):
        (fixture_dir / f"{index:04d}-{event.event_id}.json").write_text(
            event.raw_payload + "\n", encoding="utf-8"
        )
    metrics_path = directory / "metrics.json"
    metrics_path.write_text(_json(asdict(evidence.metrics)) + "\n", encoding="utf-8")
    with (directory / "reconciliation.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("metric", "value"))
        for key, value in asdict(evidence.metrics).items():
            writer.writerow((key, value))
    with (directory / "failures.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("reason",))
        writer.writerows((reason,) for reason in certification.reasons)
    promotion_path = directory / "promotion.json"
    promotion_path.write_text(
        _json(
            {
                "promoted": promotion is not None,
                "record": asdict(promotion) if promotion is not None else None,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    summary = (
        f"# Market Data Certification\n\n"
        f"- Venue: {certification.venue}\n"
        f"- Instrument: {certification.canonical_instrument_id}\n"
        f"- Capability: {certification.capability}\n"
        f"- Verdict: **{certification.verdict.value}**\n"
        f"- Events: {certification.event_count}\n"
        f"- Coverage: {evidence.metrics.coverage_ratio}\n"
        f"- Source: {contract.source_endpoint}\n"
        f"- Contract fixture SHA-256: {certification.contract_fixture_sha256}\n"
        f"- Adapter version: {certification.adapter_version}\n"
        f"- Expires: {certification.expires_at.isoformat()}\n"
        f"- Promotion: {'LIVE_VERIFIED' if promotion else 'not promoted'}\n"
        f"- Live execution: **OFF**\n"
    )
    (directory / "summary.md").write_text(summary, encoding="utf-8")
    manifest_files = tuple(
        sorted(
            path for path in directory.rglob("*") if path.is_file() and path.name != "manifest.json"
        )
    )
    manifest = {
        "certification": asdict(certification),
        "contract": asdict(contract),
        "evidence": asdict(evidence),
        "files": {
            str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in manifest_files
        },
        "live_execution": "OFF",
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(_json(manifest) + "\n", encoding="utf-8")
    return manifest_path


def _json(value: object) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def _certification(payload: str) -> MarketDataCertification:
    data = json.loads(payload)
    return MarketDataCertification(
        **{
            **data,
            "verdict": CertificationVerdict(data["verdict"]),
            "sample_start": datetime.fromisoformat(data["sample_start"]),
            "sample_end": datetime.fromisoformat(data["sample_end"]),
            "verified_at": datetime.fromisoformat(data["verified_at"]),
            "expires_at": datetime.fromisoformat(data["expires_at"]),
            "reasons": tuple(data["reasons"]),
        }
    )


def _evidence(payload: str) -> CertificationEvidence:
    data = json.loads(payload)
    raw = data["metrics"]

    def optional_decimal(name: str) -> Decimal | None:
        return Decimal(raw[name]) if raw[name] is not None else None

    metrics = CertificationMetrics(
        event_count=int(raw["event_count"]),
        coverage_ratio=Decimal(raw["coverage_ratio"]),
        missing_interval_count=int(raw["missing_interval_count"]),
        duplicate_ratio=Decimal(raw["duplicate_ratio"]),
        stale_ratio=Decimal(raw["stale_ratio"]),
        out_of_order_count=int(raw["out_of_order_count"]),
        sequence_gap_count=int(raw["sequence_gap_count"]),
        median_latency_ms=optional_decimal("median_latency_ms"),
        p95_latency_ms=optional_decimal("p95_latency_ms"),
        maximum_latency_ms=optional_decimal("maximum_latency_ms"),
        maximum_clock_skew_ms=optional_decimal("maximum_clock_skew_ms"),
        cross_source_absolute_error=optional_decimal("cross_source_absolute_error"),
        cross_source_relative_error=optional_decimal("cross_source_relative_error"),
    )
    return CertificationEvidence(
        **{
            **data,
            "metrics": metrics,
            "event_manifest": tuple(tuple(item) for item in data["event_manifest"]),
        }
    )
