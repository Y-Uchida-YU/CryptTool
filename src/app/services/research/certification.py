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
from app.services.research.data_operations import (
    DataSnapshotService,
    SnapshotEligibilityPolicy,
    canonical_market_event_content,
)
from app.services.research.models import (
    AvailabilityProvenance,
    DataSnapshotManifest,
    RawMarketEvent,
    TimestampSemantic,
    canonical_sha256,
    utc,
)
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

HISTORICAL_TIMESTAMP_SEMANTICS = {
    TimestampSemantic.HISTORICAL_EFFECTIVE_TIME,
    TimestampSemantic.CANDLE_OPEN_TIME,
    TimestampSemantic.CANDLE_CLOSE_TIME,
    TimestampSemantic.FUNDING_EFFECTIVE_TIME,
}


class CertificationVerdict(StrEnum):
    PASS = "pass"  # nosec B105
    FAIL = "fail"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class StrictPaperReadiness(StrEnum):
    NOT_READY = "not_ready"
    READY_FOR_OPERATOR_APPROVAL = "ready_for_operator_approval"


@dataclass(frozen=True)
class CapabilityContractVerdict:
    schema_valid: bool
    units_valid: bool
    timestamp_semantic_valid: bool
    value_domain_valid: bool
    source_identity_valid: bool


@dataclass(frozen=True)
class HistoricalResearchUsability:
    point_in_time_available: bool | None
    availability_provenance: AvailabilityProvenance
    verdict: CertificationVerdict
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class EventTimingMetrics:
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    exchange_server_time: datetime | None
    event_age_seconds: Decimal | None
    transport_latency_seconds: Decimal | None
    clock_skew_seconds: Decimal | None
    event_id: str = ""
    timestamp_semantic: str = TimestampSemantic.RECEIPT_ONLY.value
    availability_provenance: str = AvailabilityProvenance.UNKNOWN.value
    source_endpoint: str = "unknown"
    raw_payload_sha256: str = ""
    clock_skew_formula: str | None = None


@dataclass(frozen=True)
class FundingIntervalObservation:
    previous_effective_at: datetime
    current_effective_at: datetime
    expected_interval_seconds: int
    actual_interval_seconds: int
    difference_seconds: int
    missing_window_count: int
    duplicate_window: bool
    schedule_change: bool


@dataclass(frozen=True)
class FundingIntervalResult:
    observations: tuple[FundingIntervalObservation, ...]
    duplicate_count: int
    missing_window_count: int | None
    schedule_change_count: int
    violations: tuple[str, ...]
    insufficiencies: tuple[str, ...]
    window_assessment: str = "EVALUATED"


@dataclass(frozen=True)
class CapabilityAuditBinding:
    venue: str
    capability: str
    commit_sha: str
    adapter_version: str
    source_version: str
    fixture_sha256: str
    audit_run_id: str
    audit_artifact_sha256: str
    ci_run_id: str
    passed: bool


class CapabilityAuditArtifactResolver:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def resolve(
        self,
        *,
        venue: str,
        capability: str,
        commit_sha: str,
        adapter_version: str,
        source_version: str,
        fixture_sha256: str,
    ) -> CapabilityAuditBinding | None:
        manifest_path = self.directory / "manifest.json"
        if not manifest_path.is_file():
            return None
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        report_path = self.directory / str(manifest.get("report_file", ""))
        if not report_path.is_file():
            return None
        report_bytes = report_path.read_bytes()
        artifact_sha = hashlib.sha256(report_bytes).hexdigest()
        if (
            manifest.get("commit_sha") != commit_sha
            or manifest.get("report_sha256") != artifact_sha
        ):
            return None
        report = json.loads(report_bytes)
        for finding in report.get("findings", []):
            if (finding.get("venue"), finding.get("capability")) != (venue, capability):
                continue
            exact = (
                finding.get("adapter_version") == adapter_version
                and finding.get("source_version") == source_version
                and finding.get("contract_fixture_sha256") == fixture_sha256
                and finding.get("passed") is True
                and finding.get("test_result") == "passed"
            )
            if not exact:
                continue
            return CapabilityAuditBinding(
                venue=venue,
                capability=capability,
                commit_sha=commit_sha,
                adapter_version=adapter_version,
                source_version=source_version,
                fixture_sha256=fixture_sha256,
                audit_run_id=str(finding.get("audit_run_id", "")),
                audit_artifact_sha256=artifact_sha,
                ci_run_id=str(finding.get("ci_run_id", "")),
                passed=True,
            )
        return None


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
    live_out_of_order_count: int = 0
    historical_source_order_reversed: int = 0
    historical_duplicate_count: int = 0
    historical_timestamp_collision_count: int = 0
    clock_skew_unknown_count: int = 0
    clock_skew_known_sample_count: int = 0
    clock_skew_violation_count: int = 0
    clock_skew_violation_ratio: Decimal | None = None
    median_clock_skew_ms: Decimal | None = None
    p95_clock_skew_ms: Decimal | None = None
    clock_skew_threshold_ms: Decimal = Decimal("5000")
    clock_skew_minimum_sample_count: int = 1
    clock_skew_maximum_violation_ratio: Decimal = Decimal("0.05")
    candle_interval_alignment_violation_count: int = 0
    candle_missing_count: int = 0
    future_timestamp_count: int = 0
    timing: tuple[EventTimingMetrics, ...] = ()


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
    timestamp_semantic: TimestampSemantic = TimestampSemantic.RECEIPT_ONLY
    minimum_clock_skew_sample_count: int = 1
    maximum_clock_skew_violation_ratio: Decimal = Decimal("0.05")

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
    audit_artifact_sha256: str = ""
    funding_interval: FundingIntervalResult | None = None
    capability_contract: CapabilityContractVerdict | None = None
    historical_research_usability: HistoricalResearchUsability | None = None


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
        audit_artifact_sha256: str = "",
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
        normalized, _, _, _ = normalize_certification_events(selected, spec)
        failures = list(spec.validate(self.root))
        insufficiencies: list[str] = []
        event_failures, event_insufficiencies = self._validate_events(normalized, spec, end)
        failures.extend(event_failures)
        insufficiencies.extend(event_insufficiencies)
        metrics = certification_metrics(selected, spec, start, end, cross_source_pairs)
        reconciliation_required = spec.capability in {
            "funding_current",
            "mark_price",
            "ohlcv",
        }
        reconciliation_missing = reconciliation_required and not cross_source_pairs
        if reconciliation_missing:
            insufficiencies.append("cross-source reconciliation evidence is missing")
        if metrics.event_count < spec.minimum_event_count:
            insufficiencies.append("minimum event count not reached")
        if metrics.coverage_ratio < spec.minimum_coverage_ratio:
            insufficiencies.append("coverage ratio below threshold")
        if metrics.stale_ratio > spec.maximum_stale_ratio:
            insufficiencies.append("stale ratio above threshold")
        if (
            metrics.clock_skew_known_sample_count < spec.minimum_clock_skew_sample_count
            and spec.timestamp_semantic
            not in {TimestampSemantic.CANDLE_OPEN_TIME, TimestampSemantic.CANDLE_CLOSE_TIME}
        ):
            insufficiencies.append("exchange server time unavailable")
        if (
            metrics.clock_skew_known_sample_count >= spec.minimum_clock_skew_sample_count
            and metrics.clock_skew_violation_ratio is not None
            and metrics.clock_skew_violation_ratio > spec.maximum_clock_skew_violation_ratio
        ):
            failures.append("clock skew above threshold")
        if metrics.live_out_of_order_count > 0:
            failures.append("live out-of-order events detected")
        if (
            metrics.cross_source_relative_error is not None
            and metrics.cross_source_relative_error > spec.maximum_relative_error
            and spec.maximum_absolute_error is None
        ):
            failures.append("cross-source relative error above threshold")
        if (
            metrics.cross_source_absolute_error is not None
            and spec.maximum_absolute_error is not None
            and metrics.cross_source_absolute_error > spec.maximum_absolute_error
        ):
            failures.append("cross-source absolute error above threshold")
        if spec.capability == "funding_current":
            failures.extend(validate_funding_current(normalized, spec))
        funding_interval = None
        if spec.capability == "funding_history":
            funding_interval = analyze_funding_intervals(normalized, spec)
            failures.extend(funding_interval.violations)
            insufficiencies.extend(funding_interval.insufficiencies)
        if spec.capability in {"orderbook_snapshot", "orderbook_delta"}:
            failures.extend(validate_order_book_events(normalized, spec))
        reasons = tuple(dict.fromkeys((*failures, *insufficiencies)))
        research_usability = historical_research_usability(normalized, spec)
        contract_verdict = capability_contract_verdict(spec, failures)
        validate_certification_reason_invariants(
            reasons=reasons,
            metrics=metrics,
            funding_interval=funding_interval,
        )
        manifest = tuple(sorted((item.event_id, item.payload_sha256) for item in normalized))
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
            "cross-source" in item for item in failures
        )
        live_passed = (
            bool(normalized)
            and not event_failures
            and spec.timestamp_semantic not in HISTORICAL_TIMESTAMP_SEMANTICS
        )
        if failures:
            verdict = CertificationVerdict.FAIL
        elif not normalized or insufficiencies:
            verdict = CertificationVerdict.INSUFFICIENT_EVIDENCE
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
            event_count=len(normalized),
            verdict=verdict,
            verified_at=now,
            expires_at=now + self.certification_ttl,
            evidence_manifest_sha256=manifest_hash,
            reasons=reasons,
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
            audit_artifact_sha256=audit_artifact_sha256,
            funding_interval=funding_interval,
            capability_contract=contract_verdict,
            historical_research_usability=research_usability,
        )
        self.repository.save(certification, evidence)
        return certification

    @staticmethod
    def _validate_events(
        events: Sequence[RawMarketEvent], spec: ContractValidationSpec, sample_end: datetime
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        failures: list[str] = []
        insufficiencies: list[str] = []
        for event in events:
            if event.available_at < event.received_at:
                failures.append("available_at precedes received_at")
            if (
                event.exchange_timestamp is not None
                and event.exchange_timestamp > event.received_at + timedelta(seconds=5)
            ):
                failures.append("future exchange timestamp")
            if event.available_at > sample_end:
                failures.append("event became available after sample end")
            if event.timestamp_semantic != spec.timestamp_semantic:
                failures.append("timestamp semantic does not match capability contract")
            payload = event.payload()
            missing = [name for name in spec.response_fields if name not in payload]
            if missing:
                failures.append(f"response fields missing: {','.join(missing)}")
            if spec.capability == "trade" and not payload.get("trade_id"):
                insufficiencies.append("exchange trade id unavailable; uniqueness is not proven")
            if spec.capability == "ohlcv":
                try:
                    opened = Decimal(str(payload["open"]))
                    high = Decimal(str(payload["high"]))
                    low = Decimal(str(payload["low"]))
                    closed = Decimal(str(payload["close"]))
                    volume = Decimal(str(payload["volume"]))
                except (InvalidOperation, KeyError):
                    failures.append("OHLCV value domain is invalid")
                else:
                    if high < max(opened, closed) or low > min(opened, closed) or high < low:
                        failures.append("OHLCV structure is invalid")
                    if volume < 0:
                        failures.append("OHLCV volume is negative")
        return tuple(failures), tuple(insufficiencies)


def capability_contract_verdict(
    spec: ContractValidationSpec, failures: Sequence[str]
) -> CapabilityContractVerdict:
    lowered = tuple(reason.lower() for reason in failures)
    return CapabilityContractVerdict(
        schema_valid=not any(
            token in reason
            for reason in lowered
            for token in ("schema", "response fields missing", "contract metadata", "fixture")
        ),
        units_valid=not any("unit" in reason for reason in lowered),
        timestamp_semantic_valid=not any(
            token in reason
            for reason in lowered
            for token in ("timestamp semantic", "future exchange timestamp", "timestamp unit")
        ),
        value_domain_valid=not any(
            token in reason
            for reason in lowered
            for token in ("value domain", "ohlcv structure", "negative", "magnitude", "sign")
        ),
        source_identity_valid=not any("source identity" in reason for reason in lowered),
    )


def historical_research_usability(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> HistoricalResearchUsability | None:
    if spec.timestamp_semantic not in HISTORICAL_TIMESTAMP_SEMANTICS:
        return None
    proven = {
        AvailabilityProvenance.HISTORICAL_EFFECTIVE_TIME,
        AvailabilityProvenance.EXCHANGE_PUBLISHED_TIME,
    }
    proven_events = tuple(item for item in events if item.availability_provenance in proven)
    provenance = (
        events[0].availability_provenance
        if events
        and all(
            item.availability_provenance == events[0].availability_provenance for item in events
        )
        else AvailabilityProvenance.UNKNOWN
    )
    if events and len(proven_events) == len(events):
        return HistoricalResearchUsability(
            point_in_time_available=True,
            availability_provenance=provenance,
            verdict=CertificationVerdict.PASS,
            reasons=(),
        )
    reason = (
        "historical event count is zero"
        if not events
        else "historical availability is not point-in-time proven"
    )
    return HistoricalResearchUsability(
        point_in_time_available=None,
        availability_provenance=provenance,
        verdict=CertificationVerdict.INSUFFICIENT_EVIDENCE,
        reasons=(reason,),
    )


def validate_certification_reason_invariants(
    *,
    reasons: Sequence[str],
    metrics: CertificationMetrics,
    funding_interval: FundingIntervalResult | None,
) -> None:
    lowered = tuple(reason.lower() for reason in reasons)
    contradictions: list[str] = []
    if any("out-of-order" in reason for reason in lowered) and metrics.live_out_of_order_count <= 0:
        contradictions.append("out-of-order reason requires live_out_of_order_count > 0")
    if any("clock skew above" in reason for reason in lowered):
        threshold_exceeded = (
            metrics.clock_skew_known_sample_count >= metrics.clock_skew_minimum_sample_count
            and metrics.clock_skew_violation_count > 0
            and metrics.clock_skew_violation_ratio is not None
            and metrics.clock_skew_violation_ratio > metrics.clock_skew_maximum_violation_ratio
        )
        if not threshold_exceeded:
            contradictions.append(
                "clock skew reason requires minimum samples and violation ratio above threshold"
            )
    if any("interval mismatch" in reason for reason in lowered) and (
        funding_interval is None
        or not any("interval mismatch" in item.lower() for item in funding_interval.violations)
    ):
        contradictions.append("interval mismatch reason requires an interval violation")
    if contradictions:
        raise ValueError(
            "certification metric/reason invariant failed: " + "; ".join(contradictions)
        )


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
            len(evidence.audit_artifact_sha256) == 64,
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
                require_point_in_time_availability=True,
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
    normalized, historical_duplicates, historical_collisions, source_reversed = (
        normalize_certification_events(events, spec)
    )
    interval = spec.funding_interval_seconds or max(
        1, int((sample_end - sample_start).total_seconds() / max(1, spec.minimum_event_count))
    )
    expected = max(1, int((sample_end - sample_start).total_seconds() / interval) + 1)
    unique = {(item.event_id, item.payload_sha256) for item in normalized}
    duplicate_count = len(events) - len(normalized)
    stale_count = sum(
        (sample_end - item.available_at).total_seconds() > interval * 2 for item in normalized
    )
    ordered = sorted(normalized, key=lambda item: item.received_at)
    out_of_order = live_trade_out_of_order_count(ordered)
    sequences = [item.sequence for item in ordered if item.sequence is not None]
    gaps = sum(current != previous + 1 for previous, current in pairwise(sequences))
    timing = tuple(
        event_timing_metrics(item, source_endpoint=spec.source_endpoint) for item in normalized
    )
    latencies = sorted(
        item.transport_latency_seconds * Decimal("1000")
        for item in timing
        if item.transport_latency_seconds is not None
    )
    clock_skews = sorted(
        abs(item.clock_skew_seconds * Decimal("1000"))
        for item in timing
        if item.clock_skew_seconds is not None
    )
    clock_skew_violations = [value for value in clock_skews if value > spec.maximum_clock_skew_ms]
    clock_skew_ratio = (
        Decimal(len(clock_skew_violations)) / Decimal(len(clock_skews)) if clock_skews else None
    )
    absolute_errors = [abs(left - right) for left, right in cross_source_pairs]
    relative_errors = [
        value / max(abs(left), abs(right), Decimal("0.000000000001"))
        for value, (left, right) in zip(absolute_errors, cross_source_pairs, strict=True)
    ]
    candle_alignment, candle_missing, future_timestamps = ohlcv_time_metrics(normalized, spec)
    return CertificationMetrics(
        event_count=len(normalized),
        coverage_ratio=min(Decimal("1"), Decimal(len(unique)) / Decimal(expected)),
        missing_interval_count=max(0, expected - len(unique)),
        duplicate_ratio=Decimal(duplicate_count) / Decimal(max(1, len(events))),
        stale_ratio=Decimal(stale_count) / Decimal(max(1, len(normalized))),
        out_of_order_count=out_of_order,
        sequence_gap_count=gaps,
        median_latency_ms=Decimal(str(median(latencies))) if latencies else None,
        p95_latency_ms=(
            latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))] if latencies else None
        ),
        maximum_latency_ms=max(latencies) if latencies else None,
        maximum_clock_skew_ms=max(clock_skews, default=None),
        cross_source_absolute_error=max(absolute_errors, default=None),
        cross_source_relative_error=max(relative_errors, default=None),
        live_out_of_order_count=out_of_order,
        historical_source_order_reversed=source_reversed,
        historical_duplicate_count=historical_duplicates,
        historical_timestamp_collision_count=historical_collisions,
        clock_skew_unknown_count=sum(item.clock_skew_seconds is None for item in timing),
        clock_skew_known_sample_count=len(clock_skews),
        clock_skew_violation_count=len(clock_skew_violations),
        clock_skew_violation_ratio=clock_skew_ratio,
        median_clock_skew_ms=(Decimal(str(median(clock_skews))) if clock_skews else None),
        p95_clock_skew_ms=(
            clock_skews[max(0, (95 * len(clock_skews) + 99) // 100 - 1)] if clock_skews else None
        ),
        clock_skew_threshold_ms=spec.maximum_clock_skew_ms,
        clock_skew_minimum_sample_count=spec.minimum_clock_skew_sample_count,
        clock_skew_maximum_violation_ratio=spec.maximum_clock_skew_violation_ratio,
        candle_interval_alignment_violation_count=candle_alignment,
        candle_missing_count=candle_missing,
        future_timestamp_count=future_timestamps,
        timing=timing,
    )


def live_trade_out_of_order_count(events: Sequence[RawMarketEvent]) -> int:
    streams: dict[tuple[str, str, str, int], list[RawMarketEvent]] = {}
    for event in events:
        if (
            event.event_type != "trade"
            or event.timestamp_semantic is not TimestampSemantic.REALTIME_EVENT
        ):
            continue
        key = (
            event.venue,
            event.canonical_instrument_id,
            event.channel,
            event.connection_epoch or 0,
        )
        streams.setdefault(key, []).append(event)
    count = 0
    for stream in streams.values():
        ordered = sorted(stream, key=lambda item: item.received_at)
        for previous, current in pairwise(ordered):
            if (
                previous.exchange_timestamp is not None
                and current.exchange_timestamp == previous.exchange_timestamp
            ):
                continue
            if previous.sequence is not None and current.sequence is not None:
                count += int(current.sequence < previous.sequence)
                continue
            previous_id = str(previous.payload().get("trade_id", ""))
            current_id = str(current.payload().get("trade_id", ""))
            if previous_id.isdecimal() and current_id.isdecimal():
                count += int(int(current_id) < int(previous_id))
                continue
            if previous.exchange_timestamp is not None and current.exchange_timestamp is not None:
                count += int(current.exchange_timestamp < previous.exchange_timestamp)
    return count


def _timeframe_seconds(value: str | None) -> int | None:
    if not value or len(value) < 2 or not value[:-1].isdigit():
        return None
    unit = {"m": 60, "h": 3600, "d": 86400}.get(value[-1])
    return int(value[:-1]) * unit if unit is not None else None


def ohlcv_time_metrics(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> tuple[int, int, int]:
    if spec.capability != "ohlcv":
        return 0, 0, 0
    timestamps = sorted(
        item.exchange_timestamp for item in events if item.exchange_timestamp is not None
    )
    now = max((item.received_at for item in events), default=datetime.now(UTC))
    future = sum(item > now + timedelta(seconds=5) for item in timestamps)
    timeframe = next(
        (_timeframe_seconds(item.timeframe) for item in events if item.timeframe), None
    )
    if timeframe is None:
        return 0, 0, future
    alignment = sum(int(item.timestamp()) % timeframe != 0 for item in timestamps)
    missing = sum(
        max(0, int((current - previous).total_seconds()) // timeframe - 1)
        for previous, current in pairwise(timestamps)
        if current > previous
    )
    return alignment, missing, future


def event_timing_metrics(
    event: RawMarketEvent, *, source_endpoint: str = "unknown"
) -> EventTimingMetrics:
    historical = event.timestamp_semantic in HISTORICAL_TIMESTAMP_SEMANTICS
    event_age = (
        Decimal(str((event.received_at - event.exchange_timestamp).total_seconds()))
        if historical and event.exchange_timestamp is not None
        else None
    )
    transport = (
        Decimal(str((event.available_at - event.exchange_timestamp).total_seconds()))
        if event.timestamp_semantic is TimestampSemantic.REALTIME_EVENT
        and event.exchange_timestamp is not None
        else None
    )
    skew = (
        Decimal(str((event.received_at - event.exchange_server_time).total_seconds()))
        if event.exchange_server_time is not None
        and event.timestamp_semantic not in HISTORICAL_TIMESTAMP_SEMANTICS
        else None
    )
    return EventTimingMetrics(
        exchange_timestamp=event.exchange_timestamp,
        received_at=event.received_at,
        available_at=event.available_at,
        exchange_server_time=event.exchange_server_time,
        event_age_seconds=event_age,
        transport_latency_seconds=transport,
        clock_skew_seconds=skew,
        event_id=event.event_id,
        timestamp_semantic=event.timestamp_semantic.value,
        availability_provenance=event.availability_provenance.value,
        source_endpoint=source_endpoint,
        raw_payload_sha256=event.source_payload_sha256 or event.payload_sha256,
        clock_skew_formula=("received_at - exchange_server_time" if skew is not None else None),
    )


def normalize_certification_events(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> tuple[tuple[RawMarketEvent, ...], int, int, int]:
    historical = spec.timestamp_semantic in HISTORICAL_TIMESTAMP_SEMANTICS
    if not historical:
        return tuple(events), 0, 0, 0
    timestamps = [item.exchange_timestamp for item in events if item.exchange_timestamp is not None]
    source_reversed = sum(current < previous for previous, current in pairwise(timestamps))
    by_identity: dict[str, RawMarketEvent] = {}
    duplicate_count = 0
    collision_count = 0
    for event in events:
        identity = event.event_id
        current = by_identity.get(identity)
        if current is None:
            by_identity[identity] = event
        elif canonical_market_event_content(current) == canonical_market_event_content(event):
            duplicate_count += 1
        else:
            collision_count += 1
    ordered = tuple(
        sorted(
            by_identity.values(),
            key=lambda item: (item.exchange_timestamp or item.received_at, item.event_id),
        )
    )
    return ordered, duplicate_count, collision_count, source_reversed


def analyze_funding_intervals(
    events: Sequence[RawMarketEvent], spec: ContractValidationSpec
) -> FundingIntervalResult:
    if spec.funding_interval_seconds is None:
        return FundingIntervalResult(
            (),
            0,
            None,
            0,
            ("funding interval is not declared",),
            (),
            "NOT_EVALUATED",
        )
    finalized: dict[datetime, RawMarketEvent] = {}
    duplicate_count = 0
    for event in events:
        payload = event.payload()
        if not bool(payload.get("is_finalized", True)) or payload.get("predicted", False):
            continue
        if event.exchange_timestamp is None:
            continue
        if event.exchange_timestamp in finalized:
            duplicate_count += 1
            continue
        finalized[event.exchange_timestamp] = event
    timestamps = sorted(finalized)
    observations: list[FundingIntervalObservation] = []
    violations: list[str] = []
    insufficiencies: list[str] = []
    missing_total = 0
    schedule_changes = 0
    for previous, current in pairwise(timestamps):
        previous_payload = finalized[previous].payload()
        current_payload = finalized[current].payload()
        previous_expected = int(
            previous_payload.get("funding_interval_seconds") or spec.funding_interval_seconds
        )
        expected = int(current_payload.get("funding_interval_seconds") or previous_expected)
        changed = expected != previous_expected
        schedule_changes += int(changed)
        actual = int((current - previous).total_seconds())
        difference = actual - expected
        missing = max(0, actual // expected - 1) if expected > 0 and actual % expected == 0 else 0
        missing_total += missing
        if missing:
            insufficiencies.append("funding history has missing windows")
        elif abs(difference) > 300:
            violations.append("funding interval mismatch")
        observations.append(
            FundingIntervalObservation(
                previous_effective_at=previous,
                current_effective_at=current,
                expected_interval_seconds=expected,
                actual_interval_seconds=actual,
                difference_seconds=difference,
                missing_window_count=missing,
                duplicate_window=False,
                schedule_change=changed,
            )
        )
    window_assessment = "EVALUATED"
    reported_missing: int | None = missing_total
    if len(timestamps) < 2:
        insufficiencies.append("funding window evidence is insufficient")
        window_assessment = "NOT_EVALUATED"
        reported_missing = None
    return FundingIntervalResult(
        observations=tuple(observations),
        duplicate_count=duplicate_count,
        missing_window_count=reported_missing,
        schedule_change_count=schedule_changes,
        violations=tuple(dict.fromkeys(violations)),
        insufficiencies=tuple(dict.fromkeys(insufficiencies)),
        window_assessment=window_assessment,
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
    validate_certification_reason_invariants(
        reasons=certification.reasons,
        metrics=evidence.metrics,
        funding_interval=evidence.funding_interval,
    )
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
    with (directory / "clock-skew-evidence.csv").open("w", encoding="utf-8", newline="") as handle:
        timing_writer = csv.DictWriter(
            handle,
            fieldnames=tuple(EventTimingMetrics.__dataclass_fields__),
        )
        timing_writer.writeheader()
        timing_writer.writerows(asdict(item) for item in evidence.metrics.timing)
    with (directory / "reconciliation.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("metric", "value"))
        for key, value in asdict(evidence.metrics).items():
            writer.writerow((key, value))
    with (directory / "failures.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("reason",))
        writer.writerows((reason,) for reason in certification.reasons)
    if evidence.funding_interval is not None:
        with (directory / "funding-interval.csv").open("w", encoding="utf-8", newline="") as handle:
            funding_writer = csv.DictWriter(
                handle,
                fieldnames=tuple(FundingIntervalObservation.__dataclass_fields__),
            )
            funding_writer.writeheader()
            funding_writer.writerows(
                asdict(item) for item in evidence.funding_interval.observations
            )
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
        f"- Capability contract: {evidence.capability_contract}\n"
        f"- Historical research usability: {evidence.historical_research_usability}\n"
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
        "artifact_invariant": "PASS",
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

    timing = tuple(
        EventTimingMetrics(
            exchange_timestamp=(
                datetime.fromisoformat(item["exchange_timestamp"])
                if item["exchange_timestamp"] is not None
                else None
            ),
            received_at=datetime.fromisoformat(item["received_at"]),
            available_at=datetime.fromisoformat(item["available_at"]),
            exchange_server_time=(
                datetime.fromisoformat(item["exchange_server_time"])
                if item["exchange_server_time"] is not None
                else None
            ),
            event_age_seconds=(
                Decimal(item["event_age_seconds"])
                if item["event_age_seconds"] is not None
                else None
            ),
            transport_latency_seconds=(
                Decimal(item["transport_latency_seconds"])
                if item["transport_latency_seconds"] is not None
                else None
            ),
            clock_skew_seconds=(
                Decimal(item["clock_skew_seconds"])
                if item["clock_skew_seconds"] is not None
                else None
            ),
            event_id=str(item.get("event_id", "")),
            timestamp_semantic=str(
                item.get("timestamp_semantic", TimestampSemantic.RECEIPT_ONLY.value)
            ),
            availability_provenance=str(
                item.get("availability_provenance", AvailabilityProvenance.UNKNOWN.value)
            ),
            source_endpoint=str(item.get("source_endpoint", "unknown")),
            raw_payload_sha256=str(item.get("raw_payload_sha256", "")),
            clock_skew_formula=item.get("clock_skew_formula"),
        )
        for item in raw.get("timing", ())
    )
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
        live_out_of_order_count=int(raw.get("live_out_of_order_count", 0)),
        historical_source_order_reversed=int(raw.get("historical_source_order_reversed", 0)),
        historical_duplicate_count=int(raw.get("historical_duplicate_count", 0)),
        historical_timestamp_collision_count=int(
            raw.get("historical_timestamp_collision_count", 0)
        ),
        clock_skew_unknown_count=int(raw.get("clock_skew_unknown_count", 0)),
        clock_skew_known_sample_count=int(raw.get("clock_skew_known_sample_count", 0)),
        clock_skew_violation_count=int(raw.get("clock_skew_violation_count", 0)),
        clock_skew_violation_ratio=(
            Decimal(raw["clock_skew_violation_ratio"])
            if raw.get("clock_skew_violation_ratio") is not None
            else None
        ),
        median_clock_skew_ms=(
            Decimal(raw["median_clock_skew_ms"])
            if raw.get("median_clock_skew_ms") is not None
            else None
        ),
        p95_clock_skew_ms=(
            Decimal(raw["p95_clock_skew_ms"]) if raw.get("p95_clock_skew_ms") is not None else None
        ),
        clock_skew_threshold_ms=Decimal(raw.get("clock_skew_threshold_ms", "5000")),
        clock_skew_minimum_sample_count=int(raw.get("clock_skew_minimum_sample_count", 1)),
        clock_skew_maximum_violation_ratio=Decimal(
            raw.get("clock_skew_maximum_violation_ratio", "0.05")
        ),
        candle_interval_alignment_violation_count=int(
            raw.get("candle_interval_alignment_violation_count", 0)
        ),
        candle_missing_count=int(raw.get("candle_missing_count", 0)),
        future_timestamp_count=int(raw.get("future_timestamp_count", 0)),
        timing=timing,
    )
    funding_raw = data.get("funding_interval")
    funding_interval = None
    if funding_raw is not None:
        funding_interval = FundingIntervalResult(
            observations=tuple(
                FundingIntervalObservation(
                    previous_effective_at=datetime.fromisoformat(item["previous_effective_at"]),
                    current_effective_at=datetime.fromisoformat(item["current_effective_at"]),
                    expected_interval_seconds=int(item["expected_interval_seconds"]),
                    actual_interval_seconds=int(item["actual_interval_seconds"]),
                    difference_seconds=int(item["difference_seconds"]),
                    missing_window_count=int(item["missing_window_count"]),
                    duplicate_window=bool(item["duplicate_window"]),
                    schedule_change=bool(item["schedule_change"]),
                )
                for item in funding_raw["observations"]
            ),
            duplicate_count=int(funding_raw["duplicate_count"]),
            missing_window_count=(
                int(funding_raw["missing_window_count"])
                if funding_raw["missing_window_count"] is not None
                else None
            ),
            schedule_change_count=int(funding_raw["schedule_change_count"]),
            violations=tuple(funding_raw["violations"]),
            insufficiencies=tuple(funding_raw["insufficiencies"]),
            window_assessment=str(funding_raw.get("window_assessment", "EVALUATED")),
        )
    contract_raw = data.get("capability_contract")
    research_raw = data.get("historical_research_usability")
    return CertificationEvidence(
        **{
            **data,
            "metrics": metrics,
            "event_manifest": tuple(tuple(item) for item in data["event_manifest"]),
            "funding_interval": funding_interval,
            "capability_contract": (
                CapabilityContractVerdict(**contract_raw) if contract_raw is not None else None
            ),
            "historical_research_usability": (
                HistoricalResearchUsability(
                    point_in_time_available=research_raw["point_in_time_available"],
                    availability_provenance=AvailabilityProvenance(
                        research_raw["availability_provenance"]
                    ),
                    verdict=CertificationVerdict(research_raw["verdict"]),
                    reasons=tuple(research_raw["reasons"]),
                )
                if research_raw is not None
                else None
            ),
        }
    )
