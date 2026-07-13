from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from uuid import UUID

from app.adapters.exchanges.websocket import ReconciliationState
from app.domain.venues.models import CapabilitySupport, CapabilityUseCase


@dataclass(frozen=True)
class SourceEventEvidence:
    event_id: str
    venue: str
    symbol: str
    event_type: str
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    payload_sha256: str
    sequence: int | None
    connection_id: UUID | None
    reconciliation_state: ReconciliationState | None
    data_quality_score: float

    def __post_init__(self) -> None:
        for name in ("exchange_timestamp", "received_at", "available_at"):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware")
            if value is not None:
                object.__setattr__(self, name, value.astimezone(UTC))
        if not 0 <= self.data_quality_score <= 1:
            raise ValueError("data quality score must be between zero and one")


@dataclass(frozen=True)
class CapabilityEvidence:
    venue: str
    capability: str
    use_case: CapabilityUseCase
    support: CapabilitySupport
    verified_at: datetime
    verification_run_id: str
    source_events: tuple[SourceEventEvidence, ...]

    def __post_init__(self) -> None:
        if self.verified_at.tzinfo is None:
            raise ValueError("verified_at must be timezone-aware")
        object.__setattr__(self, "verified_at", self.verified_at.astimezone(UTC))
        if not self.verification_run_id or not self.source_events:
            raise ValueError("verification run and source events are required")


@dataclass(frozen=True)
class SignalDataEvidence:
    signal_id: str
    capabilities: tuple[CapabilityEvidence, ...]
    evidence_hash: str

    @classmethod
    def build(
        cls, signal_id: str, capabilities: tuple[CapabilityEvidence, ...]
    ) -> SignalDataEvidence:
        payload = json.dumps(
            [asdict(item) for item in capabilities],
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
        return cls(signal_id, capabilities, hashlib.sha256(payload).hexdigest())

    def valid_hash(self) -> bool:
        return self == self.build(self.signal_id, self.capabilities)


@dataclass(frozen=True)
class LegDataEvidence:
    role: str
    venue: str
    capabilities: tuple[CapabilityEvidence, ...]
    evidence_hash: str

    @classmethod
    def build(
        cls, role: str, venue: str, capabilities: tuple[CapabilityEvidence, ...]
    ) -> LegDataEvidence:
        payload = json.dumps(
            {"role": role, "venue": venue, "capabilities": [asdict(x) for x in capabilities]},
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
        return cls(role, venue, capabilities, hashlib.sha256(payload).hexdigest())

    def valid_hash(self) -> bool:
        return self == self.build(self.role, self.venue, self.capabilities)


@dataclass(frozen=True)
class CrossVenueSignalEvidence:
    signal_id: str
    receive_leg: LegDataEvidence | None
    pay_leg: LegDataEvidence | None
    evidence_hash: str

    @classmethod
    def build(
        cls,
        signal_id: str,
        receive_leg: LegDataEvidence | None,
        pay_leg: LegDataEvidence | None,
    ) -> CrossVenueSignalEvidence:
        payload = json.dumps(
            {
                "signal_id": signal_id,
                "receive_leg": asdict(receive_leg) if receive_leg is not None else None,
                "pay_leg": asdict(pay_leg) if pay_leg is not None else None,
            },
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
        return cls(signal_id, receive_leg, pay_leg, hashlib.sha256(payload).hexdigest())

    def valid_hash(self) -> bool:
        return self == self.build(self.signal_id, self.receive_leg, self.pay_leg)

    def leg(self, role: str) -> LegDataEvidence | None:
        return {"receive_leg": self.receive_leg, "pay_leg": self.pay_leg}.get(role)


@dataclass(frozen=True)
class RejectedSignal:
    signal_id: str
    reason: str
    missing_capabilities: tuple[str, ...]


def require_signal_capabilities(
    signal_id: str,
    evidence: SignalDataEvidence,
    required: tuple[str, ...],
    venue: str,
    now: datetime,
    maximum_age_seconds: int,
) -> RejectedSignal | None:
    keys = [(item.venue, item.capability) for item in evidence.capabilities]
    if len(keys) != len(set(keys)):
        return RejectedSignal(signal_id, "duplicate venue and capability evidence", required)
    available = {(item.venue, item.capability): item for item in evidence.capabilities}
    missing = tuple(
        name
        for name in required
        if (venue, name) not in available
        or available[(venue, name)].use_case != CapabilityUseCase.SIGNAL_GENERATION
        or available[(venue, name)].support != CapabilitySupport.LIVE_VERIFIED
        or not (
            0
            <= (now.astimezone(UTC) - available[(venue, name)].verified_at).total_seconds()
            <= maximum_age_seconds
        )
    )
    if evidence.signal_id != signal_id or not evidence.valid_hash():
        return RejectedSignal(signal_id, "invalid signal evidence identity or hash", required)
    return (
        RejectedSignal(signal_id, "capability evidence is missing or stale", missing)
        if missing
        else None
    )
