from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from app.domain.venues.models import CapabilitySupport, CapabilityUseCase


@dataclass(frozen=True)
class CapabilityEvidence:
    venue: str
    capability: str
    use_case: CapabilityUseCase
    support: CapabilitySupport
    verified_at: datetime
    verification_run_id: str
    source_event_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.verified_at.tzinfo is None:
            raise ValueError("verified_at must be timezone-aware")
        object.__setattr__(self, "verified_at", self.verified_at.astimezone(UTC))
        if not self.verification_run_id or not self.source_event_ids:
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
    available = {item.capability: item for item in evidence.capabilities}
    missing = tuple(
        name
        for name in required
        if name not in available
        or available[name].venue != venue
        or available[name].use_case != CapabilityUseCase.SIGNAL_GENERATION
        or available[name].support != CapabilitySupport.LIVE_VERIFIED
        or (now.astimezone(UTC) - available[name].verified_at).total_seconds() > maximum_age_seconds
    )
    if evidence.signal_id != signal_id or not evidence.valid_hash():
        return RejectedSignal(signal_id, "invalid signal evidence identity or hash", required)
    return (
        RejectedSignal(signal_id, "capability evidence is missing or stale", missing)
        if missing
        else None
    )
