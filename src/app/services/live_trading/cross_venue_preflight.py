from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from app.domain.execution.live_models import CrossVenueExecutionPreflight


class PreflightBindingState(StrEnum):
    UNBOUND = "unbound"
    RESERVED = "reserved"
    FIRST_LEG_ACCEPTED = "first_leg_accepted"
    COMPLETED = "completed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class PreflightBinding:
    signal_id: str
    preflight_hash: str | None
    state: PreflightBindingState
    first_leg_role: str | None = None


class CrossVenuePreflightService:
    """The sole trusted issuer and verifier of cross-venue execution snapshots."""

    def __init__(self, *, issuer_id: str, signing_key: bytes, commit_sha: str) -> None:
        if not issuer_id or len(signing_key) < 32 or not commit_sha:
            raise ValueError("preflight issuer, 32-byte signing key, and commit SHA are required")
        self.issuer_id = issuer_id
        self._signing_key = signing_key
        self.commit_sha = commit_sha

    def issue(self, **values: Any) -> CrossVenueExecutionPreflight:
        snapshot_ids = tuple(values.get("snapshot_ids", ()))
        if not snapshot_ids or len(snapshot_ids) != len(set(snapshot_ids)):
            raise ValueError("preflight snapshot ids must be non-empty and unique")
        payload = {
            **values,
            "issuer_id": self.issuer_id,
            "commit_sha": self.commit_sha,
            "snapshot_ids": snapshot_ids,
        }
        preflight_hash = self._hash(payload)
        return CrossVenueExecutionPreflight(
            **payload,
            preflight_hash=preflight_hash,
            signature=self._signature(preflight_hash),
        )

    def verify(self, preflight: CrossVenueExecutionPreflight, now: datetime) -> None:
        if preflight.issuer_id != self.issuer_id:
            raise ValueError("preflight issuer is not trusted")
        if preflight.commit_sha != self.commit_sha:
            raise ValueError("preflight commit SHA does not match deployment")
        if not preflight.snapshot_ids or len(preflight.snapshot_ids) != len(
            set(preflight.snapshot_ids)
        ):
            raise ValueError("preflight snapshot ids are invalid")
        if not preflight.issued_at <= now:
            raise ValueError("preflight issuance is future-dated")
        values = asdict(preflight)
        signature = values.pop("signature")
        claimed_hash = values.pop("preflight_hash")
        actual_hash = self._hash(values)
        if not hmac.compare_digest(claimed_hash, actual_hash):
            raise ValueError("preflight hash is invalid")
        if not hmac.compare_digest(signature, self._signature(actual_hash)):
            raise ValueError("preflight signature is invalid")

    @staticmethod
    def _hash(values: dict[str, Any]) -> str:
        payload = json.dumps(values, sort_keys=True, default=str, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    def _signature(self, preflight_hash: str) -> str:
        return hmac.new(self._signing_key, preflight_hash.encode(), hashlib.sha256).hexdigest()
