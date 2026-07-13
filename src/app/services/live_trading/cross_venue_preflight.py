from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from threading import Lock
from typing import Any, Protocol

from app.domain.execution.live_models import CrossVenueExecutionPreflight


class PreflightBindingState(StrEnum):
    UNBOUND = "unbound"
    RESERVED = "reserved"
    FIRST_LEG_ACCEPTED = "first_leg_accepted"
    SECOND_LEG_SUBMITTED = "second_leg_submitted"
    HEDGING_REQUIRED = "hedging_required"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    COMPLETED = "completed"
    ABORTED = "aborted"
    HALTED = "halted"


@dataclass(frozen=True)
class PositionReconciliationSnapshot:
    venue: str
    symbol: str
    quantity_before: Decimal
    captured_at: datetime

    def __post_init__(self) -> None:
        if not self.venue or not self.symbol:
            raise ValueError("position snapshot venue and symbol are required")
        if self.captured_at.tzinfo is None:
            raise ValueError("position snapshot timestamp must be timezone-aware")
        object.__setattr__(self, "captured_at", self.captured_at.astimezone(UTC))


@dataclass(frozen=True)
class PreflightBinding:
    signal_id: str
    preflight_hash: str | None
    state: PreflightBindingState
    first_leg_role: str | None = None
    first_order_request_id: str | None = None
    first_external_order_id: str | None = None
    second_order_request_id: str | None = None
    second_external_order_id: str | None = None
    version: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    failure_reason: str | None = None
    position_snapshot: PositionReconciliationSnapshot | None = None


class PreflightBindingRepository(Protocol):
    @property
    def durable(self) -> bool: ...

    def get(self, signal_id: str) -> PreflightBinding | None: ...

    def compare_and_set(
        self,
        signal_id: str,
        expected_version: int,
        binding: PreflightBinding,
    ) -> bool: ...


class InMemoryPreflightBindingRepository:
    """Thread-safe test/local repository with the same CAS contract as PostgreSQL."""

    def __init__(self) -> None:
        self._bindings: dict[str, PreflightBinding] = {}
        self._lock = Lock()

    @property
    def durable(self) -> bool:
        return False

    def get(self, signal_id: str) -> PreflightBinding | None:
        with self._lock:
            return self._bindings.get(signal_id)

    def compare_and_set(
        self, signal_id: str, expected_version: int, binding: PreflightBinding
    ) -> bool:
        with self._lock:
            current = self._bindings.get(signal_id)
            actual_version = current.version if current is not None else 0
            if (
                binding.signal_id != signal_id
                or actual_version != expected_version
                or binding.version != expected_version + 1
            ):
                return False
            self._bindings[signal_id] = binding
            return True


def new_binding(
    *,
    signal_id: str,
    preflight_hash: str,
    state: PreflightBindingState,
    now: datetime,
) -> PreflightBinding:
    now = now.astimezone(UTC)
    return PreflightBinding(
        signal_id=signal_id,
        preflight_hash=preflight_hash,
        state=state,
        version=1,
        created_at=now,
        updated_at=now,
    )


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
