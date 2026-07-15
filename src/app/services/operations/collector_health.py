from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from app.adapters.exchanges.websocket import ReconciliationState
from app.services.operations.models import CollectorHealthStatus, CollectorHealthSummary
from app.services.research.models import CollectionCheckpoint, CollectionFailureEvent

EXPECTED_SKIP_ERROR_TYPES = frozenset(
    {
        "CapabilityNotVerified",
        "ExpectedSkip",
        "InstrumentUnavailable",
        "NotImplementedError",
        "UnsupportedCapability",
    }
)
FATAL_ERROR_TYPES = frozenset(
    {
        "AuthenticationError",
        "DataCorruptionError",
        "PermissionError",
        "SchemaViolation",
    }
)


def summarize_collector_health(
    *,
    failures: Sequence[CollectionFailureEvent],
    checkpoints: Sequence[CollectionCheckpoint],
    now: datetime,
    production_market_event_count: int = 0,
    production_control_event_count: int = 0,
    experimental_market_event_count: int = 0,
) -> CollectorHealthSummary:
    if now.tzinfo is None:
        raise ValueError("collector health time must be timezone-aware")
    failures_by_venue: dict[str, int] = {}
    failures_by_instrument: dict[str, int] = {}
    failures_by_event_type: dict[str, int] = {}
    failures_by_error_type: dict[str, int] = {}
    expected_skips = 0
    fatal_failures = 0
    for failure in failures:
        _increment(failures_by_venue, failure.venue)
        _increment(failures_by_instrument, failure.instrument)
        _increment(failures_by_event_type, failure.event_type)
        _increment(failures_by_error_type, failure.error_type)
        if _is_expected_skip(failure):
            expected_skips += 1
        elif failure.error_type in FATAL_ERROR_TYPES:
            fatal_failures += 1
    degraded_failures = len(failures) - expected_skips - fatal_failures
    permanently_degraded = sum(
        checkpoint.reconciliation_state is ReconciliationState.DEGRADED
        and bool(checkpoint.last_recovery_failure)
        for checkpoint in checkpoints
    )
    recovery_required = sum(checkpoint.recovery_required for checkpoint in checkpoints)
    synchronized = tuple(
        checkpoint
        for checkpoint in checkpoints
        if checkpoint.reconciliation_state is ReconciliationState.SYNCHRONIZED
        and not checkpoint.recovery_required
    )
    lag = (
        max(
            Decimal(str(max(0.0, (now - item.checkpointed_at).total_seconds())))
            for item in checkpoints
        )
        if checkpoints
        else None
    )
    last_healthy_at = max((item.checkpointed_at for item in synchronized), default=None)
    reasons: list[str] = []
    if fatal_failures:
        status = CollectorHealthStatus.UNHEALTHY
        reasons.append(f"fatal collection failures={fatal_failures}")
    elif permanently_degraded or recovery_required:
        status = CollectorHealthStatus.DEGRADED
        if permanently_degraded:
            reasons.append(f"permanently degraded streams={permanently_degraded}")
        if recovery_required:
            reasons.append(f"recovery required streams={recovery_required}")
    elif not checkpoints:
        status = CollectorHealthStatus.INSUFFICIENT_EVIDENCE
        reasons.append("no collector checkpoints")
    else:
        status = CollectorHealthStatus.HEALTHY
        if degraded_failures:
            reasons.append(f"recovered/transient failures={degraded_failures}")
        if expected_skips:
            reasons.append(f"expected skips={expected_skips}")
    return CollectorHealthSummary(
        status=status,
        total_failures=len(failures),
        fatal_failures=fatal_failures,
        degraded_failures=degraded_failures,
        expected_skips=expected_skips,
        failures_by_venue=failures_by_venue,
        failures_by_instrument=failures_by_instrument,
        failures_by_event_type=failures_by_event_type,
        failures_by_error_type=failures_by_error_type,
        permanently_degraded_streams=permanently_degraded,
        recovery_required_streams=recovery_required,
        checkpoint_lag_max_seconds=lag,
        last_healthy_at=last_healthy_at,
        reasons=tuple(reasons),
        production_market_event_count=production_market_event_count,
        production_control_event_count=production_control_event_count,
        experimental_market_event_count=experimental_market_event_count,
    )


def _is_expected_skip(failure: CollectionFailureEvent) -> bool:
    if failure.error_type in EXPECTED_SKIP_ERROR_TYPES:
        return True
    message = failure.error_message.lower()
    return any(
        token in message
        for token in (
            "capability not verified",
            "instrument is not listed",
            "not supported by venue",
            "unsupported capability",
        )
    )


def _increment(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1
