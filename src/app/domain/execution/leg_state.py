from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LegExecutionState(StrEnum):
    PLANNED = "planned"
    FIRST_SUBMITTED = "first_submitted"
    FIRST_PARTIAL = "first_partial"
    FIRST_FILLED = "first_filled"
    SECOND_SUBMITTED = "second_submitted"
    HEDGED = "hedged"
    UNWINDING = "unwinding"
    COMPLETED = "completed"
    HALTED = "halted"


class PreflightChangeAction(StrEnum):
    CHASE = "chase"
    PARTIAL_HEDGE = "partial_hedge"
    ALTERNATE_HEDGE = "alternate_hedge"
    UNWIND = "unwind"
    HALT = "halt"


class LegRiskPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)
    first_leg_timeout: timedelta = timedelta(seconds=3)
    second_leg_chase_limit_bps: Decimal = Field(default=Decimal("15"), ge=0)
    maximum_naked_exposure: Decimal = Field(gt=0)
    emergency_hedge_venue: str
    unwind_policy: str = "cancel_remainder_then_reduce_filled_leg"


class LegExecutionSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)
    state: LegExecutionState
    started_at: datetime
    updated_at: datetime
    target_quantity: Decimal = Field(gt=0)
    first_filled_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    second_filled_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    reference_price: Decimal = Field(gt=0)
    reason: str

    @field_validator("started_at", "updated_at")
    @classmethod
    def timestamps_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("leg timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @property
    def naked_exposure(self) -> Decimal:
        return abs(self.first_filled_quantity - self.second_filled_quantity) * self.reference_price


class LegExecutionMachine:
    def __init__(
        self, policy: LegRiskPolicy, started_at: datetime, quantity: Decimal, price: Decimal
    ):
        self.policy = policy
        self.snapshot = LegExecutionSnapshot(
            state=LegExecutionState.PLANNED,
            started_at=started_at,
            updated_at=started_at,
            target_quantity=quantity,
            reference_price=price,
            reason="planned",
        )

    def submit_first(self, now: datetime) -> LegExecutionSnapshot:
        return self._transition(LegExecutionState.FIRST_SUBMITTED, now, "first leg submitted")

    def submit_second(self, now: datetime, chase_price: Decimal) -> LegExecutionSnapshot:
        if chase_price <= 0:
            raise ValueError("chase price must be positive")
        deviation_bps = (
            abs(chase_price - self.snapshot.reference_price)
            / self.snapshot.reference_price
            * Decimal("10000")
        )
        if deviation_bps > self.policy.second_leg_chase_limit_bps:
            return self._transition(
                LegExecutionState.UNWINDING,
                now,
                "second-leg chase limit exceeded; " + self.policy.unwind_policy,
            )
        return self._transition(LegExecutionState.SECOND_SUBMITTED, now, "second leg submitted")

    def reconcile(
        self, now: datetime, first_filled: Decimal, second_filled: Decimal
    ) -> LegExecutionSnapshot:
        if (
            first_filled < self.snapshot.first_filled_quantity
            or second_filled < self.snapshot.second_filled_quantity
        ):
            raise ValueError("cumulative fills cannot decrease")
        if (
            first_filled > self.snapshot.target_quantity
            or second_filled > self.snapshot.target_quantity
        ):
            raise ValueError("filled quantity exceeds target")
        state = self.snapshot.state
        reason = "partial fills reconciled"
        if (
            first_filled == self.snapshot.target_quantity
            and second_filled == self.snapshot.target_quantity
        ):
            state, reason = LegExecutionState.COMPLETED, "both legs filled"
        elif second_filled > 0 and second_filled == first_filled:
            state, reason = LegExecutionState.HEDGED, "filled exposure is hedged"
        elif first_filled == self.snapshot.target_quantity:
            state = LegExecutionState.FIRST_FILLED
        elif first_filled > 0:
            state = LegExecutionState.FIRST_PARTIAL
        updated = self.snapshot.model_copy(
            update={
                "state": state,
                "updated_at": _utc(now),
                "first_filled_quantity": first_filled,
                "second_filled_quantity": second_filled,
                "reason": reason,
            }
        )
        if updated.naked_exposure > self.policy.maximum_naked_exposure:
            updated = updated.model_copy(
                update={
                    "state": LegExecutionState.HALTED,
                    "reason": "maximum naked exposure exceeded",
                }
            )
        self.snapshot = updated
        return updated

    def check_timeout(self, now: datetime) -> LegExecutionSnapshot:
        now = _utc(now)
        if (
            self.snapshot.state
            in {LegExecutionState.FIRST_SUBMITTED, LegExecutionState.FIRST_PARTIAL}
            and now - self.snapshot.started_at >= self.policy.first_leg_timeout
        ):
            return self._transition(LegExecutionState.UNWINDING, now, self.policy.unwind_policy)
        return self.snapshot

    def handle_preflight_change(
        self,
        now: datetime,
        *,
        evidence_valid: bool,
        execution_health: bool,
        available_collateral: Decimal,
        fillable_quantity: Decimal,
        expected_vwap: Decimal,
        alternate_hedge_available: bool,
    ) -> PreflightChangeAction:
        if not evidence_valid:
            self._transition(LegExecutionState.HALTED, now, "changed evidence is untrusted")
            return PreflightChangeAction.HALT
        if not execution_health and alternate_hedge_available:
            self._transition(LegExecutionState.SECOND_SUBMITTED, now, "alternate hedge selected")
            return PreflightChangeAction.ALTERNATE_HEDGE
        if not execution_health or available_collateral <= 0 or fillable_quantity <= 0:
            self._transition(LegExecutionState.UNWINDING, now, self.policy.unwind_policy)
            return PreflightChangeAction.UNWIND
        if fillable_quantity < self.snapshot.target_quantity:
            self._transition(LegExecutionState.SECOND_SUBMITTED, now, "partial hedge selected")
            return PreflightChangeAction.PARTIAL_HEDGE
        deviation_bps = (
            abs(expected_vwap - self.snapshot.reference_price)
            / self.snapshot.reference_price
            * Decimal("10000")
        )
        if deviation_bps <= self.policy.second_leg_chase_limit_bps:
            self._transition(LegExecutionState.SECOND_SUBMITTED, now, "chase selected")
            return PreflightChangeAction.CHASE
        self._transition(LegExecutionState.UNWINDING, now, self.policy.unwind_policy)
        return PreflightChangeAction.UNWIND

    def _transition(
        self, state: LegExecutionState, now: datetime, reason: str
    ) -> LegExecutionSnapshot:
        self.snapshot = self.snapshot.model_copy(
            update={"state": state, "updated_at": _utc(now), "reason": reason}
        )
        return self.snapshot


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
