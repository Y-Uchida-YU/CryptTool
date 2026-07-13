from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.execution.models import OrderType
from app.domain.market_data.evidence import CrossVenueSignalEvidence, LegDataEvidence
from app.domain.market_data.models import Side


class LiveOrderState(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    DRY_RUN = "dry_run"
    CANCELED = "canceled"


class ReconciledOrderStatus(StrEnum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class PreflightRecoveryAction(StrEnum):
    ALTERNATE_HEDGE = "alternate_hedge"
    PARTIAL_HEDGE = "partial_hedge"
    FIRST_LEG_UNWIND = "first_leg_unwind"


@dataclass(frozen=True)
class CrossVenueExecutionPreflight:
    issuer_id: str
    issued_at: datetime
    commit_sha: str
    snapshot_ids: tuple[str, ...]
    signal_id: str
    receive_venue: str
    pay_venue: str
    canonical_instrument_id: str
    receive_capability_hash: str
    pay_capability_hash: str
    receive_source_event_hash: str
    pay_source_event_hash: str
    receive_execution_health: bool
    pay_execution_health: bool
    receive_available_collateral: Decimal
    pay_available_collateral: Decimal
    receive_fillable_quantity: Decimal
    pay_fillable_quantity: Decimal
    receive_expected_vwap: Decimal
    pay_expected_vwap: Decimal
    maximum_naked_exposure_duration_ms: int
    created_at: datetime
    expires_at: datetime
    preflight_hash: str
    signature: str


class LiveOrderRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str = Field(min_length=8, max_length=100)
    idempotency_key: str = Field(min_length=16, max_length=128)
    signal_id: str = Field(min_length=8, max_length=100)
    strategy_id: str = Field(min_length=1, max_length=100)
    strategy_version: str = Field(min_length=1, max_length=40)
    cross_venue_signal_evidence: CrossVenueSignalEvidence
    cross_venue_signal_hash: str = Field(min_length=64, max_length=64)
    order_leg_role: str = Field(pattern="^(receive_leg|pay_leg)$")
    order_leg_evidence: LegDataEvidence
    cross_venue_preflight: CrossVenueExecutionPreflight
    preflight_recovery_action: PreflightRecoveryAction | None = None
    required_capabilities: tuple[str, ...] = Field(min_length=1)
    risk_decision_id: str = Field(min_length=8, max_length=100)
    model_version: str = Field(min_length=1, max_length=100)
    config_version: str = Field(min_length=1, max_length=100)
    exchange: str = Field(min_length=1, max_length=40)
    symbol: str = Field(min_length=1, max_length=40)
    side: Side
    quantity: Decimal = Field(gt=0)
    reference_price: Decimal = Field(gt=0)
    maximum_slippage_bps: Decimal = Field(default=Decimal("10"), gt=0, le=100)
    order_type: OrderType = OrderType.MARKET
    limit_price: Decimal | None = Field(default=None, gt=0)
    reduce_only: bool = False
    created_at: datetime
    expires_at: datetime

    @field_validator("created_at", "expires_at")
    @classmethod
    def timestamps_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("live order timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_order(self) -> LiveOrderRequest:
        if self.expires_at <= self.created_at:
            raise ValueError("live order expiry must follow creation")
        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit live orders require limit_price")
        if self.order_type == OrderType.MARKET and self.limit_price is not None:
            raise ValueError("market live orders cannot specify limit_price")
        return self

    @property
    def reference_notional(self) -> Decimal:
        return self.quantity * self.reference_price


class ExecutionOrderAck(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str
    external_order_id: str | None = None
    state: LiveOrderState
    accepted_at: datetime
    reason: str
    adapter_called: bool

    @field_validator("accepted_at")
    @classmethod
    def accepted_at_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("ack timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def accepted_orders_require_external_identity(self) -> ExecutionOrderAck:
        if self.state == LiveOrderState.ACCEPTED:
            if not self.adapter_called or not self.external_order_id:
                raise ValueError(
                    "accepted live orders require adapter_called and an external_order_id"
                )
        elif self.external_order_id is not None and not self.adapter_called:
            raise ValueError("an external_order_id requires an adapter call")
        return self


class CancelAck(BaseModel):
    model_config = ConfigDict(frozen=True)

    external_order_id: str
    canceled: bool
    timestamp: datetime
    reason: str

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("cancel timestamp must be timezone-aware")
        return value.astimezone(UTC)


class LiveOpenOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    client_order_id: str = Field(min_length=1)
    external_order_id: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    side: Side
    quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    price: Decimal = Field(gt=0)
    reduce_only: bool
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("open-order timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def filled_quantity_cannot_exceed_quantity(self) -> LiveOpenOrder:
        if self.filled_quantity > self.quantity:
            raise ValueError("filled quantity cannot exceed order quantity")
        return self


@dataclass(frozen=True)
class ExecutionFill:
    """Exchange fill with positive fees paid and negative maker rebates received."""

    fill_id: str
    exchange_order_id: str
    client_order_id: str
    exchange: str
    symbol: str
    side: Side
    price: Decimal
    quantity: Decimal
    signed_fee: Decimal
    executed_at: datetime
    liquidity_role: str | None = None

    def __post_init__(self) -> None:
        if (
            not self.fill_id
            or not self.exchange_order_id
            or not self.client_order_id
            or not self.exchange
            or not self.symbol
        ):
            raise ValueError("fill and order identities are required")
        if self.price <= 0 or self.quantity <= 0:
            raise ValueError("fill price and quantity are invalid")
        if self.executed_at.tzinfo is None:
            raise ValueError("fill timestamp must be timezone-aware")
        object.__setattr__(self, "executed_at", self.executed_at.astimezone(UTC))


@dataclass(frozen=True)
class ReconciledOrder:
    client_order_id: str
    external_order_id: str
    exchange: str
    symbol: str
    side: Side
    original_quantity: Decimal
    filled_quantity: Decimal
    average_fill_price: Decimal | None
    reduce_only: bool
    status: ReconciledOrderStatus
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if not all((self.client_order_id, self.external_order_id, self.exchange, self.symbol)):
            raise ValueError("reconciled order identities are required")
        if (
            self.original_quantity <= 0
            or self.filled_quantity < 0
            or self.filled_quantity > self.original_quantity
        ):
            raise ValueError("reconciled order quantities are invalid")
        if self.average_fill_price is not None and self.average_fill_price <= 0:
            raise ValueError("average fill price must be positive")
        if self.filled_quantity > 0 and self.average_fill_price is None:
            raise ValueError("filled reconciled orders require an average fill price")
        if self.created_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("reconciled order timestamps must be timezone-aware")
        created_at = self.created_at.astimezone(UTC)
        updated_at = self.updated_at.astimezone(UTC)
        if updated_at < created_at:
            raise ValueError("reconciled order update cannot precede creation")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)


class LivePosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    exchange: str
    symbol: str
    quantity: Decimal
    mark_price: Decimal = Field(gt=0)
    unrealized_pnl: Decimal
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def observed_at_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("position timestamp must be timezone-aware")
        return value.astimezone(UTC)


class ExecutionAuditEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str
    timestamp: datetime
    event_type: str
    request_id: str | None = None
    allowed: bool
    reason: str
    model_version: str
    config_version: str
    details: dict[str, str] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("audit timestamp must be timezone-aware")
        return value.astimezone(UTC)
