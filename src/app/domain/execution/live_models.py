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
    def validate_order(self) -> "LiveOrderRequest":
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
    def accepted_orders_require_external_identity(self) -> "ExecutionOrderAck":
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

    external_order_id: str
    exchange: str
    symbol: str
    side: Side
    quantity: Decimal = Field(gt=0)
    reduce_only: bool
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("open-order timestamp must be timezone-aware")
        return value.astimezone(UTC)


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
