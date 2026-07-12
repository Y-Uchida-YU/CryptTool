from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.market_data.models import Side


class PaperOrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class PaperOrderStatus(StrEnum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class PaperOrderRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    client_order_id: str = Field(min_length=1, max_length=100)
    symbol: str = Field(min_length=1, max_length=40)
    side: Side
    quantity: Decimal = Field(gt=0)
    order_type: PaperOrderType = PaperOrderType.MARKET
    limit_price: Decimal | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def require_limit_price(self) -> "PaperOrderRequest":
        if self.order_type == PaperOrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit orders require limit_price")
        if self.order_type == PaperOrderType.MARKET and self.limit_price is not None:
            raise ValueError("market orders cannot specify limit_price")
        return self


class PaperQuote(BaseModel):
    model_config = ConfigDict(frozen=True)
    symbol: str
    timestamp: datetime
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)
    bid_size: Decimal = Field(gt=0)
    ask_size: Decimal = Field(gt=0)
    data_quality_score: float = Field(ge=0, le=1)
    sequence: int | None = None

    @field_validator("timestamp")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def valid_spread(self) -> "PaperQuote":
        if self.bid >= self.ask:
            raise ValueError("quote must have a positive spread")
        return self


class PaperOrder(BaseModel):
    request: PaperOrderRequest
    submitted_at: datetime
    status: PaperOrderStatus = PaperOrderStatus.OPEN
    filled_quantity: Decimal = Decimal("0")
    rejection_reason: str | None = None

    @field_validator("submitted_at")
    @classmethod
    def submitted_at_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("order timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @property
    def remaining_quantity(self) -> Decimal:
        return self.request.quantity - self.filled_quantity


class PaperFill(BaseModel):
    fill_id: str
    client_order_id: str
    symbol: str
    side: Side
    timestamp: datetime
    quantity: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    fee: Decimal = Field(ge=0)
    slippage_cost: Decimal = Field(ge=0)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("fill timestamp must be timezone-aware")
        return value.astimezone(UTC)


class PaperPosition(BaseModel):
    symbol: str
    quantity: Decimal = Decimal("0")
    average_entry: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    funding_pnl: Decimal = Decimal("0")


class PaperAccountSnapshot(BaseModel):
    timestamp: datetime
    cash: Decimal
    equity: Decimal
    gross_exposure: Decimal = Field(ge=0)
    fees_paid: Decimal = Field(ge=0)
    funding_pnl: Decimal
    open_orders: int = Field(ge=0)
    positions: dict[str, Decimal]

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("snapshot timestamp must be timezone-aware")
        return value.astimezone(UTC)


class PaperAuditEvent(BaseModel):
    event_id: str
    timestamp: datetime
    event_type: str
    entity_id: str | None = None
    payload: dict[str, str]

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("audit timestamp must be timezone-aware")
        return value.astimezone(UTC)
