from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Market(BaseModel):
    exchange: str
    symbol: str
    base: str
    quote: str
    market_type: str
    tick_size: Decimal | None = None
    lot_size: Decimal | None = None
    minimum_notional: Decimal | None = None


class TimedModel(BaseModel):
    model_config = ConfigDict(frozen=True)
    exchange: str
    symbol: str
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)


class OHLCV(TimedModel):
    timeframe: str
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    closed: bool = True

    @model_validator(mode="after")
    def validate_ohlc(self) -> "OHLCV":
        if (
            self.high < max(self.open, self.close)
            or self.low > min(self.open, self.close)
            or self.low > self.high
        ):
            raise ValueError("inconsistent OHLC values")
        return self


class FundingRate(TimedModel):
    rate: Decimal
    predicted_rate: Decimal | None = None
    next_funding_at: datetime | None = None


class OpenInterest(TimedModel):
    value: Decimal = Field(ge=0)
    unit: str


class Trade(TimedModel):
    trade_id: str
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    side: Side


class OrderBookLevel(BaseModel):
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)


class OrderBook(TimedModel):
    sequence: int | None = None
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]

    @model_validator(mode="after")
    def validate_book(self) -> "OrderBook":
        if not self.bids or not self.asks:
            raise ValueError("both sides of order book are required")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("crossed or locked order book")
        return self


class DataQualityIssue(BaseModel):
    code: str
    severity: str
    timestamp: datetime | None = None
    field: str | None = None
    original_value: str | None = None
    corrected_value: str | None = None
    reason: str
