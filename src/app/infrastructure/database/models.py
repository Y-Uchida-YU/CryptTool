from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, Float, Numeric, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class OHLCVRow(Base):
    __tablename__ = "ohlcv"
    __table_args__ = (UniqueConstraint("exchange", "symbol", "timeframe", "timestamp"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(40), index=True)
    symbol: Mapped[str] = mapped_column(String(40), index=True)
    timeframe: Mapped[str] = mapped_column(String(8))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    exchange_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    local_monotonic_time: Mapped[float | None] = mapped_column(Float)
    clock_offset_estimate: Mapped[float | None] = mapped_column(Float)
    open: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    high: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    low: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    close: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    volume: Mapped[Decimal] = mapped_column(Numeric(38, 12))


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    entity_id: Mapped[str | None] = mapped_column(String(100))
    payload_json: Mapped[str]
    model_version: Mapped[str | None] = mapped_column(String(80))
    config_version: Mapped[str | None] = mapped_column(String(80))


class PreflightBindingRow(Base):
    __tablename__ = "preflight_bindings"
    __table_args__ = (
        CheckConstraint(
            "state IN ('unbound','reserved','first_leg_accepted','second_leg_submitted',"
            "'hedging_required','reconciliation_required','completed','aborted','halted')",
            name="ck_preflight_bindings_state",
        ),
        CheckConstraint("version >= 1", name="ck_preflight_bindings_version"),
        CheckConstraint("updated_at >= created_at", name="ck_preflight_bindings_timestamp_order"),
    )
    signal_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    preflight_hash: Mapped[str | None] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(40), index=True)
    first_leg_role: Mapped[str | None] = mapped_column(String(20))
    first_order_request_id: Mapped[str | None] = mapped_column(String(100))
    first_external_order_id: Mapped[str | None] = mapped_column(String(160))
    second_order_request_id: Mapped[str | None] = mapped_column(String(100))
    second_external_order_id: Mapped[str | None] = mapped_column(String(160))
    version: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    failure_reason: Mapped[str | None] = mapped_column(String(500))
    position_venue: Mapped[str | None] = mapped_column(String(40))
    position_symbol: Mapped[str | None] = mapped_column(String(40))
    position_quantity_before: Mapped[Decimal | None] = mapped_column(Numeric(38, 12))
    position_captured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
