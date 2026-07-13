from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
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
        CheckConstraint(
            "(position_venue IS NULL AND position_symbol IS NULL "
            "AND position_quantity_before IS NULL AND position_captured_at IS NULL) OR "
            "(position_venue IS NOT NULL AND position_symbol IS NOT NULL "
            "AND position_quantity_before IS NOT NULL AND position_captured_at IS NOT NULL)",
            name="ck_preflight_bindings_position_snapshot_complete",
        ),
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


class RawMarketEventRow(Base):
    __tablename__ = "raw_market_events"
    event_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    canonical_instrument_id: Mapped[str] = mapped_column(String(100), index=True)
    venue_symbol: Mapped[str] = mapped_column(String(100))
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    exchange_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    sequence: Mapped[int | None] = mapped_column(BigInteger)
    connection_id: Mapped[str | None] = mapped_column(String(36))
    reconciliation_state: Mapped[str | None] = mapped_column(String(40))
    payload_sha256: Mapped[str] = mapped_column(String(64))
    raw_payload: Mapped[str] = mapped_column(Text)
    normalizer_version: Mapped[str] = mapped_column(String(80))
    capability_verification_run_id: Mapped[str] = mapped_column(String(160))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class MarketDataQuarantineRow(Base):
    __tablename__ = "market_data_quarantine"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(160), index=True)
    reason: Mapped[str] = mapped_column(String(500))
    raw_payload: Mapped[str] = mapped_column(Text)
    quarantined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class MarketDataCheckpointRow(Base):
    __tablename__ = "market_data_checkpoints"
    id: Mapped[int] = mapped_column(primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    connection_id: Mapped[str] = mapped_column(String(36))
    last_sequence: Mapped[int | None] = mapped_column(BigInteger)
    last_event_id: Mapped[str | None] = mapped_column(String(160))
    reconciliation_state: Mapped[str] = mapped_column(String(40))
    checkpointed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DataSnapshotRow(Base):
    __tablename__ = "data_snapshots"
    snapshot_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    event_count: Mapped[int] = mapped_column(Integer)
    content_sha256: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ResearchRunRow(Base):
    __tablename__ = "research_runs"
    run_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    commit_sha: Mapped[str] = mapped_column(String(80))
    config_sha256: Mapped[str] = mapped_column(String(64))
    data_snapshot_id: Mapped[str] = mapped_column(String(160), index=True)
    hypothesis_version: Mapped[str] = mapped_column(String(80))
    strategy_id: Mapped[str] = mapped_column(String(100))
    strategy_version: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(40))
    acceptance_verdict: Mapped[str | None] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FrozenHypothesisRow(Base):
    __tablename__ = "frozen_hypotheses"
    hypothesis_version: Mapped[str] = mapped_column(String(80), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(100))
    content_sha256: Mapped[str] = mapped_column(String(64))
    content_json: Mapped[str] = mapped_column(Text)
    frozen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ResearchArtifactRow(Base):
    __tablename__ = "research_artifacts"
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(String(160), index=True)
    data_snapshot_id: Mapped[str] = mapped_column(String(160), index=True)
    artifact_type: Mapped[str] = mapped_column(String(80))
    path: Mapped[str] = mapped_column(String(500))
    content_sha256: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
