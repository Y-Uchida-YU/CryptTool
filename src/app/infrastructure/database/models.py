from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
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
    raw_payload_id: Mapped[str | None] = mapped_column(
        String(160), ForeignKey("raw_market_payloads.payload_id")
    )
    source_payload_sha256: Mapped[str | None] = mapped_column(String(64))
    channel: Mapped[str] = mapped_column(String(120), default="unknown")
    snapshot_sequence: Mapped[int | None] = mapped_column(BigInteger)
    delta_sequence: Mapped[int | None] = mapped_column(BigInteger)
    connection_epoch: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class RawMarketPayloadRow(Base):
    __tablename__ = "raw_market_payloads"
    payload_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    source_endpoint: Mapped[str] = mapped_column(String(500))
    payload_sha256: Mapped[str] = mapped_column(String(64), unique=True)
    raw_payload: Mapped[str] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ExperimentalMarketEventRow(Base):
    __tablename__ = "experimental_market_events"
    event_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    canonical_instrument_id: Mapped[str] = mapped_column(String(100), index=True)
    venue_symbol: Mapped[str] = mapped_column(String(100))
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    payload_sha256: Mapped[str] = mapped_column(String(64))
    raw_payload: Mapped[str] = mapped_column(Text)
    capability_support: Mapped[str] = mapped_column(String(40))
    capability_verification_run_id: Mapped[str | None] = mapped_column(String(160))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class MarketDataQuarantineRow(Base):
    __tablename__ = "market_data_quarantine"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(160), index=True)
    reason: Mapped[str] = mapped_column(String(500))
    raw_payload: Mapped[str] = mapped_column(Text)
    quarantined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class MarketDataCheckpointRow(Base):
    __tablename__ = "market_data_checkpoints"
    __table_args__ = (UniqueConstraint("venue", "stream_key"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    stream_key: Mapped[str] = mapped_column(String(200), default="default")
    connection_id: Mapped[str] = mapped_column(String(36))
    last_sequence: Mapped[int | None] = mapped_column(BigInteger)
    last_event_id: Mapped[str | None] = mapped_column(String(160))
    reconciliation_state: Mapped[str] = mapped_column(String(40))
    checkpointed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    canonical_instrument_id: Mapped[str] = mapped_column(String(100), default="SYSTEM")
    venue_symbol: Mapped[str] = mapped_column(String(100), default="SYSTEM")
    event_type: Mapped[str] = mapped_column(String(80), default="unknown")
    channel: Mapped[str] = mapped_column(String(120), default="unknown")
    last_available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_funding_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_trade_id: Mapped[str | None] = mapped_column(String(200))
    snapshot_sequence: Mapped[int | None] = mapped_column(BigInteger)
    delta_sequence: Mapped[int | None] = mapped_column(BigInteger)
    connection_epoch: Mapped[int] = mapped_column(Integer, default=0)
    recovery_required: Mapped[bool] = mapped_column(default=False)
    checkpoint_namespace: Mapped[str] = mapped_column(String(200), default="production", index=True)


class CollectorLeaseRow(Base):
    __tablename__ = "collector_leases"
    collector_group: Mapped[str] = mapped_column(String(160), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(160), index=True)
    owner_id: Mapped[str] = mapped_column(String(240))
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    renewed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class CollectorRunRow(Base):
    __tablename__ = "collector_runs"
    run_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    collector_group: Mapped[str] = mapped_column(String(160), index=True)
    owner_id: Mapped[str] = mapped_column(String(240))
    commit_sha: Mapped[str] = mapped_column(String(64))
    config_path: Mapped[str] = mapped_column(String(1000))
    database_identity: Mapped[str] = mapped_column(String(1000))
    schema_name: Mapped[str] = mapped_column(String(160))
    checkpoint_namespace: Mapped[str] = mapped_column(String(200))
    artifact_namespace: Mapped[str] = mapped_column(String(500))
    venues_json: Mapped[str] = mapped_column(Text)
    instruments_json: Mapped[str] = mapped_column(Text)
    event_types_json: Mapped[str] = mapped_column(Text)
    duration_seconds: Mapped[float | None] = mapped_column()
    pid: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(40), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    stop_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    artifact_directory: Mapped[str | None] = mapped_column(String(1000))
    failure_reason: Mapped[str | None] = mapped_column(String(1000))


class CollectionFailureEventRow(Base):
    __tablename__ = "collection_failure_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    stream_key: Mapped[str] = mapped_column(String(300), index=True)
    instrument: Mapped[str] = mapped_column(String(100))
    event_type: Mapped[str] = mapped_column(String(80))
    endpoint: Mapped[str] = mapped_column(String(500))
    error_type: Mapped[str] = mapped_column(String(120))
    error_message: Mapped[str] = mapped_column(String(500))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    retry_count: Mapped[int] = mapped_column(Integer)


class DataSnapshotRow(Base):
    __tablename__ = "data_snapshots"
    snapshot_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    event_count: Mapped[int] = mapped_column(Integer)
    content_sha256: Mapped[str] = mapped_column(String(64))
    manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    manifest_json: Mapped[str | None] = mapped_column(Text)
    quarantine_count: Mapped[int] = mapped_column(Integer, default=0)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    eligibility_status: Mapped[str] = mapped_column(String(40), default="FINALIZED_NOT_ELIGIBLE")
    eligibility_reasons_json: Mapped[str] = mapped_column(Text, default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DataSnapshotEventRow(Base):
    __tablename__ = "data_snapshot_events"
    __table_args__ = (UniqueConstraint("snapshot_id", "ordinal"),)
    snapshot_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("data_snapshots.snapshot_id"), primary_key=True
    )
    event_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("raw_market_events.event_id"), primary_key=True
    )
    ordinal: Mapped[int] = mapped_column(Integer)
    event_payload_sha256: Mapped[str] = mapped_column(String(64))
    included_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class InstrumentRuleSnapshotRow(Base):
    __tablename__ = "instrument_rule_snapshots"
    rule_snapshot_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    venue: Mapped[str] = mapped_column(String(40), index=True)
    canonical_instrument_id: Mapped[str] = mapped_column(String(100), index=True)
    venue_symbol: Mapped[str] = mapped_column(String(100))
    tick_size: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    lot_size: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    minimum_quantity: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    minimum_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    maker_fee: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    taker_fee: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    maker_rebate: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    funding_interval: Mapped[int | None] = mapped_column(Integer)
    margin_asset: Mapped[str | None] = mapped_column(String(40))
    source_endpoint: Mapped[str] = mapped_column(String(500))
    source_payload_sha256: Mapped[str] = mapped_column(String(64))
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    field_evidence_json: Mapped[str] = mapped_column(Text, default="{}")
    fee_tier: Mapped[str] = mapped_column(String(40), default="unknown")
    verification_status: Mapped[str] = mapped_column(String(40), default="unknown")


class ResearchRunRow(Base):
    __tablename__ = "research_runs"
    __table_args__ = (
        UniqueConstraint("run_id", "data_snapshot_id"),
        ForeignKeyConstraint(
            ["strategy_id", "hypothesis_version"],
            ["frozen_hypotheses.strategy_id", "frozen_hypotheses.hypothesis_version"],
        ),
    )
    run_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    commit_sha: Mapped[str] = mapped_column(String(80))
    config_sha256: Mapped[str] = mapped_column(String(64))
    data_snapshot_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("data_snapshots.snapshot_id"), index=True
    )
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
    strategy_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    content_sha256: Mapped[str] = mapped_column(String(64))
    content_json: Mapped[str] = mapped_column(Text)
    frozen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ResearchArtifactRow(Base):
    __tablename__ = "research_artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "data_snapshot_id"],
            ["research_runs.run_id", "research_runs.data_snapshot_id"],
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(String(160), index=True)
    data_snapshot_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("data_snapshots.snapshot_id"), index=True
    )
    artifact_type: Mapped[str] = mapped_column(String(80))
    path: Mapped[str] = mapped_column(String(500))
    content_sha256: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
