"""Add durable Research Pipeline R1 storage.

Revision ID: 0007_research_pipeline
Revises: 0006_snapshot_constraint
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_research_pipeline"
down_revision: str | None = "0006_snapshot_constraint"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_market_events",
        sa.Column("event_id", sa.String(160), primary_key=True),
        sa.Column("venue", sa.String(40), nullable=False),
        sa.Column("canonical_instrument_id", sa.String(100), nullable=False),
        sa.Column("venue_symbol", sa.String(100), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("exchange_timestamp", sa.DateTime(timezone=True)),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence", sa.BigInteger()),
        sa.Column("connection_id", sa.String(36)),
        sa.Column("reconciliation_state", sa.String(40)),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("raw_payload", sa.Text(), nullable=False),
        sa.Column("normalizer_version", sa.String(80), nullable=False),
        sa.Column("capability_verification_run_id", sa.String(160), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    for column in (
        "venue",
        "canonical_instrument_id",
        "event_type",
        "exchange_timestamp",
        "received_at",
        "available_at",
        "created_at",
    ):
        op.create_index(f"ix_raw_market_events_{column}", "raw_market_events", [column])
    op.create_table(
        "market_data_quarantine",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("event_id", sa.String(160), nullable=False),
        sa.Column("reason", sa.String(500), nullable=False),
        sa.Column("raw_payload", sa.Text(), nullable=False),
        sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_market_data_quarantine_event_id", "market_data_quarantine", ["event_id"])
    op.create_index(
        "ix_market_data_quarantine_quarantined_at", "market_data_quarantine", ["quarantined_at"]
    )
    op.create_table(
        "market_data_checkpoints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("venue", sa.String(40), nullable=False),
        sa.Column("connection_id", sa.String(36), nullable=False),
        sa.Column("last_sequence", sa.BigInteger()),
        sa.Column("last_event_id", sa.String(160)),
        sa.Column("reconciliation_state", sa.String(40), nullable=False),
        sa.Column("checkpointed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_market_data_checkpoints_venue", "market_data_checkpoints", ["venue"])
    op.create_table(
        "data_snapshots",
        sa.Column("snapshot_id", sa.String(160), primary_key=True),
        sa.Column("cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_data_snapshots_cutoff_at", "data_snapshots", ["cutoff_at"])
    op.create_table(
        "research_runs",
        sa.Column("run_id", sa.String(160), primary_key=True),
        sa.Column("commit_sha", sa.String(80), nullable=False),
        sa.Column("config_sha256", sa.String(64), nullable=False),
        sa.Column("data_snapshot_id", sa.String(160), nullable=False),
        sa.Column("hypothesis_version", sa.String(80), nullable=False),
        sa.Column("strategy_id", sa.String(100), nullable=False),
        sa.Column("strategy_version", sa.String(40), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("acceptance_verdict", sa.String(40)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_research_runs_data_snapshot_id", "research_runs", ["data_snapshot_id"])
    op.create_table(
        "frozen_hypotheses",
        sa.Column("hypothesis_version", sa.String(80), primary_key=True),
        sa.Column("strategy_id", sa.String(100), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("frozen_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "research_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(160), nullable=False),
        sa.Column("data_snapshot_id", sa.String(160), nullable=False),
        sa.Column("artifact_type", sa.String(80), nullable=False),
        sa.Column("path", sa.String(500), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_research_artifacts_run_id", "research_artifacts", ["run_id"])
    op.create_index(
        "ix_research_artifacts_data_snapshot_id", "research_artifacts", ["data_snapshot_id"]
    )


def downgrade() -> None:
    op.drop_table("research_artifacts")
    op.drop_table("frozen_hypotheses")
    op.drop_table("research_runs")
    op.drop_table("data_snapshots")
    op.drop_table("market_data_checkpoints")
    op.drop_table("market_data_quarantine")
    op.drop_table("raw_market_events")
