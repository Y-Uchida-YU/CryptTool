"""Incremental funding carry shadow input cursors.

Revision ID: 0020_incremental_shadow_input
Revises: 0019_shadow_runtime_metrics
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0020_incremental_shadow_input"
down_revision: str | None = "0019_shadow_runtime_metrics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_experimental_shadow_incremental",
        "experimental_market_events",
        ["venue", "canonical_instrument_id", "event_type", "available_at", "event_id"],
    )
    op.create_table(
        "shadow_input_cursors",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(length=160), nullable=False),
        sa.Column("venue", sa.String(length=40), nullable=False),
        sa.Column("instrument", sa.String(length=100), nullable=False),
        sa.Column("event_stream", sa.String(length=80), nullable=False),
        sa.Column("last_available_at", sa.DateTime(timezone=True)),
        sa.Column("last_event_id", sa.String(length=160)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("run_id", "venue", "instrument", "event_stream"),
    )
    op.create_index("ix_shadow_input_cursors_run_id", "shadow_input_cursors", ["run_id"])
    op.create_table(
        "shadow_input_checkpoints",
        sa.Column("run_id", sa.String(length=160), primary_key=True),
        sa.Column("state_json", sa.Text(), nullable=False),
        sa.Column("events_fetched_total", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("events_processed_total", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("events_failed_total", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("batch_count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("last_batch_size", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scan_duration_ms", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column("processing_duration_ms", sa.Numeric(30, 6), nullable=False, server_default="0"),
        sa.Column("latest_db_event_available_at", sa.DateTime(timezone=True)),
        sa.Column("shadow_input_lag_seconds", sa.Numeric(30, 6)),
        sa.Column(
            "shadow_input_backlog_estimate", sa.BigInteger(), nullable=False, server_default="0"
        ),
        sa.Column("strategy_observation_first_at", sa.DateTime(timezone=True)),
        sa.Column("strategy_observation_last_at", sa.DateTime(timezone=True)),
        sa.Column("last_cursor_advanced_at", sa.DateTime(timezone=True)),
        sa.Column("runtime_status", sa.String(length=40), nullable=False, server_default="healthy"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("shadow_input_checkpoints")
    op.drop_index("ix_shadow_input_cursors_run_id", table_name="shadow_input_cursors")
    op.drop_table("shadow_input_cursors")
    op.drop_index("ix_experimental_shadow_incremental", table_name="experimental_market_events")
