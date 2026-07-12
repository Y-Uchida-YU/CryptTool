"""Initial normalized OHLCV and audit schema."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("entity_id", sa.String(length=100), nullable=True),
        sa.Column("payload_json", sa.String(), nullable=False),
        sa.Column("model_version", sa.String(length=80), nullable=True),
        sa.Column("config_version", sa.String(length=80), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])
    op.create_index("ix_audit_events_occurred_at", "audit_events", ["occurred_at"])
    op.create_table(
        "ohlcv",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(length=40), nullable=False),
        sa.Column("symbol", sa.String(length=40), nullable=False),
        sa.Column("timeframe", sa.String(length=8), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(precision=30, scale=12), nullable=False),
        sa.Column("high", sa.Numeric(precision=30, scale=12), nullable=False),
        sa.Column("low", sa.Numeric(precision=30, scale=12), nullable=False),
        sa.Column("close", sa.Numeric(precision=30, scale=12), nullable=False),
        sa.Column("volume", sa.Numeric(precision=38, scale=12), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("exchange", "symbol", "timeframe", "timestamp"),
    )
    op.create_index("ix_ohlcv_exchange", "ohlcv", ["exchange"])
    op.create_index("ix_ohlcv_symbol", "ohlcv", ["symbol"])
    op.create_index("ix_ohlcv_timestamp", "ohlcv", ["timestamp"])


def downgrade() -> None:
    op.drop_index("ix_ohlcv_timestamp", table_name="ohlcv")
    op.drop_index("ix_ohlcv_symbol", table_name="ohlcv")
    op.drop_index("ix_ohlcv_exchange", table_name="ohlcv")
    op.drop_table("ohlcv")
    op.drop_index("ix_audit_events_occurred_at", table_name="audit_events")
    op.drop_index("ix_audit_events_event_type", table_name="audit_events")
    op.drop_table("audit_events")
