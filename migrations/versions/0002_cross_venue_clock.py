"""Add cross-venue clock provenance.

Revision ID: 0002_cross_venue_clock
Revises: 0001_initial
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_cross_venue_clock"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("ohlcv", sa.Column("exchange_timestamp", sa.DateTime(timezone=True)))
    op.add_column("ohlcv", sa.Column("received_at", sa.DateTime(timezone=True)))
    op.add_column("ohlcv", sa.Column("available_at", sa.DateTime(timezone=True)))
    op.add_column("ohlcv", sa.Column("local_monotonic_time", sa.Float()))
    op.add_column("ohlcv", sa.Column("clock_offset_estimate", sa.Float()))


def downgrade() -> None:
    op.drop_column("ohlcv", "clock_offset_estimate")
    op.drop_column("ohlcv", "local_monotonic_time")
    op.drop_column("ohlcv", "available_at")
    op.drop_column("ohlcv", "received_at")
    op.drop_column("ohlcv", "exchange_timestamp")
