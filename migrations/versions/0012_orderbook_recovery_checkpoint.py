"""Persist order-book bootstrap and recovery evidence.

Revision ID: 0012_orderbook_recovery_checkpoint
Revises: 0011_collector_pid_identity
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012_orderbook_recovery_checkpoint"
down_revision: str | None = "0011_collector_pid_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("market_data_checkpoints") as batch:
        batch.add_column(
            sa.Column(
                "bootstrap_completed",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch.add_column(sa.Column("recovery_started_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("recovery_completed_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("last_recovery_failure", sa.String(length=500)))


def downgrade() -> None:
    with op.batch_alter_table("market_data_checkpoints") as batch:
        batch.drop_column("last_recovery_failure")
        batch.drop_column("recovery_completed_at")
        batch.drop_column("recovery_started_at")
        batch.drop_column("bootstrap_completed")
