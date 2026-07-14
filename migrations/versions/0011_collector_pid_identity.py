"""Bind collector run records to a strong process identity.

Revision ID: 0011_collector_pid_identity
Revises: 0010_collector_leases
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011_collector_pid_identity"
down_revision: str | None = "0010_collector_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("collector_runs") as batch:
        batch.add_column(
            sa.Column(
                "process_started_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default="1970-01-01 00:00:00+00:00",
            )
        )
        batch.add_column(
            sa.Column("hostname", sa.String(255), nullable=False, server_default="unknown")
        )
        batch.add_column(
            sa.Column("command_sha256", sa.String(64), nullable=False, server_default="")
        )
        batch.add_column(
            sa.Column("run_token_sha256", sa.String(64), nullable=False, server_default="")
        )


def downgrade() -> None:
    raise RuntimeError("collector PID identity migration is not safely reversible")
