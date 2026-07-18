"""Persist certification process lifecycle before public adapter startup.

Revision ID: 0017_certification_run_registry
Revises: 0016_historical_timing_semantics
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017_certification_run_registry"
down_revision: str | None = "0016_historical_timing_semantics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "certification_runs",
        sa.Column("run_id", sa.String(length=160), primary_key=True),
        sa.Column("status", sa.String(length=40), nullable=False, index=True),
        sa.Column("last_stage", sa.String(length=80), nullable=False),
        sa.Column("failure_reason", sa.String(length=2000)),
        sa.Column("commit_sha", sa.String(length=40), nullable=False),
        sa.Column("config_path", sa.String(length=1000), nullable=False),
        sa.Column("database_identity", sa.String(length=1000), nullable=False),
        sa.Column("artifact_directory", sa.String(length=1000), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("parent_pid", sa.Integer(), nullable=False),
        sa.Column("signal_number", sa.Integer()),
        sa.Column("exit_code", sa.Integer()),
        sa.Column("exception_type", sa.String(length=300)),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.CheckConstraint(
            "status IN ('STARTING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELED')",
            name="ck_certification_runs_status",
        ),
    )


def downgrade() -> None:
    op.drop_table("certification_runs")
