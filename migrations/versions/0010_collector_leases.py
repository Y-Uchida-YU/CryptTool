"""Add collector leases, run registry, and checkpoint namespaces.

Revision ID: 0010_collector_leases
Revises: 0009_collector_correctness
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_collector_leases"
down_revision: str | None = "0009_collector_correctness"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("market_data_checkpoints") as batch:
        batch.add_column(
            sa.Column(
                "checkpoint_namespace",
                sa.String(200),
                nullable=False,
                server_default="production",
            )
        )
    op.create_index(
        "ix_market_data_checkpoints_checkpoint_namespace",
        "market_data_checkpoints",
        ["checkpoint_namespace"],
    )
    op.create_table(
        "collector_leases",
        sa.Column("collector_group", sa.String(160), primary_key=True),
        sa.Column("run_id", sa.String(160), nullable=False),
        sa.Column("owner_id", sa.String(240), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("renewed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_collector_leases_run_id", "collector_leases", ["run_id"])
    op.create_index("ix_collector_leases_expires_at", "collector_leases", ["expires_at"])
    op.create_table(
        "collector_runs",
        sa.Column("run_id", sa.String(160), primary_key=True),
        sa.Column("collector_group", sa.String(160), nullable=False),
        sa.Column("owner_id", sa.String(240), nullable=False),
        sa.Column("commit_sha", sa.String(64), nullable=False),
        sa.Column("config_path", sa.String(1000), nullable=False),
        sa.Column("database_identity", sa.String(1000), nullable=False),
        sa.Column("schema_name", sa.String(160), nullable=False),
        sa.Column("checkpoint_namespace", sa.String(200), nullable=False),
        sa.Column("artifact_namespace", sa.String(500), nullable=False),
        sa.Column("venues_json", sa.Text(), nullable=False),
        sa.Column("instruments_json", sa.Text(), nullable=False),
        sa.Column("event_types_json", sa.Text(), nullable=False),
        sa.Column("duration_seconds", sa.Float()),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stop_requested_at", sa.DateTime(timezone=True)),
        sa.Column("stopped_at", sa.DateTime(timezone=True)),
        sa.Column("artifact_directory", sa.String(1000)),
        sa.Column("failure_reason", sa.String(1000)),
        sa.CheckConstraint(
            "status IN ('RUNNING','STOP_REQUESTED','COMPLETED','FAILED','CANCELED_DUE_TO_OVERLAP')",
            name="ck_collector_runs_status",
        ),
    )
    for column in ("collector_group", "status", "started_at"):
        op.create_index(f"ix_collector_runs_{column}", "collector_runs", [column])


def downgrade() -> None:
    raise RuntimeError("collector lease migration is not safely reversible")
