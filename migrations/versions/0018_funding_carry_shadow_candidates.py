"""Persist observation-only funding carry shadow candidates.

Revision ID: 0018_funding_carry_shadow
Revises: 0017_certification_run_registry
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_funding_carry_shadow"
down_revision: str | None = "0017_certification_run_registry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "funding_carry_shadow_candidates",
        sa.Column("candidate_id", sa.String(length=64), primary_key=True),
        sa.Column(
            "run_id",
            sa.String(length=160),
            sa.ForeignKey("operational_runs.run_id"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.String(length=100), nullable=False, index=True),
        sa.Column("instrument", sa.String(length=100), nullable=False, index=True),
        sa.Column("long_venue", sa.String(length=40)),
        sa.Column("short_venue", sa.String(length=40)),
        sa.Column("disposition", sa.String(length=40), nullable=False, index=True),
        sa.Column("rejection_reason", sa.String(length=80), index=True),
        sa.Column("source_event_ids_json", sa.Text(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("code_commit_sha", sa.String(length=80), nullable=False),
        sa.Column("config_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, index=True),
    )


def downgrade() -> None:
    op.drop_table("funding_carry_shadow_candidates")
