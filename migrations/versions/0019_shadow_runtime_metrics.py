"""Persist funding carry shadow runtime metrics.

Revision ID: 0019_shadow_runtime_metrics
Revises: 0018_funding_carry_shadow
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019_shadow_runtime_metrics"
down_revision: str | None = "0018_funding_carry_shadow"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "funding_carry_shadow_metrics",
        sa.Column(
            "run_id",
            sa.String(length=160),
            sa.ForeignKey("operational_runs.run_id"),
            primary_key=True,
        ),
        sa.Column("candidate_generation_attempt_count", sa.BigInteger(), nullable=False, default=0),
        sa.Column("candidate_inserted_count", sa.BigInteger(), nullable=False, default=0),
        sa.Column("candidate_rejected_count", sa.BigInteger(), nullable=False, default=0),
        sa.Column(
            "candidate_duplicate_suppressed_count", sa.BigInteger(), nullable=False, default=0
        ),
        sa.Column("source_pair_duplicate_count", sa.BigInteger(), nullable=False, default=0),
        sa.Column("matched_source_pair_count", sa.BigInteger(), nullable=False, default=0),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("funding_carry_shadow_metrics")
