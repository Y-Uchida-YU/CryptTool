"""Persist explicit R3 research, capital, and signal states.

Revision ID: 0014_r3_evidence_states
Revises: 0013_continuous_paper
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_r3_evidence_states"
down_revision: str | None = "0013_continuous_paper"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "operational_runs",
        sa.Column(
            "research_status", sa.String(length=50), nullable=False, server_default="not_scheduled"
        ),
    )
    op.add_column("operational_runs", sa.Column("research_skip_reason", sa.String(length=1000)))
    op.add_column(
        "strategy_eligibility",
        sa.Column(
            "capital_feasibility_status",
            sa.String(length=40),
            nullable=False,
            server_default="not_evaluated",
        ),
    )
    op.add_column(
        "paper_signals",
        sa.Column("disposition", sa.String(length=50), nullable=False, server_default="candidate"),
    )
    op.create_index("ix_paper_signals_disposition", "paper_signals", ["disposition"])


def downgrade() -> None:
    op.drop_index("ix_paper_signals_disposition", table_name="paper_signals")
    op.drop_column("paper_signals", "disposition")
    op.drop_column("strategy_eligibility", "capital_feasibility_status")
    op.drop_column("operational_runs", "research_skip_reason")
    op.drop_column("operational_runs", "research_status")
