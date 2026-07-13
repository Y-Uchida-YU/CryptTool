"""Persist pre-submission position reconciliation snapshots.

Revision ID: 0005_position_snapshot
Revises: 0004_binding_constraints
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_position_snapshot"
down_revision: str | None = "0004_binding_constraints"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("preflight_bindings", sa.Column("position_venue", sa.String(length=40)))
    op.add_column("preflight_bindings", sa.Column("position_symbol", sa.String(length=40)))
    op.add_column("preflight_bindings", sa.Column("position_quantity_before", sa.Numeric(38, 12)))
    op.add_column(
        "preflight_bindings", sa.Column("position_captured_at", sa.DateTime(timezone=True))
    )


def downgrade() -> None:
    op.drop_column("preflight_bindings", "position_captured_at")
    op.drop_column("preflight_bindings", "position_quantity_before")
    op.drop_column("preflight_bindings", "position_symbol")
    op.drop_column("preflight_bindings", "position_venue")
