"""Persist cross-venue preflight bindings.

Revision ID: 0003_preflight_bindings
Revises: 0002_cross_venue_clock
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_preflight_bindings"
down_revision: str | None = "0002_cross_venue_clock"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "preflight_bindings",
        sa.Column("signal_id", sa.String(length=100), nullable=False),
        sa.Column("preflight_hash", sa.String(length=64)),
        sa.Column("state", sa.String(length=40), nullable=False),
        sa.Column("first_leg_role", sa.String(length=20)),
        sa.Column("first_order_request_id", sa.String(length=100)),
        sa.Column("first_external_order_id", sa.String(length=160)),
        sa.Column("second_order_request_id", sa.String(length=100)),
        sa.Column("second_external_order_id", sa.String(length=160)),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("failure_reason", sa.String(length=500)),
        sa.PrimaryKeyConstraint("signal_id"),
    )
    op.create_index("ix_preflight_bindings_state", "preflight_bindings", ["state"])
    op.create_index("ix_preflight_bindings_updated_at", "preflight_bindings", ["updated_at"])


def downgrade() -> None:
    op.drop_index("ix_preflight_bindings_updated_at", table_name="preflight_bindings")
    op.drop_index("ix_preflight_bindings_state", table_name="preflight_bindings")
    op.drop_table("preflight_bindings")
