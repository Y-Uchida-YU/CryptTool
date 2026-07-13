"""Require complete position reconciliation snapshots.

Revision ID: 0006_snapshot_constraint
Revises: 0005_position_snapshot
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006_snapshot_constraint"
down_revision: str | None = "0005_position_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

POSITION_SNAPSHOT_CHECK = (
    "(position_venue IS NULL AND position_symbol IS NULL "
    "AND position_quantity_before IS NULL AND position_captured_at IS NULL) OR "
    "(position_venue IS NOT NULL AND position_symbol IS NOT NULL "
    "AND position_quantity_before IS NOT NULL AND position_captured_at IS NOT NULL)"
)


def upgrade() -> None:
    with op.batch_alter_table("preflight_bindings") as batch_op:
        batch_op.create_check_constraint(
            "ck_preflight_bindings_position_snapshot_complete",
            POSITION_SNAPSHOT_CHECK,
        )


def downgrade() -> None:
    with op.batch_alter_table("preflight_bindings") as batch_op:
        batch_op.drop_constraint("ck_preflight_bindings_position_snapshot_complete", type_="check")
