"""Constrain persisted preflight binding states and versions.

Revision ID: 0004_binding_constraints
Revises: 0003_preflight_bindings
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0004_binding_constraints"
down_revision: str | None = "0003_preflight_bindings"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATE_CHECK = (
    "state IN ('unbound','reserved','first_leg_accepted','second_leg_submitted',"
    "'hedging_required','reconciliation_required','completed','aborted','halted')"
)


def upgrade() -> None:
    with op.batch_alter_table("preflight_bindings") as batch_op:
        batch_op.create_check_constraint("ck_preflight_bindings_state", STATE_CHECK)
        batch_op.create_check_constraint("ck_preflight_bindings_version", "version >= 1")
        batch_op.create_check_constraint(
            "ck_preflight_bindings_timestamp_order", "updated_at >= created_at"
        )


def downgrade() -> None:
    with op.batch_alter_table("preflight_bindings") as batch_op:
        batch_op.drop_constraint("ck_preflight_bindings_timestamp_order", type_="check")
        batch_op.drop_constraint("ck_preflight_bindings_version", type_="check")
        batch_op.drop_constraint("ck_preflight_bindings_state", type_="check")
