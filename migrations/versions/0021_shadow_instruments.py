"""Independent shadow checkpoints and metrics by instrument.

Existing R9 checkpoints belong to BTC; existing metrics remain aggregate rows.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0021_shadow_instruments"
down_revision: str | None = "0020_incremental_shadow_input"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for table, default in (
        ("shadow_input_checkpoints", "BTC"),
        ("funding_carry_shadow_metrics", ""),
    ):
        with op.batch_alter_table(table, naming_convention={"pk": "%(table_name)s_pkey"}) as batch:
            batch.add_column(
                sa.Column("instrument", sa.String(100), nullable=False, server_default=default)
            )
            batch.drop_constraint(f"{table}_pkey", type_="primary")
            batch.create_primary_key(f"{table}_pkey", ["run_id", "instrument"])
    # All historical R9 metrics were BTC-only. Preserve both the aggregate API
    # and an accurate instrument view instead of resetting historical counters.
    metrics = sa.Table("funding_carry_shadow_metrics", sa.MetaData(), autoload_with=op.get_bind())
    columns = [column.name for column in metrics.columns]
    op.execute(
        metrics.insert().from_select(
            columns,
            sa.select(
                *(
                    sa.literal("BTC") if name == "instrument" else metrics.c[name]
                    for name in columns
                )
            ).where(metrics.c.instrument == ""),
        )
    )


def downgrade() -> None:
    # Collapsing multiple independent states into one would lose evidence.
    raise RuntimeError("multi-instrument evidence requires an explicit archival migration")
