"""Separate historical timing, availability and server-clock evidence.

Revision ID: 0016_historical_timing_semantics
Revises: 0015_market_data_certification
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016_historical_timing_semantics"
down_revision: str | None = "0015_market_data_certification"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _upgrade_table(name: str) -> None:
    with op.batch_alter_table(name) as batch:
        batch.add_column(
            sa.Column(
                "timestamp_semantic",
                sa.String(length=50),
                nullable=False,
                server_default="receipt_only",
            )
        )
        batch.add_column(
            sa.Column(
                "availability_provenance",
                sa.String(length=50),
                nullable=False,
                server_default="unknown",
            )
        )
        batch.add_column(sa.Column("exchange_server_time", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("timeframe", sa.String(length=20)))
        batch.create_check_constraint(
            f"ck_{name}_timestamp_semantic",
            "timestamp_semantic IN ('realtime_event', 'historical_effective_time', "
            "'candle_open_time', 'candle_close_time', 'funding_effective_time', "
            "'receipt_only')",
        )
        batch.create_check_constraint(
            f"ck_{name}_availability_provenance",
            "availability_provenance IN ('historical_effective_time', "
            "'observed_retrieval_time', 'exchange_published_time', 'unknown')",
        )


def upgrade() -> None:
    _upgrade_table("raw_market_events")
    _upgrade_table("experimental_market_events")


def _downgrade_table(name: str) -> None:
    with op.batch_alter_table(name) as batch:
        batch.drop_constraint(f"ck_{name}_availability_provenance", type_="check")
        batch.drop_constraint(f"ck_{name}_timestamp_semantic", type_="check")
        batch.drop_column("timeframe")
        batch.drop_column("exchange_server_time")
        batch.drop_column("availability_provenance")
        batch.drop_column("timestamp_semantic")


def downgrade() -> None:
    _downgrade_table("experimental_market_events")
    _downgrade_table("raw_market_events")
