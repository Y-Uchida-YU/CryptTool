"""Add stream-scoped collector correctness and snapshot eligibility.

Revision ID: 0009_collector_correctness
Revises: 0008_research_data_operations
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009_collector_correctness"
down_revision: str | None = "0008_research_data_operations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("raw_market_events") as batch:
        batch.add_column(
            sa.Column("channel", sa.String(120), nullable=False, server_default="unknown")
        )
        batch.add_column(sa.Column("snapshot_sequence", sa.BigInteger()))
        batch.add_column(sa.Column("delta_sequence", sa.BigInteger()))
        batch.add_column(sa.Column("connection_epoch", sa.Integer()))
    with op.batch_alter_table("market_data_checkpoints") as batch:
        batch.add_column(
            sa.Column(
                "canonical_instrument_id", sa.String(100), nullable=False, server_default="SYSTEM"
            )
        )
        batch.add_column(
            sa.Column("venue_symbol", sa.String(100), nullable=False, server_default="SYSTEM")
        )
        batch.add_column(
            sa.Column("event_type", sa.String(80), nullable=False, server_default="unknown")
        )
        batch.add_column(
            sa.Column("channel", sa.String(120), nullable=False, server_default="unknown")
        )
        batch.add_column(sa.Column("last_available_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("last_funding_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("last_trade_id", sa.String(200)))
        batch.add_column(sa.Column("snapshot_sequence", sa.BigInteger()))
        batch.add_column(sa.Column("delta_sequence", sa.BigInteger()))
        batch.add_column(
            sa.Column("connection_epoch", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column("recovery_required", sa.Boolean(), nullable=False, server_default=sa.false())
        )
    op.create_table(
        "collection_failure_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("venue", sa.String(40), nullable=False),
        sa.Column("stream_key", sa.String(300), nullable=False),
        sa.Column("instrument", sa.String(100), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("endpoint", sa.String(500), nullable=False),
        sa.Column("error_type", sa.String(120), nullable=False),
        sa.Column("error_message", sa.String(500), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
    )
    for column in ("venue", "stream_key", "occurred_at"):
        op.create_index(
            f"ix_collection_failure_events_{column}", "collection_failure_events", [column]
        )
    with op.batch_alter_table("data_snapshots") as batch:
        batch.add_column(
            sa.Column(
                "eligibility_status",
                sa.String(40),
                nullable=False,
                server_default="FINALIZED_NOT_ELIGIBLE",
            )
        )
        batch.add_column(
            sa.Column("eligibility_reasons_json", sa.Text(), nullable=False, server_default="[]")
        )
    with op.batch_alter_table("instrument_rule_snapshots") as batch:
        for column in (
            "tick_size",
            "lot_size",
            "minimum_quantity",
            "minimum_notional",
            "maker_fee",
            "taker_fee",
            "maker_rebate",
            "funding_interval",
            "margin_asset",
        ):
            batch.alter_column(column, nullable=True)
        batch.add_column(
            sa.Column("field_evidence_json", sa.Text(), nullable=False, server_default="{}")
        )
        batch.add_column(
            sa.Column("fee_tier", sa.String(40), nullable=False, server_default="unknown")
        )
        batch.add_column(
            sa.Column(
                "verification_status", sa.String(40), nullable=False, server_default="unknown"
            )
        )

    dialect = op.get_bind().dialect.name
    protected = (
        "snapshot_id",
        "cutoff_at",
        "event_count",
        "content_sha256",
        "manifest_sha256",
        "manifest_json",
        "quarantine_count",
        "finalized_at",
        "eligibility_status",
        "eligibility_reasons_json",
        "created_at",
    )
    if dialect == "postgresql":
        comparisons = " OR ".join(
            f"NEW.{column} IS DISTINCT FROM OLD.{column}" for column in protected
        )
        op.execute(
            f"""CREATE FUNCTION prevent_finalized_snapshot_row_change() RETURNS trigger AS $$
            BEGIN
              IF TG_OP = 'DELETE' THEN
                IF OLD.finalized_at IS NOT NULL THEN
                  RAISE EXCEPTION 'finalized snapshot row is immutable';
                END IF;
                RETURN OLD;
              END IF;
              IF OLD.finalized_at IS NOT NULL AND ({comparisons}) THEN
                RAISE EXCEPTION 'finalized snapshot row is immutable';
              END IF;
              RETURN NEW;
            END; $$ LANGUAGE plpgsql"""
        )
        op.execute(
            "CREATE TRIGGER data_snapshots_row_immutable BEFORE UPDATE OR DELETE "
            "ON data_snapshots FOR EACH ROW EXECUTE FUNCTION "
            "prevent_finalized_snapshot_row_change()"
        )
    elif dialect == "sqlite":
        comparisons = " OR ".join(f"NEW.{column} IS NOT OLD.{column}" for column in protected)
        op.execute(
            f"""CREATE TRIGGER data_snapshots_row_immutable_update
            BEFORE UPDATE ON data_snapshots
            WHEN OLD.finalized_at IS NOT NULL AND ({comparisons})
            BEGIN SELECT RAISE(ABORT, 'finalized snapshot row is immutable'); END"""
        )
        op.execute(
            """CREATE TRIGGER data_snapshots_row_immutable_delete
            BEFORE DELETE ON data_snapshots
            WHEN OLD.finalized_at IS NOT NULL
            BEGIN SELECT RAISE(ABORT, 'finalized snapshot row is immutable'); END"""
        )


def downgrade() -> None:
    raise RuntimeError("R2.1 collector correctness migration is not safely reversible")
