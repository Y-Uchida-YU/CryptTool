"""Add immutable Research Data Operations R2 storage.

Revision ID: 0008_research_data_operations
Revises: 0007_research_pipeline
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008_research_data_operations"
down_revision: str | None = "0007_research_pipeline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_market_payloads",
        sa.Column("payload_id", sa.String(160), primary_key=True),
        sa.Column("venue", sa.String(40), nullable=False),
        sa.Column("source_endpoint", sa.String(500), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False, unique=True),
        sa.Column("raw_payload", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_raw_market_payloads_venue", "raw_market_payloads", ["venue"])
    op.create_index("ix_raw_market_payloads_received_at", "raw_market_payloads", ["received_at"])
    with op.batch_alter_table("raw_market_events") as batch:
        batch.add_column(sa.Column("raw_payload_id", sa.String(160)))
        batch.add_column(sa.Column("source_payload_sha256", sa.String(64)))
        batch.create_foreign_key(
            "fk_raw_market_events_raw_payload_id",
            "raw_market_payloads",
            ["raw_payload_id"],
            ["payload_id"],
        )
    op.create_table(
        "experimental_market_events",
        sa.Column("event_id", sa.String(160), primary_key=True),
        sa.Column("venue", sa.String(40), nullable=False),
        sa.Column("canonical_instrument_id", sa.String(100), nullable=False),
        sa.Column("venue_symbol", sa.String(100), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("raw_payload", sa.Text(), nullable=False),
        sa.Column("capability_support", sa.String(40), nullable=False),
        sa.Column("capability_verification_run_id", sa.String(160)),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
    )
    for column in ("venue", "canonical_instrument_id", "event_type", "received_at"):
        op.create_index(
            f"ix_experimental_market_events_{column}", "experimental_market_events", [column]
        )

    with op.batch_alter_table("market_data_checkpoints") as batch:
        batch.add_column(sa.Column("stream_key", sa.String(200)))
    op.execute("UPDATE market_data_checkpoints SET stream_key = connection_id")
    with op.batch_alter_table("market_data_checkpoints") as batch:
        batch.alter_column("stream_key", existing_type=sa.String(200), nullable=False)
        batch.create_unique_constraint(
            "uq_market_data_checkpoints_venue_stream", ["venue", "stream_key"]
        )

    with op.batch_alter_table("data_snapshots") as batch:
        batch.add_column(sa.Column("manifest_sha256", sa.String(64)))
        batch.add_column(sa.Column("manifest_json", sa.Text()))
        batch.add_column(
            sa.Column("quarantine_count", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("finalized_at", sa.DateTime(timezone=True)))

    op.create_table(
        "data_snapshot_events",
        sa.Column("snapshot_id", sa.String(160), nullable=False),
        sa.Column("event_id", sa.String(160), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("event_payload_sha256", sa.String(64), nullable=False),
        sa.Column("included_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["snapshot_id"], ["data_snapshots.snapshot_id"]),
        sa.ForeignKeyConstraint(["event_id"], ["raw_market_events.event_id"]),
        sa.PrimaryKeyConstraint("snapshot_id", "event_id"),
        sa.UniqueConstraint("snapshot_id", "ordinal"),
    )
    op.create_table(
        "instrument_rule_snapshots",
        sa.Column("rule_snapshot_id", sa.String(160), primary_key=True),
        sa.Column("venue", sa.String(40), nullable=False),
        sa.Column("canonical_instrument_id", sa.String(100), nullable=False),
        sa.Column("venue_symbol", sa.String(100), nullable=False),
        sa.Column("tick_size", sa.Numeric(38, 18), nullable=False),
        sa.Column("lot_size", sa.Numeric(38, 18), nullable=False),
        sa.Column("minimum_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("minimum_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("maker_fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("taker_fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("maker_rebate", sa.Numeric(38, 18), nullable=False),
        sa.Column("funding_interval", sa.Integer(), nullable=False),
        sa.Column("margin_asset", sa.String(40), nullable=False),
        sa.Column("source_endpoint", sa.String(500), nullable=False),
        sa.Column("source_payload_sha256", sa.String(64), nullable=False),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_instrument_rule_snapshots_venue", "instrument_rule_snapshots", ["venue"])
    op.create_index(
        "ix_instrument_rule_snapshots_canonical_instrument_id",
        "instrument_rule_snapshots",
        ["canonical_instrument_id"],
    )

    # Rebuild identity tables so hypothesis versions are scoped to a strategy and
    # artifacts are bound to the exact snapshot used by their parent run.
    op.drop_index("ix_research_artifacts_run_id", table_name="research_artifacts")
    op.drop_index("ix_research_artifacts_data_snapshot_id", table_name="research_artifacts")
    op.drop_index("ix_research_runs_data_snapshot_id", table_name="research_runs")
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE research_artifacts RENAME CONSTRAINT research_artifacts_pkey "
            "TO research_artifacts_legacy_pkey"
        )
        op.execute(
            "ALTER TABLE research_runs RENAME CONSTRAINT research_runs_pkey "
            "TO research_runs_legacy_pkey"
        )
        op.execute(
            "ALTER TABLE frozen_hypotheses RENAME CONSTRAINT frozen_hypotheses_pkey "
            "TO frozen_hypotheses_legacy_pkey"
        )
    op.rename_table("research_artifacts", "research_artifacts_legacy")
    op.rename_table("research_runs", "research_runs_legacy")
    op.rename_table("frozen_hypotheses", "frozen_hypotheses_legacy")
    op.create_table(
        "frozen_hypotheses",
        sa.Column("hypothesis_version", sa.String(80), nullable=False),
        sa.Column("strategy_id", sa.String(100), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("content_json", sa.Text(), nullable=False),
        sa.Column("frozen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("strategy_id", "hypothesis_version"),
    )
    op.create_table(
        "research_runs",
        sa.Column("run_id", sa.String(160), primary_key=True),
        sa.Column("commit_sha", sa.String(80), nullable=False),
        sa.Column("config_sha256", sa.String(64), nullable=False),
        sa.Column("data_snapshot_id", sa.String(160), nullable=False),
        sa.Column("hypothesis_version", sa.String(80), nullable=False),
        sa.Column("strategy_id", sa.String(100), nullable=False),
        sa.Column("strategy_version", sa.String(40), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("acceptance_verdict", sa.String(40)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.snapshot_id"]),
        sa.ForeignKeyConstraint(
            ["strategy_id", "hypothesis_version"],
            ["frozen_hypotheses.strategy_id", "frozen_hypotheses.hypothesis_version"],
        ),
        sa.UniqueConstraint("run_id", "data_snapshot_id"),
    )
    op.create_index("ix_research_runs_data_snapshot_id", "research_runs", ["data_snapshot_id"])
    op.create_table(
        "research_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(160), nullable=False),
        sa.Column("data_snapshot_id", sa.String(160), nullable=False),
        sa.Column("artifact_type", sa.String(80), nullable=False),
        sa.Column("path", sa.String(500), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["data_snapshot_id"], ["data_snapshots.snapshot_id"]),
        sa.ForeignKeyConstraint(
            ["run_id", "data_snapshot_id"],
            ["research_runs.run_id", "research_runs.data_snapshot_id"],
        ),
    )
    op.create_index("ix_research_artifacts_run_id", "research_artifacts", ["run_id"])
    op.create_index(
        "ix_research_artifacts_data_snapshot_id", "research_artifacts", ["data_snapshot_id"]
    )
    op.execute(
        "INSERT INTO frozen_hypotheses SELECT hypothesis_version, strategy_id, "
        "content_sha256, content_json, frozen_at FROM frozen_hypotheses_legacy"
    )
    op.execute(
        "INSERT INTO research_runs SELECT run_id, commit_sha, config_sha256, data_snapshot_id, "
        "hypothesis_version, strategy_id, strategy_version, status, acceptance_verdict, "
        "created_at, completed_at FROM research_runs_legacy"
    )
    op.execute(
        "INSERT INTO research_artifacts SELECT id, run_id, data_snapshot_id, artifact_type, "
        "path, content_sha256, created_at FROM research_artifacts_legacy"
    )
    op.drop_table("research_artifacts_legacy")
    op.drop_table("research_runs_legacy")
    op.drop_table("frozen_hypotheses_legacy")

    dialect = op.get_bind().dialect.name
    if dialect == "postgresql":
        op.execute(
            """CREATE FUNCTION prevent_finalized_snapshot_membership_change() RETURNS trigger AS $$
            DECLARE target_snapshot_id text;
            BEGIN
              IF TG_OP = 'DELETE' THEN
                target_snapshot_id := OLD.snapshot_id;
              ELSE
                target_snapshot_id := NEW.snapshot_id;
              END IF;
              IF EXISTS (
                SELECT 1 FROM data_snapshots
                WHERE snapshot_id = target_snapshot_id
                  AND finalized_at IS NOT NULL
              ) THEN
                RAISE EXCEPTION 'finalized snapshot membership is immutable';
              END IF;
              IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
              RETURN NEW;
            END; $$ LANGUAGE plpgsql"""
        )
        op.execute(
            "CREATE TRIGGER data_snapshot_events_immutable BEFORE INSERT OR UPDATE OR DELETE "
            "ON data_snapshot_events FOR EACH ROW EXECUTE FUNCTION "
            "prevent_finalized_snapshot_membership_change()"
        )
    elif dialect == "sqlite":
        for operation in ("INSERT", "UPDATE", "DELETE"):
            ref = "NEW" if operation == "INSERT" else "OLD"
            op.execute(
                f"""CREATE TRIGGER data_snapshot_events_immutable_{operation.lower()}
                BEFORE {operation} ON data_snapshot_events
                WHEN (SELECT finalized_at FROM data_snapshots
                      WHERE snapshot_id={ref}.snapshot_id) IS NOT NULL
                BEGIN SELECT RAISE(ABORT, 'finalized snapshot membership is immutable'); END"""
            )


def downgrade() -> None:
    raise RuntimeError("R2 immutable research lineage migration is not safely reversible")
