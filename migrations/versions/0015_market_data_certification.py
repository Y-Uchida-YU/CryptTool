"""Persist instrument-scoped market-data certification evidence.

Revision ID: 0015_market_data_certification
Revises: 0014_r3_evidence_states
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015_market_data_certification"
down_revision: str | None = "0014_r3_evidence_states"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("experimental_market_events") as batch:
        batch.add_column(sa.Column("exchange_timestamp", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("available_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("sequence", sa.BigInteger()))
        batch.add_column(sa.Column("connection_id", sa.String(length=36)))
        batch.add_column(sa.Column("reconciliation_state", sa.String(length=40)))
        batch.add_column(
            sa.Column(
                "normalizer_version",
                sa.String(length=120),
                nullable=False,
                server_default="unknown",
            )
        )
        batch.add_column(
            sa.Column("channel", sa.String(length=120), nullable=False, server_default="unknown")
        )
        batch.add_column(sa.Column("connection_epoch", sa.Integer()))
        batch.create_index("ix_experimental_market_events_available_at", ["available_at"])
    op.execute("UPDATE experimental_market_events SET available_at = received_at")
    with op.batch_alter_table("experimental_market_events") as batch:
        batch.alter_column("available_at", nullable=False)
    op.create_table(
        "market_data_certifications",
        sa.Column("certification_id", sa.String(length=160), primary_key=True),
        sa.Column("venue", sa.String(length=40), nullable=False, index=True),
        sa.Column("capability", sa.String(length=80), nullable=False, index=True),
        sa.Column("canonical_instrument_id", sa.String(length=100), nullable=False, index=True),
        sa.Column("verdict", sa.String(length=40), nullable=False, index=True),
        sa.Column("commit_sha", sa.String(length=40), nullable=False),
        sa.Column("adapter_version", sa.String(length=120), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("evidence_manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("evidence_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("venue", "capability", "canonical_instrument_id", "certification_id"),
        sa.CheckConstraint(
            "verdict IN ('pass', 'fail', 'insufficient_evidence')",
            name="ck_market_data_certification_verdict",
        ),
        sa.CheckConstraint("expires_at >= verified_at", name="ck_market_data_certification_expiry"),
        sa.CheckConstraint("length(commit_sha) = 40", name="ck_market_data_certification_commit"),
        sa.CheckConstraint(
            "length(evidence_manifest_sha256) = 64",
            name="ck_market_data_certification_manifest_hash",
        ),
    )
    op.create_table(
        "capability_promotions",
        sa.Column(
            "certification_id",
            sa.String(length=160),
            sa.ForeignKey("market_data_certifications.certification_id"),
            primary_key=True,
        ),
        sa.Column("venue", sa.String(length=40), nullable=False, index=True),
        sa.Column("capability", sa.String(length=80), nullable=False, index=True),
        sa.Column("canonical_instrument_id", sa.String(length=100), nullable=False, index=True),
        sa.Column("verification_run_id", sa.String(length=160), nullable=False, unique=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("capability_promotions")
    op.drop_table("market_data_certifications")
    with op.batch_alter_table("experimental_market_events") as batch:
        batch.drop_index("ix_experimental_market_events_available_at")
        for name in (
            "connection_epoch",
            "channel",
            "normalizer_version",
            "reconciliation_state",
            "connection_id",
            "sequence",
            "available_at",
            "exchange_timestamp",
        ):
            batch.drop_column(name)
