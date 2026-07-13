from __future__ import annotations

from datetime import UTC
from typing import Any, cast

from sqlalchemy import insert, update
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.infrastructure.database.models import PreflightBindingRow
from app.services.live_trading.cross_venue_preflight import (
    PositionReconciliationSnapshot,
    PreflightBinding,
    PreflightBindingState,
)


class PostgreSQLPreflightBindingRepository:
    """Durable optimistic-CAS storage for cross-venue execution bindings."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @property
    def durable(self) -> bool:
        return True

    def get(self, signal_id: str) -> PreflightBinding | None:
        with Session(self.engine) as session:
            row = session.get(PreflightBindingRow, signal_id)
            return self._to_binding(row) if row is not None else None

    def compare_and_set(
        self, signal_id: str, expected_version: int, binding: PreflightBinding
    ) -> bool:
        if binding.signal_id != signal_id or binding.version != expected_version + 1:
            return False
        values = self._values(binding)
        with Session(self.engine) as session:
            try:
                if expected_version == 0:
                    result = session.execute(insert(PreflightBindingRow).values(**values))
                else:
                    result = session.execute(
                        update(PreflightBindingRow)
                        .where(
                            PreflightBindingRow.signal_id == signal_id,
                            PreflightBindingRow.version == expected_version,
                        )
                        .values(**values)
                    )
                if cast(CursorResult[Any], result).rowcount != 1:
                    session.rollback()
                    return False
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

    @staticmethod
    def _values(binding: PreflightBinding) -> dict[str, object]:
        return {
            "signal_id": binding.signal_id,
            "preflight_hash": binding.preflight_hash,
            "state": binding.state.value,
            "first_leg_role": binding.first_leg_role,
            "first_order_request_id": binding.first_order_request_id,
            "first_external_order_id": binding.first_external_order_id,
            "second_order_request_id": binding.second_order_request_id,
            "second_external_order_id": binding.second_external_order_id,
            "version": binding.version,
            "created_at": binding.created_at,
            "updated_at": binding.updated_at,
            "failure_reason": binding.failure_reason,
            "position_venue": (
                binding.position_snapshot.venue if binding.position_snapshot else None
            ),
            "position_symbol": (
                binding.position_snapshot.symbol if binding.position_snapshot else None
            ),
            "position_quantity_before": (
                binding.position_snapshot.quantity_before if binding.position_snapshot else None
            ),
            "position_captured_at": (
                binding.position_snapshot.captured_at if binding.position_snapshot else None
            ),
        }

    @staticmethod
    def _to_binding(row: PreflightBindingRow) -> PreflightBinding:
        return PreflightBinding(
            signal_id=row.signal_id,
            preflight_hash=row.preflight_hash,
            state=PreflightBindingState(row.state),
            first_leg_role=row.first_leg_role,
            first_order_request_id=row.first_order_request_id,
            first_external_order_id=row.first_external_order_id,
            second_order_request_id=row.second_order_request_id,
            second_external_order_id=row.second_external_order_id,
            version=row.version,
            created_at=row.created_at,
            updated_at=row.updated_at,
            failure_reason=row.failure_reason,
            position_snapshot=(
                PositionReconciliationSnapshot(
                    venue=row.position_venue,
                    symbol=row.position_symbol,
                    quantity_before=row.position_quantity_before,
                    captured_at=(
                        row.position_captured_at.replace(tzinfo=UTC)
                        if row.position_captured_at.tzinfo is None
                        else row.position_captured_at
                    ),
                )
                if row.position_venue is not None
                and row.position_symbol is not None
                and row.position_quantity_before is not None
                and row.position_captured_at is not None
                else None
            ),
        )
