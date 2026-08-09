from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from app.domain.strategies.funding_carry import (
    FundingCarryEvaluationStage,
    FundingCarryRejectCode,
    FundingCarryShadowConfig,
    FundingCarryShadowEvaluator,
    FundingObservation,
    OrderBookObservation,
    ShadowDisposition,
)

FIXTURE = (
    Path(__file__).parents[1] / "fixtures" / "funding_carry_shadow_smoke_20260808T145448Z.json"
)


def _timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def test_saved_smoke_events_reproduce_rejected_economics_and_candidate_ids() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["source_run"] == "funding-carry-shadow-smoke-20260808T145448Z"
    assert fixture["raw_evidence_modified"] is False
    observed_ids: list[str] = []
    for record in fixture["records"]:
        funding = tuple(
            FundingObservation(
                event_id=item["event_id"],
                venue=item["venue"],
                instrument=item["instrument"],
                raw_funding_rate=Decimal(item["raw_funding_rate"]),
                funding_unit=item["funding_unit"],
                funding_interval_seconds=item["funding_interval_seconds"],
                next_funding_at=_timestamp(item["next_funding_at"]),
                source_timestamp=datetime.fromisoformat(item["source_timestamp"]),
                received_at=datetime.fromisoformat(item["received_at"]),
            )
            for item in record["funding"]
        )
        orderbooks = tuple(
            OrderBookObservation(
                event_id=item["event_id"],
                venue=item["venue"],
                instrument=item["instrument"],
                bids=tuple((Decimal(price), Decimal(quantity)) for price, quantity in item["bids"]),
                asks=tuple((Decimal(price), Decimal(quantity)) for price, quantity in item["asks"]),
                source_timestamp=datetime.fromisoformat(item["source_timestamp"]),
                received_at=datetime.fromisoformat(item["received_at"]),
                source_event_ids=tuple(item["source_event_ids"]),
            )
            for item in record["orderbooks"]
        )
        result = FundingCarryShadowEvaluator(FundingCarryShadowConfig()).evaluate(
            run_id=fixture["source_run"],
            funding=funding,
            orderbooks=orderbooks,
            now=datetime.fromisoformat(record["evaluated_at"]),
            code_commit_sha=record["code_commit_sha"],
            config_sha=record["config_sha"],
        )
        observed_ids.append(result.candidate_id)
        assert result.candidate_id == record["candidate_id"]
        assert result.disposition is ShadowDisposition.REJECTED
        assert result.rejection_reason is FundingCarryRejectCode.EDGE_BELOW_THRESHOLD
        assert result.evaluation_stage is FundingCarryEvaluationStage.THRESHOLD_EVALUATED
        assert result.economics_calculated
        assert result.gross_funding_edge_per_hour == Decimal(
            record["expected"]["gross_funding_edge_per_hour"]
        )
        assert result.expected_net_edge == Decimal(record["expected"]["expected_net_edge"])
        assert result.break_even_holding_hours == Decimal(
            record["expected"]["break_even_holding_hours"]
        )
        assert all(
            value is not None
            for value in (
                result.long_entry_vwap,
                result.short_entry_vwap,
                result.entry_fee_total,
                result.estimated_exit_fee,
                result.entry_slippage_total,
                result.estimated_exit_slippage,
                result.entry_basis_cost,
                result.round_trip_cost,
            )
        )
    assert len(observed_ids) == len(set(observed_ids)) == 5
