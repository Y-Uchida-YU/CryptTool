from dataclasses import replace
from datetime import UTC, datetime, timedelta
from inspect import signature
from pathlib import Path

import pytest

from app.adapters.exchanges.dex import DydxMarketDataAdapter
from app.adapters.exchanges.public import PublicRestAdapter
from app.domain.market_data.evidence import (
    CapabilityEvidence,
    SignalDataEvidence,
    SourceEventEvidence,
    require_signal_capabilities,
)
from app.domain.strategies.cross_venue import CrossVenueFundingArbitrageStrategy
from app.domain.strategies.liquidation import LiquidationStrategyCapabilityGate
from app.domain.venues.models import CapabilitySupport, CapabilityUseCase, VenueCapabilityMatrix
from app.services.capability_audit import CapabilityAuditReport, CapabilityContractAuditor

NOW = datetime(2025, 1, 1, tzinfo=UTC)


def evidence(
    capability: str,
    *,
    support: CapabilitySupport = CapabilitySupport.LIVE_VERIFIED,
    use_case: CapabilityUseCase = CapabilityUseCase.SIGNAL_GENERATION,
) -> CapabilityEvidence:
    event = SourceEventEvidence(
        "event", "dydx", "BTC", capability, NOW, NOW, NOW, "a" * 64, None, None, None, 1
    )
    return CapabilityEvidence("dydx", capability, use_case, support, NOW, "run", (event,))


def test_signal_gate_requires_fresh_live_verified_evidence() -> None:
    value = SignalDataEvidence.build("signal-1", (evidence("funding_history"),))
    assert (
        require_signal_capabilities("signal-1", value, ("funding_history",), "dydx", NOW, 30)
        is None
    )
    assert require_signal_capabilities("signal-1", value, ("trades",), "dydx", NOW, 30)
    assert require_signal_capabilities("wrong", value, ("funding_history",), "dydx", NOW, 30)
    stale = SignalDataEvidence.build("signal-1", (evidence("funding_history"),))
    assert require_signal_capabilities(
        "signal-1", stale, ("funding_history",), "dydx", NOW + timedelta(seconds=31), 30
    )
    assert require_signal_capabilities(
        "signal-1", stale, ("funding_history",), "dydx", NOW - timedelta(seconds=1), 30
    )
    assert CrossVenueFundingArbitrageStrategy().validate_evidence(
        "signal-1", value, "dydx", NOW, 30
    )
    assert LiquidationStrategyCapabilityGate().validate_evidence("signal-1", value, "dydx", NOW, 30)


def test_strategy_evaluate_contract_cannot_bypass_evidence() -> None:
    parameters = signature(CrossVenueFundingArbitrageStrategy.evaluate).parameters
    for name in (
        "signal_id",
        "evidence",
        "receive_venue",
        "pay_venue",
        "now",
        "maximum_age_seconds",
    ):
        assert name in parameters and parameters[name].default is parameters[name].empty


def test_same_capability_from_two_venues_is_preserved_and_duplicate_key_rejected() -> None:
    first = evidence("funding_history")
    second_event = replace(first.source_events[0], event_id="event-2", venue="other")
    second = replace(first, venue="other", source_events=(second_event,))
    dual = SignalDataEvidence.build("signal-1", (first, second))
    assert (
        require_signal_capabilities("signal-1", dual, ("funding_history",), "dydx", NOW, 30) is None
    )
    assert (
        require_signal_capabilities("signal-1", dual, ("funding_history",), "other", NOW, 30)
        is None
    )

    duplicate = SignalDataEvidence.build("signal-1", (first, replace(first)))
    rejection = require_signal_capabilities(
        "signal-1", duplicate, ("funding_history",), "dydx", NOW, 30
    )
    assert rejection is not None and "duplicate" in rejection.reason


def test_auditor_manifest_owner_hash_and_empty_report() -> None:
    root = Path(__file__).parents[2]

    def test_runner(arguments: list[str]) -> int:
        return 1 if "test_node_that_does_not_exist" in arguments[-1] else 0

    auditor = CapabilityContractAuditor(root, now=NOW, test_runner=test_runner)
    adapter = DydxMarketDataAdapter()
    report = auditor.audit(adapter, adapter.capabilities)
    assert report.passed and report.findings
    assert all(
        finding.test_node_id
        and finding.test_file_sha256
        and finding.test_result == "passed"
        and finding.audit_run_id
        and finding.audited_at == NOW
        for finding in report.findings
    )
    capability = report.findings[0].capability
    original = auditor.entries[("dydx", capability)]
    auditor.entries[("dydx", capability)] = {
        **original,
        "normalization_test": {
            "test_id": "tests/unit/test_venues.py::test_node_that_does_not_exist"
        },
    }
    failed_node = auditor.audit(adapter, adapter.capabilities)
    assert not failed_node.passed
    assert any(item.test_result == "not_collected" for item in failed_node.findings)
    assert not CapabilityAuditReport("x", NOW, ()).passed
    assert auditor._implementation_owner(DydxMarketDataAdapter, "health_check") is PublicRestAdapter
    assert auditor._implementation_owner(DydxMarketDataAdapter, "missing") is None

    empty = VenueCapabilityMatrix(venue="x", detected_at=NOW, source_version="x")
    assert not auditor.audit(adapter, empty).passed


def test_boolean_capability_declarations_are_rejected() -> None:
    with pytest.raises(TypeError, match="Boolean capabilities"):
        VenueCapabilityMatrix(venue="x", detected_at=NOW, source_version="x", spot=True)
