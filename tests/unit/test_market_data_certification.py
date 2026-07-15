from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine

from app.adapters.exchanges.public import BitgetMarketDataAdapter, HyperliquidMarketDataAdapter
from app.adapters.exchanges.websocket import ReconciliationState
from app.domain.venues.models import CapabilitySupport
from app.domain.venues.trusted_capabilities import TrustedCapabilityRecord
from app.infrastructure.database.models import Base
from app.services.research.certification import (
    FUNDING_CARRY_REQUIREMENT,
    CapabilityAuditArtifactResolver,
    CapabilityPromotionService,
    CertificationVerdict,
    ContractValidationSpec,
    InMemoryCertificationRepository,
    MarketDataCertificationService,
    ProductionEventCertificationGate,
    SQLCertificationRepository,
    StrategySnapshotService,
    StrictPaperReadiness,
    analyze_funding_intervals,
    certification_metrics,
    evaluate_strict_paper_readiness,
    event_timing_metrics,
    funding_payment_direction,
    normalize_exchange_timestamp,
    normalize_funding_rate,
    reconcile_funding_current_history,
    reconcile_values,
    require_operator_approval,
    validate_funding_interval,
    validate_order_book_events,
    write_certification_artifacts,
)
from app.services.research.models import (
    AvailabilityProvenance,
    RawMarketEvent,
    TimestampSemantic,
)
from app.services.research.repository import (
    InMemoryResearchRepository,
    PostgreSQLResearchRepository,
)

NOW = datetime(2026, 7, 15, 1, 0, tzinfo=UTC)
ROOT = Path(__file__).parents[2]
COMMIT = "a" * 40


def payload_for(capability: str) -> dict[str, object]:
    common: dict[str, object] = {
        "exchange": "hyperliquid",
        "symbol": "BTC",
        "received_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
    }
    if capability.startswith("funding"):
        return {
            **common,
            "rate": "0.0001",
            "next_funding_at": (NOW + timedelta(hours=8)).isoformat(),
            "funding_interval_seconds": 3600,
            "funding_schedule_source": "test_contract",
        }
    if capability == "trade":
        return {**common, "trade_id": "t-1", "price": "100", "quantity": "1", "side": "buy"}
    if capability == "ohlcv":
        return {**common, "open": "99", "high": "101", "low": "98", "close": "100", "volume": "10"}
    if capability == "open_interest":
        return {**common, "value": "1000", "unit": "base"}
    return {**common, "mid": "100"}


def event(
    capability: str = "funding_current",
    *,
    venue: str = "hyperliquid",
    instrument: str = "BTC",
    event_id: str | None = None,
    exchange_timestamp: datetime | None = NOW - timedelta(milliseconds=100),
    available_at: datetime = NOW,
) -> RawMarketEvent:
    raw = json.dumps(payload_for(capability), sort_keys=True, separators=(",", ":"))
    semantics = {
        "funding_history": TimestampSemantic.FUNDING_EFFECTIVE_TIME,
        "ohlcv": TimestampSemantic.CANDLE_OPEN_TIME,
        "trade": TimestampSemantic.REALTIME_EVENT,
    }
    return RawMarketEvent(
        event_id=event_id or f"{venue}-{instrument}-{capability}",
        venue=venue,
        canonical_instrument_id=instrument,
        venue_symbol=instrument if venue == "hyperliquid" else f"{instrument}USDT",
        event_type=capability,
        exchange_timestamp=exchange_timestamp,
        received_at=NOW,
        available_at=available_at,
        sequence=None,
        connection_id=None,
        reconciliation_state=None,
        payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),
        raw_payload=raw,
        normalizer_version="test-r4",
        capability_verification_run_id="unverified-experimental",
        created_at=NOW,
        timestamp_semantic=semantics.get(capability, TimestampSemantic.RECEIPT_ONLY),
        availability_provenance=(
            AvailabilityProvenance.EXCHANGE_PUBLISHED_TIME
            if capability in {"funding_history", "ohlcv"}
            else AvailabilityProvenance.OBSERVED_RETRIEVAL_TIME
        ),
        exchange_server_time=NOW - timedelta(milliseconds=20),
        timeframe="1m" if capability == "ohlcv" else None,
    )


def spec(
    capability: str = "funding_current",
    *,
    venue: str = "hyperliquid",
    instrument: str = "BTC",
) -> ContractValidationSpec:
    fixture = ROOT / f"tests/contracts/{venue}/public.json"
    fields = tuple(payload_for(capability))
    semantics = {
        "funding_history": TimestampSemantic.FUNDING_EFFECTIVE_TIME,
        "ohlcv": TimestampSemantic.CANDLE_OPEN_TIME,
        "trade": TimestampSemantic.REALTIME_EVENT,
    }
    return ContractValidationSpec(
        venue=venue,
        capability=capability,
        canonical_instrument_id=instrument,
        source_endpoint=f"public:{capability}",
        request_parameters=("symbol",),
        response_fields=fields,
        symbol=instrument if venue == "hyperliquid" else f"{instrument}USDT",
        price_unit="USD",
        quantity_unit="base",
        funding_unit="decimal" if capability.startswith("funding") else None,
        funding_interval_seconds=(
            (3600 if venue == "hyperliquid" else 28800)
            if capability.startswith("funding")
            else None
        ),
        timestamp_unit="milliseconds",
        timestamp_timezone="UTC",
        sequence_semantics="none for REST",
        snapshot_delta_semantics="not_applicable",
        null_behavior="missing optional timestamp is None",
        rate_limit_behavior="429 recorded",
        error_behavior="error persisted",
        fixture_path=str(fixture.relative_to(ROOT)),
        fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),
        normalization_test_node_id=(
            "tests/unit/test_market_data_certification.py::"
            "test_contract_fixture_is_bound_to_normalization"
        ),
        normalization_test_passed=True,
        minimum_event_count=1,
        minimum_coverage_ratio=Decimal("0.01"),
        maximum_stale_ratio=Decimal("1"),
        timestamp_semantic=semantics.get(capability, TimestampSemantic.RECEIPT_ONLY),
    )


def certified(
    capability: str = "funding_current",
) -> tuple[InMemoryCertificationRepository, object]:
    repository = InMemoryCertificationRepository()
    service = MarketDataCertificationService(
        repository,
        root=ROOT,
        commit_sha=COMMIT,
        adapter_version="adapter-v1",
        source_version="source-v1",
    )
    item = service.certify(
        certification_id=f"cert-{capability}",
        spec=spec(capability),
        events=(event(capability),),
        sample_start=NOW - timedelta(minutes=1),
        sample_end=NOW,
        cross_source_pairs=((Decimal("0.0001"), Decimal("0.0001")),),
        audit_passed=True,
        audit_run_id="audit-1",
        ci_run_id="ci-1",
        audit_artifact_sha256="f" * 64,
    )
    return repository, item


def test_old_historical_funding_is_event_age_not_clock_skew() -> None:
    old = replace(
        event("funding_history"),
        exchange_timestamp=NOW - timedelta(days=30),
        exchange_server_time=NOW - timedelta(milliseconds=25),
    )
    timing = event_timing_metrics(old)
    assert timing.event_age_seconds == Decimal(str(timedelta(days=30).total_seconds()))
    assert timing.transport_latency_seconds is None
    assert timing.clock_skew_seconds == Decimal("0.025")


def test_old_ohlcv_candle_is_not_clock_skew() -> None:
    old = replace(
        event("ohlcv"),
        exchange_timestamp=NOW - timedelta(days=7),
        exchange_server_time=NOW,
    )
    metrics = certification_metrics((old,), spec("ohlcv"), NOW - timedelta(minutes=1), NOW, ())
    assert metrics.maximum_clock_skew_ms == Decimal("0")
    assert metrics.timing[0].event_age_seconds == Decimal(str(timedelta(days=7).total_seconds()))


def test_exchange_server_time_drives_clock_skew_and_missing_is_unknown() -> None:
    realtime = event("trade")
    measured = event_timing_metrics(realtime)
    assert measured.clock_skew_seconds == Decimal("0.02")
    unknown = event_timing_metrics(replace(realtime, exchange_server_time=None))
    assert unknown.clock_skew_seconds is None


def test_descending_historical_history_is_not_live_out_of_order() -> None:
    first = replace(
        event("funding_history", event_id="new"),
        exchange_timestamp=NOW - timedelta(hours=1),
    )
    second = replace(
        event("funding_history", event_id="old"),
        exchange_timestamp=NOW - timedelta(hours=2),
    )
    metrics = certification_metrics(
        (first, second), spec("funding_history"), NOW - timedelta(minutes=1), NOW, ()
    )
    assert metrics.live_out_of_order_count == 0
    assert metrics.historical_source_order_reversed == 1


def test_duplicate_funding_does_not_cause_interval_mismatch() -> None:
    older = replace(
        event("funding_history", event_id="older"),
        exchange_timestamp=NOW - timedelta(hours=2),
    )
    newer = replace(
        event("funding_history", event_id="newer"),
        exchange_timestamp=NOW - timedelta(hours=1),
    )
    duplicate = replace(newer, event_id="overlap")
    result = analyze_funding_intervals((newer, duplicate, older), spec("funding_history"))
    assert result.duplicate_count == 1
    assert result.violations == ()


def test_null_payload_funding_interval_uses_contract_schedule() -> None:
    raw = json.dumps(
        {**payload_for("funding_history"), "funding_interval_seconds": None},
        sort_keys=True,
        separators=(",", ":"),
    )
    older = replace(
        event("funding_history", event_id="older-null-interval"),
        exchange_timestamp=NOW - timedelta(hours=2),
        raw_payload=raw,
        payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )
    newer = replace(
        event("funding_history", event_id="newer-null-interval"),
        exchange_timestamp=NOW - timedelta(hours=1),
        raw_payload=raw,
        payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )
    result = analyze_funding_intervals((older, newer), spec("funding_history"))
    assert result.violations == ()
    assert result.observations[0].expected_interval_seconds == 3600


def test_missing_funding_window_is_insufficient_evidence() -> None:
    older = replace(
        event("funding_history", event_id="older"),
        exchange_timestamp=NOW - timedelta(hours=3),
    )
    newer = replace(
        event("funding_history", event_id="newer"),
        exchange_timestamp=NOW - timedelta(hours=1),
    )
    result = analyze_funding_intervals((older, newer), spec("funding_history"))
    assert result.violations == ()
    assert result.missing_window_count == 1
    assert "funding history has missing windows" in result.insufficiencies


def test_historical_and_live_trade_have_different_order_rules() -> None:
    newer = event("trade", event_id="newer")
    older = replace(newer, event_id="older", exchange_timestamp=NOW - timedelta(seconds=2))
    live = certification_metrics((newer, older), spec("trade"), NOW - timedelta(minutes=1), NOW, ())
    historical_spec = replace(
        spec("trade"), timestamp_semantic=TimestampSemantic.HISTORICAL_EFFECTIVE_TIME
    )
    historical = certification_metrics(
        (
            replace(newer, timestamp_semantic=TimestampSemantic.HISTORICAL_EFFECTIVE_TIME),
            replace(older, timestamp_semantic=TimestampSemantic.HISTORICAL_EFFECTIVE_TIME),
        ),
        historical_spec,
        NOW - timedelta(minutes=1),
        NOW,
        (),
    )
    assert live.live_out_of_order_count == 1
    assert historical.live_out_of_order_count == 0


def test_audit_artifact_mismatch_blocks_and_valid_artifact_allows(tmp_path: Path) -> None:
    report = {
        "findings": [
            {
                "venue": "hyperliquid",
                "capability": "funding_history",
                "adapter_version": "adapter-v1",
                "source_version": "source-v1",
                "contract_fixture_sha256": "f" * 64,
                "passed": True,
                "test_result": "passed",
                "audit_run_id": "audit-1",
                "ci_run_id": "ci-1",
            }
        ]
    }
    report_bytes = json.dumps(report).encode()
    (tmp_path / "report.json").write_bytes(report_bytes)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "commit_sha": COMMIT,
                "report_file": "report.json",
                "report_sha256": hashlib.sha256(report_bytes).hexdigest(),
            }
        )
    )
    resolver = CapabilityAuditArtifactResolver(tmp_path)
    arguments = {
        "venue": "hyperliquid",
        "capability": "funding_history",
        "commit_sha": COMMIT,
        "adapter_version": "adapter-v1",
        "source_version": "source-v1",
        "fixture_sha256": "f" * 64,
    }
    assert resolver.resolve(**arguments) is not None
    assert resolver.resolve(**{**arguments, "adapter_version": "forged"}) is None


def test_historical_availability_provenance_blocks_unsupported_research() -> None:
    repository = InMemoryResearchRepository()
    unsupported = replace(
        event("funding_history"),
        availability_provenance=AvailabilityProvenance.OBSERVED_RETRIEVAL_TIME,
        capability_verification_run_id="verified-certification",
    )
    assert repository.add_raw_event(unsupported)
    manifest = StrategySnapshotService(repository).finalize(
        requirement=replace(
            FUNDING_CARRY_REQUIREMENT,
            required_capabilities=("funding_history",),
            required_venues=("hyperliquid",),
            minimum_coverage_ratio=Decimal("1"),
            minimum_history_windows=1,
        ),
        cutoff_at=NOW,
    )
    assert manifest.eligibility_status == "FINALIZED_NOT_ELIGIBLE"
    assert "historical availability is not point-in-time proven" in manifest.eligibility_reasons


def test_funding_decimal_unit_normalization() -> None:
    assert normalize_funding_rate("0.01", unit="decimal") == Decimal("0.01")
    assert normalize_funding_rate("0.01", unit="percent") == Decimal("0.0001")
    assert normalize_funding_rate("1", unit="percent") == Decimal("0.01")


def test_funding_sign_direction() -> None:
    assert (
        funding_payment_direction(Decimal("0.001"), position_quantity=Decimal("1")) == "long_pays"
    )
    assert (
        funding_payment_direction(Decimal("-0.001"), position_quantity=Decimal("1"))
        == "long_receives"
    )
    assert funding_payment_direction(Decimal("0"), position_quantity=Decimal("1")) == "neutral"


def test_funding_units_reject_invalid_values() -> None:
    assert normalize_funding_rate("1", unit="basis_points") == Decimal("0.0001")
    with pytest.raises(ValueError, match="not decimal"):
        normalize_funding_rate("not-a-rate", unit="decimal")
    with pytest.raises(ValueError, match="unknown"):
        normalize_funding_rate("1", unit="parts-per-million")


@pytest.mark.asyncio
async def test_venue_current_funding_contracts_preserve_units_and_missing_exchange_time() -> None:
    def hyperliquid_handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content)["type"] == "metaAndAssetCtxs"
        return httpx.Response(
            200,
            json=[{"universe": [{"name": "BTC"}]}, [{"funding": "0.0000125"}]],
        )

    hyper_client = httpx.AsyncClient(
        transport=httpx.MockTransport(hyperliquid_handler), base_url="https://test"
    )
    hyper = await HyperliquidMarketDataAdapter(hyper_client).fetch_current_funding_rate("BTC")
    assert hyper.rate == Decimal("0.0000125")
    assert hyper.exchange_timestamp is None
    assert hyper.next_funding_at is not None
    assert hyper.funding_schedule_source == "hyperliquid_documented_hourly_schedule"
    await hyper_client.aclose()

    def bitget_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v2/mix/market/current-fund-rate"
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "fundingRate": "0.000068",
                        "fundingRateInterval": "8",
                        "nextUpdate": "1784077200000",
                    }
                ]
            },
        )

    bitget_client = httpx.AsyncClient(
        transport=httpx.MockTransport(bitget_handler), base_url="https://test"
    )
    bitget = await BitgetMarketDataAdapter(bitget_client).fetch_current_funding_rate("BTCUSDT")
    assert bitget.rate == Decimal("0.000068")
    assert bitget.exchange_timestamp is None
    assert bitget.next_funding_at == datetime(2026, 7, 15, 1, 0, tzinfo=UTC)
    assert bitget.funding_schedule_source == "exchange_payload"
    await bitget_client.aclose()


def test_funding_interval_validation() -> None:
    validate_funding_interval(NOW, NOW + timedelta(hours=8), expected_seconds=28800)
    with pytest.raises(ValueError, match="interval"):
        validate_funding_interval(NOW, NOW + timedelta(hours=1), expected_seconds=28800)


def test_current_history_reconciliation() -> None:
    assert reconcile_funding_current_history(Decimal("0.001"), Decimal("0.001")) == 0
    with pytest.raises(ValueError, match="current/history"):
        reconcile_funding_current_history(Decimal("0.001"), Decimal("0.01"))


def test_timestamp_unit_mismatch_rejection() -> None:
    with pytest.raises(ValueError, match="timestamp unit mismatch"):
        normalize_exchange_timestamp(1_784_000_000_000, unit="seconds", received_at=NOW)


def test_future_exchange_timestamp_rejection() -> None:
    future = int((NOW + timedelta(hours=1)).timestamp() * 1000)
    with pytest.raises(ValueError, match="future"):
        normalize_exchange_timestamp(future, unit="milliseconds", received_at=NOW)


def test_timestamp_supported_units_and_invalid_unit() -> None:
    expected = NOW - timedelta(seconds=1)
    assert (
        normalize_exchange_timestamp(int(expected.timestamp()), unit="seconds", received_at=NOW)
        == expected
    )
    assert (
        normalize_exchange_timestamp(
            int(expected.timestamp() * 1_000_000), unit="microseconds", received_at=NOW
        )
        == expected
    )
    with pytest.raises(ValueError, match="unsupported"):
        normalize_exchange_timestamp(1, unit="nanoseconds", received_at=NOW)


def test_rest_ws_mark_reconciliation() -> None:
    absolute, relative = reconcile_values(
        Decimal("100"), Decimal("100.01"), maximum_relative_error=Decimal("0.001")
    )
    assert absolute == Decimal("0.01") and relative < Decimal("0.001")


def test_ohlcv_trade_reconciliation() -> None:
    _, relative = reconcile_values(
        Decimal("100"), Decimal("99.9"), maximum_relative_error=Decimal("0.01")
    )
    assert relative < Decimal("0.01")
    with pytest.raises(ValueError, match="cross-source"):
        reconcile_values(Decimal("100"), Decimal("80"), maximum_relative_error=Decimal("0.01"))


def test_instrument_specific_certification() -> None:
    repository = InMemoryCertificationRepository()
    service = MarketDataCertificationService(
        repository,
        root=ROOT,
        commit_sha=COMMIT,
        adapter_version="adapter-v1",
        source_version="source-v1",
    )
    btc = service.certify(
        certification_id="btc-cert",
        spec=spec(),
        events=(event(),),
        sample_start=NOW - timedelta(minutes=1),
        sample_end=NOW,
        cross_source_pairs=((Decimal("0.0001"), Decimal("0.0001")),),
    )
    hype = service.certify(
        certification_id="hype-cert",
        spec=spec(instrument="HYPE"),
        events=(event(),),
        sample_start=NOW - timedelta(minutes=1),
        sample_end=NOW,
    )
    assert btc.verdict is CertificationVerdict.PASS
    assert hype.verdict is CertificationVerdict.INSUFFICIENT_EVIDENCE


def test_expired_certification_blocks_production() -> None:
    repository, item = certified()
    record = CapabilityPromotionService(
        repository,
        commit_sha=COMMIT,
        adapter_version="adapter-v1",
        now=lambda: NOW,
    ).promote(item)
    with pytest.raises(ValueError, match="expired"):
        ProductionEventCertificationGate().require(
            event=event(),
            certification=item,
            trusted_record=record,
            adapter_version="adapter-v1",
            now=item.expires_at + timedelta(seconds=1),
        )


def test_adapter_version_mismatch_blocks_production() -> None:
    repository, item = certified()
    record = CapabilityPromotionService(
        repository, commit_sha=COMMIT, adapter_version="adapter-v1", now=lambda: NOW
    ).promote(item)
    with pytest.raises(ValueError, match="exactly match"):
        ProductionEventCertificationGate().require(
            event=event(),
            certification=item,
            trusted_record=record,
            adapter_version="adapter-v2",
            now=NOW,
        )


def test_failed_certification_cannot_promote() -> None:
    repository, item = certified()
    failed = replace(item, verdict=CertificationVerdict.FAIL, reasons=("failed",))
    repository.records[failed.certification_id] = (
        failed,
        repository.records[item.certification_id][1],
    )
    with pytest.raises(ValueError, match="cannot be promoted"):
        CapabilityPromotionService(
            repository, commit_sha=COMMIT, adapter_version="adapter-v1", now=lambda: NOW
        ).promote(failed)


def test_manual_live_verified_edit_cannot_bypass_registry() -> None:
    _, item = certified()
    forged = TrustedCapabilityRecord(
        venue="hyperliquid",
        capability="funding_current",
        support=CapabilitySupport.LIVE_VERIFIED,
        verification_run_id="manual-edit",
        verified_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        adapter_version="adapter-v1",
        source_version="source-v1",
        contract_fixture_sha256="a" * 64,
        audit_run_id="manual",
        canonical_instrument_id="BTC",
    )
    with pytest.raises(ValueError, match="exactly match"):
        ProductionEventCertificationGate().require(
            event=event(),
            certification=item,
            trusted_record=forged,
            adapter_version="adapter-v1",
            now=NOW,
        )


def production_repository() -> InMemoryResearchRepository:
    repository = InMemoryResearchRepository()
    requirement = replace(FUNDING_CARRY_REQUIREMENT, minimum_history_windows=1)
    for venue in requirement.required_venues:
        for capability in requirement.required_capabilities:
            repository.add_raw_event(
                event(
                    capability,
                    venue=venue,
                    event_id=f"{venue}-BTC-{capability}",
                )
            )
    return repository


def test_strategy_specific_snapshot_requirement() -> None:
    requirement = replace(FUNDING_CARRY_REQUIREMENT, minimum_history_windows=1)
    manifest = StrategySnapshotService(production_repository()).finalize(
        requirement=requirement,
        cutoff_at=NOW,
        snapshot_id="funding-snapshot",
    )
    assert manifest.eligibility_status == "FINALIZED_RESEARCH_ELIGIBLE"


def test_funding_snapshot_does_not_require_orderbook() -> None:
    requirement = replace(FUNDING_CARRY_REQUIREMENT, minimum_history_windows=1)
    repository = production_repository()
    manifest = StrategySnapshotService(repository).finalize(
        requirement=requirement,
        cutoff_at=NOW,
        snapshot_id="funding-without-book",
    )
    assert all(
        repository.get_raw_event(event_id).event_type != "orderbook_snapshot"
        for _, event_id, _ in manifest.events
    )


def test_strategy_snapshot_fails_closed_when_a_venue_capability_is_missing() -> None:
    repository = production_repository()
    repository.events.pop("bitget-BTC-funding_history")
    manifest = StrategySnapshotService(repository).finalize(
        requirement=replace(FUNDING_CARRY_REQUIREMENT, minimum_history_windows=1),
        cutoff_at=NOW,
        snapshot_id="funding-incomplete",
    )
    assert manifest.eligibility_status == "FINALIZED_NOT_ELIGIBLE"
    assert any("history windows" in reason for reason in manifest.eligibility_reasons)


def test_production_event_requires_exact_certification() -> None:
    repository, item = certified()
    record = CapabilityPromotionService(
        repository, commit_sha=COMMIT, adapter_version="adapter-v1", now=lambda: NOW
    ).promote(item)
    ProductionEventCertificationGate().require(
        event=event(),
        certification=item,
        trusted_record=record,
        adapter_version="adapter-v1",
        now=NOW,
    )
    with pytest.raises(ValueError, match="exactly match"):
        ProductionEventCertificationGate().require(
            event=event(instrument="HYPE"),
            certification=item,
            trusted_record=record,
            adapter_version="adapter-v1",
            now=NOW,
        )


def test_strict_paper_requires_operator_approval() -> None:
    readiness = evaluate_strict_paper_readiness(
        capabilities_live_verified=True,
        snapshot_eligible=True,
        research_completed=True,
        strategy_eligible=True,
        instrument_rules_complete=True,
        paper_risk_enabled=True,
        observation_candidate_exists=True,
    )
    assert readiness is StrictPaperReadiness.READY_FOR_OPERATOR_APPROVAL
    with pytest.raises(ValueError, match="operator approval"):
        require_operator_approval(readiness, operator_approved=False)


def test_contract_fixture_is_bound_to_normalization() -> None:
    assert spec().validate(ROOT) == ()
    invalid = replace(spec(), fixture_sha256="0" * 64)
    assert "contract fixture SHA-256 mismatch" in invalid.validate(ROOT)
    incomplete = replace(
        spec(),
        source_endpoint="",
        request_parameters=(),
        fixture_path="tests/contracts/missing.json",
        normalization_test_passed=False,
    )
    reasons = incomplete.validate(ROOT)
    assert "contract metadata is incomplete" in reasons
    assert "contract fixture is missing" in reasons
    assert "normalization test did not pass" in reasons


def test_certification_artifact_manifest_hashes(tmp_path: Path) -> None:
    repository, item = certified()
    stored = repository.get(item.certification_id)
    assert stored is not None
    manifest_path = write_certification_artifacts(
        root=tmp_path,
        certification=item,
        evidence=stored[1],
        contract=spec(),
        events=(event(),),
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for relative, expected in manifest["files"].items():
        path = manifest_path.parent / relative
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected


def test_sql_certification_and_experimental_provenance_roundtrip() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    research = PostgreSQLResearchRepository(engine)
    original = event()
    assert research.add_experimental_event(original, "implemented")
    assert research.list_experimental_events() == (original,)

    memory, item = certified()
    stored = memory.get(item.certification_id)
    assert stored is not None
    sql = SQLCertificationRepository(engine)
    sql.save(item, stored[1])
    assert sql.get(item.certification_id) == stored
    assert sql.list() == (item,)
    sql.save(item, stored[1])

    record = CapabilityPromotionService(
        sql,
        commit_sha=COMMIT,
        adapter_version="adapter-v1",
        now=lambda: NOW,
    ).promote(item)
    sql.save_promotion(item.certification_id, "BTC", record)


def test_certification_models_and_repository_fail_closed() -> None:
    repository, item = certified()
    evidence = repository.get(item.certification_id)
    assert evidence is not None
    with pytest.raises(ValueError, match="identity"):
        repository.save(item, replace(evidence[1], certification_id="wrong"))
    with pytest.raises(ValueError, match="immutable"):
        repository.save(replace(item, reasons=("changed",)), evidence[1])
    record = CapabilityPromotionService(
        repository,
        commit_sha=COMMIT,
        adapter_version="adapter-v1",
        now=lambda: NOW,
    ).promote(item)
    with pytest.raises(ValueError, match="instrument"):
        repository.save_promotion(item.certification_id, "HYPE", record)
    with pytest.raises(ValueError, match="venue"):
        replace(item, venue="mexc")
    with pytest.raises(ValueError, match="hashes"):
        replace(item, commit_sha="short")
    with pytest.raises(ValueError, match="manifest"):
        replace(item, evidence_manifest_sha256="short")
    with pytest.raises(ValueError, match="monotonic"):
        replace(item, expires_at=NOW - timedelta(seconds=1))


def test_promotion_rejects_untrusted_mismatched_and_incomplete_evidence() -> None:
    repository, item = certified()
    with pytest.raises(ValueError, match="repository-trusted"):
        CapabilityPromotionService(
            InMemoryCertificationRepository(),
            commit_sha=COMMIT,
            adapter_version="adapter-v1",
            now=lambda: NOW,
        ).promote(item)
    for service, message in (
        (
            CapabilityPromotionService(
                repository,
                commit_sha="b" * 40,
                adapter_version="adapter-v1",
                now=lambda: NOW,
            ),
            "commit SHA",
        ),
        (
            CapabilityPromotionService(
                repository,
                commit_sha=COMMIT,
                adapter_version="adapter-v2",
                now=lambda: NOW,
            ),
            "adapter version",
        ),
        (
            CapabilityPromotionService(
                repository,
                commit_sha=COMMIT,
                adapter_version="adapter-v1",
                now=lambda: item.expires_at + timedelta(seconds=1),
            ),
            "expired",
        ),
    ):
        with pytest.raises(ValueError, match=message):
            service.promote(item)

    original = repository.records[item.certification_id][1]
    repository.records[item.certification_id] = (item, replace(original, audit_passed=False))
    with pytest.raises(ValueError, match="incomplete"):
        CapabilityPromotionService(
            repository,
            commit_sha=COMMIT,
            adapter_version="adapter-v1",
            now=lambda: NOW,
        ).promote(item)


def test_event_validation_reports_future_missing_and_availability_errors() -> None:
    repository = InMemoryCertificationRepository()
    service = MarketDataCertificationService(
        repository,
        root=ROOT,
        commit_sha=COMMIT,
        adapter_version="adapter-v1",
        source_version="source-v1",
    )
    malformed = replace(
        event(),
        exchange_timestamp=NOW + timedelta(seconds=10),
        received_at=NOW,
        available_at=NOW - timedelta(seconds=1),
        raw_payload="{}",
        payload_sha256=hashlib.sha256(b"{}").hexdigest(),
    )
    item = service.certify(
        certification_id="invalid-event",
        spec=spec(),
        events=(malformed,),
        sample_start=NOW - timedelta(minutes=1),
        sample_end=NOW,
        cross_source_pairs=((Decimal("0.1"), Decimal("0.1")),),
    )
    assert item.verdict is CertificationVerdict.FAIL
    assert "available_at precedes received_at" in item.reasons
    assert "future exchange timestamp" in item.reasons
    assert any(reason.startswith("response fields missing") for reason in item.reasons)


def test_order_book_contract_semantics_and_quality() -> None:
    def book_event(
        *,
        venue: str,
        payload: dict[str, object],
        state: ReconciliationState | None = ReconciliationState.SYNCHRONIZED,
    ) -> RawMarketEvent:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return replace(
            event("trade", venue=venue),
            event_id=f"{venue}-book",
            event_type="orderbook_snapshot",
            raw_payload=raw,
            payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),
            reconciliation_state=state,
        )

    valid_payload = {
        "bids": ({"price": "99", "quantity": "2"}, {"price": "98", "quantity": "1"}),
        "asks": ({"price": "100", "quantity": "1"}, {"price": "101", "quantity": "2"}),
    }
    hyperliquid = replace(
        spec("trade"),
        capability="orderbook_snapshot",
        response_fields=("bids", "asks"),
        snapshot_delta_semantics="snapshot_only",
    )
    assert (
        validate_order_book_events(
            (book_event(venue="hyperliquid", payload=valid_payload),), hyperliquid
        )
        == ()
    )

    invalid_payload = {
        "bids": ({"price": "99", "quantity": "0"}, {"price": "99", "quantity": "1"}),
        "asks": ({"price": "98", "quantity": "1"}, {"price": "97", "quantity": "1"}),
    }
    reasons = validate_order_book_events(
        (book_event(venue="hyperliquid", payload=invalid_payload, state=None),),
        replace(hyperliquid, snapshot_delta_semantics="snapshot_and_delta"),
    )
    assert "order-book quantity is non-positive" in reasons
    assert "asks are not ascending" in reasons
    assert "order-book is crossed or locked" in reasons
    assert "duplicate order-book price level" in reasons
    assert "order-book is not synchronized" in reasons
    assert "Hyperliquid order-book semantics must be snapshot_only" in reasons

    bitget = replace(
        hyperliquid,
        venue="bitget",
        symbol="BTCUSDT",
        snapshot_delta_semantics="snapshot_only",
    )
    bitget_reasons = validate_order_book_events(
        (book_event(venue="bitget", payload={"bids": [], "asks": []}),), bitget
    )
    assert "order-book side is empty" in bitget_reasons
    schema_reasons = validate_order_book_events(
        (book_event(venue="bitget", payload={"unexpected": True}),), bitget
    )
    assert "order-book schema is invalid" in schema_reasons


def test_strict_paper_not_ready_and_approval_success() -> None:
    readiness = evaluate_strict_paper_readiness(
        capabilities_live_verified=False,
        snapshot_eligible=True,
        research_completed=True,
        strategy_eligible=True,
        instrument_rules_complete=True,
        paper_risk_enabled=True,
        observation_candidate_exists=True,
    )
    assert readiness is StrictPaperReadiness.NOT_READY
    with pytest.raises(ValueError, match="gate"):
        require_operator_approval(readiness, operator_approved=True)
    require_operator_approval(
        StrictPaperReadiness.READY_FOR_OPERATOR_APPROVAL,
        operator_approved=True,
    )
