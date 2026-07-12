from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from app.adapters.exchanges.base import CapabilityUnavailableError
from app.adapters.exchanges.public import (
    AsterMarketDataAdapter,
    BitgetMarketDataAdapter,
    HyperliquidMarketDataAdapter,
    MexcMarketDataAdapter,
)
from app.adapters.exchanges.staged_execution import (
    AsterExecutionAdapter,
    BitgetExecutionAdapter,
    ExecutionNotActivatedError,
    HyperliquidExecutionAdapter,
    MexcExecutionAdapter,
)
from app.config.settings import Settings
from app.domain.execution.leg_state import LegExecutionMachine, LegExecutionState, LegRiskPolicy
from app.domain.market_data.clock import VenueClock
from app.domain.strategies.cross_venue import (
    CrossVenueBasisStrategy,
    CrossVenueFundingArbitrageStrategy,
    ExecutableBook,
    FundingLeg,
)
from app.domain.venues.models import (
    CanonicalInstrument,
    InstrumentKind,
    VenueCapabilityMatrix,
    VenueEligibility,
    VenueEligibilityStatus,
    VenueInstrumentMapping,
)
from app.domain.venues.risk import (
    DEX_SIGNALS,
    VenueRiskObservation,
    VenueRiskSignal,
    venue_risk_score,
)
from app.services.instrument_mapping import InstrumentMappingRegistry
from app.services.venue_monitor import VenueRiskMonitor
from app.services.venue_risk import VenueRiskBudget
from app.services.whale_analytics import WalletSnapshot, WhaleWalletAnalytics

NOW = datetime(2026, 7, 12, tzinfo=UTC)


def eligibility(**overrides: object) -> VenueEligibility:
    values = {
        "venue": "venue",
        "status": VenueEligibilityStatus.ENABLED,
        "jurisdiction": "JP",
        "terms_checked_at": NOW,
        "operator_account_verified": True,
        "api_market_data_available": True,
        "api_execution_available": True,
        "deposits_available": True,
        "withdrawals_available": True,
        "execution_smoke_test_passed": True,
        "requires_location_evasion": False,
        "reason": "verified",
        **overrides,
    }
    return VenueEligibility(**values)  # type: ignore[arg-type]


def test_eligibility_fails_closed_and_exit_only_reduces() -> None:
    allowed, _ = eligibility().permits_new_orders(NOW + timedelta(days=29))
    assert allowed
    stale, reason = eligibility().permits_new_orders(NOW + timedelta(days=30))
    assert not stale and "stale" in reason
    evasion, _ = eligibility(requires_location_evasion=True).permits_new_orders(NOW)
    assert not evasion
    exit_only = eligibility(status="exit_only")
    assert not exit_only.permits_new_orders(NOW)[0]
    assert exit_only.permits_reduction(NOW)[0]
    assert not eligibility(status="data_only").permits_reduction(NOW)[0]


def test_capability_and_instrument_identity_are_explicit() -> None:
    matrix = VenueCapabilityMatrix(venue="x", detected_at=NOW, source_version="test", spot=True)
    matrix.require("spot")
    with pytest.raises(CapabilityUnavailableError):
        matrix.require("open_interest")
    with pytest.raises(ValueError, match="unknown"):
        matrix.require("made_up")
    canonical = CanonicalInstrument(
        instrument_id="BTC-USDT-PERP",
        base_asset_id="BTC",
        quote_asset_id="USDT",
        settlement_asset_id="USDT",
        kind=InstrumentKind.PERPETUAL,
        contract_multiplier=Decimal("1"),
        index_composition_id="btc-usd-index-a",
        funding_interval_minutes=480,
    )
    mapping = VenueInstrumentMapping(
        venue="x",
        venue_symbol="BTCUSDT",
        canonical_instrument_id=canonical.instrument_id,
        base="BTC",
        quote="USDT",
        settlement="USDT",
        kind="perpetual",
        inverse=False,
        contract_multiplier=1,
        index_composition_id="btc-usd-index-a",
        funding_interval_minutes=480,
        verified_at=NOW,
    )
    assert mapping.matches(canonical)
    assert not mapping.model_copy(update={"quote": "USDC"}).matches(canonical)
    registry = InstrumentMappingRegistry(
        (canonical,),
        (
            mapping,
            mapping.model_copy(update={"venue": "y", "venue_symbol": "BTC-USDT"}),
        ),
    )
    assert registry.require_same_instrument(("x", "BTCUSDT"), ("y", "BTC-USDT")) == canonical
    with pytest.raises(ValueError, match="inconsistent"):
        InstrumentMappingRegistry(
            (canonical,), (mapping.model_copy(update={"settlement": "USDC"}),)
        )


def test_clock_vwap_funding_and_non_atomic_leg_risk() -> None:
    clock = VenueClock(timedelta(seconds=2))
    first_clock = clock.stamp("first", NOW, NOW)
    second_clock = clock.stamp("second", NOW + timedelta(milliseconds=100), NOW)
    buy = ExecutableBook(
        venue="first",
        symbol="BTC",
        bids=((Decimal("99"), Decimal("2")),),
        asks=((Decimal("100"), Decimal("1")), (Decimal("101"), Decimal("1"))),
        clock=first_clock,
    )
    sell = ExecutableBook(
        venue="second",
        symbol="BTC",
        bids=((Decimal("103"), Decimal("1")), (Decimal("102"), Decimal("1"))),
        asks=((Decimal("104"), Decimal("2")),),
        clock=second_clock,
    )
    basis = CrossVenueBasisStrategy(clock).evaluate(
        buy,
        sell,
        Decimal("2"),
        fees=Decimal("1"),
        expected_exit_cost=Decimal("1"),
        latency_buffer=Decimal("0.5"),
        risk_premium=Decimal("0.5"),
    )
    assert basis.buy_ask_vwap == Decimal("100.5")
    assert basis.sell_bid_vwap == Decimal("102.5")
    assert basis.executable_spread == Decimal("1")
    clock.observe_server_time("first", NOW, NOW - timedelta(milliseconds=100), NOW)
    assert clock.synchronized("first", timedelta(milliseconds=50))[0]
    assert not clock.synchronized("missing", timedelta(0))[0]
    leg = FundingLeg(
        venue="a",
        symbol="BTC",
        quote_currency="USDT",
        settlement_currency="USDT",
        expected_rates=(Decimal("0.001"), Decimal("0.001")),
        notional=1000,
        fee_rate_round_trip=Decimal("0.001"),
        slippage_rate_round_trip=Decimal("0.001"),
    )
    other = leg.model_copy(
        update={
            "venue": "b",
            "quote_currency": "USDC",
            "settlement_currency": "USDC",
            "expected_rates": (Decimal("0.0002"), Decimal("0.0002")),
        }
    )
    carry = CrossVenueFundingArbitrageStrategy().evaluate(
        leg,
        other,
        expected_basis_convergence_loss=Decimal("0.2"),
        transfer_cost=Decimal("0.1"),
        venue_risk_premium=Decimal("0.3"),
    )
    assert carry.currency_risk_charge > 0
    assert carry.stressed_net_carry < carry.expected_net_carry
    machine = LegExecutionMachine(
        LegRiskPolicy(maximum_naked_exposure=50, emergency_hedge_venue="emergency"),
        NOW,
        Decimal("1"),
        Decimal("100"),
    )
    machine.submit_first(NOW)
    chase = machine.submit_second(NOW, Decimal("101"))
    assert chase.state == LegExecutionState.UNWINDING
    machine.submit_first(NOW)
    halted = machine.reconcile(NOW, Decimal("1"), Decimal("0"))
    assert halted.state == LegExecutionState.HALTED


def test_whale_features_are_analytics_only() -> None:
    previous = WalletSnapshot(
        venue="hyperliquid",
        wallet="0xabc",
        symbol="BTC",
        observed_at=NOW,
        position=1,
        realized_pnl=0,
        unrealized_pnl=0,
        leverage=2,
        liquidation_price=80,
        mark_price=100,
        account_equity=1000,
    )
    current = previous.model_copy(
        update={
            "observed_at": NOW + timedelta(minutes=1),
            "position": Decimal("2"),
            "unrealized_pnl": Decimal("10"),
            "cumulative_deposits": Decimal("50"),
        }
    )
    features = WhaleWalletAnalytics().build(
        current,
        previous,
        peer_positions=(Decimal("1"), Decimal("-1")),
        historical_outcomes=(True, False),
    )
    assert features.position_change == 1
    assert features.crowding == 0.5
    assert features.historical_hit_rate == 0.5
    adjusted, evidence = WhaleWalletAnalytics().regime_confidence_overlay(0.8, (features,))
    assert adjusted < 0.8 and evidence


@pytest.mark.asyncio
async def test_hyperliquid_contract_parsing_and_staged_execution() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        payload = __import__("json").loads(request.content)
        if payload["type"] == "meta":
            return httpx.Response(200, json={"universe": [{"name": "BTC", "szDecimals": 5}]})
        if payload["type"] == "l2Book":
            return httpx.Response(
                200,
                json={
                    "time": 1_700_000_000_000,
                    "levels": [[{"px": "99", "sz": "1"}], [{"px": "101", "sz": "2"}]],
                },
            )
        if payload["type"] == "clearinghouseState":
            return httpx.Response(
                200,
                json={
                    "marginSummary": {"accountValue": "1000"},
                    "assetPositions": [
                        {
                            "position": {
                                "coin": "BTC",
                                "szi": "2",
                                "unrealizedPnl": "10",
                                "liquidationPx": "80",
                                "leverage": {"value": 2},
                            }
                        }
                    ],
                },
            )
        if payload["type"] == "userFills":
            return httpx.Response(200, json=[{"coin": "BTC", "closedPnl": "3"}])
        if payload["type"] == "userNonFundingLedgerUpdates":
            return httpx.Response(200, json=[{"delta": {"type": "deposit", "usdc": "50"}}])
        if payload["type"] == "allMids":
            return httpx.Response(200, json={"BTC": "100"})
        raise AssertionError(payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://test")
    adapter = HyperliquidMarketDataAdapter(client)
    markets = await adapter.fetch_markets()
    book = await adapter.fetch_order_book("BTC")
    wallet = await adapter.fetch_wallet_snapshot("0xabc", "BTC")
    assert markets[0].quote == "USDC" and book.bids[0].price == 99
    assert wallet.realized_pnl == 3 and wallet.cumulative_deposits == 50
    for execution in (
        HyperliquidExecutionAdapter(),
        AsterExecutionAdapter(),
        BitgetExecutionAdapter(),
        MexcExecutionAdapter(),
    ):
        assert not execution.is_concrete and not await execution.health_check()
        with pytest.raises(ExecutionNotActivatedError):
            await execution.fetch_positions()
    await client.aclose()


def test_default_settings_disable_all_execution_and_forbid_evasion() -> None:
    settings = Settings(_env_file=None)
    assert settings.venues["btcc"].eligibility_status == VenueEligibilityStatus.PENDING_VERIFICATION
    assert all(not venue.execution_enabled for venue in settings.venues.values())
    invalid = settings.model_dump()
    invalid["venues"]["bybit"]["execution_enabled"] = True
    with pytest.raises(ValueError, match="forbidden"):
        Settings.model_validate(invalid)


def test_venue_risk_observations_fail_closed() -> None:
    healthy = VenueRiskObservation(
        venue="hyperliquid",
        signal=VenueRiskSignal.ORACLE_DIVERGENCE,
        observed_at=NOW,
        healthy=True,
        severity=0,
        evidence="within tolerance",
    )
    degraded = healthy.model_copy(
        update={"healthy": False, "severity": 0.8, "signal": VenueRiskSignal.RPC_DISAGREEMENT}
    )
    assert venue_risk_score((healthy, degraded)) == 0.8
    assert venue_risk_score(()) == 0
    monitor = VenueRiskMonitor()
    assert not monitor.execution_health("hyperliquid", NOW, dex=True)[0]
    for signal in DEX_SIGNALS:
        monitor.record(
            VenueRiskObservation(
                venue="hyperliquid",
                signal=signal,
                observed_at=NOW,
                healthy=True,
                severity=0,
                evidence="healthy",
            )
        )
    assert monitor.execution_health("hyperliquid", NOW, dex=True)[0]


def test_venue_risk_budget_checks_individual_group_and_total_allocations() -> None:
    settings = Settings(_env_file=None)
    budget = VenueRiskBudget(settings.venue_risk)
    accepted = budget.evaluate(
        Decimal("1000"),
        {"hyperliquid": Decimal("300"), "gmo_coin": Decimal("300")},
    )
    assert accepted.allowed
    rejected = budget.evaluate(
        Decimal("1000"),
        {"hyperliquid": Decimal("400"), "mexc": Decimal("200"), "bitbank": Decimal("500")},
    )
    assert not rejected.allowed
    assert any("total equity" in reason for reason in rejected.reasons)


def test_priority_one_capability_matrices_exist() -> None:
    for adapter in (
        HyperliquidMarketDataAdapter,
        AsterMarketDataAdapter,
        BitgetMarketDataAdapter,
        MexcMarketDataAdapter,
    ):
        assert adapter.capabilities.perpetual
        assert adapter.capabilities.orderbook_snapshot
    assert not MexcMarketDataAdapter.capabilities.open_interest
