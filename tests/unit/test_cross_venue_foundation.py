from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from app.adapters.exchanges.base import CapabilityUnavailableError
from app.adapters.exchanges.dex import (
    DydxMarketDataAdapter,
    LighterMarketDataAdapter,
    ParadexMarketDataAdapter,
)
from app.adapters.exchanges.public import HyperliquidMarketDataAdapter, MexcMarketDataAdapter
from app.adapters.exchanges.websocket import ResilientWebSocketSession, StreamClassification
from app.domain.market_data.models import OrderBook, OrderBookLevel
from app.domain.venues.attribution import VenueValueAttribution, aggregate_venue_value
from app.domain.venues.discovery import DISCOVERY_REGISTRY, DiscoveryDecision
from app.domain.venues.models import CapabilitySupport, CapabilityUseCase, VenueCapability

NOW = datetime(2026, 7, 12, tzinfo=UTC)


def test_capability_lifecycle_and_stale_expiry() -> None:
    capability = VenueCapability(name="trades", support=CapabilitySupport.UNAVAILABLE)
    capability = capability.transition(
        CapabilitySupport.DOCUMENTED, at=NOW, source_url="https://docs"
    )
    capability = capability.transition(CapabilitySupport.IMPLEMENTED, at=NOW + timedelta(hours=1))
    capability = capability.transition(
        CapabilitySupport.LIVE_VERIFIED,
        at=NOW + timedelta(hours=2),
        verification_run_id="run-1",
    )
    assert capability.supports(
        CapabilityUseCase.NEW_EXPOSURE, now=NOW + timedelta(hours=3), maximum_age=timedelta(days=1)
    )
    assert not capability.supports(
        CapabilityUseCase.NEW_EXPOSURE, now=NOW + timedelta(days=2), maximum_age=timedelta(days=1)
    )
    with pytest.raises(TypeError):
        bool(capability)
    assert MexcMarketDataAdapter.capabilities.open_interest.support == CapabilitySupport.DEGRADED
    assert MexcMarketDataAdapter.capabilities.open_interest.failure_reason


@pytest.mark.asyncio
async def test_hyperliquid_spot_and_perpetual_are_separate() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        kind = __import__("json").loads(request.content)["type"]
        if kind == "meta":
            return httpx.Response(200, json={"universe": [{"name": "BTC", "szDecimals": 5}]})
        if kind == "spotMeta":
            return httpx.Response(
                200,
                json={
                    "tokens": [
                        {
                            "name": "UBTC",
                            "index": 1,
                            "szDecimals": 5,
                            "weiDecimals": 8,
                            "deployer": "0x1",
                        },
                        {"name": "USDC", "index": 0, "szDecimals": 2, "weiDecimals": 6},
                    ],
                    "universe": [{"name": "@1", "tokens": [1, 0], "index": 1}],
                },
            )
        if kind == "predictedFundings":
            return httpx.Response(200, json=[["BTC", [["HlPerp", {"fundingRate": "0.1"}]]]])
        raise AssertionError(kind)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://test")
    adapter = HyperliquidMarketDataAdapter(client)
    assert (await adapter.fetch_perpetual_markets())[0].symbol == "BTC"
    spot = (await adapter.fetch_spot_markets())[0]
    assert spot["venue_internal_pair_name"] == "@1" and spot["canonical_token_name"] == "UBTC"
    assert (await adapter.fetch_predicted_funding())[0]["venue_symbol"] == "BTC"
    assert adapter.capabilities.spot.support != CapabilitySupport.LIVE_VERIFIED
    await client.aclose()


def test_nullable_exchange_timestamp_and_limited_depth_classification() -> None:
    now = datetime.now(UTC)
    book = OrderBook(
        exchange="x",
        symbol="BTC",
        exchange_timestamp=None,
        received_at=now,
        available_at=now,
        bids=(OrderBookLevel(price=99, quantity=1),),
        asks=(OrderBookLevel(price=101, quantity=1),),
    )
    assert book.exchange_timestamp is None
    session = ResilientWebSocketSession(
        venue="x",
        url="wss://example",
        subscription_id="depth20",
        subscribe=None,
        acknowledgement=None,
        classification=StreamClassification.LIMITED_DEPTH_SNAPSHOT_STREAM,
    )
    assert session.classification == StreamClassification.LIMITED_DEPTH_SNAPSHOT_STREAM


@pytest.mark.asyncio
async def test_dex_public_fixtures_and_execution_disabled() -> None:
    fixtures = {
        "/perpetualMarkets": {"markets": {"BTC-USD": {"baseAsset": "BTC", "quoteAsset": "USD"}}},
        "/markets": {
            "results": [
                {
                    "symbol": "BTC-USD-PERP",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "asset_kind": "PERP",
                }
            ]
        },
        "/orderBooks": {
            "order_books": [
                {
                    "symbol": "ETH-USDC",
                    "market_id": 1,
                    "quote_symbol": "USDC",
                    "supported_price_decimals": 2,
                    "supported_size_decimals": 4,
                }
            ]
        },
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=fixtures[request.url.path])

    for adapter_type, path in (
        (DydxMarketDataAdapter, "/perpetualMarkets"),
        (ParadexMarketDataAdapter, "/markets"),
        (LighterMarketDataAdapter, "/orderBooks"),
    ):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://test")
        adapter = adapter_type(client)
        assert await adapter.fetch_markets(), path
        assert not adapter.data_enabled and not adapter.execution_enabled
        await client.aclose()
    with pytest.raises(CapabilityUnavailableError):
        await LighterMarketDataAdapter().fetch_liquidations("0")


def test_incremental_value_and_discovery_fail_closed() -> None:
    row = VenueValueAttribution(
        venue="dydx",
        opportunities_discovered=2,
        opportunities_unique_to_venue=1,
        gross_edge_contribution=Decimal("10"),
        net_edge_contribution=Decimal("7"),
        fees=1,
        slippage=1,
        failed_leg_cost=1,
        stale_data_rejection_count=2,
        api_outage_count=1,
        venue_exclusion_count=3,
        capital_required=100,
        capital_efficiency=Decimal("0.07"),
        maximum_venue_exposure=50,
        risk_reduction=Decimal("2"),
    )
    report = aggregate_venue_value((row,))
    assert report["incremental_net_pnl"] == 7 and report["risk_reduction"] == 2
    assert {candidate.venue for candidate in DISCOVERY_REGISTRY} == {"btcc", "gateio"}
    assert all(
        candidate.decision == DiscoveryDecision.PENDING_OPERATOR_VERIFICATION
        and not candidate.eligible_for_adapter(
            minimum_uptime=0.99, minimum_depth_usd=Decimal("10000"), withdrawals_verified=False
        )
        for candidate in DISCOVERY_REGISTRY
    )
