from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.adapters.exchanges.base import CapabilityUnavailableError
from app.adapters.exchanges.dex import (
    DydxMarketDataAdapter,
    LighterMarketDataAdapter,
    ParadexMarketDataAdapter,
)

NOW = datetime(2025, 1, 1, tzinfo=UTC)


def client(handler: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://test")


@pytest.mark.asyncio
async def test_dydx_typed_public_contracts_and_fail_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/perpetualMarkets":
            assert request.url.params.get("ticker") is None
            return httpx.Response(
                200,
                json={
                    "markets": {
                        "BTC-USD": {
                            "baseAsset": "BTC",
                            "quoteAsset": "USD",
                            "tickSize": "1",
                            "stepSize": ".001",
                        }
                    }
                },
            )
        if path.startswith("/candles/"):
            assert request.url.params["resolution"] == "1MIN"
            return httpx.Response(
                200,
                json={
                    "candles": [
                        {
                            "startedAt": "2025-01-01T00:00:00Z",
                            "open": "100",
                            "high": "102",
                            "low": "99",
                            "close": "101",
                            "baseTokenVolume": "4",
                            "closed": True,
                        }
                    ]
                },
            )
        if path.startswith("/orderbooks/"):
            return httpx.Response(
                200,
                json={
                    "bids": [{"price": "99", "size": "1"}],
                    "asks": [{"price": "101", "size": "2"}],
                },
            )
        if path.startswith("/trades/"):
            return httpx.Response(
                200,
                json={
                    "trades": [
                        {
                            "createdAt": "2025-01-01T00:00:00Z",
                            "id": "t1",
                            "price": "100",
                            "size": "1",
                            "side": "BUY",
                        }
                    ]
                },
            )
        if path.startswith("/historicalFunding/"):
            return httpx.Response(
                200,
                json={
                    "historicalFunding": [{"effectiveAt": "2025-01-01T00:00:00Z", "rate": ".001"}]
                },
            )
        if path == "/time":
            return httpx.Response(200, json={"iso": "2025-01-01T00:00:00Z"})
        if path == "/height":
            return httpx.Response(200, json={"height": 10})
        if path == "/screen":
            assert request.url.params["address"] == "addr"
            return httpx.Response(200, json={"restricted": False})
        raise AssertionError(str(request.url))

    c = client(handler)
    adapter = DydxMarketDataAdapter(c)
    assert await adapter.fetch_markets() and (await adapter.fetch_market_metadata()).payload
    assert (await adapter.fetch_ohlcv("BTC-USD", "1m", NOW, NOW))[0].close == 101
    assert (await adapter.fetch_order_book("BTC-USD")).exchange_timestamp is None
    assert (await adapter.fetch_recent_trades("BTC-USD"))[0].trade_id == "t1"
    assert (await adapter.fetch_funding_rates("BTC-USD", end=NOW))[0].rate == Decimal(".001")
    assert (await adapter.fetch_indexer_time()).payload and (
        await adapter.fetch_indexer_height()
    ).payload
    assert not (await adapter.screen_address("addr")).payload["restricted"]
    assert (await adapter.fetch_current_market_state("BTC-USD")).payload
    with pytest.raises(ValueError, match="unsupported"):
        await adapter.fetch_ohlcv("BTC", "2m")
    with pytest.raises(ValueError, match="address"):
        await adapter.screen_address(" ")
    with pytest.raises(CapabilityUnavailableError):
        await adapter.fetch_liquidations("BTC")
    await c.aclose()


@pytest.mark.asyncio
async def test_paradex_typed_and_raw_public_contracts() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        payload: object
        if path == "/markets":
            payload = {
                "results": [
                    {
                        "symbol": "BTC-USD-PERP",
                        "base_currency": "BTC",
                        "quote_currency": "USD",
                        "asset_kind": "PERP",
                        "price_tick_size": "1",
                        "order_size_increment": ".001",
                    }
                ]
            }
        elif path == "/markets/klines":
            payload = {"results": [[1735689600000, "100", "102", "99", "101", "2"]]}
        elif path == "/orderbook/BTC":
            payload = {
                "last_updated_at": 1735689600000,
                "seq_no": 2,
                "bids": [["99", "1"]],
                "asks": [["101", "1"]],
            }
        elif path == "/trades":
            payload = {
                "results": [
                    {
                        "created_at": 1735689600000,
                        "id": "t",
                        "price": "100",
                        "size": "1",
                        "side": "BUY",
                    }
                ]
            }
        elif path == "/funding/data":
            payload = {"results": [{"created_at": 1735689600000, "funding_rate": ".001"}]}
        else:
            payload = {"ok": True}
        return httpx.Response(200, json=payload)

    c = client(handler)
    a = ParadexMarketDataAdapter(c)
    assert await a.fetch_markets()
    with pytest.raises(ValueError):
        await a.fetch_ohlcv("BTC", "1m")
    assert await a.fetch_ohlcv("BTC", "1m", NOW, NOW)
    assert (await a.fetch_order_book("BTC")).sequence == 2
    assert await a.fetch_recent_trades("BTC") and await a.fetch_funding_rates("BTC", NOW, NOW)
    assert (await a.fetch_market_specification_history()).payload
    assert (await a.fetch_interactive_order_book("BTC")).payload
    assert (await a.fetch_market_impact_price("BTC", Decimal("1"))).payload
    assert (await a.fetch_bbo("BTC")).payload and (await a.fetch_liquidations("BTC")).payload
    assert (await a.fetch_system_time()).payload and (await a.fetch_system_state()).payload
    assert (await a.fetch_insurance_fund()).payload
    await c.aclose()


@pytest.mark.asyncio
async def test_lighter_numeric_mapping_and_public_contracts() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.path
        if path == "/orderBooks":
            return httpx.Response(
                200,
                json={
                    "order_books": [
                        {
                            "symbol": "ETH-USDC",
                            "market_id": 7,
                            "quote_symbol": "USDC",
                            "supported_price_decimals": 2,
                            "supported_size_decimals": 3,
                        }
                    ]
                },
            )
        if path == "/orderBookOrders":
            return httpx.Response(
                200,
                json={
                    "nonce": 2,
                    "bids": [{"price": "99", "remaining_base_amount": "1"}],
                    "asks": [{"price": "101", "remaining_base_amount": "1"}],
                },
            )
        if path == "/recentTrades":
            return httpx.Response(
                200,
                json={
                    "trades": [
                        {
                            "timestamp": 1735689600000,
                            "trade_id": "t",
                            "price": "100",
                            "size": "1",
                            "side": "buy",
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"ok": True})

    c = client(handler)
    a = LighterMarketDataAdapter(c)
    mapping = (await a.fetch_instrument_mappings())[0]
    assert mapping.market_id == 7 and await a.fetch_markets()
    assert (await a.fetch_order_book("ETH-USDC")).sequence == 2
    assert await a.fetch_recent_trades("ETH-USDC")
    assert all(req.url.params.get("market_id") in {None, "7"} for req in seen)
    with pytest.raises(ValueError, match="unknown"):
        await a.fetch_order_book("BAD")
    assert (await a.fetch_contract_specifications()).payload
    assert (await a.fetch_market_stats(7)).payload and (await a.fetch_system_status()).payload
    with pytest.raises(CapabilityUnavailableError):
        await a.fetch_liquidations("ETH")
    with pytest.raises(CapabilityUnavailableError):
        await a.fetch_ohlcv("ETH", "1m")
    await c.aclose()
