from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from app.adapters.exchanges.base import CapabilityUnavailableError
from app.adapters.exchanges.public import PublicRestAdapter, _ms
from app.domain.market_data.models import OHLCV, Market, OrderBook, OrderBookLevel, Side, Trade
from app.domain.venues.models import CapabilitySupport, VenueCapabilityMatrix


class GmoCoinMarketDataAdapter(PublicRestAdapter):
    venue = "gmo_coin"
    base_url = "https://api.coin.z.com"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official v1 checked 2026-07-12",
        spot=CapabilitySupport.DOCUMENTED,
        perpetual=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
        post_only=CapabilitySupport.DOCUMENTED,
        reduce_only=CapabilitySupport.DOCUMENTED,
        ioc=CapabilitySupport.DOCUMENTED,
    )

    async def fetch_markets(self) -> Sequence[Market]:
        response = await self.client.get("/public/v1/symbols")
        response.raise_for_status()
        return tuple(
            Market(
                exchange=self.venue,
                symbol=item["symbol"],
                base=item["symbol"].split("_")[0],
                quote="JPY",
                market_type="perpetual" if item.get("optionType") else "spot",
                tick_size=item.get("tickSize"),
            )
            for item in response.json()["data"]
        )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        del start, end, limit
        response = await self.client.get(
            "/public/v1/klines",
            params={
                "symbol": symbol,
                "interval": timeframe,
                "date": datetime.now(UTC).strftime("%Y%m%d"),
            },
        )
        response.raise_for_status()
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=_ms(item["openTime"]),
                open=item["open"],
                high=item["high"],
                low=item["low"],
                close=item["close"],
                volume=item["volume"],
            )
            for item in response.json()["data"]
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        response = await self.client.get("/public/v1/orderbooks", params={"symbol": symbol})
        response.raise_for_status()
        payload = response.json()
        data = payload["data"]
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=datetime.fromisoformat(payload["responsetime"].replace("Z", "+00:00")),
            bids=tuple(
                OrderBookLevel(price=item["price"], quantity=item["size"])
                for item in data["bids"][:depth]
            ),
            asks=tuple(
                OrderBookLevel(price=item["price"], quantity=item["size"])
                for item in data["asks"][:depth]
            ),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        response = await self.client.get(
            "/public/v1/trades", params={"symbol": symbol, "page": 1, "count": min(limit, 100)}
        )
        response.raise_for_status()
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00")),
                trade_id=str(item.get("id", item["timestamp"])),
                price=item["price"],
                quantity=item["size"],
                side=Side(item["side"].lower()),
            )
            for item in response.json()["data"]["list"]
        )


class BitbankMarketDataAdapter(PublicRestAdapter):
    venue = "bitbank"
    base_url = "https://public.bitbank.cc"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official public API checked 2026-07-12",
        spot=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
        post_only=CapabilitySupport.DOCUMENTED,
    )

    async def fetch_markets(self) -> Sequence[Market]:
        return tuple(
            Market(
                exchange=self.venue,
                symbol=f"{base.lower()}_jpy",
                base=base,
                quote="JPY",
                market_type="spot",
            )
            for base in ("BTC", "ETH")
        )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        del end, limit
        intervals = {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "1h": "1hour",
            "4h": "4hour",
            "1d": "1day",
        }
        date = (start or datetime.now(UTC)).strftime("%Y%m%d")
        response = await self.client.get(f"/{symbol}/candlestick/{intervals[timeframe]}/{date}")
        response.raise_for_status()
        rows = response.json()["data"]["candlestick"][0]["ohlcv"]
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=_ms(row[5]),
                open=row[0],
                high=row[1],
                low=row[2],
                close=row[3],
                volume=row[4],
            )
            for row in rows
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        response = await self.client.get(f"/{symbol}/depth")
        response.raise_for_status()
        data = response.json()["data"]
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=_ms(data["timestamp"]),
            bids=tuple(
                OrderBookLevel(price=row[0], quantity=row[1]) for row in data["bids"][:depth]
            ),
            asks=tuple(
                OrderBookLevel(price=row[0], quantity=row[1]) for row in data["asks"][:depth]
            ),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        response = await self.client.get(f"/{symbol}/transactions")
        response.raise_for_status()
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["executed_at"]),
                trade_id=str(item["transaction_id"]),
                price=item["price"],
                quantity=item["amount"],
                side=Side(item["side"]),
            )
            for item in response.json()["data"]["transactions"][:limit]
        )


class BitflyerMarketDataAdapter(PublicRestAdapter):
    venue = "bitflyer"
    base_url = "https://api.bitflyer.com"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official v1 checked 2026-07-12",
        spot=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
    )

    async def fetch_markets(self) -> Sequence[Market]:
        response = await self.client.get("/v1/getmarkets")
        response.raise_for_status()
        return tuple(
            Market(
                exchange=self.venue,
                symbol=item["product_code"],
                base=item["product_code"].split("_")[0],
                quote=item["product_code"].split("_")[-1],
                market_type="spot",
            )
            for item in response.json()
            if "_JPY" in item["product_code"]
        )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        del symbol, timeframe, start, end, limit
        raise CapabilityUnavailableError("bitFlyer official public API does not provide OHLCV")

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        response = await self.client.get("/v1/getboard", params={"product_code": symbol})
        response.raise_for_status()
        data = response.json()
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=datetime.now(UTC),
            bids=tuple(
                OrderBookLevel(price=item["price"], quantity=item["size"])
                for item in data["bids"][:depth]
            ),
            asks=tuple(
                OrderBookLevel(price=item["price"], quantity=item["size"])
                for item in data["asks"][:depth]
            ),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        response = await self.client.get(
            "/v1/getexecutions", params={"product_code": symbol, "count": min(limit, 500)}
        )
        response.raise_for_status()
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=datetime.fromisoformat(item["exec_date"].replace("Z", "+00:00")),
                trade_id=str(item["id"]),
                price=item["price"],
                quantity=item["size"],
                side=Side(item["side"].lower()),
            )
            for item in response.json()
        )
