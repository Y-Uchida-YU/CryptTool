from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import aclosing
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import websockets

from app.adapters.exchanges.base import CapabilityUnavailableError, MarketDataAdapter
from app.domain.market_data.models import (
    OHLCV,
    FundingRate,
    Market,
    OpenInterest,
    OrderBook,
    OrderBookLevel,
    Side,
    Trade,
)
from app.domain.venues.models import VenueCapabilityMatrix
from app.services.whale_analytics import WalletSnapshot


def _ms(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


async def _websocket_json(
    url: str, subscribe: dict[str, object] | None = None
) -> AsyncGenerator[dict[str, Any], None]:
    delay = 1.0
    while True:
        try:
            async with websockets.connect(url, open_timeout=10, ping_interval=20) as socket:
                if subscribe is not None:
                    await socket.send(json.dumps(subscribe))
                delay = 1.0
                async for raw in socket:
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    if raw == "pong":
                        continue
                    yield json.loads(raw)
        except (OSError, TimeoutError, websockets.WebSocketException, json.JSONDecodeError):
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


class PublicRestAdapter(MarketDataAdapter):
    venue: str
    base_url: str
    capabilities: VenueCapabilityMatrix

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._owned_client = client is None
        self.client = client or httpx.AsyncClient(base_url=self.base_url, timeout=10)

    async def close(self) -> None:
        if self._owned_client:
            await self.client.aclose()

    async def health_check(self) -> bool:
        try:
            await self.fetch_markets()
        except (httpx.HTTPError, ValueError, KeyError):
            return False
        return True

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        del symbol, start, end
        raise CapabilityUnavailableError(f"{self.venue} funding history is unavailable")

    async def fetch_open_interest(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[OpenInterest]:
        del symbol, start, end
        raise CapabilityUnavailableError(f"{self.venue} open interest is unavailable")

    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        del symbol
        raise CapabilityUnavailableError(
            f"{self.venue} WebSocket stream requires deployment reconnect composition"
        )

    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        del symbol
        raise CapabilityUnavailableError(
            f"{self.venue} WebSocket stream requires deployment reconnect composition"
        )

    def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        del symbol
        raise CapabilityUnavailableError(
            f"{self.venue} WebSocket stream requires deployment reconnect composition"
        )


class BinanceCompatiblePerpAdapter(PublicRestAdapter):
    async def fetch_server_time(self) -> datetime:
        response = await self.client.get("/fapi/v1/time")
        response.raise_for_status()
        return _ms(response.json()["serverTime"])

    async def fetch_markets(self) -> Sequence[Market]:
        response = await self.client.get("/fapi/v1/exchangeInfo")
        response.raise_for_status()
        result: list[Market] = []
        for item in response.json()["symbols"]:
            filters = {entry["filterType"]: entry for entry in item.get("filters", [])}
            result.append(
                Market(
                    exchange=self.venue,
                    symbol=item["symbol"],
                    base=item["baseAsset"],
                    quote=item["quoteAsset"],
                    market_type="perpetual"
                    if item.get("contractType") == "PERPETUAL"
                    else "dated_future",
                    tick_size=_decimal(filters.get("PRICE_FILTER", {}).get("tickSize", 0)) or None,
                    lot_size=_decimal(filters.get("LOT_SIZE", {}).get("stepSize", 0)) or None,
                    minimum_notional=_decimal(filters.get("MIN_NOTIONAL", {}).get("notional", 0))
                    or None,
                )
            )
        return result

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        params: dict[str, str | int] = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit,
        }
        if start:
            params["startTime"] = int(start.timestamp() * 1000)
        if end:
            params["endTime"] = int(end.timestamp() * 1000)
        response = await self.client.get("/fapi/v1/klines", params=params)
        response.raise_for_status()
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=_ms(row[0]),
                open=_decimal(row[1]),
                high=_decimal(row[2]),
                low=_decimal(row[3]),
                close=_decimal(row[4]),
                volume=_decimal(row[5]),
                closed=True,
            )
            for row in response.json()
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        response = await self.client.get(
            "/fapi/v1/depth", params={"symbol": symbol, "limit": depth}
        )
        response.raise_for_status()
        data = response.json()
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=datetime.now(UTC),
            sequence=data.get("lastUpdateId"),
            bids=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in data["bids"]),
            asks=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in data["asks"]),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        response = await self.client.get(
            "/fapi/v1/trades", params={"symbol": symbol, "limit": limit}
        )
        response.raise_for_status()
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["time"]),
                trade_id=str(item["id"]),
                price=item["price"],
                quantity=item["qty"],
                side=Side.SELL if item.get("isBuyerMaker") else Side.BUY,
            )
            for item in response.json()
        )

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        params: dict[str, str | int] = {"symbol": symbol, "limit": 1000}
        if start:
            params["startTime"] = int(start.timestamp() * 1000)
        if end:
            params["endTime"] = int(end.timestamp() * 1000)
        response = await self.client.get("/fapi/v1/fundingRate", params=params)
        response.raise_for_status()
        return tuple(
            FundingRate(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["fundingTime"]),
                rate=item["fundingRate"],
            )
            for item in response.json()
        )

    async def fetch_open_interest(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[OpenInterest]:
        del start, end
        response = await self.client.get("/fapi/v1/openInterest", params={"symbol": symbol})
        response.raise_for_status()
        item = response.json()
        return (
            OpenInterest(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["time"]),
                value=item["openInterest"],
                unit="base",
            ),
        )


class AsterMarketDataAdapter(BinanceCompatiblePerpAdapter):
    venue = "aster"
    base_url = "https://fapi.asterdex.com"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official API checked 2026-07-12",
        perpetual=True,
        funding_current=True,
        funding_history=True,
        open_interest=True,
        orderbook_snapshot=True,
        orderbook_delta=True,
        trades=True,
        mark_price=True,
        index_price=True,
        wallet_positions=True,
        private_websocket=True,
        post_only=True,
        reduce_only=True,
        ioc=True,
        fok=True,
        batch_orders=True,
    )

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@depth20@100ms"
        async with aclosing(_websocket_json(url)) as messages:
            async for item in messages:
                yield OrderBook(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["E"]),
                    sequence=item["u"],
                    bids=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in item["b"]),
                    asks=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in item["a"]),
                )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@aggTrade"
        async with aclosing(_websocket_json(url)) as messages:
            async for item in messages:
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["T"]),
                    trade_id=str(item["a"]),
                    price=item["p"],
                    quantity=item["q"],
                    side=Side.SELL if item["m"] else Side.BUY,
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@bookTicker"
        async with aclosing(_websocket_json(url)) as messages:
            async for item in messages:
                yield item


class HyperliquidMarketDataAdapter(PublicRestAdapter):
    venue = "hyperliquid"
    base_url = "https://api.hyperliquid.xyz"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official docs checked 2026-07-12",
        spot=True,
        perpetual=True,
        funding_current=True,
        funding_history=True,
        predicted_funding=True,
        open_interest=True,
        liquidations=True,
        orderbook_snapshot=True,
        orderbook_delta=True,
        trades=True,
        mark_price=True,
        index_price=True,
        wallet_positions=True,
        wallet_transfers=True,
        private_websocket=True,
        post_only=True,
        reduce_only=True,
        ioc=True,
        batch_orders=True,
        subaccounts=True,
    )

    async def _info(self, payload: dict[str, object]) -> Any:
        response = await self.client.post("/info", json=payload)
        response.raise_for_status()
        return response.json()

    async def fetch_server_time(self, symbol: str = "BTC") -> datetime:
        data = await self._info({"type": "l2Book", "coin": symbol})
        return _ms(data["time"])

    async def fetch_markets(self) -> Sequence[Market]:
        data = await self._info({"type": "meta"})
        return tuple(
            Market(
                exchange=self.venue,
                symbol=item["name"],
                base=item["name"],
                quote="USDC",
                market_type="perpetual",
                lot_size=Decimal(1).scaleb(-int(item["szDecimals"])),
            )
            for item in data["universe"]
        )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        del limit
        now = datetime.now(UTC)
        data = await self._info(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": timeframe,
                    "startTime": int((start or now.replace(hour=0)).timestamp() * 1000),
                    "endTime": int((end or now).timestamp() * 1000),
                },
            }
        )
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=_ms(item["t"]),
                open=item["o"],
                high=item["h"],
                low=item["l"],
                close=item["c"],
                volume=item["v"],
            )
            for item in data
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        data = await self._info({"type": "l2Book", "coin": symbol})
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=_ms(data["time"]),
            bids=tuple(
                OrderBookLevel(price=item["px"], quantity=item["sz"])
                for item in data["levels"][0][:depth]
            ),
            asks=tuple(
                OrderBookLevel(price=item["px"], quantity=item["sz"])
                for item in data["levels"][1][:depth]
            ),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        data = await self._info({"type": "recentTrades", "coin": symbol})
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["time"]),
                trade_id=str(item["hash"]),
                price=item["px"],
                quantity=item["sz"],
                side=Side.BUY if item["side"] == "B" else Side.SELL,
            )
            for item in data[:limit]
        )

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        now = datetime.now(UTC)
        payload: dict[str, object] = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": int((start or now.replace(hour=0)).timestamp() * 1000),
        }
        if end:
            payload["endTime"] = int(end.timestamp() * 1000)
        data = await self._info(payload)
        return tuple(
            FundingRate(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["time"]),
                rate=item["fundingRate"],
            )
            for item in data
        )

    async def fetch_open_interest(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[OpenInterest]:
        del start, end
        meta, contexts = await self._info({"type": "metaAndAssetCtxs"})
        index = next(i for i, item in enumerate(meta["universe"]) if item["name"] == symbol)
        item = contexts[index]
        return (
            OpenInterest(
                exchange=self.venue,
                symbol=symbol,
                timestamp=datetime.now(UTC),
                value=item["openInterest"],
                unit="base",
            ),
        )

    async def fetch_wallet_snapshot(self, wallet: str, symbol: str) -> WalletSnapshot:
        """Read public Hyperliquid account state; this method never submits or signs an action."""
        state = await self._info({"type": "clearinghouseState", "user": wallet})
        positions = [item["position"] for item in state.get("assetPositions", [])]
        position = next((item for item in positions if item["coin"] == symbol), None)
        if position is None:
            raise ValueError(f"wallet has no {symbol} position")
        fills = await self._info({"type": "userFills", "user": wallet})
        ledger = await self._info({"type": "userNonFundingLedgerUpdates", "user": wallet})
        mids = await self._info({"type": "allMids"})
        realized = sum(
            (_decimal(item.get("closedPnl", 0)) for item in fills if item.get("coin") == symbol),
            Decimal("0"),
        )
        deposits = sum(
            (
                abs(_decimal(item["delta"].get("usdc", 0)))
                for item in ledger
                if item.get("delta", {}).get("type") == "deposit"
            ),
            Decimal("0"),
        )
        withdrawals = sum(
            (
                abs(_decimal(item["delta"].get("usdc", 0)))
                for item in ledger
                if item.get("delta", {}).get("type") == "withdraw"
            ),
            Decimal("0"),
        )
        leverage = position.get("leverage", {})
        return WalletSnapshot(
            venue=self.venue,
            wallet=wallet,
            symbol=symbol,
            observed_at=datetime.now(UTC),
            position=position["szi"],
            realized_pnl=realized,
            unrealized_pnl=position["unrealizedPnl"],
            leverage=leverage.get("value", 0),
            liquidation_price=position.get("liquidationPx"),
            mark_price=mids[symbol],
            account_equity=state["marginSummary"]["accountValue"],
            cumulative_deposits=deposits,
            cumulative_withdrawals=withdrawals,
        )

    async def _stream(self, subscription: dict[str, object]) -> AsyncIterator[dict[str, Any]]:
        request: dict[str, object] = {"method": "subscribe", "subscription": subscription}
        async with aclosing(_websocket_json("wss://api.hyperliquid.xyz/ws", request)) as messages:
            async for message in messages:
                if message.get("channel") != "subscriptionResponse":
                    yield message

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        async for message in self._stream({"type": "l2Book", "coin": symbol}):
            data = message["data"]
            yield OrderBook(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(data["time"]),
                bids=tuple(
                    OrderBookLevel(price=item["px"], quantity=item["sz"])
                    for item in data["levels"][0]
                ),
                asks=tuple(
                    OrderBookLevel(price=item["px"], quantity=item["sz"])
                    for item in data["levels"][1]
                ),
            )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        async for message in self._stream({"type": "trades", "coin": symbol}):
            for item in message["data"]:
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["time"]),
                    trade_id=str(item["hash"]),
                    price=item["px"],
                    quantity=item["sz"],
                    side=Side.BUY if item["side"] == "B" else Side.SELL,
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        async for message in self._stream({"type": "allMids"}):
            mids = message["data"]["mids"]
            if symbol in mids:
                yield {"symbol": symbol, "mid": mids[symbol], "timestamp": datetime.now(UTC)}


class BitgetMarketDataAdapter(PublicRestAdapter):
    venue = "bitget"
    base_url = "https://api.bitget.com"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official v2 API checked 2026-07-12",
        spot=True,
        perpetual=True,
        dated_futures=True,
        funding_current=True,
        funding_history=True,
        open_interest=True,
        liquidations=True,
        orderbook_snapshot=True,
        orderbook_delta=True,
        trades=True,
        mark_price=True,
        index_price=True,
        long_short_ratio=True,
        wallet_positions=True,
        private_websocket=True,
        post_only=True,
        reduce_only=True,
        ioc=True,
        fok=True,
        batch_orders=True,
        subaccounts=True,
    )

    async def fetch_server_time(self) -> datetime:
        response = await self.client.get("/api/v2/public/time")
        response.raise_for_status()
        return _ms(response.json()["data"]["serverTime"])

    async def fetch_markets(self) -> Sequence[Market]:
        response = await self.client.get(
            "/api/v2/mix/market/contracts", params={"productType": "USDT-FUTURES"}
        )
        response.raise_for_status()
        return tuple(
            Market(
                exchange=self.venue,
                symbol=item["symbol"],
                base=item["baseCoin"],
                quote=item["quoteCoin"],
                market_type="perpetual" if not item.get("deliveryTime") else "dated_future",
                tick_size=Decimal(1).scaleb(-int(item["pricePlace"])),
                lot_size=Decimal(1).scaleb(-int(item["volumePlace"])),
                minimum_notional=_decimal(item.get("minTradeUSDT", 0)) or None,
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
        params: dict[str, str | int] = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "granularity": timeframe,
            "limit": limit,
        }
        if start:
            params["startTime"] = int(start.timestamp() * 1000)
        if end:
            params["endTime"] = int(end.timestamp() * 1000)
        response = await self.client.get("/api/v2/mix/market/candles", params=params)
        response.raise_for_status()
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=_ms(row[0]),
                open=row[1],
                high=row[2],
                low=row[3],
                close=row[4],
                volume=row[5],
            )
            for row in response.json()["data"]
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        response = await self.client.get(
            "/api/v2/mix/market/merge-depth",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "limit": depth},
        )
        response.raise_for_status()
        data = response.json()["data"]
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=_ms(data["ts"]),
            sequence=int(data.get("checksum", 0)) or None,
            bids=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in data["bids"]),
            asks=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in data["asks"]),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        response = await self.client.get(
            "/api/v2/mix/market/fills",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "limit": limit},
        )
        response.raise_for_status()
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["ts"]),
                trade_id=str(item["tradeId"]),
                price=item["price"],
                quantity=item["size"],
                side=Side(item["side"]),
            )
            for item in response.json()["data"]
        )

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        del start, end
        response = await self.client.get(
            "/api/v2/mix/market/history-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "pageSize": 100},
        )
        response.raise_for_status()
        return tuple(
            FundingRate(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["fundingTime"]),
                rate=item["fundingRate"],
            )
            for item in response.json()["data"]
        )

    async def fetch_open_interest(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[OpenInterest]:
        del start, end
        response = await self.client.get(
            "/api/v2/mix/market/open-interest",
            params={"symbol": symbol, "productType": "USDT-FUTURES"},
        )
        response.raise_for_status()
        data = response.json()["data"]
        return tuple(
            OpenInterest(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(data["ts"]),
                value=item["size"],
                unit="base",
            )
            for item in data["openInterestList"]
        )

    async def _stream(self, channel: str, symbol: str) -> AsyncIterator[dict[str, Any]]:
        request: dict[str, object] = {
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": channel, "instId": symbol}],
        }
        async with aclosing(
            _websocket_json("wss://ws.bitget.com/v2/ws/public", request)
        ) as messages:
            async for message in messages:
                if "data" in message:
                    yield message

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        async for message in self._stream("books", symbol):
            for data in message["data"]:
                yield OrderBook(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(data["ts"]),
                    sequence=int(data.get("seq", 0)) or None,
                    bids=tuple(
                        OrderBookLevel(price=row[0], quantity=row[1]) for row in data["bids"]
                    ),
                    asks=tuple(
                        OrderBookLevel(price=row[0], quantity=row[1]) for row in data["asks"]
                    ),
                )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        async for message in self._stream("trade", symbol):
            for item in message["data"]:
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["ts"]),
                    trade_id=str(item.get("tradeId", item["ts"])),
                    price=item["price"],
                    quantity=item["size"],
                    side=Side(item["side"]),
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        async for message in self._stream("ticker", symbol):
            for item in message["data"]:
                yield item


class MexcMarketDataAdapter(PublicRestAdapter):
    venue = "mexc"
    base_url = "https://contract.mexc.com"
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official contract v1 checked 2026-07-12",
        perpetual=True,
        funding_current=True,
        funding_history=True,
        predicted_funding=True,
        open_interest=False,
        liquidations=True,
        orderbook_snapshot=True,
        orderbook_delta=True,
        trades=True,
        mark_price=True,
        index_price=True,
        wallet_positions=True,
        private_websocket=True,
        post_only=True,
        reduce_only=True,
        ioc=True,
        fok=True,
        batch_orders=True,
    )

    async def fetch_server_time(self) -> datetime:
        response = await self.client.get("/api/v1/contract/ping")
        response.raise_for_status()
        return _ms(response.json()["data"])

    async def fetch_markets(self) -> Sequence[Market]:
        response = await self.client.get("/api/v1/contract/detail")
        response.raise_for_status()
        return tuple(
            Market(
                exchange=self.venue,
                symbol=item["symbol"],
                base=item["baseCoin"],
                quote=item["quoteCoin"],
                market_type="perpetual",
                tick_size=item.get("priceUnit"),
                lot_size=item.get("volUnit"),
            )
            for item in response.json()["data"]
            if item.get("apiAllowed", True)
        )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        del limit
        intervals = {
            "1m": "Min1",
            "5m": "Min5",
            "15m": "Min15",
            "1h": "Min60",
            "4h": "Hour4",
            "1d": "Day1",
        }
        if timeframe not in intervals:
            raise ValueError(f"unsupported MEXC timeframe: {timeframe}")
        params: dict[str, str | int] = {"interval": intervals[timeframe]}
        if start:
            params["start"] = int(start.timestamp())
        if end:
            params["end"] = int(end.timestamp())
        response = await self.client.get(f"/api/v1/contract/kline/{symbol}", params=params)
        response.raise_for_status()
        data = response.json()["data"]
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=datetime.fromtimestamp(int(ts), tz=UTC),
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
            for ts, open_price, high, low, close, volume in zip(
                data["time"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["vol"],
                strict=True,
            )
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        response = await self.client.get(
            f"/api/v1/contract/depth/{symbol}", params={"limit": depth}
        )
        response.raise_for_status()
        data = response.json()["data"]
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            timestamp=_ms(data["timestamp"]),
            sequence=data.get("version"),
            bids=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in data["bids"]),
            asks=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in data["asks"]),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 1000) -> Sequence[Trade]:
        response = await self.client.get(
            f"/api/v1/contract/deals/{symbol}", params={"limit": limit}
        )
        response.raise_for_status()
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["t"]),
                trade_id=str(item.get("id", item["t"])),
                price=item["p"],
                quantity=item["v"],
                side=Side.BUY if int(item["T"]) == 1 else Side.SELL,
            )
            for item in response.json()["data"]
        )

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        del start, end
        response = await self.client.get(f"/api/v1/contract/funding_rate/{symbol}")
        response.raise_for_status()
        item = response.json()["data"]
        return (
            FundingRate(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["timestamp"]),
                rate=item["fundingRate"],
                next_funding_at=_ms(item["nextSettleTime"]),
            ),
        )

    async def fetch_open_interest(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[OpenInterest]:
        del symbol, start, end
        raise CapabilityUnavailableError(
            "MEXC contract OI returned 403 from the operator environment on 2026-07-12; "
            "location evasion is forbidden"
        )

    async def _stream(self, method: str, symbol: str) -> AsyncIterator[dict[str, Any]]:
        request: dict[str, object] = {"method": method, "param": {"symbol": symbol}}
        async with aclosing(_websocket_json("wss://contract.mexc.com/edge", request)) as messages:
            async for message in messages:
                channel = str(message.get("channel", ""))
                if channel != "pong" and not channel.startswith("rs.sub"):
                    yield message

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        async for message in self._stream("sub.depth", symbol):
            data = message["data"]
            yield OrderBook(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(message.get("ts", data.get("timestamp"))),
                sequence=data.get("version"),
                bids=tuple(
                    OrderBookLevel(price=item[0], quantity=item[1])
                    for item in data["bids"]
                    if item[1] > 0
                ),
                asks=tuple(
                    OrderBookLevel(price=item[0], quantity=item[1])
                    for item in data["asks"]
                    if item[1] > 0
                ),
            )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        async for message in self._stream("sub.deal", symbol):
            for item in message["data"]:
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["t"]),
                    trade_id=str(item.get("id", item["t"])),
                    price=item["p"],
                    quantity=item["v"],
                    side=Side.BUY if int(item["T"]) == 1 else Side.SELL,
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        async for message in self._stream("sub.ticker", symbol):
            yield message["data"]
