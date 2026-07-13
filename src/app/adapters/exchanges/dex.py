from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from app.adapters.exchanges.base import CapabilityUnavailableError
from app.adapters.exchanges.public import PublicRestAdapter, _ms
from app.domain.market_data.models import (
    OHLCV,
    FundingRate,
    Market,
    OrderBook,
    OrderBookLevel,
    Side,
    Trade,
)
from app.domain.venues.models import CapabilitySupport, VenueCapabilityMatrix

DYDX_RESOLUTIONS = {
    "1m": "1MIN",
    "5m": "5MINS",
    "15m": "15MINS",
    "30m": "30MINS",
    "1h": "1HOUR",
    "4h": "4HOURS",
    "1d": "1DAY",
}


class DataSourceLayer(StrEnum):
    INDEXER = "indexer"
    FULL_NODE = "full_node"
    WEBSOCKET = "websocket"


@dataclass(frozen=True)
class SourcedData:
    source_layer: DataSourceLayer
    payload: Any


class PermissionedKeyInterface(Protocol):
    """Future execution boundary. Data adapters never invoke this interface."""

    async def public_key(self) -> str: ...
    async def sign(self, payload: bytes) -> bytes: ...


def _matrix(venue: str, **supports: CapabilitySupport) -> VenueCapabilityMatrix:
    payload: dict[str, Any] = {
        "venue": venue,
        "detected_at": datetime(2026, 7, 12, tzinfo=UTC),
        "source_version": "official public API; implementation is not live verification",
        **supports,
    }
    return VenueCapabilityMatrix(**payload)


class DydxMarketDataAdapter(PublicRestAdapter):
    venue = "dydx"
    base_url = "https://indexer.dydx.trade/v4"
    data_enabled, execution_enabled, status = False, False, "EXPERIMENTAL_DATA_ONLY"
    capabilities = _matrix(
        venue,
        perpetual=CapabilitySupport.IMPLEMENTED,
        funding_current=CapabilitySupport.DOCUMENTED,
        funding_history=CapabilitySupport.IMPLEMENTED,
        open_interest=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.IMPLEMENTED,
        orderbook_delta=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.IMPLEMENTED,
        mark_price=CapabilitySupport.DOCUMENTED,
        index_price=CapabilitySupport.DOCUMENTED,
    )

    async def _get(self, path: str, **params: object) -> Any:
        response = await self.client.get(path, params=params or None)  # type: ignore[arg-type]
        response.raise_for_status()
        return response.json()

    async def fetch_markets(self) -> Sequence[Market]:
        data = await self._get("/perpetualMarkets")
        markets = data.get("markets", data)
        return tuple(
            Market(
                exchange=self.venue,
                symbol=symbol,
                base=item.get("baseAsset", symbol.split("-")[0]),
                quote=item.get("quoteAsset", "USD"),
                market_type="perpetual",
                tick_size=item.get("tickSize"),
                lot_size=item.get("stepSize"),
            )
            for symbol, item in markets.items()
        )

    async def fetch_market_metadata(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/perpetualMarkets"))

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
    ) -> Sequence[OHLCV]:
        try:
            resolution = DYDX_RESOLUTIONS[timeframe]
        except KeyError as exc:
            raise ValueError(f"unsupported dYdX timeframe: {timeframe}") from exc
        data = await self._get(
            f"/candles/perpetualMarkets/{symbol}",
            resolution=resolution,
            limit=limit,
            fromISO=start.isoformat() if start else None,
            toISO=end.isoformat() if end else None,
        )
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=datetime.fromisoformat(x["startedAt"].replace("Z", "+00:00")),
                open=x["open"],
                high=x["high"],
                low=x["low"],
                close=x["close"],
                volume=x["baseTokenVolume"],
                closed=x.get("closed", True),
            )
            for x in data.get("candles", [])
        )

    async def fetch_order_book(self, symbol: str, depth: int = 100) -> OrderBook:
        received = datetime.now(UTC)
        data = await self._get(f"/orderbooks/perpetualMarket/{symbol}")
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            exchange_timestamp=None,
            received_at=received,
            available_at=datetime.now(UTC),
            bids=tuple(
                OrderBookLevel(price=x["price"], quantity=x["size"]) for x in data["bids"][:depth]
            ),
            asks=tuple(
                OrderBookLevel(price=x["price"], quantity=x["size"]) for x in data["asks"][:depth]
            ),
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 100) -> Sequence[Trade]:
        data = await self._get(f"/trades/perpetualMarket/{symbol}", limit=limit)
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=datetime.fromisoformat(x["createdAt"].replace("Z", "+00:00")),
                trade_id=str(x["id"]),
                price=x["price"],
                quantity=x["size"],
                side=Side(x["side"].lower()),
            )
            for x in data.get("trades", [])
        )

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        data = await self._get(
            f"/historicalFunding/{symbol}", effectiveBeforeOrAt=end.isoformat() if end else None
        )
        return tuple(
            FundingRate(
                exchange=self.venue,
                symbol=symbol,
                timestamp=datetime.fromisoformat(x["effectiveAt"].replace("Z", "+00:00")),
                rate=x["rate"],
            )
            for x in data.get("historicalFunding", [])
        )

    async def fetch_indexer_time(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/time"))

    async def fetch_indexer_height(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/height"))

    async def screen_address(self, address: str) -> SourcedData:
        if not address.strip():
            raise ValueError("address is required")
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/screen", address=address))

    async def fetch_liquidations(self, symbol: str) -> SourcedData:
        del symbol
        raise CapabilityUnavailableError("dYdX liquidation contract is not verified")

    async def fetch_current_market_state(self, symbol: str) -> SourcedData:
        return SourcedData(
            DataSourceLayer.INDEXER,
            await self._get("/perpetualMarkets", market=symbol),
        )


class ParadexMarketDataAdapter(PublicRestAdapter):
    venue, base_url = "paradex", "https://api.prod.paradex.trade/v1"
    data_enabled, execution_enabled, status = False, False, "EXPERIMENTAL_DATA_ONLY"
    capabilities = _matrix(
        venue,
        perpetual=CapabilitySupport.IMPLEMENTED,
        funding_history=CapabilitySupport.IMPLEMENTED,
        liquidations=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.IMPLEMENTED,
        trades=CapabilitySupport.IMPLEMENTED,
    )

    async def _get(self, path: str, **params: object) -> Any:
        response = await self.client.get(path, params=params or None)  # type: ignore[arg-type]
        response.raise_for_status()
        return response.json()

    async def fetch_markets(self) -> Sequence[Market]:
        data = await self._get("/markets")
        return tuple(
            Market(
                exchange=self.venue,
                symbol=x["symbol"],
                base=x.get("base_currency", x["symbol"].split("-")[0]),
                quote=x.get("quote_currency", "USD"),
                market_type=x.get("asset_kind", "perpetual").lower(),
                tick_size=x.get("price_tick_size"),
                lot_size=x.get("order_size_increment"),
            )
            for x in data.get("results", data.get("markets", []))
        )

    async def fetch_market_specification_history(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/markets/history"))

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        del limit
        if start is None or end is None:
            raise ValueError("Paradex OHLCV requires start and end")
        data = await self._get(
            "/markets/klines",
            symbol=symbol,
            resolution=timeframe,
            start_at=int(start.timestamp() * 1000),
            end_at=int(end.timestamp() * 1000),
        )
        return tuple(
            OHLCV(
                exchange=self.venue,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=_ms(x[0]),
                open=x[1],
                high=x[2],
                low=x[3],
                close=x[4],
                volume=x[5],
            )
            for x in data.get("results", [])
        )

    async def fetch_order_book(self, symbol: str, depth: int = 50) -> OrderBook:
        received = datetime.now(UTC)
        data = await self._get(f"/orderbook/{symbol}", depth=depth)
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            exchange_timestamp=_ms(data["last_updated_at"])
            if data.get("last_updated_at")
            else None,
            received_at=received,
            available_at=datetime.now(UTC),
            sequence=data.get("seq_no"),
            bids=tuple(OrderBookLevel(price=x[0], quantity=x[1]) for x in data["bids"]),
            asks=tuple(OrderBookLevel(price=x[0], quantity=x[1]) for x in data["asks"]),
        )

    async def fetch_interactive_order_book(self, symbol: str, depth: int = 20) -> SourcedData:
        return SourcedData(
            DataSourceLayer.INDEXER,
            await self._get(f"/orderbook/{symbol}/interactive", depth=depth),
        )

    async def fetch_market_impact_price(self, symbol: str, size: Decimal) -> SourcedData:
        return SourcedData(
            DataSourceLayer.INDEXER,
            await self._get(f"/orderbook/{symbol}/impact-price", size=str(size)),
        )

    async def fetch_bbo(self, symbol: str) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get(f"/bbo/{symbol}"))

    async def fetch_recent_trades(self, symbol: str, limit: int = 100) -> Sequence[Trade]:
        data = await self._get("/trades", market=symbol, page_size=limit)
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["created_at"]),
                trade_id=str(item["id"]),
                price=item["price"],
                quantity=item["size"],
                side=Side(item["side"].lower()),
            )
            for item in data.get("results", [])
        )

    async def fetch_funding_rates(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> Sequence[FundingRate]:
        data = await self._get(
            "/funding/data",
            market=symbol,
            start_at=int(start.timestamp() * 1000) if start else None,
            end_at=int(end.timestamp() * 1000) if end else None,
        )
        return tuple(
            FundingRate(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["created_at"]),
                rate=item["funding_rate"],
            )
            for item in data.get("results", [])
        )

    async def fetch_liquidations(self, symbol: str) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/liquidations", market=symbol))

    async def fetch_system_time(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/system/time"))

    async def fetch_system_state(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/system/state"))

    async def fetch_insurance_fund(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/insurance-fund"))


class LighterMarketDataAdapter(PublicRestAdapter):
    venue, base_url = "lighter", "https://mainnet.zklighter.elliot.ai/api/v1"
    data_enabled, execution_enabled, status = False, False, "EXPERIMENTAL_DATA_ONLY"
    capabilities = _matrix(
        venue,
        perpetual=CapabilitySupport.IMPLEMENTED,
        funding_current=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.IMPLEMENTED,
        orderbook_delta=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.IMPLEMENTED,
        mark_price=CapabilitySupport.DOCUMENTED,
        index_price=CapabilitySupport.DOCUMENTED,
    )

    async def _get(self, path: str, **params: object) -> Any:
        response = await self.client.get(path, params=params or None)  # type: ignore[arg-type]
        response.raise_for_status()
        return response.json()

    async def fetch_instrument_mappings(self) -> Sequence[LighterInstrumentMapping]:
        data = await self._get("/orderBooks")
        return tuple(
            LighterInstrumentMapping(
                venue_symbol=x["symbol"],
                market_id=int(x.get("market_id", x.get("market_index"))),
                base=x.get("symbol", "").split("-")[0],
                quote=x.get("quote_symbol", "USDC"),
                tick_size=Decimal(1).scaleb(-int(x["supported_price_decimals"])),
                lot_size=Decimal(1).scaleb(-int(x["supported_size_decimals"])),
            )
            for x in data.get("order_books", [])
        )

    async def fetch_markets(self) -> Sequence[Market]:
        return tuple(
            Market(
                exchange=self.venue,
                symbol=item.venue_symbol,
                base=item.base,
                quote=item.quote,
                market_type="perpetual",
                tick_size=item.tick_size,
                lot_size=item.lot_size,
            )
            for item in await self.fetch_instrument_mappings()
        )

    async def _mapping(self, symbol: str) -> LighterInstrumentMapping:
        try:
            return next(
                item
                for item in await self.fetch_instrument_mappings()
                if item.venue_symbol == symbol
            )
        except StopIteration as exc:
            raise ValueError(f"unknown Lighter venue symbol: {symbol}") from exc

    async def fetch_contract_specifications(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/orderBooks"))

    async def fetch_order_book(self, symbol: str, depth: int = 100) -> OrderBook:
        received = datetime.now(UTC)
        mapping = await self._mapping(symbol)
        data = await self._get("/orderBookOrders", market_id=mapping.market_id, limit=depth)
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            exchange_timestamp=None,
            received_at=received,
            available_at=datetime.now(UTC),
            sequence=data.get("nonce"),
            bids=tuple(
                OrderBookLevel(price=x["price"], quantity=x["remaining_base_amount"])
                for x in data["bids"]
            ),
            asks=tuple(
                OrderBookLevel(price=x["price"], quantity=x["remaining_base_amount"])
                for x in data["asks"]
            ),
        )

    async def fetch_liquidations(self, symbol: str) -> Sequence[Any]:
        del symbol
        raise CapabilityUnavailableError(
            "Lighter public API does not currently expose liquidations"
        )

    async def fetch_market_stats(self, market_id: int) -> SourcedData:
        return SourcedData(
            DataSourceLayer.INDEXER, await self._get("/marketStats", market_id=market_id)
        )

    async def fetch_recent_trades(self, symbol: str, limit: int = 100) -> Sequence[Trade]:
        mapping = await self._mapping(symbol)
        data = await self._get("/recentTrades", market_id=mapping.market_id, limit=limit)
        return tuple(
            Trade(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(item["timestamp"]),
                trade_id=str(item.get("trade_id", item["timestamp"])),
                price=item["price"],
                quantity=item["size"],
                side=Side(item["side"].lower()),
            )
            for item in data.get("trades", [])
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
        raise CapabilityUnavailableError("Lighter public API does not expose OHLCV")

    async def fetch_system_status(self) -> SourcedData:
        return SourcedData(DataSourceLayer.INDEXER, await self._get("/info"))


@dataclass(frozen=True)
class LighterInstrumentMapping:
    venue_symbol: str
    market_id: int
    base: str
    quote: str
    tick_size: Decimal
    lot_size: Decimal


@dataclass(frozen=True)
class LighterHealthObservation:
    observed_at: datetime
    api_uptime: float
    websocket_disconnect_rate: float
    book_update_frequency: float
    displayed_depth_stability: float
    funding_discontinuity: bool
    withdrawal_deposit_status: str
    chain_prover_status: str
