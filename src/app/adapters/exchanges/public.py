from __future__ import annotations

from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Sequence
from contextlib import aclosing
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx

from app.adapters.exchanges.base import CapabilityUnavailableError, MarketDataAdapter
from app.adapters.exchanges.websocket import (
    OrderBookStreamSemantics,
    ResilientWebSocketSession,
    StreamClassification,
    WebSocketConnectionLifecycle,
)
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
from app.domain.venues.models import (
    CapabilitySupport,
    VenueCapability,
    VenueCapabilityMatrix,
)
from app.services.whale_analytics import WalletSnapshot


def _ms(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _valid_exchange_ms(value: object, *, future_tolerance_seconds: int = 60) -> bool:
    try:
        timestamp = _ms(int(str(value)))
    except (TypeError, ValueError, OverflowError):
        return False
    now = datetime.now(UTC)
    return (
        datetime(2009, 1, 1, tzinfo=UTC)
        <= timestamp
        <= now + timedelta(seconds=future_tolerance_seconds)
    )


async def _websocket_json(
    url: str,
    subscribe: dict[str, object] | None = None,
    *,
    venue: str = "unknown",
    classification: StreamClassification = StreamClassification.EVENTS,
    order_book_semantics: OrderBookStreamSemantics | None = None,
    instrument: str = "SYSTEM",
    sequence: Callable[[Any], int | None] = lambda value: None,
    sequence_predecessor: Callable[[Any], int | None] = lambda value: None,
    snapshot: Callable[[Any], bool] = lambda value: False,
    delta: Callable[[Any], bool] = lambda value: False,
    validate_snapshot: Callable[[Any], bool] = lambda value: True,
    snapshot_applier: Callable[[Any], int | None] | None = None,
    delta_applier: Callable[[Any], None] | None = None,
    exchange_timestamp: Callable[[Any], datetime | None] = lambda value: None,
    lifecycle_sink: Callable[[WebSocketConnectionLifecycle], None] | None = None,
    application_heartbeat: str | dict[str, object] | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    def acknowledged(message: Any) -> bool:
        return isinstance(message, dict) and (
            message.get("channel") == "subscriptionResponse"
            or message.get("event") == "subscribe"
            or message.get("result") is not None
            or str(message.get("type", "")).startswith("subscribed")
        )

    session = ResilientWebSocketSession(
        venue=venue,
        url=url,
        subscription_id=str(subscribe or url),
        subscribe=subscribe,
        acknowledgement=acknowledged if subscribe is not None else None,
        classification=classification,
        order_book_semantics=order_book_semantics,
        instrument=instrument,
        sequence=sequence,
        sequence_predecessor=sequence_predecessor,
        snapshot=snapshot,
        delta=delta,
        validate_snapshot=validate_snapshot,
        snapshot_applier=snapshot_applier,
        delta_applier=delta_applier,
        exchange_timestamp=exchange_timestamp,
        connection_lifecycle_sink=lifecycle_sink,
        application_heartbeat=application_heartbeat,
        heartbeat=lambda value: (
            (isinstance(value, str) and value in {"pong", "ping"})
            or (isinstance(value, dict) and value.get("channel") == "pong")
        ),
    )
    try:
        async for message in session.messages():
            if isinstance(message.normalized_payload, dict):
                payload = dict(message.normalized_payload)
                raw_payload = message.payload.decode("utf-8", errors="replace")
                payload["_collector_source"] = {
                    "raw_payload": raw_payload,
                    "payload_sha256": message.payload_sha256,
                }
                payload["_collector_reconciliation"] = {
                    "connection_id": str(message.connection_id),
                    "connection_epoch": message.connection_epoch,
                    "snapshot_sequence": message.snapshot_sequence,
                    "delta_sequence": message.venue_sequence,
                    "previous_delta_sequence": message.previous_venue_sequence,
                    "reconciliation_state": (
                        message.reconciliation_state.value
                        if message.reconciliation_state is not None
                        else None
                    ),
                    "stream_semantics": (
                        message.stream_semantics.value if message.stream_semantics else None
                    ),
                    "bootstrap_completed": message.bootstrap_completed,
                    "recovery_started_at": (
                        message.recovery_started_at.isoformat()
                        if message.recovery_started_at
                        else None
                    ),
                    "recovery_completed_at": (
                        message.recovery_completed_at.isoformat()
                        if message.recovery_completed_at
                        else None
                    ),
                    "last_recovery_failure": message.last_recovery_failure,
                }
                yield payload
    finally:
        await session.close()


def _orderbook_reconciliation(message: dict[str, Any]) -> dict[str, Any]:
    metadata = message.get("_collector_reconciliation", {})
    return {
        "connection_id": metadata.get("connection_id"),
        "connection_epoch": int(metadata.get("connection_epoch", 0)),
        "snapshot_sequence": metadata.get("snapshot_sequence"),
        "delta_sequence": metadata.get("delta_sequence"),
        "previous_delta_sequence": metadata.get("previous_delta_sequence"),
        "reconciliation_state": metadata.get("reconciliation_state"),
        "stream_semantics": metadata.get("stream_semantics"),
        "bootstrap_completed": bool(metadata.get("bootstrap_completed", False)),
        "recovery_started_at": metadata.get("recovery_started_at"),
        "recovery_completed_at": metadata.get("recovery_completed_at"),
        "last_recovery_failure": metadata.get("last_recovery_failure"),
        **_source_provenance(message),
    }


def _source_provenance(message: dict[str, Any]) -> dict[str, Any]:
    metadata = message.get("_collector_source", {})
    return {
        "source_raw_payload": metadata.get("raw_payload"),
        "source_payload_sha256": metadata.get("payload_sha256"),
    }


class PublicRestAdapter(MarketDataAdapter):
    venue: str
    base_url: str
    capabilities: VenueCapabilityMatrix

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._owned_client = client is None
        self.client = client or httpx.AsyncClient(base_url=self.base_url, timeout=10)
        self.connection_lifecycle_events: deque[WebSocketConnectionLifecycle] = deque()

    def _record_connection_lifecycle(self, event: WebSocketConnectionLifecycle) -> None:
        self.connection_lifecycle_events.append(event)

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

    async def fetch_current_funding_rate(self, symbol: str) -> FundingRate:
        values = await self.fetch_funding_rates(symbol)
        if not values:
            raise CapabilityUnavailableError(f"{self.venue} current funding is unavailable")
        return values[-1]

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
        received = datetime.now(UTC)
        return OrderBook(
            exchange=self.venue,
            symbol=symbol,
            exchange_timestamp=None,
            received_at=received,
            available_at=datetime.now(UTC),
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
    order_book_stream_semantics = OrderBookStreamSemantics.LIMITED_DEPTH_SNAPSHOT
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official API checked 2026-07-12",
        perpetual=CapabilitySupport.DOCUMENTED,
        funding_current=CapabilitySupport.DOCUMENTED,
        funding_history=CapabilitySupport.DOCUMENTED,
        open_interest=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.DOCUMENTED,
        orderbook_delta=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        mark_price=CapabilitySupport.DOCUMENTED,
        index_price=CapabilitySupport.DOCUMENTED,
        wallet_positions=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
        post_only=CapabilitySupport.DOCUMENTED,
        reduce_only=CapabilitySupport.DOCUMENTED,
        ioc=CapabilitySupport.DOCUMENTED,
        fok=CapabilitySupport.DOCUMENTED,
        batch_orders=CapabilitySupport.DOCUMENTED,
    )

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@depth20@100ms"
        async with aclosing(
            _websocket_json(
                url,
                venue=self.venue,
                classification=StreamClassification.LIMITED_DEPTH_SNAPSHOT_STREAM,
                order_book_semantics=self.order_book_stream_semantics,
                instrument=symbol,
                snapshot=lambda value: (
                    isinstance(value, dict) and {"E", "s", "b", "a"}.issubset(value)
                ),
                validate_snapshot=lambda value: (
                    value.get("s") == symbol
                    and _valid_exchange_ms(value.get("E"))
                    and bool(value.get("b"))
                    and bool(value.get("a"))
                ),
                exchange_timestamp=lambda value: _ms(value["E"]),
                lifecycle_sink=self._record_connection_lifecycle,
            )
        ) as messages:
            async for item in messages:
                yield OrderBook(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["E"]),
                    sequence=item["u"],
                    bids=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in item["b"]),
                    asks=tuple(OrderBookLevel(price=row[0], quantity=row[1]) for row in item["a"]),
                    **_orderbook_reconciliation(item),
                )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@aggTrade"
        async with aclosing(_websocket_json(url, venue=self.venue)) as messages:
            async for item in messages:
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["T"]),
                    trade_id=str(item["a"]),
                    price=item["p"],
                    quantity=item["q"],
                    side=Side.SELL if item["m"] else Side.BUY,
                    **_source_provenance(item),
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@bookTicker"
        async with aclosing(_websocket_json(url, venue=self.venue)) as messages:
            async for item in messages:
                yield item


class HyperliquidMarketDataAdapter(PublicRestAdapter):
    venue = "hyperliquid"
    base_url = "https://api.hyperliquid.xyz"
    order_book_stream_semantics = OrderBookStreamSemantics.SNAPSHOT_ONLY
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official docs checked 2026-07-12",
        spot=CapabilitySupport.IMPLEMENTED,
        perpetual=CapabilitySupport.IMPLEMENTED,
        funding_current=CapabilitySupport.DOCUMENTED,
        funding_history=CapabilitySupport.DOCUMENTED,
        predicted_funding=CapabilitySupport.IMPLEMENTED,
        open_interest=CapabilitySupport.DOCUMENTED,
        liquidations=CapabilitySupport.UNAVAILABLE,
        wallet_liquidation_history=CapabilitySupport.IMPLEMENTED,
        market_liquidation_stream=CapabilitySupport.UNAVAILABLE,
        aggregate_liquidation_history=CapabilitySupport.UNAVAILABLE,
        orderbook_snapshot=CapabilitySupport.IMPLEMENTED,
        orderbook_delta=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        mark_price=CapabilitySupport.DOCUMENTED,
        index_price=CapabilitySupport.DOCUMENTED,
        wallet_positions=CapabilitySupport.DOCUMENTED,
        wallet_transfers=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
        post_only=CapabilitySupport.DOCUMENTED,
        reduce_only=CapabilitySupport.DOCUMENTED,
        ioc=CapabilitySupport.DOCUMENTED,
        batch_orders=CapabilitySupport.DOCUMENTED,
        subaccounts=CapabilitySupport.DOCUMENTED,
    )

    async def _info(self, payload: dict[str, object]) -> Any:
        response = await self.client.post("/info", json=payload)
        response.raise_for_status()
        return response.json()

    async def fetch_server_time(self, symbol: str = "BTC") -> datetime:
        data = await self._info({"type": "l2Book", "coin": symbol})
        return _ms(data["time"])

    async def fetch_markets(self) -> Sequence[Market]:
        return await self.fetch_perpetual_markets()

    async def fetch_perpetual_markets(self) -> Sequence[Market]:
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

    async def fetch_spot_markets(self) -> Sequence[dict[str, object]]:
        data = await self._info({"type": "spotMeta"})
        tokens = {int(item["index"]): item for item in data["tokens"]}
        result: list[dict[str, object]] = []
        for pair in data["universe"]:
            base, quote = (tokens[int(index)] for index in pair["tokens"])
            result.append(
                {
                    "token_index": int(base["index"]),
                    "canonical_token_name": base["name"],
                    "venue_internal_pair_name": pair["name"],
                    "base_token": base["name"],
                    "quote_token": quote["name"],
                    "szDecimals": base["szDecimals"],
                    "weiDecimals": base["weiDecimals"],
                    "index": pair["index"],
                    "deployer": base.get("deployer"),
                }
            )
        return tuple(result)

    async def fetch_perpetual_asset_contexts(self) -> Sequence[dict[str, object]]:
        meta, contexts = await self._info({"type": "metaAndAssetCtxs"})
        return tuple(
            {"venue_symbol": item["name"], "kind": "perpetual", **context}
            for item, context in zip(meta["universe"], contexts, strict=True)
        )

    async def fetch_spot_asset_contexts(self) -> Sequence[dict[str, object]]:
        meta, contexts = await self._info({"type": "spotMetaAndAssetCtxs"})
        return tuple(
            {
                "venue_internal_pair_name": item["name"],
                "pair_index": item["index"],
                "kind": "spot",
                **context,
            }
            for item, context in zip(meta["universe"], contexts, strict=True)
        )

    async def fetch_predicted_funding(self) -> Sequence[dict[str, object]]:
        data = await self._info({"type": "predictedFundings"})
        return tuple({"venue_symbol": row[0], "venues": row[1]} for row in data)

    async def fetch_wallet_liquidation_history(self, wallet: str) -> Sequence[dict[str, object]]:
        fills = await self._info({"type": "userFills", "user": wallet})
        return tuple(
            item
            for item in fills
            if item.get("dir") in {"Liquidated Long", "Liquidated Short"}
            or item.get("liquidation") is not None
        )

    async def fetch_wallet_transfers(self, wallet: str) -> Sequence[dict[str, object]]:
        data = await self._info({"type": "userNonFundingLedgerUpdates", "user": wallet})
        return tuple(
            item
            for item in data
            if item.get("delta", {}).get("type")
            in {"deposit", "withdraw", "internalTransfer", "spotTransfer", "accountClassTransfer"}
        )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[OHLCV]:
        if start is None or end is None:
            raise ValueError("Hyperliquid OHLCV requires explicit start and end")
        if end <= start:
            raise ValueError("end must be after start")
        interval_ms = _timeframe_ms(timeframe)
        cursor_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        rows: dict[int, dict[str, Any]] = {}
        while cursor_ms < end_ms:
            page_end = min(end_ms, cursor_ms + interval_ms * max(1, min(limit, 5000)))
            page = await self._info(
                {
                    "type": "candleSnapshot",
                    "req": {
                        "coin": symbol,
                        "interval": timeframe,
                        "startTime": cursor_ms,
                        "endTime": page_end,
                    },
                }
            )
            for item in page:
                opened = int(item["t"])
                if cursor_ms <= opened < end_ms:
                    rows[opened] = item
            if not page:
                cursor_ms = page_end
            else:
                cursor_ms = max(page_end, max(int(item["t"]) for item in page) + interval_ms)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
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
                closed=opened + interval_ms <= now_ms,
            )
            for opened, item in sorted(rows.items())
            if opened + interval_ms <= now_ms
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
        requested_end = end or now
        cursor = start or requested_end - timedelta(days=7)
        by_effective_time: dict[datetime, FundingRate] = {}
        for _ in range(20):
            payload: dict[str, object] = {
                "type": "fundingHistory",
                "coin": symbol,
                "startTime": int(cursor.timestamp() * 1000),
                "endTime": int(requested_end.timestamp() * 1000),
            }
            data = await self._info(payload)
            page = tuple(
                FundingRate(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["time"]),
                    rate=item["fundingRate"],
                    funding_interval_seconds=3600,
                    funding_schedule_source="hyperliquid_documented_hourly_schedule",
                )
                for item in data
            )
            for item in page:
                if item.exchange_timestamp is not None:
                    by_effective_time[item.exchange_timestamp] = item
            if len(page) < 500:
                break
            newest = max(
                item.exchange_timestamp for item in page if item.exchange_timestamp is not None
            )
            if newest >= requested_end:
                break
            cursor = newest + timedelta(milliseconds=1)
        return tuple(by_effective_time[key] for key in sorted(by_effective_time))

    async def fetch_current_funding_rate(self, symbol: str) -> FundingRate:
        meta, contexts = await self._info({"type": "metaAndAssetCtxs"})
        index = next(i for i, item in enumerate(meta["universe"]) if item["name"] == symbol)
        item = contexts[index]
        received_at = datetime.now(UTC)
        next_hour = received_at.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return FundingRate(
            exchange=self.venue,
            symbol=symbol,
            exchange_timestamp=None,
            received_at=received_at,
            available_at=received_at,
            rate=item["funding"],
            next_funding_at=next_hour,
            funding_interval_seconds=3600,
            funding_schedule_source="hyperliquid_documented_hourly_schedule",
            is_finalized=False,
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
                exchange_timestamp=None,
                received_at=datetime.now(UTC),
                available_at=datetime.now(UTC),
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

    async def _stream(
        self,
        subscription: dict[str, object],
        *,
        order_book: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        request: dict[str, object] = {"method": "subscribe", "subscription": subscription}
        symbol = str(subscription.get("coin", "SYSTEM"))
        async with aclosing(
            _websocket_json(
                "wss://api.hyperliquid.xyz/ws",
                request,
                venue=self.venue,
                classification=(
                    StreamClassification.LIMITED_DEPTH_SNAPSHOT_STREAM
                    if order_book
                    else StreamClassification.EVENTS
                ),
                order_book_semantics=(self.order_book_stream_semantics if order_book else None),
                instrument=symbol,
                snapshot=lambda value: (
                    order_book and isinstance(value, dict) and value.get("channel") == "l2Book"
                ),
                validate_snapshot=lambda value: (
                    not order_book
                    or (
                        value.get("channel") == "l2Book"
                        and value.get("data", {}).get("coin") == symbol
                        and _valid_exchange_ms(value.get("data", {}).get("time"))
                        and len(value.get("data", {}).get("levels", [])) == 2
                        and all(value.get("data", {}).get("levels", []))
                    )
                ),
                exchange_timestamp=lambda value: (
                    _ms(value["data"]["time"])
                    if order_book and value.get("channel") == "l2Book"
                    else None
                ),
                lifecycle_sink=self._record_connection_lifecycle,
                application_heartbeat={"method": "ping"},
            )
        ) as messages:
            async for message in messages:
                if message.get("channel") != "subscriptionResponse":
                    yield message

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        async for message in self._stream({"type": "l2Book", "coin": symbol}, order_book=True):
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
                **_orderbook_reconciliation(message),
            )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        async for message in self._stream({"type": "trades", "coin": symbol}):
            for item in sorted(message["data"], key=lambda value: int(value["time"])):
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["time"]),
                    trade_id=str(item["hash"]),
                    price=item["px"],
                    quantity=item["sz"],
                    side=Side.BUY if item["side"] == "B" else Side.SELL,
                    **_source_provenance(message),
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        async for message in self._stream({"type": "allMids"}):
            mids = message["data"]["mids"]
            if symbol in mids:
                yield {"symbol": symbol, "mid": mids[symbol], "timestamp": datetime.now(UTC)}


def _timeframe_ms(timeframe: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    try:
        return int(timeframe[:-1]) * units[timeframe[-1]]
    except (KeyError, ValueError, IndexError) as exc:
        raise ValueError(f"unsupported timeframe: {timeframe}") from exc


class BitgetMarketDataAdapter(PublicRestAdapter):
    venue = "bitget"
    base_url = "https://api.bitget.com"
    order_book_stream_semantics = OrderBookStreamSemantics.SNAPSHOT_AND_DELTA
    capabilities = VenueCapabilityMatrix(
        venue=venue,
        detected_at=datetime(2026, 7, 12, tzinfo=UTC),
        source_version="official v2 API checked 2026-07-12",
        spot=CapabilitySupport.DOCUMENTED,
        perpetual=CapabilitySupport.DOCUMENTED,
        dated_futures=CapabilitySupport.DOCUMENTED,
        funding_current=CapabilitySupport.DOCUMENTED,
        funding_history=CapabilitySupport.DOCUMENTED,
        open_interest=CapabilitySupport.DOCUMENTED,
        liquidations=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.DOCUMENTED,
        orderbook_delta=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        mark_price=CapabilitySupport.DOCUMENTED,
        index_price=CapabilitySupport.DOCUMENTED,
        long_short_ratio=CapabilitySupport.DOCUMENTED,
        wallet_positions=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
        post_only=CapabilitySupport.DOCUMENTED,
        reduce_only=CapabilitySupport.DOCUMENTED,
        ioc=CapabilitySupport.DOCUMENTED,
        fok=CapabilitySupport.DOCUMENTED,
        batch_orders=CapabilitySupport.DOCUMENTED,
        subaccounts=CapabilitySupport.DOCUMENTED,
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
        granularity = {
            "1h": "1H",
            "4h": "4H",
            "1d": "1D",
            "1w": "1W",
        }.get(timeframe, timeframe)
        params: dict[str, str | int] = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "granularity": granularity,
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
        requested_end = end or datetime.now(UTC)
        requested_start = start or requested_end - timedelta(days=30)
        by_effective_time: dict[datetime, FundingRate] = {}
        for page_number in range(1, 21):
            response = await self.client.get(
                "/api/v2/mix/market/history-fund-rate",
                params={
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "pageSize": 100,
                    "pageNo": page_number,
                },
            )
            response.raise_for_status()
            rows = response.json()["data"]
            page = tuple(
                FundingRate(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["fundingTime"]),
                    rate=item["fundingRate"],
                    funding_interval_seconds=28800,
                    funding_schedule_source="bitget_documented_eight_hour_schedule",
                )
                for item in rows
            )
            for item in page:
                timestamp = item.exchange_timestamp
                if timestamp is not None and requested_start <= timestamp <= requested_end:
                    by_effective_time[timestamp] = item
            if len(rows) < 100 or (
                page
                and min(item.exchange_timestamp for item in page if item.exchange_timestamp)
                <= requested_start
            ):
                break
        return tuple(by_effective_time[key] for key in sorted(by_effective_time))

    async def fetch_current_funding_rate(self, symbol: str) -> FundingRate:
        response = await self.client.get(
            "/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"},
        )
        response.raise_for_status()
        data = response.json()["data"]
        if not data:
            raise CapabilityUnavailableError(f"{self.venue} current funding is unavailable")
        item = data[0]
        received_at = datetime.now(UTC)
        return FundingRate(
            exchange=self.venue,
            symbol=symbol,
            exchange_timestamp=None,
            received_at=received_at,
            available_at=received_at,
            rate=item["fundingRate"],
            next_funding_at=_ms(item["nextUpdate"]),
            funding_interval_seconds=int(item["fundingRateInterval"]) * 3600,
            funding_schedule_source="exchange_payload",
            is_finalized=False,
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

    async def _stream(
        self,
        channel: str,
        symbol: str,
        classification: StreamClassification = StreamClassification.EVENTS,
        order_book_state: tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        request: dict[str, object] = {
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": channel, "instId": symbol}],
        }
        bids, asks = order_book_state or ({}, {})

        def first(value: Any) -> dict[str, Any]:
            data = value.get("data", []) if isinstance(value, dict) else []
            if not data or not isinstance(data[0], dict):
                raise ValueError("Bitget order-book payload data is missing")
            return data[0]

        def apply_levels(target: dict[Decimal, Decimal], levels: Any) -> None:
            for price_raw, quantity_raw, *_ in levels:
                price, quantity = _decimal(price_raw), _decimal(quantity_raw)
                if quantity == 0:
                    target.pop(price, None)
                else:
                    target[price] = quantity

        def apply_snapshot(value: Any) -> int | None:
            item = first(value)
            bids.clear()
            asks.clear()
            apply_levels(bids, item.get("bids", []))
            apply_levels(asks, item.get("asks", []))
            return int(item["seq"])

        def apply_delta(value: Any) -> None:
            item = first(value)
            apply_levels(bids, item.get("bids", []))
            apply_levels(asks, item.get("asks", []))

        order_book = classification == StreamClassification.SNAPSHOT_DELTA
        async with aclosing(
            _websocket_json(
                "wss://ws.bitget.com/v2/ws/public",
                request,
                venue=self.venue,
                classification=classification,
                order_book_semantics=(self.order_book_stream_semantics if order_book else None),
                instrument=symbol,
                sequence=lambda value: int(first(value)["seq"]) if order_book else None,
                sequence_predecessor=lambda value: (
                    int(first(value).get("pseq", 0)) or None if order_book else None
                ),
                snapshot=lambda value: order_book and value.get("action") == "snapshot",
                delta=lambda value: order_book and value.get("action") == "update",
                validate_snapshot=lambda value: (
                    not order_book
                    or (
                        value.get("arg", {}).get("instId") == symbol
                        and value.get("action") == "snapshot"
                        and _valid_exchange_ms(first(value).get("ts"))
                        and bool(first(value).get("bids"))
                        and bool(first(value).get("asks"))
                    )
                ),
                snapshot_applier=apply_snapshot if order_book else None,
                delta_applier=apply_delta if order_book else None,
                exchange_timestamp=lambda value: _ms(first(value)["ts"]) if order_book else None,
                lifecycle_sink=self._record_connection_lifecycle,
                application_heartbeat="ping",
            )
        ) as messages:
            async for message in messages:
                if "data" in message:
                    yield message

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        async for message in self._stream(
            "books",
            symbol,
            StreamClassification.SNAPSHOT_DELTA,
            (bids, asks),
        ):
            data = message["data"][0]
            yield OrderBook(
                exchange=self.venue,
                symbol=symbol,
                timestamp=_ms(data["ts"]),
                sequence=int(data.get("seq", 0)) or None,
                bids=tuple(
                    OrderBookLevel(price=price, quantity=quantity)
                    for price, quantity in sorted(bids.items(), reverse=True)
                ),
                asks=tuple(
                    OrderBookLevel(price=price, quantity=quantity)
                    for price, quantity in sorted(asks.items())
                ),
                **_orderbook_reconciliation(message),
            )

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        async for message in self._stream("trade", symbol):
            for item in sorted(message["data"], key=lambda value: int(value["ts"])):
                yield Trade(
                    exchange=self.venue,
                    symbol=symbol,
                    timestamp=_ms(item["ts"]),
                    trade_id=str(item.get("tradeId", item["ts"])),
                    price=item["price"],
                    quantity=item["size"],
                    side=Side(item["side"]),
                    **_source_provenance(message),
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
        perpetual=CapabilitySupport.DOCUMENTED,
        funding_current=CapabilitySupport.DOCUMENTED,
        funding_history=CapabilitySupport.DOCUMENTED,
        predicted_funding=CapabilitySupport.DOCUMENTED,
        open_interest=VenueCapability(
            name="open_interest",
            support=CapabilitySupport.DEGRADED,
            documented_at=datetime(2026, 7, 12, tzinfo=UTC),
            implemented_at=datetime(2026, 7, 12, tzinfo=UTC),
            live_verified_at=datetime(2026, 7, 12, tzinfo=UTC),
            source_url="https://mexcdevelop.github.io/apidocs/contract_v1_en/",
            verification_run_id="public-api-smoke-2026-07-12",
            failure_reason="public endpoint returned HTTP 403 from deployment environment",
        ),
        liquidations=CapabilitySupport.DOCUMENTED,
        orderbook_snapshot=CapabilitySupport.DOCUMENTED,
        orderbook_delta=CapabilitySupport.DOCUMENTED,
        trades=CapabilitySupport.DOCUMENTED,
        mark_price=CapabilitySupport.DOCUMENTED,
        index_price=CapabilitySupport.DOCUMENTED,
        wallet_positions=CapabilitySupport.DOCUMENTED,
        private_websocket=CapabilitySupport.DOCUMENTED,
        post_only=CapabilitySupport.DOCUMENTED,
        reduce_only=CapabilitySupport.DOCUMENTED,
        ioc=CapabilitySupport.DOCUMENTED,
        fok=CapabilitySupport.DOCUMENTED,
        batch_orders=CapabilitySupport.DOCUMENTED,
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
        async with aclosing(
            _websocket_json("wss://contract.mexc.com/edge", request, venue=self.venue)
        ) as messages:
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
                **_orderbook_reconciliation(message),
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
                    **_source_provenance(message),
                )

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        async for message in self._stream("sub.ticker", symbol):
            yield message["data"]
