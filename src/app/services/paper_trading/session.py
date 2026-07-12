from collections.abc import AsyncIterable, Awaitable, Callable, Sequence
from datetime import datetime

from app.adapters.notifications.base import NotificationAdapter
from app.services.paper_trading.broker import PaperBroker
from app.services.paper_trading.models import PaperAccountSnapshot, PaperOrderRequest, PaperQuote

SignalCallback = Callable[[PaperQuote], Awaitable[Sequence[PaperOrderRequest]]]


class PaperTradingSession:
    def __init__(
        self,
        broker: PaperBroker,
        notifier: NotificationAdapter,
        signal_callback: SignalCallback,
    ) -> None:
        self.broker = broker
        self.notifier = notifier
        self.signal_callback = signal_callback

    async def run(self, feed: AsyncIterable[PaperQuote]) -> None:
        async for quote in feed:
            fills = self.broker.on_quote(quote)
            for fill in fills:
                await self.notifier.send(
                    "Paper fill",
                    f"{fill.symbol} {fill.side} {fill.quantity} @ {fill.price}",
                )
            if quote.data_quality_score < self.broker.minimum_data_quality:
                continue
            for request in await self.signal_callback(quote):
                self.broker.submit(request, quote.timestamp)

    async def on_stream_disconnect(self, timestamp: datetime, reason: str) -> None:
        self.broker.halt(f"stream disconnected: {reason}", timestamp)
        await self.notifier.send("Paper trading halted", reason, "warning")

    async def on_stream_recovered(self, timestamp: datetime, reason: str) -> None:
        self.broker.resume(f"stream recovered: {reason}", timestamp)
        await self.notifier.send("Paper trading resumed", reason, "info")

    def account_snapshot(self, timestamp: datetime) -> PaperAccountSnapshot:
        return self.broker.snapshot(timestamp)
