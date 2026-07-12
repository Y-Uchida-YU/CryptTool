import httpx


class DiscordWebhookNotificationAdapter:
    """Optional outbound adapter; never enabled without an explicit webhook URL."""

    def __init__(
        self,
        webhook_url: str,
        timeout_seconds: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not webhook_url.startswith("https://"):
            raise ValueError("Discord webhook URL must use HTTPS")
        self._webhook_url = webhook_url
        self._timeout = timeout_seconds
        self._transport = transport

    async def send(self, title: str, message: str, severity: str = "info") -> None:
        payload = {
            "embeds": [{"title": title, "description": message, "footer": {"text": severity}}]
        }
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
            response = await client.post(self._webhook_url, json=payload)
            response.raise_for_status()
