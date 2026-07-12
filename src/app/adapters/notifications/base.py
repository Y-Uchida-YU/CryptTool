from typing import Protocol


class NotificationAdapter(Protocol):
    async def send(self, title: str, message: str, severity: str = "info") -> None: ...


class NullNotificationAdapter:
    async def send(self, title: str, message: str, severity: str = "info") -> None:
        del title, message, severity
