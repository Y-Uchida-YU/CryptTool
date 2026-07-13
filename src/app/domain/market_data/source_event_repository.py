from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from app.adapters.exchanges.websocket import ReconciliationState


@dataclass(frozen=True)
class StoredSourceEvent:
    event_id: str
    venue: str
    symbol: str
    event_type: str
    exchange_timestamp: datetime | None
    received_at: datetime
    available_at: datetime
    payload_sha256: str
    sequence: int | None
    connection_id: UUID | None
    reconciliation_state: ReconciliationState | None
    data_quality_score: float


class SourceEventRepository(Protocol):
    def get(self, event_id: str) -> StoredSourceEvent | None: ...
