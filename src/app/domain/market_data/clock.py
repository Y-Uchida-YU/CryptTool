from __future__ import annotations

from datetime import UTC, datetime, timedelta
from time import monotonic

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CrossVenueTimestamp(BaseModel):
    model_config = ConfigDict(frozen=True)
    exchange_timestamp: datetime
    received_at: datetime
    available_at: datetime
    local_monotonic_time: float = Field(ge=0)
    clock_offset_estimate: timedelta

    @field_validator("exchange_timestamp", "received_at", "available_at")
    @classmethod
    def timestamps_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("cross-venue timestamps must be timezone-aware")
        return value.astimezone(UTC)


class VenueClock:
    def __init__(self, maximum_skew: timedelta = timedelta(seconds=2)) -> None:
        if maximum_skew <= timedelta(0):
            raise ValueError("maximum_skew must be positive")
        self.maximum_skew = maximum_skew
        self.offsets: dict[str, timedelta] = {}

    def observe_server_time(
        self, venue: str, server_time: datetime, request_sent_at: datetime, received_at: datetime
    ) -> timedelta:
        server_time, request_sent_at, received_at = map(
            _utc, (server_time, request_sent_at, received_at)
        )
        if received_at < request_sent_at:
            raise ValueError("received_at precedes request_sent_at")
        midpoint = request_sent_at + (received_at - request_sent_at) / 2
        offset = server_time - midpoint
        self.offsets[venue] = offset
        return offset

    def stamp(
        self, venue: str, exchange_timestamp: datetime, received_at: datetime | None = None
    ) -> CrossVenueTimestamp:
        received = _utc(received_at or datetime.now(UTC))
        exchange = _utc(exchange_timestamp)
        offset = self.offsets.get(venue, exchange - received)
        return CrossVenueTimestamp(
            exchange_timestamp=exchange,
            received_at=received,
            available_at=received,
            local_monotonic_time=monotonic(),
            clock_offset_estimate=offset,
        )

    def comparable(self, *events: CrossVenueTimestamp) -> bool:
        if len(events) < 2:
            return True
        available = [event.available_at for event in events]
        offsets_ok = all(abs(event.clock_offset_estimate) <= self.maximum_skew for event in events)
        return offsets_ok and max(available) - min(available) <= self.maximum_skew

    def synchronized(self, venue: str, ntp_offset: timedelta) -> tuple[bool, str]:
        venue_offset = self.offsets.get(venue)
        if venue_offset is None:
            return False, "venue server-time offset has not been observed"
        if abs(ntp_offset) > self.maximum_skew:
            return False, "local NTP offset exceeds tolerance"
        disagreement = abs(venue_offset - ntp_offset)
        if disagreement > self.maximum_skew:
            return False, "NTP and venue server time disagree"
        return True, "clock synchronized"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
