from datetime import UTC, datetime, timedelta

from app.domain.venues.risk import (
    CEX_SIGNALS,
    DEX_SIGNALS,
    VenueRiskObservation,
    VenueRiskSignal,
    venue_risk_score,
)


class VenueRiskMonitor:
    def __init__(self, *, maximum_age: timedelta = timedelta(minutes=5)) -> None:
        if maximum_age <= timedelta(0):
            raise ValueError("maximum age must be positive")
        self.maximum_age = maximum_age
        self._latest: dict[tuple[str, VenueRiskSignal], VenueRiskObservation] = {}

    def record(self, observation: VenueRiskObservation) -> None:
        key = (observation.venue, observation.signal)
        previous = self._latest.get(key)
        if previous and observation.observed_at < previous.observed_at:
            raise ValueError("out-of-order venue risk observation")
        self._latest[key] = observation

    def execution_health(
        self, venue: str, now: datetime, *, dex: bool
    ) -> tuple[bool, float, tuple[str, ...]]:
        if now.tzinfo is None:
            raise ValueError("monitor timestamp must be timezone-aware")
        now = now.astimezone(UTC)
        required = DEX_SIGNALS if dex else CEX_SIGNALS
        observations: list[VenueRiskObservation] = []
        reasons: list[str] = []
        for signal in required:
            item = self._latest.get((venue, signal))
            if item is None:
                reasons.append(f"missing {signal.value}")
            elif now - item.observed_at > self.maximum_age or now < item.observed_at:
                reasons.append(f"stale {signal.value}")
            else:
                observations.append(item)
                if not item.healthy:
                    reasons.append(f"unhealthy {signal.value}: {item.evidence}")
        return not reasons, venue_risk_score(tuple(observations)), tuple(reasons)
