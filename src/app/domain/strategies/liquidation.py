from datetime import datetime

from app.domain.market_data.evidence import (
    RejectedSignal,
    SignalDataEvidence,
    require_signal_capabilities,
)


class LiquidationStrategyCapabilityGate:
    required_capabilities = ("market_liquidation_stream", "open_interest", "trades")

    def validate_evidence(
        self,
        signal_id: str,
        evidence: SignalDataEvidence,
        venue: str,
        now: datetime,
        maximum_age_seconds: int,
    ) -> RejectedSignal | None:
        return require_signal_capabilities(
            signal_id, evidence, self.required_capabilities, venue, now, maximum_age_seconds
        )
