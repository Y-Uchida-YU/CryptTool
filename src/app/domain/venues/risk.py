from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class VenueRiskSignal(StrEnum):
    SMART_CONTRACT = "smart_contract_risk"
    ORACLE_DIVERGENCE = "oracle_divergence"
    BRIDGE = "bridge_risk"
    CHAIN_HALT = "chain_halt"
    CONSENSUS_DEGRADATION = "sequencer_or_validator_degradation"
    RPC_DISAGREEMENT = "rpc_disagreement"
    STABLECOIN_DEPEG = "stablecoin_depeg"
    WALLET_NONCE_FAILURE = "wallet_nonce_failure"
    GAS_INSUFFICIENCY = "gas_insufficiency"
    FRONTEND_UNAVAILABLE = "frontend_unavailable"
    WITHDRAWAL_SUSPENSION = "withdrawal_suspension"
    DEPOSIT_SUSPENSION = "deposit_suspension"
    MAINTENANCE = "maintenance"
    API_DEGRADATION = "api_degradation"
    MARK_INDEX_DIVERGENCE = "mark_index_divergence"
    ADL = "adl_risk"
    INSURANCE_FUND_CHANGE = "insurance_fund_change"
    ACCOUNT_RESTRICTION = "account_restriction"
    KYC_CHANGE = "kyc_status_change"
    JURISDICTION_CHANGE = "jurisdiction_status_change"


class VenueRiskObservation(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: str
    signal: VenueRiskSignal
    observed_at: datetime
    healthy: bool
    severity: float = Field(ge=0, le=1)
    evidence: str

    @field_validator("observed_at")
    @classmethod
    def observed_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("risk observation timestamp must be timezone-aware")
        return value.astimezone(UTC)


def venue_risk_score(observations: tuple[VenueRiskObservation, ...]) -> float:
    if not observations:
        return 0.0
    return min(1.0, max((item.severity for item in observations if not item.healthy), default=0.0))


DEX_SIGNALS = frozenset(
    {
        VenueRiskSignal.SMART_CONTRACT,
        VenueRiskSignal.ORACLE_DIVERGENCE,
        VenueRiskSignal.BRIDGE,
        VenueRiskSignal.CHAIN_HALT,
        VenueRiskSignal.CONSENSUS_DEGRADATION,
        VenueRiskSignal.RPC_DISAGREEMENT,
        VenueRiskSignal.STABLECOIN_DEPEG,
        VenueRiskSignal.WALLET_NONCE_FAILURE,
        VenueRiskSignal.GAS_INSUFFICIENCY,
        VenueRiskSignal.FRONTEND_UNAVAILABLE,
    }
)

CEX_SIGNALS = frozenset(
    {
        VenueRiskSignal.WITHDRAWAL_SUSPENSION,
        VenueRiskSignal.DEPOSIT_SUSPENSION,
        VenueRiskSignal.MAINTENANCE,
        VenueRiskSignal.API_DEGRADATION,
        VenueRiskSignal.MARK_INDEX_DIVERGENCE,
        VenueRiskSignal.ADL,
        VenueRiskSignal.INSURANCE_FUND_CHANGE,
        VenueRiskSignal.ACCOUNT_RESTRICTION,
        VenueRiskSignal.KYC_CHANGE,
        VenueRiskSignal.JURISDICTION_CHANGE,
    }
)
