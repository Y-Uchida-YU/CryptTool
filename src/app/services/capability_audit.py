from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from app.adapters.exchanges.base import MarketDataAdapter
from app.domain.venues.models import CapabilitySupport, VenueCapabilityMatrix


@dataclass(frozen=True)
class CapabilityAuditFinding:
    capability: str
    passed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CapabilityAuditReport:
    venue: str
    audited_at: datetime
    findings: tuple[CapabilityAuditFinding, ...]

    @property
    def passed(self) -> bool:
        return all(finding.passed for finding in self.findings)


class CapabilityContractAuditor:
    METHOD_BY_CAPABILITY: ClassVar[dict[str, str]] = {
        "spot": "fetch_spot_markets",
        "perpetual": "fetch_perpetual_markets",
        "funding_current": "fetch_current_funding",
        "funding_history": "fetch_funding_rates",
        "predicted_funding": "fetch_predicted_funding",
        "open_interest": "fetch_open_interest",
        "orderbook_snapshot": "fetch_order_book",
        "orderbook_delta": "stream_order_book",
        "trades": "fetch_recent_trades",
        "mark_price": "fetch_mark_price",
        "index_price": "fetch_index_price",
        "wallet_transfers": "fetch_wallet_transfers",
        "wallet_liquidation_history": "fetch_wallet_liquidation_history",
    }

    def __init__(
        self,
        *,
        contract_fixtures: set[tuple[str, str]],
        normalized_capabilities: set[tuple[str, str]],
        live_smoke_runs: set[str] | None = None,
        maximum_age: timedelta = timedelta(days=7),
        now: datetime | None = None,
    ) -> None:
        self.contract_fixtures = contract_fixtures
        self.normalized_capabilities = normalized_capabilities
        self.live_smoke_runs = live_smoke_runs or set()
        self.maximum_age = maximum_age
        self.now = (now or datetime.now(UTC)).astimezone(UTC)

    def audit(
        self, adapter: MarketDataAdapter, matrix: VenueCapabilityMatrix
    ) -> CapabilityAuditReport:
        findings: list[CapabilityAuditFinding] = []
        for name, capability in matrix.capabilities.items():
            if capability.support not in {
                CapabilitySupport.IMPLEMENTED,
                CapabilitySupport.LIVE_VERIFIED,
            }:
                continue
            reasons: list[str] = []
            method_name = self.METHOD_BY_CAPABILITY.get(name)
            if method_name is None or not self._overrides(adapter, method_name):
                reasons.append("adapter method is not implemented by the concrete adapter")
            if (matrix.venue, name) not in self.contract_fixtures:
                reasons.append("contract fixture is missing")
            if (matrix.venue, name) not in self.normalized_capabilities:
                reasons.append("domain normalization evidence is missing")
            if name == "orderbook_delta":
                for attribute in (
                    "sequence_semantics",
                    "snapshot_loader",
                    "delta_applier",
                    "gap_recovery",
                    "duplicate_detection",
                    "reconnect_policy",
                ):
                    if not getattr(adapter, attribute, None):
                        reasons.append(f"{attribute} is missing")
            if capability.support == CapabilitySupport.LIVE_VERIFIED:
                if not capability.verification_run_id:
                    reasons.append("verification_run_id is missing")
                elif capability.verification_run_id not in self.live_smoke_runs:
                    reasons.append("live smoke run is not registered")
                if capability.live_verified_at is None:
                    reasons.append("live_verified_at is missing")
                elif not timedelta(0) <= self.now - capability.live_verified_at <= self.maximum_age:
                    reasons.append("live verification is stale or future-dated")
            findings.append(CapabilityAuditFinding(name, not reasons, tuple(reasons)))
        return CapabilityAuditReport(matrix.venue, self.now, tuple(findings))

    @staticmethod
    def _overrides(adapter: MarketDataAdapter, method_name: str) -> bool:
        method = getattr(type(adapter), method_name, None)
        base_method = getattr(MarketDataAdapter, method_name, None)
        return method is not None and method is not base_method and inspect.isroutine(method)
