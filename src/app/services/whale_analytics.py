from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class WalletSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: str
    wallet: str
    symbol: str
    observed_at: datetime
    position: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    leverage: Decimal = Field(ge=0)
    liquidation_price: Decimal | None = Field(default=None, gt=0)
    mark_price: Decimal = Field(gt=0)
    account_equity: Decimal = Field(gt=0)
    cumulative_deposits: Decimal = Field(default=Decimal("0"), ge=0)
    cumulative_withdrawals: Decimal = Field(default=Decimal("0"), ge=0)

    @field_validator("observed_at")
    @classmethod
    def observed_at_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("wallet timestamp must be timezone-aware")
        return value.astimezone(UTC)


class WhaleFeatureSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: str
    wallet: str
    symbol: str
    observed_at: datetime
    position_change: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    leverage: Decimal
    liquidation_distance: Decimal | None
    net_transfer: Decimal
    position_concentration: Decimal
    historical_hit_rate: float | None = Field(default=None, ge=0, le=1)
    crowding: float = Field(ge=0, le=1)


class WhaleWalletAnalytics:
    """Produces auxiliary crowding/liquidation features; never emits copy-trade orders."""

    def build(
        self,
        current: WalletSnapshot,
        previous: WalletSnapshot | None,
        *,
        peer_positions: tuple[Decimal, ...] = (),
        historical_outcomes: tuple[bool, ...] = (),
    ) -> WhaleFeatureSnapshot:
        position_change = current.position - (previous.position if previous else Decimal("0"))
        liquidation_distance = (
            abs(current.mark_price - current.liquidation_price) / current.mark_price
            if current.liquidation_price is not None
            else None
        )
        net_transfer = current.cumulative_deposits - current.cumulative_withdrawals
        concentration = abs(current.position * current.mark_price) / current.account_equity
        same_direction = sum(
            1 for position in peer_positions if position != 0 and position * current.position > 0
        )
        crowding = same_direction / len(peer_positions) if peer_positions else 0.0
        hit_rate = (
            sum(historical_outcomes) / len(historical_outcomes) if historical_outcomes else None
        )
        return WhaleFeatureSnapshot(
            venue=current.venue,
            wallet=current.wallet,
            symbol=current.symbol,
            observed_at=current.observed_at,
            position_change=position_change,
            realized_pnl=current.realized_pnl,
            unrealized_pnl=current.unrealized_pnl,
            leverage=current.leverage,
            liquidation_distance=liquidation_distance,
            net_transfer=net_transfer,
            position_concentration=concentration,
            historical_hit_rate=hit_rate,
            crowding=crowding,
        )

    def regime_confidence_overlay(
        self, base_confidence: float, features: tuple[WhaleFeatureSnapshot, ...]
    ) -> tuple[float, tuple[str, ...]]:
        if not 0 <= base_confidence <= 1:
            raise ValueError("base confidence must be within [0, 1]")
        if not features:
            return base_confidence, ("no whale observations; confidence unchanged",)
        maximum_crowding = max(item.crowding for item in features)
        minimum_liquidation_distance = min(
            (
                item.liquidation_distance
                for item in features
                if item.liquidation_distance is not None
            ),
            default=None,
        )
        penalty = maximum_crowding * 0.20
        evidence = [f"whale crowding={maximum_crowding:.3f}"]
        if minimum_liquidation_distance is not None and minimum_liquidation_distance < Decimal(
            "0.05"
        ):
            penalty += 0.15
            evidence.append("whale liquidation distance below 5%")
        # The overlay only lowers confidence; it can never create or strengthen a trade signal.
        return max(0.0, base_confidence - penalty), tuple(evidence)
