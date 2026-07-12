from dataclasses import dataclass
from datetime import datetime
from math import isfinite

from app.config.settings import RegimeSettings
from app.domain.regimes.models import Regime


@dataclass(frozen=True)
class RuleEvidence:
    regime: Regime
    score: float
    reason: str


class DeterministicRuleEngine:
    def __init__(self, settings: RegimeSettings) -> None:
        self.settings = settings

    def evaluate(
        self, features: dict[str, float | None], timestamp: datetime
    ) -> list[RuleEvidence]:
        del timestamp
        f = {key: value for key, value in features.items() if value is not None and isfinite(value)}
        z, severe = self.settings.z_extreme, self.settings.z_severe
        out: list[RuleEvidence] = []
        slope, return_z = f.get("ma_slope_z"), f.get("return_zscore")
        vol_z, oi_z = f.get("volatility_zscore"), f.get("oi_zscore")
        funding_z, liq_z = f.get("funding_zscore"), f.get("liquidation_zscore")
        spread_z = f.get("spread_zscore")
        spot_return_z = f.get("spot_return_zscore")
        perp_return_z = f.get("perp_return_zscore")
        risk_return_z = f.get("cross_asset_return_zscore")
        risk_breadth = f.get("risk_breadth")
        if slope is not None and slope >= self.settings.trend_slope_z:
            out.append(
                RuleEvidence(Regime.TREND_UP, min(abs(slope) / z, 1), f"ma_slope_z={slope:.3f}")
            )
        elif slope is not None and slope <= -self.settings.trend_slope_z:
            out.append(
                RuleEvidence(Regime.TREND_DOWN, min(abs(slope) / z, 1), f"ma_slope_z={slope:.3f}")
            )
        elif slope is not None and abs(slope) < self.settings.trend_slope_z * 0.5:
            out.append(
                RuleEvidence(
                    Regime.RANGE,
                    1 - abs(slope) / self.settings.trend_slope_z,
                    f"flat slope={slope:.3f}",
                )
            )
        if vol_z is not None and vol_z >= z:
            out.append(
                RuleEvidence(
                    Regime.HIGH_VOLATILITY, min(vol_z / severe, 1), f"volatility_zscore={vol_z:.3f}"
                )
            )
        elif vol_z is not None and vol_z <= self.settings.low_vol_z:
            out.append(
                RuleEvidence(
                    Regime.LOW_VOLATILITY, min(abs(vol_z) / z, 1), f"volatility_zscore={vol_z:.3f}"
                )
            )
        if funding_z is not None and funding_z >= z:
            out.append(
                RuleEvidence(
                    Regime.FUNDING_EXTREME_POSITIVE,
                    min(funding_z / severe, 1),
                    f"funding_zscore={funding_z:.3f}",
                )
            )
        elif funding_z is not None and funding_z <= -z:
            out.append(
                RuleEvidence(
                    Regime.FUNDING_EXTREME_NEGATIVE,
                    min(abs(funding_z) / severe, 1),
                    f"funding_zscore={funding_z:.3f}",
                )
            )
        if oi_z is not None and oi_z >= z:
            out.append(
                RuleEvidence(Regime.OI_EXPANSION, min(oi_z / severe, 1), f"oi_zscore={oi_z:.3f}")
            )
        elif oi_z is not None and oi_z <= -z:
            out.append(
                RuleEvidence(
                    Regime.OI_CONTRACTION, min(abs(oi_z) / severe, 1), f"oi_zscore={oi_z:.3f}"
                )
            )
        if return_z is not None and return_z <= -severe and liq_z is not None and liq_z >= z:
            out.append(
                RuleEvidence(
                    Regime.LONG_SQUEEZE,
                    min((abs(return_z) + liq_z) / (2 * severe), 1),
                    f"negative return_z={return_z:.3f}, liquidation_z={liq_z:.3f}",
                )
            )
            if oi_z is not None and oi_z <= -z and spread_z is not None and spread_z >= z:
                out.append(
                    RuleEvidence(
                        Regime.FLASH_CRASH,
                        min((abs(return_z) + liq_z + abs(oi_z) + spread_z) / (4 * severe), 1),
                        "multi-stage crash confirmation",
                    )
                )
        if return_z is not None and return_z >= severe and liq_z is not None and liq_z >= z:
            out.append(
                RuleEvidence(
                    Regime.SHORT_SQUEEZE,
                    min((return_z + liq_z) / (2 * severe), 1),
                    f"positive return_z={return_z:.3f}, liquidation_z={liq_z:.3f}",
                )
            )
        if liq_z is not None and liq_z >= severe:
            out.append(
                RuleEvidence(
                    Regime.LIQUIDATION_CASCADE,
                    min(liq_z / (severe + 1), 1),
                    f"liquidation_zscore={liq_z:.3f}",
                )
            )
        if spot_return_z is not None and perp_return_z is not None:
            if abs(spot_return_z) >= z and abs(spot_return_z) > abs(perp_return_z) + 0.5:
                out.append(
                    RuleEvidence(
                        Regime.SPOT_LED_MOVE,
                        min((abs(spot_return_z) - abs(perp_return_z)) / z, 1),
                        f"spot/perp return z={spot_return_z:.3f}/{perp_return_z:.3f}",
                    )
                )
            elif abs(perp_return_z) >= z and abs(perp_return_z) > abs(spot_return_z) + 0.5:
                out.append(
                    RuleEvidence(
                        Regime.PERP_LED_MOVE,
                        min((abs(perp_return_z) - abs(spot_return_z)) / z, 1),
                        f"perp/spot return z={perp_return_z:.3f}/{spot_return_z:.3f}",
                    )
                )
        if (
            risk_return_z is not None
            and risk_return_z <= -z
            and risk_breadth is not None
            and risk_breadth <= -0.67
        ):
            out.append(
                RuleEvidence(
                    Regime.RISK_OFF,
                    min((abs(risk_return_z) / severe + abs(risk_breadth)) / 2, 1),
                    f"cross_asset_return_z={risk_return_z:.3f}, breadth={risk_breadth:.3f}",
                )
            )
        return out
