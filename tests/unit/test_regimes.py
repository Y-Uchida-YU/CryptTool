from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from app.config.settings import RegimeSettings
from app.domain.regimes.models import Regime
from app.services.regime_engine.ensemble import EnsembleRegimeEngine
from app.services.regime_engine.rules import DeterministicRuleEngine
from app.services.regime_engine.statistical import GaussianMixtureRegimeModel

NOW = datetime(2024, 1, 1, tzinfo=UTC)


def test_boundary_and_multiple_regimes() -> None:
    rules = DeterministicRuleEngine(RegimeSettings())
    exact = rules.evaluate(
        {
            "ma_slope_z": 1.0,
            "volatility_zscore": 2.326,
            "funding_zscore": 2.326,
            "oi_zscore": 2.326,
        },
        NOW,
    )
    regimes = {item.regime for item in exact}
    assert {
        Regime.TREND_UP,
        Regime.HIGH_VOLATILITY,
        Regime.FUNDING_EXTREME_POSITIVE,
        Regime.OI_EXPANSION,
    } <= regimes
    below = rules.evaluate({"ma_slope_z": 0.999, "volatility_zscore": 2.325}, NOW)
    assert Regime.TREND_UP not in {item.regime for item in below}


def test_flash_crash_requires_multistage_confirmation() -> None:
    rules = DeterministicRuleEngine(RegimeSettings())
    features = {"return_zscore": -4, "liquidation_zscore": 3, "oi_zscore": -3, "spread_zscore": 3}
    assert Regime.FLASH_CRASH in {item.regime for item in rules.evaluate(features, NOW)}
    features.pop("spread_zscore")
    assert Regime.FLASH_CRASH not in {item.regime for item in rules.evaluate(features, NOW)}


def test_unknown_for_bad_quality_and_duration() -> None:
    engine = EnsembleRegimeEngine(DeterministicRuleEngine(RegimeSettings()))
    unknown = engine.detect({"ma_slope_z": 3}, NOW, 0.5)
    assert unknown.primary_regime == Regime.UNKNOWN and unknown.confidence == 0
    first = engine.detect({"ma_slope_z": 3}, NOW + timedelta(minutes=1), 1)
    second = engine.detect({"ma_slope_z": 3}, NOW + timedelta(minutes=2), 1)
    assert first.primary_regime == Regime.TREND_UP and second.regime_duration_seconds == 60
    assert second.confidence <= 0.8


def test_gmm_is_reproducible_and_requires_fit() -> None:
    values = np.vstack(
        [
            np.random.default_rng(1).normal(-2, 0.2, (30, 2)),
            np.random.default_rng(2).normal(2, 0.2, (30, 2)),
        ]
    )
    one = GaussianMixtureRegimeModel(2).fit(values).predict(np.array([2.0, 2.0]))
    two = GaussianMixtureRegimeModel(2).fit(values).predict(np.array([2.0, 2.0]))
    assert one == two and sum(one.probabilities) == pytest.approx(1)
    with pytest.raises(RuntimeError):
        GaussianMixtureRegimeModel().predict(np.array([0.0, 0.0]))


def test_spot_perp_leadership_and_risk_off() -> None:
    rules = DeterministicRuleEngine(RegimeSettings())
    regimes = {
        item.regime
        for item in rules.evaluate(
            {
                "spot_return_zscore": -3.0,
                "perp_return_zscore": -1.0,
                "cross_asset_return_zscore": -3.0,
                "risk_breadth": -1.0,
            },
            NOW,
        )
    }
    assert Regime.SPOT_LED_MOVE in regimes
    assert Regime.RISK_OFF in regimes
