import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from app.domain.features.engine import FeatureEngine, rolling_zscore


def fixture(rows: int = 40) -> pd.DataFrame:
    close = np.arange(100.0, 100.0 + rows)
    return pd.DataFrame(
        {
            "open": close - 0.5,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": np.arange(1.0, rows + 1),
            "buy_volume": 6.0,
            "sell_volume": 4.0,
            "open_interest": np.arange(200.0, 200.0 + rows),
            "funding_rate": np.arange(rows) / 10000,
            "spot_close": close,
            "perp_close": close * 1.01,
            "bid_depth": 60.0,
            "ask_depth": 40.0,
            "best_bid": close - 0.1,
            "best_ask": close + 0.1,
            "liquidation_volume": 2.0,
        }
    )


def test_hand_calculable_features() -> None:
    result = FeatureEngine(window=5, annualization=1).build(fixture())
    assert np.isclose(result.loc[1, "log_return"], np.log(101 / 100))
    assert result.loc[13, "atr"] == 2
    assert result.loc[39, "buy_volume_ratio"] == 0.6
    assert result.loc[39, "taker_imbalance"] == 0.2
    assert result.loc[39, "cvd"] == 80
    assert result.loc[39, "cvd_momentum"] == 6
    assert result.loc[39, "oi_change"] == 1
    assert np.isclose(result.loc[39, "basis"], 0.01)
    assert result.loc[39, "book_imbalance"] == 0.2
    assert np.isclose(result.loc[39, "microprice"], result.loc[39, "close"] + 0.02)
    assert np.isclose(result.loc[39, "liquidation_ratio"], 2 / 40)
    assert result.loc[39, "funding_momentum"] == pytest.approx(0.0001)


def test_zscore_excludes_current_value() -> None:
    values = pd.Series([1.0, 2.0, 3.0, 100.0])
    assert np.isclose(rolling_zscore(values, 3).iloc[-1], (100 - 2) / 1)


def test_features_do_not_change_when_future_is_mutated() -> None:
    source = fixture()
    baseline = FeatureEngine(window=5).build(source)
    source.loc[30:, "close"] *= 10
    changed = FeatureEngine(window=5).build(source)
    pdt.assert_frame_equal(baseline.loc[:29], changed.loc[:29])


def test_missing_optional_inputs_are_not_zero_filled() -> None:
    output = FeatureEngine(window=5).build(fixture().drop(columns=["open_interest"]))
    assert "oi_change" not in output
    assert output["realized_volatility"].iloc[:5].isna().all()
    assert "ma_slope_z" in output and "volatility_zscore" in output


def test_quality_report_discloses_missing_outliers_correlation_and_stability() -> None:
    report = FeatureEngine(window=5).quality_report(FeatureEngine(window=5).build(fixture()))
    assert {
        "missing_ratio",
        "infinite_count",
        "outlier_ratio",
        "stability_shift",
        "maximum_absolute_correlation",
    } <= set(report.columns)


def test_feature_availability_names_missing_inputs_without_zero_filling() -> None:
    availability = FeatureEngine(window=5).availability(fixture().drop(columns=["bid_depth"]))
    assert not bool(availability.loc["microprice", "available"])
    assert "bid_depth" in availability.loc["microprice", "missing_inputs"]
    assert availability.loc["microprice", "missing_policy"] == "propagate_nan"
