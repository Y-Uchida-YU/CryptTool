from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FeatureRequirement:
    inputs: tuple[str, ...]
    minimum_history: int
    missing_policy: str = "propagate_nan"


FEATURE_REQUIREMENTS = {
    "log_return": FeatureRequirement(("close",), 2),
    "atr": FeatureRequirement(("high", "low", "close"), 15),
    "realized_volatility": FeatureRequirement(("close",), 31),
    "parkinson_volatility": FeatureRequirement(("high", "low"), 30),
    "garman_klass_volatility": FeatureRequirement(("open", "high", "low", "close"), 30),
    "volume_zscore": FeatureRequirement(("volume",), 30),
    "oi_change": FeatureRequirement(("open_interest",), 2),
    "funding_zscore": FeatureRequirement(("funding_rate",), 30),
    "basis": FeatureRequirement(("spot_close", "perp_close"), 1),
    "book_imbalance": FeatureRequirement(("bid_depth", "ask_depth"), 1),
    "liquidation_ratio": FeatureRequirement(("liquidation_volume", "volume"), 1),
    "vwap_distance": FeatureRequirement(("high", "low", "close", "volume"), 30),
    "adx": FeatureRequirement(("high", "low", "close"), 28),
    "cvd": FeatureRequirement(("buy_volume", "sell_volume"), 1),
    "funding_momentum": FeatureRequirement(("funding_rate",), 2),
    "basis_momentum": FeatureRequirement(("spot_close", "perp_close"), 2),
    "microprice": FeatureRequirement(("best_bid", "best_ask", "bid_depth", "ask_depth"), 1),
    "liquidity_recovery": FeatureRequirement(("bid_depth", "ask_depth"), 2),
    "mark_index_premium": FeatureRequirement(("mark_price", "index_price"), 1),
}


def rolling_zscore(series: pd.Series, window: int, minimum: int | None = None) -> pd.Series:
    shifted = series.shift(1)
    mean = shifted.rolling(window, min_periods=minimum or window).mean()
    std = shifted.rolling(window, min_periods=minimum or window).std(ddof=1)
    return (series - mean) / std.replace(0, np.nan)


class FeatureEngine:
    """Causal features: rolling reference distributions exclude the current row."""

    def __init__(self, window: int = 30, annualization: float = 365 * 24 * 60) -> None:
        if window < 2:
            raise ValueError("window must be >= 2")
        self.window, self.annualization = window, annualization

    def build(self, source: pd.DataFrame) -> pd.DataFrame:
        data = source.sort_index().copy()
        close = data["close"].astype(float)
        data["log_return"] = np.log(close / close.shift(1))
        data["return_5"] = close.pct_change(5, fill_method=None)
        prior_close = close.shift(1)
        true_range = pd.concat(
            [
                (data["high"] - data["low"]).abs(),
                (data["high"] - prior_close).abs(),
                (data["low"] - prior_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        data["atr"] = true_range.rolling(14, min_periods=14).mean()
        upward = data["high"].diff()
        downward = -data["low"].diff()
        plus_dm = upward.where((upward > downward) & (upward > 0), 0.0)
        minus_dm = downward.where((downward > upward) & (downward > 0), 0.0)
        smoothed_range = true_range.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
        plus_di = 100 * plus_dm.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
        minus_di = 100 * minus_dm.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
        plus_di = plus_di / smoothed_range.replace(0, np.nan)
        minus_di = minus_di / smoothed_range.replace(0, np.nan)
        directional_sum = (plus_di + minus_di).replace(0, np.nan)
        data["adx"] = (
            (100 * (plus_di - minus_di).abs() / directional_sum)
            .ewm(alpha=1 / 14, adjust=False, min_periods=14)
            .mean()
        )
        data["realized_volatility"] = data["log_return"].shift(1).rolling(
            self.window, min_periods=self.window
        ).std(ddof=1) * np.sqrt(self.annualization)
        hl = pd.Series(np.log(data["high"] / data["low"]) ** 2, index=data.index)
        data["parkinson_volatility"] = np.sqrt(
            hl.shift(1).rolling(self.window, min_periods=self.window).mean()
            / (4 * np.log(2))
            * self.annualization
        )
        co = pd.Series(np.log(data["close"] / data["open"]) ** 2, index=data.index)
        gk = pd.Series(0.5 * hl - (2 * np.log(2) - 1) * co, index=data.index)
        data["garman_klass_volatility"] = np.sqrt(
            gk.clip(lower=0).shift(1).rolling(self.window, min_periods=self.window).mean()
            * self.annualization
        )
        ema = close.ewm(span=20, adjust=False).mean()
        data["ema_distance"] = close / ema - 1
        data["ma_slope"] = ema.pct_change(5, fill_method=None)
        data["ma_slope_z"] = rolling_zscore(data["ma_slope"], self.window)
        data["volatility_zscore"] = rolling_zscore(data["realized_volatility"], self.window)
        rolling_high = data["high"].shift(1).rolling(self.window).max()
        rolling_low = data["low"].shift(1).rolling(self.window).min()
        data["breakout_distance"] = np.where(
            close >= rolling_high, close / rolling_high - 1, close / rolling_low - 1
        )
        channel_width = (rolling_high - rolling_low).replace(0, np.nan)
        data["donchian_position"] = (close - rolling_low) / channel_width
        mean = close.shift(1).rolling(20).mean()
        std = close.shift(1).rolling(20).std(ddof=1)
        data["bollinger_width"] = 4 * std / mean
        data["bollinger_position"] = (close - (mean - 2 * std)) / (4 * std).replace(0, np.nan)
        data["return_zscore"] = rolling_zscore(data["log_return"], self.window)
        data["volume_zscore"] = rolling_zscore(data["volume"].astype(float), self.window)
        lagged_volume_mean = data["volume"].shift(1).rolling(self.window).mean()
        data["relative_volume"] = data["volume"] / lagged_volume_mean.replace(0, np.nan)
        typical_price = (data["high"] + data["low"] + close) / 3
        rolling_notional = (typical_price * data["volume"]).rolling(self.window).sum()
        rolling_volume = data["volume"].rolling(self.window).sum()
        data["rolling_vwap"] = rolling_notional / rolling_volume.replace(0, np.nan)
        data["vwap_distance"] = close / data["rolling_vwap"] - 1
        data["drawdown"] = close / close.cummax() - 1
        data["recovery_rate"] = data["drawdown"].diff()
        data["momentum"] = data["return_5"]
        per_period_volatility = data["realized_volatility"] / np.sqrt(self.annualization)
        data["vol_adjusted_return"] = data["return_5"] / (
            per_period_volatility * np.sqrt(5)
        ).replace(0, np.nan)
        self._optional_features(data)
        return data

    def availability(self, source: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        columns = set(source.columns)
        for name, requirement in FEATURE_REQUIREMENTS.items():
            missing = tuple(field for field in requirement.inputs if field not in columns)
            rows.append(
                {
                    "feature": name,
                    "available": not missing and len(source) >= requirement.minimum_history,
                    "missing_inputs": ",".join(missing),
                    "minimum_history": requirement.minimum_history,
                    "observations": len(source),
                    "missing_policy": requirement.missing_policy,
                }
            )
        return pd.DataFrame(rows).set_index("feature")

    def _optional_features(self, data: pd.DataFrame) -> None:
        if "buy_volume" in data and "sell_volume" in data:
            total = data["buy_volume"] + data["sell_volume"]
            data["buy_volume_ratio"] = data["buy_volume"] / total.replace(0, np.nan)
            data["taker_imbalance"] = (data["buy_volume"] - data["sell_volume"]) / total.replace(
                0, np.nan
            )
            data["cvd"] = (data["buy_volume"] - data["sell_volume"]).cumsum()
            data["cvd_momentum"] = data["cvd"].diff(3)
            data["sell_pressure_change"] = data["sell_volume"].pct_change(fill_method=None)
        if "open_interest" in data:
            data["oi_change"] = data["open_interest"].diff()
            data["oi_pct_change"] = data["open_interest"].pct_change(fill_method=None)
            data["oi_zscore"] = rolling_zscore(data["oi_pct_change"], self.window)
            data["oi_expansion_rate"] = data["oi_pct_change"].clip(lower=0)
            data["oi_contraction_rate"] = (-data["oi_pct_change"]).clip(lower=0)
            data["oi_volume_ratio"] = data["open_interest"] / data["volume"].replace(0, np.nan)
        if "funding_rate" in data:
            data["funding_zscore"] = rolling_zscore(data["funding_rate"], self.window)
            data["funding_percentile"] = (
                data["funding_rate"]
                .rolling(self.window + 1)
                .apply(lambda values: float(np.mean(values[:-1] <= values[-1])), raw=True)
            )
            data["funding_momentum"] = data["funding_rate"].diff()
            data["funding_reversal"] = (
                np.sign(data["funding_rate"]) != np.sign(data["funding_rate"].shift(1))
            ).astype(float)
            data["funding_cost"] = data["funding_rate"]
        if "predicted_funding_rate" in data:
            data["predicted_funding_zscore"] = rolling_zscore(
                data["predicted_funding_rate"], self.window
            )
        if "spot_close" in data and "perp_close" in data:
            data["basis"] = data["perp_close"] / data["spot_close"] - 1
            data["basis_zscore"] = rolling_zscore(data["basis"], self.window)
            data["annualized_basis"] = data["basis"] * 365 * 3
            data["basis_momentum"] = data["basis"].diff()
            data["spot_perp_premium_zscore"] = data["basis_zscore"]
        if "bid_depth" in data and "ask_depth" in data:
            depth = data["bid_depth"] + data["ask_depth"]
            data["book_imbalance"] = (data["bid_depth"] - data["ask_depth"]) / depth.replace(
                0, np.nan
            )
            data["depth_depletion"] = -depth.pct_change(fill_method=None)
            data["liquidity_recovery"] = depth.pct_change(fill_method=None)
        if "best_bid" in data and "best_ask" in data:
            midpoint = (data["best_bid"] + data["best_ask"]) / 2
            data["spread"] = (data["best_ask"] - data["best_bid"]) / midpoint
            data["spread_zscore"] = rolling_zscore(data["spread"], self.window)
            data["spread_recovery_ratio"] = -data["spread"].pct_change(fill_method=None)
            if "bid_depth" in data and "ask_depth" in data:
                total_depth = (data["bid_depth"] + data["ask_depth"]).replace(0, np.nan)
                data["microprice"] = (
                    data["best_ask"] * data["bid_depth"] + data["best_bid"] * data["ask_depth"]
                ) / total_depth
                data["weighted_mid_price"] = data["microprice"]
                data["slippage_estimate"] = data["spread"] / 2 + 1 / total_depth
                log_depth = pd.Series(np.log1p(total_depth), index=data.index)
                depth_z = rolling_zscore(log_depth, self.window)
                data["liquidity_score"] = 1 / (1 + np.exp(-(depth_z - data["spread_zscore"])))
        if "liquidation_volume" in data:
            data["liquidation_ratio"] = data["liquidation_volume"] / data["volume"].replace(
                0, np.nan
            )
            data["liquidation_zscore"] = rolling_zscore(data["liquidation_volume"], self.window)
            data["liquidation_acceleration"] = data["liquidation_volume"].diff().diff()
            data["liquidation_clustering"] = (
                (data["liquidation_zscore"] > 2).astype(float).rolling(5).sum()
            )
        if "long_liquidation_volume" in data:
            data["long_liquidation_zscore"] = rolling_zscore(
                data["long_liquidation_volume"], self.window
            )
        if "short_liquidation_volume" in data:
            data["short_liquidation_zscore"] = rolling_zscore(
                data["short_liquidation_volume"], self.window
            )
        if "mark_price" in data and "index_price" in data:
            data["mark_index_premium"] = data["mark_price"] / data["index_price"] - 1
        if "long_short_ratio" in data:
            data["long_short_ratio_zscore"] = rolling_zscore(data["long_short_ratio"], self.window)

    def quality_report(self, features: pd.DataFrame) -> pd.DataFrame:
        numeric = features.select_dtypes(include=[np.number])
        correlation = numeric.corr().abs()
        if not correlation.empty:
            correlation = correlation.mask(np.eye(len(correlation), dtype=bool))
        midpoint = max(1, len(numeric) // 2)
        scale = numeric.std().replace(0, np.nan)
        stability_shift = (
            numeric.iloc[:midpoint].mean() - numeric.iloc[midpoint:].mean()
        ).abs() / scale
        median = numeric.median()
        median_deviation = (numeric - median).abs().median().replace(0, np.nan)
        robust_z = (numeric - median).abs() / (1.4826 * median_deviation)
        return pd.DataFrame(
            {
                "missing_ratio": numeric.isna().mean(),
                "infinite_count": np.isinf(numeric).sum(),
                "unique_count": numeric.nunique(),
                "std": numeric.std(),
                "outlier_ratio": (robust_z > 6).mean(),
                "stability_shift": stability_shift,
                "maximum_absolute_correlation": correlation.max(),
            }
        )
