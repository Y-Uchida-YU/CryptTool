from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from app.domain.regimes.models import Regime, RegimeResult
from app.domain.strategies.base import StrategyContext
from app.domain.strategies.flash_crash import FlashCrashReversalStrategy
from app.domain.strategies.funding_extreme import FundingExtremeStrategy
from app.domain.strategies.mean_reversion import MeanReversionStrategy
from app.domain.strategies.models import (
    Signal,
    SignalIntent,
    SignalSide,
    StrategyBatchEvaluation,
    StrategyEvaluation,
    StrategyName,
)
from app.domain.strategies.relative_strength import PairStatistics, RelativeStrengthStrategy
from app.domain.strategies.trend_following import TrendFollowingStrategy
from app.services.strategy_engine import StrategyEngine

NOW = datetime(2024, 1, 1, tzinfo=UTC)


def regime(
    primary: Regime,
    *secondary: Regime,
    confidence: float = 0.90,
    quality: float = 0.95,
) -> RegimeResult:
    return RegimeResult(
        primary_regime=primary,
        secondary_regimes=secondary,
        confidence=confidence,
        evidence=("test evidence",),
        feature_snapshot={},
        regime_started_at=NOW - timedelta(hours=1),
        regime_duration_seconds=3600,
        model_version="test-v1",
        data_quality_score=quality,
    )


def context(
    features: Mapping[str, float | None],
    market_regime: RegimeResult,
    symbol: str = "BTC",
) -> StrategyContext:
    return StrategyContext(
        timestamp=NOW,
        symbol=symbol,
        features=features,
        regime=market_regime,
        exchange="test_exchange",
    )


def test_trend_following_is_regime_gated_and_deterministic() -> None:
    strategy = TrendFollowingStrategy()
    features = {
        "ma_slope_z": 2.0,
        "breakout_distance": 0.01,
        "adx": 30.0,
        "volume_zscore": 1.0,
        "oi_zscore": 0.5,
    }
    first = strategy.evaluate(context(features, regime(Regime.TREND_UP)))
    second = strategy.evaluate(context(features, regime(Regime.TREND_UP)))
    assert first.signal is not None
    assert first.signal.side == SignalSide.BUY
    assert first.signal.signal_id == second.signal.signal_id  # type: ignore[union-attr]
    assert first.signal.confidence <= first.signal.data_quality_score
    assert first.signal.evidence

    blocked = strategy.evaluate(context(features, regime(Regime.RANGE)))
    assert not blocked.gate_passed
    assert blocked.signal is None
    assert "required regime absent" in blocked.reasons[0]

    unhealthy = strategy.evaluate(
        context(
            features,
            regime(
                Regime.TREND_UP,
                Regime.RISK_OFF,
                confidence=0.59,
                quality=0.79,
            ),
        )
    )
    assert not unhealthy.gate_passed
    assert len(unhealthy.reasons) == 3


def test_trend_down_requires_directionally_consistent_breakout() -> None:
    strategy = TrendFollowingStrategy()
    rejected = strategy.evaluate(
        context(
            {"ma_slope_z": -2.0, "breakout_distance": 0.01, "adx": 30.0},
            regime(Regime.TREND_DOWN),
        )
    )
    assert rejected.gate_passed and rejected.signal is None
    accepted = strategy.evaluate(
        context(
            {"ma_slope_z": -2.0, "breakout_distance": -0.01, "adx": 30.0},
            regime(Regime.TREND_DOWN),
        )
    )
    assert accepted.signal is not None and accepted.signal.side == SignalSide.SELL
    contradictory = strategy.evaluate(
        context(
            {"ma_slope_z": 2.0, "breakout_distance": 0.01, "adx": 30.0},
            regime(Regime.TREND_UP, Regime.TREND_DOWN),
        )
    )
    assert contradictory.signal is None and "contradictory" in contradictory.reasons[0]


def test_mean_reversion_requires_range_deviation_and_liquidity_recovery() -> None:
    strategy = MeanReversionStrategy()
    features = {
        "return_zscore": -3.0,
        "ema_distance": -0.02,
        "spread_zscore": 0.5,
        "ma_slope_z": 0.1,
    }
    result = strategy.evaluate(context(features, regime(Regime.RANGE, Regime.LOW_VOLATILITY)))
    assert result.signal is not None and result.signal.side == SignalSide.BUY

    no_liquidity = strategy.evaluate(
        context({"return_zscore": 3.0, "ema_distance": 0.02}, regime(Regime.RANGE))
    )
    assert no_liquidity.signal is None
    assert any("liquidity" in reason for reason in no_liquidity.reasons)

    cascade = strategy.evaluate(context(features, regime(Regime.RANGE, Regime.LIQUIDATION_CASCADE)))
    assert not cascade.gate_passed and cascade.signal is None


def test_flash_crash_reversal_requires_shock_and_two_recovery_stages() -> None:
    strategy = FlashCrashReversalStrategy()
    shock = {
        "return_zscore": -4.0,
        "volume_zscore": 3.0,
        "long_liquidation_zscore": 3.5,
        "oi_zscore": -3.0,
        "spread_zscore": 3.0,
    }
    incomplete = strategy.evaluate(context(shock, regime(Regime.FLASH_CRASH)))
    assert incomplete.signal is None
    assert "0 recovery confirmations" in incomplete.reasons[0]

    confirmed = strategy.evaluate(
        context(
            {
                **shock,
                "liquidity_recovery": 0.5,
                "sell_pressure_change": -0.4,
                "cvd_momentum": 0.3,
            },
            regime(Regime.FLASH_CRASH, Regime.LIQUIDATION_CASCADE),
        )
    )
    assert confirmed.signal is not None
    assert confirmed.signal.side == SignalSide.BUY
    assert confirmed.signal.metadata["stages"] == (
        "shock",
        "liquidation_and_oi",
        "liquidity_recovery",
    )


def test_funding_extreme_is_not_naive_funding_contrarian() -> None:
    strategy = FundingExtremeStrategy()
    only_funding = {
        "funding_rate": 0.001,
        "predicted_funding_rate": 0.0012,
        "funding_zscore": 3.0,
    }
    rejected = strategy.evaluate(context(only_funding, regime(Regime.FUNDING_EXTREME_POSITIVE)))
    assert rejected.signal is None
    assert any("crowding" in reason for reason in rejected.reasons)

    evidence = {
        **only_funding,
        "funding_percentile": 0.995,
        "basis_zscore": 2.0,
        "long_short_ratio_zscore": 2.0,
        "oi_zscore": 1.5,
        "ma_slope_z": 0.1,
        "cvd_momentum": -0.5,
        "basis_momentum": -0.1,
    }
    accepted = strategy.evaluate(context(evidence, regime(Regime.FUNDING_EXTREME_POSITIVE)))
    assert accepted.signal is not None and accepted.signal.side == SignalSide.SELL
    assert len(accepted.signal.evidence) >= 7

    sign_mismatch = strategy.evaluate(
        context(
            {**evidence, "predicted_funding_rate": -0.001},
            regime(Regime.FUNDING_EXTREME_POSITIVE),
        )
    )
    assert sign_mismatch.signal is None
    contradictory = strategy.evaluate(
        context(
            evidence,
            regime(
                Regime.FUNDING_EXTREME_POSITIVE,
                Regime.FUNDING_EXTREME_NEGATIVE,
            ),
        )
    )
    assert contradictory.signal is None and "contradictory" in contradictory.reasons[0]


def test_relative_strength_uses_three_symbols_and_causal_pair_statistics() -> None:
    strategy = RelativeStrengthStrategy()
    features = {
        "BTC": {
            "vol_adjusted_return": 2.0,
            "momentum": 1.0,
            "drawdown": -0.02,
            "liquidity_score": 0.95,
            "funding_cost": 0.00001,
        },
        "ETH": {
            "vol_adjusted_return": 0.2,
            "momentum": 0.1,
            "drawdown": -0.10,
            "liquidity_score": 0.85,
            "funding_cost": 0.00002,
        },
        "SOL": {
            "vol_adjusted_return": -2.0,
            "momentum": -1.0,
            "drawdown": -0.35,
            "liquidity_score": 0.65,
            "funding_cost": 0.00010,
        },
    }
    regimes = {symbol: regime(Regime.RANGE) for symbol in features}
    pair = PairStatistics(
        symbol_a="BTC",
        symbol_b="SOL",
        estimated_at=NOW - timedelta(minutes=1),
        beta=1.5,
        correlation=0.8,
        cointegration_pvalue=0.04,
        spread_zscore=2.0,
        observations=500,
    )
    result = strategy.evaluate_universe(NOW, features, regimes, {("BTC", "SOL"): pair})
    assert result.gate_passed and len(result.signals) == 2
    assert [(signal.symbol, signal.side) for signal in result.signals] == [
        ("BTC", SignalSide.BUY),
        ("SOL", SignalSide.SELL),
    ]
    assert (
        result.signals[0].metadata["pair_group_id"] == result.signals[1].metadata["pair_group_id"]
    )
    weights = [float(signal.metadata["target_notional_weight"]) for signal in result.signals]
    assert sum(weights) == pytest.approx(1)

    future_pair = pair.model_copy(update={"estimated_at": NOW + timedelta(seconds=1)})
    rejected = strategy.evaluate_universe(NOW, features, regimes, {("BTC", "SOL"): future_pair})
    assert not rejected.signals
    assert "future-dated" in rejected.reasons[0]

    mismatched = pair.model_copy(update={"symbol_a": "ETH"})
    mismatch_result = strategy.evaluate_universe(
        NOW, features, regimes, {("BTC", "SOL"): mismatched}
    )
    assert not mismatch_result.signals
    assert "do not match" in mismatch_result.reasons[0]
    with pytest.raises(ValueError, match="timezone-aware"):
        strategy.evaluate_universe(datetime(2024, 1, 1), features, regimes, {("BTC", "SOL"): pair})

    missing_pair = strategy.evaluate_universe(NOW, features, regimes, {})
    assert not missing_pair.signals and "missing causal" in missing_pair.reasons[0]
    bad_pair = pair.model_copy(
        update={
            "estimated_at": NOW - timedelta(days=2),
            "correlation": 0.1,
            "cointegration_pvalue": 0.5,
            "beta": 5.0,
            "spread_zscore": 0.1,
        }
    )
    bad_result = strategy.evaluate_universe(NOW, features, regimes, {("BTC", "SOL"): bad_pair})
    combined_reasons = " ".join(bad_result.reasons)
    assert all(
        text in combined_reasons
        for text in ("stale", "correlation", "cointegration", "beta", "spread")
    )


def test_relative_strength_explains_unusable_symbols_and_statistics() -> None:
    strategy = RelativeStrengthStrategy()
    complete = {
        "vol_adjusted_return": 1.0,
        "momentum": 1.0,
        "drawdown": -0.1,
        "liquidity_score": 0.8,
        "funding_cost": 0.0,
    }
    result = strategy.evaluate_universe(
        NOW,
        {
            "BTC": complete,
            "ETH": {**complete, "liquidity_score": 2.0},
            "SOL": {key: value for key, value in complete.items() if key != "momentum"},
            "DOGE": complete,
        },
        {
            "ETH": regime(Regime.RANGE),
            "SOL": regime(Regime.RANGE),
            "DOGE": regime(Regime.RISK_OFF),
        },
        {},
    )
    assert not result.gate_passed
    reason = result.reasons[0]
    assert all(
        text in reason
        for text in ("missing regime", "liquidity_score", "missing momentum", "blocking regime")
    )

    with pytest.raises(ValidationError, match="timezone-aware"):
        PairStatistics(
            symbol_a="BTC",
            symbol_b="ETH",
            estimated_at=datetime(2024, 1, 1),
            beta=1,
            correlation=0.8,
            cointegration_pvalue=0.05,
            spread_zscore=1,
            observations=100,
        )
    with pytest.raises(ValidationError, match="finite"):
        PairStatistics(
            symbol_a="BTC",
            symbol_b="ETH",
            estimated_at=NOW,
            beta=1,
            correlation=0.8,
            cointegration_pvalue=0.05,
            spread_zscore=float("inf"),
            observations=100,
        )


def test_strategy_engine_retains_each_no_signal_decision() -> None:
    engine = StrategyEngine((TrendFollowingStrategy(), MeanReversionStrategy()))
    decisions = engine.evaluate(
        context(
            {"return_zscore": 0.1, "ema_distance": 0.0, "spread_zscore": 0.0},
            regime(Regime.RANGE),
        )
    )
    assert len(decisions) == 2
    assert all(decision.signal is None and decision.reasons for decision in decisions)


def test_strategy_engine_guards_duplicate_and_optional_universe_strategy() -> None:
    with pytest.raises(ValueError, match="unique"):
        StrategyEngine((TrendFollowingStrategy(), TrendFollowingStrategy()))
    with pytest.raises(RuntimeError, match="not configured"):
        StrategyEngine(()).evaluate_relative_strength(NOW, {}, {}, {})
    result = StrategyEngine((), RelativeStrengthStrategy()).evaluate_relative_strength(
        NOW, {}, {}, {}
    )
    assert not result.gate_passed and not result.signals


def test_signal_and_evaluation_contracts_reject_ambiguous_or_unsafe_values() -> None:
    generated = (
        TrendFollowingStrategy()
        .evaluate(
            context(
                {
                    "ma_slope_z": 2.0,
                    "breakout_distance": 0.01,
                    "adx": 30.0,
                },
                regime(Regime.TREND_UP),
            )
        )
        .signal
    )
    assert generated is not None
    payload = generated.model_dump()
    invalid_payloads = (
        {**payload, "timestamp": datetime(2024, 1, 1)},
        {**payload, "feature_snapshot": {"bad": float("nan")}},
        {**payload, "valid_until": payload["timestamp"]},
        {**payload, "side": SignalSide.FLAT},
        {**payload, "strength": 0.0},
    )
    for invalid in invalid_payloads:
        with pytest.raises(ValidationError):
            Signal.model_validate(invalid)

    with pytest.raises(ValidationError, match="timezone-aware"):
        StrategyEvaluation(
            timestamp=datetime(2024, 1, 1),
            strategy=StrategyName.TREND_FOLLOWING,
            symbol="BTC",
            gate_passed=True,
            reasons=("test",),
        )
    with pytest.raises(ValidationError, match="closed regime gate"):
        StrategyEvaluation(
            timestamp=NOW,
            strategy=StrategyName.TREND_FOLLOWING,
            symbol="BTC",
            gate_passed=False,
            signal=generated,
            reasons=("test",),
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        StrategyBatchEvaluation(
            timestamp=datetime(2024, 1, 1),
            strategy=StrategyName.RELATIVE_STRENGTH,
            gate_passed=True,
            reasons=("test",),
        )
    with pytest.raises(ValidationError, match="closed regime gate"):
        StrategyBatchEvaluation(
            timestamp=NOW,
            strategy=StrategyName.RELATIVE_STRENGTH,
            gate_passed=False,
            signals=(generated,),
            reasons=("test",),
        )


def test_reducing_flat_signal_has_unambiguous_contract() -> None:
    flat = Signal(
        signal_id="b" * 64,
        timestamp=NOW,
        strategy=StrategyName.MEAN_REVERSION,
        symbol="BTC",
        side=SignalSide.FLAT,
        intent=SignalIntent.EXIT,
        strength=0,
        confidence=1,
        primary_regime=Regime.RANGE,
        data_quality_score=1,
        valid_until=NOW + timedelta(minutes=1),
        evidence=("close existing position",),
        feature_snapshot={},
    )
    assert flat.side == SignalSide.FLAT and flat.intent == SignalIntent.EXIT
