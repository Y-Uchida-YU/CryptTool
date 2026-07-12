"""Performance metrics with explicit undefined values and cost accounting."""

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, cast

import numpy as np
import pandas as pd
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]


@dataclass(frozen=True)
class PerformanceMetrics:
    """Complete strategy metrics; ``None`` means unavailable or undefined, never zero."""

    total_return: float
    cagr: float | None
    annualized_volatility: float | None
    sharpe_ratio: float | None
    sortino_ratio: float | None
    calmar_ratio: float | None
    maximum_drawdown: float
    profit_factor: float | None
    win_rate: float | None
    payoff_ratio: float | None
    expectancy: float | None
    average_trade: float | None
    median_trade: float | None
    average_holding_time_seconds: float | None
    turnover: float | None
    fee_ratio: float | None
    funding_pnl: float | None
    slippage_cost: float | None
    market_impact_cost: float | None
    exposure: float | None
    time_in_market: float | None
    consecutive_wins: int
    consecutive_losses: int
    tail_loss: float
    value_at_risk: float
    conditional_value_at_risk: float
    ulcer_index: float
    recovery_factor: float | None
    trade_count: int
    fee_cost: float | None
    net_pnl: float

    def to_dict(self) -> dict[str, float | int | None]:
        """Return stable snake-case names for JSON and CSV reports."""

        return asdict(self)


def _series(values: Sequence[float] | FloatArray | pd.Series, name: str) -> pd.Series:
    if isinstance(values, pd.Series):
        result = pd.to_numeric(values.copy(), errors="coerce").astype(float)
    else:
        result = pd.Series(np.asarray(values, dtype=np.float64), dtype=float)
    if (
        result.empty
        or result.isna().any()
        or not np.isfinite(result.to_numpy(dtype=np.float64)).all()
    ):
        raise ValueError(f"{name} must be a non-empty finite one-dimensional sequence")
    return result


def _derive_returns(equity: pd.Series) -> pd.Series:
    values = equity.to_numpy(dtype=np.float64)
    if values.size < 2:
        raise ValueError("equity_curve requires at least two observations")
    if np.any(values < 0):
        raise ValueError("equity_curve cannot contain negative balances")
    previous, current = values[:-1], values[1:]
    if np.any((previous == 0) & (current > 0)):
        raise ValueError("equity cannot recover from zero without an external cash flow")
    derived = (
        np.divide(
            current,
            previous,
            out=np.ones_like(current),
            where=previous > 0,
        )
        - 1
    )
    return pd.Series(derived, index=equity.index[1:], name="return")


def drawdown_series(equity_curve: Sequence[float] | FloatArray | pd.Series) -> pd.Series:
    """Return signed drawdowns (zero at peaks, negative below peaks)."""

    equity = _series(equity_curve, "equity_curve")
    if np.any(equity.to_numpy(dtype=np.float64) < 0):
        raise ValueError("equity_curve cannot contain negative balances")
    running_peak = equity.cummax()
    drawdown = equity.divide(running_peak.where(running_peak > 0, np.nan)) - 1
    drawdown = drawdown.fillna(0.0)
    drawdown.name = "drawdown"
    return cast(pd.Series, drawdown)


def _column(frame: pd.DataFrame, names: Sequence[str]) -> pd.Series | None:
    for name in names:
        if name in frame.columns:
            result = pd.to_numeric(frame[name], errors="coerce")
            if result.isna().any() or not np.isfinite(result.to_numpy(dtype=np.float64)).all():
                raise ValueError(f"trade column {name!r} must contain only finite numeric values")
            return result.astype(float)
    return None


def _pnl_column(trades: pd.DataFrame) -> pd.Series:
    pnl = _column(trades, ("net_pnl", "pnl", "realized_pnl"))
    if pnl is None:
        raise ValueError("trades require one of: net_pnl, pnl, realized_pnl")
    return pnl


def _holding_seconds(trades: pd.DataFrame) -> pd.Series | None:
    duration = _column(trades, ("holding_time_seconds", "duration_seconds"))
    if duration is not None:
        if (duration < 0).any():
            raise ValueError("holding duration cannot be negative")
        return duration
    if "entry_time" not in trades or "exit_time" not in trades:
        return None
    entry = pd.to_datetime(trades["entry_time"], utc=True, errors="coerce")
    exit_time = pd.to_datetime(trades["exit_time"], utc=True, errors="coerce")
    if entry.isna().any() or exit_time.isna().any():
        raise ValueError("trade entry_time and exit_time must be valid timestamps")
    seconds = (exit_time - entry).dt.total_seconds()
    if (seconds < 0).any():
        raise ValueError("trade exit_time cannot precede entry_time")
    return seconds


def _maximum_run(signs: NDArray[np.bool_]) -> int:
    longest = current = 0
    for value in signs:
        current = current + 1 if value else 0
        longest = max(longest, current)
    return longest


def calculate_performance_metrics(
    equity_curve: Sequence[float] | FloatArray | pd.Series,
    *,
    returns: Sequence[float] | FloatArray | pd.Series | None = None,
    trades: pd.DataFrame | Sequence[Mapping[str, Any]] | None = None,
    exposure_series: Sequence[float] | FloatArray | pd.Series | None = None,
    periods_per_year: float = 365.0,
    annual_risk_free_rate: float = 0.0,
    var_confidence: float = 0.95,
) -> PerformanceMetrics:
    """Calculate return, risk, execution-cost and trade-distribution metrics.

    Equity and returns must already include all cash flows. Costs are reported from
    trade fields and are not subtracted a second time. ``periods_per_year`` must
    match the supplied return frequency (365 for daily crypto observations).
    """

    equity = _series(equity_curve, "equity_curve")
    equity_values = equity.to_numpy(dtype=np.float64)
    if equity_values.size < 2 or equity_values[0] <= 0 or np.any(equity_values < 0):
        raise ValueError(
            "equity_curve needs at least two values, a positive start, and no negatives"
        )
    if isinstance(equity.index, pd.DatetimeIndex) and (
        equity.index.tz is None
        or not equity.index.is_monotonic_increasing
        or equity.index.has_duplicates
    ):
        raise ValueError("equity timestamps must be timezone-aware, unique and chronological")
    period_returns = _derive_returns(equity) if returns is None else _series(returns, "returns")
    return_values = period_returns.to_numpy(dtype=np.float64)
    if np.any(return_values < -1):
        raise ValueError("simple returns cannot be below -100%")
    if not np.isfinite(periods_per_year) or periods_per_year <= 0:
        raise ValueError("periods_per_year must be finite and positive")
    if not np.isfinite(annual_risk_free_rate):
        raise ValueError("annual_risk_free_rate must be finite")
    if not 0.5 < var_confidence < 1:
        raise ValueError("var_confidence must be between 0.5 and one")

    total_return = float(equity_values[-1] / equity_values[0] - 1)
    elapsed_years: float
    if isinstance(equity.index, pd.DatetimeIndex):
        elapsed_seconds = (equity.index[-1] - equity.index[0]).total_seconds()
        elapsed_years = elapsed_seconds / (365.2425 * 24 * 60 * 60)
    else:
        elapsed_years = return_values.size / periods_per_year
    if elapsed_years <= 0:
        cagr = None
    elif equity_values[-1] == 0:
        cagr = -1.0
    else:
        cagr = float((equity_values[-1] / equity_values[0]) ** (1 / elapsed_years) - 1)

    standard_deviation = float(np.std(return_values, ddof=1)) if return_values.size > 1 else 0.0
    annualized_volatility = (
        standard_deviation * float(np.sqrt(periods_per_year)) if return_values.size > 1 else None
    )
    excess = return_values - annual_risk_free_rate / periods_per_year
    sharpe = (
        float(np.mean(excess) / standard_deviation * np.sqrt(periods_per_year))
        if standard_deviation > np.finfo(np.float64).eps
        else None
    )
    downside = np.minimum(excess, 0.0)
    downside_deviation = float(np.sqrt(np.mean(downside**2)))
    sortino = (
        float(np.mean(excess) / downside_deviation * np.sqrt(periods_per_year))
        if downside_deviation > np.finfo(np.float64).eps
        else None
    )
    drawdowns = drawdown_series(equity)
    maximum_drawdown = float(max(0.0, -drawdowns.min()))
    ulcer_index = float(np.sqrt(np.mean(np.square(drawdowns.to_numpy(dtype=np.float64)))))
    calmar = cagr / maximum_drawdown if cagr is not None and maximum_drawdown > 0 else None
    recovery = total_return / maximum_drawdown if maximum_drawdown > 0 else None

    quantile = float(np.quantile(return_values, 1 - var_confidence))
    tail = return_values[return_values <= quantile]
    value_at_risk = float(max(0.0, -quantile))
    conditional_value_at_risk = float(max(0.0, -np.mean(tail)))
    tail_loss = float(max(0.0, -np.min(return_values)))

    trade_frame = pd.DataFrame(trades).copy() if trades is not None else pd.DataFrame()
    if trade_frame.empty:
        trade_count = 0
        profit_factor = win_rate = payoff = expectancy = average_trade = median_trade = None
        average_holding = turnover = fee_ratio = None
        funding_pnl = slippage_cost = market_impact_cost = fee_cost = None
        consecutive_wins = consecutive_losses = 0
    else:
        pnl = _pnl_column(trade_frame)
        pnl_values = pnl.to_numpy(dtype=np.float64)
        trade_count = int(pnl_values.size)
        winners = pnl_values[pnl_values > 0]
        losers = pnl_values[pnl_values < 0]
        gross_wins = float(np.sum(winners))
        gross_losses = float(-np.sum(losers))
        profit_factor = gross_wins / gross_losses if gross_losses > 0 else None
        win_rate = float(np.mean(pnl_values > 0))
        payoff = (
            float(np.mean(winners) / -np.mean(losers)) if winners.size and losers.size else None
        )
        expectancy = average_trade = float(np.mean(pnl_values))
        median_trade = float(np.median(pnl_values))
        holding = _holding_seconds(trade_frame)
        average_holding = float(holding.mean()) if holding is not None else None
        notionals = _column(trade_frame, ("turnover", "notional", "filled_notional"))
        average_equity = float(np.mean(equity_values))
        turnover = (
            float(np.sum(np.abs(notionals.to_numpy(dtype=np.float64))) / average_equity)
            if notionals is not None and average_equity > 0
            else None
        )
        fees = _column(trade_frame, ("fee", "fees", "fee_cost"))
        funding = _column(trade_frame, ("funding_pnl", "funding"))
        slippage = _column(trade_frame, ("slippage_cost", "slippage"))
        impact = _column(trade_frame, ("market_impact_cost", "impact_cost"))
        fee_cost = float(np.sum(fees)) if fees is not None else None
        funding_pnl = float(np.sum(funding)) if funding is not None else None
        slippage_cost = float(np.sum(slippage)) if slippage is not None else None
        market_impact_cost = float(np.sum(impact)) if impact is not None else None
        gross_pnl = _column(trade_frame, ("gross_pnl",))
        fee_denominator_source = gross_pnl if gross_pnl is not None else pnl
        fee_denominator = float(np.sum(np.abs(fee_denominator_source)))
        fee_ratio = (
            fee_cost / fee_denominator if fee_cost is not None and fee_denominator > 0 else None
        )
        consecutive_wins = _maximum_run(pnl_values > 0)
        consecutive_losses = _maximum_run(pnl_values < 0)

    if exposure_series is None:
        exposure = time_in_market = None
    else:
        exposure_values = _series(exposure_series, "exposure_series").to_numpy(dtype=np.float64)
        if np.any(exposure_values < 0):
            raise ValueError("exposure_series must contain non-negative gross exposure")
        exposure = float(np.mean(exposure_values))
        time_in_market = float(np.mean(exposure_values > 0))

    return PerformanceMetrics(
        total_return=total_return,
        cagr=cagr,
        annualized_volatility=annualized_volatility,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        maximum_drawdown=maximum_drawdown,
        profit_factor=profit_factor,
        win_rate=win_rate,
        payoff_ratio=payoff,
        expectancy=expectancy,
        average_trade=average_trade,
        median_trade=median_trade,
        average_holding_time_seconds=average_holding,
        turnover=turnover,
        fee_ratio=fee_ratio,
        funding_pnl=funding_pnl,
        slippage_cost=slippage_cost,
        market_impact_cost=market_impact_cost,
        exposure=exposure,
        time_in_market=time_in_market,
        consecutive_wins=consecutive_wins,
        consecutive_losses=consecutive_losses,
        tail_loss=tail_loss,
        value_at_risk=value_at_risk,
        conditional_value_at_risk=conditional_value_at_risk,
        ulcer_index=ulcer_index,
        recovery_factor=recovery,
        trade_count=trade_count,
        fee_cost=fee_cost,
        net_pnl=float(equity_values[-1] - equity_values[0]),
    )


def aggregate_trade_performance(
    trades: pd.DataFrame | Sequence[Mapping[str, Any]],
    *,
    dimensions: Sequence[str] = ("regime", "strategy", "symbol"),
) -> dict[str, pd.DataFrame]:
    """Return complete group tables, including losing and zero-PnL groups."""

    frame = pd.DataFrame(trades).copy()
    if frame.empty:
        return {dimension: pd.DataFrame() for dimension in dimensions}
    pnl = _pnl_column(frame)
    frame = frame.assign(_report_pnl=pnl)
    cost_aliases: dict[str, tuple[str, ...]] = {
        "fee_cost": ("fee", "fees", "fee_cost"),
        "funding_pnl": ("funding_pnl", "funding"),
        "slippage_cost": ("slippage_cost", "slippage"),
        "market_impact_cost": ("market_impact_cost", "impact_cost"),
    }
    output: dict[str, pd.DataFrame] = {}
    for dimension in dimensions:
        if dimension not in frame:
            output[dimension] = pd.DataFrame()
            continue
        rows: list[dict[str, float | int | str | None]] = []
        for group_name, group in frame.groupby(dimension, dropna=False, sort=True):
            values = group["_report_pnl"].to_numpy(dtype=np.float64)
            winners, losers = values[values > 0], values[values < 0]
            gross_loss = float(-np.sum(losers))
            row: dict[str, float | int | str | None] = {
                dimension: str(group_name),
                "trade_count": int(values.size),
                "total_pnl": float(np.sum(values)),
                "gross_profit": float(np.sum(winners)),
                "gross_loss": gross_loss,
                "win_rate": float(np.mean(values > 0)),
                "profit_factor": float(np.sum(winners) / gross_loss) if gross_loss > 0 else None,
                "expectancy": float(np.mean(values)),
                "average_trade": float(np.mean(values)),
                "median_trade": float(np.median(values)),
            }
            for output_name, aliases in cost_aliases.items():
                cost = _column(group, aliases)
                row[output_name] = float(np.sum(cost)) if cost is not None else None
            rows.append(row)
        output[dimension] = pd.DataFrame(rows)
    return output


def monthly_returns(equity_curve: pd.Series) -> pd.DataFrame:
    """Calculate calendar-month returns from a timezone-aware equity curve."""

    equity = _series(equity_curve, "equity_curve")
    if not isinstance(equity.index, pd.DatetimeIndex):
        raise ValueError("monthly_returns requires a DatetimeIndex")
    if equity.index.tz is None:
        raise ValueError("monthly_returns requires timezone-aware timestamps")
    if not equity.index.is_monotonic_increasing or equity.index.has_duplicates:
        raise ValueError("equity timestamps must be unique and chronological")
    month_end = equity.resample("ME").last()
    values = month_end.pct_change()
    values.iloc[0] = month_end.iloc[0] / equity.iloc[0] - 1
    return pd.DataFrame({"month": values.index, "return": values.to_numpy(dtype=np.float64)})


def regime_distribution(regimes: Sequence[str] | pd.Series) -> pd.DataFrame:
    """Count every observed regime without omitting UNKNOWN or rare states."""

    series = pd.Series(regimes, dtype="string")
    if series.empty or series.isna().any():
        raise ValueError("regimes must be non-empty and cannot contain missing values")
    counts = series.value_counts(sort=False).sort_index()
    return pd.DataFrame(
        {
            "regime": counts.index.astype(str),
            "count": counts.to_numpy(dtype=np.int64),
            "fraction": (counts / counts.sum()).to_numpy(dtype=np.float64),
        }
    )


def regime_transition_matrix(
    regimes: Sequence[str] | pd.Series,
    *,
    normalize: bool = True,
) -> pd.DataFrame:
    """Return transitions with the union of origin and destination states."""

    series = pd.Series(regimes, dtype="string")
    if series.empty or series.isna().any():
        raise ValueError("regimes must be non-empty and cannot contain missing values")
    states = sorted(str(value) for value in series.unique())
    origins = series.iloc[:-1].reset_index(drop=True)
    destinations = series.iloc[1:].reset_index(drop=True)
    transitions = pd.crosstab(origins, destinations)
    transitions = transitions.reindex(index=states, columns=states, fill_value=0)
    transitions.index.name = "from_regime"
    transitions.columns.name = "to_regime"
    if normalize:
        totals = transitions.sum(axis=1).replace(0, np.nan)
        transitions = transitions.div(totals, axis=0).fillna(0.0)
    return transitions
