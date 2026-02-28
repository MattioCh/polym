"""
Backtesting performance metrics library.

Computes standard portfolio/strategy metrics from a daily PnL series:
- Sharpe ratio (annualized)
- Sortino ratio (annualized)
- Maximum drawdown (absolute and percentage)
- Maximum drawdown duration
- Calmar ratio
- Profit factor
- Win rate (daily)
- Total and average PnL
- Annualized return and volatility
- Skewness and kurtosis of returns

Usage:
    from src.common.metrics import compute_metrics, compute_metrics_df

    # From a pandas Series of daily PnL (index = dates, values = $ PnL)
    metrics = compute_metrics(daily_pnl_series, initial_capital=100_000)

    # Returns a dict with all metrics, or a single-row DataFrame
    df = compute_metrics_df(daily_pnl_series, initial_capital=100_000)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


TRADING_DAYS_PER_YEAR = 365  # prediction markets trade every day


@dataclass
class BacktestMetrics:
    """Container for all backtest performance metrics."""

    # PnL
    total_pnl: float
    total_pnl_dollars: float
    average_daily_pnl: float
    median_daily_pnl: float

    # Returns
    total_return_pct: float
    annualized_return_pct: float
    daily_return_volatility: float
    annualized_volatility: float

    # Risk-adjusted
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float

    # Drawdown
    max_drawdown_pct: float
    max_drawdown_dollars: float
    max_drawdown_duration_days: int

    # Win/loss
    win_rate_daily: float
    profit_factor: float
    best_day_pnl: float
    worst_day_pnl: float

    # Distribution
    skewness: float
    kurtosis: float

    # Volume
    total_trades: int
    total_capital_deployed: float
    average_trade_pnl: float
    trade_win_rate: float

    # Period
    start_date: str
    end_date: str
    num_trading_days: int

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total_pnl_cents": round(self.total_pnl, 2),
            "total_pnl_dollars": round(self.total_pnl_dollars, 2),
            "avg_daily_pnl_dollars": round(self.average_daily_pnl / 100, 4),
            "median_daily_pnl_dollars": round(self.median_daily_pnl / 100, 4),
            "total_return_pct": round(self.total_return_pct, 4),
            "annualized_return_pct": round(self.annualized_return_pct, 4),
            "daily_volatility_pct": round(self.daily_return_volatility, 6),
            "annualized_volatility_pct": round(self.annualized_volatility, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 4),
            "sortino_ratio": round(self.sortino_ratio, 4),
            "calmar_ratio": round(self.calmar_ratio, 4),
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "max_drawdown_dollars": round(self.max_drawdown_dollars, 2),
            "max_drawdown_duration_days": self.max_drawdown_duration_days,
            "win_rate_daily_pct": round(self.win_rate_daily, 4),
            "profit_factor": round(self.profit_factor, 4),
            "best_day_pnl_dollars": round(self.best_day_pnl / 100, 2),
            "worst_day_pnl_dollars": round(self.worst_day_pnl / 100, 2),
            "skewness": round(self.skewness, 4),
            "kurtosis": round(self.kurtosis, 4),
            "total_trades": self.total_trades,
            "total_capital_deployed_dollars": round(self.total_capital_deployed / 100, 2),
            "avg_trade_pnl_cents": round(self.average_trade_pnl, 4),
            "trade_win_rate_pct": round(self.trade_win_rate, 4),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "num_trading_days": self.num_trading_days,
        }

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to single-row DataFrame."""
        return pd.DataFrame([self.to_dict()])


def _compute_drawdown_series(cumulative_pnl: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Compute drawdown and drawdown percentage from cumulative PnL.

    Args:
        cumulative_pnl: Cumulative PnL series (in cents).

    Returns:
        Tuple of (drawdown_abs, drawdown_pct) series.
    """
    running_max = cumulative_pnl.cummax()
    drawdown_abs = cumulative_pnl - running_max
    # Avoid division by zero
    drawdown_pct = drawdown_abs / running_max.replace(0, np.nan)
    drawdown_pct = drawdown_pct.fillna(0)
    return drawdown_abs, drawdown_pct


def _max_drawdown_duration(cumulative_pnl: pd.Series) -> int:
    """Compute the longest drawdown duration in days.

    Args:
        cumulative_pnl: Cumulative PnL series indexed by date.

    Returns:
        Maximum number of days spent in drawdown before recovering to peak.
    """
    running_max = cumulative_pnl.cummax()
    in_drawdown = cumulative_pnl < running_max

    if not in_drawdown.any():
        return 0

    # Find consecutive drawdown streaks
    max_duration = 0
    current_duration = 0
    for is_dd in in_drawdown:
        if is_dd:
            current_duration += 1
            max_duration = max(max_duration, current_duration)
        else:
            current_duration = 0

    return max_duration


def compute_metrics(
    daily_pnl: pd.Series,
    initial_capital: float = 100_000_00,  # in cents ($100,000)
    total_trades: int = 0,
    total_capital_deployed: float = 0.0,
    trade_wins: int = 0,
    risk_free_rate: float = 0.0,
    trading_days_per_year: int = TRADING_DAYS_PER_YEAR,
) -> BacktestMetrics:
    """Compute comprehensive backtest metrics from a daily PnL series.

    Args:
        daily_pnl: Series indexed by date with daily PnL values (in cents).
        initial_capital: Starting capital in cents.
        total_trades: Total number of individual trades.
        total_capital_deployed: Total capital at risk across all trades (cents).
        trade_wins: Number of winning individual trades.
        risk_free_rate: Annual risk-free rate (decimal, e.g. 0.05 for 5%).
        trading_days_per_year: Number of trading days per year for annualization.

    Returns:
        BacktestMetrics with all computed values.
    """
    if daily_pnl.empty:
        return _empty_metrics()

    daily_pnl = daily_pnl.sort_index()
    n_days = len(daily_pnl)

    # Basic PnL
    total_pnl = daily_pnl.sum()
    avg_daily = daily_pnl.mean()
    median_daily = daily_pnl.median()

    # Returns as fraction of capital
    daily_returns = daily_pnl / initial_capital
    total_return = total_pnl / initial_capital

    # Annualized return
    n_years = n_days / trading_days_per_year
    if n_years > 0 and (1 + total_return) > 0:
        annualized_return = (1 + total_return) ** (1 / n_years) - 1
    else:
        annualized_return = 0.0

    # Volatility
    daily_vol = daily_returns.std() if n_days > 1 else 0.0
    annual_vol = daily_vol * np.sqrt(trading_days_per_year)

    # Daily risk-free rate
    daily_rf = (1 + risk_free_rate) ** (1 / trading_days_per_year) - 1

    # Sharpe ratio (annualized)
    excess_returns = daily_returns - daily_rf
    if daily_vol > 0:
        sharpe = (excess_returns.mean() / daily_vol) * np.sqrt(trading_days_per_year)
    else:
        sharpe = 0.0

    # Sortino ratio (annualized, using downside deviation)
    downside_returns = excess_returns[excess_returns < 0]
    if len(downside_returns) > 0:
        downside_dev = np.sqrt((downside_returns**2).mean())
        sortino = (excess_returns.mean() / downside_dev) * np.sqrt(trading_days_per_year) if downside_dev > 0 else 0.0
    else:
        sortino = float("inf") if excess_returns.mean() > 0 else 0.0

    # Cumulative PnL for drawdown
    cumulative_pnl = daily_pnl.cumsum()
    dd_abs, dd_pct = _compute_drawdown_series(cumulative_pnl)

    max_dd_abs = abs(dd_abs.min()) if len(dd_abs) > 0 else 0.0
    # Max drawdown as % of peak equity
    equity = initial_capital + cumulative_pnl
    equity_peak = equity.cummax()
    equity_dd_pct = (equity - equity_peak) / equity_peak
    max_dd_pct = abs(equity_dd_pct.min()) if len(equity_dd_pct) > 0 else 0.0

    max_dd_duration = _max_drawdown_duration(cumulative_pnl)

    # Calmar ratio
    calmar = annualized_return / max_dd_pct if max_dd_pct > 0 else 0.0

    # Win rate (daily)
    win_days = (daily_pnl > 0).sum()
    win_rate = win_days / n_days * 100 if n_days > 0 else 0.0

    # Profit factor
    gross_profit = daily_pnl[daily_pnl > 0].sum()
    gross_loss = abs(daily_pnl[daily_pnl < 0].sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Distribution
    skew = float(daily_returns.skew()) if n_days > 2 else 0.0
    kurt = float(daily_returns.kurtosis()) if n_days > 3 else 0.0

    # Trade-level stats
    avg_trade_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
    trade_win_rate = trade_wins / total_trades * 100 if total_trades > 0 else 0.0

    return BacktestMetrics(
        total_pnl=total_pnl,
        total_pnl_dollars=total_pnl / 100,
        average_daily_pnl=avg_daily,
        median_daily_pnl=median_daily,
        total_return_pct=total_return * 100,
        annualized_return_pct=annualized_return * 100,
        daily_return_volatility=daily_vol * 100,
        annualized_volatility=annual_vol * 100,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        max_drawdown_pct=max_dd_pct * 100,
        max_drawdown_dollars=max_dd_abs / 100,
        max_drawdown_duration_days=max_dd_duration,
        win_rate_daily=win_rate,
        profit_factor=profit_factor,
        best_day_pnl=daily_pnl.max(),
        worst_day_pnl=daily_pnl.min(),
        skewness=skew,
        kurtosis=kurt,
        total_trades=total_trades,
        total_capital_deployed=total_capital_deployed,
        average_trade_pnl=avg_trade_pnl,
        trade_win_rate=trade_win_rate,
        start_date=str(daily_pnl.index.min().date()) if hasattr(daily_pnl.index.min(), "date") else str(daily_pnl.index.min()),
        end_date=str(daily_pnl.index.max().date()) if hasattr(daily_pnl.index.max(), "date") else str(daily_pnl.index.max()),
        num_trading_days=n_days,
    )


def compute_metrics_df(
    daily_pnl: pd.Series,
    initial_capital: float = 100_000_00,
    **kwargs,
) -> pd.DataFrame:
    """Convenience wrapper that returns metrics as a DataFrame.

    Args:
        daily_pnl: Series indexed by date with daily PnL values (in cents).
        initial_capital: Starting capital in cents.
        **kwargs: Additional arguments passed to compute_metrics.

    Returns:
        Single-row DataFrame with all metrics.
    """
    metrics = compute_metrics(daily_pnl, initial_capital, **kwargs)
    return metrics.to_dataframe()


def compute_rolling_metrics(
    daily_pnl: pd.Series,
    window: int = 90,
    initial_capital: float = 100_000_00,
) -> pd.DataFrame:
    """Compute rolling performance metrics over a sliding window.

    Args:
        daily_pnl: Series indexed by date with daily PnL values (in cents).
        window: Rolling window size in days.
        initial_capital: Starting capital in cents.

    Returns:
        DataFrame with rolling Sharpe, Sortino, drawdown, and cumulative PnL.
    """
    daily_pnl = daily_pnl.sort_index()
    daily_returns = daily_pnl / initial_capital

    rolling_mean = daily_returns.rolling(window).mean()
    rolling_std = daily_returns.rolling(window).std()

    # Rolling Sharpe
    rolling_sharpe = (rolling_mean / rolling_std) * np.sqrt(TRADING_DAYS_PER_YEAR)

    # Rolling Sortino
    def _rolling_sortino(returns: pd.Series) -> float:
        if returns.isna().all() or len(returns) < 2:
            return np.nan
        mean_r = returns.mean()
        downside = returns[returns < 0]
        if len(downside) == 0:
            return np.nan
        dd = np.sqrt((downside**2).mean())
        return (mean_r / dd) * np.sqrt(TRADING_DAYS_PER_YEAR) if dd > 0 else np.nan

    rolling_sortino = daily_returns.rolling(window).apply(_rolling_sortino, raw=False)

    # Cumulative PnL
    cumulative_pnl = daily_pnl.cumsum()

    # Drawdown
    equity = initial_capital + cumulative_pnl
    equity_peak = equity.cummax()
    drawdown_pct = ((equity - equity_peak) / equity_peak) * 100

    return pd.DataFrame({
        "date": daily_pnl.index,
        "daily_pnl": daily_pnl.values,
        "cumulative_pnl": cumulative_pnl.values,
        "equity": equity.values,
        "drawdown_pct": drawdown_pct.values,
        "rolling_sharpe": rolling_sharpe.values,
        "rolling_sortino": rolling_sortino.values,
    })


def _empty_metrics() -> BacktestMetrics:
    """Return zeroed-out metrics for empty data."""
    return BacktestMetrics(
        total_pnl=0, total_pnl_dollars=0, average_daily_pnl=0, median_daily_pnl=0,
        total_return_pct=0, annualized_return_pct=0, daily_return_volatility=0,
        annualized_volatility=0, sharpe_ratio=0, sortino_ratio=0, calmar_ratio=0,
        max_drawdown_pct=0, max_drawdown_dollars=0, max_drawdown_duration_days=0,
        win_rate_daily=0, profit_factor=0, best_day_pnl=0, worst_day_pnl=0,
        skewness=0, kurtosis=0, total_trades=0, total_capital_deployed=0,
        average_trade_pnl=0, trade_win_rate=0, start_date="", end_date="",
        num_trading_days=0,
    )
