#!/usr/bin/env python3
"""Temporary script to analyze backtest results for maker and taker strategies.

Reads the output CSV/JSON files and produces a comprehensive summary.
"""

import json
from pathlib import Path

import pandas as pd
import numpy as np

OUTPUT_DIR = Path(__file__).parent.parent / "output"


def load_summary(name: str) -> dict:
    """Load the single-row summary CSV as a dict."""
    df = pd.read_csv(OUTPUT_DIR / f"{name}.csv")
    return df.iloc[0].to_dict()


def load_equity_curve(name: str) -> pd.DataFrame:
    """Load the JSON equity curve."""
    with open(OUTPUT_DIR / f"{name}.json") as f:
        data = json.load(f)
    df = pd.DataFrame(data["data"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_trade_log(name: str) -> pd.DataFrame:
    """Load the trade-level CSV."""
    df = pd.read_csv(OUTPUT_DIR / f"{name}_trades.csv", parse_dates=["trade_time", "close_time", "pnl_date"])
    return df


def analyze_strategy(name: str, role: str):
    """Analyze a single strategy and return summary text."""
    summary = load_summary(name)
    equity = load_equity_curve(name)
    trades = load_trade_log(name)

    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  {name.upper().replace('_', ' ')}  ({role.upper()} Strategy)")
    lines.append(f"{'='*80}")

    # ── Overall Performance ──
    lines.append("\n## Overall Performance")
    lines.append(f"  Period:              {summary['start_date']} to {summary['end_date']}  ({int(summary['num_trading_days'])} trading days)")
    lines.append(f"  Initial Capital:     $10,000.00")
    lines.append(f"  Final Equity:        ${10000 + summary['total_pnl_dollars']:,.2f}")
    lines.append(f"  Total PnL:           ${summary['total_pnl_dollars']:,.2f}")
    lines.append(f"  Total Return:        {summary['total_return_pct']:.2f}%")
    lines.append(f"  Annualized Return:   {summary['annualized_return_pct']:.2f}%")
    lines.append(f"  Avg Daily PnL:       ${summary['avg_daily_pnl_dollars']:.2f}")
    lines.append(f"  Median Daily PnL:    ${summary['median_daily_pnl_dollars']:.2f}")

    # ── Risk Metrics ──
    lines.append("\n## Risk Metrics")
    lines.append(f"  Sharpe Ratio:        {summary['sharpe_ratio']:.4f}")
    lines.append(f"  Sortino Ratio:       {summary['sortino_ratio']:.4f}")
    lines.append(f"  Calmar Ratio:        {summary['calmar_ratio']:.4f}")
    lines.append(f"  Max Drawdown:        {summary['max_drawdown_pct']:.2f}% (${summary['max_drawdown_dollars']:,.2f})")
    lines.append(f"  Max DD Duration:     {int(summary['max_drawdown_duration_days'])} days")
    lines.append(f"  Daily Volatility:    {summary['daily_volatility_pct']:.4f}%")
    lines.append(f"  Annual Volatility:   {summary['annualized_volatility_pct']:.2f}%")
    lines.append(f"  Skewness:            {summary['skewness']:.2f}")
    lines.append(f"  Kurtosis:            {summary['kurtosis']:.2f}")

    # ── Trade Statistics ──
    lines.append("\n## Trade Statistics")
    lines.append(f"  Total Trades:        {int(summary['total_trades']):,}")
    lines.append(f"  Trade Win Rate:      {summary['trade_win_rate_pct']:.2f}%")
    lines.append(f"  Daily Win Rate:      {summary['win_rate_daily_pct']:.2f}%")
    lines.append(f"  Avg Trade PnL:       {summary['avg_trade_pnl_cents']:.2f}¢  (${summary['avg_trade_pnl_cents']/100:.4f})")
    lines.append(f"  Total Deployed:      ${summary['total_capital_deployed_dollars']:,.2f}")
    lines.append(f"  Profit Factor:       {summary['profit_factor']:.4f}")
    lines.append(f"  Best Day:            ${summary['best_day_pnl_dollars']:,.2f}")
    lines.append(f"  Worst Day:           ${summary['worst_day_pnl_dollars']:,.2f}")

    # ── PnL column name ──
    pnl_col = "adj_maker_pnl_dollars" if role == "maker" else "adj_taker_pnl_dollars"
    cost_col = "adj_maker_cost_dollars" if role == "maker" else "adj_taker_cost_dollars"
    won_col = "maker_won" if role == "maker" else "taker_won"

    # ── Category Breakdown ──
    lines.append("\n## PnL by Category")
    cat_pnl = trades.groupby("category").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        avg_pnl=(pnl_col, "mean"),
        total_cost=(cost_col, "sum"),
    ).sort_values("total_pnl", ascending=False)
    cat_pnl["roi_pct"] = (cat_pnl["total_pnl"] / cat_pnl["total_cost"] * 100).round(2)
    cat_pnl["win_rate"] = (cat_pnl["win_rate"] * 100).round(2)
    lines.append(f"  {'Category':<25} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8} {'Avg PnL($)':>12} {'ROI%':>8}")
    lines.append(f"  {'-'*25} {'-'*12} {'-'*10} {'-'*8} {'-'*12} {'-'*8}")
    for cat, row in cat_pnl.iterrows():
        lines.append(f"  {cat:<25} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['avg_pnl']:>12.4f} {row['roi_pct']:>7.2f}%")

    # ── Group Breakdown (top 20) ──
    lines.append("\n## PnL by Group (Top 20 by |PnL|)")
    grp_pnl = trades.groupby("group").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        avg_pnl=(pnl_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    grp_pnl["roi_pct"] = (grp_pnl["total_pnl"] / grp_pnl["total_cost"] * 100).round(2)
    grp_pnl["win_rate"] = (grp_pnl["win_rate"] * 100).round(2)
    grp_pnl = grp_pnl.reindex(grp_pnl["total_pnl"].abs().sort_values(ascending=False).index).head(20)
    lines.append(f"  {'Group':<25} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*25} {'-'*12} {'-'*10} {'-'*8} {'-'*8}")
    for grp, row in grp_pnl.iterrows():
        lines.append(f"  {grp:<25} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── Price Bucket Breakdown ──
    lines.append("\n## PnL by Price Bucket")
    pb = trades.groupby("price_bucket").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    pb["roi_pct"] = (pb["total_pnl"] / pb["total_cost"] * 100).round(2)
    pb["win_rate"] = (pb["win_rate"] * 100).round(2)
    lines.append(f"  {'Bucket':<12} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*8} {'-'*8}")
    for bucket, row in pb.iterrows():
        lines.append(f"  {bucket:<12} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── Time Bucket Breakdown ──
    lines.append("\n## PnL by Time-to-Close Bucket")
    tb = trades.groupby("time_bucket").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    tb["roi_pct"] = (tb["total_pnl"] / tb["total_cost"] * 100).round(2)
    tb["win_rate"] = (tb["win_rate"] * 100).round(2)
    lines.append(f"  {'Bucket':<12} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*8} {'-'*8}")
    for bucket, row in tb.iterrows():
        lines.append(f"  {bucket:<12} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── Day Type Breakdown ──
    lines.append("\n## PnL by Day Type")
    dt = trades.groupby("day_type").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    dt["roi_pct"] = (dt["total_pnl"] / dt["total_cost"] * 100).round(2)
    dt["win_rate"] = (dt["win_rate"] * 100).round(2)
    lines.append(f"  {'Day Type':<12} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*8} {'-'*8}")
    for day, row in dt.iterrows():
        lines.append(f"  {day:<12} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── Taker Side Breakdown ──
    lines.append("\n## PnL by Taker Side")
    ts = trades.groupby("taker_side").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    ts["roi_pct"] = (ts["total_pnl"] / ts["total_cost"] * 100).round(2)
    ts["win_rate"] = (ts["win_rate"] * 100).round(2)
    lines.append(f"  {'Side':<12} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*8} {'-'*8}")
    for side, row in ts.iterrows():
        lines.append(f"  {side:<12} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── Monthly PnL ──
    lines.append("\n## Monthly PnL")
    trades["month"] = trades["pnl_date"].dt.to_period("M")
    monthly = trades.groupby("month").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
    )
    monthly["win_rate"] = (monthly["win_rate"] * 100).round(2)
    lines.append(f"  {'Month':<12} {'PnL ($)':>12} {'# Trades':>10} {'Win%':>8}")
    lines.append(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*8}")
    for month, row in monthly.iterrows():
        flag = " <<<" if row["total_pnl"] < -100 else (" ✓" if row["total_pnl"] > 100 else "")
        lines.append(f"  {str(month):<12} {row['total_pnl']:>12.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}%{flag}")

    # ── Equity curve key points ──
    lines.append("\n## Equity Curve Key Points")
    eq = equity.set_index("date")
    max_eq_date = eq["equity"].idxmax()
    min_eq_date = eq["equity"].idxmin()
    max_dd_date = eq["drawdown_pct"].idxmin()
    lines.append(f"  Peak Equity:    ${eq.loc[max_eq_date, 'equity']:,.2f}  on {max_eq_date.strftime('%Y-%m-%d')}")
    lines.append(f"  Trough Equity:  ${eq.loc[min_eq_date, 'equity']:,.2f}  on {min_eq_date.strftime('%Y-%m-%d')}")
    lines.append(f"  Max Drawdown:   {eq.loc[max_dd_date, 'drawdown_pct']:.2f}%  on {max_dd_date.strftime('%Y-%m-%d')}")

    # ── Largest winning and losing trades ──
    lines.append("\n## Top 10 Largest Winning Trades")
    top_wins = trades.nlargest(10, pnl_col)
    lines.append(f"  {'Date':<12} {'Category':<20} {'Group':<20} {'Side':<6} {'Result':<8} {'PnL($)':>10}")
    for _, t in top_wins.iterrows():
        lines.append(f"  {str(t['pnl_date'])[:10]:<12} {t['category']:<20} {t['group']:<20} {t['taker_side']:<6} {t['result']:<8} {t[pnl_col]:>10.2f}")

    lines.append("\n## Top 10 Largest Losing Trades")
    top_losses = trades.nsmallest(10, pnl_col)
    lines.append(f"  {'Date':<12} {'Category':<20} {'Group':<20} {'Side':<6} {'Result':<8} {'PnL($)':>10}")
    for _, t in top_losses.iterrows():
        lines.append(f"  {str(t['pnl_date'])[:10]:<12} {t['category']:<20} {t['group']:<20} {t['taker_side']:<6} {t['result']:<8} {t[pnl_col]:>10.2f}")

    # ── Participation rate analysis ──
    lines.append("\n## Participation Rate Distribution")
    for label, lo, hi in [("Full (100%)", 0.99, 1.01), ("50-99%", 0.50, 0.99), ("10-50%", 0.10, 0.50), ("<10%", 0.0, 0.10)]:
        mask = (trades["participation_rate"] >= lo) & (trades["participation_rate"] < hi)
        if label == "Full (100%)":
            mask = trades["participation_rate"] >= 0.99
        subset = trades[mask]
        if len(subset) > 0:
            lines.append(f"  {label:<15}: {len(subset):>6,} trades, PnL=${subset[pnl_col].sum():>10.2f}, Avg=${subset[pnl_col].mean():>8.4f}")

    return "\n".join(lines)


def compare_strategies():
    """Compare maker vs taker strategies head to head."""
    maker = load_summary("backtest_maker_strategy")
    taker = load_summary("backtest_taker_strategy")

    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  HEAD-TO-HEAD COMPARISON: MAKER vs TAKER")
    lines.append(f"{'='*80}")
    
    metrics = [
        ("Total PnL ($)", "total_pnl_dollars", ".2f"),
        ("Total Return (%)", "total_return_pct", ".2f"),
        ("Annualized Return (%)", "annualized_return_pct", ".2f"),
        ("Sharpe Ratio", "sharpe_ratio", ".4f"),
        ("Sortino Ratio", "sortino_ratio", ".4f"),
        ("Calmar Ratio", "calmar_ratio", ".4f"),
        ("Max Drawdown (%)", "max_drawdown_pct", ".2f"),
        ("Max DD Duration (days)", "max_drawdown_duration_days", ".0f"),
        ("Daily Win Rate (%)", "win_rate_daily_pct", ".2f"),
        ("Trade Win Rate (%)", "trade_win_rate_pct", ".2f"),
        ("Profit Factor", "profit_factor", ".4f"),
        ("Avg Trade PnL (¢)", "avg_trade_pnl_cents", ".2f"),
        ("Total Trades", "total_trades", ".0f"),
        ("Total Capital Deployed ($)", "total_capital_deployed_dollars", ".2f"),
        ("Annualized Volatility (%)", "annualized_volatility_pct", ".2f"),
        ("Skewness", "skewness", ".2f"),
        ("Kurtosis", "kurtosis", ".2f"),
        ("Best Day ($)", "best_day_pnl_dollars", ".2f"),
        ("Worst Day ($)", "worst_day_pnl_dollars", ".2f"),
    ]

    lines.append(f"\n  {'Metric':<30} {'Maker':>14} {'Taker':>14} {'Winner':>10}")
    lines.append(f"  {'-'*30} {'-'*14} {'-'*14} {'-'*10}")
    
    for label, key, fmt in metrics:
        m_val = maker[key]
        t_val = taker[key]
        # Determine winner (higher is better for most, lower for drawdown/volatility)
        lower_is_better = key in ("max_drawdown_pct", "max_drawdown_duration_days", "annualized_volatility_pct")
        if lower_is_better:
            winner = "Taker" if t_val < m_val else "Maker"
        else:
            winner = "Taker" if t_val > m_val else "Maker"
        lines.append(f"  {label:<30} {m_val:>14{fmt}} {t_val:>14{fmt}} {winner:>10}")

    lines.append("\n## Key Takeaways")
    lines.append(f"  - The MAKER strategy LOST ${abs(maker['total_pnl_dollars']):,.2f} ({maker['total_return_pct']:.1f}% return)")
    lines.append(f"  - The TAKER strategy GAINED ${taker['total_pnl_dollars']:,.2f} ({taker['total_return_pct']:.1f}% return)")
    lines.append(f"  - Taker Sharpe ({taker['sharpe_ratio']:.2f}) vs Maker Sharpe ({maker['sharpe_ratio']:.2f})")
    lines.append(f"  - Taker max drawdown ({taker['max_drawdown_pct']:.1f}%) far smaller than Maker ({maker['max_drawdown_pct']:.1f}%)")
    lines.append(f"  - Despite positive maker EDGE in raw analysis, the walk-forward backtest shows")
    lines.append(f"    the maker strategy failed to translate that edge into live PnL.")

    return "\n".join(lines)


def analyze_mean_reversion(name: str = "backtest_mean_reversion") -> str:
    """Analyze the mean-reversion fade strategy (different column names)."""
    summary = load_summary(name)
    equity = load_equity_curve(name)
    trades = load_trade_log(name)

    pnl_col = "adj_fade_pnl_dollars"
    cost_col = "adj_fade_cost_dollars"
    won_col = "fade_won"

    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  MEAN-REVERSION FADE STRATEGY")
    lines.append(f"{'='*80}")

    # ── Overall Performance ──
    lines.append("\n## Overall Performance")
    lines.append(f"  Period:              {summary['start_date']} to {summary['end_date']}  ({int(summary['num_trading_days'])} trading days)")
    lines.append(f"  Initial Capital:     $10,000.00")
    lines.append(f"  Final Equity:        ${10000 + summary['total_pnl_dollars']:,.2f}")
    lines.append(f"  Total PnL:           ${summary['total_pnl_dollars']:,.2f}")
    lines.append(f"  Total Return:        {summary['total_return_pct']:,.2f}%")
    lines.append(f"  Annualized Return:   {summary['annualized_return_pct']:,.2f}%")
    lines.append(f"  Avg Daily PnL:       ${summary['avg_daily_pnl_dollars']:,.2f}")
    lines.append(f"  Median Daily PnL:    ${summary['median_daily_pnl_dollars']:,.2f}")

    # ── Risk Metrics ──
    lines.append("\n## Risk Metrics")
    lines.append(f"  Sharpe Ratio:        {summary['sharpe_ratio']:.4f}")
    lines.append(f"  Sortino Ratio:       {summary['sortino_ratio']:.4f}")
    lines.append(f"  Calmar Ratio:        {summary['calmar_ratio']:.4f}")
    lines.append(f"  Max Drawdown:        {summary['max_drawdown_pct']:.2f}% (${summary['max_drawdown_dollars']:,.2f})")
    lines.append(f"  Max DD Duration:     {int(summary['max_drawdown_duration_days'])} days")
    lines.append(f"  Daily Volatility:    {summary['daily_volatility_pct']:.4f}%")
    lines.append(f"  Annual Volatility:   {summary['annualized_volatility_pct']:.2f}%")
    lines.append(f"  Skewness:            {summary['skewness']:.2f}")
    lines.append(f"  Kurtosis:            {summary['kurtosis']:.2f}")

    # ── Trade Statistics ──
    lines.append("\n## Trade Statistics")
    lines.append(f"  Total Trades:        {int(summary['total_trades']):,}")
    lines.append(f"  Trade Win Rate:      {summary['trade_win_rate_pct']:.2f}%")
    lines.append(f"  Daily Win Rate:      {summary['win_rate_daily_pct']:.2f}%")
    lines.append(f"  Avg Trade PnL:       {summary['avg_trade_pnl_cents']:.2f}¢  (${summary['avg_trade_pnl_cents']/100:.4f})")
    lines.append(f"  Total Deployed:      ${summary['total_capital_deployed_dollars']:,.2f}")
    lines.append(f"  Profit Factor:       {summary['profit_factor']:.4f}")
    lines.append(f"  Best Day:            ${summary['best_day_pnl_dollars']:,.2f}")
    lines.append(f"  Worst Day:           ${summary['worst_day_pnl_dollars']:,.2f}")

    # ── PnL by Fade Direction ──
    lines.append("\n## PnL by Fade Direction")
    fd = trades.groupby("fade_direction").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    fd["roi_pct"] = (fd["total_pnl"] / fd["total_cost"] * 100).round(2)
    fd["win_rate"] = (fd["win_rate"] * 100).round(2)
    lines.append(f"  {'Direction':<15} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*15} {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for dirn, row in fd.iterrows():
        lines.append(f"  {dirn:<15} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── PnL by Deviation Magnitude ──
    lines.append("\n## PnL by Deviation Magnitude")
    dm = trades.groupby("dev_magnitude").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    dm["roi_pct"] = (dm["total_pnl"] / dm["total_cost"] * 100).round(2)
    dm["win_rate"] = (dm["win_rate"] * 100).round(2)
    lines.append(f"  {'Magnitude':<15} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*15} {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for mag, row in dm.iterrows():
        lines.append(f"  {mag:<15} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── PnL by Group (top 15) ──
    lines.append("\n## PnL by Group (Top 15 by |PnL|)")
    grp_pnl = trades.groupby("group").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    grp_pnl["roi_pct"] = (grp_pnl["total_pnl"] / grp_pnl["total_cost"] * 100).round(2)
    grp_pnl["win_rate"] = (grp_pnl["win_rate"] * 100).round(2)
    grp_pnl = grp_pnl.reindex(grp_pnl["total_pnl"].abs().sort_values(ascending=False).index).head(15)
    lines.append(f"  {'Group':<25} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*25} {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for grp, row in grp_pnl.iterrows():
        lines.append(f"  {grp:<25} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── PnL by Price Bucket ──
    lines.append("\n## PnL by Price Bucket")
    pb = trades.groupby("price_bucket").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    pb["roi_pct"] = (pb["total_pnl"] / pb["total_cost"] * 100).round(2)
    pb["win_rate"] = (pb["win_rate"] * 100).round(2)
    lines.append(f"  {'Bucket':<12} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for bucket, row in pb.iterrows():
        lines.append(f"  {bucket:<12} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── PnL by Time Bucket ──
    lines.append("\n## PnL by Time-to-Close Bucket")
    tb = trades.groupby("time_bucket").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    tb["roi_pct"] = (tb["total_pnl"] / tb["total_cost"] * 100).round(2)
    tb["win_rate"] = (tb["win_rate"] * 100).round(2)
    lines.append(f"  {'Bucket':<12} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for bucket, row in tb.iterrows():
        lines.append(f"  {bucket:<12} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── PnL by Day Type ──
    lines.append("\n## PnL by Day Type")
    dt = trades.groupby("day_type").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
        total_cost=(cost_col, "sum"),
    )
    dt["roi_pct"] = (dt["total_pnl"] / dt["total_cost"] * 100).round(2)
    dt["win_rate"] = (dt["win_rate"] * 100).round(2)
    lines.append(f"  {'Day Type':<12} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8} {'ROI%':>8}")
    lines.append(f"  {'-'*12} {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for day, row in dt.iterrows():
        lines.append(f"  {day:<12} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}% {row['roi_pct']:>7.2f}%")

    # ── Monthly PnL ──
    lines.append("\n## Monthly PnL")
    trades["month"] = trades["pnl_date"].dt.to_period("M")
    monthly = trades.groupby("month").agg(
        total_pnl=(pnl_col, "sum"),
        num_trades=(pnl_col, "count"),
        win_rate=(won_col, "mean"),
    )
    monthly["win_rate"] = (monthly["win_rate"] * 100).round(2)
    lines.append(f"  {'Month':<12} {'PnL ($)':>14} {'# Trades':>10} {'Win%':>8}")
    lines.append(f"  {'-'*12} {'-'*14} {'-'*10} {'-'*8}")
    for month, row in monthly.iterrows():
        flag = " <<<" if row["total_pnl"] < -100 else (" ✓" if row["total_pnl"] > 100 else "")
        lines.append(f"  {str(month):<12} {row['total_pnl']:>14,.2f} {int(row['num_trades']):>10,} {row['win_rate']:>7.1f}%{flag}")

    # ── Equity curve key points ──
    lines.append("\n## Equity Curve Key Points")
    eq = equity.set_index("date")
    max_eq_date = eq["equity"].idxmax()
    min_eq_date = eq["equity"].idxmin()
    max_dd_date = eq["drawdown_pct"].idxmin()
    lines.append(f"  Peak Equity:    ${eq.loc[max_eq_date, 'equity']:,.2f}  on {max_eq_date.strftime('%Y-%m-%d')}")
    lines.append(f"  Trough Equity:  ${eq.loc[min_eq_date, 'equity']:,.2f}  on {min_eq_date.strftime('%Y-%m-%d')}")
    lines.append(f"  Max Drawdown:   {eq.loc[max_dd_date, 'drawdown_pct']:.2f}%  on {max_dd_date.strftime('%Y-%m-%d')}")

    # ── Top 10 winning / losing trades ──
    lines.append("\n## Top 10 Largest Winning Trades")
    top_wins = trades.nlargest(10, pnl_col)
    lines.append(f"  {'Date':<12} {'Category':<20} {'Group':<20} {'Dir':<10} {'Result':<8} {'PnL($)':>12}")
    for _, t in top_wins.iterrows():
        lines.append(f"  {str(t['pnl_date'])[:10]:<12} {t['category']:<20} {t['group']:<20} {t['fade_direction']:<10} {t['result']:<8} {t[pnl_col]:>12,.2f}")

    lines.append("\n## Top 10 Largest Losing Trades")
    top_losses = trades.nsmallest(10, pnl_col)
    lines.append(f"  {'Date':<12} {'Category':<20} {'Group':<20} {'Dir':<10} {'Result':<8} {'PnL($)':>12}")
    for _, t in top_losses.iterrows():
        lines.append(f"  {str(t['pnl_date'])[:10]:<12} {t['category']:<20} {t['group']:<20} {t['fade_direction']:<10} {t['result']:<8} {t[pnl_col]:>12,.2f}")

    return "\n".join(lines)


def compare_all_strategies():
    """Compare all three strategies head to head."""
    maker = load_summary("backtest_maker_strategy")
    taker = load_summary("backtest_taker_strategy")
    mr = load_summary("backtest_mean_reversion")

    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  HEAD-TO-HEAD COMPARISON: MAKER vs TAKER vs MEAN-REVERSION")
    lines.append(f"{'='*80}")

    metrics = [
        ("Total PnL ($)", "total_pnl_dollars", ".2f"),
        ("Total Return (%)", "total_return_pct", ".2f"),
        ("Annualized Return (%)", "annualized_return_pct", ".2f"),
        ("Sharpe Ratio", "sharpe_ratio", ".4f"),
        ("Sortino Ratio", "sortino_ratio", ".4f"),
        ("Calmar Ratio", "calmar_ratio", ".4f"),
        ("Max Drawdown (%)", "max_drawdown_pct", ".2f"),
        ("Max DD Duration (days)", "max_drawdown_duration_days", ".0f"),
        ("Daily Win Rate (%)", "win_rate_daily_pct", ".2f"),
        ("Trade Win Rate (%)", "trade_win_rate_pct", ".2f"),
        ("Profit Factor", "profit_factor", ".4f"),
        ("Avg Trade PnL (¢)", "avg_trade_pnl_cents", ".2f"),
        ("Total Trades", "total_trades", ".0f"),
        ("Total Capital Deployed ($)", "total_capital_deployed_dollars", ".2f"),
        ("Annualized Volatility (%)", "annualized_volatility_pct", ".2f"),
        ("Skewness", "skewness", ".2f"),
        ("Kurtosis", "kurtosis", ".2f"),
        ("Best Day ($)", "best_day_pnl_dollars", ".2f"),
        ("Worst Day ($)", "worst_day_pnl_dollars", ".2f"),
    ]

    lines.append(f"\n  {'Metric':<30} {'Maker':>14} {'Taker':>14} {'Mean-Rev':>14} {'Winner':>10}")
    lines.append(f"  {'-'*30} {'-'*14} {'-'*14} {'-'*14} {'-'*10}")

    for label, key, fmt in metrics:
        m_val = maker[key]
        t_val = taker[key]
        r_val = mr[key]
        lower_is_better = key in ("max_drawdown_pct", "max_drawdown_duration_days", "annualized_volatility_pct")
        vals = {"Maker": m_val, "Taker": t_val, "Mean-Rev": r_val}
        if lower_is_better:
            winner = min(vals, key=vals.get)
        else:
            winner = max(vals, key=vals.get)
        lines.append(f"  {label:<30} {m_val:>14{fmt}} {t_val:>14{fmt}} {r_val:>14{fmt}} {winner:>10}")

    lines.append("\n## Key Takeaways")
    lines.append(f"  - The MAKER strategy LOST ${abs(maker['total_pnl_dollars']):,.2f} ({maker['total_return_pct']:.1f}% return)")
    lines.append(f"  - The TAKER strategy GAINED ${taker['total_pnl_dollars']:,.2f} ({taker['total_return_pct']:.1f}% return)")
    lines.append(f"  - The MEAN-REVERSION strategy GAINED ${mr['total_pnl_dollars']:,.2f} ({mr['total_return_pct']:,.1f}% return, compounded)")
    lines.append(f"  - Mean-Rev Sharpe ({mr['sharpe_ratio']:.2f}) >> Taker Sharpe ({taker['sharpe_ratio']:.2f}) >> Maker Sharpe ({maker['sharpe_ratio']:.2f})")
    lines.append(f"  - Mean-Rev max drawdown ({mr['max_drawdown_pct']:.1f}%) is larger than Taker ({taker['max_drawdown_pct']:.1f}%) but far less than Maker ({maker['max_drawdown_pct']:.1f}%)")
    lines.append(f"  - NOTE: Mean-Rev return is amplified by full reinvestment compounding.")
    lines.append(f"    Unweighted per-trade edge is +23.2% mean return, +35.2% aggregate ROI on deployed capital.")

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("  BACKTEST RESULTS ANALYSIS")
    print("=" * 80)
    
    # Individual strategy analysis
    maker_text = analyze_strategy("backtest_maker_strategy", "maker")
    taker_text = analyze_strategy("backtest_taker_strategy", "taker")
    mr_text = analyze_mean_reversion()
    comparison_text = compare_all_strategies()
    
    full_report = maker_text + "\n\n" + taker_text + "\n\n" + mr_text + "\n\n" + comparison_text
    print(full_report)
    
    # Save to a temporary report file
    report_path = OUTPUT_DIR / "backtest_analysis_report.txt"
    with open(report_path, "w") as f:
        f.write(full_report)
    print(f"\n\nReport saved to: {report_path}")


if __name__ == "__main__":
    main()
