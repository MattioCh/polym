"""Mean-reversion backtest for Kalshi prediction markets.

Strategy
--------
For each market the algorithm maintains a rolling window of *volume-weighted
average prices* (VWAP) computed from historical trades.  When the current
trade price deviates by more than ``threshold`` standard deviations from the
rolling VWAP it takes the counter-directional side:

* price > rolling_mean + threshold * rolling_std  →  sell YES at *price*
  (fade the high; we act as taker buying NO)
* price < rolling_mean - threshold * rolling_std  →  buy  YES at *price*
  (fade the low)

The position is closed at market resolution and realised PnL is computed.

Key outputs
-----------
* Per-trade P&L and cumulative equity curve
* Win rate, average return, Sharpe ratio, and max drawdown
* Performance breakdown by market category and price bucket
* Comparison against a naïve "always-buy-YES" baseline
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.analysis.kalshi.util.categories import CATEGORY_SQL, get_group
from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class BacktestMeanReversionAnalysis(Analysis):
    """Backtest a price mean-reversion strategy on resolved Kalshi markets.

    Parameters
    ----------
    trades_dir:
        Path to the directory containing Kalshi trades Parquet files.
    markets_dir:
        Path to the directory containing Kalshi markets Parquet files.
    lookback:
        Number of trades used to compute the rolling mean and standard
        deviation at each decision point (default 20).
    threshold:
        Z-score threshold that must be exceeded to enter a trade (default 2.0).
    max_position_cents:
        Maximum cost per trade in cents (default 5 000¢ = $50).
    """

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
        lookback: int = 20,
        threshold: float = 2.0,
        max_position_cents: int = 5_000,
    ):
        super().__init__(
            name="backtest_mean_reversion",
            description="Mean-reversion strategy backtest on resolved Kalshi markets",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")
        self.lookback = lookback
        self.threshold = threshold
        self.max_position_cents = max_position_cents

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> AnalysisOutput:
        """Execute the backtest and return outputs."""
        with self.progress("Loading trades and market outcomes"):
            trades_df = self._load_data()

        if trades_df.empty:
            return AnalysisOutput(figure=None, data=pd.DataFrame(), chart=None)

        with self.progress("Running mean-reversion backtest"):
            signals_df = self._generate_signals(trades_df)

        if signals_df.empty:
            return AnalysisOutput(figure=None, data=pd.DataFrame(), chart=None)

        with self.progress("Computing performance metrics"):
            perf = self._compute_performance(signals_df)

        fig = self._create_figure(signals_df, perf)
        chart = self._create_chart(perf)

        return AnalysisOutput(figure=fig, data=signals_df, chart=chart, metadata=perf)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_data(self) -> pd.DataFrame:
        con = duckdb.connect()
        df = con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT
                    ticker,
                    {CATEGORY_SQL} AS category,
                    result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            )
            SELECT
                t.ticker,
                t.trade_id,
                t.yes_price,
                t.no_price,
                t.taker_side,
                t.count       AS contracts,
                t.created_time,
                m.result,
                m.category
            FROM '{self.trades_dir}/*.parquet' t
            INNER JOIN resolved_markets m ON t.ticker = m.ticker
            WHERE t.yes_price BETWEEN 1 AND 99
            ORDER BY t.ticker, t.created_time
            """
        ).df()
        return df

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------

    def _generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the rolling mean-reversion logic per market."""
        records: list[dict] = []

        for ticker, group in df.groupby("ticker"):
            group = group.sort_values("created_time").reset_index(drop=True)
            prices = group["yes_price"].to_numpy(dtype=float)
            contracts_arr = group["contracts"].to_numpy(dtype=float)
            result = group["result"].iloc[0]
            category = group["category"].iloc[0]

            # Compute rolling VWAP and std using a lookback window
            for i in range(self.lookback, len(group)):
                window_prices = prices[i - self.lookback : i]
                window_vols = contracts_arr[i - self.lookback : i]

                total_vol = window_vols.sum()
                if total_vol == 0:
                    continue
                vwap = float(np.dot(window_prices, window_vols) / total_vol)
                std = float(np.std(window_prices))
                if std < 0.5:
                    continue

                current_price = prices[i]
                z = (current_price - vwap) / std

                if abs(z) < self.threshold:
                    continue

                # Determine entry direction
                if z > self.threshold:
                    # Price too high → sell YES (buy NO), entry at current_price
                    entry_side = "no"
                    entry_price = int(100 - current_price)
                else:
                    # Price too low → buy YES, entry at current_price
                    entry_side = "yes"
                    entry_price = int(current_price)

                # Clamp to valid range
                entry_price = max(1, min(99, entry_price))
                max_contracts = self.max_position_cents // entry_price
                if max_contracts < 1:
                    continue

                # PnL at resolution
                payout = 100 if result == entry_side else 0
                pnl_per_contract = payout - entry_price
                total_pnl = pnl_per_contract * max_contracts
                cost = entry_price * max_contracts
                ret_pct = pnl_per_contract / entry_price * 100.0

                records.append(
                    {
                        "ticker": ticker,
                        "category": category,
                        "group": get_group(str(category)),
                        "trade_index": i,
                        "created_time": group["created_time"].iloc[i],
                        "current_price": current_price,
                        "vwap": round(vwap, 2),
                        "std": round(std, 2),
                        "z_score": round(z, 3),
                        "entry_side": entry_side,
                        "entry_price": entry_price,
                        "contracts": max_contracts,
                        "cost_cents": cost,
                        "result": result,
                        "pnl_cents": total_pnl,
                        "pnl_dollars": round(total_pnl / 100, 4),
                        "return_pct": round(ret_pct, 2),
                        "won": pnl_per_contract > 0,
                    }
                )

        if not records:
            return pd.DataFrame()

        signals_df = pd.DataFrame(records)
        signals_df = signals_df.sort_values("created_time").reset_index(drop=True)
        signals_df["cumulative_pnl_dollars"] = signals_df["pnl_dollars"].cumsum().round(4)
        return signals_df

    # ------------------------------------------------------------------
    # Performance metrics
    # ------------------------------------------------------------------

    def _compute_performance(self, df: pd.DataFrame) -> dict:
        n = len(df)
        if n == 0:
            return {}

        wins = df["won"].sum()
        total_pnl = df["pnl_dollars"].sum()
        avg_ret = df["return_pct"].mean()
        std_ret = df["return_pct"].std()
        sharpe = (avg_ret / std_ret * (n**0.5)) if std_ret > 0 else 0.0

        # Max drawdown on cumulative PnL curve
        cum = df["cumulative_pnl_dollars"].values
        peak = np.maximum.accumulate(cum)
        drawdown = cum - peak
        max_dd = float(drawdown.min())

        # By-group breakdown
        group_perf = (
            df.groupby("group")
            .agg(
                trade_count=("pnl_dollars", "count"),
                total_pnl=("pnl_dollars", "sum"),
                win_rate=("won", "mean"),
                avg_return=("return_pct", "mean"),
            )
            .reset_index()
        )

        # By price bucket
        df["price_bucket"] = pd.cut(
            df["entry_price"],
            bins=[0, 20, 40, 60, 80, 100],
            labels=["01-20", "21-40", "41-60", "61-80", "81-99"],
        )
        price_perf = (
            df.groupby("price_bucket", observed=True)
            .agg(
                trade_count=("pnl_dollars", "count"),
                total_pnl=("pnl_dollars", "sum"),
                win_rate=("won", "mean"),
                avg_return=("return_pct", "mean"),
            )
            .reset_index()
        )

        return {
            "total_signals": n,
            "win_rate": round(wins / n, 4),
            "total_pnl_dollars": round(total_pnl, 4),
            "avg_return_pct": round(avg_ret, 4),
            "std_return_pct": round(std_ret, 4),
            "sharpe": round(sharpe, 4),
            "max_drawdown_dollars": round(max_dd, 4),
            "group_perf": group_perf.to_dict("records"),
            "price_perf": price_perf.to_dict("records"),
        }

    # ------------------------------------------------------------------
    # Visualisation
    # ------------------------------------------------------------------

    def _create_figure(self, df: pd.DataFrame, perf: dict) -> plt.Figure:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(
            f"Mean-Reversion Backtest  |  lookback={self.lookback}  "
            f"threshold={self.threshold}σ  |  "
            f"n={perf.get('total_signals', 0)}  "
            f"WR={perf.get('win_rate', 0) * 100:.1f}%  "
            f"PnL=${perf.get('total_pnl_dollars', 0):.2f}  "
            f"Sharpe={perf.get('sharpe', 0):.2f}",
            fontsize=11,
        )

        # Panel 1: Cumulative PnL curve
        ax1 = axes[0, 0]
        ax1.plot(df["created_time"], df["cumulative_pnl_dollars"], color="#3498db", linewidth=1.5)
        ax1.axhline(0, color="black", linewidth=0.5)
        ax1.set_ylabel("Cumulative PnL ($)")
        ax1.set_title("Equity Curve")
        ax1.grid(alpha=0.3)

        # Panel 2: Return distribution
        ax2 = axes[0, 1]
        ax2.hist(df["return_pct"], bins=30, color="#3498db", alpha=0.7, edgecolor="black", linewidth=0.3)
        ax2.axvline(0, color="red", linewidth=1.5)
        avg_r = perf.get("avg_return_pct", 0.0)
        ax2.axvline(avg_r, color="green", linewidth=1.5, linestyle="--", label=f"Mean={avg_r:.1f}%")
        ax2.set_xlabel("Return per Trade (%)")
        ax2.set_ylabel("Count")
        ax2.set_title("Trade Return Distribution")
        ax2.legend(fontsize=8)
        ax2.grid(alpha=0.3)

        # Panel 3: Win rate by category group
        ax3 = axes[1, 0]
        gp = pd.DataFrame(perf.get("group_perf", []))
        if not gp.empty:
            gp = gp.sort_values("win_rate", ascending=True)
            colors = ["#2ecc71" if v >= 0.5 else "#e74c3c" for v in gp["win_rate"]]
            ax3.barh(range(len(gp)), gp["win_rate"] * 100, color=colors, alpha=0.8)
            ax3.set_yticks(range(len(gp)))
            ax3.set_yticklabels(gp["group"], fontsize=8)
            ax3.axvline(50, color="black", linewidth=0.5, linestyle="--")
        ax3.set_xlabel("Win Rate (%)")
        ax3.set_title("Win Rate by Category Group")
        ax3.grid(axis="x", alpha=0.3)

        # Panel 4: PnL by price bucket
        ax4 = axes[1, 1]
        pp = pd.DataFrame(perf.get("price_perf", []))
        if not pp.empty:
            colors2 = ["#2ecc71" if v >= 0 else "#e74c3c" for v in pp["total_pnl"]]
            ax4.bar(range(len(pp)), pp["total_pnl"], color=colors2, alpha=0.8)
            ax4.set_xticks(range(len(pp)))
            ax4.set_xticklabels(pp["price_bucket"].astype(str), rotation=0)
            ax4.axhline(0, color="black", linewidth=0.5)
        ax4.set_ylabel("Total PnL ($)")
        ax4.set_title("Total PnL by Entry Price Bucket")
        ax4.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, perf: dict) -> ChartConfig:
        data = [
            {
                "metric": "total_signals",
                "value": perf.get("total_signals", 0),
            },
            {
                "metric": "win_rate_pct",
                "value": round(perf.get("win_rate", 0) * 100, 2),
            },
            {
                "metric": "total_pnl_dollars",
                "value": perf.get("total_pnl_dollars", 0),
            },
            {
                "metric": "sharpe",
                "value": perf.get("sharpe", 0),
            },
            {
                "metric": "max_drawdown_dollars",
                "value": perf.get("max_drawdown_dollars", 0),
            },
        ]
        return ChartConfig(
            type=ChartType.BAR,
            data=data,
            xKey="metric",
            yKeys=["value"],
            title=f"Mean-Reversion Backtest Summary (lookback={self.lookback}, z={self.threshold})",
            yUnit=UnitType.NUMBER,
        )
