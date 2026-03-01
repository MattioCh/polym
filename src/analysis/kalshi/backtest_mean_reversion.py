"""Mean-reversion backtest on Kalshi trade data.

Strategy:
  At each trade, compute the rolling VWAP of the preceding ``window`` trades
  for that market.  If the current trade price deviates from the VWAP by more
  than ``entry_threshold`` cents, simulate entering a counter-trend position
  (taker buy when price is below VWAP, taker sell when above) and hold until
  either the price reverts to the VWAP or ``max_hold`` trades elapse.

Outputs:
  - Equity curve (cumulative simulated P&L over time)
  - Per-market P&L breakdown
  - Summary statistics: total return, Sharpe ratio, max drawdown, win rate
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class BacktestMeanReversionAnalysis(Analysis):
    """Backtest a simple mean-reversion strategy on Kalshi historical trades.

    The strategy looks for short-term price deviations from a rolling VWAP
    and fades the move, holding until reversion or a max-hold threshold.
    """

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
        window: int = 20,
        entry_threshold: float = 3.0,
        max_hold: int = 10,
    ):
        super().__init__(
            name="backtest_mean_reversion",
            description="Mean-reversion strategy backtest on Kalshi trade data",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")
        self.window = window
        self.entry_threshold = entry_threshold
        self.max_hold = max_hold

    def run(self) -> AnalysisOutput:
        """Execute the backtest and return outputs."""
        con = duckdb.connect()

        # Load resolved trades with market result for scoring
        raw = con.execute(
            f"""
            WITH resolved AS (
                SELECT ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            )
            SELECT
                t.ticker,
                t.yes_price AS price,
                t.count AS qty,
                t.taker_side,
                t.created_time,
                m.result
            FROM '{self.trades_dir}/*.parquet' t
            INNER JOIN resolved m ON t.ticker = m.ticker
            WHERE t.yes_price BETWEEN 1 AND 99
            ORDER BY t.ticker, t.created_time
            """
        ).df()

        if raw.empty:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            ax.set_title("Backtest Mean Reversion: No Data")
            return AnalysisOutput(figure=fig, data=pd.DataFrame())

        pnl_records = self._run_backtest(raw)
        summary_df, market_pnl = self._compute_summary(pnl_records)
        fig = self._create_figure(pnl_records, market_pnl, summary_df)
        chart = self._create_chart(pnl_records)

        output_df = pd.concat([summary_df, market_pnl], ignore_index=True)
        return AnalysisOutput(figure=fig, data=output_df, chart=chart)

    def _run_backtest(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Simulate the mean-reversion strategy trade-by-trade.

        Returns a DataFrame of simulated trades with columns:
        ticker, entry_price, exit_price, direction, pnl_cents, entry_time, exit_time.
        """
        records = []

        for ticker, group in raw.groupby("ticker"):
            group = group.sort_values("created_time").reset_index(drop=True)
            prices = group["price"].to_numpy()
            times = group["created_time"].to_numpy()
            result = group["result"].iloc[0]  # market outcome

            price_history: list[float] = []
            i = 0
            while i < len(prices):
                p = float(prices[i])
                price_history.append(p)
                if len(price_history) > self.window:
                    price_history.pop(0)

                if len(price_history) < self.window:
                    i += 1
                    continue

                sma = float(np.mean(price_history))
                deviation = p - sma

                if abs(deviation) < self.entry_threshold:
                    i += 1
                    continue

                # Fade the move: sell if above SMA, buy if below
                direction = "sell" if deviation > 0 else "buy"
                entry_price = p
                entry_time = times[i]

                # Simulate hold: exit when price reverts or max_hold elapses
                exit_price = None
                exit_time = None
                for j in range(i + 1, min(i + 1 + self.max_hold, len(prices))):
                    next_p = float(prices[j])
                    reverted = (direction == "sell" and next_p <= sma) or (
                        direction == "buy" and next_p >= sma
                    )
                    if reverted:
                        exit_price = next_p
                        exit_time = times[j]
                        break

                if exit_price is None:
                    # Force exit at last available price within hold window
                    last_j = min(i + self.max_hold, len(prices) - 1)
                    exit_price = float(prices[last_j])
                    exit_time = times[last_j]

                # P&L: for a taker-buy, profit when exit > entry; sell is inverse
                if direction == "buy":
                    pnl_cents = exit_price - entry_price
                else:
                    pnl_cents = entry_price - exit_price

                records.append({
                    "ticker": ticker,
                    "direction": direction,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl_cents": pnl_cents,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "result": result,
                })
                # Skip ahead past the hold period
                i += self.max_hold

        if not records:
            return pd.DataFrame(columns=["ticker", "direction", "entry_price", "exit_price", "pnl_cents", "entry_time", "exit_time", "result"])

        df = pd.DataFrame(records)
        df["entry_time"] = pd.to_datetime(df["entry_time"], utc=True, errors="coerce")
        df = df.sort_values("entry_time").reset_index(drop=True)
        df["cumulative_pnl"] = df["pnl_cents"].cumsum()
        return df

    def _compute_summary(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Compute summary statistics and per-market P&L."""
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        n = len(df)
        wins = (df["pnl_cents"] > 0).sum()
        losses = (df["pnl_cents"] < 0).sum()
        total_pnl = df["pnl_cents"].sum()

        returns = df["pnl_cents"].values
        sharpe = (returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0.0

        cum = df["cumulative_pnl"].values
        running_max = np.maximum.accumulate(cum)
        drawdowns = cum - running_max
        max_drawdown = drawdowns.min()

        summary_rows = [
            {"metric": "total_trades", "value": n, "section": "summary"},
            {"metric": "wins", "value": int(wins), "section": "summary"},
            {"metric": "losses", "value": int(losses), "section": "summary"},
            {"metric": "win_rate", "value": round(wins / n, 4) if n else 0, "section": "summary"},
            {"metric": "total_pnl_cents", "value": round(total_pnl, 2), "section": "summary"},
            {"metric": "total_pnl_usd", "value": round(total_pnl / 100, 2), "section": "summary"},
            {"metric": "avg_pnl_per_trade_cents", "value": round(total_pnl / n, 2) if n else 0, "section": "summary"},
            {"metric": "annualised_sharpe", "value": round(float(sharpe), 4), "section": "summary"},
            {"metric": "max_drawdown_cents", "value": round(float(max_drawdown), 2), "section": "summary"},
            {"metric": "window", "value": self.window, "section": "params"},
            {"metric": "entry_threshold_cents", "value": self.entry_threshold, "section": "params"},
            {"metric": "max_hold_trades", "value": self.max_hold, "section": "params"},
        ]
        summary_df = pd.DataFrame(summary_rows)

        market_pnl = (
            df.groupby("ticker")
            .agg(trades=("pnl_cents", "count"), total_pnl_cents=("pnl_cents", "sum"))
            .reset_index()
            .sort_values("total_pnl_cents", ascending=False)
        )
        market_pnl["section"] = "per_market"

        return summary_df, market_pnl

    def _create_figure(self, df: pd.DataFrame, market_pnl: pd.DataFrame, summary_df: pd.DataFrame) -> plt.Figure:
        """Create a 2×2 figure: equity curve, P&L distribution, per-market bar, win/loss."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        fig.suptitle(
            f"Mean-Reversion Backtest  |  window={self.window}  "
            f"threshold={self.entry_threshold}¢  hold={self.max_hold}",
            fontsize=13,
        )

        # Panel 1: Equity curve
        ax1 = axes[0, 0]
        if not df.empty:
            ax1.plot(df["cumulative_pnl"].values / 100, color="#3498db", linewidth=1.5)
            ax1.axhline(0, color="gray", linewidth=0.8, linestyle="--")
            ax1.fill_between(
                range(len(df)),
                df["cumulative_pnl"].values / 100,
                where=df["cumulative_pnl"] >= 0,
                alpha=0.2, color="#2ecc71",
            )
            ax1.fill_between(
                range(len(df)),
                df["cumulative_pnl"].values / 100,
                where=df["cumulative_pnl"] < 0,
                alpha=0.2, color="#e74c3c",
            )
        ax1.set_xlabel("Trade #")
        ax1.set_ylabel("Cumulative P&L ($)")
        ax1.set_title("Equity Curve")
        ax1.grid(alpha=0.3)

        # Panel 2: P&L per trade distribution
        ax2 = axes[0, 1]
        if not df.empty:
            pnl = df["pnl_cents"].values / 100
            ax2.hist(pnl, bins=40, color="#9b59b6", alpha=0.7, edgecolor="black", linewidth=0.3)
            ax2.axvline(0, color="red", linewidth=1.5)
            ax2.axvline(pnl.mean(), color="orange", linewidth=1.5, linestyle="--", label=f"mean={pnl.mean():.3f}")
        ax2.set_xlabel("P&L per Trade ($)")
        ax2.set_ylabel("Count")
        ax2.set_title("P&L Distribution per Trade")
        ax2.legend(fontsize=8)
        ax2.set_yscale("log" if not df.empty and len(df) > 20 else "linear")
        ax2.grid(alpha=0.3)

        # Panel 3: Top / bottom market P&L
        ax3 = axes[1, 0]
        if not market_pnl.empty:
            top = pd.concat([market_pnl.head(10), market_pnl.tail(5)]).drop_duplicates("ticker")
            colors = ["#2ecc71" if v > 0 else "#e74c3c" for v in top["total_pnl_cents"]]
            ax3.barh(range(len(top)), top["total_pnl_cents"].values / 100, color=colors)
            ax3.set_yticks(range(len(top)))
            ax3.set_yticklabels(top["ticker"], fontsize=7)
            ax3.axvline(0, color="black", linewidth=0.5)
        ax3.set_xlabel("P&L ($)")
        ax3.set_title("Per-Market P&L (top 10 + bottom 5)")
        ax3.grid(axis="x", alpha=0.3)

        # Panel 4: Summary stats text
        ax4 = axes[1, 1]
        ax4.axis("off")
        if not summary_df.empty:
            summary_subset = summary_df[summary_df["section"] == "summary"]
            text = "\n".join(
                f"{row['metric']:30s} {row['value']}"
                for _, row in summary_subset.iterrows()
            )
            ax4.text(0.05, 0.95, text, transform=ax4.transAxes,
                     fontsize=9, verticalalignment="top", fontfamily="monospace")
        ax4.set_title("Summary Statistics")

        plt.tight_layout()
        return fig

    def _create_chart(self, df: pd.DataFrame) -> ChartConfig:
        """Return chart config for the equity curve."""
        if df.empty:
            chart_data = []
        else:
            step = max(1, len(df) // 200)
            chart_data = [
                {"trade": int(i), "cumulative_pnl_usd": round(float(row["cumulative_pnl"]) / 100, 2)}
                for i, row in df.iloc[::step].iterrows()
            ]
        return ChartConfig(
            type=ChartType.LINE,
            data=chart_data,
            xKey="trade",
            yKeys=["cumulative_pnl_usd"],
            title=f"Mean-Reversion Equity Curve (window={self.window}, threshold={self.entry_threshold}¢)",
            yUnit=UnitType.DOLLARS,
            xLabel="Trade #",
            yLabel="Cumulative P&L ($)",
        )
