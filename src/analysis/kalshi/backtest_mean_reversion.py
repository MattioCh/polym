"""Mean reversion backtest for Kalshi prediction markets with explicit position closing.

Strategy
--------
1. For each finalized market, compute a rolling VWAP (volume-weighted average price)
   over all trades seen so far.
2. Enter a LONG position on YES contracts whenever ``yes_price`` falls at least
   ``entry_threshold`` cents below the current rolling VWAP.
3. Close the position when a subsequent trade offers ``yes_price >= entry_vwap -
   reversion_margin``, capturing the spread without waiting for binary resolution.
4. Any position still open when the market ends is settled at the binary resolution
   value (100 if result='yes', 0 if result='no').

Key nuance vs. a hold-to-resolution backtest
---------------------------------------------
A plain hold-to-resolution model treats every open contract as a binary bet.
Here we model an explicit **close signal** so the strategy can capture mean
reversion profits independent of the final outcome.  The output separates:

* ``close_type='reversion'``  – trade closed early at the reversion price.
* ``close_type='resolution'`` – trade settled at the binary outcome.

Comparing the two groups reveals how much of the strategy's edge comes from
pure price-reversion versus directional accuracy.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType

# Minimum trades required per market to include it in the backtest.
MIN_TRADES_PER_MARKET = 10

# Default parameters (can be overridden in __init__).
DEFAULT_ENTRY_THRESHOLD = 5  # cents below VWAP required to enter
DEFAULT_REVERSION_MARGIN = 2  # cents; close when price >= entry_vwap - margin

# Binary resolution prices (cents).
YES_RESOLUTION_PRICE = 100
NO_RESOLUTION_PRICE = 0

# Maximum data points emitted in the chart config (downsampled if exceeded).
MAX_CHART_POINTS = 500


class BacktestMeanReversionAnalysis(Analysis):
    """Backtest a mean reversion strategy on Kalshi with explicit position closing.

    Parameters
    ----------
    trades_dir:
        Directory containing Kalshi trades parquet files.
    markets_dir:
        Directory containing Kalshi markets parquet files.
    entry_threshold:
        Minimum deviation (cents) below the rolling VWAP required to open a
        long position.  Default: 5.
    reversion_margin:
        Allowable shortfall (cents) below the entry VWAP at the close price.
        The position is closed when ``yes_price >= entry_vwap - reversion_margin``.
        Default: 2.
    """

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
        entry_threshold: int = DEFAULT_ENTRY_THRESHOLD,
        reversion_margin: int = DEFAULT_REVERSION_MARGIN,
    ):
        super().__init__(
            name="backtest_mean_reversion",
            description="Mean reversion backtest with explicit position close signals",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")
        self.entry_threshold = entry_threshold
        self.reversion_margin = reversion_margin

    # ------------------------------------------------------------------
    # Core simulation helpers
    # ------------------------------------------------------------------

    def _simulate_market(
        self,
        ticker: str,
        mkt_trades: pd.DataFrame,
        result: str,
    ) -> list[dict]:
        """Run the mean reversion simulation for a single market.

        Returns a list of position dicts with entry/exit prices and PnL.
        """
        resolution_price = YES_RESOLUTION_PRICE if result == "yes" else NO_RESOLUTION_PRICE
        prices = mkt_trades["yes_price"].values.astype(float)
        counts = mkt_trades["contracts"].values.astype(float)
        times = mkt_trades["created_time"].values

        # Rolling VWAP: vwap[i] = VWAP of all trades *before* trade i.
        cum_pv = np.cumsum(prices * counts)
        cum_qty = np.cumsum(counts)
        vwap = np.empty(len(prices))
        vwap[0] = prices[0]
        vwap[1:] = cum_pv[:-1] / cum_qty[:-1]

        open_positions: list[dict] = []
        positions: list[dict] = []

        for i in range(len(mkt_trades)):
            current_price = prices[i]
            current_vwap = vwap[i]
            current_time = pd.Timestamp(times[i])

            # --- Try to close open positions on reversion ---
            remaining: list[dict] = []
            for pos in open_positions:
                close_target = pos["entry_vwap"] - self.reversion_margin
                if current_price >= close_target:
                    hold_h = (current_time - pos["entry_time"]).total_seconds() / 3600.0
                    positions.append(
                        {
                            "ticker": ticker,
                            "entry_price": pos["entry_price"],
                            "entry_vwap": pos["entry_vwap"],
                            "exit_price": float(current_price),
                            "contracts": pos["contracts"],
                            "pnl": (current_price - pos["entry_price"]) * pos["contracts"],
                            "hold_hours": hold_h,
                            "close_type": "reversion",
                            "result": result,
                        }
                    )
                else:
                    remaining.append(pos)
            open_positions = remaining

            # --- Entry signal: price at least entry_threshold below VWAP ---
            if current_price <= current_vwap - self.entry_threshold:
                open_positions.append(
                    {
                        "entry_price": float(current_price),
                        "entry_vwap": float(current_vwap),
                        "entry_time": current_time,
                        "contracts": counts[i],
                    }
                )

        # --- Settle remaining open positions at binary resolution ---
        last_time = pd.Timestamp(times[-1])
        for pos in open_positions:
            hold_h = (last_time - pos["entry_time"]).total_seconds() / 3600.0
            positions.append(
                {
                    "ticker": ticker,
                    "entry_price": pos["entry_price"],
                    "entry_vwap": pos["entry_vwap"],
                    "exit_price": float(resolution_price),
                    "contracts": pos["contracts"],
                    "pnl": (resolution_price - pos["entry_price"]) * pos["contracts"],
                    "hold_hours": hold_h,
                    "close_type": "resolution",
                    "result": result,
                }
            )

        return positions

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self) -> AnalysisOutput:
        """Execute the mean reversion backtest and return outputs."""
        con = duckdb.connect()

        with self.progress("Loading market outcomes"):
            markets = con.execute(
                f"""
                SELECT ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
                """
            ).df()

        with self.progress("Loading trades"):
            trades = con.execute(
                f"""
                SELECT
                    t.ticker,
                    t.yes_price,
                    t.count AS contracts,
                    t.created_time
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN (
                    SELECT ticker
                    FROM '{self.markets_dir}/*.parquet'
                    WHERE status = 'finalized' AND result IN ('yes', 'no')
                ) m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
                ORDER BY t.ticker, t.created_time
                """
            ).df()

        result_map: dict[str, str] = dict(zip(markets["ticker"], markets["result"]))

        all_positions: list[dict] = []

        with self.progress("Simulating positions"):
            for ticker, mkt_trades in trades.groupby("ticker"):
                if len(mkt_trades) < MIN_TRADES_PER_MARKET:
                    continue
                mkt_trades = mkt_trades.reset_index(drop=True)
                result = result_map.get(str(ticker))
                if result is None:
                    continue
                all_positions.extend(self._simulate_market(str(ticker), mkt_trades, result))

        if not all_positions:
            empty_df = pd.DataFrame(
                columns=[
                    "ticker",
                    "entry_price",
                    "entry_vwap",
                    "exit_price",
                    "contracts",
                    "pnl",
                    "hold_hours",
                    "close_type",
                    "result",
                ]
            )
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "No mean reversion signals found", ha="center", va="center", fontsize=14)
            ax.set_title("Mean Reversion Backtest")
            ax.axis("off")
            return AnalysisOutput(figure=fig, data=empty_df)

        df = pd.DataFrame(all_positions)
        df["entry_deviation"] = df["entry_vwap"] - df["entry_price"]

        summary = (
            df.groupby("close_type")
            .agg(
                total_pnl=("pnl", "sum"),
                n_trades=("pnl", "count"),
                win_rate=("pnl", lambda x: float((x > 0).mean())),
                avg_hold_hours=("hold_hours", "mean"),
                avg_pnl_per_trade=("pnl", "mean"),
            )
            .reset_index()
        )

        fig = self._create_figure(df, summary)
        chart = self._create_chart(df)

        return AnalysisOutput(
            figure=fig,
            data=df,
            chart=chart,
            metadata=summary.to_dict(orient="records"),
        )

    # ------------------------------------------------------------------
    # Visualisation
    # ------------------------------------------------------------------

    def _create_figure(self, df: pd.DataFrame, summary: pd.DataFrame) -> plt.Figure:
        """Create a 2×2 panel figure summarising backtest results."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        close_styles = {
            "reversion": ("#2ecc71", "Reversion closed"),
            "resolution": ("#e74c3c", "Resolution settled"),
        }

        # Panel 1: PnL distribution by close type
        ax1 = axes[0, 0]
        for ctype, (color, label) in close_styles.items():
            subset = df.loc[df["close_type"] == ctype, "pnl"]
            if len(subset):
                ax1.hist(subset, bins=40, alpha=0.6, color=color, label=label, density=True)
        ax1.axvline(0, color="black", linewidth=1, linestyle="--")
        ax1.set_xlabel("PnL per trade (cents × contracts)")
        ax1.set_ylabel("Density")
        ax1.set_title("PnL Distribution by Close Type")
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Panel 2: Cumulative PnL
        ax2 = axes[0, 1]
        for ctype, (color, label) in close_styles.items():
            subset = df.loc[df["close_type"] == ctype, "pnl"].values
            if len(subset):
                ax2.plot(np.cumsum(subset), color=color, linewidth=1.5, label=label)
        ax2.axhline(0, color="black", linewidth=0.5, linestyle="--")
        ax2.set_xlabel("Trade #")
        ax2.set_ylabel("Cumulative PnL (cents × contracts)")
        ax2.set_title("Cumulative PnL by Close Type")
        ax2.legend()
        ax2.grid(alpha=0.3)

        # Panel 3: Win rate by close type
        ax3 = axes[1, 0]
        x = np.arange(len(summary))
        colors_bar = [close_styles.get(ct, ("#aaaaaa", ""))[0] for ct in summary["close_type"]]
        ax3.bar(x, summary["win_rate"] * 100, color=colors_bar, alpha=0.85)
        ax3.axhline(50, color="black", linewidth=0.8, linestyle="--", label="50% baseline")
        ax3.set_xticks(x)
        ax3.set_xticklabels(summary["close_type"].str.capitalize())
        ax3.set_ylabel("Win Rate (%)")
        ax3.set_ylim(0, 105)
        ax3.set_title("Win Rate by Close Type")
        ax3.legend()
        ax3.grid(axis="y", alpha=0.3)
        for i, row in summary.iterrows():
            ax3.text(int(i), row["win_rate"] * 100 + 2, f"n={int(row['n_trades'])}", ha="center", fontsize=9)

        # Panel 4: Distribution of entry deviations
        ax4 = axes[1, 1]
        ax4.hist(df["entry_deviation"], bins=30, color="#3498db", alpha=0.8)
        ax4.axvline(
            self.entry_threshold,
            color="#e74c3c",
            linewidth=1.5,
            linestyle="--",
            label=f"Entry threshold ({self.entry_threshold}¢)",
        )
        ax4.set_xlabel("Entry Deviation below VWAP (cents)")
        ax4.set_ylabel("Count")
        ax4.set_title("Distribution of Entry Deviations from VWAP")
        ax4.legend()
        ax4.grid(alpha=0.3)

        fig.suptitle(
            f"Mean Reversion Backtest  |  Entry ≥ {self.entry_threshold}¢ below VWAP  |  "
            f"Close when price ≥ VWAP − {self.reversion_margin}¢",
            fontsize=12,
        )
        plt.tight_layout()
        return fig

    def _create_chart(self, df: pd.DataFrame) -> ChartConfig:
        """Create chart config showing cumulative PnL for web display."""
        rev = df.loc[df["close_type"] == "reversion", "pnl"].values
        res = df.loc[df["close_type"] == "resolution", "pnl"].values

        max_len = max(len(rev), len(res), 1)
        rev_cum = np.cumsum(rev) if len(rev) else np.array([])
        res_cum = np.cumsum(res) if len(res) else np.array([])

        chart_data: list[dict] = []
        for i in range(max_len):
            point: dict = {"trade_index": i + 1}
            if i < len(rev_cum):
                point["Reversion Closed"] = int(rev_cum[i])
            if i < len(res_cum):
                point["Resolution Settled"] = int(res_cum[i])
            chart_data.append(point)

        # Downsample to at most MAX_CHART_POINTS points for the chart
        if len(chart_data) > MAX_CHART_POINTS:
            step = max(len(chart_data) // MAX_CHART_POINTS, 1)
            chart_data = chart_data[::step]

        return ChartConfig(
            type=ChartType.LINE,
            data=chart_data,
            xKey="trade_index",
            yKeys=["Reversion Closed", "Resolution Settled"],
            title="Mean Reversion Backtest: Cumulative PnL by Close Type",
            yUnit=UnitType.CENTS,
            xLabel="Trade #",
            yLabel="Cumulative PnL (cents × contracts)",
            colors={"Reversion Closed": "#2ecc71", "Resolution Settled": "#e74c3c"},
        )
