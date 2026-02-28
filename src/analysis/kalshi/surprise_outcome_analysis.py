"""Surprise outcome analysis: maker P&L when market result contradicts last price.

Key question: Where does maker profit come from? Two hypotheses:
1. "Grind" hypothesis: makers earn a small spread on correctly-priced markets
2. "Surprise" hypothesis: makers earn windfall when outcomes surprise the market

If last_price was 80 and result was "no", that's a surprise. Does the maker
P&L concentrate in these surprise outcomes, or is it evenly distributed?

This directly tests whether maker alpha is systematic (grinding) or event-driven
(absorbing surprise risk), which fundamentally changes strategy design.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class SurpriseOutcomeAnalysis(Analysis):
    """Analyze maker P&L as a function of outcome surprise (last_price vs result)."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="surprise_outcome_analysis",
            description="Maker P&L distribution by outcome surprise level",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        """Execute the analysis and return outputs."""
        con = duckdb.connect()

        df = con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT ticker, result, last_price
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
                  AND last_price IS NOT NULL
                  AND last_price BETWEEN 1 AND 99
            ),
            trade_data AS (
                SELECT
                    t.ticker,
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.count AS contracts,
                    m.result,
                    m.last_price,
                    -- Surprise: how far was last_price from truth?
                    -- If result=yes, truth=100, surprise = 100 - last_price
                    -- If result=no, truth=0, surprise = last_price
                    CASE
                        WHEN m.result = 'yes' THEN 100 - m.last_price
                        ELSE m.last_price
                    END AS surprise_magnitude,
                    -- Maker PnL
                    CASE
                        WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN -(100 - t.yes_price) * t.count
                        WHEN t.taker_side = 'yes' AND m.result = 'no' THEN t.yes_price * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'no' THEN -(100 - t.no_price) * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'yes' THEN t.no_price * t.count
                    END AS maker_pnl,
                    CASE
                        WHEN t.taker_side = 'yes' THEN t.no_price * t.count
                        ELSE t.yes_price * t.count
                    END AS maker_cost,
                    -- Taker PnL
                    CASE
                        WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN (100 - t.yes_price) * t.count
                        WHEN t.taker_side = 'yes' AND m.result = 'no' THEN -t.yes_price * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'no' THEN (100 - t.no_price) * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'yes' THEN -t.no_price * t.count
                    END AS taker_pnl,
                    CASE
                        WHEN t.taker_side = 'yes' THEN t.yes_price * t.count
                        ELSE t.no_price * t.count
                    END AS taker_cost
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            )
            SELECT
                CASE
                    WHEN surprise_magnitude BETWEEN 0 AND 5 THEN '00-05 (Expected)'
                    WHEN surprise_magnitude BETWEEN 6 AND 15 THEN '06-15 (Minor)'
                    WHEN surprise_magnitude BETWEEN 16 AND 30 THEN '16-30 (Moderate)'
                    WHEN surprise_magnitude BETWEEN 31 AND 50 THEN '31-50 (Upset)'
                    WHEN surprise_magnitude BETWEEN 51 AND 75 THEN '51-75 (Major Upset)'
                    ELSE '76-100 (Shock)'
                END AS surprise_bucket,
                MIN(surprise_magnitude) AS min_surprise,
                SUM(maker_pnl) AS maker_pnl,
                SUM(maker_cost) AS maker_cost,
                SUM(taker_pnl) AS taker_pnl,
                SUM(taker_cost) AS taker_cost,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count,
                COUNT(DISTINCT ticker) AS unique_markets
            FROM trade_data
            GROUP BY surprise_bucket
            ORDER BY MIN(surprise_magnitude)
            """
        ).df()

        df["maker_excess_pct"] = df["maker_pnl"] * 100.0 / df["maker_cost"].replace(0, np.nan)
        df["taker_excess_pct"] = df["taker_pnl"] * 100.0 / df["taker_cost"].replace(0, np.nan)
        df["maker_pnl_share"] = df["maker_pnl"] / df["maker_pnl"].sum() * 100

        # Also get per-market surprise distribution
        market_surprise = con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT ticker, result, last_price
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
                  AND last_price IS NOT NULL
                  AND last_price BETWEEN 1 AND 99
            ),
            trade_pnl AS (
                SELECT
                    t.ticker,
                    CASE
                        WHEN m.result = 'yes' THEN 100 - m.last_price
                        ELSE m.last_price
                    END AS surprise_magnitude,
                    SUM(CASE
                        WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN -(100 - t.yes_price) * t.count
                        WHEN t.taker_side = 'yes' AND m.result = 'no' THEN t.yes_price * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'no' THEN -(100 - t.no_price) * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'yes' THEN t.no_price * t.count
                    END) AS maker_pnl,
                    SUM(t.count) AS contracts
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
                GROUP BY t.ticker, surprise_magnitude
            )
            SELECT
                surprise_magnitude,
                COUNT(*) AS num_markets,
                SUM(maker_pnl) AS total_maker_pnl,
                AVG(maker_pnl) AS avg_maker_pnl,
                SUM(CASE WHEN maker_pnl > 0 THEN 1 ELSE 0 END) AS maker_profitable_markets,
                SUM(contracts) AS total_contracts
            FROM trade_pnl
            GROUP BY surprise_magnitude
            ORDER BY surprise_magnitude
            """
        ).df()

        market_surprise["maker_win_rate"] = (
            market_surprise["maker_profitable_markets"] / market_surprise["num_markets"]
        )

        fig = self._create_figure(df, market_surprise)
        chart = self._create_chart(df)

        # Combine both datasets for output
        df["section"] = "trade_level"
        market_surprise["section"] = "market_level"
        combined = pd.concat([df, market_surprise], ignore_index=True)

        return AnalysisOutput(
            figure=fig,
            data=combined,
            chart=chart,
            metadata={
                "total_maker_pnl": float(df["maker_pnl"].sum()),
                "surprise_above_30_pnl_share": float(
                    df[df["min_surprise"] >= 31]["maker_pnl"].sum() / df["maker_pnl"].sum() * 100
                ),
            },
        )

    def _create_figure(self, df, market_surprise):
        """Create surprise outcome visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # Panel 1: Maker excess by surprise bucket
        ax1 = axes[0, 0]
        x = np.arange(len(df))
        width = 0.35
        ax1.bar(x - width / 2, df["maker_excess_pct"], width, label="Maker", color="#2ecc71")
        ax1.bar(x + width / 2, df["taker_excess_pct"], width, label="Taker", color="#e74c3c")
        ax1.set_xticks(x)
        ax1.set_xticklabels(df["surprise_bucket"], rotation=30, ha="right", fontsize=8)
        ax1.set_ylabel("Excess Return (%)")
        ax1.set_title("Maker vs Taker Excess Return by Surprise Level")
        ax1.legend()
        ax1.axhline(0, color="black", linewidth=0.5)
        ax1.grid(axis="y", alpha=0.3)

        # Panel 2: Share of total maker PnL by surprise bucket
        ax2 = axes[0, 1]
        colors_pie = ["#2ecc71", "#27ae60", "#f39c12", "#e67e22", "#e74c3c", "#c0392b"][:len(df)]
        ax2.bar(x, df["maker_pnl_share"], color=colors_pie, alpha=0.8)
        ax2.set_xticks(x)
        ax2.set_xticklabels(df["surprise_bucket"], rotation=30, ha="right", fontsize=8)
        ax2.set_ylabel("Share of Total Maker PnL (%)")
        ax2.set_title("Where Does Maker Profit Come From?")
        ax2.grid(axis="y", alpha=0.3)

        # Annotate absolute PnL
        for i, (_, row) in enumerate(df.iterrows()):
            pnl_m = row["maker_pnl"] / 1e8  # to millions of dollars
            ax2.text(i, row["maker_pnl_share"] + 0.5, f"${pnl_m:.1f}M",
                     ha="center", fontsize=8, color="gray")

        # Panel 3: Maker win rate by surprise magnitude (scatter)
        ax3 = axes[1, 0]
        ms = market_surprise
        ax3.scatter(ms["surprise_magnitude"], ms["maker_win_rate"] * 100,
                    s=ms["num_markets"] / ms["num_markets"].max() * 100,
                    alpha=0.5, color="#3498db")
        # Add smoothed trend
        window = 5
        smoothed = ms["maker_win_rate"].rolling(window, center=True).mean() * 100
        ax3.plot(ms["surprise_magnitude"], smoothed, color="#e74c3c", linewidth=2, label=f"Smoothed ({window}-pt)")
        ax3.axhline(50, color="black", linewidth=0.5, linestyle="--")
        ax3.set_xlabel("Surprise Magnitude (cents from truth)")
        ax3.set_ylabel("Maker Win Rate (%)")
        ax3.set_title("Maker Win Rate vs Outcome Surprise")
        ax3.legend()
        ax3.grid(alpha=0.3)

        # Panel 4: Volume by surprise bucket
        ax4 = axes[1, 1]
        ax4.bar(x, df["total_contracts"] / 1e9, color="#9b59b6", alpha=0.8)
        ax4.set_xticks(x)
        ax4.set_xticklabels(df["surprise_bucket"], rotation=30, ha="right", fontsize=8)
        ax4.set_ylabel("Contracts (billions)")
        ax4.set_title("Trade Volume by Surprise Level")
        ax4.grid(axis="y", alpha=0.3)

        # Add unique markets count
        for i, (_, row) in enumerate(df.iterrows()):
            ax4.text(i, row["total_contracts"] / 1e9 + 0.05,
                     f"{int(row['unique_markets']):,} mkts",
                     ha="center", fontsize=7, color="gray")

        plt.tight_layout()
        return fig

    def _create_chart(self, df):
        chart_data = []
        for _, row in df.iterrows():
            chart_data.append({
                "surprise_bucket": row["surprise_bucket"],
                "maker_excess_pct": round(float(row["maker_excess_pct"]), 4),
                "taker_excess_pct": round(float(row["taker_excess_pct"]), 4),
                "maker_pnl_share": round(float(row["maker_pnl_share"]), 2),
                "total_contracts": int(row["total_contracts"]),
            })
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="surprise_bucket",
            yKeys=["maker_excess_pct", "taker_excess_pct"],
            title="Maker vs Taker Returns by Outcome Surprise",
            yUnit=UnitType.PERCENT,
            colors=["#2ecc71", "#e74c3c"],
        )
