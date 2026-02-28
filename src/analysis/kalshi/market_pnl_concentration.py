"""Market-level P&L concentration analysis.

Key question: Is maker profit distributed across many markets, or do a few 
blockbuster markets drive all profits? What fraction of markets are maker-
profitable? If maker alpha is concentrated in few markets, the strategy is
fragile and depends on identifying those markets ex-ante.

Also answers: what characterizes high-P&L markets (volume? price range? category?)
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


class MarketPnlConcentrationAnalysis(Analysis):
    """Analyze how maker/taker P&L is distributed across markets."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="market_pnl_concentration",
            description="How concentrated is maker profit across individual markets",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        """Execute the analysis and return outputs."""
        con = duckdb.connect()

        # Get per-market P&L
        market_pnl = con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT ticker, event_ticker, result, volume
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            ),
            trade_data AS (
                SELECT
                    t.ticker,
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.count AS contracts,
                    m.result,
                    m.volume AS market_volume,
                    -- Maker PnL per trade
                    CASE
                        WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN -(100 - t.yes_price) * t.count
                        WHEN t.taker_side = 'yes' AND m.result = 'no' THEN t.yes_price * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'no' THEN -(100 - t.no_price) * t.count
                        WHEN t.taker_side = 'no' AND m.result = 'yes' THEN t.no_price * t.count
                    END AS maker_pnl,
                    CASE
                        WHEN t.taker_side = 'yes' THEN t.no_price * t.count
                        ELSE t.yes_price * t.count
                    END AS maker_cost
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            )
            SELECT
                ticker,
                category,
                SUM(maker_pnl) AS maker_pnl,
                SUM(maker_cost) AS maker_cost,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count,
                AVG(yes_price) AS avg_yes_price,
                MAX(market_volume) AS market_volume
            FROM trade_data
            GROUP BY ticker, category
            """
        ).df()

        # Vectorized group mapping: map unique categories first
        unique_cats = market_pnl["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        market_pnl["group"] = market_pnl["category"].map(cat_to_group)
        market_pnl["maker_excess_pct"] = market_pnl["maker_pnl"] * 100.0 / market_pnl["maker_cost"].replace(0, np.nan)
        market_pnl = market_pnl.sort_values("maker_pnl", ascending=False).reset_index(drop=True)

        total_maker_pnl = market_pnl["maker_pnl"].sum()
        total_markets = len(market_pnl)

        # Concentration stats
        market_pnl_sorted = market_pnl.sort_values("maker_pnl", ascending=False)
        cumulative_pnl = market_pnl_sorted["maker_pnl"].cumsum()

        # Find how many markets needed for X% of total profit
        pct_thresholds = [0.25, 0.50, 0.75, 0.90, 0.95]
        concentration_stats = {}
        for pct in pct_thresholds:
            if total_maker_pnl > 0:
                n_markets = (cumulative_pnl >= total_maker_pnl * pct).idxmax() + 1
                concentration_stats[f"top_{int(pct*100)}pct_markets"] = int(n_markets)
                concentration_stats[f"top_{int(pct*100)}pct_fraction"] = round(int(n_markets) / total_markets, 6)

        # Profitable vs unprofitable markets
        profitable_markets = (market_pnl["maker_pnl"] > 0).sum()
        unprofitable_markets = (market_pnl["maker_pnl"] < 0).sum()
        breakeven_markets = (market_pnl["maker_pnl"] == 0).sum()

        profitable_pnl = market_pnl[market_pnl["maker_pnl"] > 0]["maker_pnl"].sum()
        unprofitable_pnl = market_pnl[market_pnl["maker_pnl"] < 0]["maker_pnl"].sum()

        # By group
        group_stats = (
            market_pnl.groupby("group")
            .agg(
                total_maker_pnl=("maker_pnl", "sum"),
                total_markets=("ticker", "count"),
                profitable_markets=("maker_pnl", lambda x: (x > 0).sum()),
                mean_maker_pnl=("maker_pnl", "mean"),
                median_maker_pnl=("maker_pnl", "median"),
                total_contracts=("total_contracts", "sum"),
            )
            .reset_index()
        )
        group_stats["win_rate"] = group_stats["profitable_markets"] / group_stats["total_markets"]
        group_stats = group_stats.sort_values("total_maker_pnl", ascending=False).reset_index(drop=True)

        # Build summary output
        summary_rows = []
        summary_rows.append({
            "metric": "total_markets",
            "value": total_markets,
        })
        summary_rows.append({
            "metric": "profitable_markets",
            "value": profitable_markets,
        })
        summary_rows.append({
            "metric": "unprofitable_markets",
            "value": unprofitable_markets,
        })
        summary_rows.append({
            "metric": "breakeven_markets",
            "value": breakeven_markets,
        })
        summary_rows.append({
            "metric": "maker_win_rate",
            "value": round(profitable_markets / total_markets, 6),
        })
        summary_rows.append({
            "metric": "total_maker_pnl_cents",
            "value": total_maker_pnl,
        })
        summary_rows.append({
            "metric": "profitable_pnl_cents",
            "value": profitable_pnl,
        })
        summary_rows.append({
            "metric": "unprofitable_pnl_cents",
            "value": unprofitable_pnl,
        })
        summary_rows.append({
            "metric": "profit_factor",
            "value": round(profitable_pnl / abs(unprofitable_pnl), 4) if unprofitable_pnl != 0 else float("inf"),
        })
        for k, v in concentration_stats.items():
            summary_rows.append({"metric": k, "value": v})

        summary_df = pd.DataFrame(summary_rows)

        # Combined output: summary + group stats + top/bottom 50 markets
        top50 = market_pnl.head(50).copy()
        top50["rank"] = range(1, len(top50) + 1)
        top50["section"] = "top50"

        bottom50 = market_pnl.tail(50).copy()
        bottom50["rank"] = range(total_markets - 49, total_markets + 1)
        bottom50["section"] = "bottom50"

        group_stats["section"] = "group_summary"
        summary_df["section"] = "overall_summary"

        fig = self._create_figure(market_pnl, group_stats, concentration_stats, total_maker_pnl, total_markets, profitable_markets)
        chart = self._create_chart(group_stats)

        # For CSV: concat everything
        output_df = pd.concat([summary_df, group_stats, top50, bottom50], ignore_index=True)

        return AnalysisOutput(
            figure=fig,
            data=output_df,
            chart=chart,
            metadata={
                "total_markets": total_markets,
                "profitable_fraction": round(profitable_markets / total_markets, 4),
                "profit_factor": round(profitable_pnl / abs(unprofitable_pnl), 4) if unprofitable_pnl != 0 else None,
                **concentration_stats,
            },
        )

    def _create_figure(self, market_pnl, group_stats, concentration_stats, total_pnl, total_markets, profitable_markets):
        """Create multi-panel concentration analysis figure."""
        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # Panel 1: Lorenz curve of maker P&L
        ax1 = axes[0, 0]
        sorted_pnl = market_pnl.sort_values("maker_pnl", ascending=False)["maker_pnl"].values
        cumulative = np.cumsum(sorted_pnl)
        x = np.arange(1, len(cumulative) + 1) / len(cumulative) * 100
        y = cumulative / total_pnl * 100

        ax1.plot(x, y, color="#3498db", linewidth=2)
        ax1.plot([0, 100], [0, 100], "--", color="gray", alpha=0.5, label="Equal distribution")
        ax1.fill_between(x, y, alpha=0.1, color="#3498db")
        ax1.set_xlabel("% of Markets (sorted by maker P&L)")
        ax1.set_ylabel("% of Total Maker Profit")
        ax1.set_title("Lorenz Curve: Maker Profit Concentration")
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Add key annotations
        for pct_key, n_val in concentration_stats.items():
            if "fraction" in pct_key:
                pct_label = pct_key.replace("top_", "").replace("pct_fraction", "%")
                ax1.axhline(int(pct_label.replace("%", "")), color="red", linestyle=":", alpha=0.3)

        # Panel 2: Histogram of per-market maker P&L
        ax2 = axes[0, 1]
        pnl_clipped = np.clip(market_pnl["maker_pnl"].values / 100, -1000, 1000)  # clip to $±1000
        ax2.hist(pnl_clipped, bins=100, color="#3498db", alpha=0.7, edgecolor="black", linewidth=0.3)
        ax2.axvline(0, color="red", linewidth=1.5)
        ax2.set_xlabel("Maker P&L per Market ($)")
        ax2.set_ylabel("Number of Markets")
        ax2.set_title(f"Distribution of Per-Market Maker P&L\n(Win rate: {profitable_markets}/{total_markets} = {profitable_markets/total_markets*100:.1f}%)")
        ax2.set_yscale("log")
        ax2.grid(alpha=0.3)

        # Panel 3: Group-level bar chart
        ax3 = axes[1, 0]
        gs = group_stats.sort_values("total_maker_pnl", ascending=True)
        colors_bar = ["#2ecc71" if v > 0 else "#e74c3c" for v in gs["total_maker_pnl"]]
        ax3.barh(range(len(gs)), gs["total_maker_pnl"] / 1e8, color=colors_bar)
        ax3.set_yticks(range(len(gs)))
        ax3.set_yticklabels(gs["group"])
        ax3.set_xlabel("Total Maker P&L ($M)")
        ax3.set_title("Total Maker P&L by Category Group")
        ax3.axvline(0, color="black", linewidth=0.5)
        ax3.grid(axis="x", alpha=0.3)

        # Panel 4: Win rate by group
        ax4 = axes[1, 1]
        gs2 = group_stats.sort_values("win_rate", ascending=True)
        ax4.barh(range(len(gs2)), gs2["win_rate"] * 100, color="#9b59b6", alpha=0.8)
        ax4.set_yticks(range(len(gs2)))
        ax4.set_yticklabels(gs2["group"])
        ax4.set_xlabel("Maker Win Rate (%)")
        ax4.set_title("% of Markets Where Maker is Profitable")
        ax4.axvline(50, color="red", linewidth=1, linestyle="--", label="50%")
        ax4.legend()
        ax4.grid(axis="x", alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, group_stats):
        chart_data = []
        for _, row in group_stats.iterrows():
            chart_data.append({
                "group": row["group"],
                "total_maker_pnl": round(float(row["total_maker_pnl"]) / 100, 2),
                "win_rate": round(float(row["win_rate"]) * 100, 2),
                "total_markets": int(row["total_markets"]),
                "mean_maker_pnl": round(float(row["mean_maker_pnl"]) / 100, 2),
            })
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="group",
            yKeys=["total_maker_pnl", "win_rate"],
            title="Maker P&L Concentration by Category Group",
            yUnit=UnitType.DOLLARS,
        )
