"""Taker-positive niches: price × category intersections where takers have +EV.

Key question: Where do takers actually win? Most analysis shows makers dominate
overall, but there must be niches where informed takers extract value. This
analysis identifies specific (category, price_bucket) cells where taker EV > 0,
which are candidate alpha sources for active trading strategies.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.analysis.kalshi.util.categories import CATEGORY_SQL, GROUP_COLORS, get_group
from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class TakerAlphaNichesAnalysis(Analysis):
    """Find category × price niches where takers have positive excess returns."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="taker_alpha_niches",
            description="Category × price niches where takers achieve positive excess return",
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
                SELECT ticker, event_ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            ),
            trade_data AS (
                SELECT
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.count AS contracts,
                    m.result,
                    CASE
                        WHEN t.taker_side = 'yes' THEN t.yes_price
                        ELSE t.no_price
                    END AS taker_price,
                    CASE
                        WHEN t.taker_side = m.result THEN 1 ELSE 0
                    END AS taker_won
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            )
            SELECT
                category,
                CASE
                    WHEN taker_price BETWEEN 1 AND 10 THEN '01-10'
                    WHEN taker_price BETWEEN 11 AND 20 THEN '11-20'
                    WHEN taker_price BETWEEN 21 AND 30 THEN '21-30'
                    WHEN taker_price BETWEEN 31 AND 40 THEN '31-40'
                    WHEN taker_price BETWEEN 41 AND 50 THEN '41-50'
                    WHEN taker_price BETWEEN 51 AND 60 THEN '51-60'
                    WHEN taker_price BETWEEN 61 AND 70 THEN '61-70'
                    WHEN taker_price BETWEEN 71 AND 80 THEN '71-80'
                    WHEN taker_price BETWEEN 81 AND 90 THEN '81-90'
                    WHEN taker_price BETWEEN 91 AND 99 THEN '91-99'
                END AS price_bucket,
                -- Taker PnL
                SUM(CASE
                    WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                    WHEN taker_side = 'yes' AND result = 'no' THEN -yes_price * contracts
                    WHEN taker_side = 'no' AND result = 'no' THEN (100 - no_price) * contracts
                    WHEN taker_side = 'no' AND result = 'yes' THEN -no_price * contracts
                END) AS taker_pnl,
                SUM(CASE
                    WHEN taker_side = 'yes' THEN yes_price * contracts
                    ELSE no_price * contracts
                END) AS taker_cost,
                SUM(contracts) AS total_contracts,
                SUM(taker_won * contracts) * 1.0 / SUM(contracts) AS taker_win_rate,
                AVG(taker_price) AS avg_taker_price,
                COUNT(*) AS trade_count
            FROM trade_data
            GROUP BY category, price_bucket
            HAVING SUM(contracts) >= 50000  -- minimum volume filter
            """
        ).df()

        # Compute taker excess return
        df["taker_excess_pct"] = df["taker_pnl"] * 100.0 / df["taker_cost"]
        df["taker_ev"] = 100 * df["taker_win_rate"] - df["avg_taker_price"]

        # Map to groups
        df["group"] = df["category"].apply(get_group)

        # Aggregate by group × price_bucket
        group_rows = []
        for (group, bucket), gdf in df.groupby(["group", "price_bucket"]):
            total_c = gdf["total_contracts"].sum()
            total_pnl = gdf["taker_pnl"].sum()
            total_cost = gdf["taker_cost"].sum()
            if total_cost == 0:
                continue
            group_rows.append(
                {
                    "group": group,
                    "price_bucket": bucket,
                    "taker_pnl": total_pnl,
                    "taker_cost": total_cost,
                    "taker_excess_pct": total_pnl * 100.0 / total_cost,
                    "total_contracts": total_c,
                }
            )

        group_df = pd.DataFrame(group_rows)

        # Identify positive-EV niches (taker wins)
        positive_niches = group_df[group_df["taker_excess_pct"] > 0].sort_values(
            "taker_excess_pct", ascending=False
        )

        # Also keep raw category-level for CSV output
        # Filter to taker-positive and sort
        raw_positive = df[df["taker_excess_pct"] > 0].sort_values(
            "taker_excess_pct", ascending=False
        )

        # Combine: group-level summary and category-level detail
        group_df_out = group_df.copy()
        group_df_out["level"] = "group"
        group_df_out["category"] = group_df_out["group"]

        raw_out = df[["category", "group", "price_bucket", "taker_pnl", "taker_cost",
                       "taker_excess_pct", "total_contracts", "taker_win_rate", "taker_ev"]].copy()
        raw_out["level"] = "category"

        combined = pd.concat([group_df_out, raw_out], ignore_index=True)
        combined = combined.sort_values(["level", "group", "price_bucket", "taker_excess_pct"],
                                         ascending=[True, True, True, False])

        fig = self._create_figure(group_df, positive_niches, raw_positive)
        chart = self._create_chart(group_df)

        return AnalysisOutput(
            figure=fig,
            data=combined,
            chart=chart,
            metadata={
                "total_positive_group_niches": len(positive_niches),
                "total_positive_category_niches": len(raw_positive),
                "total_group_cells": len(group_df),
                "total_category_cells": len(df),
            },
        )

    def _create_figure(
        self,
        group_df: pd.DataFrame,
        positive_niches: pd.DataFrame,
        raw_positive: pd.DataFrame,
    ) -> plt.Figure:
        """Create visualization of taker alpha niches."""
        fig, axes = plt.subplots(1, 2, figsize=(18, 10))

        # Left: Heatmap of taker excess % by group × price
        groups = sorted(group_df["group"].unique())
        buckets = sorted(group_df["price_bucket"].unique())

        matrix = np.full((len(groups), len(buckets)), np.nan)
        for _, row in group_df.iterrows():
            i = groups.index(row["group"])
            j = buckets.index(row["price_bucket"])
            matrix[i, j] = row["taker_excess_pct"]

        ax1 = axes[0]
        vmax = max(abs(np.nanmin(matrix)), abs(np.nanmax(matrix)))
        vmax = min(vmax, 30)  # cap for readability
        im = ax1.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)

        ax1.set_xticks(range(len(buckets)))
        ax1.set_xticklabels(buckets, rotation=45, ha="right")
        ax1.set_yticks(range(len(groups)))
        ax1.set_yticklabels(groups)

        for i in range(len(groups)):
            for j in range(len(buckets)):
                val = matrix[i, j]
                if not np.isnan(val):
                    text = f"{val:+.1f}%"
                    color = "white" if abs(val) > vmax * 0.5 else "black"
                    ax1.text(j, i, text, ha="center", va="center", fontsize=7, color=color)

        ax1.set_xlabel("Taker Price Bucket (cents)")
        ax1.set_ylabel("Category Group")
        ax1.set_title("Taker Excess Return by Group × Taker Price\n(Green = Taker Wins)")
        fig.colorbar(im, ax=ax1, label="Taker Excess Return (%)", shrink=0.8)

        # Right: Top 20 raw category niches where takers win
        ax2 = axes[1]
        top_niches = raw_positive.head(20)
        if len(top_niches) > 0:
            labels = [
                f"{row['group']}: {row['category']} @ {row['price_bucket']}"
                for _, row in top_niches.iterrows()
            ]
            colors = [GROUP_COLORS.get(row["group"], "#aaaaaa") for _, row in top_niches.iterrows()]
            y = np.arange(len(top_niches))
            ax2.barh(y, top_niches["taker_excess_pct"], color=colors, alpha=0.8)
            ax2.set_yticks(y)
            ax2.set_yticklabels(labels, fontsize=7)
            # Add volume annotation
            for i, (_, row) in enumerate(top_niches.iterrows()):
                vol = row["total_contracts"]
                label = f"{vol / 1e6:.1f}M" if vol > 1e6 else f"{vol / 1e3:.0f}K"
                ax2.text(
                    row["taker_excess_pct"] + 0.3,
                    i,
                    label,
                    va="center",
                    fontsize=7,
                    color="gray",
                )
            ax2.set_xlabel("Taker Excess Return (%)")
            ax2.set_title("Top 20 Taker-Positive Niches\n(with volume)")
        else:
            ax2.text(0.5, 0.5, "No taker-positive niches found", ha="center", va="center")

        ax2.axvline(0, color="black", linewidth=0.5)
        ax2.grid(axis="x", alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, group_df: pd.DataFrame) -> ChartConfig:
        """Create chart configuration."""
        chart_data = []
        for _, row in group_df.iterrows():
            chart_data.append(
                {
                    "group": row["group"],
                    "price_bucket": row["price_bucket"],
                    "taker_excess_pct": round(float(row["taker_excess_pct"]), 4),
                    "total_contracts": int(row["total_contracts"]),
                    "taker_positive": bool(row["taker_excess_pct"] > 0),
                }
            )

        return ChartConfig(
            type=ChartType.HEATMAP,
            data=chart_data,
            xKey="price_bucket",
            yKey="group",
            title="Taker Excess Return by Category × Price",
            yUnit=UnitType.PERCENT,
        )
