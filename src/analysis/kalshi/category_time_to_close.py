"""Category × time-to-close interaction analysis.

Key question: The overall maker edge peaks at 6h-7d before close. But does
this hold for ALL categories? Politics (event-driven) might have a completely
different decay curve than Sports (recurring daily markets).

This reveals whether the time-to-close signal is universal or category-specific,
which determines whether a single withdrawal schedule works or if category-
specific timing rules are needed.
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


TIME_LABELS = ["0-1h", "1-6h", "6-24h", "1-3d", "3-7d", "7-30d", "30d+"]


class CategoryTimeToCloseAnalysis(Analysis):
    """Maker edge by category group × time-to-close interaction."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="category_time_to_close",
            description="Maker edge by category group and time remaining until close",
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
                SELECT ticker, event_ticker, result, close_time
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
                  AND close_time IS NOT NULL
            ),
            trade_data AS (
                SELECT
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.count AS contracts,
                    m.result,
                    EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 AS hours_to_close
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
                  AND m.close_time > t.created_time
            ),
            bucketed AS (
                SELECT
                    category,
                    CASE
                        WHEN hours_to_close <= 1 THEN '0-1h'
                        WHEN hours_to_close <= 6 THEN '1-6h'
                        WHEN hours_to_close <= 24 THEN '6-24h'
                        WHEN hours_to_close <= 72 THEN '1-3d'
                        WHEN hours_to_close <= 168 THEN '3-7d'
                        WHEN hours_to_close <= 720 THEN '7-30d'
                        ELSE '30d+'
                    END AS time_bucket,
                    SUM(CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN -(100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no' THEN yes_price * contracts
                        WHEN taker_side = 'no' AND result = 'no' THEN -(100 - no_price) * contracts
                        WHEN taker_side = 'no' AND result = 'yes' THEN no_price * contracts
                    END) AS maker_pnl,
                    SUM(CASE
                        WHEN taker_side = 'yes' THEN no_price * contracts
                        ELSE yes_price * contracts
                    END) AS maker_cost,
                    SUM(contracts) AS total_contracts,
                    COUNT(*) AS trade_count
                FROM trade_data
                GROUP BY category, time_bucket
            )
            SELECT * FROM bucketed
            """
        ).df()

        # Vectorized group mapping
        unique_cats = df["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        df["group"] = df["category"].map(cat_to_group)

        # Aggregate by group × time_bucket
        group_time = (
            df.groupby(["group", "time_bucket"])
            .agg({
                "maker_pnl": "sum",
                "maker_cost": "sum",
                "total_contracts": "sum",
                "trade_count": "sum",
            })
            .reset_index()
        )
        group_time["maker_excess_pct"] = group_time["maker_pnl"] * 100.0 / group_time["maker_cost"].replace(0, np.nan)

        # Sort
        time_order = {t: i for i, t in enumerate(TIME_LABELS)}
        group_time["time_sort"] = group_time["time_bucket"].map(time_order)
        group_time = group_time.sort_values(["group", "time_sort"]).drop(columns=["time_sort"]).reset_index(drop=True)

        # Find peak time for each group
        peak_times = {}
        for group in group_time["group"].unique():
            g = group_time[group_time["group"] == group]
            if len(g) > 0:
                peak_idx = g["maker_excess_pct"].idxmax()
                peak_times[group] = g.loc[peak_idx, "time_bucket"]

        fig = self._create_figure(group_time, peak_times)
        chart = self._create_chart(group_time)

        return AnalysisOutput(
            figure=fig,
            data=group_time,
            chart=chart,
            metadata={"peak_times": peak_times},
        )

    def _create_figure(self, group_time, peak_times):
        """Create multi-panel figure showing time-to-close curves by category."""
        # Heatmap + line plots for top groups
        fig = plt.figure(figsize=(20, 12))

        # Panel 1: Heatmap
        ax1 = fig.add_subplot(1, 2, 1)
        groups = sorted(group_time["group"].unique())
        times = TIME_LABELS

        matrix = np.full((len(groups), len(times)), np.nan)
        for _, row in group_time.iterrows():
            if row["group"] in groups and row["time_bucket"] in times:
                i = groups.index(row["group"])
                j = times.index(row["time_bucket"])
                matrix[i, j] = row["maker_excess_pct"]

        vmax = min(np.nanmax(np.abs(matrix)), 20)
        im = ax1.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
        ax1.set_xticks(range(len(times)))
        ax1.set_xticklabels(times, rotation=45, ha="right")
        ax1.set_yticks(range(len(groups)))
        ax1.set_yticklabels(groups)
        ax1.set_title("Maker Excess Return: Category × Time to Close")
        ax1.set_xlabel("Time to Market Close")

        for i in range(len(groups)):
            for j in range(len(times)):
                val = matrix[i, j]
                if not np.isnan(val):
                    color = "white" if abs(val) > vmax * 0.5 else "black"
                    # Mark peak with asterisk
                    marker = "*" if groups[i] in peak_times and peak_times[groups[i]] == times[j] else ""
                    ax1.text(j, i, f"{val:+.1f}{marker}", ha="center", va="center", fontsize=7, color=color)

        fig.colorbar(im, ax=ax1, label="Maker Excess %", shrink=0.8)

        # Panel 2: Line plots for top 6 groups by volume
        ax2 = fig.add_subplot(1, 2, 2)
        top_groups = (
            group_time.groupby("group")["total_contracts"]
            .sum()
            .nlargest(6)
            .index.tolist()
        )

        time_order = {t: i for i, t in enumerate(TIME_LABELS)}
        for group in top_groups:
            gdata = group_time[group_time["group"] == group].copy()
            gdata["time_idx"] = gdata["time_bucket"].map(time_order)
            gdata = gdata.sort_values("time_idx")
            color = GROUP_COLORS.get(group, "#aaaaaa")
            ax2.plot(
                gdata["time_idx"],
                gdata["maker_excess_pct"],
                "o-",
                color=color,
                label=group,
                markersize=6,
                linewidth=2,
            )

        ax2.set_xticks(range(len(TIME_LABELS)))
        ax2.set_xticklabels(TIME_LABELS, rotation=45, ha="right")
        ax2.set_xlabel("Time to Market Close")
        ax2.set_ylabel("Maker Excess Return (%)")
        ax2.set_title("Maker Edge Curve by Category (Top 6 by Volume)")
        ax2.legend(loc="upper right", fontsize=8)
        ax2.axhline(0, color="black", linewidth=0.5)
        ax2.grid(alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, group_time):
        chart_data = []
        for _, row in group_time.iterrows():
            chart_data.append({
                "group": row["group"],
                "time_bucket": row["time_bucket"],
                "maker_excess_pct": round(float(row["maker_excess_pct"]), 4),
                "total_contracts": int(row["total_contracts"]),
            })

        return ChartConfig(
            type=ChartType.HEATMAP,
            data=chart_data,
            xKey="time_bucket",
            yKey="group",
            title="Maker Excess Return: Category × Time to Close",
            yUnit=UnitType.PERCENT,
        )
