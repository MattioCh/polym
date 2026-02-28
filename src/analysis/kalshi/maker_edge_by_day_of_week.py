"""Maker edge by day of week and category group.

Key question: Are there weekly patterns in maker profitability? Sports
markets cluster on weekends, finance on weekdays. Does the maker edge 
differ by day, and does it interact with category? 

This answers: should a maker adjust quoting schedules by day of the week?
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


DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


class MakerEdgeByDayOfWeekAnalysis(Analysis):
    """Maker excess return by day of week, cross-cut by category group."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="maker_edge_by_day_of_week",
            description="Maker vs taker excess return by day of week and category",
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
                    dayname(t.created_time) AS day_of_week,
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
                category,
                day_of_week,
                SUM(maker_pnl) AS maker_pnl,
                SUM(maker_cost) AS maker_cost,
                SUM(taker_pnl) AS taker_pnl,
                SUM(taker_cost) AS taker_cost,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM trade_data
            GROUP BY category, day_of_week
            """
        ).df()

        # Vectorized group mapping
        unique_cats = df["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        df["group"] = df["category"].map(cat_to_group)

        # Aggregate by group × day
        group_day = (
            df.groupby(["group", "day_of_week"])
            .agg({
                "maker_pnl": "sum",
                "maker_cost": "sum",
                "taker_pnl": "sum",
                "taker_cost": "sum",
                "total_contracts": "sum",
                "trade_count": "sum",
            })
            .reset_index()
        )
        group_day["maker_excess_pct"] = group_day["maker_pnl"] * 100.0 / group_day["maker_cost"].replace(0, np.nan)
        group_day["taker_excess_pct"] = group_day["taker_pnl"] * 100.0 / group_day["taker_cost"].replace(0, np.nan)

        # Also aggregate day-only
        day_agg = (
            df.groupby("day_of_week")
            .agg({
                "maker_pnl": "sum",
                "maker_cost": "sum",
                "taker_pnl": "sum",
                "taker_cost": "sum",
                "total_contracts": "sum",
                "trade_count": "sum",
            })
            .reset_index()
        )
        day_agg["maker_excess_pct"] = day_agg["maker_pnl"] * 100.0 / day_agg["maker_cost"].replace(0, np.nan)
        day_agg["taker_excess_pct"] = day_agg["taker_pnl"] * 100.0 / day_agg["taker_cost"].replace(0, np.nan)
        day_agg["group"] = "(ALL)"

        combined = pd.concat([day_agg, group_day], ignore_index=True)

        # Sort by day order
        day_sort = {d: i for i, d in enumerate(DAY_ORDER)}
        combined["day_sort"] = combined["day_of_week"].map(day_sort)
        combined = combined.sort_values(["group", "day_sort"]).drop(columns=["day_sort"]).reset_index(drop=True)

        fig = self._create_figure(day_agg, group_day)
        chart = self._create_chart(day_agg)

        return AnalysisOutput(figure=fig, data=combined, chart=chart)

    def _create_figure(self, day_agg, group_day):
        """Create day-of-week analysis figure."""
        fig, axes = plt.subplots(1, 3, figsize=(20, 8))

        # Sort days
        day_sort = {d: i for i, d in enumerate(DAY_ORDER)}

        # Panel 1: Overall maker vs taker by day
        ax1 = axes[0]
        da = day_agg.copy()
        da["day_sort"] = da["day_of_week"].map(day_sort)
        da = da.sort_values("day_sort")

        x = np.arange(len(da))
        width = 0.35
        ax1.bar(x - width / 2, da["maker_excess_pct"], width, label="Maker", color="#2ecc71")
        ax1.bar(x + width / 2, da["taker_excess_pct"], width, label="Taker", color="#e74c3c")
        ax1.set_xticks(x)
        ax1.set_xticklabels([d[:3] for d in da["day_of_week"]], rotation=0)
        ax1.set_ylabel("Excess Return (%)")
        ax1.set_title("Maker vs Taker Returns by Day of Week")
        ax1.legend()
        ax1.axhline(0, color="black", linewidth=0.5)
        ax1.grid(axis="y", alpha=0.3)

        # Panel 2: Heatmap - maker excess by group × day
        ax2 = axes[1]
        groups = sorted(group_day["group"].unique())
        days = DAY_ORDER

        matrix = np.full((len(groups), len(days)), np.nan)
        for _, row in group_day.iterrows():
            if row["group"] in groups and row["day_of_week"] in days:
                i = groups.index(row["group"])
                j = days.index(row["day_of_week"])
                matrix[i, j] = row["maker_excess_pct"]

        vmax = min(np.nanmax(np.abs(matrix)), 15)
        im = ax2.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
        ax2.set_xticks(range(len(days)))
        ax2.set_xticklabels([d[:3] for d in days])
        ax2.set_yticks(range(len(groups)))
        ax2.set_yticklabels(groups, fontsize=8)
        ax2.set_title("Maker Excess Return: Group × Day")

        for i in range(len(groups)):
            for j in range(len(days)):
                val = matrix[i, j]
                if not np.isnan(val):
                    color = "white" if abs(val) > vmax * 0.5 else "black"
                    ax2.text(j, i, f"{val:+.1f}", ha="center", va="center", fontsize=7, color=color)

        fig.colorbar(im, ax=ax2, label="Maker Excess %", shrink=0.8)

        # Panel 3: Volume by day
        ax3 = axes[2]
        ax3.bar(x, da["total_contracts"] / 1e9, color="#3498db", alpha=0.8)
        ax3.set_xticks(x)
        ax3.set_xticklabels([d[:3] for d in da["day_of_week"]])
        ax3.set_ylabel("Contracts (billions)")
        ax3.set_title("Trading Volume by Day of Week")
        ax3.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, day_agg):
        da = day_agg.copy()
        day_sort = {d: i for i, d in enumerate(DAY_ORDER)}
        da["day_sort"] = da["day_of_week"].map(day_sort)
        da = da.sort_values("day_sort")

        chart_data = []
        for _, row in da.iterrows():
            chart_data.append({
                "day": row["day_of_week"],
                "maker_excess_pct": round(float(row["maker_excess_pct"]), 4),
                "taker_excess_pct": round(float(row["taker_excess_pct"]), 4),
                "total_contracts": int(row["total_contracts"]),
            })

        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="day",
            yKeys=["maker_excess_pct", "taker_excess_pct"],
            title="Maker vs Taker Excess Return by Day of Week",
            yUnit=UnitType.PERCENT,
            colors=["#2ecc71", "#e74c3c"],
        )
