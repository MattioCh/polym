"""Composite maker strategy scorer.

Combines all findings into a single scoring table that rates every
(category_group, price_bucket, time_to_close, day_of_week) combination
by expected maker excess return. 

This is the "lookup table" a maker would consult before placing an order:
given what I'm quoting (category), at what price, how far from close,
and what day of week — what is my expected edge?

Also identifies the absolute best and worst 50 combinations.
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


PRICE_LABELS = ["01-20", "21-40", "41-60", "61-80", "81-99"]
TIME_LABELS = ["0-6h", "6h-3d", "3d+"]
DAY_LABELS = ["Weekday", "Weekend"]


class CompositeMakerStrategyAnalysis(Analysis):
    """Multi-factor maker edge scoring across category, price, time, and day."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="composite_maker_strategy",
            description="Multi-factor maker edge lookup table: category × price × time × day",
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
                    EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 AS hours_to_close,
                    CASE
                        WHEN dayofweek(t.created_time) IN (0, 6) THEN 'Weekend'
                        ELSE 'Weekday'
                    END AS day_type,
                    CASE
                        WHEN t.yes_price BETWEEN 1 AND 20 THEN '01-20'
                        WHEN t.yes_price BETWEEN 21 AND 40 THEN '21-40'
                        WHEN t.yes_price BETWEEN 41 AND 60 THEN '41-60'
                        WHEN t.yes_price BETWEEN 61 AND 80 THEN '61-80'
                        ELSE '81-99'
                    END AS price_bucket,
                    CASE
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 <= 6 THEN '0-6h'
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 <= 72 THEN '6h-3d'
                        ELSE '3d+'
                    END AS time_bucket,
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
                    END AS maker_cost
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
                  AND m.close_time > t.created_time
            )
            SELECT
                category,
                price_bucket,
                time_bucket,
                day_type,
                SUM(maker_pnl) AS maker_pnl,
                SUM(maker_cost) AS maker_cost,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM trade_data
            GROUP BY category, price_bucket, time_bucket, day_type
            """
        ).df()

        # Vectorized group mapping
        unique_cats = df["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        df["group"] = df["category"].map(cat_to_group)

        # Aggregate by group × price × time × day
        combo = (
            df.groupby(["group", "price_bucket", "time_bucket", "day_type"])
            .agg({
                "maker_pnl": "sum",
                "maker_cost": "sum",
                "total_contracts": "sum",
                "trade_count": "sum",
            })
            .reset_index()
        )
        combo["maker_excess_pct"] = combo["maker_pnl"] * 100.0 / combo["maker_cost"].replace(0, np.nan)

        # Filter to minimum volume
        combo = combo[combo["total_contracts"] >= 100000].reset_index(drop=True)

        # Sort by maker excess
        combo = combo.sort_values("maker_excess_pct", ascending=False).reset_index(drop=True)
        combo["rank"] = range(1, len(combo) + 1)

        # Also produce a simpler group × price summary (marginalizing over time and day)
        simple = (
            df.groupby(["group", "price_bucket"])
            .agg({
                "maker_pnl": "sum",
                "maker_cost": "sum",
                "total_contracts": "sum",
            })
            .reset_index()
        )
        simple["maker_excess_pct"] = simple["maker_pnl"] * 100.0 / simple["maker_cost"].replace(0, np.nan)

        fig = self._create_figure(combo, simple)
        chart = self._create_chart(combo)

        return AnalysisOutput(figure=fig, data=combo, chart=chart)

    def _create_figure(self, combo, simple):
        """Create composite strategy visualization."""
        fig, axes = plt.subplots(1, 3, figsize=(22, 10))

        # Panel 1: Top 25 best combos
        ax1 = axes[0]
        top25 = combo.head(25)
        labels = [
            f"{row['group'][:8]} | {row['price_bucket']} | {row['time_bucket']} | {row['day_type'][:3]}"
            for _, row in top25.iterrows()
        ]
        colors = ["#2ecc71" for _ in top25.iterrows()]
        y = np.arange(len(top25))
        ax1.barh(y, top25["maker_excess_pct"], color="#2ecc71", alpha=0.8)
        ax1.set_yticks(y)
        ax1.set_yticklabels(labels, fontsize=6)
        ax1.set_xlabel("Maker Excess Return (%)")
        ax1.set_title("Top 25 Maker Strategy Combinations\n(Group | Price | Time | Day)")
        ax1.invert_yaxis()
        ax1.grid(axis="x", alpha=0.3)

        # Add volume
        for i, (_, row) in enumerate(top25.iterrows()):
            vol = row["total_contracts"]
            label = f"{vol / 1e6:.1f}M" if vol > 1e6 else f"{vol / 1e3:.0f}K"
            ax1.text(row["maker_excess_pct"] + 0.1, i, label, va="center", fontsize=6, color="gray")

        # Panel 2: Bottom 25 worst combos
        ax2 = axes[1]
        bottom25 = combo.tail(25).iloc[::-1]
        labels2 = [
            f"{row['group'][:8]} | {row['price_bucket']} | {row['time_bucket']} | {row['day_type'][:3]}"
            for _, row in bottom25.iterrows()
        ]
        y2 = np.arange(len(bottom25))
        ax2.barh(y2, bottom25["maker_excess_pct"], color="#e74c3c", alpha=0.8)
        ax2.set_yticks(y2)
        ax2.set_yticklabels(labels2, fontsize=6)
        ax2.set_xlabel("Maker Excess Return (%)")
        ax2.set_title("Bottom 25 (Worst) Maker Combinations")
        ax2.invert_yaxis()
        ax2.axvline(0, color="black", linewidth=0.5)
        ax2.grid(axis="x", alpha=0.3)

        for i, (_, row) in enumerate(bottom25.iterrows()):
            vol = row["total_contracts"]
            label = f"{vol / 1e6:.1f}M" if vol > 1e6 else f"{vol / 1e3:.0f}K"
            val = row["maker_excess_pct"]
            ax2.text(val - 0.3 if val < 0 else val + 0.1, i, label, va="center", fontsize=6, color="gray")

        # Panel 3: Group × Price heatmap (simplified view)
        ax3 = axes[2]
        groups = sorted(simple["group"].unique())
        prices = sorted(simple["price_bucket"].unique())
        matrix = np.full((len(groups), len(prices)), np.nan)
        for _, row in simple.iterrows():
            if row["group"] in groups and row["price_bucket"] in prices:
                i = groups.index(row["group"])
                j = prices.index(row["price_bucket"])
                matrix[i, j] = row["maker_excess_pct"]

        vmax = min(np.nanmax(np.abs(matrix)), 15)
        im = ax3.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
        ax3.set_xticks(range(len(prices)))
        ax3.set_xticklabels(prices, rotation=45, ha="right")
        ax3.set_yticks(range(len(groups)))
        ax3.set_yticklabels(groups, fontsize=8)
        ax3.set_title("Maker Excess by Group × Price\n(Simplified View)")

        for i in range(len(groups)):
            for j in range(len(prices)):
                val = matrix[i, j]
                if not np.isnan(val):
                    color = "white" if abs(val) > vmax * 0.5 else "black"
                    ax3.text(j, i, f"{val:+.1f}", ha="center", va="center", fontsize=8, color=color)

        fig.colorbar(im, ax=ax3, label="Maker Excess %", shrink=0.8)

        plt.tight_layout()
        return fig

    def _create_chart(self, combo):
        chart_data = []
        for _, row in combo.head(100).iterrows():
            chart_data.append({
                "rank": int(row["rank"]),
                "group": row["group"],
                "price_bucket": row["price_bucket"],
                "time_bucket": row["time_bucket"],
                "day_type": row["day_type"],
                "maker_excess_pct": round(float(row["maker_excess_pct"]), 4),
                "total_contracts": int(row["total_contracts"]),
            })
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="rank",
            yKeys=["maker_excess_pct"],
            title="Top Maker Strategy Combinations",
            yUnit=UnitType.PERCENT,
        )
