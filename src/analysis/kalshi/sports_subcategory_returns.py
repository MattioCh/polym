"""Maker/Taker returns broken down by sports subcategory (NFL, NBA, MLB, etc.).

Key question: Sports is ~75% of total volume. Which sports have the biggest
maker edge? Are there specific sports where takers actually win? An NFL game
spread might behave very differently from an NBA single-game prop.

This answers: where within sports should a maker concentrate capital?
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.analysis.kalshi.util.categories import CATEGORY_SQL, get_hierarchy
from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class SportsSubcategoryReturnsAnalysis(Analysis):
    """Maker vs taker returns across sports subcategories on Kalshi."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="sports_subcategory_returns",
            description="Maker vs taker excess returns by sports subcategory",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        """Execute the analysis and return outputs."""
        con = duckdb.connect()

        # Get raw data by category
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
                    m.result
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            )
            SELECT
                category,
                -- Taker PnL
                SUM(CASE
                    WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                    WHEN taker_side = 'yes' AND result = 'no' THEN -yes_price * contracts
                    WHEN taker_side = 'no' AND result = 'no' THEN (100 - no_price) * contracts
                    WHEN taker_side = 'no' AND result = 'yes' THEN -no_price * contracts
                END) AS taker_pnl,
                -- Taker cost basis
                SUM(CASE
                    WHEN taker_side = 'yes' THEN yes_price * contracts
                    ELSE no_price * contracts
                END) AS taker_cost,
                -- Maker PnL
                SUM(CASE
                    WHEN taker_side = 'yes' AND result = 'yes' THEN -(100 - yes_price) * contracts
                    WHEN taker_side = 'yes' AND result = 'no' THEN yes_price * contracts
                    WHEN taker_side = 'no' AND result = 'no' THEN -(100 - no_price) * contracts
                    WHEN taker_side = 'no' AND result = 'yes' THEN no_price * contracts
                END) AS maker_pnl,
                -- Maker cost basis
                SUM(CASE
                    WHEN taker_side = 'yes' THEN no_price * contracts
                    ELSE yes_price * contracts
                END) AS maker_cost,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM trade_data
            GROUP BY category
            """
        ).df()

        # Filter to sports only and map to hierarchy
        sports_rows = []
        for _, row in df.iterrows():
            group, sport, subcategory = get_hierarchy(row["category"])
            if group != "Sports":
                continue
            sports_rows.append(
                {
                    "sport": sport,
                    "subcategory": subcategory,
                    "raw_category": row["category"],
                    "taker_pnl": row["taker_pnl"],
                    "taker_cost": row["taker_cost"],
                    "maker_pnl": row["maker_pnl"],
                    "maker_cost": row["maker_cost"],
                    "total_contracts": row["total_contracts"],
                    "trade_count": row["trade_count"],
                }
            )

        sports_df = pd.DataFrame(sports_rows)

        # Aggregate by sport (top-level: NFL, NBA, MLB, etc.)
        sport_agg = (
            sports_df.groupby("sport")
            .agg(
                {
                    "taker_pnl": "sum",
                    "taker_cost": "sum",
                    "maker_pnl": "sum",
                    "maker_cost": "sum",
                    "total_contracts": "sum",
                    "trade_count": "sum",
                }
            )
            .reset_index()
        )
        sport_agg["taker_excess_pct"] = sport_agg["taker_pnl"] * 100 / sport_agg["taker_cost"]
        sport_agg["maker_excess_pct"] = sport_agg["maker_pnl"] * 100 / sport_agg["maker_cost"]
        sport_agg["edge_gap"] = sport_agg["maker_excess_pct"] - sport_agg["taker_excess_pct"]
        sport_agg = sport_agg.sort_values("total_contracts", ascending=False).reset_index(drop=True)

        # Also get subcategory detail for top sports
        subcat_agg = (
            sports_df.groupby(["sport", "subcategory"])
            .agg(
                {
                    "taker_pnl": "sum",
                    "taker_cost": "sum",
                    "maker_pnl": "sum",
                    "maker_cost": "sum",
                    "total_contracts": "sum",
                    "trade_count": "sum",
                }
            )
            .reset_index()
        )
        subcat_agg["taker_excess_pct"] = subcat_agg["taker_pnl"] * 100 / subcat_agg["taker_cost"]
        subcat_agg["maker_excess_pct"] = subcat_agg["maker_pnl"] * 100 / subcat_agg["maker_cost"]
        subcat_agg["edge_gap"] = subcat_agg["maker_excess_pct"] - subcat_agg["taker_excess_pct"]
        subcat_agg = subcat_agg.sort_values(
            ["sport", "total_contracts"], ascending=[True, False]
        ).reset_index(drop=True)

        # Combine both into one output with a "level" column
        sport_agg["subcategory"] = "(ALL)"
        sport_agg["level"] = "sport"
        subcat_agg["level"] = "subcategory"
        combined = pd.concat([sport_agg, subcat_agg], ignore_index=True)
        combined = combined.sort_values(["sport", "level", "total_contracts"], ascending=[True, True, False])

        fig = self._create_figure(sport_agg, subcat_agg)
        chart = self._create_chart(sport_agg)

        return AnalysisOutput(figure=fig, data=combined, chart=chart)

    def _create_figure(self, sport_agg: pd.DataFrame, subcat_agg: pd.DataFrame) -> plt.Figure:
        """Create horizontal bar chart of maker returns by sport."""
        # Top plot: sport-level
        top_sports = sport_agg.head(12)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Left: Bar chart by sport
        y = np.arange(len(top_sports))
        width = 0.35
        ax1.barh(y - width / 2, top_sports["maker_excess_pct"], width, label="Maker", color="#2ecc71")
        ax1.barh(y + width / 2, top_sports["taker_excess_pct"], width, label="Taker", color="#e74c3c")
        ax1.set_yticks(y)
        ax1.set_yticklabels(top_sports["sport"])
        ax1.set_xlabel("Excess Return (%)")
        ax1.set_title("Maker vs Taker Returns by Sport")
        ax1.axvline(0, color="black", linewidth=0.5)
        ax1.legend()
        ax1.grid(axis="x", alpha=0.3)

        # Add volume annotation
        for i, (_, row) in enumerate(top_sports.iterrows()):
            vol_m = row["total_contracts"] / 1e6
            ax1.text(
                max(row["maker_excess_pct"], row["taker_excess_pct"]) + 0.1,
                i,
                f"{vol_m:.1f}M",
                va="center",
                fontsize=8,
                color="gray",
            )

        # Right: top subcategories by maker edge
        top_sub = subcat_agg[subcat_agg["total_contracts"] >= 100000].nlargest(15, "maker_excess_pct")
        y2 = np.arange(len(top_sub))
        colors = ["#2ecc71" if v > 0 else "#e74c3c" for v in top_sub["maker_excess_pct"]]
        ax2.barh(y2, top_sub["maker_excess_pct"], color=colors)
        labels = [f"{row['sport']}: {row['subcategory']}" for _, row in top_sub.iterrows()]
        ax2.set_yticks(y2)
        ax2.set_yticklabels(labels, fontsize=8)
        ax2.set_xlabel("Maker Excess Return (%)")
        ax2.set_title("Top 15 Subcategories by Maker Edge\n(min 100K contracts)")
        ax2.axvline(0, color="black", linewidth=0.5)
        ax2.grid(axis="x", alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, sport_agg: pd.DataFrame) -> ChartConfig:
        """Create chart configuration."""
        chart_data = []
        for _, row in sport_agg.iterrows():
            chart_data.append(
                {
                    "sport": row["sport"],
                    "maker_excess_pct": round(float(row["maker_excess_pct"]), 4),
                    "taker_excess_pct": round(float(row["taker_excess_pct"]), 4),
                    "edge_gap": round(float(row["edge_gap"]), 4),
                    "total_contracts": int(row["total_contracts"]),
                }
            )

        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="sport",
            yKeys=["maker_excess_pct", "taker_excess_pct"],
            title="Maker vs Taker Excess Returns by Sport",
            yUnit=UnitType.PERCENT,
            colors=["#2ecc71", "#e74c3c"],
        )
