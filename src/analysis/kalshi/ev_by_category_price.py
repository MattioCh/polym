"""EV of YES vs NO bets broken down by BOTH category and price bucket.

Key question: Does the optimal bet direction (YES vs NO) change by category?
For example, sports longshots at 10c might behave differently from political
longshots at 10c. This analysis reveals category-specific pricing biases
that a one-size-fits-all strategy would miss.

Price buckets: 1-10, 11-20, 21-30, 31-40, 41-50, 51-60, 61-70, 71-80, 81-90, 91-99
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


class EvByCategoryPriceAnalysis(Analysis):
    """EV of YES vs NO bets by category and price bucket on Kalshi."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="ev_by_category_price",
            description="Expected value by category and price bucket",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        """Execute the analysis and return outputs."""
        con = duckdb.connect()

        # Compute YES and NO EV by category × price bucket
        df = con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT ticker, event_ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            ),
            trade_enriched AS (
                SELECT
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    t.yes_price,
                    t.no_price,
                    t.count AS contracts,
                    m.result
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            ),
            -- YES side: buy YES at yes_price, win if result='yes'
            yes_stats AS (
                SELECT
                    category,
                    CASE
                        WHEN yes_price BETWEEN 1 AND 10 THEN '01-10'
                        WHEN yes_price BETWEEN 11 AND 20 THEN '11-20'
                        WHEN yes_price BETWEEN 21 AND 30 THEN '21-30'
                        WHEN yes_price BETWEEN 31 AND 40 THEN '31-40'
                        WHEN yes_price BETWEEN 41 AND 50 THEN '41-50'
                        WHEN yes_price BETWEEN 51 AND 60 THEN '51-60'
                        WHEN yes_price BETWEEN 61 AND 70 THEN '61-70'
                        WHEN yes_price BETWEEN 71 AND 80 THEN '71-80'
                        WHEN yes_price BETWEEN 81 AND 90 THEN '81-90'
                        WHEN yes_price BETWEEN 91 AND 99 THEN '91-99'
                    END AS price_bucket,
                    SUM(CASE WHEN result = 'yes' THEN contracts ELSE 0 END) * 1.0 / SUM(contracts) AS yes_win_rate,
                    SUM(contracts) AS yes_contracts,
                    AVG(yes_price) AS avg_yes_price
                FROM trade_enriched
                GROUP BY category, price_bucket
            ),
            -- NO side: buy NO at no_price, win if result='no'
            no_stats AS (
                SELECT
                    category,
                    CASE
                        WHEN no_price BETWEEN 1 AND 10 THEN '01-10'
                        WHEN no_price BETWEEN 11 AND 20 THEN '11-20'
                        WHEN no_price BETWEEN 21 AND 30 THEN '21-30'
                        WHEN no_price BETWEEN 31 AND 40 THEN '31-40'
                        WHEN no_price BETWEEN 41 AND 50 THEN '41-50'
                        WHEN no_price BETWEEN 51 AND 60 THEN '51-60'
                        WHEN no_price BETWEEN 61 AND 70 THEN '61-70'
                        WHEN no_price BETWEEN 71 AND 80 THEN '71-80'
                        WHEN no_price BETWEEN 81 AND 90 THEN '81-90'
                        WHEN no_price BETWEEN 91 AND 99 THEN '91-99'
                    END AS price_bucket,
                    SUM(CASE WHEN result = 'no' THEN contracts ELSE 0 END) * 1.0 / SUM(contracts) AS no_win_rate,
                    SUM(contracts) AS no_contracts,
                    AVG(no_price) AS avg_no_price
                FROM trade_enriched
                GROUP BY category, price_bucket
            )
            SELECT
                y.category,
                y.price_bucket,
                y.yes_win_rate,
                y.yes_contracts,
                y.avg_yes_price,
                100 * y.yes_win_rate - y.avg_yes_price AS yes_ev,
                n.no_win_rate,
                n.no_contracts,
                n.avg_no_price,
                100 * n.no_win_rate - n.avg_no_price AS no_ev,
                CASE
                    WHEN (100 * y.yes_win_rate - y.avg_yes_price) > (100 * n.no_win_rate - n.avg_no_price)
                    THEN 'YES' ELSE 'NO'
                END AS best_bet,
                GREATEST(100 * y.yes_win_rate - y.avg_yes_price, 100 * n.no_win_rate - n.avg_no_price) AS best_ev,
                y.yes_contracts + n.no_contracts AS total_contracts
            FROM yes_stats y
            INNER JOIN no_stats n ON y.category = n.category AND y.price_bucket = n.price_bucket
            WHERE y.yes_contracts >= 10000 AND n.no_contracts >= 10000
            ORDER BY y.category, y.price_bucket
            """
        ).df()

        # Map to groups
        df["group"] = df["category"].apply(get_group)

        # Aggregate by group × price_bucket (volume-weighted)
        group_rows = []
        for (group, bucket), gdf in df.groupby(["group", "price_bucket"]):
            total_yes_c = gdf["yes_contracts"].sum()
            total_no_c = gdf["no_contracts"].sum()
            if total_yes_c == 0 or total_no_c == 0:
                continue
            w_yes_ev = (gdf["yes_ev"] * gdf["yes_contracts"]).sum() / total_yes_c
            w_no_ev = (gdf["no_ev"] * gdf["no_contracts"]).sum() / total_no_c
            group_rows.append(
                {
                    "group": group,
                    "price_bucket": bucket,
                    "yes_ev": w_yes_ev,
                    "no_ev": w_no_ev,
                    "best_bet": "YES" if w_yes_ev > w_no_ev else "NO",
                    "best_ev": max(w_yes_ev, w_no_ev),
                    "ev_gap": w_yes_ev - w_no_ev,
                    "yes_contracts": total_yes_c,
                    "no_contracts": total_no_c,
                }
            )

        group_df = pd.DataFrame(group_rows)
        group_df = group_df.sort_values(["group", "price_bucket"])

        fig = self._create_figure(group_df)
        chart = self._create_chart(group_df)

        return AnalysisOutput(figure=fig, data=group_df, chart=chart)

    def _create_figure(self, df: pd.DataFrame) -> plt.Figure:
        """Create heatmap of YES-NO EV gap by group × price bucket."""
        groups = sorted(df["group"].unique())
        buckets = sorted(df["price_bucket"].unique())

        matrix = np.full((len(groups), len(buckets)), np.nan)
        for _, row in df.iterrows():
            i = groups.index(row["group"])
            j = buckets.index(row["price_bucket"])
            matrix[i, j] = row["ev_gap"]

        fig, ax = plt.subplots(figsize=(14, 8))
        vmax = max(abs(np.nanmin(matrix)), abs(np.nanmax(matrix)))
        im = ax.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)

        ax.set_xticks(range(len(buckets)))
        ax.set_xticklabels(buckets, rotation=45, ha="right")
        ax.set_yticks(range(len(groups)))
        ax.set_yticklabels(groups)

        # Annotate cells
        for i in range(len(groups)):
            for j in range(len(buckets)):
                val = matrix[i, j]
                if not np.isnan(val):
                    text = f"{val:+.1f}"
                    color = "white" if abs(val) > vmax * 0.6 else "black"
                    ax.text(j, i, text, ha="center", va="center", fontsize=8, color=color)

        ax.set_xlabel("Price Bucket (cents)")
        ax.set_ylabel("Category")
        ax.set_title("YES - NO EV Gap by Category × Price\n(Green = YES better, Red = NO better)")
        fig.colorbar(im, ax=ax, label="YES-NO EV gap (cents)")

        plt.tight_layout()
        return fig

    def _create_chart(self, df: pd.DataFrame) -> ChartConfig:
        """Create chart configuration."""
        chart_data = []
        for _, row in df.iterrows():
            chart_data.append(
                {
                    "group": row["group"],
                    "price_bucket": row["price_bucket"],
                    "yes_ev": round(float(row["yes_ev"]), 2),
                    "no_ev": round(float(row["no_ev"]), 2),
                    "best_bet": row["best_bet"],
                    "ev_gap": round(float(row["ev_gap"]), 2),
                }
            )

        return ChartConfig(
            type=ChartType.HEATMAP,
            data=chart_data,
            xKey="price_bucket",
            yKey="group",
            title="YES - NO EV Gap by Category × Price",
            yUnit=UnitType.CENTS,
        )
