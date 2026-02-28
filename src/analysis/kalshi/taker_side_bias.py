"""YES-taker vs NO-taker win rates and excess returns by price.

Key question: Is there an asymmetry between YES-takers and NO-takers? 
At the same price point, do takers who buy YES win more or less often 
than takers who buy NO? If YES-takers systematically overpay, that means
there's a "YES bias" (optimism tax) in the market.

This also reveals: at what prices should a maker preferentially quote 
on the YES side vs NO side?
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class TakerSideBiasAnalysis(Analysis):
    """Compare YES-taker vs NO-taker win rates and PnL at each price."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="taker_side_bias",
            description="YES-taker vs NO-taker win rates and PnL at each price",
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
                SELECT ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            )
            SELECT
                t.yes_price,
                t.taker_side,
                -- YES-taker stats
                SUM(CASE WHEN t.taker_side = 'yes' THEN t.count ELSE 0 END) AS yes_taker_contracts,
                SUM(CASE WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN t.count ELSE 0 END) AS yes_taker_wins,
                SUM(CASE WHEN t.taker_side = 'yes' THEN
                    CASE 
                        WHEN m.result = 'yes' THEN (100 - t.yes_price) * t.count
                        ELSE -t.yes_price * t.count
                    END
                ELSE 0 END) AS yes_taker_pnl,
                SUM(CASE WHEN t.taker_side = 'yes' THEN t.yes_price * t.count ELSE 0 END) AS yes_taker_cost,
                -- NO-taker stats
                SUM(CASE WHEN t.taker_side = 'no' THEN t.count ELSE 0 END) AS no_taker_contracts,
                SUM(CASE WHEN t.taker_side = 'no' AND m.result = 'no' THEN t.count ELSE 0 END) AS no_taker_wins,
                SUM(CASE WHEN t.taker_side = 'no' THEN
                    CASE
                        WHEN m.result = 'no' THEN (100 - t.no_price) * t.count
                        ELSE -t.no_price * t.count
                    END
                ELSE 0 END) AS no_taker_pnl,
                SUM(CASE WHEN t.taker_side = 'no' THEN t.no_price * t.count ELSE 0 END) AS no_taker_cost
            FROM '{self.trades_dir}/*.parquet' t
            INNER JOIN resolved_markets m ON t.ticker = m.ticker
            WHERE t.yes_price BETWEEN 1 AND 99
            GROUP BY t.yes_price, t.taker_side
            """
        ).df()

        # Aggregate by yes_price (across taker_side grouping)
        agg = (
            df.groupby("yes_price")
            .agg({
                "yes_taker_contracts": "sum",
                "yes_taker_wins": "sum",
                "yes_taker_pnl": "sum",
                "yes_taker_cost": "sum",
                "no_taker_contracts": "sum",
                "no_taker_wins": "sum",
                "no_taker_pnl": "sum",
                "no_taker_cost": "sum",
            })
            .reset_index()
        )

        # Win rates
        agg["yes_taker_win_rate"] = agg["yes_taker_wins"] / agg["yes_taker_contracts"].replace(0, np.nan)
        agg["no_taker_win_rate"] = agg["no_taker_wins"] / agg["no_taker_contracts"].replace(0, np.nan)

        # Excess win rates (above fair price implied)
        agg["yes_taker_excess_wr"] = agg["yes_taker_win_rate"] - agg["yes_price"] / 100.0
        # NO taker pays no_price = 100 - yes_price, so fair probability they win is no_price/100
        agg["no_taker_excess_wr"] = agg["no_taker_win_rate"] - (100 - agg["yes_price"]) / 100.0

        # Excess return %
        agg["yes_taker_excess_pct"] = agg["yes_taker_pnl"] * 100.0 / agg["yes_taker_cost"].replace(0, np.nan)
        agg["no_taker_excess_pct"] = agg["no_taker_pnl"] * 100.0 / agg["no_taker_cost"].replace(0, np.nan)

        # Volume share: what fraction chose YES at each price
        agg["yes_share"] = agg["yes_taker_contracts"] / (agg["yes_taker_contracts"] + agg["no_taker_contracts"])

        # Bias metric: positive = YES-takers overpay more (worse excess)
        agg["bias"] = agg["no_taker_excess_wr"] - agg["yes_taker_excess_wr"]

        agg = agg.sort_values("yes_price").reset_index(drop=True)

        fig = self._create_figure(agg)
        chart = self._create_chart(agg)

        return AnalysisOutput(figure=fig, data=agg, chart=chart)

    def _create_figure(self, df: pd.DataFrame) -> plt.Figure:
        """Create multi-panel taker side bias visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        prices = df["yes_price"].values

        # Panel 1: Excess win rate by taker side
        ax1 = axes[0, 0]
        ax1.plot(prices, df["yes_taker_excess_wr"] * 100, label="YES takers", color="#2ecc71", linewidth=1.5)
        ax1.plot(prices, df["no_taker_excess_wr"] * 100, label="NO takers", color="#e74c3c", linewidth=1.5)
        ax1.axhline(0, color="black", linewidth=0.5, linestyle="--")
        ax1.set_xlabel("YES Price (cents)")
        ax1.set_ylabel("Excess Win Rate (pp)")
        ax1.set_title("Taker Excess Win Rate: YES vs NO Takers")
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Panel 2: Excess return % by taker side
        ax2 = axes[0, 1]
        ax2.plot(prices, df["yes_taker_excess_pct"], label="YES takers", color="#2ecc71", linewidth=1.5)
        ax2.plot(prices, df["no_taker_excess_pct"], label="NO takers", color="#e74c3c", linewidth=1.5)
        ax2.axhline(0, color="black", linewidth=0.5, linestyle="--")
        ax2.set_xlabel("YES Price (cents)")
        ax2.set_ylabel("Excess Return (%)")
        ax2.set_title("Taker Excess Return: YES vs NO Takers")
        ax2.legend()
        ax2.grid(alpha=0.3)

        # Panel 3: YES share (what fraction bought YES)
        ax3 = axes[1, 0]
        ax3.fill_between(prices, df["yes_share"] * 100, 50, where=df["yes_share"] > 0.5,
                         alpha=0.3, color="#2ecc71", label="YES dominant")
        ax3.fill_between(prices, df["yes_share"] * 100, 50, where=df["yes_share"] <= 0.5,
                         alpha=0.3, color="#e74c3c", label="NO dominant")
        ax3.plot(prices, df["yes_share"] * 100, color="#3498db", linewidth=1.5)
        ax3.axhline(50, color="black", linewidth=0.5, linestyle="--")
        ax3.set_xlabel("YES Price (cents)")
        ax3.set_ylabel("YES Taker Share (%)")
        ax3.set_title("Fraction of Takers Buying YES at Each Price")
        ax3.legend()
        ax3.grid(alpha=0.3)

        # Panel 4: Bias metric (NO excess WR - YES excess WR)
        ax4 = axes[1, 1]
        colors = ["#2ecc71" if b > 0 else "#e74c3c" for b in df["bias"]]
        ax4.bar(prices, df["bias"] * 100, color=colors, alpha=0.7, width=1)
        ax4.axhline(0, color="black", linewidth=0.5)
        ax4.set_xlabel("YES Price (cents)")
        ax4.set_ylabel("NO - YES Excess Win Rate (pp)")
        ax4.set_title("Directional Bias: Positive = YES Takers Overpay More")
        ax4.grid(alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, df: pd.DataFrame) -> ChartConfig:
        chart_data = []
        for _, row in df.iterrows():
            chart_data.append({
                "yes_price": int(row["yes_price"]),
                "yes_taker_excess_wr": round(float(row["yes_taker_excess_wr"]) * 100, 4),
                "no_taker_excess_wr": round(float(row["no_taker_excess_wr"]) * 100, 4),
                "yes_share": round(float(row["yes_share"]) * 100, 2),
                "bias": round(float(row["bias"]) * 100, 4),
            })

        return ChartConfig(
            type=ChartType.LINE,
            data=chart_data,
            xKey="yes_price",
            yKeys=["yes_taker_excess_wr", "no_taker_excess_wr"],
            title="YES vs NO Taker Excess Win Rate",
            yUnit=UnitType.PERCENT,
            colors=["#2ecc71", "#e74c3c"],
        )
