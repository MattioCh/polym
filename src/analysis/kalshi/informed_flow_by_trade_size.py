"""Large vs small taker returns: does trade size predict informed flow?

Key question: Do takers placing larger orders outperform takers placing small
orders? If so, trade size is a signal of informed flow, and makers should
widen spreads when facing large takers. This tests the adverse selection
hypothesis that plagues market makers.

Trade size buckets (by contract count): 1, 2-5, 6-25, 26-100, 101-500, 500+
Cross-cut with time-to-close to test: does large order toxicity increase
near market close?
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


SIZE_LABELS = ["1", "2-5", "6-25", "26-100", "101-500", "500+"]
TIME_LABELS = ["0-1h", "1-6h", "6-24h", "1-7d", "7d+"]


class InformedFlowByTradeSizeAnalysis(Analysis):
    """Test whether large taker orders are more informed (higher win rate)."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="informed_flow_by_trade_size",
            description="Taker excess returns by trade size and time-to-close",
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
                SELECT ticker, result, close_time
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
                  AND close_time IS NOT NULL
            ),
            trade_data AS (
                SELECT
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.count AS contracts,
                    m.result,
                    t.created_time,
                    m.close_time,
                    EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 AS hours_to_close
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            ),
            bucketed AS (
                SELECT *,
                    CASE
                        WHEN contracts = 1 THEN '1'
                        WHEN contracts BETWEEN 2 AND 5 THEN '2-5'
                        WHEN contracts BETWEEN 6 AND 25 THEN '6-25'
                        WHEN contracts BETWEEN 26 AND 100 THEN '26-100'
                        WHEN contracts BETWEEN 101 AND 500 THEN '101-500'
                        ELSE '500+'
                    END AS size_bucket,
                    CASE
                        WHEN hours_to_close <= 1 THEN '0-1h'
                        WHEN hours_to_close <= 6 THEN '1-6h'
                        WHEN hours_to_close <= 24 THEN '6-24h'
                        WHEN hours_to_close <= 168 THEN '1-7d'
                        ELSE '7d+'
                    END AS time_bucket,
                    -- Taker PnL
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no' THEN -yes_price * contracts
                        WHEN taker_side = 'no' AND result = 'no' THEN (100 - no_price) * contracts
                        WHEN taker_side = 'no' AND result = 'yes' THEN -no_price * contracts
                    END AS taker_pnl,
                    CASE
                        WHEN taker_side = 'yes' THEN yes_price * contracts
                        ELSE no_price * contracts
                    END AS taker_cost,
                    CASE
                        WHEN taker_side = result THEN contracts ELSE 0
                    END AS taker_won_contracts
                FROM trade_data
                WHERE hours_to_close >= 0
            )
            SELECT
                size_bucket,
                time_bucket,
                SUM(taker_pnl) AS taker_pnl,
                SUM(taker_cost) AS taker_cost,
                SUM(taker_pnl) * 100.0 / NULLIF(SUM(taker_cost), 0) AS taker_excess_pct,
                SUM(taker_won_contracts) * 1.0 / SUM(contracts) AS taker_win_rate,
                AVG(CASE WHEN taker_side = 'yes' THEN yes_price ELSE no_price END) AS avg_taker_price,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM bucketed
            GROUP BY size_bucket, time_bucket
            """
        ).df()

        # Compute price-adjusted win rate (excess)
        df["excess_win_rate"] = df["taker_win_rate"] - df["avg_taker_price"] / 100.0

        # Sort by bucket order
        size_order = {s: i for i, s in enumerate(SIZE_LABELS)}
        time_order = {t: i for i, t in enumerate(TIME_LABELS)}
        df["size_sort"] = df["size_bucket"].map(size_order)
        df["time_sort"] = df["time_bucket"].map(time_order)
        df = df.sort_values(["size_sort", "time_sort"]).drop(
            columns=["size_sort", "time_sort"]
        ).reset_index(drop=True)

        # Also compute size-only aggregation
        size_agg = con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            ),
            trade_data AS (
                SELECT
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.count AS contracts,
                    m.result
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            ),
            bucketed AS (
                SELECT *,
                    CASE
                        WHEN contracts = 1 THEN '1'
                        WHEN contracts BETWEEN 2 AND 5 THEN '2-5'
                        WHEN contracts BETWEEN 6 AND 25 THEN '6-25'
                        WHEN contracts BETWEEN 26 AND 100 THEN '26-100'
                        WHEN contracts BETWEEN 101 AND 500 THEN '101-500'
                        ELSE '500+'
                    END AS size_bucket,
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no' THEN -yes_price * contracts
                        WHEN taker_side = 'no' AND result = 'no' THEN (100 - no_price) * contracts
                        WHEN taker_side = 'no' AND result = 'yes' THEN -no_price * contracts
                    END AS taker_pnl,
                    CASE
                        WHEN taker_side = 'yes' THEN yes_price * contracts
                        ELSE no_price * contracts
                    END AS taker_cost,
                    CASE
                        WHEN taker_side = result THEN contracts ELSE 0
                    END AS taker_won_contracts
                FROM trade_data
            )
            SELECT
                size_bucket,
                SUM(taker_pnl) AS taker_pnl,
                SUM(taker_cost) AS taker_cost,
                SUM(taker_pnl) * 100.0 / NULLIF(SUM(taker_cost), 0) AS taker_excess_pct,
                SUM(taker_won_contracts) * 1.0 / SUM(contracts) AS taker_win_rate,
                AVG(CASE WHEN taker_side = 'yes' THEN yes_price ELSE no_price END) AS avg_taker_price,
                SUM(contracts) AS total_contracts,
                COUNT(*) AS trade_count
            FROM bucketed
            GROUP BY size_bucket
            """
        ).df()

        size_agg["excess_win_rate"] = size_agg["taker_win_rate"] - size_agg["avg_taker_price"] / 100.0
        size_agg["size_sort"] = size_agg["size_bucket"].map(size_order)
        size_agg = size_agg.sort_values("size_sort").drop(columns=["size_sort"]).reset_index(drop=True)

        # Add size_agg to output
        size_agg["time_bucket"] = "(ALL)"
        combined = pd.concat([size_agg, df], ignore_index=True)

        fig = self._create_figure(size_agg, df)
        chart = self._create_chart(size_agg)

        return AnalysisOutput(figure=fig, data=combined, chart=chart)

    def _create_figure(self, size_agg: pd.DataFrame, cross_df: pd.DataFrame) -> plt.Figure:
        """Create multi-panel figure."""
        fig, axes = plt.subplots(1, 3, figsize=(20, 8))

        # Panel 1: Taker excess by size (bars)
        ax1 = axes[0]
        colors = ["#e74c3c" if v < 0 else "#2ecc71" for v in size_agg["taker_excess_pct"]]
        ax1.bar(range(len(size_agg)), size_agg["taker_excess_pct"], color=colors)
        ax1.set_xticks(range(len(size_agg)))
        ax1.set_xticklabels(size_agg["size_bucket"])
        ax1.set_xlabel("Trade Size (contracts)")
        ax1.set_ylabel("Taker Excess Return (%)")
        ax1.set_title("Taker Returns by Trade Size")
        ax1.axhline(0, color="black", linewidth=0.5)
        ax1.grid(axis="y", alpha=0.3)
        # Volume annotation
        for i, (_, row) in enumerate(size_agg.iterrows()):
            vol = row["total_contracts"]
            label = f"{vol / 1e6:.0f}M" if vol > 1e6 else f"{vol / 1e3:.0f}K"
            ax1.text(i, row["taker_excess_pct"], label, ha="center",
                     va="bottom" if row["taker_excess_pct"] > 0 else "top",
                     fontsize=7, color="gray")

        # Panel 2: Heatmap of taker excess by size × time
        ax2 = axes[1]
        sizes = [s for s in SIZE_LABELS if s in cross_df["size_bucket"].values]
        times = [t for t in TIME_LABELS if t in cross_df["time_bucket"].values]
        matrix = np.full((len(sizes), len(times)), np.nan)
        for _, row in cross_df.iterrows():
            if row["size_bucket"] in sizes and row["time_bucket"] in times:
                i = sizes.index(row["size_bucket"])
                j = times.index(row["time_bucket"])
                matrix[i, j] = row["taker_excess_pct"]

        vmax = min(max(abs(np.nanmin(matrix)), abs(np.nanmax(matrix))), 15)
        im = ax2.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)
        ax2.set_xticks(range(len(times)))
        ax2.set_xticklabels(times)
        ax2.set_yticks(range(len(sizes)))
        ax2.set_yticklabels(sizes)
        ax2.set_xlabel("Time to Close")
        ax2.set_ylabel("Trade Size (contracts)")
        ax2.set_title("Taker Excess Return\nby Size × Time to Close")

        for i in range(len(sizes)):
            for j in range(len(times)):
                val = matrix[i, j]
                if not np.isnan(val):
                    color = "white" if abs(val) > vmax * 0.5 else "black"
                    ax2.text(j, i, f"{val:+.1f}%", ha="center", va="center", fontsize=8, color=color)

        fig.colorbar(im, ax=ax2, label="Taker Excess %", shrink=0.8)

        # Panel 3: Excess win rate by size (raw informedness signal)
        ax3 = axes[2]
        ax3.plot(
            range(len(size_agg)),
            size_agg["excess_win_rate"] * 100,
            "o-",
            color="#3498db",
            markersize=8,
        )
        ax3.set_xticks(range(len(size_agg)))
        ax3.set_xticklabels(size_agg["size_bucket"])
        ax3.set_xlabel("Trade Size (contracts)")
        ax3.set_ylabel("Excess Win Rate (pp)")
        ax3.set_title("Taker Excess Win Rate by Size\n(above price-implied baseline)")
        ax3.axhline(0, color="black", linewidth=0.5, linestyle="--")
        ax3.grid(alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, size_agg: pd.DataFrame) -> ChartConfig:
        """Create chart configuration."""
        chart_data = []
        for _, row in size_agg.iterrows():
            chart_data.append(
                {
                    "size_bucket": row["size_bucket"],
                    "taker_excess_pct": round(float(row["taker_excess_pct"]), 4),
                    "excess_win_rate": round(float(row["excess_win_rate"]) * 100, 4),
                    "total_contracts": int(row["total_contracts"]),
                    "trade_count": int(row["trade_count"]),
                }
            )

        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="size_bucket",
            yKeys=["taker_excess_pct"],
            title="Taker Excess Return by Trade Size",
            yUnit=UnitType.PERCENT,
        )
