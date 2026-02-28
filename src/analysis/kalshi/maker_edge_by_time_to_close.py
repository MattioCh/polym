"""Maker edge vs time remaining until market closes.

Key question: Does the maker's surplus grow as markets approach their close
time? If so, liquidity providers earn a premium for stale-quote risk during
the final hours before resolution. This informs when to pull maker orders.

Buckets: 0-1h, 1-6h, 6-24h, 1-3d, 3-7d, 7-30d, 30d+
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


BUCKET_LABELS = ["0-1h", "1-6h", "6-24h", "1-3d", "3-7d", "7-30d", "30d+"]


class MakerEdgeByTimeToCloseAnalysis(Analysis):
    """How maker edge varies with time remaining until market close."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="maker_edge_by_time_to_close",
            description="Maker excess return by time remaining until market close",
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
                    m.close_time,
                    t.created_time,
                    -- hours until market closes
                    EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 AS hours_to_close
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.yes_price BETWEEN 1 AND 99
            ),
            bucketed AS (
                SELECT
                    *,
                    CASE
                        WHEN hours_to_close <= 1 THEN '0-1h'
                        WHEN hours_to_close <= 6 THEN '1-6h'
                        WHEN hours_to_close <= 24 THEN '6-24h'
                        WHEN hours_to_close <= 72 THEN '1-3d'
                        WHEN hours_to_close <= 168 THEN '3-7d'
                        WHEN hours_to_close <= 720 THEN '7-30d'
                        ELSE '30d+'
                    END AS time_bucket,
                    -- Taker PnL
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no' THEN -yes_price * contracts
                        WHEN taker_side = 'no' AND result = 'no' THEN (100 - no_price) * contracts
                        WHEN taker_side = 'no' AND result = 'yes' THEN -no_price * contracts
                    END AS taker_pnl,
                    -- Taker fair value reference
                    CASE
                        WHEN taker_side = 'yes' THEN yes_price * contracts
                        ELSE no_price * contracts
                    END AS taker_cost,
                    -- Maker PnL (opposite of taker)
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN -(100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no' THEN yes_price * contracts
                        WHEN taker_side = 'no' AND result = 'no' THEN -(100 - no_price) * contracts
                        WHEN taker_side = 'no' AND result = 'yes' THEN no_price * contracts
                    END AS maker_pnl,
                    CASE
                        WHEN taker_side = 'yes' THEN no_price * contracts
                        ELSE yes_price * contracts
                    END AS maker_cost,
                    contracts
                FROM trade_data
                WHERE hours_to_close >= 0  -- exclude trades after close
            )
            SELECT
                time_bucket,
                SUM(taker_pnl) AS taker_total_pnl,
                SUM(maker_pnl) AS maker_total_pnl,
                SUM(taker_cost) AS taker_total_cost,
                SUM(maker_cost) AS maker_total_cost,
                SUM(contracts) AS total_contracts,
                SUM(taker_pnl) * 100.0 / NULLIF(SUM(taker_cost), 0) AS taker_excess_pct,
                SUM(maker_pnl) * 100.0 / NULLIF(SUM(maker_cost), 0) AS maker_excess_pct,
                COUNT(*) AS trade_count
            FROM bucketed
            GROUP BY time_bucket
            """
        ).df()

        # Sort by bucket order
        bucket_order = {b: i for i, b in enumerate(BUCKET_LABELS)}
        df["sort_key"] = df["time_bucket"].map(bucket_order)
        df = df.sort_values("sort_key").drop(columns=["sort_key"]).reset_index(drop=True)

        fig = self._create_figure(df)
        chart = self._create_chart(df)

        return AnalysisOutput(figure=fig, data=df, chart=chart)

    def _create_figure(self, df: pd.DataFrame) -> plt.Figure:
        """Create bar chart comparing maker vs taker returns by time-to-close."""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        x = np.arange(len(df))
        width = 0.35

        # Top: excess return %
        ax1.bar(x - width / 2, df["maker_excess_pct"], width, label="Maker", color="#2ecc71")
        ax1.bar(x + width / 2, df["taker_excess_pct"], width, label="Taker", color="#e74c3c")
        ax1.set_ylabel("Excess Return (%)")
        ax1.set_title("Maker vs Taker Excess Return by Time to Market Close")
        ax1.set_xticks(x)
        ax1.set_xticklabels(df["time_bucket"])
        ax1.legend()
        ax1.axhline(0, color="black", linewidth=0.5)
        ax1.grid(axis="y", alpha=0.3)

        # Bottom: volume bars
        ax2.bar(x, df["total_contracts"] / 1e6, color="#3498db", alpha=0.8)
        ax2.set_ylabel("Contracts (millions)")
        ax2.set_xlabel("Time to Close")
        ax2.set_title("Trade Volume by Time to Market Close")
        ax2.set_xticks(x)
        ax2.set_xticklabels(df["time_bucket"])
        ax2.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        return fig

    def _create_chart(self, df: pd.DataFrame) -> ChartConfig:
        """Create chart configuration."""
        chart_data = []
        for _, row in df.iterrows():
            chart_data.append(
                {
                    "time_bucket": row["time_bucket"],
                    "maker_excess_pct": round(float(row["maker_excess_pct"]), 4),
                    "taker_excess_pct": round(float(row["taker_excess_pct"]), 4),
                    "total_contracts": int(row["total_contracts"]),
                }
            )

        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="time_bucket",
            yKeys=["maker_excess_pct", "taker_excess_pct"],
            title="Maker vs Taker Excess Return by Time to Market Close",
            yUnit=UnitType.PERCENT,
            colors=["#2ecc71", "#e74c3c"],
        )
