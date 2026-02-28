"""Animated trade size by role (maker vs taker) evolving over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class TradeSizeByRoleAnimatedAnalysis(Analysis):
    """Animated trade size comparison between makers and takers over time."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="trade_size_by_role_animated",
            description="Animated trade size by role over time",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def save(self, output_dir: Path | str, formats: list[str] | None = None, dpi: int = 100) -> dict[str, Path]:
        if formats is None:
            formats = ["gif", "csv"]
        return super().save(output_dir, formats, dpi)

    def run(self) -> AnalysisOutput:
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
                DATE_TRUNC('month', t.created_time) AS month,
                t.count * (CASE WHEN t.taker_side = 'yes' THEN t.yes_price ELSE t.no_price END) / 100.0 AS taker_size,
                t.count * (CASE WHEN t.taker_side = 'yes' THEN t.no_price ELSE t.yes_price END) / 100.0 AS maker_size
            FROM '{self.trades_dir}/*.parquet' t
            INNER JOIN resolved_markets m ON t.ticker = m.ticker
            """
        ).df()

        df["month"] = pd.to_datetime(df["month"])
        if df["month"].dt.tz is not None:
            df["month"] = df["month"].dt.tz_convert(None)

        frame_months = sorted(df["month"].unique())

        # Pre-compute cumulative stats for each frame
        frame_stats = []
        for fm in frame_months:
            subset = df[df["month"] <= fm]
            frame_stats.append({
                "month": fm,
                "taker_mean": subset["taker_size"].mean(),
                "taker_median": subset["taker_size"].median(),
                "maker_mean": subset["maker_size"].mean(),
                "maker_median": subset["maker_size"].median(),
                "n_trades": len(subset),
            })

        fig, ax = plt.subplots(figsize=(10, 6))
        info_text = ax.text(
            0.02, 0.98, "", transform=ax.transAxes, fontsize=12, fontweight="bold",
            verticalalignment="top", bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        ax.set_ylabel("Trade Size (USD)")
        ax.set_title("Trade Size by Role: Mean vs Median Over Time")
        plt.tight_layout()

        total_frames = len(frame_stats) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_stats) - 1)
            stats = frame_stats[idx]
            ax.clear()

            x = np.arange(2)
            width = 0.35
            ax.bar(
                x - width / 2,
                [stats["taker_mean"], stats["maker_mean"]],
                width, label="Mean", color="#3498db", alpha=0.8,
            )
            ax.bar(
                x + width / 2,
                [stats["taker_median"], stats["maker_median"]],
                width, label="Median", color="#e74c3c", alpha=0.8,
            )
            ax.set_ylabel("Trade Size (USD)")
            ax.set_title("Trade Size by Role: Mean vs Median Over Time")
            ax.set_xticks(x)
            ax.set_xticklabels(["Taker", "Maker"])
            ax.legend()
            ax.grid(True, alpha=0.3, axis="y")

            for i, (mean, median) in enumerate(
                zip(
                    [stats["taker_mean"], stats["maker_mean"]],
                    [stats["taker_median"], stats["maker_median"]],
                )
            ):
                ax.annotate(f"${mean:.0f}", (i - width / 2, mean), ha="center", va="bottom", fontsize=9)
                ax.annotate(f"${median:.0f}", (i + width / 2, median), ha="center", va="bottom", fontsize=9)

            ax.text(
                0.02, 0.98,
                f"Through {stats['month'].strftime('%Y-%m')} | {stats['n_trades']:,} trades",
                transform=ax.transAxes, fontsize=12, fontweight="bold",
                verticalalignment="top",
                bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
            )
            return ()

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Final output
        output_df = pd.DataFrame(frame_stats)
        return AnalysisOutput(figure=anim, data=output_df, metadata={"n_frames": len(frame_stats)})
