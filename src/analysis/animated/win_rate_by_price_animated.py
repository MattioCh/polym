"""Animated win rate by price showing calibration evolution over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

# Number of cumulative time slices to show in the animation
PAUSE_FRAMES = 10


class WinRateByPriceAnimatedAnalysis(Analysis):
    """Animated win rate vs price calibration evolving over cumulative time windows."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="win_rate_by_price_animated",
            description="Animated win rate vs price calibration over time",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def save(
        self,
        output_dir: Path | str,
        formats: list[str] | None = None,
        dpi: int = 100,
    ) -> dict[str, Path]:
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
            ),
            all_positions AS (
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    CASE WHEN t.taker_side = 'yes' THEN t.yes_price ELSE t.no_price END AS price,
                    CASE WHEN t.taker_side = m.result THEN 1 ELSE 0 END AS won
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker

                UNION ALL

                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    CASE WHEN t.taker_side = 'yes' THEN t.no_price ELSE t.yes_price END AS price,
                    CASE WHEN t.taker_side != m.result THEN 1 ELSE 0 END AS won
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
            )
            SELECT month, price, COUNT(*) AS total, SUM(won) AS wins
            FROM all_positions
            WHERE price BETWEEN 1 AND 99
            GROUP BY month, price
            ORDER BY month, price
            """
        ).df()

        cumulative = self._compute_cumulative(df)
        frame_months = sorted(cumulative.keys())

        fig, ax = plt.subplots(figsize=(10, 10))
        (scatter,) = ax.plot([], [], "o", markersize=5, alpha=0.8, color="#4C72B0")
        ax.plot([0, 100], [0, 100], "--", color="#D65F5F", linewidth=1.5, label="Perfect calibration")
        info_text = ax.text(
            0.02, 0.98, "", transform=ax.transAxes, fontsize=12, fontweight="bold",
            verticalalignment="top", bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        ax.set_xlabel("Contract Price (cents)")
        ax.set_ylabel("Win Rate (%)")
        ax.set_title("Win Rate vs Price: Market Calibration Over Time")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.set_xticks(range(0, 101, 10))
        ax.set_yticks(range(0, 101, 10))
        ax.set_aspect("equal")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        total_frames = len(frame_months) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_months) - 1)
            month = frame_months[idx]
            data = cumulative[month]
            prices = sorted(data["by_price"].keys())
            win_rates = [100.0 * data["by_price"][p]["wins"] / data["by_price"][p]["total"] for p in prices]
            scatter.set_data(prices, win_rates)
            total = data["total"]
            info_text.set_text(f"{month.strftime('%Y-%m')} | {total:,} trades")
            return scatter, info_text

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Build output from final frame
        output_rows = []
        if frame_months:
            final = cumulative[frame_months[-1]]
            for price, vals in final["by_price"].items():
                output_rows.append({
                    "price": price, "total": vals["total"], "wins": vals["wins"],
                    "win_rate": 100.0 * vals["wins"] / vals["total"],
                })
        output_df = pd.DataFrame(output_rows)

        return AnalysisOutput(figure=anim, data=output_df, metadata={"n_frames": len(frame_months)})

    def _compute_cumulative(self, df: pd.DataFrame) -> dict:
        if df.empty:
            return {}
        df = df.copy()
        df["month"] = pd.to_datetime(df["month"])
        if df["month"].dt.tz is not None:
            df["month"] = df["month"].dt.tz_convert(None)

        months = sorted(df["month"].unique())
        cumulative: dict = {}
        running: dict[int, dict] = {}

        for month in months:
            chunk = df[df["month"] == month]
            for _, row in chunk.iterrows():
                p = int(row["price"])
                if p not in running:
                    running[p] = {"total": 0, "wins": 0}
                running[p]["total"] += int(row["total"])
                running[p]["wins"] += int(row["wins"])
            cumulative[month] = {
                "total": sum(v["total"] for v in running.values()),
                "by_price": {p: dict(v) for p, v in running.items()},
            }
        return cumulative
