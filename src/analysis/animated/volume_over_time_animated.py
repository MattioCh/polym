"""Animated cumulative volume over time on Kalshi."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class VolumeOverTimeAnimatedAnalysis(Analysis):
    """Animated bar chart of cumulative quarterly volume growing over time."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
    ):
        super().__init__(
            name="volume_over_time_animated",
            description="Animated cumulative quarterly volume over time",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")

    def save(self, output_dir: Path | str, formats: list[str] | None = None, dpi: int = 100) -> dict[str, Path]:
        if formats is None:
            formats = ["gif", "csv"]
        return super().save(output_dir, formats, dpi)

    def run(self) -> AnalysisOutput:
        con = duckdb.connect()

        df = con.execute(
            f"""
            SELECT
                DATE_TRUNC('quarter', created_time) AS quarter,
                SUM(count) AS volume_usd
            FROM '{self.trades_dir}/*.parquet'
            GROUP BY quarter
            ORDER BY quarter
            """
        ).df()

        df["quarter"] = pd.to_datetime(df["quarter"])
        quarters = df["quarter"].tolist()
        volumes = df["volume_usd"].tolist()

        # Show bars appearing one-by-one, month by month
        frame_counts = list(range(1, len(quarters) + 1))

        fig, ax = plt.subplots(figsize=(12, 6))
        info_text = ax.text(
            0.02, 0.98, "", transform=ax.transAxes, fontsize=12, fontweight="bold",
            verticalalignment="top", bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        ax.set_xlabel("Date")
        ax.set_ylabel("Quarterly Volume (millions USD)")
        ax.set_title("Kalshi Quarterly Notional Volume")
        ax.set_yscale("log")
        ax.set_ylim(bottom=1)
        plt.tight_layout()

        total_frames = len(frame_counts) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_counts) - 1)
            n = frame_counts[idx]
            ax.clear()
            q = quarters[:n]
            v = volumes[:n]
            v_m = [x / 1e6 for x in v]
            bars = ax.bar(q, v_m, width=80, color="#4C72B0")
            labels = [f"${x / 1e3:.2f}B" if x > 999 else f"${x:.2f}M" for x in v_m]
            ax.bar_label(bars, labels=labels, fontsize=7, rotation=90, label_type="center", color="white", fontweight="bold")
            ax.set_xlabel("Date")
            ax.set_ylabel("Quarterly Volume (millions USD)")
            ax.set_title("Kalshi Quarterly Notional Volume")
            ax.set_yscale("log")
            ax.set_ylim(bottom=1)
            # Set x limit to full range
            if quarters:
                ax.set_xlim(
                    quarters[0] - pd.Timedelta(days=45),
                    quarters[-1] + pd.Timedelta(days=45),
                )
            cum_total = sum(v)
            label = f"${cum_total / 1e9:.2f}B" if cum_total >= 1e9 else f"${cum_total / 1e6:.1f}M"
            ax.text(
                0.02, 0.98, f"Cumulative: {label}",
                transform=ax.transAxes, fontsize=12, fontweight="bold",
                verticalalignment="top",
                bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
            )
            return ()

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        return AnalysisOutput(figure=anim, data=df, metadata={"n_frames": len(frame_counts)})
