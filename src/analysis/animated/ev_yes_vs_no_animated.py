"""Animated expected value of YES vs NO bets evolving over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class EvYesVsNoAnimatedAnalysis(Analysis):
    """Animated EV comparison of YES vs NO bets evolving over cumulative time windows."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="ev_yes_vs_no_animated",
            description="Animated expected value of YES vs NO bets over time",
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

        # YES side: aggregate by month and yes_price
        yes_df = con.execute(
            f"""
            SELECT
                DATE_TRUNC('month', t.created_time) AS month,
                t.yes_price AS price,
                SUM(t.count) AS total_contracts,
                SUM(CASE WHEN m.result = 'yes' THEN t.count ELSE 0 END) AS yes_wins
            FROM '{self.trades_dir}/*.parquet' t
            INNER JOIN '{self.markets_dir}/*.parquet' m ON t.ticker = m.ticker
            WHERE m.result IN ('yes', 'no')
              AND t.yes_price BETWEEN 1 AND 99
            GROUP BY month, t.yes_price
            ORDER BY month, t.yes_price
            """
        ).df()

        # NO side: aggregate by month and no_price
        no_df = con.execute(
            f"""
            SELECT
                DATE_TRUNC('month', t.created_time) AS month,
                t.no_price AS price,
                SUM(t.count) AS total_contracts,
                SUM(CASE WHEN m.result = 'no' THEN t.count ELSE 0 END) AS no_wins
            FROM '{self.trades_dir}/*.parquet' t
            INNER JOIN '{self.markets_dir}/*.parquet' m ON t.ticker = m.ticker
            WHERE m.result IN ('yes', 'no')
              AND t.no_price BETWEEN 1 AND 99
            GROUP BY month, t.no_price
            ORDER BY month, t.no_price
            """
        ).df()

        yes_cum = self._compute_cumulative(yes_df, "yes_wins")
        no_cum = self._compute_cumulative(no_df, "no_wins")

        frame_months = sorted(set(yes_cum.keys()) | set(no_cum.keys()))

        fig, ax = plt.subplots(figsize=(12, 7))
        (yes_line,) = ax.plot([], [], label="YES bets", color="#2ecc71", linewidth=2.5)
        (no_line,) = ax.plot([], [], label="NO bets", color="#e74c3c", linewidth=2.5)
        ax.axhline(y=0, color="black", linestyle="-", alpha=0.7, linewidth=1)
        ax.axvline(x=50, color="gray", linestyle="--", alpha=0.5)
        info_text = ax.text(
            0.02, 0.98, "", transform=ax.transAxes, fontsize=12, fontweight="bold",
            verticalalignment="top", bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        ax.set_xlabel("Purchase Price (cents)")
        ax.set_ylabel("Expected Value (cents per contract)")
        ax.set_title("Expected Value of YES vs NO Bets Over Time")
        ax.set_xlim(1, 99)
        ax.set_ylim(-15, 15)
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        total_frames = len(frame_months) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_months) - 1)
            month = frame_months[idx]

            # YES EV
            y_data = yes_cum.get(month, {})
            if y_data:
                prices = sorted(y_data.keys())
                evs = [100.0 * y_data[p]["wins"] / y_data[p]["total"] - p for p in prices]
                yes_line.set_data(prices, evs)
            else:
                yes_line.set_data([], [])

            # NO EV
            n_data = no_cum.get(month, {})
            if n_data:
                prices = sorted(n_data.keys())
                evs = [100.0 * n_data[p]["wins"] / n_data[p]["total"] - p for p in prices]
                no_line.set_data(prices, evs)
            else:
                no_line.set_data([], [])

            info_text.set_text(f"Through {month.strftime('%Y-%m')}")
            return yes_line, no_line, info_text

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Final output
        output_rows = []
        if frame_months:
            m = frame_months[-1]
            for side, cum in [("yes", yes_cum), ("no", no_cum)]:
                data = cum.get(m, {})
                for price, vals in data.items():
                    wr = vals["wins"] / vals["total"]
                    output_rows.append({
                        "side": side, "price": price,
                        "win_rate": wr, "ev": 100.0 * wr - price,
                        "total_contracts": vals["total"],
                    })

        return AnalysisOutput(figure=anim, data=pd.DataFrame(output_rows), metadata={"n_frames": len(frame_months)})

    def _compute_cumulative(self, df: pd.DataFrame, wins_col: str) -> dict:
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
                running[p]["total"] += int(row["total_contracts"])
                running[p]["wins"] += int(row[wins_col])
            cumulative[month] = {p: dict(v) for p, v in running.items()}
        return cumulative
