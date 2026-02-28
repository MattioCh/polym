"""Animated YES vs NO volume by price evolving over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class YesVsNoByPriceAnimatedAnalysis(Analysis):
    """Animated YES/NO volume preference by price over time."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
    ):
        super().__init__(
            name="yes_vs_no_by_price_animated",
            description="Animated YES vs NO volume by price over time",
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
                DATE_TRUNC('month', created_time) AS month,
                yes_price AS price,
                taker_side,
                SUM(count) AS contracts
            FROM '{self.trades_dir}/*.parquet'
            WHERE yes_price BETWEEN 1 AND 99
            GROUP BY month, yes_price, taker_side
            ORDER BY month, yes_price, taker_side
            """
        ).df()

        cumulative = self._compute_cumulative(df)
        frame_months = sorted(cumulative.keys())

        fig, ax = plt.subplots(figsize=(12, 6))
        plt.tight_layout()

        total_frames = len(frame_months) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_months) - 1)
            month = frame_months[idx]
            data = cumulative[month]
            ax.clear()

            prices = sorted(data.keys())
            taker_yes_pct = []
            maker_yes_pct = []
            taker_no_pct = []
            maker_no_pct = []

            for p in prices:
                d = data[p]
                total = d["taker_yes"] + d["taker_no"] + d["maker_yes"] + d["maker_no"]
                if total > 0:
                    taker_yes_pct.append(d["taker_yes"] / total * 100)
                    maker_yes_pct.append(d["maker_yes"] / total * 100)
                    taker_no_pct.append(d["taker_no"] / total * 100)
                    maker_no_pct.append(d["maker_no"] / total * 100)
                else:
                    taker_yes_pct.append(0)
                    maker_yes_pct.append(0)
                    taker_no_pct.append(0)
                    maker_no_pct.append(0)

            ty = taker_yes_pct
            my_ = maker_yes_pct
            tn = taker_no_pct
            mn = maker_no_pct

            ax.bar(prices, ty, width=1, color="#2ecc71", label="Taker YES")
            ax.bar(prices, my_, width=1, color="#27ae60", label="Maker YES",
                   bottom=ty)
            bottom2 = [a + b for a, b in zip(ty, my_)]
            ax.bar(prices, tn, width=1, color="#e74c3c", label="Taker NO",
                   bottom=bottom2)
            bottom3 = [a + b for a, b in zip(bottom2, tn)]
            ax.bar(prices, mn, width=1, color="#c0392b", label="Maker NO",
                   bottom=bottom3)

            ax.set_xlabel("Contract Price (cents)")
            ax.set_ylabel("Share of Volume (%)")
            ax.set_title("YES vs NO by Price Over Time")
            ax.set_xlim(0, 100)
            ax.set_ylim(0, 100)
            ax.set_xticks(range(0, 101, 10))
            ax.legend(loc="upper right")
            ax.text(
                0.02, 0.12, f"Through {month.strftime('%Y-%m')}",
                transform=ax.transAxes, fontsize=12, fontweight="bold",
                verticalalignment="top",
                bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
            )
            return ()

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Final output
        output_rows = []
        if frame_months:
            data = cumulative[frame_months[-1]]
            for price, d in sorted(data.items()):
                total = d["taker_yes"] + d["taker_no"] + d["maker_yes"] + d["maker_no"]
                if total > 0:
                    output_rows.append({
                        "price": price,
                        "taker_yes": d["taker_yes"],
                        "taker_no": d["taker_no"],
                        "maker_yes": d["maker_yes"],
                        "maker_no": d["maker_no"],
                        "taker_yes_pct": d["taker_yes"] / total * 100,
                        "taker_no_pct": d["taker_no"] / total * 100,
                        "maker_yes_pct": d["maker_yes"] / total * 100,
                        "maker_no_pct": d["maker_no"] / total * 100,
                    })

        return AnalysisOutput(figure=anim, data=pd.DataFrame(output_rows), metadata={"n_frames": len(frame_months)})

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
                no_price = 100 - p
                contracts = int(row["contracts"])

                if p not in running:
                    running[p] = {"taker_yes": 0, "taker_no": 0, "maker_yes": 0, "maker_no": 0}

                if row["taker_side"] == "yes":
                    # Taker bought YES at yes_price, Maker bought NO at no_price
                    running[p]["taker_yes"] += contracts
                    if no_price not in running:
                        running[no_price] = {"taker_yes": 0, "taker_no": 0, "maker_yes": 0, "maker_no": 0}
                    running[no_price]["maker_no"] += contracts
                else:
                    # Taker bought NO at no_price, Maker bought YES at yes_price
                    running[p]["maker_yes"] += contracts
                    if no_price not in running:
                        running[no_price] = {"taker_yes": 0, "taker_no": 0, "maker_yes": 0, "maker_no": 0}
                    running[no_price]["taker_no"] += contracts

            cumulative[month] = {
                p: dict(v) for p, v in running.items()
                if 1 <= p <= 99
            }
        return cumulative
