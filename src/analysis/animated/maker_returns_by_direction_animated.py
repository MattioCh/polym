"""Animated maker returns by direction (YES vs NO) over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class MakerReturnsByDirectionAnimatedAnalysis(Analysis):
    """Animated maker excess returns by YES/NO direction evolving over time."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="maker_returns_by_direction_animated",
            description="Animated maker excess returns by direction over time",
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
            ),
            maker_positions AS (
                -- Maker bought YES (taker bought NO)
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    t.yes_price AS price,
                    CASE WHEN m.result = 'yes' THEN 1.0 ELSE 0.0 END AS won,
                    'YES' AS maker_side
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.taker_side = 'no'

                UNION ALL

                -- Maker bought NO (taker bought YES)
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    t.no_price AS price,
                    CASE WHEN m.result = 'no' THEN 1.0 ELSE 0.0 END AS won,
                    'NO' AS maker_side
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
                WHERE t.taker_side = 'yes'
            )
            SELECT month, maker_side, price,
                   COUNT(*) AS total,
                   SUM(won) AS wins
            FROM maker_positions
            WHERE price BETWEEN 1 AND 99
            GROUP BY month, maker_side, price
            ORDER BY month, maker_side, price
            """
        ).df()

        cumulative = self._compute_cumulative(df)
        frame_months = sorted(cumulative.keys())

        fig, ax = plt.subplots(figsize=(12, 7))
        (yes_line,) = ax.plot([], [], color="#2ecc71", linewidth=1.5, label="Maker bought YES", alpha=0.8)
        (no_line,) = ax.plot([], [], color="#e74c3c", linewidth=1.5, label="Maker bought NO", alpha=0.8)
        ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
        info_text = ax.text(
            0.02, 0.98, "", transform=ax.transAxes, fontsize=12, fontweight="bold",
            verticalalignment="top", bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        ax.set_xlabel("Maker's Purchase Price (cents)")
        ax.set_ylabel("Excess Return (pp)")
        ax.set_title("Maker Excess Returns by Direction Over Time")
        ax.set_xlim(1, 99)
        ax.set_ylim(-30, 30)
        ax.set_xticks(range(0, 101, 10))
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        total_frames = len(frame_months) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_months) - 1)
            month = frame_months[idx]
            data = cumulative[month]

            for side, line in [("YES", yes_line), ("NO", no_line)]:
                side_data = data.get(side, {})
                prices = sorted(side_data.keys())
                if prices:
                    excess = [(side_data[p]["wins"] / side_data[p]["total"] - p / 100.0) * 100 for p in prices]
                    line.set_data(prices, excess)
                else:
                    line.set_data([], [])

            info_text.set_text(f"Through {month.strftime('%Y-%m')}")
            return yes_line, no_line, info_text

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Final frame output
        output_rows = []
        if frame_months:
            data = cumulative[frame_months[-1]]
            for side in ("YES", "NO"):
                for price, vals in data.get(side, {}).items():
                    wr = vals["wins"] / vals["total"]
                    output_rows.append({
                        "maker_side": side, "price": price,
                        "win_rate": wr, "excess_return": (wr - price / 100.0) * 100,
                        "total": vals["total"],
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
        running: dict[str, dict[int, dict]] = {}

        for month in months:
            chunk = df[df["month"] == month]
            for _, row in chunk.iterrows():
                side = row["maker_side"]
                p = int(row["price"])
                if side not in running:
                    running[side] = {}
                if p not in running[side]:
                    running[side][p] = {"total": 0, "wins": 0}
                running[side][p]["total"] += int(row["total"])
                running[side][p]["wins"] += int(row["wins"])
            cumulative[month] = {
                side: {p: dict(v) for p, v in prices.items()}
                for side, prices in running.items()
            }
        return cumulative
