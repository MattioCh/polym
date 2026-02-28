"""Animated mispricing by price evolving over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class MispricingByPriceAnimatedAnalysis(Analysis):
    """Animated mispricing (actual - implied) by price for takers, makers, and combined."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="mispricing_by_price_animated",
            description="Animated mispricing by contract price over time",
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
            all_positions AS (
                -- Taker positions
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    'taker' AS role,
                    CASE WHEN t.taker_side = 'yes' THEN t.yes_price ELSE t.no_price END AS price,
                    CASE WHEN t.taker_side = m.result THEN 1 ELSE 0 END AS won
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker

                UNION ALL

                -- Maker positions
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    'maker' AS role,
                    CASE WHEN t.taker_side = 'yes' THEN t.no_price ELSE t.yes_price END AS price,
                    CASE WHEN t.taker_side != m.result THEN 1 ELSE 0 END AS won
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
            )
            SELECT month, role, price, COUNT(*) AS total, SUM(won) AS wins
            FROM all_positions
            WHERE price BETWEEN 1 AND 99
            GROUP BY month, role, price
            ORDER BY month, role, price
            """
        ).df()

        cumulative = self._compute_cumulative(df)
        frame_months = sorted(cumulative.keys())

        fig, ax = plt.subplots(figsize=(10, 6))
        (taker_line,) = ax.plot([], [], "o", markersize=4, alpha=0.7, color="#e74c3c", label="Taker")
        (maker_line,) = ax.plot([], [], "o", markersize=4, alpha=0.7, color="#2ecc71", label="Maker")
        (combined_line,) = ax.plot([], [], "o", markersize=4, alpha=0.7, color="#4C72B0", label="Combined")
        ax.axhline(y=0, linestyle="--", color="gray", linewidth=1.5, label="Perfect calibration")
        info_text = ax.text(
            0.02, 0.98, "", transform=ax.transAxes, fontsize=12, fontweight="bold",
            verticalalignment="top", bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        )
        ax.set_xlabel("Contract Price (cents)")
        ax.set_ylabel("Mispricing (%)")
        ax.set_title("Mispricing by Contract Price Over Time")
        ax.set_xlim(0, 100)
        ax.set_xticks(range(0, 101, 10))
        ax.legend(loc="lower right")
        plt.tight_layout()

        total_frames = len(frame_months) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_months) - 1)
            month = frame_months[idx]
            data = cumulative[month]

            for role, line in [("taker", taker_line), ("maker", maker_line)]:
                role_data = data.get(role, {})
                prices = sorted(role_data.keys())
                if prices:
                    mispricing = [
                        (role_data[p]["wins"] / role_data[p]["total"] * 100 - p) / p * 100
                        for p in prices
                    ]
                    line.set_data(prices, mispricing)
                else:
                    line.set_data([], [])

            # Combined
            taker_d = data.get("taker", {})
            maker_d = data.get("maker", {})
            all_prices = sorted(set(taker_d.keys()) | set(maker_d.keys()))
            if all_prices:
                combined_mispricing = []
                for p in all_prices:
                    t_total = taker_d.get(p, {}).get("total", 0)
                    t_wins = taker_d.get(p, {}).get("wins", 0)
                    m_total = maker_d.get(p, {}).get("total", 0)
                    m_wins = maker_d.get(p, {}).get("wins", 0)
                    total = t_total + m_total
                    wins = t_wins + m_wins
                    if total > 0:
                        combined_mispricing.append((wins / total * 100 - p) / p * 100)
                    else:
                        combined_mispricing.append(0)
                combined_line.set_data(all_prices, combined_mispricing)
            else:
                combined_line.set_data([], [])

            info_text.set_text(f"Through {month.strftime('%Y-%m')}")
            return taker_line, maker_line, combined_line, info_text

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Final output
        output_rows = []
        if frame_months:
            data = cumulative[frame_months[-1]]
            for role in ("taker", "maker"):
                for price, vals in data.get(role, {}).items():
                    wr = vals["wins"] / vals["total"] * 100
                    output_rows.append({
                        "role": role, "price": price,
                        "win_rate": wr, "mispricing_pct": (wr - price) / price * 100,
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
                role = row["role"]
                p = int(row["price"])
                if role not in running:
                    running[role] = {}
                if p not in running[role]:
                    running[role][p] = {"total": 0, "wins": 0}
                running[role][p]["total"] += int(row["total"])
                running[role][p]["wins"] += int(row["wins"])
            cumulative[month] = {
                role: {p: dict(v) for p, v in prices.items()}
                for role, prices in running.items()
            }
        return cumulative
