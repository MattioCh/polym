"""Animated maker vs taker returns by category evolving over time."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.animation import FuncAnimation

from src.analysis.kalshi.util.categories import CATEGORY_SQL, get_group
from src.common.analysis import Analysis, AnalysisOutput

PAUSE_FRAMES = 10


class MakerTakerReturnsByCategoryAnimatedAnalysis(Analysis):
    """Animated maker vs taker excess returns by category over time."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="maker_taker_returns_by_category_animated",
            description="Animated maker vs taker returns by category over time",
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
                SELECT ticker, event_ticker, result
                FROM '{self.markets_dir}/*.parquet'
                WHERE status = 'finalized'
                  AND result IN ('yes', 'no')
            ),
            positions AS (
                -- Taker
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    'taker' AS role,
                    CASE WHEN t.taker_side = 'yes' THEN t.yes_price ELSE t.no_price END AS price,
                    CASE WHEN t.taker_side = m.result THEN 1.0 ELSE 0.0 END AS won,
                    t.count AS contracts
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker

                UNION ALL

                -- Maker
                SELECT
                    DATE_TRUNC('month', t.created_time) AS month,
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    'maker' AS role,
                    CASE WHEN t.taker_side = 'yes' THEN t.no_price ELSE t.yes_price END AS price,
                    CASE WHEN t.taker_side != m.result THEN 1.0 ELSE 0.0 END AS won,
                    t.count AS contracts
                FROM '{self.trades_dir}/*.parquet' t
                INNER JOIN resolved_markets m ON t.ticker = m.ticker
            )
            SELECT month, category, role,
                   SUM(contracts) AS contracts,
                   SUM(won * contracts) AS won_contracts,
                   SUM(contracts * price / 100.0) AS price_contracts
            FROM positions
            GROUP BY month, category, role
            ORDER BY month, category, role
            """
        ).df()

        df["group"] = df["category"].apply(get_group)

        cumulative = self._compute_cumulative(df)
        frame_months = sorted(cumulative.keys())

        # Determine top groups from final month
        top_groups = self._get_top_groups(cumulative, frame_months[-1] if frame_months else None)

        fig, ax = plt.subplots(figsize=(12, 7))
        plt.tight_layout()

        total_frames = len(frame_months) + PAUSE_FRAMES

        def animate(frame_idx: int):
            idx = min(frame_idx, len(frame_months) - 1)
            month = frame_months[idx]
            data = cumulative[month]
            ax.clear()

            groups = top_groups[:8]
            taker_excess = []
            maker_excess = []
            for g in groups:
                t = data.get(g, {}).get("taker", {"contracts": 0, "won": 0, "price_sum": 0})
                m = data.get(g, {}).get("maker", {"contracts": 0, "won": 0, "price_sum": 0})
                t_excess = (t["won"] / t["contracts"] - t["price_sum"] / t["contracts"]) * 100 if t["contracts"] > 0 else 0
                m_excess = (m["won"] / m["contracts"] - m["price_sum"] / m["contracts"]) * 100 if m["contracts"] > 0 else 0
                taker_excess.append(t_excess)
                maker_excess.append(m_excess)

            x = np.arange(len(groups))
            width = 0.35
            ax.bar(x - width / 2, taker_excess, width, label="Taker", color="#e74c3c", alpha=0.8)
            ax.bar(x + width / 2, maker_excess, width, label="Maker", color="#2ecc71", alpha=0.8)
            ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
            ax.set_xlabel("Category")
            ax.set_ylabel("Volume-Weighted Excess Return (pp)")
            ax.set_title("Maker vs Taker Returns by Category Over Time")
            ax.set_xticks(x)
            ax.set_xticklabels(groups, rotation=45, ha="right")
            ax.legend(loc="upper right")
            ax.grid(True, alpha=0.3, axis="y")
            ax.text(
                0.02, 0.98, f"Through {month.strftime('%Y-%m')}",
                transform=ax.transAxes, fontsize=12, fontweight="bold",
                verticalalignment="top",
                bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
            )
            fig.subplots_adjust(bottom=0.25)
            return ()

        anim = FuncAnimation(fig, animate, frames=total_frames, interval=500, blit=False, repeat=False)

        # Final output
        output_rows = []
        if frame_months:
            data = cumulative[frame_months[-1]]
            for group, roles in data.items():
                for role, vals in roles.items():
                    if vals["contracts"] > 0:
                        excess = (vals["won"] / vals["contracts"] - vals["price_sum"] / vals["contracts"]) * 100
                        output_rows.append({
                            "group": group, "role": role,
                            "excess_return": excess, "contracts": vals["contracts"],
                        })

        return AnalysisOutput(figure=anim, data=pd.DataFrame(output_rows), metadata={"n_frames": len(frame_months)})

    def _get_top_groups(self, cumulative: dict, month) -> list[str]:
        if month is None or month not in cumulative:
            return []
        data = cumulative[month]
        group_volume = {}
        for group, roles in data.items():
            total = sum(r["contracts"] for r in roles.values())
            group_volume[group] = total
        return sorted(group_volume, key=group_volume.get, reverse=True)  # type: ignore[arg-type]

    def _compute_cumulative(self, df: pd.DataFrame) -> dict:
        if df.empty:
            return {}
        df = df.copy()
        df["month"] = pd.to_datetime(df["month"])
        if df["month"].dt.tz is not None:
            df["month"] = df["month"].dt.tz_convert(None)

        months = sorted(df["month"].unique())
        cumulative: dict = {}
        running: dict[str, dict[str, dict]] = {}  # group -> role -> {contracts, won, price_sum}

        for month in months:
            chunk = df[df["month"] == month]
            for _, row in chunk.iterrows():
                group = row["group"]
                role = row["role"]
                if group not in running:
                    running[group] = {}
                if role not in running[group]:
                    running[group][role] = {"contracts": 0, "won": 0, "price_sum": 0}
                running[group][role]["contracts"] += float(row["contracts"])
                running[group][role]["won"] += float(row["won_contracts"])
                running[group][role]["price_sum"] += float(row["price_contracts"])
            cumulative[month] = {
                g: {r: dict(v) for r, v in roles.items()}
                for g, roles in running.items()
            }
        return cumulative
