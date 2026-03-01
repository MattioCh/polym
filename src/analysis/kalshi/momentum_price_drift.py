"""Momentum analysis: does intra-market price drift predict resolution?

Surgical definition of momentum
────────────────────────────────
For trade *i* in market *m* at yes_price P_i, the **N-trade momentum** is:

    Δ_N(i) = P_i − P_{i−N}

where P_{i−N} is the yes_price exactly N trades earlier *in the same market*.

  • Δ_N > 0  → YES price is rising   (bullish momentum)
  • Δ_N < 0  → YES price is falling  (bearish momentum)

Momentum-following strategy (taker)
────────────────────────────────────
  • Δ_N > 0 → buy YES at P_i          cost = P_i,     payout = 100 if YES
  • Δ_N < 0 → buy NO  at (100 − P_i)  cost = 100−P_i, payout = 100 if NO
  • Hold to resolution.

Evaluation metric – **excess return per contract** (cents):
    excess = payout − cost = 100 × I(won) − entry_cost

If the market is perfectly efficient (win prob = price / 100),
expected excess is zero.  Positive excess means momentum carries
information beyond the current price.

This analysis tests lookback windows of 3, 5, 10, 25, and 50 trades
and reports excess return, win rate, t-statistic, and breakdowns by
momentum magnitude and direction.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType

# ── Lookback windows (in trades) ─────────────────────────────────────────────
LOOKBACKS = [3, 5, 10, 25, 50]

# ── SQL helpers ──────────────────────────────────────────────────────────────

def _follow_pnl(col: str) -> str:
    """SQL CASE: PnL per contract when following momentum."""
    return f"""CASE
        WHEN {col} > 0 AND result = 'yes' THEN 100 - yes_price
        WHEN {col} > 0 AND result = 'no'  THEN -yes_price
        WHEN {col} < 0 AND result = 'no'  THEN yes_price
        WHEN {col} < 0 AND result = 'yes' THEN -(100 - yes_price)
    END"""


def _follow_won(col: str) -> str:
    """SQL CASE: 1.0 if following momentum would have won."""
    return f"""CASE
        WHEN {col} > 0 AND result = 'yes' THEN 1.0
        WHEN {col} > 0 AND result = 'no'  THEN 0.0
        WHEN {col} < 0 AND result = 'no'  THEN 1.0
        WHEN {col} < 0 AND result = 'yes' THEN 0.0
    END"""


def _follow_cost(col: str) -> str:
    """SQL CASE: entry cost when following momentum."""
    return f"""CASE
        WHEN {col} > 0 THEN yes_price
        WHEN {col} < 0 THEN 100 - yes_price
    END"""


# ── Analysis class ───────────────────────────────────────────────────────────

class MomentumPriceDriftAnalysis(Analysis):
    """Tests whether intra-market price momentum predicts market resolution."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="momentum_price_drift",
            description="Does intra-market price drift predict market resolution?",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        con = duckdb.connect()

        with self.progress("Loading trades and markets"):
            self._load_data(con)

        with self.progress("Computing momentum signals (window functions on all trades)"):
            self._compute_momentum_table(con)

        with self.progress("Computing baseline taker excess"):
            baseline = self._compute_baseline(con)

        with self.progress("Aggregating by lookback window"):
            by_lookback = self._aggregate_by_lookback(con)

        with self.progress("Aggregating by momentum magnitude"):
            by_magnitude = self._aggregate_by_magnitude(con)

        with self.progress("Splitting bullish vs bearish momentum"):
            by_direction = self._aggregate_by_direction(con)

        with self.progress("Analyzing actual taker alignment with momentum"):
            taker_alignment = self._aggregate_taker_alignment(con)

        fig = self._create_figure(by_lookback, by_magnitude, by_direction, taker_alignment, baseline)
        chart = self._create_chart(by_lookback, baseline)

        # Combine all views into one CSV
        output_data = pd.concat(
            [
                by_lookback.assign(view="by_lookback"),
                by_magnitude.assign(view="by_magnitude"),
                by_direction.assign(view="by_direction"),
                taker_alignment.assign(view="taker_alignment"),
            ],
            ignore_index=True,
        )

        return AnalysisOutput(
            figure=fig,
            data=output_data,
            chart=chart,
            metadata={
                "baseline_taker_excess": baseline,
                "by_lookback": by_lookback,
                "by_magnitude": by_magnitude,
                "by_direction": by_direction,
                "taker_alignment": taker_alignment,
            },
        )

    # ── Data loading ─────────────────────────────────────────────────────────

    def _load_data(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(f"""
            CREATE TABLE trades AS
            SELECT ticker, yes_price, no_price, taker_side,
                   count AS contracts, created_time
            FROM '{self.trades_dir}/*.parquet'
            WHERE yes_price BETWEEN 1 AND 99
        """)
        con.execute(f"""
            CREATE TABLE markets AS
            SELECT ticker, result, close_time
            FROM '{self.markets_dir}/*.parquet'
            WHERE status = 'finalized'
              AND result IN ('yes', 'no')
              AND close_time IS NOT NULL
        """)

    def _compute_momentum_table(self, con: duckdb.DuckDBPyConnection) -> None:
        lag_cols = ",\n                ".join(
            f"t.yes_price - LAG(t.yes_price, {n}) OVER w AS m{n}"
            for n in LOOKBACKS
        )
        con.execute(f"""
            CREATE TEMP TABLE momentum_all AS
            SELECT
                t.yes_price,
                t.contracts,
                t.taker_side,
                m.result,
                {lag_cols}
            FROM trades t
            INNER JOIN markets m ON t.ticker = m.ticker
            WINDOW w AS (PARTITION BY t.ticker ORDER BY t.created_time)
        """)

    # ── Baseline ─────────────────────────────────────────────────────────────

    def _compute_baseline(self, con: duckdb.DuckDBPyConnection) -> float:
        """Average taker PnL per contract across ALL trades (no signal)."""
        row = con.execute("""
            SELECT
                SUM(
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no'  THEN -yes_price * contracts
                        WHEN taker_side = 'no'  AND result = 'no'  THEN yes_price * contracts
                        WHEN taker_side = 'no'  AND result = 'yes' THEN -(100 - yes_price) * contracts
                    END
                ) * 1.0 / NULLIF(SUM(contracts), 0)
            FROM momentum_all
        """).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    # ── Aggregations ─────────────────────────────────────────────────────────

    def _aggregate_by_lookback(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        parts: list[pd.DataFrame] = []
        for n in LOOKBACKS:
            col = f"m{n}"
            df = con.execute(f"""
                WITH filtered AS (
                    SELECT *, ({_follow_pnl(col)}) AS fpnl
                    FROM momentum_all
                    WHERE {col} IS NOT NULL AND {col} != 0
                ),
                zero_stats AS (
                    SELECT
                        COUNT(CASE WHEN {col} = 0 THEN 1 END) * 100.0
                            / NULLIF(COUNT(*), 0) AS pct_zero
                    FROM momentum_all
                    WHERE {col} IS NOT NULL
                )
                SELECT
                    {n}                          AS lookback,
                    COUNT(*)                     AS n_trades,
                    SUM(contracts)               AS n_contracts,
                    SUM(fpnl * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0)
                                                 AS follow_excess,
                    AVG(fpnl)                    AS follow_excess_unweighted,
                    STDDEV_SAMP(fpnl)            AS follow_std,
                    AVG(fpnl)
                        / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0)
                                                 AS t_stat,
                    SUM(({_follow_won(col)}) * contracts) * 100.0
                        / NULLIF(SUM(contracts), 0)
                                                 AS follow_win_rate,
                    SUM(({_follow_cost(col)}) * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0)
                                                 AS follow_avg_implied,
                    (SELECT pct_zero FROM zero_stats)
                                                 AS pct_zero_momentum
                FROM filtered
            """).df()
            parts.append(df)
        return pd.concat(parts, ignore_index=True)

    def _aggregate_by_magnitude(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Excess return by |momentum| bucket (using 10-trade lookback)."""
        col = "m10"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *, ({_follow_pnl(col)}) AS fpnl
                FROM momentum_all
                WHERE {col} IS NOT NULL AND {col} != 0
            )
            SELECT
                10 AS lookback,
                CASE
                    WHEN ABS({col}) BETWEEN 1 AND 2 THEN '01-02'
                    WHEN ABS({col}) BETWEEN 3 AND 5 THEN '03-05'
                    WHEN ABS({col}) BETWEEN 6 AND 10 THEN '06-10'
                    WHEN ABS({col}) BETWEEN 11 AND 20 THEN '11-20'
                    WHEN ABS({col}) > 20 THEN '21+'
                END AS momentum_magnitude,
                COUNT(*)                  AS n_trades,
                SUM(contracts)            AS n_contracts,
                SUM(fpnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS follow_excess,
                AVG(fpnl)                 AS follow_excess_unweighted,
                AVG(fpnl)
                    / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0)
                                          AS t_stat,
                SUM(({_follow_won(col)}) * contracts) * 100.0
                    / NULLIF(SUM(contracts), 0)
                                          AS follow_win_rate,
                SUM(({_follow_cost(col)}) * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS follow_avg_implied
            FROM filtered
            GROUP BY 2
            ORDER BY 2
        """).df()

    def _aggregate_by_direction(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Bullish (Δ > 0) vs bearish (Δ < 0) momentum, 10-trade lookback."""
        col = "m10"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *, ({_follow_pnl(col)}) AS fpnl
                FROM momentum_all
                WHERE {col} IS NOT NULL AND {col} != 0
            )
            SELECT
                10 AS lookback,
                CASE WHEN {col} > 0 THEN 'bullish' ELSE 'bearish' END AS direction,
                COUNT(*)                  AS n_trades,
                SUM(contracts)            AS n_contracts,
                SUM(fpnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS follow_excess,
                AVG(fpnl)                 AS follow_excess_unweighted,
                AVG(fpnl)
                    / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0)
                                          AS t_stat,
                SUM(({_follow_won(col)}) * contracts) * 100.0
                    / NULLIF(SUM(contracts), 0)
                                          AS follow_win_rate,
                SUM(({_follow_cost(col)}) * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS follow_avg_implied
            FROM filtered
            GROUP BY 2
            ORDER BY 2
        """).df()

    def _aggregate_taker_alignment(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Do actual takers follow or fade momentum, and how does each perform?"""
        col = "m10"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *
                FROM momentum_all
                WHERE {col} IS NOT NULL AND {col} != 0
            )
            SELECT
                10 AS lookback,
                CASE
                    WHEN ({col} > 0 AND taker_side = 'yes')
                      OR ({col} < 0 AND taker_side = 'no')
                    THEN 'follows_momentum'
                    ELSE 'fades_momentum'
                END AS taker_alignment,
                COUNT(*)       AS n_trades,
                SUM(contracts) AS n_contracts,
                -- Actual taker excess (what the taker really bought)
                SUM(
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no'  THEN -yes_price * contracts
                        WHEN taker_side = 'no'  AND result = 'no'  THEN yes_price * contracts
                        WHEN taker_side = 'no'  AND result = 'yes' THEN -(100 - yes_price) * contracts
                    END
                ) * 1.0 / NULLIF(SUM(contracts), 0)
                                AS taker_excess,
                SUM(
                    CASE WHEN taker_side = result THEN 1.0 ELSE 0.0 END * contracts
                ) * 100.0 / NULLIF(SUM(contracts), 0)
                                AS taker_win_rate,
                SUM(
                    CASE WHEN taker_side = 'yes' THEN yes_price ELSE 100 - yes_price END * contracts
                ) * 1.0 / NULLIF(SUM(contracts), 0)
                                AS taker_avg_implied
            FROM filtered
            GROUP BY 2
            ORDER BY 2
        """).df()

    # ── Visualization ────────────────────────────────────────────────────────

    def _create_figure(
        self,
        by_lookback: pd.DataFrame,
        by_magnitude: pd.DataFrame,
        by_direction: pd.DataFrame,
        taker_alignment: pd.DataFrame,
        baseline: float,
    ) -> plt.Figure:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(
            "Momentum Price Drift: Does Following Intra-Market Price Trends Pay Off?",
            fontsize=14,
            fontweight="bold",
            y=0.98,
        )

        # ── Panel 1: Follow excess by lookback ───────────────────────────────
        ax = axes[0, 0]
        x = np.arange(len(by_lookback))
        bars = ax.bar(x, by_lookback["follow_excess"], color="#3498db", alpha=0.85, edgecolor="white")
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--",
                    label=f"Baseline taker excess ({baseline:.2f}¢)")
        ax.set_xticks(x)
        ax.set_xticklabels([str(n) for n in by_lookback["lookback"]])
        ax.set_xlabel("Lookback Window (trades)")
        ax.set_ylabel("Excess Return (¢ / contract)")
        ax.set_title("Follow-Momentum Excess by Lookback")
        ax.legend(fontsize=8)
        # Annotate t-stats
        for i, (_, row) in enumerate(by_lookback.iterrows()):
            t = row.get("t_stat", 0)
            if pd.notna(t):
                ax.annotate(
                    f't={t:.1f}',
                    (i, row["follow_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["follow_excess"] >= 0 else -14),
                    ha="center",
                    fontsize=7,
                    color="#555",
                )

        # ── Panel 2: Excess by |momentum| magnitude ─────────────────────────
        ax = axes[0, 1]
        mag = by_magnitude.dropna(subset=["momentum_magnitude"])
        x = np.arange(len(mag))
        colors = ["#27ae60" if v >= 0 else "#c0392b" for v in mag["follow_excess"]]
        ax.bar(x, mag["follow_excess"], color=colors, alpha=0.85, edgecolor="white")
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--")
        ax.set_xticks(x)
        ax.set_xticklabels(mag["momentum_magnitude"], rotation=45, ha="right")
        ax.set_xlabel("|Momentum| Bucket (¢)")
        ax.set_ylabel("Excess Return (¢ / contract)")
        ax.set_title("Excess by Momentum Magnitude (10-trade)")
        for i, (_, row) in enumerate(mag.iterrows()):
            t = row.get("t_stat", 0)
            if pd.notna(t):
                ax.annotate(
                    f't={t:.1f}',
                    (i, row["follow_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["follow_excess"] >= 0 else -14),
                    ha="center",
                    fontsize=7,
                    color="#555",
                )

        # ── Panel 3: Bullish vs Bearish momentum ────────────────────────────
        ax = axes[1, 0]
        x = np.arange(len(by_direction))
        colors_dir = {"bullish": "#2980b9", "bearish": "#e67e22"}
        bars = ax.bar(
            x,
            by_direction["follow_excess"],
            color=[colors_dir.get(d, "#999") for d in by_direction["direction"]],
            alpha=0.85,
            edgecolor="white",
        )
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--",
                    label=f"Baseline ({baseline:.2f}¢)")
        ax.set_xticks(x)
        ax.set_xticklabels(
            [f"{d}\n(win {row['follow_win_rate']:.1f}% vs impl {row['follow_avg_implied']:.0f}%)"
             for d, (_, row) in zip(by_direction["direction"], by_direction.iterrows())],
            fontsize=8,
        )
        ax.set_xlabel("Momentum Direction (10-trade)")
        ax.set_ylabel("Excess Return (¢ / contract)")
        ax.set_title("Bullish vs Bearish Momentum")
        ax.legend(fontsize=8)

        # ── Panel 4: Taker alignment ─────────────────────────────────────────
        ax = axes[1, 1]
        x = np.arange(len(taker_alignment))
        alignment_colors = {"follows_momentum": "#27ae60", "fades_momentum": "#c0392b"}
        bars = ax.bar(
            x,
            taker_alignment["taker_excess"],
            color=[alignment_colors.get(a, "#999") for a in taker_alignment["taker_alignment"]],
            alpha=0.85,
            edgecolor="white",
        )
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.set_xticks(x)
        labels = []
        for _, row in taker_alignment.iterrows():
            pct = row["n_contracts"] / taker_alignment["n_contracts"].sum() * 100
            labels.append(
                f"{row['taker_alignment'].replace('_', ' ').title()}\n"
                f"({pct:.0f}% of volume)"
            )
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_ylabel("Actual Taker Excess (¢ / contract)")
        ax.set_title("Do Takers Who Follow Momentum Outperform?")
        for i, (_, row) in enumerate(taker_alignment.iterrows()):
            ax.annotate(
                f'{row["taker_excess"]:.2f}¢',
                (i, row["taker_excess"]),
                textcoords="offset points",
                xytext=(0, 8 if row["taker_excess"] >= 0 else -14),
                ha="center",
                fontsize=9,
                fontweight="bold",
            )

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        return fig

    # ── Chart config (web) ───────────────────────────────────────────────────

    def _create_chart(self, by_lookback: pd.DataFrame, baseline: float) -> ChartConfig:
        chart_data = []
        for _, row in by_lookback.iterrows():
            chart_data.append(
                {
                    "lookback": int(row["lookback"]),
                    "follow_excess": round(float(row["follow_excess"]), 3),
                    "baseline": round(baseline, 3),
                    "follow_win_rate": round(float(row["follow_win_rate"]), 2),
                    "implied_prob": round(float(row["follow_avg_implied"]), 2),
                }
            )
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="lookback",
            yKeys=["follow_excess", "baseline"],
            title="Momentum Follow Excess Return vs Baseline Taker Excess",
            xLabel="Lookback Window (trades)",
            yLabel="Excess Return (cents / contract)",
            yUnit=UnitType.CENTS,
            strokeDasharrays=[None, "5 5"],
            caption=(
                "Each bar shows the average PnL per contract from buying in the "
                "direction of recent price drift.  Dashed line = naive taker average."
            ),
        )
