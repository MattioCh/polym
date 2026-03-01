"""Mean-reversion analysis: does fading price deviations from recent averages pay off?

Surgical definition of mean-reversion signal
─────────────────────────────────────────────
For trade *i* in market *m* at yes_price P_i, compute the **rolling moving
average** over the last k trades *in the same market*:

    MA_k(i) = (1/k) × Σ_{j=i-k}^{i-1}  P_j

The **deviation** is:

    D_k(i) = P_i − MA_k(i)

  • D_k > 0 → price is ABOVE its recent average (overextended up)
  • D_k < 0 → price is BELOW its recent average (overextended down)

Mean-reversion strategy (taker – "fade the deviation"):
  • D_k > 0 → buy NO  at (100 − P_i)    (bet price reverts down → NO wins)
  • D_k < 0 → buy YES at P_i             (bet price reverts up  → YES wins)
  • Hold to resolution.

This is the *exact opposite* of the momentum-following strategy.  The momentum
analysis showed that following momentum loses ≈ −0.25 to −0.63¢ per contract.
Mean-reversion tests whether the *inverse* is profitable.

Key pivot questions this analysis explores:
  1. Which lookback window (10–200 trades) produces the best fade signal?
  2. Does fade excess improve with larger deviations (|D_k|)?
  3. Is there an asymmetry between fading up-moves vs down-moves?
  4. How does the mean-reversion signal compare to median and VWAP baselines?
  5. What fraction of takers naturally fade vs follow, and who does better?

Evaluation metric – **fade excess return per contract** (cents):
    If D_k > 0: fade_pnl = (YES resolves NO → +no_price) or (YES → −no_price)
    If D_k < 0: fade_pnl = (YES resolves YES → +yes_price) or (NO → −yes_price)
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
# Wider range than momentum: mean-reversion may need longer memory
LOOKBACKS = [10, 25, 50, 100, 200]

# ── SQL helpers ──────────────────────────────────────────────────────────────


def _fade_pnl(col: str) -> str:
    """SQL CASE: PnL per contract when FADING the signal (mean-reversion).

    D > 0 (price above MA) → buy NO → profit if result='no'
    D < 0 (price below MA) → buy YES → profit if result='yes'
    """
    return f"""CASE
        WHEN {col} > 0 AND result = 'no'  THEN yes_price
        WHEN {col} > 0 AND result = 'yes' THEN -(100 - yes_price)
        WHEN {col} < 0 AND result = 'yes' THEN (100 - yes_price)
        WHEN {col} < 0 AND result = 'no'  THEN -yes_price
    END"""


def _fade_won(col: str) -> str:
    """SQL CASE: 1.0 if fading would have won."""
    return f"""CASE
        WHEN {col} > 0 AND result = 'no'  THEN 1.0
        WHEN {col} > 0 AND result = 'yes' THEN 0.0
        WHEN {col} < 0 AND result = 'yes' THEN 1.0
        WHEN {col} < 0 AND result = 'no'  THEN 0.0
    END"""


def _fade_cost(col: str) -> str:
    """SQL CASE: entry cost when fading the signal.

    D > 0 → buying NO at (100 - yes_price)
    D < 0 → buying YES at yes_price
    """
    return f"""CASE
        WHEN {col} > 0 THEN 100 - yes_price
        WHEN {col} < 0 THEN yes_price
    END"""


# ── Analysis class ───────────────────────────────────────────────────────────


class MeanReversionPriceAnalysis(Analysis):
    """Tests whether fading price deviations from moving averages is profitable."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="mean_reversion_price",
            description="Does fading deviations from moving-average price produce taker alpha?",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        con = duckdb.connect()
        # Tune for large workload
        con.execute("SET preserve_insertion_order = false")

        with self.progress("Loading trades and markets"):
            self._load_data(con)

        with self.progress("Computing MA deviations (window functions)"):
            self._compute_deviation_table(con)

        with self.progress("Computing baseline taker excess"):
            baseline = self._compute_baseline(con)

        with self.progress("Aggregating by lookback window"):
            by_lookback = self._aggregate_by_lookback(con)

        with self.progress("Aggregating by deviation magnitude"):
            by_magnitude = self._aggregate_by_magnitude(con)

        with self.progress("Splitting fade-up vs fade-down"):
            by_direction = self._aggregate_by_direction(con)

        with self.progress("Comparing MA vs Median vs VWAP"):
            center_comparison = self._compare_centers(con)

        with self.progress("Analyzing actual taker alignment"):
            taker_alignment = self._aggregate_taker_alignment(con)

        fig = self._create_figure(
            by_lookback, by_magnitude, by_direction,
            center_comparison, taker_alignment, baseline,
        )
        chart = self._create_chart(by_lookback, baseline)

        output_data = pd.concat(
            [
                by_lookback.assign(view="by_lookback"),
                by_magnitude.assign(view="by_magnitude"),
                by_direction.assign(view="by_direction"),
                center_comparison.assign(view="center_comparison"),
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
                "center_comparison": center_comparison,
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

    def _compute_deviation_table(self, con: duckdb.DuckDBPyConnection) -> None:
        """Compute MA, Median, and VWAP deviations on trades, then join markets.

        Done in two steps to avoid OOM:
        1. Window functions on trades alone
        2. Join with markets
        """
        # Step 1: all deviation signals on trades table
        ma_cols = ",\n                ".join(
            f"yes_price - AVG(yes_price) OVER "
            f"(PARTITION BY ticker ORDER BY created_time "
            f"ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING) AS dev_ma{n}"
            for n in LOOKBACKS
        )
        # Also compute 50-trade median deviation and 50-trade VWAP deviation
        con.execute(f"""
            CREATE TEMP TABLE trades_dev AS
            SELECT
                ticker,
                yes_price,
                no_price,
                taker_side,
                contracts,
                created_time,
                {ma_cols},
                -- Median deviation (50-trade)
                yes_price - MEDIAN(yes_price) OVER
                    (PARTITION BY ticker ORDER BY created_time
                     ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) AS dev_med50,
                -- VWAP deviation (50-trade)
                yes_price - (
                    SUM(yes_price * contracts) OVER
                        (PARTITION BY ticker ORDER BY created_time
                         ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING)
                    * 1.0 / NULLIF(
                        SUM(contracts) OVER
                            (PARTITION BY ticker ORDER BY created_time
                             ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING),
                        0)
                ) AS dev_vwap50
            FROM trades
        """)

        # Step 2: join with markets
        con.execute("""
            CREATE TEMP TABLE mr_all AS
            SELECT
                t.*,
                m.result
            FROM trades_dev t
            INNER JOIN markets m ON t.ticker = m.ticker
        """)
        con.execute("DROP TABLE trades_dev")

    # ── Baseline ─────────────────────────────────────────────────────────────

    def _compute_baseline(self, con: duckdb.DuckDBPyConnection) -> float:
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
            FROM mr_all
        """).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    # ── Aggregations ─────────────────────────────────────────────────────────

    def _aggregate_by_lookback(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Fade excess return by lookback window."""
        parts: list[pd.DataFrame] = []
        for n in LOOKBACKS:
            col = f"dev_ma{n}"
            df = con.execute(f"""
                WITH filtered AS (
                    SELECT *, ({_fade_pnl(col)}) AS fpnl
                    FROM mr_all
                    WHERE {col} IS NOT NULL AND ABS({col}) >= 0.5
                )
                SELECT
                    {n}                          AS lookback,
                    'MA'                         AS center_type,
                    COUNT(*)                     AS n_trades,
                    SUM(contracts)               AS n_contracts,
                    SUM(fpnl * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0)
                                                 AS fade_excess,
                    AVG(fpnl)                    AS fade_excess_unweighted,
                    STDDEV_SAMP(fpnl)            AS fade_std,
                    AVG(fpnl)
                        / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0)
                                                 AS t_stat,
                    SUM(({_fade_won(col)}) * contracts) * 100.0
                        / NULLIF(SUM(contracts), 0)
                                                 AS fade_win_rate,
                    SUM(({_fade_cost(col)}) * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0)
                                                 AS fade_avg_implied,
                    AVG(ABS({col}))              AS avg_abs_deviation
                FROM filtered
            """).df()
            parts.append(df)
        return pd.concat(parts, ignore_index=True)

    def _aggregate_by_magnitude(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Fade excess by deviation magnitude bucket (50-trade MA)."""
        col = "dev_ma50"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *, ({_fade_pnl(col)}) AS fpnl
                FROM mr_all
                WHERE {col} IS NOT NULL AND ABS({col}) >= 0.5
            )
            SELECT
                50 AS lookback,
                CASE
                    WHEN ABS({col}) < 2   THEN '00-02'
                    WHEN ABS({col}) < 5   THEN '02-05'
                    WHEN ABS({col}) < 10  THEN '05-10'
                    WHEN ABS({col}) < 20  THEN '10-20'
                    WHEN ABS({col}) >= 20 THEN '20+'
                END AS deviation_magnitude,
                COUNT(*)                  AS n_trades,
                SUM(contracts)            AS n_contracts,
                SUM(fpnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS fade_excess,
                AVG(fpnl)                 AS fade_excess_unweighted,
                AVG(fpnl)
                    / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0)
                                          AS t_stat,
                SUM(({_fade_won(col)}) * contracts) * 100.0
                    / NULLIF(SUM(contracts), 0)
                                          AS fade_win_rate,
                SUM(({_fade_cost(col)}) * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS fade_avg_implied
            FROM filtered
            GROUP BY 2
            ORDER BY 2
        """).df()

    def _aggregate_by_direction(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Fade-up (price above MA → buy NO) vs fade-down (price below MA → buy YES)."""
        col = "dev_ma50"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *, ({_fade_pnl(col)}) AS fpnl
                FROM mr_all
                WHERE {col} IS NOT NULL AND ABS({col}) >= 0.5
            )
            SELECT
                50 AS lookback,
                CASE WHEN {col} > 0 THEN 'fade_up (buy NO)' ELSE 'fade_down (buy YES)' END AS direction,
                COUNT(*)                  AS n_trades,
                SUM(contracts)            AS n_contracts,
                SUM(fpnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS fade_excess,
                AVG(fpnl)                 AS fade_excess_unweighted,
                AVG(fpnl)
                    / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0)
                                          AS t_stat,
                SUM(({_fade_won(col)}) * contracts) * 100.0
                    / NULLIF(SUM(contracts), 0)
                                          AS fade_win_rate,
                SUM(({_fade_cost(col)}) * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                          AS fade_avg_implied
            FROM filtered
            GROUP BY 2
            ORDER BY 2
        """).df()

    def _compare_centers(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Compare MA50 vs Median50 vs VWAP50 as the "center" to revert to."""
        parts: list[pd.DataFrame] = []
        for col, name in [
            ("dev_ma50", "MA(50)"),
            ("dev_med50", "Median(50)"),
            ("dev_vwap50", "VWAP(50)"),
        ]:
            df = con.execute(f"""
                WITH filtered AS (
                    SELECT *, ({_fade_pnl(col)}) AS fpnl
                    FROM mr_all
                    WHERE {col} IS NOT NULL AND ABS({col}) >= 0.5
                )
                SELECT
                    50 AS lookback,
                    '{name}' AS center_type,
                    COUNT(*) AS n_trades,
                    SUM(contracts) AS n_contracts,
                    SUM(fpnl * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0) AS fade_excess,
                    AVG(fpnl) AS fade_excess_unweighted,
                    AVG(fpnl)
                        / NULLIF(STDDEV_SAMP(fpnl) / SQRT(COUNT(*)), 0) AS t_stat,
                    SUM(({_fade_won(col)}) * contracts) * 100.0
                        / NULLIF(SUM(contracts), 0) AS fade_win_rate,
                    SUM(({_fade_cost(col)}) * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0) AS fade_avg_implied,
                    AVG(ABS({col})) AS avg_abs_deviation
                FROM filtered
            """).df()
            parts.append(df)
        return pd.concat(parts, ignore_index=True)

    def _aggregate_taker_alignment(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Do actual takers naturally fade or follow the MA deviation?"""
        col = "dev_ma50"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *
                FROM mr_all
                WHERE {col} IS NOT NULL AND ABS({col}) >= 0.5
            )
            SELECT
                50 AS lookback,
                CASE
                    -- Taker fades: price above MA → taker buys NO; price below MA → taker buys YES
                    WHEN ({col} > 0 AND taker_side = 'no')
                      OR ({col} < 0 AND taker_side = 'yes')
                    THEN 'fades_deviation'
                    ELSE 'follows_deviation'
                END AS taker_alignment,
                COUNT(*)       AS n_trades,
                SUM(contracts) AS n_contracts,
                SUM(
                    CASE
                        WHEN taker_side = 'yes' AND result = 'yes' THEN (100 - yes_price) * contracts
                        WHEN taker_side = 'yes' AND result = 'no'  THEN -yes_price * contracts
                        WHEN taker_side = 'no'  AND result = 'no'  THEN yes_price * contracts
                        WHEN taker_side = 'no'  AND result = 'yes' THEN -(100 - yes_price) * contracts
                    END
                ) * 1.0 / NULLIF(SUM(contracts), 0) AS taker_excess,
                SUM(
                    CASE WHEN taker_side = result THEN 1.0 ELSE 0.0 END * contracts
                ) * 100.0 / NULLIF(SUM(contracts), 0) AS taker_win_rate,
                SUM(
                    CASE WHEN taker_side = 'yes' THEN yes_price ELSE 100 - yes_price END * contracts
                ) * 1.0 / NULLIF(SUM(contracts), 0) AS taker_avg_implied
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
        center_comparison: pd.DataFrame,
        taker_alignment: pd.DataFrame,
        baseline: float,
    ) -> plt.Figure:
        fig, axes = plt.subplots(2, 3, figsize=(18, 10))
        fig.suptitle(
            "Mean-Reversion: Does Fading Price Deviations from Moving Averages Pay Off?",
            fontsize=14,
            fontweight="bold",
            y=0.98,
        )

        # ── Panel 1: Fade excess by lookback ─────────────────────────────────
        ax = axes[0, 0]
        x = np.arange(len(by_lookback))
        colors = ["#27ae60" if v >= 0 else "#c0392b" for v in by_lookback["fade_excess"]]
        ax.bar(x, by_lookback["fade_excess"], color=colors, alpha=0.85, edgecolor="white")
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--",
                    label=f"Baseline taker ({baseline:.2f}¢)")
        ax.set_xticks(x)
        ax.set_xticklabels([str(n) for n in by_lookback["lookback"]])
        ax.set_xlabel("Lookback Window (trades)")
        ax.set_ylabel("Fade Excess (¢ / contract)")
        ax.set_title("Fade Excess by Lookback")
        ax.legend(fontsize=7)
        for i, (_, row) in enumerate(by_lookback.iterrows()):
            t = row.get("t_stat", 0)
            if pd.notna(t):
                ax.annotate(
                    f't={t:.0f}', (i, row["fade_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["fade_excess"] >= 0 else -14),
                    ha="center", fontsize=7, color="#555",
                )

        # ── Panel 2: Fade excess by deviation magnitude ──────────────────────
        ax = axes[0, 1]
        mag = by_magnitude.dropna(subset=["deviation_magnitude"])
        x = np.arange(len(mag))
        colors = ["#27ae60" if v >= 0 else "#c0392b" for v in mag["fade_excess"]]
        ax.bar(x, mag["fade_excess"], color=colors, alpha=0.85, edgecolor="white")
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--")
        ax.set_xticks(x)
        ax.set_xticklabels(mag["deviation_magnitude"], rotation=45, ha="right")
        ax.set_xlabel("|Deviation from MA50| (¢)")
        ax.set_ylabel("Fade Excess (¢ / contract)")
        ax.set_title("Fade Excess by Deviation Size")
        for i, (_, row) in enumerate(mag.iterrows()):
            t = row.get("t_stat", 0)
            if pd.notna(t):
                ax.annotate(
                    f't={t:.0f}', (i, row["fade_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["fade_excess"] >= 0 else -14),
                    ha="center", fontsize=7, color="#555",
                )

        # ── Panel 3: Fade-up vs Fade-down ────────────────────────────────────
        ax = axes[0, 2]
        x = np.arange(len(by_direction))
        dir_colors = {"fade_up (buy NO)": "#2980b9", "fade_down (buy YES)": "#e67e22"}
        ax.bar(
            x, by_direction["fade_excess"],
            color=[dir_colors.get(d, "#999") for d in by_direction["direction"]],
            alpha=0.85, edgecolor="white",
        )
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--",
                    label=f"Baseline ({baseline:.2f}¢)")
        ax.set_xticks(x)
        ax.set_xticklabels(
            [f"{d}\n(win {row['fade_win_rate']:.1f}% vs impl {row['fade_avg_implied']:.0f}%)"
             for d, (_, row) in zip(by_direction["direction"], by_direction.iterrows())],
            fontsize=7,
        )
        ax.set_xlabel("Fade Direction (MA50)")
        ax.set_ylabel("Fade Excess (¢ / contract)")
        ax.set_title("Fade-Up (Buy NO) vs Fade-Down (Buy YES)")
        ax.legend(fontsize=7)

        # ── Panel 4: MA vs Median vs VWAP comparison ─────────────────────────
        ax = axes[1, 0]
        x = np.arange(len(center_comparison))
        colors_ctr = ["#3498db", "#9b59b6", "#1abc9c"]
        ax.bar(x, center_comparison["fade_excess"],
               color=colors_ctr[:len(center_comparison)], alpha=0.85, edgecolor="white")
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--",
                    label=f"Baseline ({baseline:.2f}¢)")
        ax.set_xticks(x)
        ax.set_xticklabels(center_comparison["center_type"], fontsize=9)
        ax.set_xlabel("Center Metric (50-trade window)")
        ax.set_ylabel("Fade Excess (¢ / contract)")
        ax.set_title("Which 'Mean' to Revert To?")
        ax.legend(fontsize=7)
        for i, (_, row) in enumerate(center_comparison.iterrows()):
            t = row.get("t_stat", 0)
            if pd.notna(t):
                ax.annotate(
                    f'{row["fade_excess"]:.3f}¢\nt={t:.0f}',
                    (i, row["fade_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["fade_excess"] >= 0 else -20),
                    ha="center", fontsize=8, fontweight="bold",
                )

        # ── Panel 5: Taker alignment ─────────────────────────────────────────
        ax = axes[1, 1]
        x = np.arange(len(taker_alignment))
        al_colors = {"fades_deviation": "#27ae60", "follows_deviation": "#c0392b"}
        ax.bar(
            x, taker_alignment["taker_excess"],
            color=[al_colors.get(a, "#999") for a in taker_alignment["taker_alignment"]],
            alpha=0.85, edgecolor="white",
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
        ax.set_title("Do Takers Who Fade Deviations Outperform?")
        for i, (_, row) in enumerate(taker_alignment.iterrows()):
            ax.annotate(
                f'{row["taker_excess"]:.2f}¢',
                (i, row["taker_excess"]),
                textcoords="offset points",
                xytext=(0, 8 if row["taker_excess"] >= 0 else -14),
                ha="center", fontsize=9, fontweight="bold",
            )

        # ── Panel 6: Summary table ───────────────────────────────────────────
        ax = axes[1, 2]
        ax.axis("off")
        lines = [
            "Summary: Mean-Reversion Signal",
            "═" * 44,
            "",
            f"  Baseline taker excess:  {baseline:.3f}¢",
            "",
            "  Lookback │ Fade Excess │  t-stat",
            "  ─────────┼─────────────┼────────",
        ]
        for _, row in by_lookback.iterrows():
            lines.append(
                f"  MA({int(row['lookback']):>3d})  │  {row['fade_excess']:+.3f}¢   │ {row['t_stat']:+.1f}"
            )
        lines += ["", "  Center Type  │ Fade Excess │  t-stat", "  ─────────────┼─────────────┼────────"]
        for _, row in center_comparison.iterrows():
            lines.append(
                f"  {row['center_type']:<12s} │  {row['fade_excess']:+.3f}¢   │ {row['t_stat']:+.1f}"
            )
        ax.text(
            0.02, 0.95, "\n".join(lines),
            transform=ax.transAxes, fontsize=8, fontfamily="monospace",
            verticalalignment="top",
        )

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        return fig

    # ── Chart config ─────────────────────────────────────────────────────────

    def _create_chart(self, by_lookback: pd.DataFrame, baseline: float) -> ChartConfig:
        chart_data = []
        for _, row in by_lookback.iterrows():
            chart_data.append({
                "lookback": int(row["lookback"]),
                "fade_excess": round(float(row["fade_excess"]), 3),
                "baseline": round(baseline, 3),
                "fade_win_rate": round(float(row["fade_win_rate"]), 2),
                "implied_prob": round(float(row["fade_avg_implied"]), 2),
            })
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="lookback",
            yKeys=["fade_excess", "baseline"],
            title="Mean-Reversion Fade Excess Return vs Baseline Taker Excess",
            xLabel="Lookback Window (trades)",
            yLabel="Excess Return (cents / contract)",
            yUnit=UnitType.CENTS,
            strokeDasharrays=[None, "5 5"],
            caption=(
                "Each bar shows the average PnL per contract from fading "
                "deviations from the rolling moving average. "
                "Positive = mean-reversion is profitable."
            ),
        )
