"""Momentum analysis: does one-sided taker flow predict resolution?

Surgical definition of flow momentum
─────────────────────────────────────
For trade *i* in market *m*, the **N-trade flow score** is the volume-weighted
fraction of taker YES buys over the preceding N trades:

    flow_N(i) = Σ_{j=i-N}^{i-1} [contracts_j × I(taker_side_j = 'yes')]
                ─────────────────────────────────────────────────────────
                Σ_{j=i-N}^{i-1} contracts_j

  • flow > 0.5 → takers are net buying YES  (bullish flow)
  • flow < 0.5 → takers are net buying NO   (bearish flow)

This is DISTINCT from price momentum:
  • Price momentum measures where the price IS going.
  • Flow momentum measures what TAKERS are buying.

A maker can absorb heavy YES flow without moving the price.  In that case,
flow momentum is high but price momentum is zero — the takers may be wrong,
or the maker may be mis-pricing.

Flow-following strategy (taker)
───────────────────────────────
  • flow > 0.5 → buy YES at P_i
  • flow < 0.5 → buy NO  at (100 − P_i)
  • Hold to resolution.

This analysis tests whether "following the crowd" of other takers
predicts resolution better than the current price implies.
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
FLOW_LOOKBACKS = [5, 10, 25, 50]


class MomentumTradeFlowAnalysis(Analysis):
    """Tests whether directional taker flow predicts market resolution."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="momentum_trade_flow",
            description="Does one-sided taker flow predict market resolution?",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        con = duckdb.connect()

        with self.progress("Loading trades and markets"):
            self._load_data(con)

        with self.progress("Computing flow signals (rolling windows)"):
            self._compute_flow_table(con)

        with self.progress("Computing baseline taker excess"):
            baseline = self._compute_baseline(con)

        with self.progress("Aggregating by lookback window"):
            by_lookback = self._aggregate_by_lookback(con)

        with self.progress("Aggregating by flow intensity"):
            by_intensity = self._aggregate_by_flow_intensity(con)

        with self.progress("Flow vs price momentum interaction"):
            flow_vs_price = self._flow_vs_price(con)

        fig = self._create_figure(by_lookback, by_intensity, flow_vs_price, baseline)
        chart = self._create_chart(by_lookback, baseline)

        output_data = pd.concat(
            [
                by_lookback.assign(view="by_lookback"),
                by_intensity.assign(view="by_intensity"),
                flow_vs_price.assign(view="flow_vs_price"),
            ],
            ignore_index=True,
        )

        return AnalysisOutput(
            figure=fig,
            data=output_data,
            chart=chart,
            metadata={
                "baseline": baseline,
                "by_lookback": by_lookback,
                "by_intensity": by_intensity,
                "flow_vs_price": flow_vs_price,
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

    def _compute_flow_table(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create temp table with flow scores and price momentum for comparison."""
        flow_cols = []
        for n in FLOW_LOOKBACKS:
            flow_cols.append(f"""
                SUM(CASE WHEN t.taker_side = 'yes' THEN t.contracts ELSE 0 END)
                    OVER (PARTITION BY t.ticker ORDER BY t.created_time
                          ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING)
                    * 1.0
                / NULLIF(
                    SUM(t.contracts)
                    OVER (PARTITION BY t.ticker ORDER BY t.created_time
                          ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING),
                    0)
                AS flow_{n}""")
            # Window size for filtering incomplete windows
            flow_cols.append(f"""
                COUNT(*)
                    OVER (PARTITION BY t.ticker ORDER BY t.created_time
                          ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING)
                AS wsize_{n}""")

        # Also compute 10-trade price momentum for interaction analysis
        flow_cols.append("""
                t.yes_price - LAG(t.yes_price, 10)
                    OVER (PARTITION BY t.ticker ORDER BY t.created_time)
                AS price_mom_10""")

        all_cols = ",".join(flow_cols)
        con.execute(f"""
            CREATE TEMP TABLE flow_all AS
            SELECT
                t.yes_price,
                t.contracts,
                t.taker_side,
                m.result,
                {all_cols}
            FROM trades t
            INNER JOIN markets m ON t.ticker = m.ticker
        """)

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
            FROM flow_all
        """).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    # ── Aggregations ─────────────────────────────────────────────────────────

    def _aggregate_by_lookback(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        parts: list[pd.DataFrame] = []
        for n in FLOW_LOOKBACKS:
            flow_col = f"flow_{n}"
            wsize_col = f"wsize_{n}"
            df = con.execute(f"""
                WITH filtered AS (
                    SELECT *,
                        CASE
                            WHEN {flow_col} > 0.5 AND result = 'yes' THEN 100 - yes_price
                            WHEN {flow_col} > 0.5 AND result = 'no'  THEN -yes_price
                            WHEN {flow_col} < 0.5 AND result = 'no'  THEN yes_price
                            WHEN {flow_col} < 0.5 AND result = 'yes' THEN -(100 - yes_price)
                        END AS follow_pnl,
                        CASE
                            WHEN {flow_col} > 0.5 THEN yes_price
                            WHEN {flow_col} < 0.5 THEN 100 - yes_price
                        END AS follow_cost
                    FROM flow_all
                    WHERE {flow_col} IS NOT NULL
                      AND {flow_col} != 0.5
                      AND {wsize_col} >= {n}
                )
                SELECT
                    {n}                              AS lookback,
                    COUNT(*)                         AS n_trades,
                    SUM(contracts)                   AS n_contracts,
                    SUM(follow_pnl * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0)  AS follow_excess,
                    AVG(follow_pnl)                  AS follow_excess_unweighted,
                    AVG(follow_pnl)
                        / NULLIF(STDDEV_SAMP(follow_pnl) / SQRT(COUNT(*)), 0)
                                                     AS t_stat,
                    SUM(CASE
                            WHEN ({flow_col} > 0.5 AND result = 'yes')
                              OR ({flow_col} < 0.5 AND result = 'no')
                            THEN 1.0 ELSE 0.0
                        END * contracts) * 100.0
                        / NULLIF(SUM(contracts), 0)  AS follow_win_rate,
                    SUM(follow_cost * contracts) * 1.0
                        / NULLIF(SUM(contracts), 0)  AS follow_avg_implied
                FROM filtered
            """).df()
            parts.append(df)
        return pd.concat(parts, ignore_index=True)

    def _aggregate_by_flow_intensity(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Excess return by flow intensity bucket (using 10-trade lookback)."""
        flow_col = "flow_10"
        wsize_col = "wsize_10"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *,
                    CASE
                        WHEN {flow_col} > 0.5 AND result = 'yes' THEN 100 - yes_price
                        WHEN {flow_col} > 0.5 AND result = 'no'  THEN -yes_price
                        WHEN {flow_col} < 0.5 AND result = 'no'  THEN yes_price
                        WHEN {flow_col} < 0.5 AND result = 'yes' THEN -(100 - yes_price)
                    END AS follow_pnl
                FROM flow_all
                WHERE {flow_col} IS NOT NULL
                  AND {flow_col} != 0.5
                  AND {wsize_col} >= 10
            )
            SELECT
                10 AS lookback,
                CASE
                    WHEN {flow_col} >= 0.9 THEN '90-100% YES'
                    WHEN {flow_col} >= 0.7 THEN '70-90% YES'
                    WHEN {flow_col} > 0.5  THEN '50-70% YES'
                    WHEN {flow_col} > 0.3  THEN '30-50% YES'
                    WHEN {flow_col} > 0.1  THEN '10-30% YES'
                    ELSE                        '0-10% YES'
                END AS flow_bucket,
                CASE WHEN {flow_col} > 0.5 THEN 'bullish' ELSE 'bearish' END AS flow_direction,
                COUNT(*)               AS n_trades,
                SUM(contracts)         AS n_contracts,
                SUM(follow_pnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                       AS follow_excess,
                AVG(follow_pnl)
                    / NULLIF(STDDEV_SAMP(follow_pnl) / SQRT(COUNT(*)), 0)
                                       AS t_stat,
                AVG({flow_col}) * 100  AS avg_flow_pct,
                AVG(yes_price)         AS avg_yes_price
            FROM filtered
            GROUP BY 2, 3
            ORDER BY AVG({flow_col})
        """).df()

    def _flow_vs_price(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Interaction: does flow add information beyond price momentum?"""
        flow_col = "flow_10"
        wsize_col = "wsize_10"
        return con.execute(f"""
            WITH filtered AS (
                SELECT *,
                    -- Flow-following PnL
                    CASE
                        WHEN {flow_col} > 0.5 AND result = 'yes' THEN 100 - yes_price
                        WHEN {flow_col} > 0.5 AND result = 'no'  THEN -yes_price
                        WHEN {flow_col} < 0.5 AND result = 'no'  THEN yes_price
                        WHEN {flow_col} < 0.5 AND result = 'yes' THEN -(100 - yes_price)
                    END AS flow_follow_pnl,
                    -- Price-following PnL
                    CASE
                        WHEN price_mom_10 > 0 AND result = 'yes' THEN 100 - yes_price
                        WHEN price_mom_10 > 0 AND result = 'no'  THEN -yes_price
                        WHEN price_mom_10 < 0 AND result = 'no'  THEN yes_price
                        WHEN price_mom_10 < 0 AND result = 'yes' THEN -(100 - yes_price)
                    END AS price_follow_pnl,
                    -- Agreement: both signals point same direction
                    CASE
                        WHEN ({flow_col} > 0.5 AND price_mom_10 > 0)
                          OR ({flow_col} < 0.5 AND price_mom_10 < 0)
                        THEN 'agree'
                        ELSE 'disagree'
                    END AS signal_agreement
                FROM flow_all
                WHERE {flow_col} IS NOT NULL
                  AND {flow_col} != 0.5
                  AND {wsize_col} >= 10
                  AND price_mom_10 IS NOT NULL
                  AND price_mom_10 != 0
            )
            SELECT
                signal_agreement,
                COUNT(*)               AS n_trades,
                SUM(contracts)         AS n_contracts,
                SUM(flow_follow_pnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                       AS flow_follow_excess,
                SUM(price_follow_pnl * contracts) * 1.0
                    / NULLIF(SUM(contracts), 0)
                                       AS price_follow_excess,
                -- Combined signal: when both agree, follow; when disagree, no trade
                -- Here we just measure the excess when both agree (flow direction)
                SUM(CASE WHEN signal_agreement = 'agree' THEN flow_follow_pnl * contracts ELSE 0 END) * 1.0
                    / NULLIF(SUM(CASE WHEN signal_agreement = 'agree' THEN contracts ELSE 0 END), 0)
                                       AS agree_follow_excess
            FROM filtered
            GROUP BY signal_agreement
            ORDER BY signal_agreement
        """).df()

    # ── Visualization ────────────────────────────────────────────────────────

    def _create_figure(
        self,
        by_lookback: pd.DataFrame,
        by_intensity: pd.DataFrame,
        flow_vs_price: pd.DataFrame,
        baseline: float,
    ) -> plt.Figure:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(
            "Taker Flow Momentum: Does Following the Crowd Pay Off?",
            fontsize=14,
            fontweight="bold",
            y=0.98,
        )

        # ── Panel 1: Follow-flow excess by lookback ──────────────────────────
        ax = axes[0, 0]
        x = np.arange(len(by_lookback))
        ax.bar(x, by_lookback["follow_excess"], color="#9b59b6", alpha=0.85, edgecolor="white")
        ax.axhline(y=0, color="black", linewidth=0.5)
        ax.axhline(y=baseline, color="#e74c3c", linewidth=1.2, linestyle="--",
                    label=f"Baseline taker ({baseline:.2f}¢)")
        ax.set_xticks(x)
        ax.set_xticklabels([str(n) for n in by_lookback["lookback"]])
        ax.set_xlabel("Lookback Window (trades)")
        ax.set_ylabel("Excess Return (¢ / contract)")
        ax.set_title("Follow-Flow Excess by Lookback")
        ax.legend(fontsize=8)
        for i, (_, row) in enumerate(by_lookback.iterrows()):
            t = row.get("t_stat", 0)
            if pd.notna(t):
                ax.annotate(
                    f't={t:.1f}', (i, row["follow_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["follow_excess"] >= 0 else -14),
                    ha="center", fontsize=7, color="#555",
                )

        # ── Panel 2: Excess by flow intensity ────────────────────────────────
        ax = axes[0, 1]
        if not by_intensity.empty:
            x = np.arange(len(by_intensity))
            colors = ["#27ae60" if v >= 0 else "#c0392b" for v in by_intensity["follow_excess"]]
            ax.bar(x, by_intensity["follow_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axhline(y=0, color="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(by_intensity["flow_bucket"], rotation=45, ha="right", fontsize=7)
            ax.set_xlabel("Flow Bucket (% YES volume in last 10 trades)")
            ax.set_ylabel("Excess Return (¢ / contract)")
            ax.set_title("Excess by Flow Intensity")

        # ── Panel 3: Flow vs price momentum ──────────────────────────────────
        ax = axes[1, 0]
        if not flow_vs_price.empty:
            x = np.arange(len(flow_vs_price))
            width = 0.35
            ax.bar(x - width / 2, flow_vs_price["flow_follow_excess"], width,
                   label="Flow signal", color="#9b59b6", alpha=0.8)
            ax.bar(x + width / 2, flow_vs_price["price_follow_excess"], width,
                   label="Price signal", color="#3498db", alpha=0.8)
            ax.axhline(y=0, color="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(flow_vs_price["signal_agreement"])
            ax.set_xlabel("Flow & Price Momentum Agreement")
            ax.set_ylabel("Excess Return (¢ / contract)")
            ax.set_title("Flow vs Price Momentum: Agreement Matters?")
            ax.legend(fontsize=8)
            for i, (_, row) in enumerate(flow_vs_price.iterrows()):
                pct = row["n_contracts"] / flow_vs_price["n_contracts"].sum() * 100
                ax.annotate(
                    f'{pct:.0f}% vol',
                    (i, max(row["flow_follow_excess"], row["price_follow_excess"])),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=7, color="#555",
                )

        # ── Panel 4: Summary text ────────────────────────────────────────────
        ax = axes[1, 1]
        ax.axis("off")
        best_row = by_lookback.loc[by_lookback["follow_excess"].abs().idxmax()]
        summary_lines = [
            "Flow Momentum Summary (10-trade default lookback)",
            "─" * 48,
            "",
        ]
        for _, row in by_lookback.iterrows():
            star = " ***" if abs(row.get("t_stat", 0)) > 3 else (" **" if abs(row.get("t_stat", 0)) > 2 else "")
            summary_lines.append(
                f"  Lookback {int(row['lookback']):>2d}: "
                f"follow excess = {row['follow_excess']:+.3f}¢  "
                f"(t = {row.get('t_stat', 0):.1f}){star}"
            )
        summary_lines += [
            "",
            f"  Baseline taker excess: {baseline:.3f}¢",
            "",
            "Flow vs Price agreement (10-trade):",
        ]
        if not flow_vs_price.empty:
            for _, row in flow_vs_price.iterrows():
                summary_lines.append(
                    f"  {row['signal_agreement']:>9s}: flow {row['flow_follow_excess']:+.3f}¢  "
                    f"price {row['price_follow_excess']:+.3f}¢"
                )
        ax.text(
            0.05, 0.95, "\n".join(summary_lines),
            transform=ax.transAxes, fontsize=8, fontfamily="monospace",
            verticalalignment="top",
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
                    "follow_flow_excess": round(float(row["follow_excess"]), 3),
                    "baseline": round(baseline, 3),
                }
            )
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="lookback",
            yKeys=["follow_flow_excess", "baseline"],
            title="Flow-Following Excess Return vs Baseline",
            xLabel="Lookback Window (trades)",
            yLabel="Excess Return (cents / contract)",
            yUnit=UnitType.CENTS,
            strokeDasharrays=[None, "5 5"],
            caption=(
                "Each bar shows the average PnL per contract from buying in the "
                "direction of concentrated taker flow.  Dashed line = naive taker average."
            ),
        )
