"""Momentum regimes: where does momentum work vs fail?

This analysis identifies the specific CONDITIONS under which price momentum
is profitable for takers.  It tests momentum excess return across three
conditioning dimensions:

  1. **Price level** (1–20, 21–40, 41–60, 61–80, 81–99)
  2. **Time-to-close** (0–1h, 1–6h, 6–24h, 1–3d, 3–7d, 7–30d, 30d+)
  3. **Category group** (Sports, Politics, Crypto, Finance, etc.)

The core signal is the 10-trade price momentum:  Δ₁₀ = P_i − P_{i−10}.
Following momentum means buying YES when Δ₁₀ > 0, NO when Δ₁₀ < 0.

Key hypotheses:
  • Near-close momentum is more informative (events unfolding in real time).
  • Mid-price momentum is stronger (extreme prices leave little room).
  • Category matters: sports in-play momentum ≠ politics momentum.

The output is a set of heatmaps and tables showing the optimal momentum
"regime" — the combination of conditions where taker momentum trading has
the highest (and potentially positive) excess return.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.analysis.kalshi.util.categories import CATEGORY_SQL, GROUP_COLORS, get_group
from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType

# ── Bucket definitions ───────────────────────────────────────────────────────
PRICE_BUCKETS_SQL = """CASE
    WHEN yes_price BETWEEN 1  AND 20 THEN '01-20'
    WHEN yes_price BETWEEN 21 AND 40 THEN '21-40'
    WHEN yes_price BETWEEN 41 AND 60 THEN '41-60'
    WHEN yes_price BETWEEN 61 AND 80 THEN '61-80'
    WHEN yes_price BETWEEN 81 AND 99 THEN '81-99'
END"""

TIME_BUCKETS_SQL = """CASE
    WHEN hours_to_close <= 1   THEN '0-1h'
    WHEN hours_to_close <= 6   THEN '1-6h'
    WHEN hours_to_close <= 24  THEN '6-24h'
    WHEN hours_to_close <= 72  THEN '1-3d'
    WHEN hours_to_close <= 168 THEN '3-7d'
    WHEN hours_to_close <= 720 THEN '7-30d'
    ELSE '30d+'
END"""

TIME_BUCKET_ORDER = ["0-1h", "1-6h", "6-24h", "1-3d", "3-7d", "7-30d", "30d+"]
PRICE_BUCKET_ORDER = ["01-20", "21-40", "41-60", "61-80", "81-99"]

MIN_CONTRACTS = 100_000  # Minimum contracts per cell for reliability


class MomentumRegimesAnalysis(Analysis):
    """Identifies where (price, time, category) momentum is profitable."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="momentum_regimes",
            description="Where does price momentum work vs fail for takers?",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        con = duckdb.connect()

        with self.progress("Loading trades and markets"):
            self._load_data(con)

        with self.progress("Computing momentum + regime signals"):
            self._compute_regime_table(con)

        with self.progress("Aggregating by price level"):
            by_price = self._aggregate_by_price(con)

        with self.progress("Aggregating by time-to-close"):
            by_time = self._aggregate_by_time(con)

        with self.progress("Aggregating by category"):
            by_category = self._aggregate_by_category(con)

        with self.progress("Computing price × time heatmap"):
            price_time = self._aggregate_price_time(con)

        with self.progress("Finding best regimes"):
            best_regimes = self._find_best_regimes(con)

        fig = self._create_figure(by_price, by_time, by_category, price_time, best_regimes)
        chart = self._create_chart(by_price, by_time)

        output_data = pd.concat(
            [
                by_price.assign(view="by_price"),
                by_time.assign(view="by_time"),
                by_category.assign(view="by_category"),
                price_time.assign(view="price_x_time"),
                best_regimes.assign(view="best_regimes"),
            ],
            ignore_index=True,
        )

        return AnalysisOutput(
            figure=fig,
            data=output_data,
            chart=chart,
            metadata={
                "by_price": by_price,
                "by_time": by_time,
                "by_category": by_category,
                "price_time": price_time,
                "best_regimes": best_regimes,
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
            SELECT ticker, event_ticker, result, close_time
            FROM '{self.markets_dir}/*.parquet'
            WHERE status = 'finalized'
              AND result IN ('yes', 'no')
              AND close_time IS NOT NULL
        """)

    def _compute_regime_table(self, con: duckdb.DuckDBPyConnection) -> None:
        # Tune DuckDB for large workload
        con.execute("SET preserve_insertion_order = false")

        # Step 1: compute momentum on trades alone (lightweight window)
        con.execute("""
            CREATE TEMP TABLE trades_mom AS
            SELECT
                ticker,
                yes_price,
                contracts,
                taker_side,
                created_time,
                yes_price - LAG(yes_price, 10)
                    OVER (PARTITION BY ticker ORDER BY created_time) AS mom10
            FROM trades
        """)
        # Drop rows with NULL momentum early to reduce join size
        con.execute("DELETE FROM trades_mom WHERE mom10 IS NULL OR mom10 = 0")

        # Step 2: join with markets and add regime dimensions
        cat_sql = CATEGORY_SQL.replace("event_ticker", "m.event_ticker")
        con.execute(f"""
            CREATE TEMP TABLE regime_all AS
            SELECT
                t.yes_price,
                t.contracts,
                t.taker_side,
                m.result,
                t.mom10,
                EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0
                    AS hours_to_close,
                {cat_sql} AS category
            FROM trades_mom t
            INNER JOIN markets m ON t.ticker = m.ticker
        """)
        # Free intermediate table
        con.execute("DROP TABLE trades_mom")

    # ── Aggregation helpers ──────────────────────────────────────────────────

    def _follow_excess_sql(self) -> str:
        return """
            SUM(
                CASE
                    WHEN mom10 > 0 AND result = 'yes' THEN (100 - yes_price) * contracts
                    WHEN mom10 > 0 AND result = 'no'  THEN -yes_price * contracts
                    WHEN mom10 < 0 AND result = 'no'  THEN yes_price * contracts
                    WHEN mom10 < 0 AND result = 'yes' THEN -(100 - yes_price) * contracts
                END
            ) * 1.0 / NULLIF(SUM(
                CASE WHEN mom10 != 0 THEN contracts END
            ), 0)
        """

    def _follow_winrate_sql(self) -> str:
        return """
            SUM(
                CASE
                    WHEN mom10 > 0 AND result = 'yes' THEN 1.0 * contracts
                    WHEN mom10 > 0 AND result = 'no'  THEN 0.0
                    WHEN mom10 < 0 AND result = 'no'  THEN 1.0 * contracts
                    WHEN mom10 < 0 AND result = 'yes' THEN 0.0
                END
            ) * 100.0 / NULLIF(SUM(
                CASE WHEN mom10 != 0 THEN contracts END
            ), 0)
        """

    def _follow_implied_sql(self) -> str:
        return """
            SUM(
                CASE
                    WHEN mom10 > 0 THEN yes_price * contracts
                    WHEN mom10 < 0 THEN (100 - yes_price) * contracts
                END
            ) * 1.0 / NULLIF(SUM(
                CASE WHEN mom10 != 0 THEN contracts END
            ), 0)
        """

    # ── Aggregations ─────────────────────────────────────────────────────────

    def _aggregate_by_price(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(f"""
            SELECT
                {PRICE_BUCKETS_SQL} AS price_bucket,
                COUNT(*)           AS n_trades,
                SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) AS n_contracts,
                {self._follow_excess_sql()}   AS follow_excess,
                {self._follow_winrate_sql()}  AS follow_win_rate,
                {self._follow_implied_sql()}  AS follow_avg_implied
            FROM regime_all
            WHERE mom10 IS NOT NULL AND mom10 != 0
            GROUP BY 1
            ORDER BY 1
        """).df()

    def _aggregate_by_time(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(f"""
            SELECT
                {TIME_BUCKETS_SQL} AS time_bucket,
                COUNT(*)           AS n_trades,
                SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) AS n_contracts,
                {self._follow_excess_sql()}   AS follow_excess,
                {self._follow_winrate_sql()}  AS follow_win_rate,
                {self._follow_implied_sql()}  AS follow_avg_implied
            FROM regime_all
            WHERE mom10 IS NOT NULL AND mom10 != 0
              AND hours_to_close >= 0
            GROUP BY 1
            ORDER BY 1
        """).df()

    def _aggregate_by_category(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        df = con.execute(f"""
            SELECT
                category,
                COUNT(*)           AS n_trades,
                SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) AS n_contracts,
                {self._follow_excess_sql()}   AS follow_excess,
                {self._follow_winrate_sql()}  AS follow_win_rate,
                {self._follow_implied_sql()}  AS follow_avg_implied
            FROM regime_all
            WHERE mom10 IS NOT NULL AND mom10 != 0
            GROUP BY 1
            HAVING SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS}
            ORDER BY 4 DESC
        """).df()
        # Map raw category to group
        df["group"] = df["category"].apply(get_group)
        return df

    def _aggregate_price_time(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(f"""
            SELECT
                {PRICE_BUCKETS_SQL} AS price_bucket,
                {TIME_BUCKETS_SQL}  AS time_bucket,
                COUNT(*)            AS n_trades,
                SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) AS n_contracts,
                {self._follow_excess_sql()}   AS follow_excess,
                {self._follow_winrate_sql()}  AS follow_win_rate,
                {self._follow_implied_sql()}  AS follow_avg_implied
            FROM regime_all
            WHERE mom10 IS NOT NULL AND mom10 != 0
              AND hours_to_close >= 0
            GROUP BY 1, 2
            HAVING SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS // 10}
            ORDER BY 1, 2
        """).df()

    def _find_best_regimes(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Top 15 (price, time, category-group) regimes by follow excess."""
        df = con.execute(f"""
            SELECT
                {PRICE_BUCKETS_SQL} AS price_bucket,
                {TIME_BUCKETS_SQL}  AS time_bucket,
                category,
                COUNT(*)            AS n_trades,
                SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) AS n_contracts,
                {self._follow_excess_sql()}   AS follow_excess,
                {self._follow_winrate_sql()}  AS follow_win_rate,
                {self._follow_implied_sql()}  AS follow_avg_implied
            FROM regime_all
            WHERE mom10 IS NOT NULL AND mom10 != 0
              AND hours_to_close >= 0
            GROUP BY 1, 2, 3
            HAVING SUM(CASE WHEN mom10 != 0 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS // 5}
            ORDER BY follow_excess DESC
            LIMIT 15
        """).df()
        df["group"] = df["category"].apply(get_group)
        return df

    # ── Visualization ────────────────────────────────────────────────────────

    def _create_figure(
        self,
        by_price: pd.DataFrame,
        by_time: pd.DataFrame,
        by_category: pd.DataFrame,
        price_time: pd.DataFrame,
        best_regimes: pd.DataFrame,
    ) -> plt.Figure:
        fig = plt.figure(figsize=(16, 12))
        fig.suptitle(
            "Momentum Regimes: Where Does Following Price Drift Pay Off?",
            fontsize=14,
            fontweight="bold",
            y=0.98,
        )
        gs = fig.add_gridspec(2, 3, hspace=0.35, wspace=0.3)

        # ── Panel 1: By price level ──────────────────────────────────────────
        ax = fig.add_subplot(gs[0, 0])
        if not by_price.empty:
            x = np.arange(len(by_price))
            colors = ["#27ae60" if v >= 0 else "#c0392b" for v in by_price["follow_excess"]]
            ax.bar(x, by_price["follow_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axhline(y=0, color="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(by_price["price_bucket"], rotation=45, ha="right")
            ax.set_xlabel("Price Bucket (¢)")
            ax.set_ylabel("Follow Excess (¢)")
            ax.set_title("Momentum by Price Level")

        # ── Panel 2: By time-to-close ────────────────────────────────────────
        ax = fig.add_subplot(gs[0, 1])
        if not by_time.empty:
            # Reorder
            ordered = by_time.set_index("time_bucket").reindex(TIME_BUCKET_ORDER).reset_index()
            ordered = ordered.dropna(subset=["follow_excess"])
            x = np.arange(len(ordered))
            colors = ["#27ae60" if v >= 0 else "#c0392b" for v in ordered["follow_excess"]]
            ax.bar(x, ordered["follow_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axhline(y=0, color="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(ordered["time_bucket"], rotation=45, ha="right")
            ax.set_xlabel("Time to Close")
            ax.set_ylabel("Follow Excess (¢)")
            ax.set_title("Momentum by Time-to-Close")

        # ── Panel 3: By category group ───────────────────────────────────────
        ax = fig.add_subplot(gs[0, 2])
        if not by_category.empty:
            cat_grouped = (
                by_category.groupby("group")
                .apply(
                    lambda g: pd.Series({
                        "follow_excess": (g["follow_excess"] * g["n_contracts"]).sum()
                        / g["n_contracts"].sum()
                        if g["n_contracts"].sum() > 0
                        else 0,
                        "n_contracts": g["n_contracts"].sum(),
                    }),
                    include_groups=False,
                )
                .reset_index()
                .sort_values("follow_excess", ascending=False)
            )
            cat_grouped = cat_grouped[cat_grouped["n_contracts"] >= MIN_CONTRACTS]
            x = np.arange(len(cat_grouped))
            colors = [
                GROUP_COLORS.get(g, "#999") for g in cat_grouped["group"]
            ]
            ax.barh(x, cat_grouped["follow_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axvline(x=0, color="black", linewidth=0.5)
            ax.set_yticks(x)
            ax.set_yticklabels(cat_grouped["group"], fontsize=8)
            ax.set_xlabel("Follow Excess (¢)")
            ax.set_title("Momentum by Category")
            ax.invert_yaxis()

        # ── Panel 4: Price × Time heatmap ────────────────────────────────────
        ax = fig.add_subplot(gs[1, 0:2])
        if not price_time.empty:
            pivot = price_time.pivot_table(
                index="price_bucket",
                columns="time_bucket",
                values="follow_excess",
                aggfunc="first",
            )
            # Reorder
            pivot = pivot.reindex(index=PRICE_BUCKET_ORDER, columns=TIME_BUCKET_ORDER)
            mask = pivot.isna()
            data = pivot.fillna(0).values

            vmax = max(abs(np.nanmin(data)), abs(np.nanmax(data)), 0.5)
            im = ax.imshow(
                data,
                cmap="RdYlGn",
                aspect="auto",
                vmin=-vmax,
                vmax=vmax,
                interpolation="nearest",
            )
            ax.set_xticks(np.arange(len(TIME_BUCKET_ORDER)))
            ax.set_xticklabels(TIME_BUCKET_ORDER, rotation=45, ha="right", fontsize=8)
            ax.set_yticks(np.arange(len(PRICE_BUCKET_ORDER)))
            ax.set_yticklabels(PRICE_BUCKET_ORDER, fontsize=8)
            ax.set_xlabel("Time to Close")
            ax.set_ylabel("Price Bucket (¢)")
            ax.set_title("Momentum Follow Excess: Price × Time (¢ / contract)")
            fig.colorbar(im, ax=ax, shrink=0.8, label="¢ / contract")
            # Annotate cells
            for i in range(data.shape[0]):
                for j in range(data.shape[1]):
                    if not mask.iloc[i, j]:
                        ax.text(
                            j, i, f"{data[i, j]:.2f}",
                            ha="center", va="center", fontsize=7,
                            color="white" if abs(data[i, j]) > vmax * 0.6 else "black",
                        )

        # ── Panel 5: Best regimes table ──────────────────────────────────────
        ax = fig.add_subplot(gs[1, 2])
        ax.axis("off")
        if not best_regimes.empty:
            lines = ["Top Momentum Regimes", "═" * 48, ""]
            for rank, (_, row) in enumerate(best_regimes.head(10).iterrows(), 1):
                contracts_m = row["n_contracts"] / 1e6
                lines.append(
                    f" {rank:>2d}. {row['group']:>12s} │ {row['price_bucket']} │ "
                    f"{row['time_bucket']:>5s} │ {row['follow_excess']:+.2f}¢ "
                    f"│ {contracts_m:.1f}M"
                )
            lines += ["", "─" * 48, "  Group │ Price │ Time │ Excess │ Vol"]
            ax.text(
                0.02, 0.95, "\n".join(lines),
                transform=ax.transAxes, fontsize=7, fontfamily="monospace",
                verticalalignment="top",
            )

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        return fig

    # ── Chart config ─────────────────────────────────────────────────────────

    def _create_chart(
        self, by_price: pd.DataFrame, by_time: pd.DataFrame
    ) -> ChartConfig:
        # Use price-level data as primary chart
        chart_data = []
        for _, row in by_price.iterrows():
            chart_data.append(
                {
                    "price_bucket": str(row["price_bucket"]),
                    "follow_excess": round(float(row["follow_excess"]), 3),
                    "follow_win_rate": round(float(row["follow_win_rate"]), 2),
                    "implied_prob": round(float(row["follow_avg_implied"]), 2),
                }
            )
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="price_bucket",
            yKeys=["follow_excess"],
            title="Momentum Follow Excess by Price Level",
            xLabel="Price Bucket (cents)",
            yLabel="Excess Return (cents / contract)",
            yUnit=UnitType.CENTS,
            caption=(
                "Excess return from following 10-trade price momentum, "
                "split by price level.  Positive = momentum is profitable."
            ),
        )
