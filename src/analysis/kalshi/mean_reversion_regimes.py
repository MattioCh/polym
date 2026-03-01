"""Mean-reversion regimes: where does fading price deviations work best?

The first mean-reversion study (mean_reversion_price.py) established that
fading deviations from moving averages produces positive excess return
across ALL lookbacks (+0.18 to +0.66¢/contract) — the mirror image of
momentum's consistent losses.

This follow-up analysis stratifies the mean-reversion signal by:

  1. **Price level** (1–20, 21–40, 41–60, 61–80, 81–99)
  2. **Time-to-close** (0–1h, 1–6h, 6–24h, 1–3d, 3–7d, 7–30d, 30d+)
  3. **Category group** (Sports, Politics, Crypto, etc.)
  4. **Deviation magnitude** (small vs large) crossed with the above

The core signal is: deviation = P_i − MA_50(P_{i-1,...,i-50}).
Fading means: buy NO when deviation > 0, buy YES when deviation < 0.

Key questions:
  • Where is the fade excess strongest — near-close or far-from-close?
  • Which price levels have the best reversion? (Extremes or mid?)
  • Do certain categories mean-revert more than others?
  • Is the signal stronger for large deviations in specific regimes?
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

DEV_MAGNITUDE_SQL = """CASE
    WHEN ABS(dev50) < 5  THEN 'small (<5¢)'
    WHEN ABS(dev50) < 15 THEN 'medium (5-15¢)'
    ELSE 'large (15+¢)'
END"""

TIME_BUCKET_ORDER = ["0-1h", "1-6h", "6-24h", "1-3d", "3-7d", "7-30d", "30d+"]
PRICE_BUCKET_ORDER = ["01-20", "21-40", "41-60", "61-80", "81-99"]
DEV_MAG_ORDER = ["small (<5¢)", "medium (5-15¢)", "large (15+¢)"]

MIN_CONTRACTS = 100_000


# ── SQL helpers ──────────────────────────────────────────────────────────────

def _fade_excess_sql() -> str:
    """Weighted fade excess: SUM(pnl * contracts) / SUM(contracts)."""
    return """
        SUM(
            CASE
                WHEN dev50 > 0 AND result = 'no'  THEN yes_price * contracts
                WHEN dev50 > 0 AND result = 'yes' THEN -(100 - yes_price) * contracts
                WHEN dev50 < 0 AND result = 'yes' THEN (100 - yes_price) * contracts
                WHEN dev50 < 0 AND result = 'no'  THEN -yes_price * contracts
            END
        ) * 1.0 / NULLIF(SUM(
            CASE WHEN ABS(dev50) >= 0.5 THEN contracts END
        ), 0)
    """


def _fade_winrate_sql() -> str:
    return """
        SUM(
            CASE
                WHEN dev50 > 0 AND result = 'no'  THEN 1.0 * contracts
                WHEN dev50 > 0 AND result = 'yes' THEN 0.0
                WHEN dev50 < 0 AND result = 'yes' THEN 1.0 * contracts
                WHEN dev50 < 0 AND result = 'no'  THEN 0.0
            END
        ) * 100.0 / NULLIF(SUM(
            CASE WHEN ABS(dev50) >= 0.5 THEN contracts END
        ), 0)
    """


def _fade_cost_sql() -> str:
    return """
        SUM(
            CASE
                WHEN dev50 > 0 THEN (100 - yes_price) * contracts
                WHEN dev50 < 0 THEN yes_price * contracts
            END
        ) * 1.0 / NULLIF(SUM(
            CASE WHEN ABS(dev50) >= 0.5 THEN contracts END
        ), 0)
    """


class MeanReversionRegimesAnalysis(Analysis):
    """Identifies where (price, time, category) mean-reversion is strongest."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        super().__init__(
            name="mean_reversion_regimes",
            description="Where does fading price deviations from MA work best?",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")

    def run(self) -> AnalysisOutput:
        con = duckdb.connect()
        con.execute("SET preserve_insertion_order = false")

        with self.progress("Loading trades and markets"):
            self._load_data(con)

        with self.progress("Computing MA50 deviation + regime dimensions"):
            self._compute_regime_table(con)

        with self.progress("Aggregating by price level"):
            by_price = self._aggregate_by_price(con)

        with self.progress("Aggregating by time-to-close"):
            by_time = self._aggregate_by_time(con)

        with self.progress("Aggregating by category"):
            by_category = self._aggregate_by_category(con)

        with self.progress("Computing price × time heatmap"):
            price_time = self._aggregate_price_time(con)

        with self.progress("Computing deviation × time heatmap"):
            dev_time = self._aggregate_dev_time(con)

        with self.progress("Finding best regimes"):
            best_regimes = self._find_best_regimes(con)

        fig = self._create_figure(by_price, by_time, by_category, price_time, dev_time, best_regimes)
        chart = self._create_chart(by_price, by_time)

        output_data = pd.concat(
            [
                by_price.assign(view="by_price"),
                by_time.assign(view="by_time"),
                by_category.assign(view="by_category"),
                price_time.assign(view="price_x_time"),
                dev_time.assign(view="dev_x_time"),
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
                "dev_time": dev_time,
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
        """Two-step: compute MA50 on trades alone, then join with markets."""
        # Step 1: MA deviation on trades
        con.execute("""
            CREATE TEMP TABLE trades_dev AS
            SELECT
                ticker,
                yes_price,
                contracts,
                taker_side,
                created_time,
                yes_price - AVG(yes_price) OVER
                    (PARTITION BY ticker ORDER BY created_time
                     ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) AS dev50
            FROM trades
        """)
        # Drop NULL and tiny deviations early
        con.execute("DELETE FROM trades_dev WHERE dev50 IS NULL OR ABS(dev50) < 0.5")

        # Step 2: join with markets
        cat_sql = CATEGORY_SQL.replace("event_ticker", "m.event_ticker")
        con.execute(f"""
            CREATE TEMP TABLE mr_regime AS
            SELECT
                t.yes_price,
                t.contracts,
                t.taker_side,
                t.dev50,
                m.result,
                EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0
                    AS hours_to_close,
                {cat_sql} AS category
            FROM trades_dev t
            INNER JOIN markets m ON t.ticker = m.ticker
        """)
        con.execute("DROP TABLE trades_dev")

    # ── Aggregations ─────────────────────────────────────────────────────────

    def _aggregate_by_price(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(f"""
            SELECT
                {PRICE_BUCKETS_SQL} AS price_bucket,
                COUNT(*)           AS n_trades,
                SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) AS n_contracts,
                {_fade_excess_sql()}   AS fade_excess,
                {_fade_winrate_sql()}  AS fade_win_rate,
                {_fade_cost_sql()}     AS fade_avg_implied
            FROM mr_regime
            WHERE ABS(dev50) >= 0.5
            GROUP BY 1
            ORDER BY 1
        """).df()

    def _aggregate_by_time(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(f"""
            SELECT
                {TIME_BUCKETS_SQL} AS time_bucket,
                COUNT(*)           AS n_trades,
                SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) AS n_contracts,
                {_fade_excess_sql()}   AS fade_excess,
                {_fade_winrate_sql()}  AS fade_win_rate,
                {_fade_cost_sql()}     AS fade_avg_implied
            FROM mr_regime
            WHERE ABS(dev50) >= 0.5
              AND hours_to_close >= 0
            GROUP BY 1
            ORDER BY 1
        """).df()

    def _aggregate_by_category(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        df = con.execute(f"""
            SELECT
                category,
                COUNT(*)           AS n_trades,
                SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) AS n_contracts,
                {_fade_excess_sql()}   AS fade_excess,
                {_fade_winrate_sql()}  AS fade_win_rate,
                {_fade_cost_sql()}     AS fade_avg_implied
            FROM mr_regime
            WHERE ABS(dev50) >= 0.5
            GROUP BY 1
            HAVING SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS}
            ORDER BY 4 DESC
        """).df()
        df["group"] = df["category"].apply(get_group)
        return df

    def _aggregate_price_time(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(f"""
            SELECT
                {PRICE_BUCKETS_SQL} AS price_bucket,
                {TIME_BUCKETS_SQL}  AS time_bucket,
                COUNT(*)            AS n_trades,
                SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) AS n_contracts,
                {_fade_excess_sql()}   AS fade_excess,
                {_fade_winrate_sql()}  AS fade_win_rate,
                {_fade_cost_sql()}     AS fade_avg_implied
            FROM mr_regime
            WHERE ABS(dev50) >= 0.5
              AND hours_to_close >= 0
            GROUP BY 1, 2
            HAVING SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS // 10}
            ORDER BY 1, 2
        """).df()

    def _aggregate_dev_time(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Deviation magnitude × time-to-close heatmap."""
        return con.execute(f"""
            SELECT
                {DEV_MAGNITUDE_SQL} AS dev_magnitude,
                {TIME_BUCKETS_SQL}  AS time_bucket,
                COUNT(*)            AS n_trades,
                SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) AS n_contracts,
                {_fade_excess_sql()}   AS fade_excess,
                {_fade_winrate_sql()}  AS fade_win_rate,
                {_fade_cost_sql()}     AS fade_avg_implied
            FROM mr_regime
            WHERE ABS(dev50) >= 0.5
              AND hours_to_close >= 0
            GROUP BY 1, 2
            HAVING SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS // 10}
            ORDER BY 1, 2
        """).df()

    def _find_best_regimes(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """Top 15 (price, time, category-group) regimes by fade excess."""
        df = con.execute(f"""
            SELECT
                {PRICE_BUCKETS_SQL} AS price_bucket,
                {TIME_BUCKETS_SQL}  AS time_bucket,
                category,
                COUNT(*)            AS n_trades,
                SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) AS n_contracts,
                {_fade_excess_sql()}    AS fade_excess,
                {_fade_winrate_sql()}   AS fade_win_rate,
                {_fade_cost_sql()}      AS fade_avg_implied
            FROM mr_regime
            WHERE ABS(dev50) >= 0.5
              AND hours_to_close >= 0
            GROUP BY 1, 2, 3
            HAVING SUM(CASE WHEN ABS(dev50) >= 0.5 THEN contracts ELSE 0 END) >= {MIN_CONTRACTS // 5}
            ORDER BY fade_excess DESC
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
        dev_time: pd.DataFrame,
        best_regimes: pd.DataFrame,
    ) -> plt.Figure:
        fig = plt.figure(figsize=(18, 14))
        fig.suptitle(
            "Mean-Reversion Regimes: Where Does Fading Price Deviations Pay Off?",
            fontsize=14,
            fontweight="bold",
            y=0.98,
        )
        gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3)

        # ── Panel 1: By price level ──────────────────────────────────────────
        ax = fig.add_subplot(gs[0, 0])
        if not by_price.empty:
            x = np.arange(len(by_price))
            colors = ["#27ae60" if v >= 0 else "#c0392b" for v in by_price["fade_excess"]]
            ax.bar(x, by_price["fade_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axhline(y=0, color="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(by_price["price_bucket"], rotation=45, ha="right")
            ax.set_xlabel("Price Bucket (¢)")
            ax.set_ylabel("Fade Excess (¢)")
            ax.set_title("Fade Excess by Price Level")
            for i, (_, row) in enumerate(by_price.iterrows()):
                ax.annotate(
                    f'{row["fade_excess"]:.2f}', (i, row["fade_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["fade_excess"] >= 0 else -14),
                    ha="center", fontsize=8, fontweight="bold",
                )

        # ── Panel 2: By time-to-close ────────────────────────────────────────
        ax = fig.add_subplot(gs[0, 1])
        if not by_time.empty:
            ordered = by_time.set_index("time_bucket").reindex(TIME_BUCKET_ORDER).reset_index()
            ordered = ordered.dropna(subset=["fade_excess"])
            x = np.arange(len(ordered))
            colors = ["#27ae60" if v >= 0 else "#c0392b" for v in ordered["fade_excess"]]
            ax.bar(x, ordered["fade_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axhline(y=0, color="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(ordered["time_bucket"], rotation=45, ha="right")
            ax.set_xlabel("Time to Close")
            ax.set_ylabel("Fade Excess (¢)")
            ax.set_title("Fade Excess by Time-to-Close")
            for i, (_, row) in enumerate(ordered.iterrows()):
                ax.annotate(
                    f'{row["fade_excess"]:.2f}', (i, row["fade_excess"]),
                    textcoords="offset points",
                    xytext=(0, 8 if row["fade_excess"] >= 0 else -14),
                    ha="center", fontsize=7, fontweight="bold",
                )

        # ── Panel 3: By category group ───────────────────────────────────────
        ax = fig.add_subplot(gs[0, 2])
        if not by_category.empty:
            cat_grouped = (
                by_category.groupby("group")
                .apply(
                    lambda g: pd.Series({
                        "fade_excess": (g["fade_excess"] * g["n_contracts"]).sum()
                        / g["n_contracts"].sum()
                        if g["n_contracts"].sum() > 0
                        else 0,
                        "n_contracts": g["n_contracts"].sum(),
                    }),
                    include_groups=False,
                )
                .reset_index()
                .sort_values("fade_excess", ascending=False)
            )
            cat_grouped = cat_grouped[cat_grouped["n_contracts"] >= MIN_CONTRACTS]
            x = np.arange(len(cat_grouped))
            colors = [GROUP_COLORS.get(g, "#999") for g in cat_grouped["group"]]
            ax.barh(x, cat_grouped["fade_excess"], color=colors, alpha=0.85, edgecolor="white")
            ax.axvline(x=0, color="black", linewidth=0.5)
            ax.set_yticks(x)
            ax.set_yticklabels(cat_grouped["group"], fontsize=8)
            ax.set_xlabel("Fade Excess (¢)")
            ax.set_title("Fade Excess by Category")
            ax.invert_yaxis()

        # ── Panel 4: Price × Time heatmap ────────────────────────────────────
        ax = fig.add_subplot(gs[1, 0:2])
        if not price_time.empty:
            pivot = price_time.pivot_table(
                index="price_bucket", columns="time_bucket",
                values="fade_excess", aggfunc="first",
            )
            pivot = pivot.reindex(index=PRICE_BUCKET_ORDER, columns=TIME_BUCKET_ORDER)
            mask = pivot.isna()
            data = pivot.fillna(0).values
            vmax = max(abs(np.nanmin(data)), abs(np.nanmax(data)), 0.5)
            im = ax.imshow(data, cmap="RdYlGn", aspect="auto",
                           vmin=-vmax, vmax=vmax, interpolation="nearest")
            ax.set_xticks(np.arange(len(TIME_BUCKET_ORDER)))
            ax.set_xticklabels(TIME_BUCKET_ORDER, rotation=45, ha="right", fontsize=8)
            ax.set_yticks(np.arange(len(PRICE_BUCKET_ORDER)))
            ax.set_yticklabels(PRICE_BUCKET_ORDER, fontsize=8)
            ax.set_xlabel("Time to Close")
            ax.set_ylabel("Price Bucket (¢)")
            ax.set_title("Fade Excess: Price × Time (¢ / contract)")
            fig.colorbar(im, ax=ax, shrink=0.8, label="¢ / contract")
            for i in range(data.shape[0]):
                for j in range(data.shape[1]):
                    if not mask.iloc[i, j]:
                        ax.text(
                            j, i, f"{data[i, j]:.2f}",
                            ha="center", va="center", fontsize=7,
                            color="white" if abs(data[i, j]) > vmax * 0.6 else "black",
                        )

        # ── Panel 5: Deviation magnitude × Time heatmap ─────────────────────
        ax = fig.add_subplot(gs[1, 2])
        if not dev_time.empty:
            pivot = dev_time.pivot_table(
                index="dev_magnitude", columns="time_bucket",
                values="fade_excess", aggfunc="first",
            )
            pivot = pivot.reindex(index=DEV_MAG_ORDER, columns=TIME_BUCKET_ORDER)
            mask = pivot.isna()
            data = pivot.fillna(0).values
            vmax = max(abs(np.nanmin(data)), abs(np.nanmax(data)), 0.5)
            im = ax.imshow(data, cmap="RdYlGn", aspect="auto",
                           vmin=-vmax, vmax=vmax, interpolation="nearest")
            ax.set_xticks(np.arange(len(TIME_BUCKET_ORDER)))
            ax.set_xticklabels(TIME_BUCKET_ORDER, rotation=45, ha="right", fontsize=7)
            ax.set_yticks(np.arange(len(DEV_MAG_ORDER)))
            ax.set_yticklabels(DEV_MAG_ORDER, fontsize=7)
            ax.set_xlabel("Time to Close")
            ax.set_ylabel("|Deviation|")
            ax.set_title("Deviation × Time")
            fig.colorbar(im, ax=ax, shrink=0.8, label="¢")
            for i in range(data.shape[0]):
                for j in range(data.shape[1]):
                    if not mask.iloc[i, j]:
                        ax.text(
                            j, i, f"{data[i, j]:.1f}",
                            ha="center", va="center", fontsize=6,
                            color="white" if abs(data[i, j]) > vmax * 0.6 else "black",
                        )

        # ── Panel 6: Best regimes table ──────────────────────────────────────
        ax = fig.add_subplot(gs[2, :])
        ax.axis("off")
        if not best_regimes.empty:
            lines = ["Top 15 Mean-Reversion Regimes", "═" * 72, ""]
            lines.append(
                f" {'#':>2s}  {'Group':>12s} │ {'Price':>5s} │ {'Time':>5s} │ "
                f"{'Fade ¢':>7s} │ {'Win%':>5s} │ {'Vol':>6s}"
            )
            lines.append("  " + "─" * 68)
            for rank, (_, row) in enumerate(best_regimes.head(15).iterrows(), 1):
                contracts_m = row["n_contracts"] / 1e6
                lines.append(
                    f" {rank:>2d}. {row['group']:>12s} │ {row['price_bucket']:>5s} │ "
                    f"{row['time_bucket']:>5s} │ {row['fade_excess']:+7.2f} │ "
                    f"{row['fade_win_rate']:5.1f} │ {contracts_m:5.1f}M"
                )
            ax.text(
                0.02, 0.95, "\n".join(lines),
                transform=ax.transAxes, fontsize=8, fontfamily="monospace",
                verticalalignment="top",
            )

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        return fig

    # ── Chart config ─────────────────────────────────────────────────────────

    def _create_chart(self, by_price: pd.DataFrame, by_time: pd.DataFrame) -> ChartConfig:
        chart_data = []
        for _, row in by_price.iterrows():
            chart_data.append({
                "price_bucket": str(row["price_bucket"]),
                "fade_excess": round(float(row["fade_excess"]), 3),
                "fade_win_rate": round(float(row["fade_win_rate"]), 2),
                "implied_prob": round(float(row["fade_avg_implied"]), 2),
            })
        return ChartConfig(
            type=ChartType.BAR,
            data=chart_data,
            xKey="price_bucket",
            yKeys=["fade_excess"],
            title="Mean-Reversion Fade Excess by Price Level",
            xLabel="Price Bucket (cents)",
            yLabel="Excess Return (cents / contract)",
            yUnit=UnitType.CENTS,
            caption=(
                "Excess return from fading 50-trade MA deviations, "
                "split by price level. Positive = mean-reversion is profitable."
            ),
        )
