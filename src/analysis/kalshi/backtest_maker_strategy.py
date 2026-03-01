"""Backtest: rolling-window maker strategy with monthly parameter recalculation.

This backtest implements the composite maker strategy derived from the analysis
findings, with **strict temporal separation** to prevent data leakage:

1. Parameters are recalculated on a configurable schedule (default: monthly).
2. Each recalculation uses ONLY trades/markets that **both occurred and resolved**
   before the recalculation date.
3. During each period, the strategy selects trades to participate in as a maker
   based on the most recent parameters.
4. PnL is attributed to the market's close date (resolution date).

Strategy signals (all recalculated from historical data):
- Category group maker excess → skip groups with negative or near-zero edge
- Composite edge table (group × price × time × day) → trade selection filter
- Directional bias by price → lean NO ≤ threshold, YES above
- Time-to-close sweet spot → position sizing multiplier
- Day-of-week effect → weekend sizing boost

Outputs:
- Daily PnL time series
- Cumulative equity curve
- Performance metrics (Sharpe, Sortino, max drawdown, etc.)
- Parameter evolution over time
- Monthly breakdown
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from tqdm import trange

from src.analysis.kalshi.util.categories import CATEGORY_SQL, get_group
from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType
from src.common.metrics import (
    BacktestMetrics,
    compute_metrics,
    compute_rolling_metrics,
)


# ── Recalculation frequency presets ──────────────────────────────────────────
RECALC_FREQUENCIES = {
    "weekly": relativedelta(weeks=1),
    "biweekly": relativedelta(weeks=2),
    "monthly": relativedelta(months=1),
    "quarterly": relativedelta(months=3),
}

# ── Default strategy hyper-parameters ────────────────────────────────────────
DEFAULT_CONFIG = {
    # How often to recalculate strategy parameters from historical data
    "recalc_frequency": "monthly",
    # Minimum number of contracts in a composite bucket to trust its edge
    "min_bucket_contracts": 10_000,
    # Minimum maker excess (%) in a bucket to participate
    "min_maker_excess_pct": 0.0,
    # Price range filter (cents)
    "price_min": 15,
    "price_max": 85,
    # Time-to-close range filter (hours)
    "time_min_hours": 1,
    "time_max_hours": 720,  # 30 days
    # Edge-weight multipliers (adjust effective_edge for capital allocation)
    "weekend_size_mult": 1.5,
    "sweet_spot_size_mult": 1.5,
    "near_close_size_mult": 0.25,
    "long_duration_size_mult": 0.5,
    # Minimum months of historical data before first trade
    "min_warmup_months": 6,
    # ── Capital management ────────────────────────────────────────────────
    # Initial capital in cents ($10,000 = 1,000,000 cents)
    "initial_capital": 1_000_000,
    # Maximum fraction of equity in a single trade
    "max_single_trade_frac": 0.10,
    # Maximum fraction of available balance to deploy per day
    "max_daily_deploy_frac": 1,
    # Maximum fraction of total equity that can be in open positions
    "max_total_exposure_frac": 0.80,
    # Minimum allocation per trade in cents ($1 = 100 cents)
    "min_trade_allocation": 1,
    # Focus capital on top-N trades by effective edge per day
    "max_trades_per_day": 3000,
    # Rolling lookback window (months) for parameter estimation.
    "lookback_months": 12,
    # Shorter lookback window used from lookback_transition_date onward.
    "lookback_months_recent": 3,
    # Date from which to switch to the shorter lookback window.
    "lookback_transition_date": "2024-01-01",
    # Backtest start date (first recalc will be on or after this date).
    "backtest_start_date": "2023-01-01",
    # Groups to always exclude (data-driven only)
    "prior_exclude_groups": [],
}


class BacktestMakerStrategyAnalysis(Analysis):
    """Walk-forward backtest of the composite maker strategy."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
        config: dict | None = None,
    ):
        super().__init__(
            name="backtest_maker_strategy",
            description="Walk-forward backtest of composite maker strategy with monthly parameter recalculation",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")
        self.config = {**DEFAULT_CONFIG, **(config or {})}

    # ── Main entry point ─────────────────────────────────────────────────────

    def _run_impl(self) -> AnalysisOutput:
        con = duckdb.connect()

        # ── Step 1: Load all data into DuckDB temp tables ────────────────────
        with self.progress("Loading trades and markets into DuckDB"):
            self._load_data(con)

        # ── Step 2: Determine the backtest date range ────────────────────────
        with self.progress("Computing date range"):
            date_range = self._get_date_range(con)
        if date_range is None:
            return AnalysisOutput(data=pd.DataFrame())

        first_trade_date, last_close_date = date_range

        # ── Step 3: Build recalculation schedule ─────────────────────────────
        recalc_dates = self._build_recalc_schedule(first_trade_date, last_close_date)
        if len(recalc_dates) < 2:
            return AnalysisOutput(data=pd.DataFrame())

        # ── Step 4: Walk-forward backtest ────────────────────────────────────
        with self.progress(f"Running walk-forward backtest ({len(recalc_dates)-1} periods)"):
            trade_results, param_history = self._walk_forward(con, recalc_dates)

        if trade_results.empty:
            return AnalysisOutput(data=pd.DataFrame())

        # ── Step 5: Build daily PnL and compute metrics ──────────────────────
        with self.progress("Computing performance metrics"):
            daily_pnl, metrics, rolling = self._compute_results(trade_results)

        # ── Step 6: Build monthly breakdown ──────────────────────────────────
        monthly_df = self._monthly_breakdown(trade_results)

        # ── Step 7: Visualize ────────────────────────────────────────────────
        fig = self._create_figure(daily_pnl, rolling, metrics, monthly_df, param_history)
        chart = self._create_chart(rolling, metrics)

        # Combine output data
        output_data = metrics.to_dataframe()
        output_data["strategy"] = "composite_maker"
        output_data["recalc_frequency"] = self.config["recalc_frequency"]

        # ── Step 8: Build trade log with running balance ─────────────────
        trade_log = self._build_trade_log(trade_results)

        return AnalysisOutput(figure=fig, data=output_data, chart=chart, metadata={
            "daily_pnl": daily_pnl,
            "rolling": rolling,
            "monthly": monthly_df,
            "param_history": param_history,
            "trade_results": trade_results,
            "trade_log": trade_log,
        })

    # ── Data loading ─────────────────────────────────────────────────────────

    def _load_data(self, con: duckdb.DuckDBPyConnection) -> None:
        """Load parquet files into temp DuckDB tables for efficient querying."""
        con.execute(f"""
            CREATE TABLE trades AS
            SELECT
                ticker,
                yes_price,
                no_price,
                taker_side,
                count AS contracts,
                created_time
            FROM '{self.trades_dir}/*.parquet'
            WHERE yes_price BETWEEN 1 AND 99
        """)
        con.execute(f"""
            CREATE TABLE markets AS
            SELECT
                ticker,
                event_ticker,
                status,
                result,
                close_time
            FROM '{self.markets_dir}/*.parquet'
            WHERE status = 'finalized'
              AND result IN ('yes', 'no')
              AND close_time IS NOT NULL
        """)

    def _get_date_range(self, con: duckdb.DuckDBPyConnection):
        """Get the min trade date and max close date."""
        row = con.execute("""
            SELECT
                MIN(t.created_time) AS first_trade,
                MAX(m.close_time) AS last_close
            FROM trades t
            INNER JOIN markets m ON t.ticker = m.ticker
        """).fetchone()
        if row is None or row[0] is None:
            return None
        first_trade = pd.Timestamp(row[0])
        last_close = pd.Timestamp(row[1])
        # Normalize to tz-naive for consistent comparisons
        if first_trade.tzinfo is not None:
            first_trade = first_trade.tz_localize(None)
        if last_close.tzinfo is not None:
            last_close = last_close.tz_localize(None)
        return first_trade, last_close

    # ── Schedule ─────────────────────────────────────────────────────────────

    def _build_recalc_schedule(
        self, first_date: pd.Timestamp, last_date: pd.Timestamp
    ) -> list[pd.Timestamp]:
        """Build list of parameter recalculation dates.

        The first recalc_date is after the warmup period.
        """
        freq_key = self.config["recalc_frequency"]
        delta = RECALC_FREQUENCIES.get(freq_key)
        if delta is None:
            raise ValueError(f"Unknown recalc_frequency: {freq_key!r}. Use one of {list(RECALC_FREQUENCIES)}")

        # Normalize to tz-naive to avoid tz-aware vs tz-naive comparison issues
        first_date = first_date.tz_localize(None) if first_date.tzinfo else first_date
        last_date = last_date.tz_localize(None) if last_date.tzinfo else last_date

        warmup = relativedelta(months=self.config["min_warmup_months"])
        warmup_start = first_date + warmup
        # Snap to first of month for cleanliness
        warmup_start = pd.Timestamp(warmup_start.year, warmup_start.month, 1)

        # Honour explicit backtest_start_date if configured
        bt_start_str = self.config.get("backtest_start_date")
        if bt_start_str:
            explicit_start = pd.Timestamp(bt_start_str)
            start = max(warmup_start, explicit_start)
            start = pd.Timestamp(start.year, start.month, 1)
        else:
            start = warmup_start

        dates = []
        current = start
        while current <= last_date:
            dates.append(current)
            current = current + delta

        # Add a sentinel at the end to close the last period
        if dates and dates[-1] < last_date:
            dates.append(last_date + pd.Timedelta(days=1))

        return dates

    # ── Parameter estimation ─────────────────────────────────────────────────

    def _estimate_parameters(
        self, con: duckdb.DuckDBPyConnection, cutoff: pd.Timestamp
    ) -> dict:
        """Compute strategy parameters from a rolling window before cutoff.

        Uses only trades/markets in [cutoff − lookback_months, cutoff) where
        both created_time and close_time fall within that window.  This avoids
        regime-stale data and limits the influence of ancient history.

        Returns dict with:
        - group_edge: {group: maker_excess_pct}
        - composite_edge: {(group, price_bucket, time_bucket, day_type): excess_pct}
        - no_side_threshold: price below which we lean NO
        """
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        # Use shorter lookback from the transition date onward
        transition = pd.Timestamp(self.config.get("lookback_transition_date", "2099-01-01"))
        if cutoff >= transition:
            lb_months = self.config["lookback_months_recent"]
        else:
            lb_months = self.config["lookback_months"]
        lookback_start = cutoff - relativedelta(months=lb_months)
        lookback_str = lookback_start.strftime("%Y-%m-%d %H:%M:%S")

        # ── Composite edge table (group × price × time × day) ───────────────
        df = con.execute(f"""
            WITH trade_data AS (
                SELECT
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                    t.yes_price,
                    t.no_price,
                    t.taker_side,
                    t.contracts,
                    m.result,
                    EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 AS hours_to_close,
                    CASE
                        WHEN dayofweek(t.created_time) IN (0, 6) THEN 'Weekend'
                        ELSE 'Weekday'
                    END AS day_type,
                    CASE
                        WHEN t.yes_price BETWEEN 1 AND 20 THEN '01-20'
                        WHEN t.yes_price BETWEEN 21 AND 40 THEN '21-40'
                        WHEN t.yes_price BETWEEN 41 AND 60 THEN '41-60'
                        WHEN t.yes_price BETWEEN 61 AND 80 THEN '61-80'
                        ELSE '81-99'
                    END AS price_bucket,
                    CASE
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 <= 6 THEN '0-6h'
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 <= 72 THEN '6h-3d'
                        ELSE '3d+'
                    END AS time_bucket,
                    CASE
                        WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN -(100 - t.yes_price) * t.contracts
                        WHEN t.taker_side = 'yes' AND m.result = 'no' THEN t.yes_price * t.contracts
                        WHEN t.taker_side = 'no' AND m.result = 'no' THEN -(100 - t.no_price) * t.contracts
                        WHEN t.taker_side = 'no' AND m.result = 'yes' THEN t.no_price * t.contracts
                    END AS maker_pnl,
                    CASE
                        WHEN t.taker_side = 'yes' THEN t.no_price * t.contracts
                        ELSE t.yes_price * t.contracts
                    END AS maker_cost
                FROM trades t
                INNER JOIN markets m ON t.ticker = m.ticker
                WHERE t.created_time >= TIMESTAMP '{lookback_str}'
                  AND t.created_time < TIMESTAMP '{cutoff_str}'
                  AND m.close_time < TIMESTAMP '{cutoff_str}'
                  AND m.close_time > t.created_time
            )
            SELECT
                category,
                price_bucket,
                time_bucket,
                day_type,
                SUM(maker_pnl) AS maker_pnl,
                SUM(maker_cost) AS maker_cost,
                SUM(contracts) AS total_contracts
            FROM trade_data
            GROUP BY category, price_bucket, time_bucket, day_type
        """).df()

        if df.empty:
            return {"group_edge": {}, "composite_edge": {}, "no_side_threshold": 55}

        # Map categories to groups
        unique_cats = df["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        df["group"] = df["category"].map(cat_to_group)

        # Group-level edge
        group_agg = (
            df.groupby("group")
            .agg({"maker_pnl": "sum", "maker_cost": "sum", "total_contracts": "sum"})
            .reset_index()
        )
        group_agg["maker_excess_pct"] = (
            group_agg["maker_pnl"] * 100.0 / group_agg["maker_cost"].replace(0, np.nan)
        )
        group_edge = dict(zip(group_agg["group"], group_agg["maker_excess_pct"]))

        # Composite edge
        combo = (
            df.groupby(["group", "price_bucket", "time_bucket", "day_type"])
            .agg({"maker_pnl": "sum", "maker_cost": "sum", "total_contracts": "sum"})
            .reset_index()
        )
        combo["maker_excess_pct"] = (
            combo["maker_pnl"] * 100.0 / combo["maker_cost"].replace(0, np.nan)
        )
        min_contracts = self.config["min_bucket_contracts"]
        combo = combo[combo["total_contracts"] >= min_contracts]

        composite_edge = {}
        for _, row in combo.iterrows():
            key = (row["group"], row["price_bucket"], row["time_bucket"], row["day_type"])
            composite_edge[key] = row["maker_excess_pct"]

        # ── Directional bias: find NO-side threshold ─────────────────────────
        # Compute maker YES vs NO excess by price range from historical data
        dir_df = con.execute(f"""
            WITH trade_data AS (
                SELECT
                    t.yes_price,
                    t.taker_side,
                    t.contracts,
                    m.result,
                    CASE
                        WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN -(100 - t.yes_price) * t.contracts
                        WHEN t.taker_side = 'yes' AND m.result = 'no' THEN t.yes_price * t.contracts
                        WHEN t.taker_side = 'no' AND m.result = 'no' THEN -(100 - t.no_price) * t.contracts
                        WHEN t.taker_side = 'no' AND m.result = 'yes' THEN t.no_price * t.contracts
                    END AS maker_pnl,
                    CASE
                        WHEN t.taker_side = 'yes' THEN t.no_price * t.contracts
                        ELSE t.yes_price * t.contracts
                    END AS maker_cost
                FROM trades t
                INNER JOIN markets m ON t.ticker = m.ticker
                WHERE t.created_time >= TIMESTAMP '{lookback_str}'
                  AND t.created_time < TIMESTAMP '{cutoff_str}'
                  AND m.close_time < TIMESTAMP '{cutoff_str}'
                  AND m.close_time > t.created_time
            )
            SELECT
                CASE
                    WHEN yes_price BETWEEN 1 AND 20 THEN 10
                    WHEN yes_price BETWEEN 21 AND 40 THEN 30
                    WHEN yes_price BETWEEN 41 AND 60 THEN 50
                    WHEN yes_price BETWEEN 61 AND 80 THEN 70
                    ELSE 90
                END AS price_mid,
                taker_side,
                SUM(maker_pnl) AS maker_pnl,
                SUM(maker_cost) AS maker_cost
            FROM trade_data
            GROUP BY price_mid, taker_side
        """).df()

        # Determine threshold where NO stops outperforming YES for maker
        no_side_threshold = 55  # default
        if not dir_df.empty:
            # Pivot: for each price_mid, compare maker edge when taker_side='yes'
            # (maker is on NO side) vs taker_side='no' (maker is on YES side)
            pivoted = dir_df.pivot_table(
                index="price_mid",
                columns="taker_side",
                values=["maker_pnl", "maker_cost"],
                aggfunc="sum",
            ).fillna(0)
            if ("maker_pnl", "yes") in pivoted.columns and ("maker_pnl", "no") in pivoted.columns:
                # Maker NO excess: when taker buys YES, maker is on NO side
                maker_no_excess = (
                    pivoted[("maker_pnl", "yes")] * 100.0
                    / pivoted[("maker_cost", "yes")].replace(0, np.nan)
                )
                # Maker YES excess: when taker buys NO, maker is on YES side
                maker_yes_excess = (
                    pivoted[("maker_pnl", "no")] * 100.0
                    / pivoted[("maker_cost", "no")].replace(0, np.nan)
                )
                # Find highest price_mid where NO > YES
                for pm in sorted(pivoted.index):
                    no_e = maker_no_excess.get(pm, 0)
                    yes_e = maker_yes_excess.get(pm, 0)
                    if pd.notna(no_e) and pd.notna(yes_e) and no_e > yes_e:
                        no_side_threshold = pm + 10  # midpoint + half bucket

        return {
            "group_edge": group_edge,
            "composite_edge": composite_edge,
            "no_side_threshold": no_side_threshold,
        }

    # ── Walk-forward engine ──────────────────────────────────────────────────

    def _walk_forward(
        self, con: duckdb.DuckDBPyConnection, recalc_dates: list[pd.Timestamp]
    ) -> tuple[pd.DataFrame, list[dict]]:
        """Day-by-day walk-forward backtest with capital management.

        Phase 1: Estimate parameters and filter trades for each recalc period.
        Phase 2: Step day-by-day through the calendar:
          - Resolve positions whose markets closed → free capital, realize PnL.
          - Allocate available capital to today's trades weighted by effective_edge.
          - Record each executed trade with balance snapshot.
        """
        cfg = self.config
        all_filtered: list[pd.DataFrame] = []
        param_history: list[dict] = []

        # ── Phase 1: Parameter estimation + filtering ────────────────────────
        for i in trange(len(recalc_dates) - 1, desc="Estimating parameters"):
            period_start = recalc_dates[i]
            period_end = recalc_dates[i + 1]

            params = self._estimate_parameters(con, period_start)

            param_history.append({
                "period_start": period_start,
                "period_end": period_end,
                "n_groups_positive": sum(1 for v in params["group_edge"].values() if v > 0),
                "n_composite_buckets": len(params["composite_edge"]),
                "no_side_threshold": params["no_side_threshold"],
                "avg_group_edge": np.mean(list(params["group_edge"].values())) if params["group_edge"] else 0,
            })

            if not params["composite_edge"]:
                continue

            period_trades = self._get_period_trades(con, period_start, period_end)
            if period_trades.empty:
                continue

            filtered = self._filter_trades(period_trades, params)
            if not filtered.empty:
                all_filtered.append(filtered)

        if not all_filtered:
            return pd.DataFrame(), param_history

        # Merge all filtered trades and sort chronologically
        all_trades = pd.concat(all_filtered, ignore_index=True)
        all_trades = all_trades.sort_values("trade_time").reset_index(drop=True)

        # Group trades by calendar date
        trade_groups: dict = dict(list(all_trades.groupby("trade_date")))

        # Determine full date range (first trade → last position resolution)
        first_day = all_trades["trade_date"].min()
        last_close = pd.to_datetime(all_trades["close_time"]).max()
        if hasattr(last_close, "tzinfo") and last_close.tzinfo is not None:
            last_close = last_close.tz_localize(None)
        last_day = max(all_trades["trade_date"].max(), last_close.date())

        # ── Phase 2: Day-by-day capital-aware simulation ─────────────────────
        available_balance = float(cfg["initial_capital"])
        locked_capital = 0.0
        # Scheduled future cash flows: date → sum of cost/pnl to return
        sched_cost_return: dict = defaultdict(float)
        sched_pnl: dict = defaultdict(float)
        all_records: list[dict] = []

        date_range = pd.date_range(first_day, last_day, freq="D")

        for day_ts in date_range:
            day = day_ts.date()

            # ── Resolve positions closing today ───────────────────────────
            if day in sched_cost_return:
                cost_back = sched_cost_return.pop(day)
                pnl_back = sched_pnl.pop(day, 0.0)
                available_balance += cost_back + pnl_back
                locked_capital -= cost_back

            # ── Skip if no trades today ───────────────────────────────────
            if day not in trade_groups:
                continue

            day_trades = trade_groups[day]

            # ── Compute deployable capital ────────────────────────────────
            total_equity = available_balance + locked_capital
            max_exposure = total_equity * cfg["max_total_exposure_frac"]
            room_for_new = max(0.0, max_exposure - locked_capital)
            daily_limit = available_balance * cfg["max_daily_deploy_frac"]
            deployable = min(room_for_new, daily_limit, available_balance)

            if deployable < cfg["min_trade_allocation"]:
                continue

            # ── Select top trades by effective edge ───────────────────────
            day_trades = day_trades.sort_values("effective_edge", ascending=False)
            max_per_day = cfg.get("max_trades_per_day", 50)
            day_trades = day_trades.head(max_per_day)

            # ── Edge-weighted allocation ──────────────────────────────────
            edges = np.maximum(day_trades["effective_edge"].values.copy(), 0.001)
            total_edge = edges.sum()
            weights = edges / total_edge
            allocations = weights * deployable

            # Cap per-trade allocation
            max_per_trade = total_equity * cfg["max_single_trade_frac"]
            allocations = np.minimum(allocations, max_per_trade)

            # Convert to contracts
            cpc = day_trades["cost_per_contract"].values
            ppc = day_trades["pnl_per_contract"].values
            orig_contracts = day_trades["contracts"].values

            max_from_alloc = allocations / np.maximum(cpc, 0.01)
            actual_contracts = np.minimum(max_from_alloc, orig_contracts)

            # Filter dust positions
            viable = actual_contracts >= 0.5
            actual_contracts = actual_contracts * viable
            actual_costs = actual_contracts * cpc

            # If total exceeds available balance, scale proportionally
            total_needed = actual_costs.sum()
            if total_needed > available_balance and total_needed > 0:
                scale = available_balance * 0.999 / total_needed
                actual_contracts = actual_contracts * scale
                actual_costs = actual_contracts * cpc
                viable = actual_contracts >= 0.5
                actual_contracts = actual_contracts * viable
                actual_costs = actual_contracts * cpc

            # ── Execute trades ────────────────────────────────────────────
            for idx_offset, (_, trade) in enumerate(day_trades.iterrows()):
                ac = actual_contracts[idx_offset]
                if ac < 0.5:
                    continue

                act_cost = actual_costs[idx_offset]
                act_pnl = ac * ppc[idx_offset]

                # Guard against overdraft
                if act_cost > available_balance:
                    ac = available_balance / max(cpc[idx_offset], 0.01)
                    if ac < 0.5:
                        continue
                    act_cost = ac * cpc[idx_offset]
                    act_pnl = ac * ppc[idx_offset]

                balance_before = available_balance
                available_balance -= act_cost
                locked_capital += act_cost

                # Schedule resolution at close_time date
                close_dt = pd.Timestamp(trade["close_time"])
                if hasattr(close_dt, "tzinfo") and close_dt.tzinfo is not None:
                    close_dt = close_dt.tz_localize(None)
                close_date = close_dt.date()

                sched_cost_return[close_date] += act_cost
                sched_pnl[close_date] += act_pnl

                all_records.append({
                    "trade_time": trade["trade_time"],
                    "close_time": trade["close_time"],
                    "category": trade["category"],
                    "group": trade["group"],
                    "taker_side": trade["taker_side"],
                    "result": trade["result"],
                    "maker_won": trade["maker_won"],
                    "yes_price": trade["yes_price"],
                    "no_price": trade["no_price"],
                    "contracts": trade["contracts"],
                    "sized_contracts": ac,
                    "participation_rate": ac / max(trade["contracts"], 1),
                    "maker_pnl": trade["maker_pnl"],
                    "adj_maker_pnl": act_pnl,
                    "maker_cost": trade["maker_cost"],
                    "adj_maker_cost": act_cost,
                    "balance_before": balance_before,
                    "balance_after": available_balance,
                    "open_exposure": locked_capital,
                    "effective_edge": trade["effective_edge"],
                    "composite_edge": trade["composite_edge"],
                    "hours_to_close": trade["hours_to_close"],
                    "price_bucket": trade["price_bucket"],
                    "time_bucket": trade["time_bucket"],
                    "day_type": trade["day_type"],
                    "pnl_date": trade["pnl_date"],
                })

        # Resolve any remaining scheduled flows
        for d in sorted(sched_cost_return.keys()):
            available_balance += sched_cost_return[d] + sched_pnl.get(d, 0.0)
            locked_capital -= sched_cost_return[d]

        if not all_records:
            return pd.DataFrame(), param_history

        return pd.DataFrame(all_records), param_history

    def _get_period_trades(
        self, con: duckdb.DuckDBPyConnection,
        period_start: pd.Timestamp,
        period_end: pd.Timestamp,
    ) -> pd.DataFrame:
        """Get all trades in the period that can be evaluated (market resolved)."""
        ps = period_start.strftime("%Y-%m-%d %H:%M:%S")
        pe = period_end.strftime("%Y-%m-%d %H:%M:%S")

        return con.execute(f"""
            SELECT
                {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")} AS category,
                t.yes_price,
                t.no_price,
                t.taker_side,
                t.contracts,
                t.created_time AS trade_time,
                m.result,
                m.close_time,
                EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 AS hours_to_close,
                CASE
                    WHEN dayofweek(t.created_time) IN (0, 6) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS day_type,
                CASE
                    WHEN t.yes_price BETWEEN 1 AND 20 THEN '01-20'
                    WHEN t.yes_price BETWEEN 21 AND 40 THEN '21-40'
                    WHEN t.yes_price BETWEEN 41 AND 60 THEN '41-60'
                    WHEN t.yes_price BETWEEN 61 AND 80 THEN '61-80'
                    ELSE '81-99'
                END AS price_bucket,
                CASE
                    WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 <= 6 THEN '0-6h'
                    WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0 <= 72 THEN '6h-3d'
                    ELSE '3d+'
                END AS time_bucket,
                -- Maker PnL (cents)
                CASE
                    WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN -(100 - t.yes_price) * t.contracts
                    WHEN t.taker_side = 'yes' AND m.result = 'no' THEN t.yes_price * t.contracts
                    WHEN t.taker_side = 'no' AND m.result = 'no' THEN -(100 - t.no_price) * t.contracts
                    WHEN t.taker_side = 'no' AND m.result = 'yes' THEN t.no_price * t.contracts
                END AS maker_pnl,
                -- Maker cost basis (cents)
                CASE
                    WHEN t.taker_side = 'yes' THEN t.no_price * t.contracts
                    ELSE t.yes_price * t.contracts
                END AS maker_cost,
                -- Did maker win?
                CASE
                    WHEN (t.taker_side = 'yes' AND m.result = 'no') THEN 1
                    WHEN (t.taker_side = 'no' AND m.result = 'yes') THEN 1
                    ELSE 0
                END AS maker_won
            FROM trades t
            INNER JOIN markets m ON t.ticker = m.ticker
            WHERE t.created_time >= TIMESTAMP '{ps}'
              AND t.created_time < TIMESTAMP '{pe}'
              AND m.close_time > t.created_time
        """).df()

    def _filter_trades(self, trades: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Apply strategy filters and compute effective_edge for capital allocation.

        Does NOT size positions — sizing is done in _walk_forward with capital
        awareness.  Returns filtered trades with:
        - composite_edge: raw bucket edge from parameter estimation
        - effective_edge: composite_edge × timing/day adjustments (used as weight)
        - cost_per_contract, pnl_per_contract: for proportional sizing
        """
        cfg = self.config

        # Map categories to groups
        unique_cats = trades["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        trades = trades.copy()
        trades["group"] = trades["category"].map(cat_to_group)

        # ── Filter 1: Group must have positive edge ──────────────────────────
        group_edge = params["group_edge"]
        trades["group_edge"] = trades["group"].map(group_edge).fillna(0)
        trades = trades[trades["group_edge"] > 0]
        if trades.empty:
            return trades

        # ── Filter 2: Price range ────────────────────────────────────────────
        trades = trades[
            (trades["yes_price"] >= cfg["price_min"])
            & (trades["yes_price"] <= cfg["price_max"])
        ]
        if trades.empty:
            return trades

        # ── Filter 3: Time-to-close range ────────────────────────────────────
        trades = trades[
            (trades["hours_to_close"] >= cfg["time_min_hours"])
            & (trades["hours_to_close"] <= cfg["time_max_hours"])
        ]
        if trades.empty:
            return trades

        # ── Filter 4: Composite bucket must have positive edge ───────────────
        composite_edge = params["composite_edge"]
        min_excess = cfg["min_maker_excess_pct"]

        def lookup_edge(row):
            key = (row["group"], row["price_bucket"], row["time_bucket"], row["day_type"])
            return composite_edge.get(key, np.nan)

        trades["composite_edge"] = trades.apply(lookup_edge, axis=1)
        trades = trades[trades["composite_edge"].notna() & (trades["composite_edge"] > min_excess)]
        if trades.empty:
            return trades

        # ── Per-contract values for proportional sizing ──────────────────────
        safe_contracts = trades["contracts"].replace(0, 1)
        trades["pnl_per_contract"] = trades["maker_pnl"] / safe_contracts
        trades["cost_per_contract"] = trades["maker_cost"] / safe_contracts

        # ── Effective edge = composite_edge × timing/day adjustments ─────────
        edge_mult = np.ones(len(trades))

        is_weekend = trades["day_type"].values == "Weekend"
        edge_mult = np.where(is_weekend, edge_mult * cfg["weekend_size_mult"], edge_mult)

        htc = trades["hours_to_close"].values
        edge_mult = np.where(htc < 1, edge_mult * cfg["near_close_size_mult"], edge_mult)
        edge_mult = np.where(
            (htc >= 6) & (htc <= 168), edge_mult * cfg["sweet_spot_size_mult"], edge_mult
        )
        edge_mult = np.where(htc > 720, edge_mult * cfg["long_duration_size_mult"], edge_mult)

        trades["effective_edge"] = trades["composite_edge"].values * edge_mult

        # ── Calendar columns for walk-forward ────────────────────────────────
        trades["trade_date"] = pd.to_datetime(trades["trade_time"]).dt.date
        trades["pnl_date"] = pd.to_datetime(trades["close_time"]).dt.date

        return trades

    # ── Trade log with running balance ───────────────────────────────────────

    def _build_trade_log(self, trade_results: pd.DataFrame) -> pd.DataFrame:
        """Build a detailed trade-level log sorted by trade time.

        Each row includes the balance snapshot at entry time, sized contracts,
        participation rate, and all trade details including open exposure.
        """
        log = trade_results.copy()
        log = log.sort_values("trade_time").reset_index(drop=True)

        # Dollar columns
        log["adj_maker_pnl_dollars"] = log["adj_maker_pnl"] / 100.0
        log["adj_maker_cost_dollars"] = log["adj_maker_cost"] / 100.0
        log["balance_before_dollars"] = log["balance_before"] / 100.0
        log["balance_after_dollars"] = log["balance_after"] / 100.0
        log["open_exposure_dollars"] = log["open_exposure"] / 100.0

        # Select and order columns
        cols = [
            "trade_time", "close_time", "category", "group",
            "taker_side", "result", "maker_won",
            "yes_price", "no_price",
            "contracts", "sized_contracts", "participation_rate",
            "maker_pnl", "adj_maker_pnl", "adj_maker_pnl_dollars",
            "maker_cost", "adj_maker_cost", "adj_maker_cost_dollars",
            "balance_before", "balance_before_dollars",
            "balance_after", "balance_after_dollars",
            "open_exposure", "open_exposure_dollars",
            "effective_edge", "composite_edge",
            "hours_to_close", "price_bucket", "time_bucket", "day_type",
            "pnl_date",
        ]
        cols = [c for c in cols if c in log.columns]
        return log[cols]

    # ── Override save to emit trade log CSV ──────────────────────────────────

    def save(
        self,
        output_dir: Path | str,
        formats: list[str] | None = None,
        dpi: int = 300,
    ) -> dict[str, Path]:
        """Save standard outputs plus a detailed trade log CSV."""
        saved = super().save(output_dir, formats=formats, dpi=dpi)

        # The trade log is built during run() and stored in metadata
        output_dir = Path(output_dir)
        if hasattr(self, "_last_output") and self._last_output and self._last_output.metadata:
            trade_log = self._last_output.metadata.get("trade_log")
            if trade_log is not None and not trade_log.empty:
                path = output_dir / f"{self.name}_trades.csv"
                trade_log.to_csv(path, index=False)
                saved["trades_csv"] = path

        return saved

    def run(self) -> AnalysisOutput:
        """Execute the analysis, cache result for save(), and return."""
        output = self._run_impl()
        self._last_output = output
        return output

    # ── Results computation ──────────────────────────────────────────────────

    def _compute_results(
        self, trade_results: pd.DataFrame
    ) -> tuple[pd.Series, BacktestMetrics, pd.DataFrame]:
        """Compute daily PnL, overall metrics, and rolling metrics."""

        # Daily PnL (attributed to market resolution date)
        daily_pnl = (
            trade_results.groupby("pnl_date")["adj_maker_pnl"]
            .sum()
        )
        daily_pnl.index = pd.to_datetime(daily_pnl.index)
        daily_pnl = daily_pnl.sort_index()

        # Fill non-trading days with 0 PnL
        full_range = pd.date_range(daily_pnl.index.min(), daily_pnl.index.max(), freq="D")
        daily_pnl = daily_pnl.reindex(full_range, fill_value=0)

        # Trade-level stats
        total_trades = len(trade_results)
        total_capital = trade_results["adj_maker_cost"].sum()
        trade_wins = int(trade_results["maker_won"].sum())

        # Compute metrics
        metrics = compute_metrics(
            daily_pnl,
            initial_capital=self.config["initial_capital"],
            total_trades=total_trades,
            total_capital_deployed=total_capital,
            trade_wins=trade_wins,
        )

        # Rolling metrics
        rolling = compute_rolling_metrics(
            daily_pnl,
            window=90,
            initial_capital=self.config["initial_capital"],
        )

        return daily_pnl, metrics, rolling

    def _monthly_breakdown(self, trade_results: pd.DataFrame) -> pd.DataFrame:
        """Compute monthly PnL breakdown."""
        tr = trade_results.copy()
        tr["month"] = pd.to_datetime(tr["pnl_date"]).dt.to_period("M")

        monthly = (
            tr.groupby("month")
            .agg(
                pnl=("adj_maker_pnl", "sum"),
                cost=("adj_maker_cost", "sum"),
                trades=("adj_maker_pnl", "count"),
                wins=("maker_won", "sum"),
            )
            .reset_index()
        )
        monthly["excess_pct"] = monthly["pnl"] * 100.0 / monthly["cost"].replace(0, np.nan)
        monthly["win_rate"] = monthly["wins"] * 100.0 / monthly["trades"].replace(0, np.nan)
        monthly["pnl_dollars"] = monthly["pnl"] / 100
        monthly["month"] = monthly["month"].astype(str)

        return monthly

    # ── Visualization ────────────────────────────────────────────────────────

    def _create_figure(
        self,
        daily_pnl: pd.Series,
        rolling: pd.DataFrame,
        metrics: BacktestMetrics,
        monthly_df: pd.DataFrame,
        param_history: list[dict],
    ) -> plt.Figure:
        """Create comprehensive backtest visualization."""
        fig = plt.figure(figsize=(20, 20))
        gs = fig.add_gridspec(4, 2, hspace=0.35, wspace=0.3)

        # ── Panel 1: Equity curve ────────────────────────────────────────────
        ax1 = fig.add_subplot(gs[0, :])
        equity = self.config["initial_capital"] + daily_pnl.cumsum()
        ax1.plot(equity.index, equity.values / 100, color="#2ecc71", linewidth=1)
        ax1.fill_between(
            equity.index,
            self.config["initial_capital"] / 100,
            equity.values / 100,
            alpha=0.15,
            color="#2ecc71",
        )
        ax1.set_title("Equity Curve (Composite Maker Strategy)", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Equity ($)")
        ax1.axhline(self.config["initial_capital"] / 100, color="gray", linewidth=0.5, linestyle="--")
        ax1.grid(alpha=0.3)

        # Add metrics text box
        textstr = (
            f'Sharpe: {metrics.sharpe_ratio:.2f}  |  '
            f'Sortino: {metrics.sortino_ratio:.2f}  |  '
            f'Max DD: {metrics.max_drawdown_pct:.1f}%  |  '
            f'Win Rate: {metrics.win_rate_daily:.1f}%  |  '
            f'Total Return: {metrics.total_return_pct:.1f}%'
        )
        ax1.text(
            0.5, 1.02, textstr, transform=ax1.transAxes,
            fontsize=10, ha="center", va="bottom",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )

        # ── Panel 2: Drawdown ────────────────────────────────────────────────
        ax2 = fig.add_subplot(gs[1, 0])
        if not rolling.empty and "drawdown_pct" in rolling.columns:
            ax2.fill_between(
                rolling["date"], rolling["drawdown_pct"], 0,
                color="#e74c3c", alpha=0.4,
            )
            ax2.plot(rolling["date"], rolling["drawdown_pct"], color="#e74c3c", linewidth=0.5)
        ax2.set_title("Drawdown (%)")
        ax2.set_ylabel("Drawdown %")
        ax2.grid(alpha=0.3)

        # ── Panel 3: Rolling Sharpe ──────────────────────────────────────────
        ax3 = fig.add_subplot(gs[1, 1])
        if not rolling.empty and "rolling_sharpe" in rolling.columns:
            rs = rolling.dropna(subset=["rolling_sharpe"])
            if not rs.empty:
                ax3.plot(rs["date"], rs["rolling_sharpe"], color="#3498db", linewidth=0.8)
                ax3.axhline(0, color="black", linewidth=0.5)
                ax3.axhline(1, color="green", linewidth=0.5, linestyle="--", alpha=0.5)
                ax3.axhline(-1, color="red", linewidth=0.5, linestyle="--", alpha=0.5)
        ax3.set_title("Rolling 90-Day Sharpe Ratio")
        ax3.set_ylabel("Sharpe")
        ax3.grid(alpha=0.3)

        # ── Panel 4: Monthly PnL bars ────────────────────────────────────────
        ax4 = fig.add_subplot(gs[2, :])
        if not monthly_df.empty:
            colors = ["#2ecc71" if x >= 0 else "#e74c3c" for x in monthly_df["pnl_dollars"]]
            x_pos = np.arange(len(monthly_df))
            ax4.bar(x_pos, monthly_df["pnl_dollars"], color=colors, alpha=0.8)
            # Show every Nth label to avoid crowding
            n_labels = min(24, len(monthly_df))
            step = max(1, len(monthly_df) // n_labels)
            ax4.set_xticks(x_pos[::step])
            ax4.set_xticklabels(monthly_df["month"].iloc[::step], rotation=45, ha="right", fontsize=7)
        ax4.set_title("Monthly PnL ($)")
        ax4.set_ylabel("PnL ($)")
        ax4.axhline(0, color="black", linewidth=0.5)
        ax4.grid(axis="y", alpha=0.3)

        # ── Panel 5: Metrics table ───────────────────────────────────────────
        ax5 = fig.add_subplot(gs[3, 0])
        ax5.axis("off")
        ax5.set_title("Performance Metrics", fontsize=12, fontweight="bold")

        metric_rows = [
            ["Metric", "Value"],
            ["Total Return", f"{metrics.total_return_pct:.2f}%"],
            ["Annualized Return", f"{metrics.annualized_return_pct:.2f}%"],
            ["Sharpe Ratio", f"{metrics.sharpe_ratio:.3f}"],
            ["Sortino Ratio", f"{metrics.sortino_ratio:.3f}"],
            ["Calmar Ratio", f"{metrics.calmar_ratio:.3f}"],
            ["Max Drawdown", f"{metrics.max_drawdown_pct:.2f}%"],
            ["Max DD Duration", f"{metrics.max_drawdown_duration_days} days"],
            ["Win Rate (Daily)", f"{metrics.win_rate_daily:.1f}%"],
            ["Profit Factor", f"{metrics.profit_factor:.3f}"],
            ["Total PnL", f"${metrics.total_pnl_dollars:,.2f}"],
            ["Total Trades", f"{metrics.total_trades:,}"],
            ["Trade Win Rate", f"{metrics.trade_win_rate:.1f}%"],
            ["Ann. Volatility", f"{metrics.annualized_volatility:.2f}%"],
            ["Skewness", f"{metrics.skewness:.3f}"],
            ["Kurtosis", f"{metrics.kurtosis:.3f}"],
        ]
        table = ax5.table(
            cellText=metric_rows[1:],
            colLabels=metric_rows[0],
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.3)

        # ── Panel 6: Parameter evolution ─────────────────────────────────────
        ax6 = fig.add_subplot(gs[3, 1])
        if param_history:
            ph = pd.DataFrame(param_history)
            ax6.plot(ph["period_start"], ph["avg_group_edge"], "o-", color="#9b59b6", markersize=3)
            ax6.set_title("Average Group Edge Over Time (%)")
            ax6.set_ylabel("Avg Maker Excess %")
            ax6.grid(alpha=0.3)
            ax6.tick_params(axis="x", rotation=45)
        else:
            ax6.text(0.5, 0.5, "No parameter history", ha="center", va="center")

        plt.suptitle(
            f"Composite Maker Strategy Backtest — {self.config['recalc_frequency'].title()} Recalculation",
            fontsize=16, fontweight="bold", y=1.01,
        )

        return fig

    def _create_chart(self, rolling: pd.DataFrame, metrics: BacktestMetrics) -> ChartConfig:
        """Create chart config for web rendering."""
        chart_data = []
        if not rolling.empty:
            # Downsample for chart if too many points
            step = max(1, len(rolling) // 500)
            for _, row in rolling.iloc[::step].iterrows():
                chart_data.append({
                    "date": str(row["date"].date()) if hasattr(row["date"], "date") else str(row["date"]),
                    "equity": round(float(row["equity"]) / 100, 2),
                    "drawdown_pct": round(float(row["drawdown_pct"]), 4),
                    "cumulative_pnl_dollars": round(float(row["cumulative_pnl"]) / 100, 2),
                })

        return ChartConfig(
            type=ChartType.LINE,
            data=chart_data,
            xKey="date",
            yKeys=["equity"],
            title=f"Composite Maker Strategy — Sharpe {metrics.sharpe_ratio:.2f}",
            yUnit=UnitType.DOLLARS,
        )
