"""Backtest: mean-reversion fade strategy with walk-forward parameter estimation.

The mean-reversion analysis (Sections 14 of CONCLUSIONS.md) established that
fading price deviations from moving averages produces **consistently positive**
excess return — the mirror image of momentum's consistent losses:

  • MA(50) fade excess: +0.41¢/contract (t=98, 72M+ trades)
  • Large deviations (20+¢ from MA): +2.82¢/contract (t=157)
  • Fade-up (buy NO when price>MA): +0.63¢ vs fade-down: +0.20¢
  • Best regime: large dev + 30d+ from close = +8.07¢/contract

This backtest implements a **selective contrarian fade strategy** as a taker:

1. **Signal**: For each trade, compute deviation from the trailing MA(k) of the
   last k trades in the same market.  If dev > 0, the price is above its recent
   average — fade by buying NO.  If dev < 0 — fade by buying YES.
2. **Minimum deviation filter**: Only trade when |deviation| ≥ threshold (default
   5¢).  Below this, the fade excess is negligible (0.07–0.18¢).
3. **Regime filtering**: Only trade in (group × price × time × day) regimes
   with positive historical fade excess, re-estimated each period.
4. **Sizing multipliers**: Larger deviations, fade-up direction, and far-from-
   close timing receive higher allocation weights.

Walk-forward temporal separation (identical to maker/taker backtests):
- Parameters recalculated on a configurable schedule (default: monthly).
- Each recalculation uses ONLY trades/markets that both occurred AND resolved
  before the recalculation date.
- MA deviations are trade-level (trailing k trades) — no forward information.
- PnL is attributed to the market's close date (resolution date).

Outputs:
- Daily PnL time series and cumulative equity curve
- Performance metrics (Sharpe, Sortino, max drawdown, etc.)
- Parameter evolution over time
- Monthly breakdown
- Detailed trade log with deviation, direction, and sizing metadata
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
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


# ── Portfolio state: explicit position tracking ──────────────────────────────

@dataclass
class Position:
    """A single open position in the portfolio."""

    ticker: str
    entry_date: _dt.date
    close_date: _dt.date          # when the market resolves
    cost: float                   # capital locked (cents)
    pnl: float                    # realized PnL when resolved (cents)
    direction: str                # 'fade_up' or 'fade_down'
    contracts: float
    dev: float                    # deviation that triggered the signal
    group: str
    category: str


@dataclass
class PortfolioState:
    """Day-by-day portfolio state with explicit position tracking.

    Tracks:
    - ``cash``: free capital available for new trades.
    - ``positions``: list of open (unsettled) Position objects.

    On each daily step, positions whose ``close_date <= today`` are settled:
    their cost is unlocked and PnL returns to cash.
    """

    cash: float
    positions: list[Position] = field(default_factory=list)

    # ── cumulative accounting (for diagnostics) ──────────────────────────
    total_settled_positions: int = 0
    total_realized_pnl: float = 0.0
    total_cost_returned: float = 0.0

    # ── daily snapshot log ───────────────────────────────────────────────
    daily_log: list[dict] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def exposure(self) -> float:
        """Total capital locked in open positions (cents)."""
        return sum(p.cost for p in self.positions)

    @property
    def equity(self) -> float:
        """Total equity = cash + locked exposure."""
        return self.cash + self.exposure

    @property
    def n_positions(self) -> int:
        return len(self.positions)

    @property
    def utilization(self) -> float:
        """Fraction of equity that is locked in open positions."""
        eq = self.equity
        return self.exposure / eq if eq > 0 else 0.0

    # ------------------------------------------------------------------
    # Daily step: settle resolved positions, return PnL to cash
    # ------------------------------------------------------------------

    def step(self, today: _dt.date, *, log: bool = True) -> list[Position]:
        """Advance the portfolio to ``today``.

        1. Find all positions whose ``close_date <= today``.
        2. For each, return *cost + pnl* to ``cash`` (unlock capital).
        3. Remove them from the open-positions list.

        Returns the list of settled positions (useful for diagnostics).
        """
        still_open: list[Position] = []
        settled: list[Position] = []

        for pos in self.positions:
            if pos.close_date <= today:
                # Settlement: cost comes back, plus PnL (can be negative)
                self.cash += pos.cost + pos.pnl
                self.total_settled_positions += 1
                self.total_realized_pnl += pos.pnl
                self.total_cost_returned += pos.cost
                settled.append(pos)
            else:
                still_open.append(pos)

        self.positions = still_open

        if log:
            self.daily_log.append({
                "date": today,
                "cash": self.cash,
                "exposure": self.exposure,
                "equity": self.equity,
                "n_positions": self.n_positions,
                "n_settled": len(settled),
                "utilization": self.utilization,
            })

        return settled

    # ------------------------------------------------------------------
    # Deploy capital: open a new position
    # ------------------------------------------------------------------

    def open_position(
        self,
        ticker: str,
        entry_date: _dt.date,
        close_date: _dt.date,
        cost: float,
        pnl: float,
        direction: str,
        contracts: float,
        dev: float,
        group: str,
        category: str,
    ) -> Position:
        """Open a new position, deducting *cost* from cash."""
        if cost > self.cash:
            raise ValueError(
                f"Insufficient cash ({self.cash:.0f}) for position "
                f"cost ({cost:.0f})"
            )
        self.cash -= cost
        pos = Position(
            ticker=ticker,
            entry_date=entry_date,
            close_date=close_date,
            cost=cost,
            pnl=pnl,
            direction=direction,
            contracts=contracts,
            dev=dev,
            group=group,
            category=category,
        )
        self.positions.append(pos)
        return pos

    # ------------------------------------------------------------------
    # Capital availability helpers (for the allocation engine)
    # ------------------------------------------------------------------

    def deployable(
        self,
        max_exposure_frac: float,
        max_daily_frac: float = 1.0,
    ) -> float:
        """Capital available for new positions today.

        Respects exposure cap and daily deployment limit.
        """
        max_exposure = self.equity * max_exposure_frac
        room = max(0.0, max_exposure - self.exposure)
        daily_limit = self.cash * max_daily_frac
        return min(room, daily_limit, self.cash)

    def settle_all(self) -> None:
        """Force-settle all remaining positions (end-of-backtest cleanup)."""
        for pos in self.positions:
            self.cash += pos.cost + pos.pnl
            self.total_settled_positions += 1
            self.total_realized_pnl += pos.pnl
            self.total_cost_returned += pos.cost
        self.positions = []



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
    # Minimum contracts in a composite bucket to trust its edge
    "min_bucket_contracts": 10_000,
    # Minimum fade excess (%) in a bucket to participate
    "min_fade_excess_pct": 0.0,
    # ── Mean-reversion signal parameters ──────────────────────────────────
    # Moving-average lookback in trades (trailing window, no forward info)
    "ma_lookback": 50,
    # Minimum absolute deviation from MA to generate a fade signal (cents)
    "min_deviation": 5,
    # ── Price and time filters ────────────────────────────────────────────
    "price_min": 15,
    "price_max": 85,
    "time_min_hours": 1,
    "time_max_hours": 168,  # 7 days — cap hold time for capital recycling
    # ── Sizing multipliers (adjust effective_edge for capital allocation) ─
    # Deviation magnitude: larger deviations get more weight
    "large_dev_mult": 2.0,         # |dev| >= 15¢
    "medium_dev_mult": 1.25,       # 5¢ ≤ |dev| < 15¢
    # Direction: fading up (buy NO) is historically stronger
    "fade_up_mult": 1.25,
    "fade_down_mult": 0.80,
    # Time-to-close: far from close is best for mean-reversion
    "far_from_close_mult": 1.5,    # 6h–30d
    "very_far_mult": 1.75,         # 30d+
    "near_close_mult": 0.5,        # < 1h
    # Weekend: neutral (MR didn't show strong day-of-week effect)
    "weekend_mult": 1.0,
    # ── Minimum warmup ────────────────────────────────────────────────────
    "min_warmup_months": 6,
    # ── Capital management ────────────────────────────────────────────────
    "initial_capital": 1_000_000,   # cents ($10,000)
    "max_single_trade_frac": 0.05,
    "max_daily_deploy_frac": 1,
    "max_total_exposure_frac": 0.90,
    "min_trade_allocation": 1,      # cents — match maker backtest
    "max_trades_per_day": 500,
    # ── Realism constraints ──────────────────────────────────────────────
    # Max fraction of a ticker's daily volume we can participate in
    "max_participation_rate": 0.10,
    # Half-spread + slippage cost deducted per contract (cents)
    "spread_cost_cents": 2,
    # Portfolio value cap — stop compounding beyond this (cents; $100k)
    "max_portfolio_cents": 10_000_000,
    # ── Lookback window for parameter estimation ─────────────────────────
    "lookback_months": 12,
    "lookback_months_recent": 3,
    "lookback_transition_date": "2024-01-01",
    "backtest_start_date": "2023-01-01",
    # Groups to always exclude (empty = purely data-driven)
    "prior_exclude_groups": [],
}


class BacktestMeanReversionAnalysis(Analysis):
    """Walk-forward backtest of a mean-reversion fade strategy."""

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
        config: dict | None = None,
    ):
        super().__init__(
            name="backtest_mean_reversion",
            description=(
                "Walk-forward backtest of mean-reversion fade strategy "
                "with monthly parameter recalculation"
            ),
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(
            markets_dir or base_dir / "data" / "kalshi" / "markets"
        )
        self.config = {**DEFAULT_CONFIG, **(config or {})}

    # ── Main entry point ─────────────────────────────────────────────────────

    def _run_impl(self) -> AnalysisOutput:
        con = duckdb.connect()
        con.execute("SET preserve_insertion_order = false")

        # ── Step 1: Load data and pre-compute MA deviations ──────────────────
        with self.progress("Loading trades and computing MA deviations"):
            self._load_data(con)

        # ── Step 2: Date range ───────────────────────────────────────────────
        with self.progress("Computing date range"):
            date_range = self._get_date_range(con)
        if date_range is None:
            return AnalysisOutput(data=pd.DataFrame())

        first_trade_date, last_close_date = date_range

        # ── Step 3: Recalculation schedule ───────────────────────────────────
        recalc_dates = self._build_recalc_schedule(
            first_trade_date, last_close_date
        )
        if len(recalc_dates) < 2:
            return AnalysisOutput(data=pd.DataFrame())

        # ── Step 4: Walk-forward backtest ────────────────────────────────────
        n_periods = len(recalc_dates) - 1
        with self.progress(
            f"Running walk-forward fade backtest ({n_periods} periods)"
        ):
            trade_results, param_history = self._walk_forward(con, recalc_dates)

        if trade_results.empty:
            return AnalysisOutput(data=pd.DataFrame())

        # ── Step 5: Metrics ──────────────────────────────────────────────────
        with self.progress("Computing performance metrics"):
            daily_pnl, metrics, rolling = self._compute_results(trade_results)

        # ── Step 6: Monthly breakdown ────────────────────────────────────────
        monthly_df = self._monthly_breakdown(trade_results)

        # ── Step 7: Visualize ────────────────────────────────────────────────
        fig = self._create_figure(
            daily_pnl, rolling, metrics, monthly_df, param_history
        )
        chart = self._create_chart(rolling, metrics)

        output_data = metrics.to_dataframe()
        output_data["strategy"] = "mean_reversion_fade"
        output_data["recalc_frequency"] = self.config["recalc_frequency"]

        # ── Step 8: Trade log ────────────────────────────────────────────────
        trade_log = self._build_trade_log(trade_results)

        return AnalysisOutput(
            figure=fig,
            data=output_data,
            chart=chart,
            metadata={
                "daily_pnl": daily_pnl,
                "rolling": rolling,
                "monthly": monthly_df,
                "param_history": param_history,
                "trade_results": trade_results,
                "trade_log": trade_log,
            },
        )

    # ── Data loading ─────────────────────────────────────────────────────────

    def _load_data(self, con: duckdb.DuckDBPyConnection) -> None:
        """Load parquet data and pre-compute trailing MA deviation.

        Two-step approach for OOM safety:
        1. Window function on trades table alone (small per-row memory).
        2. Keep trades and markets as separate tables for filtered joins later.

        The deviation column ``dev`` = yes_price − MA_k(preceding k trades).
        Positive dev = price above recent average; negative = below.
        """
        lookback = self.config["ma_lookback"]

        # Step 1: raw trades
        con.execute(f"""
            CREATE TABLE raw_trades AS
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

        # Step 2: add trailing-MA deviation (window on trades alone)
        con.execute(f"""
            CREATE TABLE trades AS
            SELECT
                ticker,
                yes_price,
                no_price,
                taker_side,
                contracts,
                created_time,
                yes_price - AVG(yes_price) OVER (
                    PARTITION BY ticker
                    ORDER BY created_time
                    ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING
                ) AS dev
            FROM raw_trades
        """)
        con.execute("DROP TABLE raw_trades")
        # Remove rows with NULL deviation (first trades per market)
        con.execute("DELETE FROM trades WHERE dev IS NULL")

        # Markets
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
                MAX(m.close_time)   AS last_close
            FROM trades t
            INNER JOIN markets m ON t.ticker = m.ticker
        """).fetchone()
        if row is None or row[0] is None:
            return None
        first_trade = pd.Timestamp(row[0])
        last_close = pd.Timestamp(row[1])
        if first_trade.tzinfo is not None:
            first_trade = first_trade.tz_localize(None)
        if last_close.tzinfo is not None:
            last_close = last_close.tz_localize(None)
        return first_trade, last_close

    # ── Schedule ─────────────────────────────────────────────────────────────

    def _build_recalc_schedule(
        self, first_date: pd.Timestamp, last_date: pd.Timestamp
    ) -> list[pd.Timestamp]:
        """Build list of parameter recalculation dates."""
        freq_key = self.config["recalc_frequency"]
        delta = RECALC_FREQUENCIES.get(freq_key)
        if delta is None:
            raise ValueError(
                f"Unknown recalc_frequency: {freq_key!r}. "
                f"Use one of {list(RECALC_FREQUENCIES)}"
            )

        first_date = (
            first_date.tz_localize(None) if first_date.tzinfo else first_date
        )
        last_date = (
            last_date.tz_localize(None) if last_date.tzinfo else last_date
        )

        warmup = relativedelta(months=self.config["min_warmup_months"])
        warmup_start = first_date + warmup
        warmup_start = pd.Timestamp(warmup_start.year, warmup_start.month, 1)

        bt_start_str = self.config.get("backtest_start_date")
        if bt_start_str:
            explicit_start = pd.Timestamp(bt_start_str)
            start = max(warmup_start, explicit_start)
            start = pd.Timestamp(start.year, start.month, 1)
        else:
            start = warmup_start

        dates: list[pd.Timestamp] = []
        current = start
        while current <= last_date:
            dates.append(current)
            current = current + delta

        if dates and dates[-1] < last_date:
            dates.append(last_date + pd.Timedelta(days=1))

        return dates

    # ── Parameter estimation ─────────────────────────────────────────────────

    def _estimate_parameters(
        self, con: duckdb.DuckDBPyConnection, cutoff: pd.Timestamp
    ) -> dict:
        """Compute fade strategy parameters from resolved data before cutoff.

        Uses only trades/markets in [cutoff − lookback, cutoff) where both
        created_time and close_time fall within that window.

        Returns:
        - group_fade_edge: {group: fade_excess_pct}
        - composite_fade_edge: {(group, price, time, day): fade_excess_pct}
        - fade_up_excess: historical fade-up excess %
        - fade_down_excess: historical fade-down excess %
        """
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        transition = pd.Timestamp(
            self.config.get("lookback_transition_date", "2099-01-01")
        )
        if cutoff >= transition:
            lb_months = self.config["lookback_months_recent"]
        else:
            lb_months = self.config["lookback_months"]
        lookback_start = cutoff - relativedelta(months=lb_months)
        lookback_str = lookback_start.strftime("%Y-%m-%d %H:%M:%S")
        min_dev = self.config["min_deviation"]

        # ── Composite fade edge (group × price × time × day_type) ────────────
        df = con.execute(f"""
            WITH trade_data AS (
                SELECT
                    {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")}
                        AS category,
                    t.yes_price,
                    t.dev,
                    t.contracts,
                    m.result,
                    EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0
                        AS hours_to_close,
                    CASE
                        WHEN dayofweek(t.created_time) IN (0, 6) THEN 'Weekend'
                        ELSE 'Weekday'
                    END AS day_type,
                    CASE
                        WHEN t.yes_price BETWEEN 1  AND 20 THEN '01-20'
                        WHEN t.yes_price BETWEEN 21 AND 40 THEN '21-40'
                        WHEN t.yes_price BETWEEN 41 AND 60 THEN '41-60'
                        WHEN t.yes_price BETWEEN 61 AND 80 THEN '61-80'
                        ELSE '81-99'
                    END AS price_bucket,
                    CASE
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time))
                             / 3600.0 <= 1   THEN '0-1h'
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time))
                             / 3600.0 <= 6   THEN '1-6h'
                        WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time))
                             / 3600.0 <= 72  THEN '6h-3d'
                        ELSE '3d+'
                    END AS time_bucket,
                    -- Fade PnL
                    CASE
                        WHEN t.dev > 0 AND m.result = 'no'
                            THEN  t.yes_price * t.contracts
                        WHEN t.dev > 0 AND m.result = 'yes'
                            THEN -(100 - t.yes_price) * t.contracts
                        WHEN t.dev < 0 AND m.result = 'yes'
                            THEN  (100 - t.yes_price) * t.contracts
                        WHEN t.dev < 0 AND m.result = 'no'
                            THEN -t.yes_price * t.contracts
                    END AS fade_pnl,
                    -- Fade cost
                    CASE
                        WHEN t.dev > 0
                            THEN (100 - t.yes_price) * t.contracts
                        WHEN t.dev < 0
                            THEN t.yes_price * t.contracts
                    END AS fade_cost
                FROM trades t
                INNER JOIN markets m ON t.ticker = m.ticker
                WHERE t.created_time >= TIMESTAMP '{lookback_str}'
                  AND t.created_time <  TIMESTAMP '{cutoff_str}'
                  AND m.close_time   <  TIMESTAMP '{cutoff_str}'
                  AND m.close_time   >  t.created_time
                  AND ABS(t.dev)     >= {min_dev}
            )
            SELECT
                category,
                price_bucket,
                time_bucket,
                day_type,
                SUM(fade_pnl)  AS fade_pnl,
                SUM(fade_cost) AS fade_cost,
                SUM(contracts) AS total_contracts
            FROM trade_data
            GROUP BY category, price_bucket, time_bucket, day_type
        """).df()

        if df.empty:
            return {
                "group_fade_edge": {},
                "composite_fade_edge": {},
                "group_price_edge": {},
                "fade_up_excess": 0.0,
                "fade_down_excess": 0.0,
            }

        # Map categories → groups
        unique_cats = df["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        df["group"] = df["category"].map(cat_to_group)

        # ── Group-level fade edge ────────────────────────────────────────────
        group_agg = (
            df.groupby("group")
            .agg({
                "fade_pnl": "sum",
                "fade_cost": "sum",
                "total_contracts": "sum",
            })
            .reset_index()
        )
        group_agg["fade_excess_pct"] = (
            group_agg["fade_pnl"]
            * 100.0
            / group_agg["fade_cost"].replace(0, np.nan)
        )
        group_fade_edge = dict(
            zip(group_agg["group"], group_agg["fade_excess_pct"])
        )

        # ── Composite fade edge (per bucket) ────────────────────────────────
        combo = (
            df.groupby(["group", "price_bucket", "time_bucket", "day_type"])
            .agg({
                "fade_pnl": "sum",
                "fade_cost": "sum",
                "total_contracts": "sum",
            })
            .reset_index()
        )
        combo["fade_excess_pct"] = (
            combo["fade_pnl"]
            * 100.0
            / combo["fade_cost"].replace(0, np.nan)
        )
        min_contracts = self.config["min_bucket_contracts"]
        combo = combo[combo["total_contracts"] >= min_contracts]

        composite_fade_edge: dict[tuple, float] = {}
        for _, row in combo.iterrows():
            key = (
                row["group"],
                row["price_bucket"],
                row["time_bucket"],
                row["day_type"],
            )
            composite_fade_edge[key] = row["fade_excess_pct"]

        # ── (Group × Price) fallback edge ────────────────────────────────────
        # Used when the exact 4-way composite bucket has insufficient data.
        gp = (
            df.groupby(["group", "price_bucket"])
            .agg({
                "fade_pnl": "sum",
                "fade_cost": "sum",
                "total_contracts": "sum",
            })
            .reset_index()
        )
        gp["fade_excess_pct"] = (
            gp["fade_pnl"]
            * 100.0
            / gp["fade_cost"].replace(0, np.nan)
        )
        # Lower threshold for 2-way: need at least 1/4 of the full bucket min
        gp = gp[gp["total_contracts"] >= max(min_contracts // 4, 1_000)]
        group_price_edge: dict[tuple, float] = {}
        for _, row in gp.iterrows():
            group_price_edge[(row["group"], row["price_bucket"])] = (
                row["fade_excess_pct"]
            )

        # ── Directional fade analysis (fade-up vs fade-down) ─────────────────
        dir_df = con.execute(f"""
            WITH trade_data AS (
                SELECT
                    t.dev,
                    t.contracts,
                    t.yes_price,
                    m.result,
                    CASE
                        WHEN t.dev > 0 AND m.result = 'no'
                            THEN  t.yes_price * t.contracts
                        WHEN t.dev > 0 AND m.result = 'yes'
                            THEN -(100 - t.yes_price) * t.contracts
                        WHEN t.dev < 0 AND m.result = 'yes'
                            THEN  (100 - t.yes_price) * t.contracts
                        WHEN t.dev < 0 AND m.result = 'no'
                            THEN -t.yes_price * t.contracts
                    END AS fade_pnl,
                    CASE
                        WHEN t.dev > 0
                            THEN (100 - t.yes_price) * t.contracts
                        WHEN t.dev < 0
                            THEN t.yes_price * t.contracts
                    END AS fade_cost
                FROM trades t
                INNER JOIN markets m ON t.ticker = m.ticker
                WHERE t.created_time >= TIMESTAMP '{lookback_str}'
                  AND t.created_time <  TIMESTAMP '{cutoff_str}'
                  AND m.close_time   <  TIMESTAMP '{cutoff_str}'
                  AND m.close_time   >  t.created_time
                  AND ABS(t.dev)     >= {min_dev}
            )
            SELECT
                CASE WHEN dev > 0 THEN 'fade_up' ELSE 'fade_down' END
                    AS direction,
                SUM(fade_pnl)  AS fade_pnl,
                SUM(fade_cost) AS fade_cost
            FROM trade_data
            GROUP BY 1
        """).df()

        fade_up_excess = 0.0
        fade_down_excess = 0.0
        if not dir_df.empty:
            for _, row in dir_df.iterrows():
                excess = (
                    row["fade_pnl"] * 100.0 / row["fade_cost"]
                    if row["fade_cost"] > 0
                    else 0.0
                )
                if row["direction"] == "fade_up":
                    fade_up_excess = excess
                else:
                    fade_down_excess = excess

        return {
            "group_fade_edge": group_fade_edge,
            "composite_fade_edge": composite_fade_edge,
            "group_price_edge": group_price_edge,
            "fade_up_excess": fade_up_excess,
            "fade_down_excess": fade_down_excess,
        }

    # ── Walk-forward engine ──────────────────────────────────────────────────

    def _walk_forward(
        self,
        con: duckdb.DuckDBPyConnection,
        recalc_dates: list[pd.Timestamp],
    ) -> tuple[pd.DataFrame, list[dict]]:
        """Day-by-day walk-forward backtest with explicit position tracking.

        Phase 1: Estimate parameters and filter trades for each recalc period.
        Phase 2: Step day-by-day using ``PortfolioState``:
          - ``portfolio.step(day)``: settle resolved positions, return PnL +
            cost to cash, prune closed positions from the open list.
          - Allocate ``portfolio.deployable(...)`` capital to today's trades,
            weighted by effective_edge.
          - ``portfolio.open_position(...)``: lock cost, record position.
        """
        cfg = self.config
        all_filtered: list[pd.DataFrame] = []
        param_history: list[dict] = []

        # ── Phase 1: Parameter estimation + filtering ────────────────────────
        for i in trange(len(recalc_dates) - 1, desc="Estimating parameters"):
            period_start = recalc_dates[i]
            period_end = recalc_dates[i + 1]

            params = self._estimate_parameters(con, period_start)

            n_positive_groups = sum(
                1 for v in params["group_fade_edge"].values() if v > 0
            )

            # Even if no composite buckets survived the min_bucket_contracts
            # filter, we still attempt trades using group-level edges as
            # fallback (the hierarchical filter in _filter_trades handles
            # this).  Only skip if there are *zero* groups with positive edge.
            if n_positive_groups == 0:
                param_history.append({
                    "period_start": period_start,
                    "period_end": period_end,
                    "n_groups_positive_fade": 0,
                    "n_composite_buckets_positive": 0,
                    "n_composite_buckets_total": 0,
                    "avg_group_fade_edge": 0,
                    "fade_up_excess": 0,
                    "fade_down_excess": 0,
                    "n_raw_trades": 0,
                    "n_filtered_trades": 0,
                })
                continue

            period_trades = self._get_period_trades(con, period_start, period_end)
            n_raw = len(period_trades)

            if period_trades.empty:
                filtered = pd.DataFrame()
            else:
                filtered = self._filter_trades(period_trades, params)

            n_filtered = len(filtered) if not filtered.empty else 0

            n_positive_buckets = sum(
                1
                for v in params["composite_fade_edge"].values()
                if v > cfg["min_fade_excess_pct"]
            )

            param_history.append({
                "period_start": period_start,
                "period_end": period_end,
                "n_groups_positive_fade": n_positive_groups,
                "n_composite_buckets_positive": n_positive_buckets,
                "n_composite_buckets_total": len(params["composite_fade_edge"]),
                "avg_group_fade_edge": (
                    np.mean(list(params["group_fade_edge"].values()))
                    if params["group_fade_edge"]
                    else 0
                ),
                "fade_up_excess": params["fade_up_excess"],
                "fade_down_excess": params["fade_down_excess"],
                "n_raw_trades": n_raw,
                "n_filtered_trades": n_filtered,
            })

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

        # ── Phase 2: Day-by-day simulation with PortfolioState ───────────────
        portfolio = PortfolioState(cash=float(cfg["initial_capital"]))
        all_records: list[dict] = []

        date_range = pd.date_range(first_day, last_day, freq="D")

        for day_ts in date_range:
            day = day_ts.date()

            # Step the portfolio: settle all positions that closed on or
            # before today.  Cost + PnL flows back to cash automatically.
            portfolio.step(day)

            if day not in trade_groups:
                continue

            day_trades = trade_groups[day]

            # ── How much capital can we deploy? ──────────────────────────
            # If equity exceeds the portfolio cap, limit deployable capital
            # to act as if the portfolio were only max_portfolio_cents large.
            effective_equity = min(
                portfolio.equity,
                cfg.get("max_portfolio_cents", float("inf")),
            )
            deployable = portfolio.deployable(
                max_exposure_frac=cfg["max_total_exposure_frac"],
                max_daily_frac=cfg["max_daily_deploy_frac"],
            )
            # Clamp deployable to effective_equity * exposure_frac
            deployable = min(
                deployable,
                effective_equity * cfg["max_total_exposure_frac"],
            )

            if deployable < cfg["min_trade_allocation"]:
                continue

            # ── Select top trades by effective edge ──────────────────────
            day_trades = day_trades.sort_values(
                "effective_edge", ascending=False
            )
            max_per_day = cfg.get("max_trades_per_day", 100_000)
            day_trades = day_trades.head(max_per_day)

            # ── Edge-weighted allocation ─────────────────────────────────
            edges = np.maximum(
                day_trades["effective_edge"].values.copy(), 0.001
            )
            total_edge = edges.sum()
            weights = edges / total_edge
            allocations = weights * deployable

            max_per_trade = effective_equity * cfg["max_single_trade_frac"]
            allocations = np.minimum(allocations, max_per_trade)

            cpc = day_trades["cost_per_contract"].values
            ppc = day_trades["pnl_per_contract"].values
            orig_contracts = day_trades["contracts"].values

            # ── Participation cap: can only take a fraction of volume ────
            max_participation = cfg.get("max_participation_rate", 1.0)
            participation_limit = orig_contracts * max_participation

            max_from_alloc = allocations / np.maximum(cpc, 0.01)
            actual_contracts = np.minimum(max_from_alloc, participation_limit)

            viable = actual_contracts >= 0.5
            actual_contracts = actual_contracts * viable
            actual_costs = actual_contracts * cpc

            # Scale down if total exceeds available cash
            total_needed = actual_costs.sum()
            if total_needed > portfolio.cash and total_needed > 0:
                scale = portfolio.cash * 0.999 / total_needed
                actual_contracts = actual_contracts * scale
                actual_costs = actual_contracts * cpc
                viable = actual_contracts >= 0.5
                actual_contracts = actual_contracts * viable
                actual_costs = actual_contracts * cpc

            # ── Execute trades via portfolio.open_position() ─────────────
            spread_cost_per = cfg.get("spread_cost_cents", 0)
            for idx_offset, (_, trade) in enumerate(day_trades.iterrows()):
                ac = actual_contracts[idx_offset]
                if ac < 0.5:
                    continue

                act_cost = actual_costs[idx_offset]
                # Deduct spread/slippage from PnL
                act_pnl = ac * ppc[idx_offset] - ac * spread_cost_per

                # Final guard: can't spend more than we have
                if act_cost > portfolio.cash:
                    ac = portfolio.cash / max(cpc[idx_offset], 0.01)
                    if ac < 0.5:
                        continue
                    act_cost = ac * cpc[idx_offset]
                    act_pnl = ac * ppc[idx_offset] - ac * spread_cost_per

                cash_before = portfolio.cash

                # Resolve close_date for this position
                close_dt = pd.Timestamp(trade["close_time"])
                if (
                    hasattr(close_dt, "tzinfo")
                    and close_dt.tzinfo is not None
                ):
                    close_dt = close_dt.tz_localize(None)
                close_date = close_dt.date()

                # Open the position — cash is deducted automatically
                portfolio.open_position(
                    ticker=trade["ticker"],
                    entry_date=day,
                    close_date=close_date,
                    cost=act_cost,
                    pnl=act_pnl,
                    direction=trade["fade_direction"],
                    contracts=ac,
                    dev=trade["dev"],
                    group=trade["group"],
                    category=trade["category"],
                )

                all_records.append({
                    "trade_time": trade["trade_time"],
                    "close_time": trade["close_time"],
                    "ticker": trade["ticker"],
                    "category": trade["category"],
                    "group": trade["group"],
                    "fade_direction": trade["fade_direction"],
                    "result": trade["result"],
                    "fade_won": trade["fade_won"],
                    "yes_price": trade["yes_price"],
                    "no_price": trade["no_price"],
                    "dev": trade["dev"],
                    "dev_magnitude": trade["dev_magnitude"],
                    "contracts": trade["contracts"],
                    "sized_contracts": ac,
                    "participation_rate": ac / max(trade["contracts"], 1),
                    "fade_pnl": trade["fade_pnl"],
                    "adj_fade_pnl": act_pnl,
                    "fade_cost": trade["fade_cost"],
                    "adj_fade_cost": act_cost,
                    "balance_before": cash_before,
                    "balance_after": portfolio.cash,
                    "open_exposure": portfolio.exposure,
                    "n_open_positions": portfolio.n_positions,
                    "effective_edge": trade["effective_edge"],
                    "composite_fade_edge": trade["composite_fade_edge"],
                    "hours_to_close": trade["hours_to_close"],
                    "price_bucket": trade["price_bucket"],
                    "time_bucket": trade["time_bucket"],
                    "day_type": trade["day_type"],
                    "pnl_date": trade["pnl_date"],
                })

        # Settle any positions still open after the last calendar day
        portfolio.settle_all()

        if not all_records:
            return pd.DataFrame(), param_history

        return pd.DataFrame(all_records), param_history

    def _get_period_trades(
        self,
        con: duckdb.DuckDBPyConnection,
        period_start: pd.Timestamp,
        period_end: pd.Timestamp,
    ) -> pd.DataFrame:
        """Get all trades in the period with their pre-computed deviations.

        We do NOT require market resolution within the period — trades are
        entered when the signal fires and resolved when the market closes
        (which may be after the period ends).
        """
        ps = period_start.strftime("%Y-%m-%d %H:%M:%S")
        pe = period_end.strftime("%Y-%m-%d %H:%M:%S")
        min_dev = self.config["min_deviation"]

        return con.execute(f"""
            SELECT
                t.ticker,
                {CATEGORY_SQL.replace("event_ticker", "m.event_ticker")}
                    AS category,
                t.yes_price,
                t.no_price,
                t.dev,
                t.contracts,
                t.created_time AS trade_time,
                m.result,
                m.close_time,
                EXTRACT(EPOCH FROM (m.close_time - t.created_time)) / 3600.0
                    AS hours_to_close,
                CASE
                    WHEN dayofweek(t.created_time) IN (0, 6) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS day_type,
                CASE
                    WHEN t.yes_price BETWEEN 1  AND 20 THEN '01-20'
                    WHEN t.yes_price BETWEEN 21 AND 40 THEN '21-40'
                    WHEN t.yes_price BETWEEN 41 AND 60 THEN '41-60'
                    WHEN t.yes_price BETWEEN 61 AND 80 THEN '61-80'
                    ELSE '81-99'
                END AS price_bucket,
                CASE
                    WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time))
                         / 3600.0 <= 1   THEN '0-1h'
                    WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time))
                         / 3600.0 <= 6   THEN '1-6h'
                    WHEN EXTRACT(EPOCH FROM (m.close_time - t.created_time))
                         / 3600.0 <= 72  THEN '6h-3d'
                    ELSE '3d+'
                END AS time_bucket,
                -- Fade direction
                CASE
                    WHEN t.dev > 0 THEN 'fade_up'
                    ELSE 'fade_down'
                END AS fade_direction,
                -- Fade PnL (total for this trade)
                CASE
                    WHEN t.dev > 0 AND m.result = 'no'
                        THEN  t.yes_price * t.contracts
                    WHEN t.dev > 0 AND m.result = 'yes'
                        THEN -(100 - t.yes_price) * t.contracts
                    WHEN t.dev < 0 AND m.result = 'yes'
                        THEN  (100 - t.yes_price) * t.contracts
                    WHEN t.dev < 0 AND m.result = 'no'
                        THEN -t.yes_price * t.contracts
                END AS fade_pnl,
                -- Fade cost (total)
                CASE
                    WHEN t.dev > 0
                        THEN (100 - t.yes_price) * t.contracts
                    WHEN t.dev < 0
                        THEN t.yes_price * t.contracts
                END AS fade_cost,
                -- Fade won
                CASE
                    WHEN t.dev > 0 AND m.result = 'no'  THEN 1
                    WHEN t.dev < 0 AND m.result = 'yes' THEN 1
                    ELSE 0
                END AS fade_won,
                -- Deviation magnitude bucket
                CASE
                    WHEN ABS(t.dev) < 10 THEN 'small'
                    WHEN ABS(t.dev) < 20 THEN 'medium'
                    ELSE 'large'
                END AS dev_magnitude
            FROM trades t
            INNER JOIN markets m ON t.ticker = m.ticker
            WHERE t.created_time >= TIMESTAMP '{ps}'
              AND t.created_time <  TIMESTAMP '{pe}'
              AND m.close_time   >  t.created_time
              AND ABS(t.dev)     >= {min_dev}
        """).df()

    def _filter_trades(
        self, trades: pd.DataFrame, params: dict
    ) -> pd.DataFrame:
        """Apply regime filters and compute effective_edge for sizing.

        Uses a **hierarchical edge lookup** to avoid dropping trades just
        because their exact 4-way composite bucket didn't have enough data
        in the lookback window:

            1. (group, price_bucket, time_bucket, day_type) — exact composite
            2. (group, price_bucket)                        — partial fallback
            3.  group                                       — group-level edge

        At every level the edge must be positive to keep the trade.

        Does NOT size positions — sizing is done in _walk_forward with capital
        awareness.  Returns filtered trades with:
        - composite_fade_edge: best available edge from the hierarchy
        - effective_edge: composite × deviation/direction/time multipliers
        - cost_per_contract, pnl_per_contract: for proportional sizing
        """
        cfg = self.config

        trades = trades.copy()
        unique_cats = trades["category"].unique()
        cat_to_group = {c: get_group(c) for c in unique_cats}
        trades["group"] = trades["category"].map(cat_to_group)

        # ── Filter 1: Price range ────────────────────────────────────────────
        trades = trades[
            (trades["yes_price"] >= cfg["price_min"])
            & (trades["yes_price"] <= cfg["price_max"])
        ]
        if trades.empty:
            return trades

        # ── Filter 2: Time-to-close range ────────────────────────────────────
        trades = trades[
            (trades["hours_to_close"] >= cfg["time_min_hours"])
            & (trades["hours_to_close"] <= cfg["time_max_hours"])
        ]
        if trades.empty:
            return trades

        # ── Filter 3: Group must have positive fade edge ─────────────────────
        group_fade_edge = params["group_fade_edge"]
        exclude = set(cfg.get("prior_exclude_groups", []))
        trades["group_fade_edge"] = (
            trades["group"].map(group_fade_edge).fillna(0)
        )
        trades = trades[
            (trades["group_fade_edge"] > 0) & (~trades["group"].isin(exclude))
        ]
        if trades.empty:
            return trades

        # ── Filter 4: Hierarchical edge lookup (composite → partial → group)─
        composite_fade_edge = params["composite_fade_edge"]
        group_price_edge = params.get("group_price_edge", {})
        min_excess = cfg["min_fade_excess_pct"]

        def lookup_edge_hierarchical(row):
            """Try composite, then (group, price), then group-level edge."""
            # Level 1: exact composite bucket
            key4 = (
                row["group"],
                row["price_bucket"],
                row["time_bucket"],
                row["day_type"],
            )
            edge = composite_fade_edge.get(key4)
            if edge is not None:
                return edge

            # Level 2: (group, price_bucket) — drops time and day
            key2 = (row["group"], row["price_bucket"])
            edge = group_price_edge.get(key2)
            if edge is not None:
                return edge

            # Level 3: group-level edge (already known to be > 0 from filter 3)
            return row["group_fade_edge"]

        trades["composite_fade_edge"] = trades.apply(
            lookup_edge_hierarchical, axis=1
        )
        trades = trades[trades["composite_fade_edge"] > min_excess]
        if trades.empty:
            return trades

        # ── Deduplicate: one signal per market per day ────────────────────
        trades["_trade_date"] = pd.to_datetime(trades["trade_time"]).dt.date
        trades["abs_dev"] = trades["dev"].abs()

        agg = (
            trades.groupby(["ticker", "_trade_date"])
            .agg(total_contracts=("contracts", "sum"))
            .reset_index()
        )
        idx_max = trades.groupby(["ticker", "_trade_date"])["abs_dev"].idxmax()
        trades = trades.loc[idx_max].copy()
        trades = trades.merge(
            agg, on=["ticker", "_trade_date"], how="left"
        )
        trades["contracts"] = trades["total_contracts"]
        trades.drop(
            columns=["_trade_date", "abs_dev", "total_contracts"], inplace=True
        )

        # Recompute fade_pnl and fade_cost with aggregated contract count
        trades["fade_pnl"] = np.where(
            trades["dev"] > 0,
            np.where(
                trades["result"] == "no",
                trades["yes_price"] * trades["contracts"],
                -(100 - trades["yes_price"]) * trades["contracts"],
            ),
            np.where(
                trades["result"] == "yes",
                (100 - trades["yes_price"]) * trades["contracts"],
                -trades["yes_price"] * trades["contracts"],
            ),
        )
        trades["fade_cost"] = np.where(
            trades["dev"] > 0,
            (100 - trades["yes_price"]) * trades["contracts"],
            trades["yes_price"] * trades["contracts"],
        )
        trades["fade_won"] = np.where(
            trades["dev"] > 0,
            (trades["result"] == "no").astype(int),
            (trades["result"] == "yes").astype(int),
        )

        if trades.empty:
            return trades

        # ── Per-contract values ──────────────────────────────────────────────
        safe_contracts = trades["contracts"].replace(0, 1)
        trades["pnl_per_contract"] = trades["fade_pnl"] / safe_contracts
        trades["cost_per_contract"] = trades["fade_cost"] / safe_contracts

        # ── Effective edge = composite × multipliers ─────────────────────────
        edge_mult = np.ones(len(trades))

        # Deviation magnitude multiplier
        dev_mag = trades["dev_magnitude"].values
        edge_mult = np.where(
            dev_mag == "large", edge_mult * cfg["large_dev_mult"], edge_mult
        )
        edge_mult = np.where(
            dev_mag == "medium", edge_mult * cfg["medium_dev_mult"], edge_mult
        )

        # Direction multiplier (fade_up = buy NO, historically stronger)
        fade_dir = trades["fade_direction"].values
        edge_mult = np.where(
            fade_dir == "fade_up", edge_mult * cfg["fade_up_mult"], edge_mult
        )
        edge_mult = np.where(
            fade_dir == "fade_down",
            edge_mult * cfg["fade_down_mult"],
            edge_mult,
        )

        # Time-to-close multiplier
        htc = trades["hours_to_close"].values
        edge_mult = np.where(
            htc < 1, edge_mult * cfg["near_close_mult"], edge_mult
        )
        edge_mult = np.where(
            (htc >= 6) & (htc <= cfg["time_max_hours"]),
            edge_mult * cfg["far_from_close_mult"],
            edge_mult,
        )

        # Weekend multiplier
        is_weekend = trades["day_type"].values == "Weekend"
        edge_mult = np.where(
            is_weekend, edge_mult * cfg["weekend_mult"], edge_mult
        )

        trades["effective_edge"] = (
            trades["composite_fade_edge"].values * edge_mult
        )

        # ── Calendar columns for walk-forward ────────────────────────────────
        trades["trade_date"] = pd.to_datetime(trades["trade_time"]).dt.date
        trades["pnl_date"] = pd.to_datetime(trades["close_time"]).dt.date

        return trades

    # ── Trade log ────────────────────────────────────────────────────────────

    def _build_trade_log(self, trade_results: pd.DataFrame) -> pd.DataFrame:
        """Build a detailed trade-level log sorted by trade time."""
        log = trade_results.copy()
        log = log.sort_values("trade_time").reset_index(drop=True)

        log["adj_fade_pnl_dollars"] = log["adj_fade_pnl"] / 100.0
        log["adj_fade_cost_dollars"] = log["adj_fade_cost"] / 100.0
        log["balance_before_dollars"] = log["balance_before"] / 100.0
        log["balance_after_dollars"] = log["balance_after"] / 100.0
        log["open_exposure_dollars"] = log["open_exposure"] / 100.0

        cols = [
            "trade_time", "close_time", "ticker", "category", "group",
            "fade_direction", "result", "fade_won",
            "yes_price", "no_price", "dev", "dev_magnitude",
            "contracts", "sized_contracts", "participation_rate",
            "fade_pnl", "adj_fade_pnl", "adj_fade_pnl_dollars",
            "fade_cost", "adj_fade_cost", "adj_fade_cost_dollars",
            "balance_before", "balance_before_dollars",
            "balance_after", "balance_after_dollars",
            "open_exposure", "open_exposure_dollars",
            "n_open_positions",
            "effective_edge", "composite_fade_edge",
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

        output_dir = Path(output_dir)
        if (
            hasattr(self, "_last_output")
            and self._last_output
            and self._last_output.metadata
        ):
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

        daily_pnl = trade_results.groupby("pnl_date")["adj_fade_pnl"].sum()
        daily_pnl.index = pd.to_datetime(daily_pnl.index)
        daily_pnl = daily_pnl.sort_index()

        full_range = pd.date_range(
            daily_pnl.index.min(), daily_pnl.index.max(), freq="D"
        )
        daily_pnl = daily_pnl.reindex(full_range, fill_value=0)

        total_trades = len(trade_results)
        total_capital = trade_results["adj_fade_cost"].sum()
        trade_wins = int(trade_results["fade_won"].sum())

        metrics = compute_metrics(
            daily_pnl,
            initial_capital=self.config["initial_capital"],
            total_trades=total_trades,
            total_capital_deployed=total_capital,
            trade_wins=trade_wins,
        )

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
                pnl=("adj_fade_pnl", "sum"),
                cost=("adj_fade_cost", "sum"),
                trades=("adj_fade_pnl", "count"),
                wins=("fade_won", "sum"),
            )
            .reset_index()
        )
        monthly["excess_pct"] = (
            monthly["pnl"] * 100.0 / monthly["cost"].replace(0, np.nan)
        )
        monthly["win_rate"] = (
            monthly["wins"] * 100.0 / monthly["trades"].replace(0, np.nan)
        )
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
        color = "#e67e22"  # orange for mean-reversion
        ax1.plot(equity.index, equity.values / 100, color=color, linewidth=1)
        ax1.fill_between(
            equity.index,
            self.config["initial_capital"] / 100,
            equity.values / 100,
            alpha=0.15,
            color=color,
        )
        ax1.set_title(
            "Equity Curve (Mean-Reversion Fade Strategy)",
            fontsize=14,
            fontweight="bold",
        )
        ax1.set_ylabel("Equity ($)")
        ax1.axhline(
            self.config["initial_capital"] / 100,
            color="gray",
            linewidth=0.5,
            linestyle="--",
        )
        ax1.grid(alpha=0.3)

        textstr = (
            f"Sharpe: {metrics.sharpe_ratio:.2f}  |  "
            f"Sortino: {metrics.sortino_ratio:.2f}  |  "
            f"Max DD: {metrics.max_drawdown_pct:.1f}%  |  "
            f"Win Rate: {metrics.win_rate_daily:.1f}%  |  "
            f"Total Return: {metrics.total_return_pct:.1f}%"
        )
        ax1.text(
            0.5,
            1.02,
            textstr,
            transform=ax1.transAxes,
            fontsize=10,
            ha="center",
            va="bottom",
            bbox=dict(boxstyle="round", facecolor="bisque", alpha=0.5),
        )

        # ── Panel 2: Drawdown ────────────────────────────────────────────────
        ax2 = fig.add_subplot(gs[1, 0])
        if not rolling.empty and "drawdown_pct" in rolling.columns:
            ax2.fill_between(
                rolling["date"],
                rolling["drawdown_pct"],
                0,
                color="#e74c3c",
                alpha=0.4,
            )
            ax2.plot(
                rolling["date"],
                rolling["drawdown_pct"],
                color="#e74c3c",
                linewidth=0.5,
            )
        ax2.set_title("Drawdown (%)")
        ax2.set_ylabel("Drawdown %")
        ax2.grid(alpha=0.3)

        # ── Panel 3: Rolling Sharpe ──────────────────────────────────────────
        ax3 = fig.add_subplot(gs[1, 1])
        if not rolling.empty and "rolling_sharpe" in rolling.columns:
            rs = rolling.dropna(subset=["rolling_sharpe"])
            if not rs.empty:
                ax3.plot(
                    rs["date"],
                    rs["rolling_sharpe"],
                    color="#e67e22",
                    linewidth=0.8,
                )
                ax3.axhline(0, color="black", linewidth=0.5)
                ax3.axhline(
                    1, color="green", linewidth=0.5, linestyle="--", alpha=0.5
                )
                ax3.axhline(
                    -1, color="red", linewidth=0.5, linestyle="--", alpha=0.5
                )
        ax3.set_title("Rolling 90-Day Sharpe Ratio")
        ax3.set_ylabel("Sharpe")
        ax3.grid(alpha=0.3)

        # ── Panel 4: Monthly PnL bars ────────────────────────────────────────
        ax4 = fig.add_subplot(gs[2, :])
        if not monthly_df.empty:
            colors = [
                "#e67e22" if x >= 0 else "#e74c3c"
                for x in monthly_df["pnl_dollars"]
            ]
            x_pos = np.arange(len(monthly_df))
            ax4.bar(x_pos, monthly_df["pnl_dollars"], color=colors, alpha=0.8)
            n_labels = min(24, len(monthly_df))
            step = max(1, len(monthly_df) // n_labels)
            ax4.set_xticks(x_pos[::step])
            ax4.set_xticklabels(
                monthly_df["month"].iloc[::step],
                rotation=45,
                ha="right",
                fontsize=7,
            )
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

        # ── Panel 6: Parameter & signal evolution ────────────────────────────
        ax6 = fig.add_subplot(gs[3, 1])
        if param_history:
            ph = pd.DataFrame(param_history)
            ax6_twin = ax6.twinx()
            ax6.plot(
                ph["period_start"],
                ph["n_composite_buckets_positive"],
                "o-",
                color="#e67e22",
                markersize=3,
                label="Positive fade buckets",
            )
            ax6_twin.plot(
                ph["period_start"],
                ph["avg_group_fade_edge"],
                "s--",
                color="#9b59b6",
                markersize=3,
                label="Avg group fade edge %",
            )
            ax6.set_title("Fade Opportunity Over Time")
            ax6.set_ylabel("# Positive Buckets", color="#e67e22")
            ax6_twin.set_ylabel("Avg Fade Edge %", color="#9b59b6")
            ax6.grid(alpha=0.3)
            ax6.tick_params(axis="x", rotation=45)
            lines1, labels1 = ax6.get_legend_handles_labels()
            lines2, labels2 = ax6_twin.get_legend_handles_labels()
            ax6.legend(
                lines1 + lines2,
                labels1 + labels2,
                loc="upper left",
                fontsize=7,
            )
        else:
            ax6.text(
                0.5, 0.5, "No parameter history", ha="center", va="center"
            )

        plt.suptitle(
            "Mean-Reversion Fade Strategy Backtest — "
            f"{self.config['recalc_frequency'].title()} Recalculation",
            fontsize=16,
            fontweight="bold",
            y=1.01,
        )

        return fig

    # ── Chart config ─────────────────────────────────────────────────────────

    def _create_chart(
        self, rolling: pd.DataFrame, metrics: BacktestMetrics
    ) -> ChartConfig:
        """Create chart config for web rendering."""
        chart_data = []
        if not rolling.empty:
            step = max(1, len(rolling) // 500)
            for _, row in rolling.iloc[::step].iterrows():
                chart_data.append({
                    "date": (
                        str(row["date"].date())
                        if hasattr(row["date"], "date")
                        else str(row["date"])
                    ),
                    "equity": round(float(row["equity"]) / 100, 2),
                    "drawdown_pct": round(float(row["drawdown_pct"]), 4),
                    "cumulative_pnl_dollars": round(
                        float(row["cumulative_pnl"]) / 100, 2
                    ),
                })

        return ChartConfig(
            type=ChartType.LINE,
            data=chart_data,
            xKey="date",
            yKeys=["equity"],
            title=(
                f"Mean-Reversion Fade Strategy — "
                f"Sharpe {metrics.sharpe_ratio:.2f}"
            ),
            yUnit=UnitType.DOLLARS,
        )
