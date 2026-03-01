"""Portfolio reporter: aggregated analytics across all open and closed positions.

Produces:
* Overall summary: total cost basis, total unrealised PnL, realised PnL,
  win/loss counts.
* Per-market breakdown: which markets are traded most, current PnL by market.
* Historical PnL curve: cumulative PnL over time from fill history.
* Trades-by-hour-to-close: counts fills bucketed by how many hours remain
  until market close at the time of the trade.

Usage
-----
    from src.trading.portfolio import PortfolioReporter

    reporter = PortfolioReporter(positions, closed_positions, fills)

    summary = reporter.summary(snapshot_map)
    pnl_df  = reporter.historical_pnl()
    h2c_df  = reporter.trades_by_hour_to_close(close_times)
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.trading.models import Fill, Position


class PortfolioReporter:
    """Aggregate analytics for a collection of positions and fills.

    Parameters
    ----------
    open_positions:
        Currently open positions (unrealised PnL computed against live prices).
    closed_positions:
        Resolved/closed positions with known outcome.
    fills:
        Complete fill history across all positions.
    """

    def __init__(
        self,
        open_positions: list[Position] | None = None,
        closed_positions: list[tuple[Position, str]] | None = None,
        fills: list[Fill] | None = None,
    ):
        self.open_positions = open_positions or []
        # closed_positions: list of (Position, result) where result='yes'|'no'
        self.closed_positions = closed_positions or []
        self.fills = fills or []

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self, snapshot_map: dict[str, object] | None = None) -> dict:
        """Return a portfolio-level summary dictionary.

        Parameters
        ----------
        snapshot_map:
            Optional ``{ticker: MarketSnapshot}`` for current prices.

        Keys
        ----
        total_open_positions, total_closed_positions,
        total_cost_basis_cents, total_unrealised_pnl_cents,
        total_unrealised_pnl_dollars, total_realised_pnl_cents,
        total_realised_pnl_dollars, total_pnl_dollars,
        win_count, loss_count, win_rate
        """
        snap_map = snapshot_map or {}

        # Open positions
        total_cost = sum(p.cost_basis for p in self.open_positions)
        unrealised = 0.0
        for pos in self.open_positions:
            snap = snap_map.get(pos.ticker)
            if snap is not None and snap.mid_price is not None:  # type: ignore[union-attr]
                unrealised += pos.unrealised_pnl(int(round(snap.mid_price)))  # type: ignore[union-attr]

        # Closed positions
        realised = 0.0
        wins = losses = 0
        for pos, result in self.closed_positions:
            pnl = pos.realised_pnl(result)
            realised += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1

        total_pnl_cents = unrealised + realised
        total_trades = wins + losses

        return {
            "total_open_positions": len(self.open_positions),
            "total_closed_positions": len(self.closed_positions),
            "total_cost_basis_cents": round(total_cost, 2),
            "total_unrealised_pnl_cents": round(unrealised, 2),
            "total_unrealised_pnl_dollars": round(unrealised / 100, 4),
            "total_realised_pnl_cents": round(realised, 2),
            "total_realised_pnl_dollars": round(realised / 100, 4),
            "total_pnl_dollars": round(total_pnl_cents / 100, 4),
            "win_count": wins,
            "loss_count": losses,
            "win_rate": round(wins / total_trades, 4) if total_trades > 0 else None,
        }

    # ------------------------------------------------------------------
    # Market concentration
    # ------------------------------------------------------------------

    def market_concentration(self) -> pd.DataFrame:
        """Return fill counts and cost basis grouped by ticker.

        Useful for answering: "which markets am I trading most?"

        Columns
        -------
        ticker, fill_count, total_contracts, total_cost_cents,
        total_cost_dollars
        """
        if not self.fills:
            return pd.DataFrame(
                columns=["ticker", "fill_count", "total_contracts", "total_cost_cents", "total_cost_dollars"]
            )

        rows = []
        by_ticker: dict[str, list[Fill]] = {}
        for f in self.fills:
            by_ticker.setdefault(f.ticker, []).append(f)

        for ticker, ticker_fills in sorted(by_ticker.items()):
            total_contracts = sum(f.contracts for f in ticker_fills)
            total_cost = sum(f.price * f.contracts for f in ticker_fills)
            rows.append(
                {
                    "ticker": ticker,
                    "fill_count": len(ticker_fills),
                    "total_contracts": total_contracts,
                    "total_cost_cents": total_cost,
                    "total_cost_dollars": round(total_cost / 100, 2),
                }
            )

        df = pd.DataFrame(rows)
        return df.sort_values("fill_count", ascending=False).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Historical PnL
    # ------------------------------------------------------------------

    def historical_pnl(self) -> pd.DataFrame:
        """Return a cumulative realised PnL curve from closed position history.

        Rows are sorted by closed time.  The ``cumulative_pnl_dollars`` column
        is suitable for plotting as a running PnL chart.

        Columns
        -------
        closed_time, ticker, side, contracts, avg_entry_price,
        result, realised_pnl_cents, realised_pnl_dollars,
        cumulative_pnl_dollars
        """
        rows = []
        for pos, result in self.closed_positions:
            pnl = pos.realised_pnl(result)
            # Use the most recent fill time as proxy for close time
            close_time: datetime | None = None
            if pos.fills:
                close_time = max(f.timestamp for f in pos.fills)
            rows.append(
                {
                    "closed_time": close_time,
                    "ticker": pos.ticker,
                    "side": pos.side,
                    "contracts": pos.contracts,
                    "avg_entry_price": round(pos.avg_entry_price, 2),
                    "result": result,
                    "realised_pnl_cents": round(pnl, 2),
                    "realised_pnl_dollars": round(pnl / 100, 4),
                }
            )

        if not rows:
            return pd.DataFrame(
                columns=[
                    "closed_time",
                    "ticker",
                    "side",
                    "contracts",
                    "avg_entry_price",
                    "result",
                    "realised_pnl_cents",
                    "realised_pnl_dollars",
                    "cumulative_pnl_dollars",
                ]
            )

        df = pd.DataFrame(rows)
        df = df.sort_values("closed_time").reset_index(drop=True)
        df["cumulative_pnl_dollars"] = df["realised_pnl_dollars"].cumsum().round(4)
        return df

    # ------------------------------------------------------------------
    # Trades by hour to close
    # ------------------------------------------------------------------

    def trades_by_hour_to_close(
        self,
        close_time_map: dict[str, datetime],
    ) -> pd.DataFrame:
        """Bucket fills by hours-remaining-to-close at the time of the fill.

        Parameters
        ----------
        close_time_map:
            ``{ticker: close_time}`` mapping, e.g. from market metadata.

        Columns
        -------
        hours_bucket, fill_count, total_contracts, total_cost_cents
        """
        buckets: dict[str, dict] = {}

        for fill in self.fills:
            close_time = close_time_map.get(fill.ticker)
            if close_time is None:
                continue
            delta_hours = (close_time - fill.timestamp).total_seconds() / 3600.0
            if delta_hours < 0:
                label = "post-close"
            elif delta_hours < 1:
                label = "0-1h"
            elif delta_hours < 6:
                label = "1-6h"
            elif delta_hours < 24:
                label = "6-24h"
            elif delta_hours < 72:
                label = "1-3d"
            elif delta_hours < 168:
                label = "3-7d"
            else:
                label = "7d+"

            b = buckets.setdefault(
                label,
                {"fill_count": 0, "total_contracts": 0, "total_cost_cents": 0},
            )
            b["fill_count"] += 1
            b["total_contracts"] += fill.contracts
            b["total_cost_cents"] += fill.price * fill.contracts

        order = ["0-1h", "1-6h", "6-24h", "1-3d", "3-7d", "7d+", "post-close"]
        rows = []
        for label in order:
            if label in buckets:
                rows.append({"hours_bucket": label, **buckets[label]})

        return (
            pd.DataFrame(rows)
            if rows
            else pd.DataFrame(columns=["hours_bucket", "fill_count", "total_contracts", "total_cost_cents"])
        )
