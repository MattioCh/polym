"""Portfolio management and P&L reporting.

Tracks open positions, realized P&L from fills, and provides both per-market
and portfolio-level summaries.

Example usage::

    from src.trading.portfolio import Portfolio

    portfolio = Portfolio()
    portfolio.add_position(my_position)
    portfolio.record_fill(my_fill)

    # Update prices from latest feed snapshot batch
    portfolio.update_prices(snapshots)

    # Print a human-readable report
    print(portfolio.report())

    # Get a DataFrame for programmatic access
    df = portfolio.position_dataframe()
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

    from src.trading.models import Fill, MarketSnapshot, PortfolioSummary, Position


class Portfolio:
    """Manages open positions, fill history, and P&L.

    All monetary values are tracked in **cents** internally and converted to
    USD ($) in output methods.
    """

    def __init__(self) -> None:
        self._positions: dict[str, Position] = {}  # ticker -> Position
        self._fills: list[Fill] = []
        self._realized_pnl_cents: float = 0.0
        self._trade_counts: dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def add_position(self, position: Position) -> None:
        """Add or replace a position for a ticker.

        Args:
            position: The position to add.
        """
        self._positions[position.ticker] = position

    def remove_position(self, ticker: str) -> None:
        """Remove the position for the given ticker (e.g. after settlement).

        Args:
            ticker: The market ticker.
        """
        self._positions.pop(ticker, None)

    def record_fill(self, fill: Fill) -> None:
        """Record an executed fill and update realized P&L for closing trades.

        For simplicity, a **sell** fill is treated as closing the position and
        realizing P&L relative to the average entry price.

        Args:
            fill: The confirmed fill.
        """
        self._fills.append(fill)
        self._trade_counts[fill.ticker] += 1

        if fill.action == "sell" and fill.ticker in self._positions:
            pos = self._positions[fill.ticker]
            realized = (fill.price - pos.avg_entry_price) * fill.quantity
            self._realized_pnl_cents += realized
            remaining = pos.quantity - fill.quantity
            if remaining <= 0:
                del self._positions[fill.ticker]
            else:
                pos.quantity = remaining

    def update_prices(self, snapshots: list[MarketSnapshot]) -> None:
        """Update the ``current_price`` on each open position from a feed batch.

        The current price is set to the mid-price of the position side if a
        two-sided quote is available, otherwise falls back to ``last_price``.

        Args:
            snapshots: Latest snapshots from the market feed.
        """
        snap_map = {s.ticker: s for s in snapshots}
        for ticker, pos in self._positions.items():
            snap = snap_map.get(ticker)
            if snap is None:
                continue
            if pos.side == "yes":
                mid = snap.yes_mid
                pos.current_price = round(mid) if mid is not None else snap.last_price
            else:
                if snap.no_bid is not None and snap.no_ask is not None:
                    pos.current_price = round((snap.no_bid + snap.no_ask) / 2.0)
                else:
                    # no_price = 100 - yes_price
                    pos.current_price = (100 - snap.last_price) if snap.last_price is not None else None

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def positions(self) -> list[Position]:
        """All open positions."""
        return list(self._positions.values())

    @property
    def fills(self) -> list[Fill]:
        """All recorded fills."""
        return list(self._fills)

    @property
    def realized_pnl_usd(self) -> float:
        """Total realized P&L in USD."""
        return self._realized_pnl_cents / 100.0

    @property
    def unrealized_pnl_usd(self) -> float:
        """Total unrealized P&L in USD across all open positions."""
        total = 0.0
        for pos in self._positions.values():
            pnl = pos.unrealized_pnl_usd
            if pnl is not None:
                total += pnl
        return total

    @property
    def total_pnl_usd(self) -> float:
        """Total P&L (realized + unrealized) in USD."""
        return self.realized_pnl_usd + self.unrealized_pnl_usd

    def most_traded(self, top_n: int = 10) -> list[tuple[str, int]]:
        """Return the most-traded tickers by fill count.

        Args:
            top_n: Number of tickers to return (default 10).

        Returns:
            List of ``(ticker, fill_count)`` sorted by fill count descending.
        """
        return sorted(self._trade_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]

    def summary(self) -> PortfolioSummary:
        """Return a :class:`~src.trading.models.PortfolioSummary` snapshot.

        Returns:
            Current portfolio summary.
        """
        from src.trading.models import PortfolioSummary

        total_cost = sum(p.cost_basis_cents for p in self._positions.values()) / 100.0
        return PortfolioSummary(
            total_positions=len(self._positions),
            total_cost_usd=total_cost,
            total_unrealized_pnl_usd=self.unrealized_pnl_usd,
            realized_pnl_usd=self.realized_pnl_usd,
            positions=self.positions,
            most_traded_tickers=self.most_traded(),
        )

    def position_dataframe(self) -> pd.DataFrame:
        """Return a DataFrame with one row per open position.

        Columns: ticker, title, side, quantity, avg_entry_price, current_price,
        unrealized_pnl_usd, cost_basis_usd, rationale, entry_time.

        Returns:
            DataFrame of open positions.
        """
        import pandas as pd

        rows = []
        for pos in self._positions.values():
            rows.append({
                "ticker": pos.ticker,
                "title": pos.title,
                "side": pos.side,
                "quantity": pos.quantity,
                "avg_entry_price": pos.avg_entry_price,
                "current_price": pos.current_price,
                "unrealized_pnl_usd": pos.unrealized_pnl_usd,
                "cost_basis_usd": pos.cost_basis_cents / 100.0,
                "rationale": pos.rationale,
                "entry_time": pos.entry_time,
            })
        return pd.DataFrame(rows)

    def historical_pnl_dataframe(self) -> pd.DataFrame:
        """Return a DataFrame of realized P&L over time, one row per fill.

        Columns: timestamp, ticker, action, side, quantity, price, notional_usd,
        cumulative_realized_pnl_usd.

        Returns:
            DataFrame of fill history.
        """
        import pandas as pd

        rows = []
        cumulative = 0.0
        for fill in self._fills:
            rows.append({
                "timestamp": fill.timestamp,
                "ticker": fill.ticker,
                "action": fill.action,
                "side": fill.side,
                "quantity": fill.quantity,
                "price": fill.price,
                "notional_usd": fill.notional_usd,
                "cumulative_realized_pnl_usd": cumulative,
            })
        return pd.DataFrame(rows)

    def trades_by_hour(self) -> pd.DataFrame:
        """Return fill counts grouped by hour of day (UTC).

        Useful for identifying when the portfolio is most active.

        Returns:
            DataFrame with columns ``hour`` (0-23) and ``fill_count``.
        """
        import pandas as pd

        if not self._fills:
            return pd.DataFrame(columns=["hour", "fill_count"])

        hours = [f.timestamp.hour for f in self._fills]
        series = pd.Series(hours, name="hour")
        counts = series.value_counts().sort_index().reset_index()
        counts.columns = ["hour", "fill_count"]
        return counts

    def report(self) -> str:
        """Return a human-readable portfolio report string.

        Returns:
            Multi-line string summarising positions and P&L.
        """
        lines: list[str] = [
            f"{'='*60}",
            f"  Portfolio Report  ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})",
            f"{'='*60}",
            f"  Open positions  : {len(self._positions)}",
            f"  Realized P&L    : ${self.realized_pnl_usd:+.2f}",
            f"  Unrealized P&L  : ${self.unrealized_pnl_usd:+.2f}",
            f"  Total P&L       : ${self.total_pnl_usd:+.2f}",
            "",
        ]

        if self._positions:
            lines.append("  Open Positions:")
            lines.append(f"  {'Ticker':<30} {'Side':<5} {'Qty':>5} {'Entry':>6} {'Cur':>6} {'uPnL':>8}")
            lines.append(f"  {'-'*64}")
            for pos in sorted(self._positions.values(), key=lambda p: p.ticker):
                cur = f"{pos.current_price}¢" if pos.current_price else "  ?"
                upnl = f"${pos.unrealized_pnl_usd:+.2f}" if pos.unrealized_pnl_usd is not None else "   ?"
                lines.append(
                    f"  {pos.ticker:<30} {pos.side:<5} {pos.quantity:>5} "
                    f"{pos.avg_entry_price:>4}¢ {cur:>5} {upnl:>8}"
                )
                if pos.rationale:
                    lines.append(f"    rationale: {pos.rationale}")

        if self._trade_counts:
            lines.append("")
            lines.append("  Most Traded Markets:")
            for ticker, count in self.most_traded(5):
                lines.append(f"    {ticker}: {count} fills")

        lines.append(f"{'='*60}")
        return "\n".join(lines)
