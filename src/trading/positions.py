"""Position manager: tracks open positions and computes unrealised PnL.

Usage
-----
    from src.trading.positions import PositionManager

    manager = PositionManager()

    # Open a position when an order fills
    for order in executor.filled_orders:
        manager.open_from_order(order, rationale="mean-rev z=2.3")

    # Generate a position report against live snapshots
    snapshot_map = {s.ticker: s for s in feed.snapshot()}
    report_df = manager.report(snapshot_map)
    print(report_df.to_string())
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.trading.models import Fill, Order, Position


class PositionManager:
    """Track open positions and calculate current PnL.

    Positions are keyed by ``(ticker, side)`` so a YES position and a
    NO position in the same market are treated separately (they will
    net against each other on resolution, but are tracked independently
    for clarity).
    """

    def __init__(self) -> None:
        self._positions: dict[tuple[str, str], Position] = {}

    # ------------------------------------------------------------------
    # Mutating operations
    # ------------------------------------------------------------------

    def open_from_order(self, order: Order, rationale: str = "") -> Position:
        """Create or update a position from a filled order.

        If a position already exists for the same ``(ticker, side)`` the
        fill is merged in (average entry price is recalculated).

        Parameters
        ----------
        order:
            A filled ``Order`` (``order.status == "filled"``).
        rationale:
            Free-text description of why this trade was entered.

        Returns
        -------
        Position
            The updated (or newly created) position.
        """
        if order.status != "filled" or order.filled_contracts == 0:
            raise ValueError(f"order {order.order_id} is not filled")

        key = (order.ticker, order.side)
        fill = Fill(
            fill_id=f"fill-{order.order_id}",
            order_id=order.order_id,
            ticker=order.ticker,
            side=order.side,
            action=order.action,
            contracts=order.filled_contracts,
            price=order.filled_price or order.limit_price,
            timestamp=order.filled_time or datetime.now(tz=timezone.utc),
        )

        if key not in self._positions:
            pos = Position(
                ticker=order.ticker,
                side=order.side,
                contracts=fill.contracts,
                avg_entry_price=float(fill.price),
                opened_time=fill.timestamp,
                rationale=rationale or order.rationale,
                fills=[fill],
            )
            self._positions[key] = pos
        else:
            pos = self._positions[key]
            pos.add_fill(fill)
            if rationale:
                pos.rationale = rationale

        return pos

    def close_position(self, ticker: str, side: str) -> Position | None:
        """Remove and return the position for ``(ticker, side)``."""
        return self._positions.pop((ticker, side), None)

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    @property
    def open_positions(self) -> list[Position]:
        """All currently tracked positions."""
        return list(self._positions.values())

    def get(self, ticker: str, side: str) -> Position | None:
        """Return the position for ``(ticker, side)`` or ``None``."""
        return self._positions.get((ticker, side))

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def report(
        self,
        snapshot_map: dict[str, object] | None = None,
    ) -> pd.DataFrame:
        """Return a DataFrame summarising all open positions.

        Parameters
        ----------
        snapshot_map:
            Optional ``{ticker: MarketSnapshot}`` mapping used to attach
            the live mid-price and unrealised PnL to each row.

        Columns
        -------
        ticker, side, contracts, avg_entry_price, cost_basis,
        current_price, unrealised_pnl_cents, unrealised_pnl_dollars,
        rationale, opened_time
        """
        rows = []
        for pos in self._positions.values():
            snap = (snapshot_map or {}).get(pos.ticker)
            current_price: float | None = snap.mid_price if snap is not None else None  # type: ignore[union-attr]
            upnl_cents: float | None = None
            if current_price is not None:
                upnl_cents = pos.unrealised_pnl(int(round(current_price)))

            rows.append(
                {
                    "ticker": pos.ticker,
                    "side": pos.side,
                    "contracts": pos.contracts,
                    "avg_entry_price": round(pos.avg_entry_price, 2),
                    "cost_basis": round(pos.cost_basis, 2),
                    "current_price": round(current_price, 2) if current_price is not None else None,
                    "unrealised_pnl_cents": round(upnl_cents, 2) if upnl_cents is not None else None,
                    "unrealised_pnl_dollars": (round(upnl_cents / 100, 4) if upnl_cents is not None else None),
                    "rationale": pos.rationale,
                    "opened_time": pos.opened_time,
                }
            )

        return pd.DataFrame(rows)
