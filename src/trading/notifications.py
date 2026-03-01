"""Trade notifications: announce every fill event to the operator.

Usage
-----
    from src.trading.notifications import TradeNotifier

    notifier = TradeNotifier()
    notifier.on_fill(order)
    notifier.on_signal(signal)
    notifier.on_pre_trade_reject(order, reason)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.trading.models import Order, TakerSignal

logger = logging.getLogger(__name__)


class TradeNotifier:
    """Emit human-readable announcements for trading events.

    By default messages are printed to stdout **and** emitted via
    ``logging`` so they appear in any configured log file.

    Parameters
    ----------
    verbose:
        If ``False``, suppress console output (log output is unaffected).
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def on_fill(self, order: Order) -> None:
        """Announce that an order has been (fully) filled."""
        msg = (
            f"[FILL] {self._now()} | {order.ticker} | "
            f"{order.action.upper()} {order.filled_contracts} x {order.side.upper()} "
            f"@ {order.filled_price}¢ | order_id={order.order_id}"
        )
        if order.rationale:
            msg += f" | rationale: {order.rationale}"
        self._emit(msg)

    def on_partial_fill(self, order: Order) -> None:
        """Announce a partial fill."""
        msg = (
            f"[PARTIAL FILL] {self._now()} | {order.ticker} | "
            f"{order.action.upper()} {order.filled_contracts}/{order.contracts} x "
            f"{order.side.upper()} @ {order.filled_price}¢ | order_id={order.order_id}"
        )
        self._emit(msg)

    def on_order_submitted(self, order: Order) -> None:
        """Announce that an order has been sent to the exchange."""
        msg = (
            f"[ORDER] {self._now()} | {order.ticker} | "
            f"{order.action.upper()} {order.contracts} x {order.side.upper()} "
            f"limit={order.limit_price}¢ | order_id={order.order_id}"
        )
        self._emit(msg)

    def on_order_cancelled(self, order: Order) -> None:
        """Announce that an order has been cancelled."""
        msg = (
            f"[CANCEL] {self._now()} | {order.ticker} | "
            f"order_id={order.order_id} | filled={order.filled_contracts}/{order.contracts}"
        )
        self._emit(msg)

    def on_signal(self, signal: TakerSignal) -> None:
        """Announce a new taker signal from the signal engine."""
        msg = (
            f"[SIGNAL] {signal.timestamp.strftime('%H:%M:%S')} | {signal.ticker} | "
            f"{signal.action.upper()} {signal.side.upper()} "
            f"limit={signal.limit_price}¢ mid={signal.current_price:.1f}¢ "
            f"z={signal.z_score:+.2f}"
        )
        self._emit(msg)

    def on_pre_trade_reject(self, reason: str, ticker: str = "", detail: str = "") -> None:
        """Announce that a trade was blocked by the pre-trade checker."""
        msg = f"[REJECT] {self._now()} | {ticker} | {reason}"
        if detail:
            msg += f" | {detail}"
        self._emit(msg)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _emit(self, message: str) -> None:
        if self.verbose:
            print(message)
        logger.info(message)

    @staticmethod
    def _now() -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
