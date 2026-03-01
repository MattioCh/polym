"""Trade notification system.

Notifiers are called whenever a fill is confirmed or an order event occurs.
Multiple notifiers can be composed via ``CompositeNotifier``.

Example usage::

    from src.trading.notifier import PrintNotifier
    from src.trading.models import Fill
    from datetime import datetime, timezone

    notifier = PrintNotifier()
    fill = Fill(
        fill_id="abc",
        order_id="xyz",
        ticker="INXD-24DEC31-B6000",
        side="yes",
        action="buy",
        quantity=10,
        price=42,
        timestamp=datetime.now(timezone.utc),
    )
    notifier.on_fill(fill)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.trading.models import Fill, Order


class TradeNotifier(ABC):
    """Base class for trade event notifications."""

    @abstractmethod
    def on_fill(self, fill: Fill) -> None:
        """Called when an order fill is confirmed.

        Args:
            fill: The confirmed fill details.
        """

    def on_order_submitted(self, order: Order) -> None:  # noqa: B027
        """Called when an order is submitted (optional hook).

        Args:
            order: The submitted order.
        """

    def on_order_cancelled(self, order: Order) -> None:  # noqa: B027
        """Called when an order is cancelled (optional hook).

        Args:
            order: The cancelled order.
        """


class PrintNotifier(TradeNotifier):
    """Notifier that prints trade events to stdout."""

    def on_fill(self, fill: Fill) -> None:
        print(
            f"[FILL] {fill.timestamp.isoformat()} | {fill.ticker} | "
            f"{fill.action.upper()} {fill.quantity}x {fill.side.upper()} "
            f"@ {fill.price}¢ (${fill.notional_usd:.2f})"
        )

    def on_order_submitted(self, order: Order) -> None:
        print(
            f"[ORDER] {order.ticker} | {order.action.upper()} {order.quantity}x "
            f"{order.side.upper()} @ {order.price}¢ | id={order.order_id}"
        )

    def on_order_cancelled(self, order: Order) -> None:
        print(f"[CANCEL] {order.ticker} | id={order.order_id}")


class CallbackNotifier(TradeNotifier):
    """Notifier that invokes user-supplied callbacks for each event type.

    Args:
        on_fill_cb: Callable invoked with a :class:`~src.trading.models.Fill`.
        on_order_submitted_cb: Optional callable invoked on order submission.
        on_order_cancelled_cb: Optional callable invoked on order cancellation.
    """

    def __init__(
        self,
        on_fill_cb: Callable[[Fill], None],
        on_order_submitted_cb: Callable[[Order], None] | None = None,
        on_order_cancelled_cb: Callable[[Order], None] | None = None,
    ) -> None:
        self._on_fill_cb = on_fill_cb
        self._on_order_submitted_cb = on_order_submitted_cb
        self._on_order_cancelled_cb = on_order_cancelled_cb

    def on_fill(self, fill: Fill) -> None:
        self._on_fill_cb(fill)

    def on_order_submitted(self, order: Order) -> None:
        if self._on_order_submitted_cb is not None:
            self._on_order_submitted_cb(order)

    def on_order_cancelled(self, order: Order) -> None:
        if self._on_order_cancelled_cb is not None:
            self._on_order_cancelled_cb(order)


class CompositeNotifier(TradeNotifier):
    """Fan-out notifier that forwards events to multiple child notifiers.

    Args:
        notifiers: One or more :class:`TradeNotifier` instances.
    """

    def __init__(self, *notifiers: TradeNotifier) -> None:
        self._notifiers = list(notifiers)

    def add(self, notifier: TradeNotifier) -> None:
        """Add a notifier at runtime."""
        self._notifiers.append(notifier)

    def on_fill(self, fill: Fill) -> None:
        for n in self._notifiers:
            n.on_fill(fill)

    def on_order_submitted(self, order: Order) -> None:
        for n in self._notifiers:
            n.on_order_submitted(order)

    def on_order_cancelled(self, order: Order) -> None:
        for n in self._notifiers:
            n.on_order_cancelled(order)
