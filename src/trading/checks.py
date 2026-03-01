"""Pre-trade safety checks.

These checks must pass before any order is submitted to the exchange.
Raising :class:`PreTradeCheckError` aborts the order.

Example usage::

    from src.trading.checks import PreTradeChecks, PreTradeCheckError
    from src.trading.models import Order, MarketSnapshot
    from datetime import datetime, timezone

    order = Order(ticker="MKT-A", side="yes", action="buy", quantity=10, price=42)
    snapshot = MarketSnapshot(
        ticker="MKT-A", title="Test", yes_bid=40, yes_ask=44,
        no_bid=56, no_ask=60, last_price=42, open_interest=500,
        timestamp=datetime.now(timezone.utc),
    )
    try:
        PreTradeChecks.run_all(
            available_balance_cents=1000,
            order=order,
            snapshot=snapshot,
        )
    except PreTradeCheckError as exc:
        print(f"Order rejected: {exc}")
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.trading.models import MarketSnapshot, Order


class PreTradeCheckError(Exception):
    """Raised when a pre-trade safety check fails."""


class PreTradeChecks:
    """Collection of pre-trade safety checks."""

    @staticmethod
    def check_balance(available_balance_cents: int, order: Order) -> None:
        """Verify the account has sufficient balance to cover the order.

        Args:
            available_balance_cents: Available cash in the account (cents).
            order: The order to check.

        Raises:
            PreTradeCheckError: If the balance is insufficient.
        """
        required = order.notional_cents
        if available_balance_cents < required:
            raise PreTradeCheckError(
                f"Insufficient balance: need {required}¢ "
                f"but only {available_balance_cents}¢ available "
                f"(order: {order.action} {order.quantity}x {order.side} "
                f"@ {order.price}¢ on {order.ticker})"
            )

    @staticmethod
    def check_price(
        order: Order,
        snapshot: MarketSnapshot,
        tolerance_pct: float = 0.05,
    ) -> None:
        """Verify the order price is within ``tolerance_pct`` of the current best quote.

        For a buy order, the reference price is the current ask.
        For a sell order, the reference price is the current bid.

        Args:
            order: The order to check.
            snapshot: Current market snapshot.
            tolerance_pct: Maximum fractional deviation allowed (default 5%).

        Raises:
            PreTradeCheckError: If the price has moved beyond tolerance.
        """
        if order.side == "yes":
            ref_price = snapshot.yes_ask if order.action == "buy" else snapshot.yes_bid
        else:
            ref_price = snapshot.no_ask if order.action == "buy" else snapshot.no_bid

        if ref_price is None:
            raise PreTradeCheckError(
                f"No quote available for {order.ticker} {order.side} "
                f"({order.action} side) — cannot verify price."
            )

        deviation = abs(order.price - ref_price) / ref_price
        if deviation > tolerance_pct:
            raise PreTradeCheckError(
                f"Price out of tolerance on {order.ticker}: "
                f"order price {order.price}¢ deviates {deviation:.1%} "
                f"from current quote {ref_price}¢ "
                f"(tolerance {tolerance_pct:.1%})"
            )

    @staticmethod
    def check_quantity(order: Order, min_quantity: int = 1, max_quantity: int = 10_000) -> None:
        """Verify the order quantity is within sensible bounds.

        Args:
            order: The order to check.
            min_quantity: Minimum allowed quantity (default 1).
            max_quantity: Maximum allowed quantity (default 10,000).

        Raises:
            PreTradeCheckError: If the quantity is out of range.
        """
        if order.quantity < min_quantity:
            raise PreTradeCheckError(
                f"Order quantity {order.quantity} is below minimum {min_quantity}."
            )
        if order.quantity > max_quantity:
            raise PreTradeCheckError(
                f"Order quantity {order.quantity} exceeds maximum {max_quantity}."
            )

    @classmethod
    def run_all(
        cls,
        available_balance_cents: int,
        order: Order,
        snapshot: MarketSnapshot,
        price_tolerance_pct: float = 0.05,
        max_quantity: int = 10_000,
    ) -> None:
        """Run all pre-trade checks in sequence.

        Args:
            available_balance_cents: Available cash in the account (cents).
            order: The order to validate.
            snapshot: Current market snapshot for price verification.
            price_tolerance_pct: Max fractional price deviation (default 5%).
            max_quantity: Maximum allowed order quantity (default 10,000).

        Raises:
            PreTradeCheckError: On the first failing check.
        """
        cls.check_quantity(order, max_quantity=max_quantity)
        cls.check_balance(available_balance_cents, order)
        cls.check_price(order, snapshot, tolerance_pct=price_tolerance_pct)
