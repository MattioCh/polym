"""Pre-trade checks and order execution.

Two classes are provided:

PreTradeChecker
    Validates that a proposed order is safe to submit:
    * sufficient account balance
    * live price is still consistent with the signal (price hasn't moved away)
    * basic sanity checks (contracts > 0, price in range)

OrderExecutor
    Submits, tracks, and cancels orders via the Kalshi trading API.
    Pass ``paper=True`` (default) to run in simulation mode without
    sending real orders to the exchange.

Usage
-----
    from src.trading.executor import PreTradeChecker, OrderExecutor
    from src.trading.notifications import TradeNotifier

    notifier = TradeNotifier()
    checker  = PreTradeChecker(client)
    executor = OrderExecutor(client, notifier, paper=True)

    signal = ...  # TakerSignal from the signal engine
    snapshot = ...  # MarketSnapshot for the same ticker

    ok, reason = checker.validate(signal, snapshot, contracts=10)
    if ok:
        order = executor.submit(signal, contracts=10, rationale="mean-rev z=2.3")
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.trading.models import Order, TakerSignal

if TYPE_CHECKING:
    from src.trading.models import MarketSnapshot
    from src.trading.notifications import TradeNotifier


# ---------------------------------------------------------------------------
# Pre-trade validation
# ---------------------------------------------------------------------------


class PreTradeChecker:
    """Validates proposed orders before submission.

    Parameters
    ----------
    client:
        A ``KalshiTradingClient`` (or any object with
        ``get_balance() -> int`` returning available balance in cents).
        Pass ``None`` to skip balance checks (useful for backtesting).
    max_position_cost:
        Maximum cost per individual order in cents (default $500 = 50 000¢).
    max_price_slip:
        Maximum cents the live mid-price may have moved from the signal's
        *current_price* before the order is rejected (default 3¢).
    """

    def __init__(
        self,
        client=None,
        max_position_cost: int = 50_000,
        max_price_slip: int = 3,
    ):
        self.client = client
        self.max_position_cost = max_position_cost
        self.max_price_slip = max_price_slip

    def validate(
        self,
        signal: TakerSignal,
        snapshot: MarketSnapshot,
        contracts: int,
    ) -> tuple[bool, str]:
        """Return ``(True, "")`` if the order is safe to submit.

        Returns ``(False, reason)`` with a human-readable explanation if any
        check fails.
        """
        # Basic sanity
        if contracts <= 0:
            return False, f"contracts must be positive, got {contracts}"
        if not (1 <= signal.limit_price <= 99):
            return False, f"limit_price {signal.limit_price} out of range [1, 99]"

        # Price staleness check
        live_mid = snapshot.mid_price
        if live_mid is not None:
            slip = abs(live_mid - signal.current_price)
            if slip > self.max_price_slip:
                return False, (
                    f"price moved too far: signal mid={signal.current_price:.1f}¢, "
                    f"live mid={live_mid:.1f}¢, slip={slip:.1f}¢ > {self.max_price_slip}¢"
                )

        # Cost check
        cost = signal.limit_price * contracts
        if cost > self.max_position_cost:
            return False, (f"order cost {cost}¢ exceeds max_position_cost {self.max_position_cost}¢")

        # Balance check (skipped if no client)
        if self.client is not None:
            try:
                balance = self.client.get_balance()
                if balance < cost:
                    return False, (f"insufficient balance: need {cost}¢, have {balance}¢")
            except Exception as exc:
                return False, f"balance check failed: {exc}"

        return True, ""


# ---------------------------------------------------------------------------
# Order executor
# ---------------------------------------------------------------------------


class OrderExecutor:
    """Submit, track, and cancel orders.

    Parameters
    ----------
    client:
        ``KalshiTradingClient`` for live trading, or ``None`` for paper mode.
    notifier:
        ``TradeNotifier`` for announcing fills and order events.
    paper:
        If ``True`` (default), simulate fills locally without calling the API.
        Orders are considered immediately filled at *limit_price*.
    """

    def __init__(
        self,
        client=None,
        notifier: TradeNotifier | None = None,
        paper: bool = True,
    ):
        self.client = client
        self.notifier = notifier
        self.paper = paper
        self._orders: dict[str, Order] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(
        self,
        signal: TakerSignal,
        contracts: int,
        rationale: str = "",
    ) -> Order:
        """Submit an order derived from *signal*.

        In paper mode the order is immediately filled at ``signal.limit_price``.
        In live mode the order is sent to the Kalshi API and left resting.

        Returns the created ``Order`` object.
        """
        now = datetime.now(tz=timezone.utc)
        order_id = str(uuid.uuid4())

        order = Order(
            order_id=order_id,
            ticker=signal.ticker,
            side=signal.side,
            action=signal.action,
            contracts=contracts,
            limit_price=signal.limit_price,
            status="pending",
            created_time=now,
            rationale=rationale,
        )

        if self.paper:
            self._simulate_fill(order)
        else:
            self._submit_live(order)

        self._orders[order_id] = order

        if self.notifier:
            self.notifier.on_order_submitted(order)
            if order.status == "filled":
                self.notifier.on_fill(order)

        return order

    def cancel(self, order_id: str) -> bool:
        """Cancel a resting order.  Returns ``True`` on success."""
        order = self._orders.get(order_id)
        if order is None:
            return False
        if order.status not in ("pending", "resting"):
            return False

        if not self.paper and self.client is not None:
            try:
                self.client.cancel_order(order_id)
            except Exception as exc:
                print(f"[executor] cancel failed: {exc}")
                return False

        order.status = "cancelled"
        if self.notifier:
            self.notifier.on_order_cancelled(order)
        return True

    def get_order(self, order_id: str) -> Order | None:
        """Return the tracked order for *order_id*, or ``None``."""
        return self._orders.get(order_id)

    @property
    def open_orders(self) -> list[Order]:
        """All orders currently resting (not filled/cancelled)."""
        return [o for o in self._orders.values() if o.status in ("pending", "resting")]

    @property
    def filled_orders(self) -> list[Order]:
        """All fully filled orders."""
        return [o for o in self._orders.values() if o.status == "filled"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _simulate_fill(self, order: Order) -> None:
        """Immediately fill the order at limit_price (paper trading)."""
        now = datetime.now(tz=timezone.utc)
        order.status = "filled"
        order.filled_price = order.limit_price
        order.filled_contracts = order.contracts
        order.filled_time = now

    def _submit_live(self, order: Order) -> None:
        """Send the order to Kalshi and update status."""
        if self.client is None:
            raise RuntimeError("No Kalshi client configured for live trading")
        try:
            result = self.client.create_order(
                ticker=order.ticker,
                side=order.side,
                action=order.action,
                contracts=order.contracts,
                limit_price=order.limit_price,
            )
            order.status = result.get("status", "resting")
            order.order_id = result.get("order_id", order.order_id)
        except Exception as exc:
            order.status = "cancelled"
            raise RuntimeError(f"[executor] live order failed: {exc}") from exc
