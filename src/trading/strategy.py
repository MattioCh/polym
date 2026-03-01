"""Taker price strategies.

A strategy inspects a :class:`~src.trading.models.MarketSnapshot` and decides
whether—and at what price—to place a taker order.

Example usage::

    from src.trading.strategy import ThresholdCrossStrategy

    strategy = ThresholdCrossStrategy(target_price=40, side="yes", action="buy")
    order = strategy.evaluate(snapshot)
    if order is not None:
        # submit order ...
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.trading.models import MarketSnapshot, Order


class TakerPriceStrategy(ABC):
    """Base class for taker price strategies.

    Subclasses implement :meth:`evaluate` which returns an :class:`~src.trading.models.Order`
    if the strategy decides to trade, or ``None`` otherwise.
    """

    @abstractmethod
    def evaluate(self, snapshot: MarketSnapshot) -> Order | None:
        """Decide whether to trade based on the current market snapshot.

        Args:
            snapshot: The latest market snapshot.

        Returns:
            An :class:`~src.trading.models.Order` if the strategy fires, else ``None``.
        """


class ThresholdCrossStrategy(TakerPriceStrategy):
    """Trade when the market price crosses a static threshold.

    For a **buy** order: fires when ``ask <= target_price``.
    For a **sell** order: fires when ``bid >= target_price``.

    Args:
        ticker: Market ticker to watch.
        side: ``"yes"`` or ``"no"``.
        action: ``"buy"`` or ``"sell"``.
        target_price: Price threshold in cents.
        quantity: Number of contracts per order.
        rationale: Optional text explaining the trade rationale.
    """

    def __init__(
        self,
        ticker: str,
        side: str,
        action: str,
        target_price: int,
        quantity: int = 1,
        rationale: str = "",
    ) -> None:
        self.ticker = ticker
        self.side = side
        self.action = action
        self.target_price = target_price
        self.quantity = quantity
        self.rationale = rationale

    def evaluate(self, snapshot: MarketSnapshot) -> Order | None:
        from src.trading.models import Order

        if snapshot.ticker != self.ticker:
            return None

        if self.action == "buy":
            ask = snapshot.yes_ask if self.side == "yes" else snapshot.no_ask
            if ask is not None and ask <= self.target_price:
                return Order(
                    ticker=self.ticker,
                    side=self.side,
                    action="buy",
                    quantity=self.quantity,
                    price=ask,
                    rationale=self.rationale,
                )
        elif self.action == "sell":
            bid = snapshot.yes_bid if self.side == "yes" else snapshot.no_bid
            if bid is not None and bid >= self.target_price:
                return Order(
                    ticker=self.ticker,
                    side=self.side,
                    action="sell",
                    quantity=self.quantity,
                    price=bid,
                    rationale=self.rationale,
                )
        return None


class MidpointStrategy(TakerPriceStrategy):
    """Post a limit order at the current midpoint, adjusted by a bias.

    The strategy fires whenever the market has a valid two-sided quote.
    The limit price is ``midpoint + bias`` (clamped to 1-99).

    Args:
        ticker: Market ticker to watch.
        side: ``"yes"`` or ``"no"``.
        action: ``"buy"`` or ``"sell"``.
        quantity: Number of contracts per order.
        bias: Cents added to the midpoint (negative = more aggressive buy).
        rationale: Optional text explaining the trade rationale.
    """

    def __init__(
        self,
        ticker: str,
        side: str,
        action: str,
        quantity: int = 1,
        bias: int = 0,
        rationale: str = "",
    ) -> None:
        self.ticker = ticker
        self.side = side
        self.action = action
        self.quantity = quantity
        self.bias = bias
        self.rationale = rationale

    def evaluate(self, snapshot: MarketSnapshot) -> Order | None:
        from src.trading.models import Order

        if snapshot.ticker != self.ticker:
            return None

        mid = snapshot.yes_mid if self.side == "yes" else (
            (snapshot.no_bid + snapshot.no_ask) / 2.0
            if snapshot.no_bid is not None and snapshot.no_ask is not None
            else None
        )
        if mid is None:
            return None

        price = max(1, min(99, round(mid + self.bias)))
        return Order(
            ticker=self.ticker,
            side=self.side,
            action=self.action,
            quantity=self.quantity,
            price=price,
            rationale=self.rationale,
        )


class MeanReversionStrategy(TakerPriceStrategy):
    """Trade when price deviates from a rolling simple moving average by a threshold.

    The strategy maintains a sliding window of mid-price observations.
    When the current mid-price deviates from the rolling SMA by more than
    ``entry_threshold`` cents, it places a taker order back toward the mean.

    Args:
        ticker: Market ticker to watch.
        side: ``"yes"`` or ``"no"``.
        quantity: Contracts per order.
        window: Number of snapshots in the rolling SMA window.
        entry_threshold: Minimum deviation in cents before entering.
        rationale: Optional text explaining the trade rationale.
    """

    def __init__(
        self,
        ticker: str,
        side: str = "yes",
        quantity: int = 1,
        window: int = 20,
        entry_threshold: float = 3.0,
        rationale: str = "mean_reversion",
    ) -> None:
        self.ticker = ticker
        self.side = side
        self.quantity = quantity
        self.window = window
        self.entry_threshold = entry_threshold
        self.rationale = rationale
        self._price_history: list[float] = []

    def _sma(self) -> float | None:
        """Compute the simple moving average of the price history."""
        if not self._price_history:
            return None
        return sum(self._price_history) / len(self._price_history)

    def evaluate(self, snapshot: MarketSnapshot) -> Order | None:
        from src.trading.models import Order

        if snapshot.ticker != self.ticker:
            return None

        mid = snapshot.yes_mid if self.side == "yes" else (
            (snapshot.no_bid + snapshot.no_ask) / 2.0
            if snapshot.no_bid is not None and snapshot.no_ask is not None
            else None
        )
        if mid is None:
            return None

        # Update history
        self._price_history.append(mid)
        if len(self._price_history) > self.window:
            self._price_history.pop(0)

        sma = self._sma()
        if sma is None or len(self._price_history) < self.window:
            return None

        deviation = mid - sma
        if abs(deviation) < self.entry_threshold:
            return None

        # If price is above SMA, sell (expect reversion downward)
        # If price is below SMA, buy (expect reversion upward)
        if deviation > 0:
            action = "sell"
            price = snapshot.yes_bid if self.side == "yes" else snapshot.no_bid
        else:
            action = "buy"
            price = snapshot.yes_ask if self.side == "yes" else snapshot.no_ask

        if price is None:
            return None

        return Order(
            ticker=self.ticker,
            side=self.side,
            action=action,
            quantity=self.quantity,
            price=price,
            rationale=f"{self.rationale} | dev={deviation:+.1f}¢ sma={sma:.1f}¢",
        )
