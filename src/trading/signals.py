"""Signal engine: produces taker-limit prices from the live market feed.

Strategy – Mean Reversion
--------------------------
For each market the engine maintains a rolling window of mid-prices.  When the
current mid deviates by more than *threshold* standard deviations from the
rolling mean it emits a ``TakerSignal`` with a limit price set *inside* the
current spread (aggressing toward fair value):

* price > mean + threshold * std  →  sell YES at  (yes_bid + 1)  (fade the high)
* price < mean - threshold * std  →  buy  YES at  (yes_ask - 1)  (fade the low)

The limit prices are clamped to [1, 99] and must sit inside the current spread
to ensure the order will queue at a favourable level rather than cross
immediately at a worse price.

Usage
-----
    from src.trading.signals import MeanReversionSignal

    engine = MeanReversionSignal(lookback=20, threshold=2.0)

    for snapshots in feed.stream():
        signals = engine.update(snapshots)
        for sig in signals:
            print(sig)
"""

from __future__ import annotations

from collections import deque

from src.trading.models import MarketSnapshot, TakerSignal


class MeanReversionSignal:
    """Emit taker-limit signals when price deviates from its rolling mean.

    Parameters
    ----------
    lookback:
        Number of price observations used to compute the rolling mean/std
        (default 20).
    threshold:
        Z-score magnitude that must be exceeded to generate a signal
        (default 2.0).
    min_spread:
        Minimum bid-ask spread required to attempt limit placement (default 2 ¢).
        If the spread is too tight there is no room to quote inside it.
    """

    def __init__(
        self,
        lookback: int = 20,
        threshold: float = 2.0,
        min_spread: int = 2,
    ):
        self.lookback = lookback
        self.threshold = threshold
        self.min_spread = min_spread
        # deque per ticker storing mid-price history
        self._history: dict[str, deque[float]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, snapshots: list[MarketSnapshot]) -> list[TakerSignal]:
        """Update price history with *snapshots* and return any new signals."""
        signals: list[TakerSignal] = []
        for snap in snapshots:
            mid = snap.mid_price
            if mid is None:
                continue
            self._push(snap.ticker, mid)
            sig = self._evaluate(snap)
            if sig is not None:
                signals.append(sig)
        return signals

    def reset(self, ticker: str | None = None) -> None:
        """Clear price history for one ticker or all tickers."""
        if ticker is None:
            self._history.clear()
        else:
            self._history.pop(ticker, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _push(self, ticker: str, price: float) -> None:
        if ticker not in self._history:
            self._history[ticker] = deque(maxlen=self.lookback)
        self._history[ticker].append(price)

    def _evaluate(self, snap: MarketSnapshot) -> TakerSignal | None:
        history = self._history.get(snap.ticker)
        if history is None or len(history) < self.lookback:
            return None

        prices = list(history)
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        std = variance**0.5
        if std < 0.5:
            # Market is essentially flat; skip to avoid spurious signals
            return None

        mid = snap.mid_price
        if mid is None:
            return None

        z = (mid - mean) / std

        if abs(z) < self.threshold:
            return None

        # Require a visible spread to place inside
        if snap.spread is None or snap.spread < self.min_spread:
            return None

        if z > self.threshold:
            # Price is high → sell YES at yes_bid + 1 (just inside ask)
            limit_price = max(1, min(99, (snap.yes_bid or 1) + 1))
            return TakerSignal(
                ticker=snap.ticker,
                side="yes",
                action="sell",
                limit_price=limit_price,
                current_price=mid,
                z_score=round(z, 3),
                timestamp=snap.timestamp,
            )
        else:
            # Price is low → buy YES at yes_ask - 1 (just inside bid)
            limit_price = max(1, min(99, (snap.yes_ask or 99) - 1))
            return TakerSignal(
                ticker=snap.ticker,
                side="yes",
                action="buy",
                limit_price=limit_price,
                current_price=mid,
                z_score=round(z, 3),
                timestamp=snap.timestamp,
            )
