"""Core data models for the trading module."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketSnapshot:
    """A point-in-time snapshot of a market's best prices.

    Prices are in cents (1-99). ``None`` means no quote on that side.
    """

    ticker: str
    title: str
    yes_bid: int | None
    yes_ask: int | None
    no_bid: int | None
    no_ask: int | None
    last_price: int | None
    open_interest: int
    timestamp: datetime

    @property
    def yes_mid(self) -> float | None:
        """Mid-price for Yes contracts (cents), or None if either side is missing."""
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return (self.yes_bid + self.yes_ask) / 2.0

    @property
    def spread(self) -> int | None:
        """Bid-ask spread for Yes contracts (cents), or None if either side is missing."""
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return self.yes_ask - self.yes_bid


@dataclass
class Position:
    """An open trading position in a prediction market.

    ``avg_entry_price`` and ``current_price`` are in cents (1-99).
    ``quantity`` is the number of contracts held.
    """

    ticker: str
    title: str
    side: str  # "yes" or "no"
    quantity: int
    avg_entry_price: int  # cents
    entry_time: datetime
    rationale: str = ""
    current_price: int | None = None

    @property
    def cost_basis_cents(self) -> int:
        """Total cost paid for this position in cents."""
        return self.avg_entry_price * self.quantity

    @property
    def unrealized_pnl_cents(self) -> float | None:
        """Unrealized P&L in cents (positive = profit)."""
        if self.current_price is None:
            return None
        return (self.current_price - self.avg_entry_price) * self.quantity

    @property
    def unrealized_pnl_usd(self) -> float | None:
        """Unrealized P&L in USD."""
        pnl = self.unrealized_pnl_cents
        return pnl / 100.0 if pnl is not None else None


@dataclass
class Order:
    """A limit order to be submitted or that has been submitted.

    Prices are in cents (1-99).
    """

    ticker: str
    side: str  # "yes" or "no"
    action: str  # "buy" or "sell"
    quantity: int
    price: int  # limit price in cents
    order_id: str | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    fill_price: int | None = None
    rationale: str = ""

    @property
    def notional_cents(self) -> int:
        """Worst-case cost of this order in cents."""
        return self.price * self.quantity


@dataclass
class Fill:
    """A confirmed trade fill (order execution).

    Prices are in cents (1-99).
    """

    fill_id: str
    order_id: str
    ticker: str
    side: str  # "yes" or "no"
    action: str  # "buy" or "sell"
    quantity: int
    price: int  # fill price in cents
    timestamp: datetime

    @property
    def notional_usd(self) -> float:
        """Value of this fill in USD."""
        return self.price * self.quantity / 100.0


@dataclass
class FeedRecord:
    """A single entry recorded from the market feed, for later orderbook analysis."""

    ticker: str
    yes_bid: int | None
    yes_ask: int | None
    no_bid: int | None
    no_ask: int | None
    last_price: int | None
    open_interest: int
    timestamp: datetime

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "yes_bid": self.yes_bid,
            "yes_ask": self.yes_ask,
            "no_bid": self.no_bid,
            "no_ask": self.no_ask,
            "last_price": self.last_price,
            "open_interest": self.open_interest,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class PortfolioSummary:
    """Aggregated portfolio-level statistics."""

    total_positions: int
    total_cost_usd: float
    total_unrealized_pnl_usd: float
    realized_pnl_usd: float
    positions: list[Position] = field(default_factory=list)
    most_traded_tickers: list[tuple[str, int]] = field(default_factory=list)

    @property
    def total_pnl_usd(self) -> float:
        return self.total_unrealized_pnl_usd + self.realized_pnl_usd
