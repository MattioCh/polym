"""Core data models for the trading system."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketSnapshot:
    """A point-in-time price snapshot for a single market."""

    ticker: str
    yes_bid: int | None
    yes_ask: int | None
    no_bid: int | None
    no_ask: int | None
    last_price: int | None
    timestamp: datetime

    @property
    def mid_price(self) -> float | None:
        """Mid-point of yes bid/ask spread, or last price as fallback."""
        if self.yes_bid is not None and self.yes_ask is not None:
            return (self.yes_bid + self.yes_ask) / 2.0
        return float(self.last_price) if self.last_price is not None else None

    @property
    def spread(self) -> int | None:
        """Yes ask minus yes bid in cents."""
        if self.yes_bid is not None and self.yes_ask is not None:
            return self.yes_ask - self.yes_bid
        return None


@dataclass
class TakerSignal:
    """A trading signal emitted by the signal engine.

    A signal says: "at this ticker, place a taker order to buy/sell *side*
    at *limit_price* cents; the current mid-price is *current_price*."
    When the market touches *limit_price* you are willing to trade.
    """

    ticker: str
    side: str  # 'yes' or 'no'
    action: str  # 'buy' or 'sell'
    limit_price: int  # cents (1-99)
    current_price: float
    z_score: float
    timestamp: datetime


@dataclass
class Order:
    """A resting or filled order."""

    order_id: str
    ticker: str
    side: str  # 'yes' or 'no'
    action: str  # 'buy' or 'sell'
    contracts: int
    limit_price: int  # cents (1-99)
    status: str  # 'pending' | 'resting' | 'filled' | 'partially_filled' | 'cancelled'
    created_time: datetime
    filled_price: int | None = None
    filled_contracts: int = 0
    filled_time: datetime | None = None
    rationale: str = ""


@dataclass
class Fill:
    """A single execution event (trade fill)."""

    fill_id: str
    order_id: str
    ticker: str
    side: str
    action: str
    contracts: int
    price: int  # cents
    timestamp: datetime


@dataclass
class Position:
    """An open trading position in a single market.

    Prices are in cents (1-99). PnL calculations assume each contract
    pays $1 (100 cents) if the outcome resolves in favour of the holder.
    """

    ticker: str
    side: str  # 'yes' or 'no'
    contracts: int
    avg_entry_price: float  # cents
    opened_time: datetime
    rationale: str = ""
    fills: list[Fill] = field(default_factory=list)

    @property
    def cost_basis(self) -> float:
        """Total amount paid for the position in cents."""
        return self.avg_entry_price * self.contracts

    def unrealised_pnl(self, current_price: int) -> float:
        """Unrealised PnL in cents given a current yes-price.

        For a YES position: PnL = (current_price - avg_entry_price) * contracts
        For a NO  position: PnL = ((100 - current_price) - avg_entry_price) * contracts
        """
        if self.side == "yes":
            return (current_price - self.avg_entry_price) * self.contracts
        else:
            return ((100 - current_price) - self.avg_entry_price) * self.contracts

    def realised_pnl(self, result: str) -> float:
        """Realised PnL in cents on market resolution.

        result: 'yes' or 'no'
        """
        payout = 100 if result == self.side else 0
        return (payout - self.avg_entry_price) * self.contracts

    def add_fill(self, fill: Fill) -> None:
        """Update position with a new fill, recalculating average entry price."""
        total_cost = self.avg_entry_price * self.contracts + fill.price * fill.contracts
        self.contracts += fill.contracts
        self.avg_entry_price = total_cost / self.contracts
        self.fills.append(fill)
