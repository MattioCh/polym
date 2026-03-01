"""Market feed scanner.

The feed polls market prices at a configurable interval and yields
:class:`~src.trading.models.MarketSnapshot` objects.

Example usage::

    from src.trading.feed import KalshiMarketFeed
    from src.indexers.kalshi.client import KalshiClient

    with KalshiClient() as client:
        feed = KalshiMarketFeed(client)
        for snapshot in feed.stream(["INXD-24DEC31-B6000"], interval=5.0):
            print(snapshot)
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Generator
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.indexers.kalshi.client import KalshiClient
    from src.trading.models import MarketSnapshot


class MarketFeed(ABC):
    """Abstract market feed that yields :class:`~src.trading.models.MarketSnapshot` objects."""

    @abstractmethod
    def scan(self, tickers: list[str]) -> list[MarketSnapshot]:
        """Fetch a single batch of snapshots for the given tickers.

        Args:
            tickers: List of market identifiers to fetch.

        Returns:
            List of snapshots, one per successfully fetched ticker.
        """

    def stream(
        self,
        tickers: list[str],
        interval: float = 5.0,
        max_polls: int | None = None,
    ) -> Generator[list[MarketSnapshot], None, None]:
        """Continuously yield batches of snapshots at a fixed polling interval.

        Args:
            tickers: List of market identifiers to poll.
            interval: Seconds between polls (default 5).
            max_polls: Stop after this many polls (``None`` = run forever).

        Yields:
            A list of :class:`~src.trading.models.MarketSnapshot` objects.
        """
        polls = 0
        while max_polls is None or polls < max_polls:
            snapshots = self.scan(tickers)
            yield snapshots
            polls += 1
            if max_polls is None or polls < max_polls:
                time.sleep(interval)


class KalshiMarketFeed(MarketFeed):
    """Market feed backed by the Kalshi public REST API.

    Args:
        client: An authenticated or anonymous :class:`~src.indexers.kalshi.client.KalshiClient`.
    """

    def __init__(self, client: KalshiClient) -> None:
        self._client = client

    def scan(self, tickers: list[str]) -> list[MarketSnapshot]:
        """Fetch current snapshots for each ticker.

        Tickers that fail to fetch are silently skipped (logged to stderr).

        Args:
            tickers: Market tickers to fetch.

        Returns:
            Snapshots for successfully fetched markets.
        """
        from src.trading.models import MarketSnapshot

        snapshots: list[MarketSnapshot] = []
        now = datetime.now(timezone.utc)
        for ticker in tickers:
            try:
                market = self._client.get_market(ticker)
                snapshots.append(
                    MarketSnapshot(
                        ticker=market.ticker,
                        title=market.title,
                        yes_bid=market.yes_bid,
                        yes_ask=market.yes_ask,
                        no_bid=market.no_bid,
                        no_ask=market.no_ask,
                        last_price=market.last_price,
                        open_interest=market.open_interest,
                        timestamp=now,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                import sys

                print(f"[feed] Failed to fetch {ticker}: {exc}", file=sys.stderr)
        return snapshots

    def scan_all_open(self, limit: int = 200) -> list[MarketSnapshot]:
        """Fetch snapshots for all currently open markets.

        Args:
            limit: Page size when listing markets (default 200).

        Returns:
            Snapshots for all open markets returned by the API.
        """
        from src.trading.models import MarketSnapshot

        now = datetime.now(timezone.utc)
        markets = self._client.list_markets(limit=limit, status="open")
        return [
            MarketSnapshot(
                ticker=m.ticker,
                title=m.title,
                yes_bid=m.yes_bid,
                yes_ask=m.yes_ask,
                no_bid=m.no_bid,
                no_ask=m.no_ask,
                last_price=m.last_price,
                open_interest=m.open_interest,
                timestamp=now,
            )
            for m in markets
        ]
