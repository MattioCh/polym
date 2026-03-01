"""Market feed: polls the Kalshi API and emits live price snapshots.

Usage
-----
    from src.trading.feed import MarketFeed
    from src.indexers.kalshi.client import KalshiClient

    with KalshiClient() as client:
        feed = MarketFeed(client, tickers=["INXD-25JAN06", "NVDA-25JAN"])

        # Single snapshot
        snapshot = feed.snapshot()

        # Continuous stream (blocks; hit Ctrl-C to stop)
        for snapshots in feed.stream(interval=5.0):
            for s in snapshots:
                print(s.ticker, s.mid_price)
"""

from __future__ import annotations

import time
from collections.abc import Generator
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.trading.models import MarketSnapshot

if TYPE_CHECKING:
    from src.indexers.kalshi.client import KalshiClient


class MarketFeed:
    """Polls the Kalshi REST API for live market prices.

    Parameters
    ----------
    client:
        An authenticated (or public) ``KalshiClient`` instance.
    tickers:
        List of market tickers to track.  Pass an empty list to track *all*
        open markets (one extra API call per poll cycle).
    """

    def __init__(self, client: KalshiClient, tickers: list[str] | None = None):
        self.client = client
        self.tickers = tickers or []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def snapshot(self) -> list[MarketSnapshot]:
        """Return one price snapshot per tracked ticker.

        If *tickers* is empty, fetches the full market list instead.
        """
        tickers = self._resolve_tickers()
        snapshots: list[MarketSnapshot] = []
        now = datetime.now(tz=timezone.utc)
        for ticker in tickers:
            try:
                market = self.client.get_market(ticker)
                snapshots.append(
                    MarketSnapshot(
                        ticker=ticker,
                        yes_bid=market.yes_bid,
                        yes_ask=market.yes_ask,
                        no_bid=market.no_bid,
                        no_ask=market.no_ask,
                        last_price=market.last_price,
                        timestamp=now,
                    )
                )
            except Exception as exc:
                print(f"[feed] failed to fetch {ticker}: {exc}")
        return snapshots

    def scan_all(self, status: str = "open") -> list[MarketSnapshot]:
        """Snapshot all markets that match *status* in a single page sweep.

        This is useful for a broad "scan all open markets" feed.
        """
        now = datetime.now(tz=timezone.utc)
        snapshots: list[MarketSnapshot] = []
        try:
            markets = self.client.list_all_markets()
            for m in markets:
                if m.status != status:
                    continue
                snapshots.append(
                    MarketSnapshot(
                        ticker=m.ticker,
                        yes_bid=m.yes_bid,
                        yes_ask=m.yes_ask,
                        no_bid=m.no_bid,
                        no_ask=m.no_ask,
                        last_price=m.last_price,
                        timestamp=now,
                    )
                )
        except Exception as exc:
            print(f"[feed] scan_all failed: {exc}")
        return snapshots

    def stream(
        self,
        interval: float = 5.0,
        max_cycles: int | None = None,
    ) -> Generator[list[MarketSnapshot], None, None]:
        """Yield a list of snapshots every *interval* seconds.

        Parameters
        ----------
        interval:
            Seconds between poll cycles (default 5 s).
        max_cycles:
            Stop after this many cycles.  ``None`` means run forever until
            interrupted.

        Yields
        ------
        List[MarketSnapshot]
            One entry per tracked ticker.

        Example
        -------
        ::

            for snapshots in feed.stream(interval=2.0, max_cycles=10):
                for s in snapshots:
                    print(f"{s.ticker}: {s.mid_price}¢")
        """
        cycles = 0
        while max_cycles is None or cycles < max_cycles:
            snapshots = self.snapshot()
            yield snapshots
            cycles += 1
            if max_cycles is None or cycles < max_cycles:
                time.sleep(interval)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_tickers(self) -> list[str]:
        if self.tickers:
            return self.tickers
        # Fallback: discover open markets dynamically
        try:
            markets = self.client.list_markets(status="open", limit=200)
            return [m.ticker for m in markets]
        except Exception as exc:
            print(f"[feed] could not resolve tickers: {exc}")
            return []
