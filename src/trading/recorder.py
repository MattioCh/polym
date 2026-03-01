"""Feed recorder: persists market snapshots as Parquet for later analysis.

Usage
-----
    from src.trading.recorder import FeedRecorder
    from src.trading.feed import MarketFeed

    recorder = FeedRecorder(output_dir="data/feed_recordings")

    for snapshots in feed.stream(interval=5.0):
        recorder.record(snapshots)
        # flush to disk every 5 minutes
        recorder.flush_if_due()

    recorder.flush()  # always flush on exit
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.trading.models import MarketSnapshot


class FeedRecorder:
    """Accumulates market snapshots and writes them to Parquet files.

    Files are named ``feed_YYYYMMDD_HHMMSS.parquet`` so they can be loaded
    with a glob pattern by downstream analysis scripts.

    Parameters
    ----------
    output_dir:
        Directory where Parquet files are written.
    flush_interval:
        Seconds between automatic flushes to disk (default 300 s / 5 min).
    """

    def __init__(self, output_dir: Path | str, flush_interval: float = 300.0):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.flush_interval = flush_interval
        self._buffer: list[dict] = []
        self._last_flush: float = time.monotonic()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(self, snapshots: list[MarketSnapshot]) -> None:
        """Add *snapshots* to the in-memory buffer."""
        for s in snapshots:
            self._buffer.append(
                {
                    "ticker": s.ticker,
                    "yes_bid": s.yes_bid,
                    "yes_ask": s.yes_ask,
                    "no_bid": s.no_bid,
                    "no_ask": s.no_ask,
                    "last_price": s.last_price,
                    "mid_price": s.mid_price,
                    "spread": s.spread,
                    "timestamp": s.timestamp,
                }
            )

    def flush_if_due(self) -> bool:
        """Flush to disk if *flush_interval* seconds have elapsed.

        Returns ``True`` if a flush was performed.
        """
        if time.monotonic() - self._last_flush >= self.flush_interval:
            self.flush()
            return True
        return False

    def flush(self) -> Path | None:
        """Write the current buffer to a Parquet file and clear it.

        Returns the path of the written file, or ``None`` if the buffer was
        empty.
        """
        if not self._buffer:
            return None

        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = self.output_dir / f"feed_{ts}.parquet"

        df = pd.DataFrame(self._buffer)
        df.to_parquet(path, index=False)
        print(f"[recorder] wrote {len(df)} rows to {path}")

        self._buffer.clear()
        self._last_flush = time.monotonic()
        return path

    @property
    def buffered_rows(self) -> int:
        """Number of rows currently in the buffer."""
        return len(self._buffer)

    # ------------------------------------------------------------------
    # Loading recorded data
    # ------------------------------------------------------------------

    @staticmethod
    def load(feed_dir: Path | str) -> pd.DataFrame:
        """Load all recorded snapshots from *feed_dir* into a single DataFrame.

        Parameters
        ----------
        feed_dir:
            Directory containing ``feed_*.parquet`` files.

        Returns
        -------
        pd.DataFrame
            All snapshots sorted by timestamp, with columns:
            ticker, yes_bid, yes_ask, no_bid, no_ask, last_price,
            mid_price, spread, timestamp.
        """
        feed_dir = Path(feed_dir)
        files = sorted(feed_dir.glob("feed_*.parquet"))
        if not files:
            return pd.DataFrame()
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
