"""Market feed recorder.

Accumulates :class:`~src.trading.models.MarketSnapshot` objects in memory and
flushes them to a Parquet file for later orderbook analysis.

Example usage::

    from src.trading.recorder import FeedRecorder
    from pathlib import Path

    recorder = FeedRecorder()
    for batch in feed.stream(tickers, interval=5.0):
        recorder.record_batch(batch)
        if recorder.pending >= 1000:
            recorder.flush(Path("data/feed/kalshi"))
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.trading.models import FeedRecord, MarketSnapshot


class FeedRecorder:
    """Buffers feed snapshots and flushes to Parquet on demand.

    Each call to :meth:`flush` writes the buffered records to a timestamped
    Parquet file inside ``output_dir``.  The buffer is cleared after each flush.
    """

    def __init__(self) -> None:
        self._buffer: list[FeedRecord] = []

    @property
    def pending(self) -> int:
        """Number of buffered records not yet flushed."""
        return len(self._buffer)

    def record(self, snapshot: MarketSnapshot) -> None:
        """Buffer a single snapshot.

        Args:
            snapshot: The market snapshot to record.
        """
        from src.trading.models import FeedRecord

        self._buffer.append(
            FeedRecord(
                ticker=snapshot.ticker,
                yes_bid=snapshot.yes_bid,
                yes_ask=snapshot.yes_ask,
                no_bid=snapshot.no_bid,
                no_ask=snapshot.no_ask,
                last_price=snapshot.last_price,
                open_interest=snapshot.open_interest,
                timestamp=snapshot.timestamp,
            )
        )

    def record_batch(self, snapshots: list[MarketSnapshot]) -> None:
        """Buffer a batch of snapshots.

        Args:
            snapshots: List of snapshots to record.
        """
        for snapshot in snapshots:
            self.record(snapshot)

    def flush(self, output_dir: Path | str) -> Path:
        """Write buffered records to a Parquet file and clear the buffer.

        The output file is named ``feed_<ISO-timestamp>.parquet`` so successive
        flushes do not overwrite each other.

        Args:
            output_dir: Directory in which to write the Parquet file.

        Returns:
            Path to the written Parquet file.

        Raises:
            RuntimeError: If there are no buffered records to flush.
        """
        import pandas as pd

        if not self._buffer:
            raise RuntimeError("No records to flush.")

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        rows = [r.to_dict() for r in self._buffer]
        df = pd.DataFrame(rows)

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        path = output_dir / f"feed_{ts}.parquet"
        df.to_parquet(path, index=False)

        self._buffer.clear()
        return path
