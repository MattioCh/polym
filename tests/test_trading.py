"""Tests for the trading module components."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.trading.executor import OrderExecutor, PreTradeChecker
from src.trading.models import Fill, MarketSnapshot, Order, Position, TakerSignal
from src.trading.portfolio import PortfolioReporter
from src.trading.positions import PositionManager
from src.trading.recorder import FeedRecorder
from src.trading.signals import MeanReversionSignal

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _snap(ticker: str, yes_bid: int, yes_ask: int, last_price: int | None = None) -> MarketSnapshot:
    return MarketSnapshot(
        ticker=ticker,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=100 - yes_ask,
        no_ask=100 - yes_bid,
        last_price=last_price,
        timestamp=_NOW,
    )


def _filled_order(
    ticker: str = "MKT-A",
    side: str = "yes",
    action: str = "buy",
    contracts: int = 10,
    price: int = 40,
) -> Order:
    return Order(
        order_id="ord-1",
        ticker=ticker,
        side=side,
        action=action,
        contracts=contracts,
        limit_price=price,
        status="filled",
        created_time=_NOW,
        filled_price=price,
        filled_contracts=contracts,
        filled_time=_NOW,
    )


# ---------------------------------------------------------------------------
# MarketSnapshot
# ---------------------------------------------------------------------------


class TestMarketSnapshot:
    def test_mid_price_from_bid_ask(self):
        snap = _snap("T", 48, 52)
        assert snap.mid_price == 50.0

    def test_mid_price_fallback_to_last(self):
        snap = MarketSnapshot("T", None, None, None, None, 45, _NOW)
        assert snap.mid_price == 45.0

    def test_mid_price_none_when_no_data(self):
        snap = MarketSnapshot("T", None, None, None, None, None, _NOW)
        assert snap.mid_price is None

    def test_spread(self):
        snap = _snap("T", 48, 52)
        assert snap.spread == 4

    def test_spread_none_when_no_bid_ask(self):
        snap = MarketSnapshot("T", None, None, None, None, 50, _NOW)
        assert snap.spread is None


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------


class TestPosition:
    def test_unrealised_pnl_yes_position(self):
        pos = Position("T", "yes", 10, 40.0, _NOW)
        # current price = 60 → pnl = (60 - 40) * 10 = 200
        assert pos.unrealised_pnl(60) == 200.0

    def test_unrealised_pnl_no_position(self):
        pos = Position("T", "no", 10, 40.0, _NOW)
        # current yes price = 60 → no price = 40
        # pnl = (40 - 40) * 10 = 0
        assert pos.unrealised_pnl(60) == 0.0

    def test_realised_pnl_win(self):
        pos = Position("T", "yes", 10, 40.0, _NOW)
        # result = yes → payout = 100, pnl = (100 - 40) * 10 = 600
        assert pos.realised_pnl("yes") == 600.0

    def test_realised_pnl_loss(self):
        pos = Position("T", "yes", 10, 40.0, _NOW)
        # result = no → payout = 0, pnl = (0 - 40) * 10 = -400
        assert pos.realised_pnl("no") == -400.0

    def test_cost_basis(self):
        pos = Position("T", "yes", 10, 40.0, _NOW)
        assert pos.cost_basis == 400.0

    def test_add_fill_updates_average(self):
        pos = Position("T", "yes", 10, 40.0, _NOW)
        fill = Fill("f1", "o1", "T", "yes", "buy", 10, 60, _NOW)
        pos.add_fill(fill)
        # New avg = (40*10 + 60*10) / 20 = 50
        assert pos.avg_entry_price == 50.0
        assert pos.contracts == 20


# ---------------------------------------------------------------------------
# MeanReversionSignal
# ---------------------------------------------------------------------------


class TestMeanReversionSignal:
    def _build_history(
        self,
        engine: MeanReversionSignal,
        ticker: str,
        base_price: float,
        n: int,
    ) -> None:
        """Feed *n* stable prices into the engine to fill the lookback window."""
        for _i in range(n):
            snap = _snap(ticker, int(base_price) - 2, int(base_price) + 2)
            engine.update([snap])

    def test_no_signal_before_lookback(self):
        engine = MeanReversionSignal(lookback=10, threshold=2.0)
        snaps = [_snap("T", 48, 52)] * 9
        for s in snaps:
            sigs = engine.update([s])
        assert sigs == []

    def test_signal_on_high_price(self):
        engine = MeanReversionSignal(lookback=10, threshold=2.0)
        # Stable price around 50
        self._build_history(engine, "T", 50, 10)
        # Now spike to 80 — should generate a sell YES signal
        snap = _snap("T", 78, 82, last_price=80)
        sigs = engine.update([snap])
        assert len(sigs) == 1
        assert sigs[0].action == "sell"
        assert sigs[0].side == "yes"
        assert sigs[0].z_score > 2.0

    def test_signal_on_low_price(self):
        engine = MeanReversionSignal(lookback=10, threshold=2.0)
        self._build_history(engine, "T", 50, 10)
        # Crash to 20 — should generate a buy YES signal
        snap = _snap("T", 18, 22)
        sigs = engine.update([snap])
        assert len(sigs) == 1
        assert sigs[0].action == "buy"
        assert sigs[0].side == "yes"
        assert sigs[0].z_score < -2.0

    def test_no_signal_when_spread_too_tight(self):
        engine = MeanReversionSignal(lookback=10, threshold=2.0, min_spread=2)
        self._build_history(engine, "T", 50, 10)
        # Spike but spread=1 (too tight)
        snap = MarketSnapshot("T", 79, 80, 20, 21, 80, _NOW)
        sigs = engine.update([snap])
        assert sigs == []

    def test_reset(self):
        engine = MeanReversionSignal(lookback=5)
        self._build_history(engine, "T", 50, 5)
        engine.reset("T")
        assert "T" not in engine._history


# ---------------------------------------------------------------------------
# PreTradeChecker
# ---------------------------------------------------------------------------


class TestPreTradeChecker:
    def _signal(self, price: int = 50, current: float = 50.0) -> TakerSignal:
        return TakerSignal(
            ticker="T",
            side="yes",
            action="buy",
            limit_price=price,
            current_price=current,
            z_score=-2.5,
            timestamp=_NOW,
        )

    def test_valid_order(self):
        checker = PreTradeChecker()
        snap = _snap("T", 48, 52)
        ok, reason = checker.validate(self._signal(50, 50.0), snap, contracts=10)
        assert ok
        assert reason == ""

    def test_reject_zero_contracts(self):
        checker = PreTradeChecker()
        ok, reason = checker.validate(self._signal(), _snap("T", 48, 52), contracts=0)
        assert not ok
        assert "contracts" in reason

    def test_reject_out_of_range_price(self):
        checker = PreTradeChecker()
        sig = TakerSignal("T", "yes", "buy", 0, 50.0, -2.5, _NOW)
        ok, reason = checker.validate(sig, _snap("T", 48, 52), contracts=1)
        assert not ok

    def test_reject_price_slip(self):
        checker = PreTradeChecker(max_price_slip=2)
        snap = _snap("T", 58, 62)  # mid = 60, signal mid = 50, slip = 10
        ok, reason = checker.validate(self._signal(50, 50.0), snap, contracts=1)
        assert not ok
        assert "slip" in reason

    def test_reject_cost_exceeds_max(self):
        checker = PreTradeChecker(max_position_cost=100)
        snap = _snap("T", 48, 52)
        # cost = 50 * 10 = 500 > 100
        ok, reason = checker.validate(self._signal(50), snap, contracts=10)
        assert not ok
        assert "max_position_cost" in reason


# ---------------------------------------------------------------------------
# OrderExecutor (paper mode)
# ---------------------------------------------------------------------------


class TestOrderExecutor:
    def _signal(self) -> TakerSignal:
        return TakerSignal("T", "yes", "buy", 45, 46.0, -2.1, _NOW)

    def test_paper_fill(self):
        executor = OrderExecutor(paper=True)
        order = executor.submit(self._signal(), contracts=5)
        assert order.status == "filled"
        assert order.filled_contracts == 5
        assert order.filled_price == 45

    def test_cancel_resting(self):
        executor = OrderExecutor(paper=False, client=None)
        self._signal()
        # Manually place a resting order
        import uuid

        order = Order(
            order_id=str(uuid.uuid4()),
            ticker="T",
            side="yes",
            action="buy",
            contracts=5,
            limit_price=45,
            status="resting",
            created_time=_NOW,
        )
        executor._orders[order.order_id] = order
        result = executor.cancel(order.order_id)
        assert result is True
        assert order.status == "cancelled"

    def test_open_orders_property(self):
        executor = OrderExecutor(paper=True)
        executor.submit(self._signal(), contracts=5)  # paper → filled immediately
        assert len(executor.open_orders) == 0
        assert len(executor.filled_orders) == 1


# ---------------------------------------------------------------------------
# PositionManager
# ---------------------------------------------------------------------------


class TestPositionManager:
    def test_open_from_order(self):
        mgr = PositionManager()
        order = _filled_order()
        pos = mgr.open_from_order(order, rationale="test")
        assert pos.contracts == 10
        assert pos.avg_entry_price == 40.0
        assert pos.rationale == "test"

    def test_merge_fills(self):
        mgr = PositionManager()
        mgr.open_from_order(_filled_order(contracts=10, price=40))
        # Second fill at 60
        mgr.open_from_order(_filled_order(contracts=10, price=60))
        pos = mgr.get("MKT-A", "yes")
        assert pos is not None
        assert pos.contracts == 20
        assert pos.avg_entry_price == 50.0

    def test_report_columns(self):
        mgr = PositionManager()
        mgr.open_from_order(_filled_order())
        snap_map = {"MKT-A": _snap("MKT-A", 48, 52)}
        df = mgr.report(snap_map)
        assert "ticker" in df.columns
        assert "unrealised_pnl_dollars" in df.columns

    def test_close_position(self):
        mgr = PositionManager()
        mgr.open_from_order(_filled_order())
        removed = mgr.close_position("MKT-A", "yes")
        assert removed is not None
        assert len(mgr.open_positions) == 0


# ---------------------------------------------------------------------------
# PortfolioReporter
# ---------------------------------------------------------------------------


class TestPortfolioReporter:
    def _make_position(self, ticker: str, side: str, contracts: int, price: float) -> Position:
        return Position(ticker, side, contracts, price, _NOW)

    def _make_fill(self, ticker: str, price: int, contracts: int) -> Fill:
        return Fill(f"f-{ticker}", "o1", ticker, "yes", "buy", contracts, price, _NOW)

    def test_summary_empty(self):
        reporter = PortfolioReporter()
        s = reporter.summary()
        assert s["total_open_positions"] == 0
        assert s["total_realised_pnl_cents"] == 0.0

    def test_summary_with_closed_positions(self):
        pos = self._make_position("T", "yes", 10, 40.0)
        reporter = PortfolioReporter(closed_positions=[(pos, "yes")])
        s = reporter.summary()
        # PnL = (100 - 40) * 10 = 600 cents = $6
        assert s["total_realised_pnl_dollars"] == pytest.approx(6.0, abs=1e-4)
        assert s["win_count"] == 1

    def test_market_concentration(self):
        fills = [
            self._make_fill("T-A", 50, 10),
            self._make_fill("T-A", 50, 5),
            self._make_fill("T-B", 30, 8),
        ]
        reporter = PortfolioReporter(fills=fills)
        df = reporter.market_concentration()
        assert df.iloc[0]["ticker"] == "T-A"
        assert df.iloc[0]["fill_count"] == 2

    def test_historical_pnl_cumulative(self):
        pos1 = self._make_position("T1", "yes", 10, 40.0)
        pos2 = self._make_position("T2", "yes", 10, 60.0)
        pos1.fills = [self._make_fill("T1", 40, 10)]
        pos2.fills = [self._make_fill("T2", 60, 10)]
        reporter = PortfolioReporter(closed_positions=[(pos1, "yes"), (pos2, "no")])
        df = reporter.historical_pnl()
        assert "cumulative_pnl_dollars" in df.columns
        assert len(df) == 2

    def test_trades_by_hour_to_close(self):
        from datetime import timedelta

        close_time = _NOW + timedelta(minutes=30)  # 0.5 h after fill → "0-1h" bucket
        fill = self._make_fill("T", 50, 5)
        reporter = PortfolioReporter(fills=[fill])
        df = reporter.trades_by_hour_to_close({"T": close_time})
        assert len(df) == 1
        assert df.iloc[0]["hours_bucket"] == "0-1h"


# ---------------------------------------------------------------------------
# FeedRecorder
# ---------------------------------------------------------------------------


class TestFeedRecorder:
    def test_record_and_flush(self, tmp_path: Path):
        recorder = FeedRecorder(tmp_path)
        snaps = [_snap("T-A", 48, 52), _snap("T-B", 30, 35)]
        recorder.record(snaps)
        assert recorder.buffered_rows == 2

        path = recorder.flush()
        assert path is not None
        assert path.exists()
        assert recorder.buffered_rows == 0

        df = pd.read_parquet(path)
        assert len(df) == 2
        assert "ticker" in df.columns

    def test_flush_empty_returns_none(self, tmp_path: Path):
        recorder = FeedRecorder(tmp_path)
        assert recorder.flush() is None

    def test_load_roundtrip(self, tmp_path: Path):
        recorder = FeedRecorder(tmp_path)
        recorder.record([_snap("T", 48, 52)])
        recorder.flush()
        loaded = FeedRecorder.load(tmp_path)
        assert len(loaded) == 1
        assert loaded.iloc[0]["ticker"] == "T"
