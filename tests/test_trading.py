"""Tests for the trading module (models, checks, notifier, feed, recorder, strategy, portfolio)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.trading.checks import PreTradeCheckError, PreTradeChecks
from src.trading.models import (
    FeedRecord,
    Fill,
    MarketSnapshot,
    Order,
    PortfolioSummary,
    Position,
)
from src.trading.notifier import (
    CallbackNotifier,
    CompositeNotifier,
    PrintNotifier,
)
from src.trading.portfolio import Portfolio
from src.trading.recorder import FeedRecorder
from src.trading.strategy import (
    MeanReversionStrategy,
    MidpointStrategy,
    ThresholdCrossStrategy,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _snapshot(
    ticker="MKT-A",
    yes_bid=40,
    yes_ask=44,
    no_bid=56,
    no_ask=60,
    last_price=42,
    open_interest=500,
) -> MarketSnapshot:
    return MarketSnapshot(
        ticker=ticker,
        title="Test Market",
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        last_price=last_price,
        open_interest=open_interest,
        timestamp=_now(),
    )


def _position(
    ticker="MKT-A",
    side="yes",
    qty=10,
    entry=42,
    current=None,
) -> Position:
    return Position(
        ticker=ticker,
        title="Test Market",
        side=side,
        quantity=qty,
        avg_entry_price=entry,
        entry_time=_now(),
        current_price=current,
    )


def _fill(ticker="MKT-A", action="buy", side="yes", qty=5, price=42) -> Fill:
    return Fill(
        fill_id="fill-1",
        order_id="order-1",
        ticker=ticker,
        side=side,
        action=action,
        quantity=qty,
        price=price,
        timestamp=_now(),
    )


def _order(ticker="MKT-A", side="yes", action="buy", qty=5, price=42) -> Order:
    return Order(ticker=ticker, side=side, action=action, quantity=qty, price=price)


# ===========================================================================
# MarketSnapshot
# ===========================================================================


class TestMarketSnapshot:
    def test_yes_mid_returns_average_of_bid_ask(self):
        snap = _snapshot(yes_bid=40, yes_ask=44)
        assert snap.yes_mid == 42.0

    def test_yes_mid_none_when_bid_missing(self):
        snap = _snapshot(yes_bid=None, yes_ask=44)
        assert snap.yes_mid is None

    def test_spread(self):
        snap = _snapshot(yes_bid=40, yes_ask=44)
        assert snap.spread == 4

    def test_spread_none_when_ask_missing(self):
        snap = _snapshot(yes_bid=40, yes_ask=None)
        assert snap.spread is None


# ===========================================================================
# Position
# ===========================================================================


class TestPosition:
    def test_cost_basis(self):
        pos = _position(qty=10, entry=42)
        assert pos.cost_basis_cents == 420

    def test_unrealized_pnl_positive(self):
        pos = _position(qty=10, entry=42, current=50)
        assert pos.unrealized_pnl_cents == 80
        assert abs(pos.unrealized_pnl_usd - 0.80) < 1e-9

    def test_unrealized_pnl_none_when_no_current_price(self):
        pos = _position(qty=10, entry=42)
        assert pos.unrealized_pnl_cents is None

    def test_unrealized_pnl_negative(self):
        pos = _position(qty=5, entry=50, current=40)
        assert pos.unrealized_pnl_cents == -50


# ===========================================================================
# Order
# ===========================================================================


class TestOrder:
    def test_notional(self):
        order = _order(qty=10, price=42)
        assert order.notional_cents == 420


# ===========================================================================
# Fill
# ===========================================================================


class TestFill:
    def test_notional_usd(self):
        fill = _fill(qty=5, price=40)
        assert abs(fill.notional_usd - 2.0) < 1e-9


# ===========================================================================
# FeedRecord
# ===========================================================================


class TestFeedRecord:
    def test_to_dict_keys(self):
        snap = _snapshot()
        rec = FeedRecord(
            ticker=snap.ticker,
            yes_bid=snap.yes_bid,
            yes_ask=snap.yes_ask,
            no_bid=snap.no_bid,
            no_ask=snap.no_ask,
            last_price=snap.last_price,
            open_interest=snap.open_interest,
            timestamp=snap.timestamp,
        )
        d = rec.to_dict()
        assert set(d.keys()) == {"ticker", "yes_bid", "yes_ask", "no_bid", "no_ask", "last_price", "open_interest", "timestamp"}


# ===========================================================================
# PreTradeChecks
# ===========================================================================


class TestPreTradeChecks:
    def test_check_balance_passes(self):
        order = _order(qty=5, price=40)  # notional = 200
        PreTradeChecks.check_balance(300, order)  # no error

    def test_check_balance_raises_on_insufficient_funds(self):
        order = _order(qty=10, price=50)  # notional = 500
        with pytest.raises(PreTradeCheckError, match="Insufficient balance"):
            PreTradeChecks.check_balance(400, order)

    def test_check_price_passes_within_tolerance(self):
        order = _order(side="yes", action="buy", price=44)  # ask = 44
        snap = _snapshot(yes_ask=44)
        PreTradeChecks.check_price(order, snap, tolerance_pct=0.10)

    def test_check_price_raises_when_price_moved(self):
        order = _order(side="yes", action="buy", price=20)  # ask = 44, huge deviation
        snap = _snapshot(yes_ask=44)
        with pytest.raises(PreTradeCheckError, match="Price out of tolerance"):
            PreTradeChecks.check_price(order, snap, tolerance_pct=0.05)

    def test_check_price_raises_when_no_quote(self):
        order = _order(side="yes", action="buy", price=42)
        snap = _snapshot(yes_ask=None)
        with pytest.raises(PreTradeCheckError, match="No quote available"):
            PreTradeChecks.check_price(order, snap)

    def test_check_quantity_raises_below_min(self):
        order = _order(qty=0)
        with pytest.raises(PreTradeCheckError, match="below minimum"):
            PreTradeChecks.check_quantity(order)

    def test_check_quantity_raises_above_max(self):
        order = _order(qty=99999)
        with pytest.raises(PreTradeCheckError, match="exceeds maximum"):
            PreTradeChecks.check_quantity(order)

    def test_run_all_passes(self):
        order = _order(qty=5, price=44)
        snap = _snapshot(yes_ask=44)
        PreTradeChecks.run_all(1000, order, snap)

    def test_run_all_raises_on_balance(self):
        order = _order(qty=100, price=90)  # notional = 9000
        snap = _snapshot(yes_ask=90)
        with pytest.raises(PreTradeCheckError):
            PreTradeChecks.run_all(500, order, snap)


# ===========================================================================
# Notifiers
# ===========================================================================


class TestPrintNotifier:
    def test_on_fill_does_not_raise(self, capsys):
        notifier = PrintNotifier()
        notifier.on_fill(_fill())
        captured = capsys.readouterr()
        assert "[FILL]" in captured.out

    def test_on_order_submitted(self, capsys):
        notifier = PrintNotifier()
        order = _order()
        order.order_id = "ord-123"
        notifier.on_order_submitted(order)
        assert "[ORDER]" in capsys.readouterr().out

    def test_on_order_cancelled(self, capsys):
        notifier = PrintNotifier()
        order = _order()
        order.order_id = "ord-123"
        notifier.on_order_cancelled(order)
        assert "[CANCEL]" in capsys.readouterr().out


class TestCallbackNotifier:
    def test_on_fill_calls_callback(self):
        received: list[Fill] = []
        notifier = CallbackNotifier(on_fill_cb=received.append)
        fill = _fill()
        notifier.on_fill(fill)
        assert received == [fill]

    def test_on_order_submitted_optional(self):
        notifier = CallbackNotifier(on_fill_cb=lambda _: None)
        notifier.on_order_submitted(_order())  # should not raise


class TestCompositeNotifier:
    def test_fan_out_to_multiple_notifiers(self):
        fills_a: list[Fill] = []
        fills_b: list[Fill] = []
        composite = CompositeNotifier(
            CallbackNotifier(on_fill_cb=fills_a.append),
            CallbackNotifier(on_fill_cb=fills_b.append),
        )
        fill = _fill()
        composite.on_fill(fill)
        assert fills_a == [fill]
        assert fills_b == [fill]

    def test_add_notifier_at_runtime(self):
        received: list[Fill] = []
        composite = CompositeNotifier()
        composite.add(CallbackNotifier(on_fill_cb=received.append))
        composite.on_fill(_fill())
        assert len(received) == 1


# ===========================================================================
# FeedRecorder
# ===========================================================================


class TestFeedRecorder:
    def test_pending_starts_at_zero(self):
        recorder = FeedRecorder()
        assert recorder.pending == 0

    def test_record_increments_pending(self):
        recorder = FeedRecorder()
        recorder.record(_snapshot())
        assert recorder.pending == 1

    def test_record_batch(self):
        recorder = FeedRecorder()
        recorder.record_batch([_snapshot("MKT-A"), _snapshot("MKT-B")])
        assert recorder.pending == 2

    def test_flush_writes_parquet(self, tmp_path: Path):
        recorder = FeedRecorder()
        recorder.record(_snapshot("MKT-A"))
        recorder.record(_snapshot("MKT-B"))
        path = recorder.flush(tmp_path)
        assert path.exists()
        df = pd.read_parquet(path)
        assert len(df) == 2
        assert set(df.columns).issuperset({"ticker", "yes_bid", "timestamp"})

    def test_flush_clears_buffer(self, tmp_path: Path):
        recorder = FeedRecorder()
        recorder.record(_snapshot())
        recorder.flush(tmp_path)
        assert recorder.pending == 0

    def test_flush_empty_raises(self):
        recorder = FeedRecorder()
        with pytest.raises(RuntimeError, match="No records"):
            recorder.flush(Path("/tmp/nowhere"))


# ===========================================================================
# Strategies
# ===========================================================================


class TestThresholdCrossStrategy:
    def test_buy_fires_when_ask_lte_threshold(self):
        strategy = ThresholdCrossStrategy("MKT-A", side="yes", action="buy", target_price=44, quantity=3)
        snap = _snapshot(yes_ask=44)
        order = strategy.evaluate(snap)
        assert order is not None
        assert order.action == "buy"
        assert order.price == 44
        assert order.quantity == 3

    def test_buy_does_not_fire_when_ask_above_threshold(self):
        strategy = ThresholdCrossStrategy("MKT-A", side="yes", action="buy", target_price=40, quantity=1)
        snap = _snapshot(yes_ask=44)
        assert strategy.evaluate(snap) is None

    def test_sell_fires_when_bid_gte_threshold(self):
        strategy = ThresholdCrossStrategy("MKT-A", side="yes", action="sell", target_price=38, quantity=1)
        snap = _snapshot(yes_bid=40)
        order = strategy.evaluate(snap)
        assert order is not None
        assert order.action == "sell"

    def test_wrong_ticker_returns_none(self):
        strategy = ThresholdCrossStrategy("MKT-B", side="yes", action="buy", target_price=44, quantity=1)
        snap = _snapshot(ticker="MKT-A")
        assert strategy.evaluate(snap) is None


class TestMidpointStrategy:
    def test_returns_order_at_midpoint(self):
        strategy = MidpointStrategy("MKT-A", side="yes", action="buy", quantity=2)
        snap = _snapshot(yes_bid=40, yes_ask=44)  # mid = 42
        order = strategy.evaluate(snap)
        assert order is not None
        assert order.price == 42

    def test_bias_applied(self):
        strategy = MidpointStrategy("MKT-A", side="yes", action="buy", quantity=1, bias=-2)
        snap = _snapshot(yes_bid=40, yes_ask=44)  # mid = 42, bias -2 => 40
        order = strategy.evaluate(snap)
        assert order.price == 40

    def test_returns_none_when_no_quote(self):
        strategy = MidpointStrategy("MKT-A", side="yes", action="buy")
        snap = _snapshot(yes_bid=None, yes_ask=None)
        assert strategy.evaluate(snap) is None


class TestMeanReversionStrategy:
    def _build_strategy(self, window=5, threshold=2.0):
        return MeanReversionStrategy("MKT-A", side="yes", quantity=1, window=window, entry_threshold=threshold)

    def test_does_not_fire_before_window_full(self):
        strategy = self._build_strategy(window=5)
        snap = _snapshot(yes_bid=40, yes_ask=44)
        for _ in range(4):
            assert strategy.evaluate(snap) is None

    def test_fires_when_price_deviates_above_threshold(self):
        strategy = self._build_strategy(window=5, threshold=3.0)
        # Fill window with price ~42
        base_snap = _snapshot(yes_bid=40, yes_ask=44)
        for _ in range(5):
            strategy.evaluate(base_snap)
        # Now push price up sharply
        high_snap = _snapshot(yes_bid=60, yes_ask=64)
        order = strategy.evaluate(high_snap)
        # Should want to sell (price above VWAP)
        assert order is not None
        assert order.action == "sell"

    def test_wrong_ticker_always_none(self):
        strategy = self._build_strategy()
        snap = _snapshot(ticker="OTHER")
        assert strategy.evaluate(snap) is None


# ===========================================================================
# Portfolio
# ===========================================================================


class TestPortfolio:
    def test_add_position(self):
        portfolio = Portfolio()
        portfolio.add_position(_position())
        assert len(portfolio.positions) == 1

    def test_unrealized_pnl(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(qty=10, entry=40, current=50))
        assert abs(portfolio.unrealized_pnl_usd - 1.0) < 1e-9

    def test_record_fill_updates_realized_pnl(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(ticker="MKT-A", qty=10, entry=40))
        sell = _fill(ticker="MKT-A", action="sell", qty=5, price=50)
        portfolio.record_fill(sell)
        # realized pnl = (50-40)*5 = 50 cents = $0.50
        assert abs(portfolio.realized_pnl_usd - 0.50) < 1e-9

    def test_record_fill_reduces_position(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(ticker="MKT-A", qty=10, entry=40))
        portfolio.record_fill(_fill(ticker="MKT-A", action="sell", qty=5, price=50))
        assert portfolio.positions[0].quantity == 5

    def test_record_fill_closes_position_fully(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(ticker="MKT-A", qty=5, entry=40))
        portfolio.record_fill(_fill(ticker="MKT-A", action="sell", qty=5, price=50))
        assert len(portfolio.positions) == 0

    def test_update_prices(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(ticker="MKT-A", qty=5, entry=40))
        snap = _snapshot(ticker="MKT-A", yes_bid=48, yes_ask=52)  # mid = 50
        portfolio.update_prices([snap])
        assert portfolio.positions[0].current_price == 50

    def test_total_pnl(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(qty=10, entry=40, current=50))
        portfolio.record_fill(_fill(action="sell", qty=2, price=50))
        # realized = (50-40)*2 = 20c = $0.20
        # unrealized on remaining 8: (50-40)*8 = 80c = $0.80
        portfolio.update_prices([_snapshot(yes_bid=48, yes_ask=52)])
        assert portfolio.total_pnl_usd > 0

    def test_most_traded(self):
        portfolio = Portfolio()
        for _ in range(3):
            portfolio.record_fill(_fill(ticker="MKT-A"))
        for _ in range(1):
            portfolio.record_fill(_fill(ticker="MKT-B"))
        most = portfolio.most_traded()
        assert most[0][0] == "MKT-A"
        assert most[0][1] == 3

    def test_position_dataframe_columns(self):
        portfolio = Portfolio()
        portfolio.add_position(_position())
        df = portfolio.position_dataframe()
        assert "ticker" in df.columns
        assert "unrealized_pnl_usd" in df.columns

    def test_historical_pnl_dataframe(self):
        portfolio = Portfolio()
        portfolio.record_fill(_fill())
        df = portfolio.historical_pnl_dataframe()
        assert len(df) == 1
        assert "cumulative_realized_pnl_usd" in df.columns

    def test_trades_by_hour(self):
        portfolio = Portfolio()
        portfolio.record_fill(_fill())
        df = portfolio.trades_by_hour()
        assert len(df) >= 1
        assert "hour" in df.columns
        assert "fill_count" in df.columns

    def test_trades_by_hour_empty(self):
        portfolio = Portfolio()
        df = portfolio.trades_by_hour()
        assert df.empty

    def test_report_is_string(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(current=50))
        report = portfolio.report()
        assert isinstance(report, str)
        assert "Portfolio Report" in report

    def test_summary_returns_portfolio_summary(self):
        portfolio = Portfolio()
        portfolio.add_position(_position(current=50))
        summary = portfolio.summary()
        assert isinstance(summary, PortfolioSummary)
        assert summary.total_positions == 1
