# Trading System Design – Conclusions & Priorities

This document records first-principle thinking on the concerns involved when
trading on Kalshi and Polymarket, the resulting system design, and the
priority order in which to build it.

---

## 1. First-Principle Concerns

Before placing any real money the following questions must be answered with
confidence for every trade:

| # | Concern | Why it matters |
|---|---------|---------------|
| 1 | **What is the current price?** | Stale prices mean you trade at the wrong level. |
| 2 | **Do I have enough balance?** | Over-committing causes rejections or margin calls. |
| 3 | **Is the signal still valid?** | The market may have already moved since the signal fired. |
| 4 | **What is my current exposure?** | Position concentration amplifies drawdowns. |
| 5 | **Did my order fill?** | Unacknowledged orders can accumulate silently. |
| 6 | **What is my current PnL?** | Without PnL visibility you cannot manage risk. |
| 7 | **Why did I enter this trade?** | Rationale traceability is essential for post-trade review. |

---

## 2. System Components & Status

### 2a. Market Feed (`src/trading/feed.py`)
Polls the Kalshi REST API at a configurable interval to produce a stream of
`MarketSnapshot` objects (bid, ask, last price, timestamp).

* `MarketFeed.snapshot()` – single point-in-time fetch for a list of tickers
* `MarketFeed.scan_all()` – sweep all open markets in one pass
* `MarketFeed.stream()` – generator that yields snapshots every N seconds

**Status:** ✅ Implemented

---

### 2b. Feed Recorder (`src/trading/recorder.py`)
Buffers snapshots in memory and flushes them to Parquet files on a rolling
schedule.  The resulting files can be loaded later with `FeedRecorder.load()`
for orderbook reconstruction and backtesting.

**Status:** ✅ Implemented

---

### 2c. Signal Engine (`src/trading/signals.py`)
`MeanReversionSignal` maintains a per-ticker rolling price window and emits
`TakerSignal` objects when the current mid-price crosses a configurable
Z-score threshold.  The signal's `limit_price` is placed inside the current
bid-ask spread so the order queues at a favourable level.

Key parameters:
* `lookback` – observations in the rolling window (default 20)
* `threshold` – Z-score magnitude required to trigger a signal (default 2.0)
* `min_spread` – minimum spread required to leave room inside (default 2 ¢)

**Status:** ✅ Implemented

---

### 2d. Pre-Trade Checker (`src/trading/executor.py:PreTradeChecker`)
Before an order is submitted the checker verifies:
1. `contracts > 0` and `limit_price ∈ [1, 99]`
2. Live mid-price has not moved more than `max_price_slip` cents since the
   signal fired
3. Order cost does not exceed `max_position_cost`
4. Available account balance ≥ order cost (skipped when no client configured)

**Status:** ✅ Implemented

---

### 2e. Order Executor (`src/trading/executor.py:OrderExecutor`)
Wraps `PreTradeChecker` and manages the order lifecycle:
* **Paper mode** (default) – simulates immediate fill at `limit_price`
* **Live mode** – calls `KalshiTradingClient.create_order()` and polls for fills

**Status:** ✅ Implemented (paper mode); live mode requires API credentials

---

### 2f. Trade Notifier (`src/trading/notifications.py`)
Announces every fill, partial fill, order submission, cancellation, signal,
and pre-trade rejection to stdout and via Python's `logging` module.

**Status:** ✅ Implemented

---

### 2g. Position Manager (`src/trading/positions.py`)
Tracks open positions keyed by `(ticker, side)`.  For each position it
computes:
* `cost_basis` = avg_entry_price × contracts
* `unrealised_pnl(current_price)` – mark-to-market PnL
* `realised_pnl(result)` – PnL on resolution

`PositionManager.report(snapshot_map)` returns a `DataFrame` suitable for
display or export.

**Status:** ✅ Implemented

---

### 2h. Portfolio Reporter (`src/trading/portfolio.py`)
Aggregates across all positions and fills to produce:

| Report | Description |
|--------|-------------|
| `summary()` | Total cost basis, unrealised PnL, realised PnL, win rate |
| `market_concentration()` | Fill counts and cost by ticker ("which markets am I trading most?") |
| `historical_pnl()` | Cumulative PnL curve sorted by closed time |
| `trades_by_hour_to_close()` | Fill counts bucketed by hours-to-close at entry |

**Status:** ✅ Implemented

---

### 2i. Kalshi Trading Client (`src/indexers/kalshi/client.py:KalshiTradingClient`)
Extends `KalshiClient` with RSA-signed authenticated endpoints:

| Method | Endpoint |
|--------|----------|
| `get_balance()` | `GET /portfolio/balance` |
| `get_positions()` | `GET /portfolio/positions` |
| `get_fills()` | `GET /portfolio/fills` |
| `create_order()` | `POST /portfolio/orders` |
| `cancel_order()` | `DELETE /portfolio/orders/{id}` |
| `get_order()` | `GET /portfolio/orders/{id}` |

Credentials are loaded from environment variables (see `.env.example`).

**Status:** ✅ Implemented

---

### 2j. Mean-Reversion Backtest (`src/analysis/kalshi/backtest_mean_reversion.py`)
Evaluates the mean-reversion signal on all resolved Kalshi markets.  Outputs:
* Per-trade signal log with Z-score, entry price, and PnL
* Cumulative equity curve
* Win rate, average return, Sharpe ratio, max drawdown
* Performance breakdown by category group and entry-price bucket

**Status:** ✅ Implemented

---

## 3. Sensible Priority Order

```
Priority 1 – See the market (feed + recorder)
Priority 2 – Know your position (position manager + portfolio reporter)
Priority 3 – Validate signals offline (backtest)
Priority 4 – Paper-trade live signals (signal engine + executor in paper mode)
Priority 5 – Go live (executor in live mode + credentials)
```

### Priority 1 – Market visibility
You cannot trade what you cannot see.  Before writing a single order line:

1. Run `MarketFeed.scan_all()` and inspect bid/ask spreads.
2. Start `FeedRecorder` so you accumulate historical tick data for later
   orderbook analysis.
3. Identify which markets have tight spreads and high volume.

### Priority 2 – Know your positions at all times
Even in paper mode, always run `PositionManager` and call
`portfolio_reporter.summary()` before each trade cycle.  This prevents
accidental over-concentration.

### Priority 3 – Validate the strategy with backtests
Run `BacktestMeanReversionAnalysis` with different `lookback` and `threshold`
values.  Look for:
* Win rate consistently > 55 % across groups
* Positive Sharpe ratio (> 0.5 is reasonable for this market structure)
* Drawdown manageable relative to expected capital allocation

### Priority 4 – Paper-trade live signals
With `OrderExecutor(paper=True)` the signal engine runs against the live feed
but fills are simulated.  Run this for at least 2 weeks to verify:
* Signal frequency (not too many, not too few)
* P50 / P95 fill quality (is limit_price actually attainable?)
* Portfolio reporter output matches expectations

### Priority 5 – Go live
1. Obtain Kalshi API credentials (see `.env.example`).
2. Fund the account.
3. Switch to `OrderExecutor(paper=False, client=KalshiTradingClient())`.
4. Set conservative `max_position_cost` and `max_price_slip` limits.
5. Monitor `TradeNotifier` output and `PositionManager.report()` continuously.

---

## 4. Risk Controls Checklist

Before going live, confirm:

- [ ] `PreTradeChecker.max_position_cost` set to a sensible single-trade limit
- [ ] `PreTradeChecker.max_price_slip` set ≤ half the average spread
- [ ] Stop-loss logic: if daily PnL falls below a threshold, halt trading
- [ ] Position concentration: no single market > X% of total capital
- [ ] Stale-signal filter: ignore signals > 30 s old
- [ ] Market close filter: do not enter new positions < 1 h before close
- [ ] Duplicate-order guard: use `client_order_id` idempotency key

---

## 5. Future Enhancements

| Enhancement | Benefit |
|-------------|---------|
| Polymarket integration | Access to additional liquidity and different market structure |
| WebSocket feed | Lower latency than polling; required for sub-second signals |
| Order book depth | Better signal quality using book imbalance rather than last price |
| Multi-leg orders | Pairs trading across correlated markets |
| Dynamic position sizing | Kelly criterion or volatility-adjusted sizing |
| Alert routing | Slack / email / SMS in addition to console logging |
