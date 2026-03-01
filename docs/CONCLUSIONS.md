# Trading Conclusions & Roadmap

This document summarises key findings from the quantitative analyses of Kalshi
and Polymarket historical data, and translates them into a prioritised trading
roadmap.

---

## Key Research Findings

### 1. Maker vs. Taker Returns

Passive market-makers (makers) earn a consistent positive edge across most price
levels. Takers face a structural disadvantage except in a narrow band of
mispriced markets. The maker edge is largest at prices 10–30¢ and 70–90¢ —
i.e., at the extremes where resolution uncertainty is highest.

**Implication:** A passive market-making strategy is likely to be more
consistently profitable than aggressive taker strategies. However, position
risk is concentrated and requires careful balance management.

### 2. Time-to-Close Effect

Maker edge peaks when there are 6 hours to 7 days remaining before market
close. Within the final 6 hours, takers gain an informational advantage
(particularly in Politics and Crypto categories), and maker edge declines
sharply.

**Implication:** Market-making orders should be withdrawn or significantly
widened within 6 hours of close. Taker strategies may be profitable in the
final hours of high-conviction markets.

### 3. Category Concentration

Profit is highly concentrated. The top 5% of markets by maker P&L account for
the majority of total maker profit. Sports markets show the most consistent
maker edge; Politics markets are the riskiest.

**Implication:** Focus capital on high-volume, high-liquidity markets. Build a
market-selection filter based on volume and category before deploying capital.

### 4. Mean Reversion

Short-term price series in Kalshi markets exhibit mild mean-reverting behaviour
within a rolling 20-trade window, particularly in high-volume markets. The
`backtest_mean_reversion` analysis quantifies this signal.

**Implication:** A threshold-based mean-reversion taker strategy can generate
positive expected value before fees, but requires tight position sizing and
stop-loss rules to manage tail risk.

### 5. Taker Timing & Hour Effects

Taker excess returns vary significantly by hour of day (ET). Returns are
highest in early morning hours (5–9 AM ET) and lowest during peak market
hours (2–4 PM ET). This suggests that morning markets are less efficiently
priced and offer better entry points for takers.

---

## Prioritised Trading Roadmap

The following priorities are ordered by risk-adjusted expected value and
implementation complexity.

### Priority 1 — Foundation (Implemented)

- [x] **Market feed scanner** (`src/trading/feed.py`) — poll live market
  prices at configurable intervals.
- [x] **Feed recorder** (`src/trading/recorder.py`) — persist snapshots to
  Parquet for future orderbook analysis.
- [x] **Pre-trade checks** (`src/trading/checks.py`) — verify balance and
  price tolerance before any order submission.
- [x] **Trade notifications** (`src/trading/notifier.py`) — announce fills
  and order events.
- [x] **Position & portfolio management** (`src/trading/portfolio.py`) —
  track open positions, unrealized/realized P&L, and generate reports.
- [x] **Kalshi trading client** (`src/trading/kalshi/client.py`) — submit
  and cancel limit orders, query balance, fetch fills.

### Priority 2 — Strategy Execution (Implemented)

- [x] **Taker price strategies** (`src/trading/strategy.py`):
  - `ThresholdCrossStrategy` — fire when ask/bid crosses a static threshold.
  - `MidpointStrategy` — post at the current midpoint ± bias.
  - `MeanReversionStrategy` — fade deviations from a rolling VWAP.
- [x] **Mean-reversion backtest** (`src/analysis/kalshi/backtest_mean_reversion.py`)
  — historical simulation with equity curve, Sharpe ratio, and drawdown metrics.

### Priority 3 — Market Making (Next Steps)

- [ ] Implement a passive maker strategy that posts two-sided quotes at the
  midpoint ± half-spread.
- [ ] Add automatic quote withdrawal when time-to-close < 6 hours.
- [ ] Implement position limits (max contracts per market, max total notional).
- [ ] Kalshi batch order API support for efficient multi-market quoting.

### Priority 4 — Risk Management

- [ ] Real-time drawdown monitoring: halt trading when daily drawdown exceeds a
  configurable threshold.
- [ ] Concentration limits: cap exposure to any single category (Sports /
  Politics / Crypto) at a portfolio-level notional limit.
- [ ] Margin / settlement monitoring: alert when open interest approaches
  account balance.

### Priority 5 — Reporting & Monitoring

- [ ] **Live portfolio dashboard**: terminal or web UI showing open positions,
  current prices, unrealized P&L, and recent fills.
- [ ] **Daily P&L report**: scheduled email/Slack digest with realized P&L,
  historical equity curve, trades-by-hour breakdown.
- [ ] **Trades-by-hour-to-close**: bucket fills by hours remaining until
  market resolution to measure edge decay.

### Priority 6 — Polymarket Integration

- [ ] Add `src/trading/polymarket/client.py` using the Polymarket CLOB API.
- [ ] Port feed scanner and strategy framework to Polymarket.
- [ ] Cross-market arbitrage: monitor correlated Kalshi/Polymarket markets for
  price discrepancies.

---

## Risk Considerations

| Risk | Mitigation |
|------|-----------|
| Adverse selection (informed takers) | Withdraw quotes 6h before close; widen spreads in Politics |
| Inventory risk | Hard position limits per market and portfolio |
| Model risk (mean reversion fails) | Live forward-testing with paper orders before capital deployment |
| API downtime / rate limits | Retry logic + exponential backoff (already implemented) |
| Authentication / key leakage | Store keys in `.env`; never commit to source control |

---

## Next Steps

1. Configure Kalshi API key in `.env` and validate `KalshiTradingClient` against
   the demo environment.
2. Run `make analyze` to generate the `backtest_mean_reversion` output and
   calibrate the `entry_threshold` and `window` parameters.
3. Begin paper-trading the `MeanReversionStrategy` on 5–10 high-volume markets
   using the feed scanner and notifier.
4. After 2–4 weeks of paper-trading, review fill quality and P&L attribution
   before committing real capital.
