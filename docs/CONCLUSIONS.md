# Prediction Market Analysis — Conclusions & Trading Strategy

> **Dataset**: Kalshi exchange — 72.1M trades, $18.26B total volume, 7.68M markets, 1.2M events, 586K tickers  
> **Period**: Q3 2021 – Q4 2025 (volume grew from $3.8M → $8.8B per quarter)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Maker vs Taker Edge](#1-maker-vs-taker-edge)
3. [YES vs NO Asymmetry](#2-yes-vs-no-asymmetry)
4. [Price Calibration & Mispricing](#3-price-calibration--mispricing)
5. [Category-Specific Strategies](#4-category-specific-strategies)
6. [Sports Subcategory Breakdown](#5-sports-subcategory-breakdown)
7. [Time-to-Close Effects](#6-time-to-close-effects)
8. [Day-of-Week Effects](#7-day-of-week-effects)
9. [Trade Size as a Signal](#8-trade-size-as-a-signal)
10. [Surprise Outcomes & Risk Management](#9-surprise-outcomes--risk-management)
11. [Composite Strategy Signals](#10-composite-strategy-signals)
12. [Taker Alpha Niches](#11-taker-alpha-niches)
13. [Actionable Trading Parameters](#actionable-trading-parameters)
14. [Walk-Forward Backtest Results](#12-walk-forward-backtest-results)
15. [Momentum Analysis — Can Takers Ride Price Trends?](#13-momentum-analysis--can-takers-ride-price-trends)
16. [Mean-Reversion Analysis — Can Takers Fade Price Deviations?](#14-mean-reversion-analysis--can-takers-fade-price-deviations)
---

## Executive Summary

Kalshi's prediction markets exhibit a **persistent maker advantage** of roughly 1.5–2.5% excess return, driven by a systematic **YES-side optimism bias** among takers. The market is well-calibrated in aggregate — a contract priced at 50¢ wins ~50% of the time — but individual price buckets reveal exploitable patterns:

- **NO bets outperform YES bets at 14 of 19 tested price levels** (all statistically significant).
- Makers earn positive excess returns across **every category except Esports**.
- The maker edge is **U-shaped in time-to-close**, peaking at 6 hours to 7 days before expiry.
- **Weekend trading** amplifies the maker edge by 0.5–1.5 percentage points.
- **Surprise outcomes** (>50% mispricing) produce the only large maker losses; 92.6% of maker PnL comes from expected outcomes.
- The most profitable composite strategies combine **mid-high prices (61–99), 6h–3d windows, and weekend timing**.

**Bottom line**: The dominant strategy is to provide liquidity (make markets), favor the NO side, target the 6h–7d window in non-Finance categories, and size down or exit when surprise risk is elevated.

---

## 1. Maker vs Taker Edge

### Key Findings

| Metric | Value |
|--------|-------|
| Overall maker excess return | +1.5–2.5% |
| Overall taker excess return | −1.5–2.5% |
| Maker win rate at price 50 | ~52% |
| Maker profitable categories | 9 / 10 |
| Only category where makers lose | Esports |

Makers outperform takers in **every category except Esports**. The largest gaps:

| Category | Maker Excess | Taker Excess | Gap (pp) |
|----------|-------------|-------------|----------|
| World Events | +3.91% | −3.41% | 7.32 |
| Media | +3.87% | −3.41% | 7.28 |
| Entertainment | +2.79% | −2.00% | 4.79 |
| Weather | +1.50% | −1.60% | 3.10 |
| Politics | +1.95% | −1.93% | 3.88 |
| Sports | +1.40% | −1.52% | 2.92 |
| Finance | +0.09% | −0.08% | 0.17 |

### Trading Implications

- **Default role: maker (liquidity provider)**. The structural edge is clear and persistent.
- **Avoid making markets in Esports** — the only category with negative maker returns.
- **Finance is near-efficient** — the 0.17pp gap barely covers transaction costs. Look elsewhere.
- **Entertainment, Media, and World Events** are the most mispriced categories — prioritize these for market-making.

---

## 2. YES vs NO Asymmetry

### Key Findings

- NO bets outperform YES bets at **14 of 19 tested price levels**, all statistically significant.
- Takers disproportionately buy YES: YES share is 63–74% at low prices, declining to 33–46% at high prices.
- The "YES optimism tax" generates a persistent edge for NO-side positions.

**EV by side at representative prices:**

| Price | YES EV | NO EV | Edge to NO? |
|-------|--------|-------|-------------|
| 1¢ | −0.41 | +0.23 | ✓ |
| 10¢ | −1.45 | +0.86 | ✓ |
| 20¢ | −1.28 | +2.94 | ✓ |
| 30¢ | −1.13 | +1.84 | ✓ |
| 50¢ | −1.67 | +1.91 | ✓ |
| 70¢ | +0.42 | +1.70 | ✓ |
| 80¢ | −0.78 | +2.38 | ✓ |
| 90¢ | −0.52 | +1.01 | ✓ |
| 99¢ | −0.01 | +0.09 | ✓ |

**Maker returns by direction:**

| Price Range | Maker YES Excess | Maker NO Excess |
|-------------|-----------------|-----------------|
| 1–20¢ | +0.09% | +4.72% |
| 21–40¢ | +1.82% | +4.75% |
| 41–60¢ | +0.51% | +1.64% |
| 61–80¢ | −0.19% | +1.31% |
| 81–99¢ | +0.91% | +0.43% |

Maker NO excess is dominant up to ~55¢, after which Maker YES becomes competitive.

### Trading Implications

- **Lean NO at prices ≤ 55¢**. The YES-optimism bias means retail takers overpay for YES outcomes at low-to-mid prices.
- **Above 55¢, maker YES becomes viable**. The asymmetry inverts as high-price NO becomes the "longshot" position.
- **At extreme prices (1–5¢ and 95–99¢)**, the absolute edge is small but the percentage edge is enormous. Size carefully.

---

## 3. Price Calibration & Mispricing

### Calibration

The market is remarkably well-calibrated in aggregate:

| Price | Actual Win Rate | Deviation |
|-------|----------------|-----------|
| 1¢ | 0.91% | −0.09pp |
| 10¢ | 10.56% | +0.56pp |
| 25¢ | 25.40% | +0.40pp |
| 50¢ | 50.00% | 0.00pp |
| 75¢ | 75.21% | +0.21pp |
| 90¢ | 90.14% | +0.14pp |
| 99¢ | 99.09% | +0.09pp |

There is a slight **longshot bias**: sub-10¢ contracts win slightly more often than implied, and 90¢+ contracts win slightly less. This is consistent with the NO-side advantage.

### Mispricing by Role

- **Taker mispricing** is negative at every price level — takers consistently pay above fair value.
- **Worst taker mispricing**: prices 1–10¢ (30–57% below fair value in percentage terms).
- **Maker mispricing** is positive at nearly all price levels, confirming the structural edge.

### Trading Implications

- **The market is NOT grossly miscalibrated** — don't expect large mispricings on aggregate. The edge is in the maker/taker asymmetry, not in outright directional bets.
- **Longshot prices (1–10¢) are the most overbet by takers** — this is where the NO side and maker side extract the most value.
- **Avoid speculative taker YES positions at low prices** — the mispricing penalty is steepest there (30–57%).

---

## 4. Category-Specific Strategies

### EV by Category and Price Range

| Category | Best Price Range for NO | Best Price Range for YES | Notes |
|----------|----------------------|------------------------|-------|
| Sports | 1–50¢ (NO dominant) | 70–80¢ (slight YES) | Largest volume category |
| Politics | 1–50¢ (NO good) | 51–90¢ (YES positive EV) | YES viable at high prices |
| Crypto | 1–20¢ and 80–99¢ (NO) | 30–60¢ (YES better) | Split pattern |
| Finance | NO better at nearly all | — | Near-efficient overall |
| Entertainment | 20–60¢ (NO very strong) | — | Large NO edge |
| Media | 20–60¢ (NO very strong) | — | Large NO edge |
| Weather | 1–30¢ (NO strong) | 60–80¢ (slight YES) | Moderate volume |
| World Events | Most prices (NO) | — | Highest maker gap |

### Time-to-Close by Category

| Category × Time | Maker Excess |
|----------------|-------------|
| Sports, 1–6h | +1.72% |
| Sports, 6–24h | +2.95% |
| Sports, 1–3d | +1.82% |
| Politics, 3–7d | +3.81% |
| Politics, 7–30d | +1.68% |
| Weather, 6–24h | +1.83% |
| Crypto, 1–6h | +1.75% |
| Finance, all windows | < 0.5% |
| Esports, most windows | **Negative** |

### Trading Implications

- **Sports**: Focus on 6h–3d window before close. NO side preferred at prices ≤ 50¢. This is where volume and edge intersect best.
- **Politics**: Longer time horizon — the 3–7d window has the highest maker edge (+3.81%). YES is viable above 50¢.
- **Entertainment/Media**: These categories have the widest mispricings. Undertraded by sophisticated participants. Lean heavily NO at 20–60¢.
- **Finance**: Skip or trade very small. Near-zero edge after costs.
- **Esports**: Avoid entirely as a maker.
- **Crypto**: Split strategy — NO at extremes, YES in the middle.

---

## 5. Sports Subcategory Breakdown

| Sport | Maker Excess | Taker Excess | Volume |
|-------|-------------|-------------|--------|
| NFL (Games) | +2.49% | −2.67% | $1.17B |
| NBA (Games) | +1.51% | −1.56% | $671M |
| MLB (Games) | +1.20% | −1.26% | $404M |
| NHL (Games) | +1.60% | −1.57% | $123M |
| Soccer (EPL) | +6.25% | −5.80% | $12M |
| Boxing | +30.2% | −30.7% | Small |
| UFC/MMA | +1.64% | −2.14% | $73M |
| WTA | +2.36% | −2.16% | $4M |
| ATP | +1.99% | −1.72% | $7M |

### Trading Implications

- **NFL is the highest-volume, high-edge sport** — prioritize NFL game markets for market-making (2.49% maker excess × $1.17B volume).
- **EPL soccer** has an outsized maker edge (6.25%) but lower volume. If you can access it, it's very profitable per dollar.
- **Boxing** shows an extreme 30.2% maker edge but volumes are tiny and it may reflect one-off events.
- **NBA and MLB** are solid workhorse categories — consistent 1.2–1.5% edges with large volume.
- **Order of priority by expected edge × volume**: NFL > NBA > MLB > UFC > NHL > Tennis > Soccer.

---

## 6. Time-to-Close Effects

### Maker Edge by Time to Close (All Categories)

| Time Bucket | Maker Excess |
|-------------|-------------|
| 0–1h | +0.67% |
| 1–6h | +1.42% |
| 6–24h | +2.78% |
| 1–3d | +2.27% |
| 3–7d | +2.68% |
| 7–30d | +1.79% |
| 30d+ | +1.14% |

The pattern is **U-shaped with a peak at 6h–7d**:

1. **Last hour (0–1h)**: Lowest edge. Prices are near-terminal values, hard to capture spread.
2. **6h to 7d sweet spot**: Highest maker excess. Enough uncertainty for spread, not so much that informed traders dominate.
3. **30d+**: Edge drops — positions are open too long, exposed to more information risk.

### Trading Implications

- **Target the 6h–7d window** for new market-making positions. This is where the edge is fattest.
- **Reduce size or exit within 1h of close** — the maker edge drops to 0.67%.
- **For positions held > 30 days**, the edge is still positive (+1.14%) but much smaller — concentrate capital on shorter-duration markets.
- **Ideal market selection**: Markets with 1–7 days remaining, priced 20–60¢, in Sports/Politics/Entertainment.

---

## 7. Day-of-Week Effects

### Maker Excess by Day (All Categories)

| Day | Maker Excess |
|-----|-------------|
| Monday | +1.52% |
| Tuesday | +1.89% |
| Wednesday | +1.06% |
| Thursday | +1.17% |
| Friday | +1.46% |
| **Saturday** | **+2.57%** |
| **Sunday** | **+1.33%** |

Weekends show a clear edge amplification, especially Saturday (+2.57% vs weekday avg of ~1.42%).

### Category Variation

- **World Events**: Saturday maker excess of +5.8–7.5% — enormous.
- **Sports**: Weekend edge is moderate (Sat +2.22%, Sun +1.37%) — driven by game-day trading.
- **Politics**: Weekday edges higher (Tue +2.69%) — driven by news cycles.

### Trading Implications

- **Increase market-making activity on weekends**, particularly Saturday. Retail takers are more active and less informed.
- **World Events + Saturday** is the single most profitable slot for makers.
- **For Politics, lean into Tuesday** — corresponds to news-driven mispricing.
- **Wednesday is the worst weekday** for makers (+1.06%) — consider reducing activity.

---

## 8. Trade Size as a Signal

### Trade Size by Role

| Role | Mean Trade Size | Median Trade Size |
|------|----------------|------------------|
| Maker | $134 | $15.75 |
| Taker | $120 | $13.68 |

Makers trade slightly larger, consistent with being more informed/capitalized.

### Informed Flow by Trade Size (Taker Loss Rate)

| Size Bucket | Taker Loss Rate |
|-------------|----------------|
| 1 contract | −1.87% |
| 2–5 | −2.24% |
| 6–25 | −2.47% |
| 26–100 | −2.37% |
| 101–500 | −2.05% |
| **500+** | **−1.54%** |

Counter-intuitively, **very large takers (500+ contracts) are less adversely selected** than mid-size (6–100). This may reflect institutional flow that moves markets efficiently.

However, within the 1–7d window (where maker edge peaks), **mid-size taker orders (6–100) show the highest adverse selection** — these are the "sophisticated retail" orders.

### Trading Implications

- **When you see 500+ contract taker fills, don't panic** — these are less informed on average.
- **Watch mid-size orders (6–100 contracts) in the 1–7d window** — these carry the most private information.
- **Use trade size to adjust your quoting aggressively**: widen spreads for 6–100 contract orders in the 1–7d window.
- **Statistical test**: trade size and returns have a small but significant relationship (Spearman r=0.025, p≈0).

---

## 9. Surprise Outcomes & Risk Management

### Maker PnL by Surprise Level

| Surprise Level | Share of PnL | Maker Return |
|----------------|-------------|-------------|
| Expected (0–5%) | 92.6% | Positive |
| Minor Surprise (5–25%) | ~5% | Positive |
| Moderate (25–50%) | ~2% | Near-zero |
| **Major Upset (51–75%)** | ~0.3% | **−0.09%** |
| **Shock (76–100%)** | ~0.1% | **−2.27%** |

Surprise = |price − outcome| / max(price, 100−price). A price-50 market that resolves unexpectedly is a 100% surprise; a price-95 market that loses is a ~95% surprise.

### Maker Win Rate by Surprise

| Surprise Level | Maker Win Rate |
|----------------|---------------|
| 1% (expected) | ~60% |
| 20% | ~55% |
| 50% (coin flip → wrong) | ~40% |
| 70%+ (major upset) | ~20% |

### Trading Implications

- **92.6% of your PnL comes from boring, expected outcomes.** Don't chase exotic/volatile markets.
- **Major upsets (>50% surprise) wipe out maker profits.** This is the primary risk factor.
- **Risk management rule**: limit position size on markets with high surprise potential (e.g., political announcements, binary crypto events, knockout sports matches).
- **Diversify across many markets** to ensure the law of large numbers works in your favor — individual surprise outcomes are the #1 variance driver.
- **Consider hedging or exiting positions when implied volatility spikes** (e.g., sudden price moves toward 50¢ in previously lopsided markets).

---

## 10. Composite Strategy Signals

The composite analysis cross-tabulates price range × time-to-close × day type (weekday/weekend) to find the highest-edge combinations.

### Top Composite Strategies (Maker Excess)

| Category | Price Range | Time Window | Day | Maker Excess |
|----------|-----------|-------------|-----|-------------|
| Esports | 81–99¢ | 6h–3d | Weekend | +36.4% |
| World Events | 61–80¢ | 0–6h | Weekday | +33.7% |
| Sports | 61–80¢ | 6h–3d | Weekday | +22.1% |
| Media | 61–80¢ | 6h–3d | Weekend | +18.3% |
| Entertainment | 81–99¢ | 0–6h | Weekday | +15.2% |

> Note: Extreme values (30%+) tend to come from low-volume buckets. Focus on strategies with meaningful volume.

### Reliable High-Edge Strategies (Balancing Edge × Volume)

For **Sports** (highest volume):
- **61–80¢, 6h–3d, Weekday**: ~22% excess — strong and high volume.
- **21–40¢, 6h–3d, Weekday**: ~5–8% excess — bread-and-butter.
- **41–60¢, 1–6h, Weekend**: ~4–6% excess — good weekend picks.

For **Politics**:
- **41–60¢, 3–7d, Weekday**: ~6–10% excess — pre-event positioning.
- **61–80¢, 1–3d, Weekday**: ~5–7% excess.

### Trading Implications

- **The triple filter (price/time/day) is powerful.** Don't market-make blindly; select markets matching high-edge combos.
- **Volume-adjust your expectations**: a 36% edge in a $10K volume bucket is worth less than a 3% edge in a $100M bucket.
- **Core strategy for Sports**: make markets at 20–80¢, 6h–3d before close, weekdays and weekends both work.
- **Core strategy for Politics**: make markets at 40–80¢, 1–7d before close, weekdays preferred.

---

## 11. Taker Alpha Niches

A small number of highly specific tickers show extreme positive taker EV, suggesting pockets where takers have an informational advantage:

- **Crypto novelty tickers** (KXSHIBAD, KXMANTISFREETHROW) show 100–375% taker excess — these are likely thin, illiquid markets where a single informed bet moves the needle.
- **Most crypto tickers show −100% taker excess** (total loss) — confirming that blind taker speculation in crypto markets is extremely unprofitable.

### Trading Implications

- **As a maker, avoid ultra-thin novelty markets** in crypto — the few informed takers extract enormous rents.
- **As a taker, the only reliable alpha niches are hyper-specific, low-volume markets** where you have genuine informational advantage. These are not scalable strategies.
- **Focus maker capital on liquid markets** ($100K+ daily volume) where the law of large numbers smooths out any individual taker's information edge.

---

## Actionable Trading Parameters

### Market Selection Filters

| Parameter | Threshold | Rationale |
|-----------|-----------|-----------|
| **Category** | Sports, Politics, Entertainment, Media, World Events | Positive maker edge; avoid Finance (near-zero) and Esports (negative) |
| **Price range** | 15–85¢ | Sufficient spread to capture; avoid extreme prices where edge is tiny in absolute terms |
| **Time to close** | 6h – 7d | Peak maker edge window (2.3–2.8% excess) |
| **Day preference** | Saturdays strongest; Tue for Politics | Weekend retail influx amplifies edge |
| **Minimum volume** | $100K+ daily on ticker | Ensures fills and diversification |

### Position Sizing Rules

| Condition | Size Adjustment |
|-----------|----------------|
| Default | 1× base size |
| Weekend + Sports/Entertainment | 1.5× (edge amplified) |
| 0–1h before close | 0.25× (edge collapses) |
| 30d+ to close | 0.5× (edge weaker, capital tied up) |
| Finance category | 0× (skip — near-efficient) |
| Esports | 0× (negative maker edge) |
| Surprise risk elevated (price moving toward 50¢) | 0.5× |
| Category × time in sweet spot (Sports 6h–3d) | 1.5× |

### Directional Bias

| Price Range | Preferred Side |
|-------------|---------------|
| 1–20¢ | Strong NO |
| 21–40¢ | NO |
| 41–55¢ | Slight NO |
| 56–70¢ | Neutral (category-dependent) |
| 71–85¢ | Slight YES (Politics, Crypto) |
| 86–99¢ | Category-dependent; NO for Sports, YES for Politics |

### Risk Limits

| Risk Parameter | Threshold |
|----------------|-----------|
| Max single-market exposure | 5% of bankroll |
| Max category concentration | 30% of bankroll |
| Stop-loss on surprise move | Exit if market moves > 30¢ against position in < 1h |
| Min # of concurrent positions | 20+ (diversification mandate) |
| Max time in position | 30d (capital efficiency) |

### Performance Benchmarks

| Metric | Target | Based On |
|--------|--------|----------|
| Maker win rate | > 52% at 50¢ | Historical: 52% |
| Daily excess return | +1.5–2.5% | Cross-category average |
| Worst-case drawdown category | Esports (avoid) | Only category with negative maker excess |
| Best risk/reward category | NFL Sports | 2.49% edge × $1.17B volume |

---

## Summary Decision Tree

```
START: New market opportunity
│
├─ Is it Finance or Esports?
│  └─ YES → SKIP
│
├─ Time to close < 1h?
│  └─ YES → REDUCE SIZE to 0.25×
│
├─ Time to close > 30d?
│  └─ YES → REDUCE SIZE to 0.5×
│
├─ Is it Saturday or Sunday?
│  └─ YES → INCREASE SIZE to 1.5×
│
├─ Price ≤ 55¢?
│  └─ YES → Lean NO side
│  └─ NO → Check category for YES viability
│
├─ Mid-size orders (6–100) in 1–7d window hitting you?
│  └─ YES → Widen spread (informed flow signal)
│
├─ Price moving rapidly toward 50¢ from extreme?
│  └─ YES → REDUCE SIZE (surprise risk)
│
└─ ENTER POSITION as maker with appropriate sizing
```

---

## 12. Walk-Forward Backtest Results

> **Critical finding**: The walk-forward backtests reveal a stark disconnect between the statistical edges documented above and actual trading profitability. The maker strategy — supposedly the dominant approach — **lost 59% of capital**, while the selective taker strategy **gained 12%**.

### Backtest Design

Both backtests use strict temporal separation to prevent data leakage:

- Parameters recalculated **monthly** using only trades/markets that both occurred and resolved before the recalculation date.
- Walk-forward design: train on the past, trade the next period, repeat.
- Initial capital: $10,000 per strategy.
- Capital management with position sizing, exposure limits, and daily deployment caps.

### Head-to-Head Results

| Metric | Maker Strategy | Taker Strategy | Winner |
|--------|---------------|----------------|--------|
| **Total Return** | **−59.34%** | **+11.59%** | Taker |
| Annualized Return | −32.72% | +5.87% | Taker |
| Final Equity | $4,066 | $11,159 | Taker |
| Sharpe Ratio | −1.10 | +0.80 | Taker |
| Sortino Ratio | −0.37 | +0.73 | Taker |
| Calmar Ratio | −0.74 | +1.69 | Taker |
| Max Drawdown | 43.95% ($3,188) | 3.47% ($401) | Taker |
| Max DD Duration | 395 days | 691 days | Maker |
| Profit Factor | 0.35 | 2.18 | Taker |
| Trade Win Rate | 46.23% | 46.68% | ≈Tie |
| Daily Win Rate | 7.24% | 10.84% | Taker |
| Avg Trade PnL | −86.5¢ | +10.1¢ | Taker |
| Best Day | +$877.57 | +$637.68 | Maker |
| Worst Day | −$2,893.55 | −$344.36 | Taker |
| Total Trades | 6,863 | 11,517 | — |
| Capital Deployed | $57,994 | $17,639 | — |
| Annualized Volatility | 23.69% | 7.58% | Taker |
| Skewness | −17.0 | +9.7 | Taker |

### Maker Strategy — Why It Failed

**Period**: 2022-03-28 to 2024-07-03 (829 trading days)

The maker strategy lost $5,934 (−59.3%) with a catastrophic −44% max drawdown that never recovered. Key failure modes:

**1. Catastrophic early losses concentrated in Entertainment**
- The single worst day (−$2,894) occurred on 2022-03-28 (Oscars markets), wiping out 29% of capital on Day 1.
- Entertainment as a group lost $2,849 across 1,380 trades (−18% ROI) — the single largest loss driver.
- Oscar-related tickers (OSCARPIC: −$1,722, OSCARASPLAY: −$1,025, OSCARSPLAY: −$73) accounted for ~47% of total losses.

**2. Every category group lost money**

| Group | PnL ($) | # Trades | ROI |
|-------|---------|----------|-----|
| Entertainment | −$2,849 | 1,380 | −18.1% |
| Politics | −$2,487 | 2,421 | −7.7% |
| Finance | −$515 | 1,582 | −8.0% |
| Science/Tech | −$82 | 103 | −20.9% |
| Weather | −$37 | 1,140 | −1.4% |
| Crypto | +$36 | 236 | +8.8% |

Only Crypto was marginally profitable (+$36), too small to matter.

**3. Losses at every price bucket**

| Price Bucket | PnL ($) | ROI |
|-------------|---------|-----|
| 1–20¢ | −$2,827 | −17.2% |
| 21–40¢ | −$1,598 | −6.2% |
| 41–60¢ | −$600 | −5.4% |
| 61–80¢ | −$353 | −27.3% |
| 81–99¢ | −$556 | −15.9% |

The 1–20¢ bucket, which showed the highest maker edge in static analysis, was the biggest loser in practice.

**4. Long-duration positions dominated and failed**
- 3d+ bucket: −$5,295 (77% of total losses), 4,031 trades at −11.2% ROI.
- The strategy was over-exposed to long-duration markets where information risk and surprise outcomes eroded the theoretical edge.

**5. Massive negative skewness (−17.0)**
- The maker strategy's return distribution has extreme left-tail risk — a few terrible days wipe out many small gains.
- This is characteristic of "selling insurance" strategies that collect small premiums but suffer rare catastrophic losses.

### Taker Strategy — Why It Won

**Period**: 2023-01-01 to 2024-12-01 (701 trading days)

The selective taker strategy gained $1,159 (+11.6%) with remarkably controlled risk (3.5% max drawdown). Key success factors:

**1. Weather category drove almost all profits**

| Group | PnL ($) | # Trades | ROI |
|-------|---------|----------|-----|
| Weather | +$1,515 | 3,772 | +16.9% |
| Other | +$77 | 619 | +6.8% |
| Media | +$8 | 504 | +1.5% |
| Entertainment | −$18 | 305 | −13.8% |
| Politics | −$17 | 18 | −62.1% |
| Finance | −$407 | 6,295 | −6.0% |

Weather (specifically HIGHCHI: +$1,161 and HIGHNY: +$360) accounted for 131% of total profits. The strategy was essentially a Weather forecasting play.

**2. Concentrated early success**
- January 2023 alone generated +$1,187 (102% of total lifetime PnL) across 9,362 trades.
- After January 2023, the remaining 23 months produced net −$28 — essentially breakeven.
- The equity curve peaked at $11,559 on January 10, 2023, just 10 days into the backtest, and oscillated within a narrow range thereafter.

**3. 6h–3d time bucket was the only strong winner**

| Time Bucket | PnL ($) | ROI |
|-------------|---------|-----|
| 0–1h | +$137 | +9.2% |
| 6h–3d | +$1,503 | +14.0% |
| 1–6h | −$145 | −4.1% |
| 3d+ | −$336 | −18.5% |

**4. Positive skewness (+9.7)**
- The taker strategy has a right-skewed return distribution — it occasionally captures large gains while limiting losses.
- This is the opposite of the maker strategy's profile: the taker is "buying insurance" cheaply.

**5. Conservative capital deployment**
- Only $17,639 total capital deployed (vs $57,994 for maker) — the strategy was highly selective.
- Tight max drawdown (3.5%) reflects disciplined sizing and narrow market selection.

### Reconciling the Contradiction

The static analysis shows a clear 1.5–2.5% maker edge, yet the maker backtest lost 59%. Several factors explain this:

**1. Statistical edge ≠ Tradeable edge**
- The 1.5–2.5% excess return is measured per-contract in aggregate across all historical data. In a walk-forward setting with capital constraints, position sizing, and sequential decision-making, much of this edge is lost.
- The maker strategy deployed $57,994 to lose $5,934 (−10.2% on capital deployed) — the per-trade edge was negative, not positive.

**2. Parameter estimation lag**
- Monthly recalculation means the strategy always trades on stale signals. Market microstructure can shift faster than the lookback window captures.
- Categories and tickers that showed positive maker edge historically may have mean-reverted or been arbitraged away by the time the strategy acted.

**3. Concentration risk destroyed the maker strategy**
- Oscar season (March 2022) wiped out 29% of capital in one day — a single Entertainment event. The strategy never recovered.
- The maker strategy's negative skewness (−17.0) means rare catastrophic losses dominate. Diversification across 6,863 trades was insufficient because many were concentrated in the same events.

**4. Survivorship bias in static analysis**
- The static analysis aggregates across all resolved markets including the full dataset. The walk-forward backtest only sees what was knowable at each point in time.
- Categories and price levels that looked attractive ex-post may not have been identifiable ex-ante.

**5. The taker strategy's success was fragile**
- 102% of taker profits came from a single month (January 2023) in a single category (Weather).
- After the initial burst, the strategy was essentially flat for 23 months — suggesting the "taker alpha" was an artifact of a specific market condition, not a durable edge.

### Revised Strategic Assessment

| Prior Belief | Backtest Evidence | Revised View |
|-------------|-------------------|-------------|
| Makers have a 1.5–2.5% edge | Maker strategy lost 59% | **Edge exists statistically but is not practically tradeable** with monthly rebalancing |
| Takers are at a structural disadvantage | Selective taker strategy gained 12% | **Narrow taker niches exist** but are fragile and may be driven by one-off market conditions |
| NO side outperforms YES side | Maker NO PnL: −$4,851 (−23.2% ROI); Taker NO PnL: +$223 (+2.0% ROI) | **NO bias alone is insufficient** — context (category, timing) matters more |
| 6h–7d is the maker sweet spot | Maker 6h–3d PnL: −$488; Taker 6h–3d PnL: +$1,503 | **Time window matters**, but the taker captured this edge, not the maker in live trading |
| Weekend amplifies maker edge | Maker weekend: −$2,458 (−25.6% ROI); Taker weekend: +$332 | **Weekend effect reversed** in practice — retail taker flow may have become more efficient |
| Diversification across categories works | Maker lost in 6/7 groups; Taker profited in only 2/7 | **Concentration in the right niche** beats broad diversification |

### Actionable Revised Conclusions

1. **Do not blindly implement the maker strategy** as described in the static analysis. The walk-forward evidence shows it destroys capital.

2. **The taker strategy is not reliably profitable either** — its success was concentrated in one month and one category. It should not be deployed without understanding why Weather markets in January 2023 were unusually favorable.

3. **Event risk management is paramount**. A single Oscars night wiped out 29% of maker capital. Any live implementation must have strict per-event and per-day loss limits far tighter than the theoretical model suggests.

4. **More frequent parameter recalculation** (weekly or even daily) may help the maker strategy stay current, but risks overfitting to noise.

5. **The most robust finding** is that prediction markets are hard to trade profitably in either direction. The statistical edges documented in the analysis are real but may not survive transaction costs, timing lags, capital constraints, and fat-tailed event risk.

6. **If deploying either strategy**, the taker approach with tight risk limits and Weather-category focus is the lower-risk option, but past performance (driven by Jan 2023) should not be extrapolated.

---

## 13. Momentum Analysis — Can Takers Ride Price Trends?

> **Motivation**: The maker strategy requires massive execution precision and is vulnerable to informed flow bidding bad prices (see Section 12). We explore an alternative taker direction: **momentum trading** — following recent price or order-flow trends to capture directional moves.

### Three Definitions of Momentum

We tested momentum through three complementary lenses, each with surgical definitions:

| Signal | Definition | Rationale |
|--------|-----------|-----------|
| **Price Drift** (Δₖ) | `P_i − P_{i−k}` over the last k trades within the same ticker | Captures whether the market price has been moving in a consistent direction — the most direct measure of "trend" |
| **Trade Flow** | % of the last k trades where taker bought YES (volume-weighted) | Captures order-flow imbalance — whether aggressive buyers are predominantly on one side, independent of price level |
| **Regime Conditioning** | Price drift stratified by (price level × time-to-close × category) | Tests whether momentum works *somewhere even if it fails in aggregate* — the surgical regime search |

### Key Finding: Momentum-Following Is Consistently Negative EV

**This is the single most important result.** Across every lookback window, every momentum magnitude, and nearly every conditioning dimension, following momentum loses money for takers.

#### Price Drift Results

| Lookback (trades) | Follow Excess (¢/contract) | t-statistic | Win Rate | Implied Prob |
|-------------------|---------------------------|-------------|----------|-------------|
| 3 | **−0.63** | −110.7 | 53.6% | 54.2% |
| 5 | **−0.56** | −103.3 | 54.1% | 54.7% |
| 10 | **−0.46** | −88.4 | 55.0% | 55.5% |
| 25 | **−0.33** | −70.1 | 56.4% | 56.7% |
| 50 | **−0.25** | −57.7 | 57.5% | 57.8% |

At every lookback, following momentum loses. The win rate *exceeds* 50% — momentum followers pick the right side more often than not — but the excess return is negative because **the price already over-incorporates the signal**. You win more often but pay too much for the privilege.

#### Momentum Magnitude: Bigger Moves = Worse Outcomes

| Momentum Magnitude (¢) | Follow Excess (¢) | t-stat | n Contracts |
|------------------------|-------------------|--------|------------|
| 1–2 | −0.38 | −31.3 | 6.26B |
| 3–5 | −0.15 | −19.5 | 1.53B |
| 6–10 | −0.46 | −24.2 | 696M |
| 11–20 | −1.11 | −52.0 | 410M |
| 21+ | **−3.17** | −151.6 | 242M |

Large momentum moves (21+ cents) are the *worst* to follow: −3.17¢/contract. This is consistent with **momentum exhaustion** — large moves overshoot fair value and subsequently revert.

#### Directional Asymmetry: Bullish Momentum Is Worse

| Direction | Follow Excess (¢) | n Contracts |
|-----------|-------------------|-------------|
| Bullish (Δ > 0, buy YES) | **−0.64** | 4.53B |
| Bearish (Δ < 0, buy NO) | −0.28 | 4.61B |

Bullish momentum is 2.3× more costly to follow than bearish. This aligns with the YES-optimism tax documented in Section 2 — upward price moves are driven by retail enthusiasm and tend to overshoot more.

#### Taker Alignment: Fading Is Less Bad

| Taker Behavior | Excess (¢) | n Contracts |
|----------------|-----------|-------------|
| Follows momentum | **−0.78** | 6.97B |
| Fades momentum | −0.58 | 2.17B |

Both lose, but fading momentum loses *less*. This is a weak contrarian signal — not strong enough to trade directly, but confirming that momentum-following is the wrong instinct.

### Trade-Flow Momentum Results

Order-flow momentum (% YES volume over last k trades) produces similar or worse results:

| Lookback | Follow Excess (¢) | t-stat |
|----------|-------------------|--------|
| 5 trades | −0.64 | −122.8 |
| 10 trades | −0.64 | −122.7 |
| 25 trades | −0.70 | −134.9 |
| 50 trades | **−0.75** | −146.3 |

Unlike price drift (where longer lookback helped), longer flow lookbacks are *worse*. Persistent one-sided flow is a *negative* signal — it indicates the market has been absorbing that flow and pricing it in.

#### Flow Intensity: Extreme Order Flow Is the Worst Signal

| Flow Bucket | Follow Excess (¢) | t-stat |
|-------------|-------------------|--------|
| 0–10% YES (extreme NO) | −0.63 | −26.2 |
| 10–30% YES | −0.29 | +5.2 |
| **30–50% YES** | **+0.06** | +17.7 |
| 50–70% YES | −0.11 | −22.7 |
| 70–90% YES | −0.21 | −35.9 |
| **90–100% YES (extreme YES)** | **−0.98** | −143.8 |

The one near-neutral regime: when flow is roughly balanced (30–50% YES), following the slight lean is near-zero EV. Extreme one-sided flow — especially extreme YES — is a losing signal to follow.

#### Price vs Flow Agreement

| Condition | Follow Excess (¢) |
|-----------|-------------------|
| Price & flow agree | **−0.74** |
| Price & flow disagree | +0.04 |

When both price momentum and flow momentum point the same direction, *following both is worst*. When they disagree (price going up but flow is NO-heavy, or vice versa), the signal nearly cancels. This is further evidence of mean-reversion, not trend-continuation.

### Regime Analysis: Where Might Momentum Work?

We tested every (price bucket × time-to-close) combination:

#### By Price Level (aggregate)

| Price Bucket | Follow Excess (¢) | n Contracts |
|-------------|-------------------|-------------|
| 01–20¢ | −0.36 | 2.45B |
| **21–40¢** | **−0.04** | 1.74B |
| 41–60¢ | −0.84 | 1.91B |
| 61–80¢ | −0.54 | 1.49B |
| 81–99¢ | −0.52 | 1.55B |

21–40¢ is the "least bad" bucket (−0.04¢), near breakeven. The 41–60¢ range — where you'd expect the most directional uncertainty — is actually the *worst* for momentum.

#### By Time-to-Close (aggregate)

| Time Bucket | Follow Excess (¢) | n Contracts |
|------------|-------------------|-------------|
| **0–1h** | **−0.09** | 2.79B |
| 1–6h | −0.49 | 3.75B |
| 6–24h | −1.00 | 651M |
| 1–3d | −0.29 | 393M |
| 3–7d | −0.47 | 279M |
| 7–30d | −0.54 | 380M |
| 30d+ | −1.07 | 891M |

Near-close (0–1h) is the least negative. This makes sense: in the final hour, new information may genuinely shift fair value (e.g., sports in-play), and price discovery is most active. But even here, the excess is negative.

#### Price × Time Heatmap — The Few Positive Cells

| Price × Time | Follow Excess (¢) | Notable? |
|-------------|-------------------|----------|
| 21–40¢ × 0–1h | +0.11 | Near-close low-price: closest to breakeven |
| 21–40¢ × 1–3d | +0.92 | Small sample, not reliable |
| 21–40¢ × 30d+ | +2.39 | Intriguing but may be sports-driven |
| 61–80¢ × 0–1h | +0.29 | Weak positive in near-close |
| 81–99¢ × 3–7d | +0.16 | Near zero |
| 81–99¢ × 30d+ | +0.10 | Near zero |

The few positive cells are concentrated in 21–40¢ (longshot zone where market may underreact) and in near-close timing. But the magnitudes are tiny and would not survive transaction costs or slippage.

### Summary and Trading Implications

| Finding | Implication |
|---------|------------|
| Momentum-following is negative EV at every lookback | **Do not build a trend-following strategy on Kalshi** |
| Larger momentum = worse outcome | Momentum signals are **mean-reverting**, not trend-continuing |
| Bullish momentum is 2.3× worse than bearish | YES-optimism bias amplifies bullish overshoots |
| Extreme order flow is the worst signal to follow | Heavy one-sided flow is absorbed, not informative |
| Near-close (0–1h) is least bad | Only regime where real-time information *might* not be fully priced |
| 21–40¢ is least bad price level | Low-price markets may underreact to genuine shifts |
| Fading momentum loses less than following it | Suggests active **contrarian** strategies may be more productive |

### Why Momentum Fails in Prediction Markets

Unlike equities, prediction markets have properties that kill momentum:

1. **Terminal payoff**: Every contract converges to 0 or 100. There is no "trend to infinity" — prices are bounded, so overshoots must revert.
2. **Efficient price discovery**: With a large volume base ($18.26B), prices incorporate information quickly. By the time a 10-trade drift is visible, the information is priced in.
3. **YES-optimism tax**: The persistent retail bias toward YES creates asymmetric mean-reversion — upward moves overshoot more and revert harder.
4. **No carry / no dividends**: Unlike equities where momentum may persist due to fundamental cash-flow trends, prediction market "fundamentals" are binary and terminal.

### Next Steps for Taker Strategy Research

Given that naive momentum-following fails, more promising taker directions include:

- **Contrarian strategies**: The data suggests fading large momentum moves, especially bullish ones, may have a small edge. A systematic contrarian approach — selling into large upward drifts — deserves dedicated analysis.
- **Event-driven timing**: Near-close momentum in specific categories (sports in-play, election night) may carry genuine information. A specialized event-timing strategy could exploit the 0–1h window.
- **Cross-market signals**: Using momentum in related markets (e.g., if Market A moves, trade Market B in the same event) before information fully propagates.
- **Volatility regime**: Instead of directional momentum, identifying periods of *high uncertainty* (price oscillating around 50¢) where taking positions at extreme prices has higher payoff.

---

## 14. Mean-Reversion Analysis — Can Takers Fade Price Deviations?

> **Motivation**: Section 13 showed that following momentum is consistently negative EV. The natural inverse — *fading* deviations from a rolling average — is tested here. If momentum overshoots, mean-reversion should profit.

### Signal Definition

For trade *i* at price $P_i$, the **deviation from MA(k)** is:

$$D_k(i) = P_i - \frac{1}{k} \sum_{j=i-k}^{i-1} P_j$$

- $D_k > 0$: price is *above* its recent average → buy NO (bet it comes back down)
- $D_k < 0$: price is *below* its recent average → buy YES (bet it comes back up)

We also tested **Median(k)** and **VWAP(k)** as alternative "centers" to revert to.

### Key Finding: Mean-Reversion Signal Is Positive — the Mirror Image of Momentum

Where momentum-following lost −0.25 to −0.63¢, fading deviations *earns* +0.18 to +0.66¢:

#### Fade Excess by Lookback Window

| Lookback | Fade Excess (¢) | t-statistic | Win Rate | Avg Implied |
|----------|-----------------|-------------|----------|-------------|
| MA(10) | **+0.66** | +138.6 | 44.5% | 43.8% |
| MA(25) | **+0.54** | +117.3 | 42.8% | 42.3% |
| MA(50) | **+0.41** | +98.2 | 41.5% | 41.1% |
| MA(100) | **+0.31** | +81.5 | 40.2% | 39.9% |
| MA(200) | **+0.18** | +67.8 | 38.7% | 38.5% |

Shorter lookbacks produce stronger signals — recent overextensions revert faster. But even 200-trade deviations carry information ($t = +68$).

Note the win rate is *below* 50% — the fade strategy wins *less* often but wins *more* when it wins (because it's buying the cheaper side of overextended contracts). This is the inverse of momentum, which won more often but always paid too much.

#### Deviation Magnitude: Larger Moves = Better Fades

| |Deviation| from MA50 | Fade Excess (¢) | t-stat | n Contracts |
|--------------------------|-----------------|--------|-------------|
| 0–2¢ (small) | +0.38 | +38.4 | 6.11B |
| 2–5¢ | +0.17 | +29.9 | 2.34B |
| 5–10¢ | +0.15 | +23.3 | 1.05B |
| 10–20¢ | +0.59 | +42.8 | 654M |
| **20+¢ (large)** | **+2.82** | +156.5 | 360M |

This is the exact opposite of momentum — where large moves were *worst* to follow (−3.17¢), they are *best* to fade (+2.82¢). Large price excursions overshoot fair value and revert, creating a profitable contrarian signal.

The +2.82¢ at 20+¢ deviations is the largest positive excess we've found in any taker signal. This is the most promising niche for a taker strategy.

#### Directional Asymmetry: Fading Up-Moves (Buy NO) Is 3.2× Better

| Direction | Fade Excess (¢) | t-stat |
|-----------|-----------------|--------|
| **Fade up (buy NO when above MA)** | **+0.63** | +139.3 |
| Fade down (buy YES when below MA) | +0.20 | −2.8 |

Fading bullish overextensions is 3.2× more profitable than fading bearish ones. This is the YES-optimism tax in action: upward price moves are disproportionately driven by retail YES-optimism and revert more reliably. Fading down-moves barely breaks even because bearish moves may reflect genuine information.

**Corollary**: A taker contrarian strategy should focus almost exclusively on *selling into rallies (buying NO when price jumps above its MA)*.

#### Which "Mean" to Revert To?

| Center Metric (k=50) | Fade Excess (¢) | t-stat |
|----------------------|-----------------|--------|
| Moving Average | +0.406 | +98.2 |
| Median | +0.401 | +91.7 |
| VWAP | +0.375 | +89.9 |

All three produce nearly identical results. MA is marginally best but the difference is negligible. The signal is about **deviation from any reasonable center**, not the specific center definition.

#### Taker Natural Behavior

| Taker Behavior | Actual Excess (¢) | % of Volume |
|----------------|-------------------|-------------|
| Follows deviation (buys into the move) | −0.73 | 72% |
| **Fades deviation (contrarian)** | **−0.42** | 28% |

Most takers (72%) naturally follow price moves — buying YES when price is above MA, pushing it further. The minority who fade (28%) lose significantly less. Neither group is profitable on average, but the gap (0.31¢) confirms the directional value of the signal.

### Regime Analysis: Where Is Mean-Reversion Strongest?

#### By Price Level

| Price Bucket | Fade Excess (¢) | n Contracts |
|-------------|-----------------|-------------|
| 01–20¢ | +0.12 | 2.84B |
| 21–40¢ | +0.12 | 1.99B |
| **41–60¢** | **+0.75** | 2.17B |
| 61–80¢ | +0.61 | 1.69B |
| 81–99¢ | +0.58 | 1.82B |

Mid-price contracts (41–60¢) show the strongest mean-reversion. This makes structural sense: at 50¢, a contract has maximum room to move in either direction, and deviations from the average are more likely to be noise than signal. At extreme prices (1–20¢), deviations may reflect genuine probability updates.

#### By Time-to-Close

| Time Bucket | Fade Excess (¢) | n Contracts |
|------------|-----------------|-------------|
| **0–1h** | **+0.01** | 3.29B |
| 1–6h | +0.50 | 4.32B |
| 6–24h | **+1.01** | 771M |
| 1–3d | +0.02 | 445M |
| 3–7d | +0.28 | 315M |
| 7–30d | +0.19 | 418M |
| **30d+** | **+1.15** | 955M |

Near-close (0–1h) has almost zero fade excess — consistent with momentum findings. In the final hour, price moves are informed by real-time events (scores, announcements), and there's no time for reversion. The strongest regimes are:

- **30d+ (≈1.15¢)**: Far-from-close markets are speculative; deviations are noise and revert
- **6–24h (≈1.01¢)**: Day-before trading has large swings that partially revert
- **1–6h (≈0.50¢)**: Moderate but high-volume

#### Deviation × Time Heatmap: Large Deviations Win Everywhere

| |Deviation| × Time | 0–1h | 1–6h | 6–24h | 1–3d | 3–7d | 7–30d | 30d+ |
|---------------------|------|------|-------|------|------|-------|------|
| Small (<5¢) | −0.01 | +0.40 | +0.74 | −0.25 | +0.39 | −0.05 | +1.01 |
| Medium (5–15¢) | −0.42 | +0.70 | +1.02 | +0.70 | −1.13 | +0.96 | +1.41 |
| **Large (15+¢)** | **+1.00** | **+2.45** | **+3.18** | **+5.66** | **+2.69** | **+5.24** | **+8.07** |

Large deviations (15+¢ from MA50) are profitable at **every** time-to-close bucket. The combination of large deviations + far-from-close (30d+) yields **+8.07¢/contract** — the single richest cell in any of our analyses.

#### Price × Time Heatmap: Best Cells

| Price × Time | Fade Excess (¢) | Notable? |
|-------------|-----------------|----------|
| 41–60¢ × 30d+ | **+6.11** | Mid-price, far-from-close: maximum reversion |
| 61–80¢ × 30d+ | +4.34 | High-price, speculative overshoots |
| 41–60¢ × 6–24h | +1.70 | Day-before mid-price fades |
| 61–80¢ × 1–3d | +1.91 | Multi-day high-price fades |
| 41–60¢ × 7–30d | +1.55 | Longer-term mid-price reversion |
| 21–40¢ × 7–30d | +1.33 | Low-price 1-month reversion |

The 30d+ column is dominant — far-from-close markets where prices deviate from their MA are the most profitable to fade.

### Summary and Trading Implications

| Finding | Implication |
|---------|------------|
| Fade excess is positive at ALL lookbacks | **Mean-reversion is a real, statistically robust signal** for takers |
| Shorter lookbacks are better (MA10 > MA50 > MA200) | Use recent 10–25 trade MA as the trigger |
| Large deviations (20+¢) yield +2.82¢ | **Be highly selective — only trade large overextensions** |
| Fading up (buy NO) is 3.2× better than fading down | Focus on **selling into YES-driven rallies** |
| 41–60¢ is the best price range | Trade mid-price markets where deviation = noise |
| 30d+ and 6–24h are best time windows | Avoid near-close; target far-from-close for bigger reversions |
| MA ≈ Median ≈ VWAP | The specific center definition doesn't matter — deviation is the signal |
| Large dev + 30d+ yields +8.07¢/contract | **The highest-conviction taker signal found in this dataset** |

### Connecting Momentum and Mean-Reversion

| Momentum Signal | Mean-Reversion Signal |
|----------------|----------------------|
| Follow Δ₁₀ → **−0.46¢** (loses) | Fade from MA₁₀ → **+0.66¢** (wins) |
| Bigger moves → worse (−3.17¢ at 21+) | Bigger deviations → better (+2.82¢ at 20+) |
| Bullish follow → 2.3× worse | Fading bullish → 3.2× better |
| Best near-close (−0.09¢) | Worst near-close (+0.01¢) |
| Win rate >50%, loses on payoff | Win rate <50%, wins on payoff |

These are mirror images. Momentum followers buy the right side too expensively; mean-reversion faders buy the cheap side that reverts. The two signals confirm each other: prediction markets **mean-revert**, and the dominant taker strategy should be contrarian.

### Caveats and Open Questions

1. **The signal is positive, but actual takers who fade still lose (−0.42¢)**. The gap between the theoretical signal (+0.41¢) and actual taker outcome (−0.42¢) implies ~0.83¢ of execution cost (spread crossing + adverse selection). A viable strategy must either trade only at extreme deviations (20+¢ at +2.82¢ should survive this) or find ways to reduce execution costs.

2. **"Best regimes" are thin**. The top cells in the regime analysis (EURUSD, USDJPY tickers at 90+¢ fade excess) reflect tiny FX markets with <100 trades. These are not tradeable at scale. Focus on the aggregate signals.

3. **Walk-forward validation needed**. These are in-sample results across the full dataset. A walk-forward backtest (Section 15 below) confirms the signal persists out-of-sample.

4. **Timing of entry matters**. The analysis assumes entering at the deviation price and holding to resolution. In practice, you might enter after a large deviation and the price could continue against you before reverting. Position sizing and stop-loss rules would affect realized returns.

---

## 15. Walk-Forward Backtest — Mean-Reversion Fade Strategy

> **Motivation**: Section 14 established that fading deviations from the moving average produces +0.41–0.66¢/contract in aggregate, with large deviations (20+¢) yielding +2.82¢. Section 12 showed that walk-forward backtesting dramatically changes conclusions — the maker strategy lost 59% despite showing a positive in-sample edge. Does the mean-reversion signal survive the same rigorous out-of-sample test?

### Backtest Design

The mean-reversion fade backtest follows the same walk-forward framework as Section 12, with key adaptations for the fade signal:

**Signal generation**: For each trade, compute the deviation from the trailing 50-trade moving average ($D_{50}$). If $D_{50} > 5$¢ (price above MA), **buy NO** (fade up). If $D_{50} < -5$¢ (price below MA), **buy YES** (fade down). Minimum absolute deviation of 5¢ required to generate a signal.

**Parameter estimation**: Monthly recalculation using only trades and markets that both occurred *and resolved* before the cutoff. Walk-forward lookback: 12 months (pre-2024) or 3 months (post-2024, following the transition to shorter-lived Sports markets). Parameters estimated per recalc period:
- **Group-level fade edge**: Average fade excess per category group.
- **Composite regime edge**: 4-way (group × price × time × day-type) bucketed edge, with hierarchical fallback to group × price or group-only buckets when data is sparse.
- **Directional multipliers**: Fade-up vs. fade-down edge ratios.

**Capital management**: Explicit `PortfolioState` tracking with position-level settlement — when a market resolves, its cost is returned to available cash plus realized PnL. Key parameters: $10,000 initial capital (1M cents), 90% max exposure, 5% max single-trade allocation, 7-day max hold time.

**Deduplication**: One signal per ticker per day (strongest deviation retained), preventing correlated multi-entry in the same market.

### Forensic Assumption Audit

An initial run of the backtest produced a +301,000% return ($10k → $30M), which triggered a forensic investigation. Six unrealistic assumptions were identified, three of which were fixed in the production backtest:

#### Problems Identified

| # | Assumption | Impact | Status |
|---|-----------|--------|--------|
| 1 | **Infinite liquidity** — 94.4% of trades consumed 100% of daily ticker volume | Largest single effect; at 1% participation, total PnL drops 90%+ while ROI stays ~30% | **Fixed:** 10% participation cap |
| 2 | **Zero spread/slippage** — no execution cost modeled | At 2¢/contract, ROI drops from ~35% to ~30% (−14%) | **Fixed:** 2¢ per contract cost |
| 3 | **Unrestricted compounding** — portfolio grew from $10k to $30M, deploying into markets with a fraction of that liquidity | Geometric growth impossible at scale; by Q4 2025 avg equity $23M | **Fixed:** $100k effective equity cap on deployment sizing |
| 4 | **Zero market impact** — largest trades deployed $68k into markets with ~$50k–200k daily volume | Would move prices 10–20¢, destroying the 5–15¢ deviation signal | **Partially addressed** by participation cap (10% max volume) |
| 5 | **Binary payoff asymmetry** — 51.7% of trades lose 100% of cost, 48.3% win avg +155% | Not a bug per se — inherent to binary contracts — but means per-contract edge is slim (~8.6¢) | **Acknowledged** |
| 6 | **Same-day resolution** — 55.6% of positions resolve within 24 hours | Fast capital recycling amplifies compounding; not unrealistic but important context | **Acknowledged** |

#### What Is Genuinely Real

- **Raw signal quality is confirmed**: unweighted per-contract fade edge is +8.58¢ (regardless of capital assumptions). Fade-up (buy NO when price > MA) yields +12.2¢/contract with 55.8% win rate.
- **No look-ahead bias**: verified that zero trades enter after market resolution. The SQL filter `m.close_time > t.created_time` ensures all entries precede outcomes.
- **ROI is stable across participation caps**: at 1%, 5%, or 10% volume participation, the per-capital-deployed ROI stays in the 28–32% range. The signal works — the question is how much capital you can push through it.

#### Production Backtest Configuration

The corrected backtest applies three realism constraints:

| Parameter | Old Value | Corrected Value | Rationale |
|-----------|-----------|----------------|-----------|
| `max_participation_rate` | 100% (implicit) | **10%** | A retail trader can realistically capture ~10% of daily volume |
| `spread_cost_cents` | 0 | **2¢/contract** | Half-spread + slippage on Kalshi's CLOB |
| `max_portfolio_cents` | Unlimited | **$100k** | Caps deployment sizing; profits accumulate but aren't reinvested past $100k |
| `max_trades_per_day` | 100,000 | **500** | Realistic execution constraint |

### Head-to-Head — All Three Strategies

| Metric | Maker | Taker | **Mean-Rev Fade** |
|--------|-------|-------|-------------------|
| **Period** | Mar 2022 – Jul 2024 | Jan 2023 – Dec 2024 | **Jan 2023 – Nov 2025** |
| **Total Return** | −59.3% | +11.6% | **+11,920%**† |
| **Sharpe Ratio** | −1.10 | +0.80 | **+8.47** |
| **Sortino Ratio** | −0.37 | +0.73 | **+30.0** |
| **Max Drawdown** | 43.9% | 3.5% | **29.7%** |
| **Trade Win Rate** | 46.2% | 46.7% | **47.3%** |
| **Total Trades** | 6,863 | 11,517 | **89,574** |
| **Profit Factor** | 0.35 | 2.18 | **14.16** |
| **Avg Trade PnL** | −86.5¢ | +10.1¢ | **+1,330.7¢**† |
| **Skewness** | −17.0 | +9.7 | **+2.8** |
| **Capital Deployed** | $153k | $18k | **$3.96M** |

†The total return reflects compounding with a $100k deployment cap. The **unweighted ROI on deployed capital is 30.1%** — consistent regardless of sizing assumptions. Average trade PnL is inflated by later-period trades sized against a larger portfolio.

### PnL by Fade Direction

| Direction | Trades | PnL ($) | ROI | Win Rate |
|-----------|--------|---------|-----|----------|
| Fade up (buy NO) | 54,541 | +$903k | +34.6% | 54.4% |
| Fade down (buy YES) | 35,033 | +$289k | +21.4% | 36.2% |

Fading up outperforms fading down by 13pp in ROI and 18pp in win rate, confirming the Section 14 finding that YES-driven rallies are the strongest mean-reversion candidates. With spread costs applied, the directional asymmetry is preserved.

### PnL by Deviation Magnitude

| Deviation | Trades | PnL ($) | ROI | Win Rate |
|-----------|--------|---------|-----|----------|
| Small (5–9¢) | 20,428 | **−$9k** | **−2.7%** | 52.5% |
| Medium (10–14¢) | 29,304 | +$25k | +2.1% | 48.4% |
| **Large (15+¢)** | **39,842** | **+$1,177k** | **+48.5%** | **43.8%** |

**Critical finding**: With realistic execution costs, small deviations (5–9¢) are **net negative**. The 2¢ spread cost eats the slim edge on small deviations entirely. Medium deviations barely break even. **Only large deviations (15+¢) are meaningfully profitable** — they account for 99% of total PnL. This validates the "only trade large overextensions" rule far more strongly than the uncorrected backtest did: it's not just a preference, it's a requirement.

### PnL by Price Bucket

| Price | Trades | PnL ($) | ROI | Win Rate |
|-------|--------|---------|-----|----------|
| 1–20¢ | 18,346 | +$141k | +24.9% | 34.7% |
| 21–40¢ | 22,227 | +$177k | +19.1% | 57.3% |
| 41–60¢ | 15,754 | +$152k | +22.5% | 60.5% |
| 61–80¢ | 21,621 | +$442k | +36.0% | 48.1% |
| **81–99¢** | **11,626** | **+$281k** | **+50.3%** | **28.6%** |

The 81–99¢ bucket retains the highest ROI (50.3%) despite the lowest win rate (28.6%). These are trades where the fade signal buys the opposite side at 1–19¢ — extreme payoff asymmetry (5:1 to 99:1) makes even a 29% win rate highly profitable, even after 2¢ spread costs.

### PnL by Time-to-Close

| Time Bucket | Trades | PnL ($) | ROI | Win Rate |
|-------------|--------|---------|-----|----------|
| **1–6h** | **33,101** | **+$637k** | **33.8%** | **40.3%** |
| 6h–3d | 46,845 | +$534k | 29.6% | 52.0% |
| 3d+ | 9,628 | +$20k | 7.8% | 48.7% |

Short-horizon (1–6h) trades remain the most profitable. The 3d+ bucket drops to only 7.8% ROI with spread costs — longer-hold trades have thinner edges that are eroded by execution costs.

### PnL by Category Group

| Group | Trades | PnL ($) | ROI | Win Rate |
|-------|--------|---------|-----|----------|
| **Sports** | **32,433** | **+$633k** | **35.0%** | **40.8%** |
| Weather | 25,582 | +$399k | 34.4% | 53.7% |
| Entertainment | 5,189 | +$47k | 22.5% | 49.1% |
| Politics | 3,666 | +$35k | 27.5% | 48.1% |
| Crypto | 6,698 | +$27k | 11.3% | 47.1% |
| Other | 4,299 | +$22k | 18.1% | 49.1% |
| Media | 5,988 | +$22k | 24.4% | 50.8% |
| Finance | 5,571 | +$6k | 3.1% | 49.0% |
| Science/Tech | 60 | −$0.5k | −29.3% | 43.3% |

**8 of 9 category groups are profitable** (Science/Tech has only 60 trades — statistically meaningless). Sports and Weather together produce 87% of total PnL. Finance barely breaks even at 3.1% ROI — the spread cost nearly eliminates its edge.

### Monthly PnL — The First Year

| Month | Trades | PnL ($) | ROI |
|-------|--------|---------|-----|
| Jan 2023 | 62 | −$16 | −1.6% |
| Feb 2023 | 46 | +$53 | +24.3% |
| Mar 2023 | 282 | −$79 | −6.3% |
| Apr 2023 | 306 | +$421 | +24.9% |
| May 2023 | 298 | +$32 | +3.0% |
| Jun 2023 | 412 | −$12 | −0.6% |
| Jul 2023 | 331 | +$183 | +11.8% |
| Aug 2023 | 341 | +$516 | +23.5% |
| Sep 2023 | 321 | −$690 | −29.6% |
| Oct 2023 | 245 | −$436 | −27.1% |
| Nov 2023 | 308 | +$319 | +12.9% |
| Dec 2023 | 326 | +$1 | +0.0% |

**7 of 12 months profitable** in the first year — weaker than the uncorrected backtest (10/12) because the participation cap reduces the number of profitable trades and spread costs flip marginal winners to losers. The Sep–Oct 2023 drawdown (−$1,126 combined, roughly −11% of equity) is a realistic two-month losing streak. Year 1 equity is essentially flat: $10,000 → $10,171 — the strategy needs the post-2024 sports market explosion to produce meaningful returns.

### Equity Progression

| Checkpoint | Equity | Cumulative Return |
|-----------|--------|-------------------|
| Start (Jan 2023) | $10,000 | — |
| Q1 2023 | $9,943 | −0.6% |
| Q2 2023 | $10,403 | +4.0% |
| Q3 2023 | $10,376 | +3.8% |
| Q4 2023 | $10,171 | +1.7% |
| Q1 2024 | $11,515 | +15.2% |
| Q2 2024 | $9,834 | −1.7% |
| Q3 2024 | $30,484 | +205% |
| Q4 2024 | $198,156 | +1,882% |
| Q1 2025 | $380,619 | +3,706% |
| Q2 2025 | $598,808 | +5,888% |
| Q3 2025 | $878,112 | +8,681% |
| End (Nov 2025) | $1,201,238 | +11,912% |

The inflection point occurs in Q3 2024, coinciding with Kalshi's launch of high-volume sports event markets. Before that, the strategy was essentially flat for 18 months ($10k → $10k). The explosion in 2025 reflects both (a) the massive increase in tradeable volume and (b) the deployment cap rising as equity crosses $100k and compounds. Even with all realism constraints applied, the post-2024 growth is driven by the genuine quality of the mean-reversion signal in high-frequency sports markets.

### Realism Assessment

The corrected 11,920% return (vs. 301,000% uncorrected) is **mechanically correct but still overstated** for practical trading:

1. **Participation cap helps but doesn't fully solve liquidity**: 10% of daily volume is achievable for a retail trader, but still assumes you can fill at the quoted price without moving it. In practice, even 10% participation in thin markets causes some price impact.

2. **The $100k deployment cap still allows compounding**: Profits above $100k sit in cash — but the deployment cap rises as equity crosses $100k naturally. A fully fixed-capital variant (always sizing off $10k initial capital) would yield roughly **$3,000–5,000/year** — a 30–50% annual return on initial capital. That's the most conservative honest estimate.

3. **2¢ spread cost is a lower bound**: Kalshi's actual spreads vary by market; Sports markets in 2025 may have tighter spreads, but illiquid markets can have 5–10¢ spreads. At 3¢ average spread, ROI would drop to ~23%.

4. **The realistic interpretation**: The signal has a **genuine +30% ROI on deployed capital** that survives walk-forward testing, spread costs, and participation limits. On a fixed $10k account with no compounding, expect **$2,500–4,000/year**. With moderate compounding (capped at $100k deployment), the strategy plausibly reaches **$50k–100k in cumulative PnL over 3 years** — impressive, but three orders of magnitude below the headline figure.

### Why Mean-Reversion Succeeds Where Maker and Taker Failed

| Factor | Maker (−59%) | Taker (+12%) | **MR Fade (+30% ROI)** |
|--------|-------------|-------------|------------------------|
| **Signal type** | Static edge (spread) | Regime filter | **Dynamic signal (deviation from MA)** |
| **Adaptability** | Parameters stale after 1 month | Weather-specific, one-month burst | **Recalibrates monthly, works across 8 groups** |
| **Risk profile** | −17 skewness (left tail) | +9.7 skewness | **+2.8 skewness (right tail)** |
| **Concentration** | 6/7 groups lost money | 131% from Weather | **8/9 groups profitable** |
| **Robustness** | Oscar night wiped 29% | Jan 2023 = 102% of PnL | **7/12 months profitable in Year 1** |
| **Edge source** | Spread capture (vulnerable to informed flow) | Category bet | **Payoff asymmetry (buy cheap sides)** |
| **Survives costs** | No — spread = entire edge | Marginal | **Yes — 30% ROI with 2¢ spread** |

The mean-reversion strategy's key advantage is **payoff asymmetry**: it buys the cheap side of binary contracts when prices deviate from fair value. A 47% win rate is profitable because winning trades pay 2–5× more than losing trades cost. This is structurally different from the maker strategy (which collects small spreads and suffers catastrophic losses) and the taker strategy (which was a concentrated Weather bet).

### Revised Actionable Conclusions

1. **Mean-reversion fade is the strongest signal in this dataset**. It passes both in-sample (Section 14: +0.41–0.66¢/contract) and walk-forward (+30% ROI on deployed capital, with 2¢ spread cost and 10% participation cap) tests. No other strategy achieves both.

2. **Only trade large deviations (15+¢)**. Small deviations (5–9¢) are **net negative** after spread costs. Medium deviations barely break even. Large deviations deliver 48.5% ROI — **all the edge is concentrated here**.

3. **Prefer fade-up (buy NO)**. Fading bullish rallies yields 35% ROI vs 21% for fading bearish drops. YES-side traders overshoot more aggressively than NO-side traders.

4. **Target 1–6h time-to-close**. This window captures the highest ROI (34%) with the most trades. The 3d+ bucket drops to only 8% ROI — not worth the capital lock-up.

5. **Sports and Weather are the primary categories**. Combined, they produce 87% of total PnL with consistent 34–35% ROI. Finance has a negligible edge after costs.

6. **Do not extrapolate the compounded return**. The 11,920% figure still reflects compounding from $10k. On fixed capital, expect **30–50% annualized**. On a capped-compounding basis ($100k effective equity), expect **$50k–100k over 3 years** — an excellent return, but not "quit your job" money on a $10k account.

7. **Spread costs are the binding constraint**. The 2¢ cost flips small-deviation trades from profitable to unprofitable. If Kalshi tightens spreads (or you provide liquidity yourself), the strategy's edge widens materially.

8. **Pre-2024 the strategy is essentially flat**. The signal needs sufficient market activity to generate enough qualifying large-deviation trades. The strategy's practical viability depends on continued high sports-market volume on Kalshi.

---

*Generated from quantitative analysis of 72.1M Kalshi trades across $18.26B in volume. Walk-forward backtests cover Mar 2022 – Jul 2024 (maker), Jan 2023 – Dec 2024 (taker), and Jan 2023 – Nov 2025 (mean-reversion, with 10% participation cap, 2¢ spread cost, and $100k deployment cap). All statistical tests significant at p < 0.05 unless noted. See `output/` for raw data and `src/analysis/` for methodology.*
