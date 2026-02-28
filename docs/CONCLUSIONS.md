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

*Generated from quantitative analysis of 72.1M Kalshi trades across $18.26B in volume. All statistical tests significant at p < 0.05 unless noted. See `output/` for raw data and `src/analysis/` for methodology.*
