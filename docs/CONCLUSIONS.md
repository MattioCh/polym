# Mean Reversion Backtest — Key Findings

Analysis script: `src/analysis/kalshi/backtest_mean_reversion.py`

## Strategy Overview

The backtest simulates a long mean-reversion strategy on Kalshi binary prediction
markets.  For each finalized market, a rolling VWAP (volume-weighted average price)
is maintained.  A long position is opened whenever `yes_price` falls at least
`entry_threshold` cents below the current VWAP.  The position is then closed by one
of two mechanisms:

| Mechanism | Trigger | Classification |
|-----------|---------|----------------|
| **Reversion close** | A later trade prints at `price ≥ entry_vwap − reversion_margin` | `close_type = 'reversion'` |
| **Resolution settle** | Market expires with no prior reversion; settled at 100 (yes) or 0 (no) | `close_type = 'resolution'` |

Default parameters: `entry_threshold = 5¢`, `reversion_margin = 2¢`.

---

## Core Finding: Reversion Closes Have a Guaranteed PnL Floor

By construction, every reversion-closed position satisfies a minimum-profit
constraint:

```
exit_price  ≥  entry_vwap − reversion_margin
entry_price ≤  entry_vwap − entry_threshold

PnL per contract ≥ entry_threshold − reversion_margin
                 = 5 − 2 = 3 ¢ (default parameters)
```

This means **reversion-closed trades always generate positive PnL** and achieve a
**100% win rate by construction**, regardless of how the market ultimately resolves.
The edge here is a pure liquidity/spread capture; it is entirely independent of
directional accuracy.

---

## Reversion vs. Resolution: Comparative Metrics

The two close types have fundamentally different risk/reward profiles:

### Reversion closed (`close_type = 'reversion'`)

| Metric | Observed |
|--------|----------|
| Win rate | **100%** (guaranteed) |
| Min PnL per contract | ≥ `entry_threshold − reversion_margin` cents |
| Average hold time | Short (minutes to hours) |
| Source of edge | Price-mean-reversion / liquidity spread |
| Binary outcome dependency | **None** |

In markets where prices oscillate around the VWAP (liquid, well-traded markets),
nearly all positions revert before expiry.  The strategy captures frequent, small,
mechanically guaranteed profits.

### Resolution settled (`close_type = 'resolution'`)

| Metric | Observed |
|--------|----------|
| Win rate | ~43–50% (depends on directional accuracy) |
| PnL range | 0 − entry_price (loss) to 100 − entry_price (win) |
| Average hold time | Long (hours to days) |
| Source of edge | Directional mispricing at time of entry |
| Binary outcome dependency | **Full** |

Resolution-settled trades occur in markets where the price was significantly
depressed and *stayed depressed* until expiry.  These positions take on full binary
risk:

- If result = **yes**: exit at 100 → large gain (up to ~90¢ per contract)
- If result = **no**: exit at 0 → large loss (equal to entry price)

A representative three-market scenario illustrates this asymmetry.  As price
keeps falling in Markets B and C, the entry signal fires multiple times (once
per additional drop of `entry_threshold` cents below the evolving VWAP),
accumulating 4 positions in Market B and 3 in Market C:

```
Market A (result=yes):  price dips 5¢ below VWAP, then reverts
  → 1 reversion close: entry=45, exit=48, PnL=+3 ¢/contract (win)

Market B (result=no):   price keeps falling (entries at 54, 50, 45, 40) — never reverts
  → 4 resolution settles at 0 ¢: losses of 54, 50, 45, 40 ¢/contract

Market C (result=yes):  price keeps falling (entries at 34, 30, 28) — never reverts
  → 3 resolution settles at 100 ¢: gains of 66, 70, 72 ¢/contract
```

Aggregate results for this scenario:

| close_type | trades | total PnL | win rate | avg PnL/trade |
|------------|--------|-----------|----------|---------------|
| reversion | 1 | +3 | 100% | +3.0 |
| resolution | 7 | +19 | 43% | +2.7 |

Despite the lower win rate, resolution-settled trades can still be net positive if
entries happen to catch genuinely undervalued markets.  However, variance is much
higher and a run of "no" outcomes can create deep drawdowns.

---

## PnL Distribution

- **Reversion PnL** is tightly clustered just above the minimum guaranteed spread
  (`entry_threshold − reversion_margin` cents per contract).  The distribution is
  right-skewed: larger deviations from VWAP at entry produce proportionally larger
  gains at close.
- **Resolution PnL** is bimodal: a cluster near `−entry_price` (losses from "no"
  outcomes) and a cluster near `100 − entry_price` (gains from "yes" outcomes).
  The spread between the two modes is roughly 100¢ per contract.

---

## Win Rate and Edge Contribution

The backtest decomposition reveals two distinct sources of edge:

1. **Mechanical reversion edge** (reversion closes)
   - Win rate = 100%
   - Driven by the market's tendency to mean-revert within a session
   - Size of edge scales with `entry_threshold − reversion_margin`

2. **Directional accuracy edge** (resolution closes)
   - Win rate reflects how often a meaningfully depressed price is genuinely
     mispriced rather than reflecting real information
   - Positive only if the market correctly identifies undervalued contracts

In liquid markets with frequent intra-session price oscillations, the majority of
positions close via reversion, making the strategy nearly direction-neutral.  In
thinner markets with persistent price trends, a larger fraction of positions reach
settlement, shifting the risk profile toward a directional bet.

---

## Key Takeaways

1. **Explicit close signals transform risk**: Adding a reversion close rule
   converts what would otherwise be a binary hold-to-resolution bet into a
   spread-capture trade, dramatically improving win rate for the portion of
   positions that revert.

2. **Parameter sensitivity**: Widening `reversion_margin` (allowing the close
   price to be further below the entry VWAP) increases the fraction of positions
   that close via reversion at the cost of a lower per-trade PnL floor.
   Narrowing `entry_threshold` triggers more frequent entries but with smaller
   expected mean-reversion spreads.

3. **Resolution risk is concentrated**: The variance contributed by
   resolution-settled trades far exceeds that of reversion-closed trades.
   Monitoring the ratio of reversion to resolution closes over time serves as
   a useful signal of how direction-neutral the strategy is operating.

4. **Market liquidity matters**: In highly liquid markets where prices oscillate
   frequently around the VWAP, nearly every position reverts before expiry.
   In illiquid markets with directional drift, resolution exposure increases.

5. **No data-snooping bias on reversion closes**: The guaranteed PnL floor is
   structural — it follows directly from the entry and exit conditions — and
   does not depend on any historical calibration of market outcomes.
