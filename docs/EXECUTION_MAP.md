# EXECUTION MAP — Polymarket Hourly Binary Options Bot

**Generated:** 2026-02-24
**Codebase:** `pm_hourly_clone_bot.py` + supporting modules
**Purpose:** Structured decision-flow reference for AI-assisted debugging and development

---

## Table of Contents

1. [Entry Decision Flow](#1-entry-decision-flow)
2. [Exit Decision Flow](#2-exit-decision-flow)
3. [Exposure & Budget Calculation](#3-exposure--budget-calculation)
4. [Safe Mode Triggers](#4-safe-mode-triggers)
5. [Truth Reconciliation Flow](#5-truth-reconciliation-flow)
6. [Scalp Engine (HYBRID_COPYWALLET)](#6-scalp-engine-hybrid_copywallet)

---

## 1. ENTRY DECISION FLOW

Every BUY order must pass through two tiers of sequential gates. If any gate
fails, execution stops immediately — the order is never placed.

### Tier 1: Global Pre-Entry Gates (`_step_market_with_data`)

These run before any engine-specific logic. They apply to ALL entry types
(directional, scalp, parity).

| # | Gate | Function / Check | Condition That Blocks | Default Value | Event When Blocked |
|---|------|------------------|-----------------------|---------------|-------------------|
| G1 | Stale hour | `t_min > 65.0` | hour_start_utc expired (>65 min old) | 65 min | `STALE_HOUR_START_DETECTED` |
| G2 | Hard stop time | `t_min >= TRADE_HARD_STOP_MIN` | Past 59:15 into hour | 59.25 min | `SETTLEMENT_TRANSITION_DIAG` |
| G3 | No price data | `hour_open <= 0 or spot <= 0` | Missing spot/open price | — | `SKIP_NO_PRICE` |
| G4 | Close imminent | `_live_safety.is_close_imminent()` | <30s to hour close, cancel-all fired | 30s | `[GATE] CLOSE_IMMINENT` |
| G5 | Hard flatten phase | `t_min >= HARD_FLATTEN_MIN` | Past 59:00 — force-close mode | 59.0 min | `HARD_FLATTEN_START` |
| G6 | Soft flatten phase | `t_min >= FLATTEN_START_MIN` | Past 57:00 — trim/exit only | 57.0 min | (silent) |
| G7 | Trade stop add | `t_min > TRADE_STOP_ADD_MIN` | Past 55:00 — no new risk | 55.0 min | `[GATE] TRADE_STOP_ADD_MIN` |
| G8 | Seconds to close | `seconds_to_close < NO_NEW_ENTRIES_SEC_TO_CLOSE` | <3 min to close | 180s | `[GATE] TRADE_STOP_ADD_MIN` |
| G9 | Risk drawdown | `not _risk_ok(st)` | Hourly/daily loss cap exceeded | — | `[GATE] RISK_GATE` |
| G10 | Global throttle | `_throttle_exceeded()` | >15 trades/min rolling 60s | 15/min | `[GATE] THROTTLE_EXCEEDED` |
| G11 | Symbol filter | `m.crypto not in LIVE_ALLOWED_SYMBOLS` | Coin not in allowed list | BTC,ETH | `[GATE] SYMBOL_NOT_ALLOWED` |
| G12 | Live safety rules | `_live_safety.check_pre_entry()` | Spread explosion, depth, slippage, loss stop, IOC-only window | composite | `[GATE] LIVE_SAFETY_BLOCK` |

### Tier 2: Directional Scalp Gates (`_dscalp_entries`)

These are the primary engine's 22+ sequential gates. **ALL must pass.**

#### A. Cooldown / Rate Gates

| # | Gate | Function | Condition That Blocks | Default | Event |
|---|------|----------|-----------------------|---------|-------|
| D1 | Entry cooldown | direct time check | `(now - last_entry_ts) * 1000 < DSCALP_COOLDOWN_MS` | 4000 ms | (silent) |
| D2 | Max position/slug | `_dscalp_invested_usd[slug] >= max_usd` | Already at max USD for this slug | $25/slug | (silent) |
| D3 | Post-stop cooldown | `(now - last_stop_ts) < STOP_COOLDOWN_SEC` | Recent stop loss on this slug | 90s | `stop_cooldown` counter |
| D4 | Rate limit | `_rate_limit_ok(slug)` | <600ms since last order on slug, or >60 submits/min | 600ms / 60/min | (tracked internally) |
| D5 | Entry throttle | `_entry_throttle_ok(slug)` | <350ms since last entry on slug, or >20 entries/min/slug | 350ms / 20/min | (silent) |
| D6 | Regime reduction | `_regime_activity_mult(slug)` | Low-vol regime — randomly skip 50% of entries | 3.0 bps threshold | (silent) |

#### B. Inventory / Exposure Gates

| # | Gate | Function | Condition That Blocks | Default | Event |
|---|------|----------|-----------------------|---------|-------|
| D7 | Inventory cap (USD) | `_inventory_cap_ok()` | slug positions + reserved >= cap | $50/slug | `[GATE] INVENTORY_CAP` |
| D8 | Inventory cap (shares) | `_inventory_cap_ok()` | outcome qty >= cap | 40 shares | `[GATE] INVENTORY_CAP` |
| D9 | Inventory cap (imbalance) | `_inventory_cap_ok()` | \|Up_qty - Down_qty\| >= cap | 12 shares | `[GATE] INVENTORY_CAP` |
| D10 | Post-fill cooldown | `_post_fill_cooldown_ok()` | <800ms since last fill on slug | 800 ms | `[GATE] POST_FILL_COOLDOWN` |
| D11 | Anti-stacking | direct time check | <1500ms (BTC/ETH) or <2500ms (SOL/XRP) since fill | coin-specific | `[GATE] ENTRY_STACKING` |

#### C. Data Quality Gates

| # | Gate | Function | Condition That Blocks | Default | Event |
|---|------|----------|-----------------------|---------|-------|
| D12 | Cache freshness | `_cache_age_ms(slug)` | Data older than 300ms (BTC/ETH) or 200ms (SOL/XRP) | coin-specific | `[GATE] CACHE_STALE` |
| D13 | Coin spread | `_coin_spread_entry_ok()` | Spread > 2c (BTC/ETH) or > 3c (SOL/XRP) | coin-specific | `[GATE] SPREAD_FILTER` |
| D14 | General spread | direct check | `book.spread * 100 > DSCALP_MAX_SPREAD_CENTS` | 2.0c | `[GATE] SPREAD_GENERAL` |
| D15 | Noisy spread | direct check | spread > 4c AND velocity < 4 bps/min (choppy) | 4c / 4 bps | `[GATE] NOISY_SPREAD` |
| D16 | Late-move filter | `_spot_move_2s_bps()` | SOL/XRP only: spot moved >8 bps in 2s on stale cache (>120ms) | 8 bps / 120ms | `[GATE] LATE_MOVE` |

#### D. Signal Quality Gates

| # | Gate | Function | Condition That Blocks | Default | Event |
|---|------|----------|-----------------------|---------|-------|
| D17 | Entry edge | `(book.mid - 0.50) * 100` | Mid-price < 3c (BTC/ETH) or < 4c (SOL/XRP) from 50c neutral | coin-specific | `[GATE] ENTRY_EDGE_LOW` |
| D18 | Signal strength | delta + spot_move check | `abs_delta < 15 bps` AND `spot_move_10s < 8 bps` — need at least one | 15 / 8 bps | `[GATE] SIGNAL_TOO_WEAK` |
| D19 | Velocity support | direct check | Velocity doesn't support direction: Up needs vel >= 2.5 (BTC/ETH) or 3.5 (SOL/XRP); Down needs vel <= -threshold | coin-specific | `[GATE] VELOCITY_BLOCK` + `GATE_VELOCITY_BLOCK` |

#### E. Execution Safety Gates

| # | Gate | Function | Condition That Blocks | Default | Event |
|---|------|----------|-----------------------|---------|-------|
| D20 | Size minimum | `step_usd < DSCALP_STEP_USD_MIN` | Remaining capacity < $6 minimum order | $6 min | (silent) |
| D21 | Price sanity | direct check | `buy_price <= 0.01` or `order_qty < 1` | — | (silent) |
| D22 | Exec safety | `_exec_safety_can_enter()` | Kill-switch, drift pause, loss-tail, window state, safe mode, exposure cap | composite | `[GATE] EXEC_SAFETY_BLOCK` |
| D23 | LIVE_SAFE cap | `_live_safe_cap_usd()` | Caps order to $5 in LIVE_SAFE mode; blocks if qty < 1 after cap | $5 | (silent) |

### Higher-Level Buy Gates

These are checked by various callers before even reaching the engine:

**`_buys_allowed_common()`** — shared by DIR and SCALP:
- Reconciler desync → block
- Ledger safe mode → block
- `_desync_hard_stop` → block
- `_total_exposure_usd() >= MAX_TOTAL_EXPOSURE_USD` → block ($100 default)

**`_buys_allowed_dir()`** — additional for directional:
- All common checks
- Flatten phase → block
- `ENTRY_ONLY_EARLY_WINDOW` past early window → block

**`_buys_allowed_scalp()`** — additional for scalp:
- All common checks
- Flatten phase → block
- Does NOT check early window or directional conviction

### Execution After All Gates Pass

```
Direction: outcome = ctx["drift_dir"]  (Up if delta_bps > 0, Down otherwise)
Book:      book = up_book if outcome == "Up" else dn_book
Price:     buy_price = book.bid  (maker at best bid)
Size:      step_usd = min(DSCALP_STEP_USD, remaining_capacity)  ($7 default)
Qty:       order_qty = step_usd / buy_price

If MODE == "LOG":
    Paper fill at buy_price
Else:
    _place_layered_buy(m, outcome, order_qty, buy_price)
    → client.place_limit_order(token_id, "BUY", ask, qty, post_only=False)
```

---

## 2. EXIT DECISION FLOW

Exits are checked BEFORE entry gates on every tick. The main loop calls
exit functions first, then gate-checks entries. Sells are never blocked by
safe mode — only buys are blocked.

### Priority Order (top = checked first)

```
EVERY TICK (line ~3251-3255):
  1. _manage_exits()              ← parity/core TP, time stops, inventory pressure
  2. _dscalp_manage_exits()       ← directional scalp TP ladder, stop loss, timeout
  3. _directional_lean_exits()    ← wrong-side imbalance trim

AFTER EXITS, BEFORE ENTRIES:
  4. CLOSE_IMMINENT check         ← blocks new orders, exits already ran
  5. HARD_FLATTEN trigger         ← force-close everything at 59.0 min
  6. SOFT_FLATTEN trigger         ← trim positions at 57.0 min
```

### A. Directional Scalp Exits (`_dscalp_manage_exits`)

**PnL Calculation:** `pnl_cents = (current_mid - entry_price) * 100`

#### Take-Profit Ladder (4 tiers, partial exits)

| Tier | Trigger | Sell Fraction | Price Used | Event Type | Latched? |
|------|---------|--------------|------------|------------|----------|
| TP1 | PnL >= +4.0c | 35% of position | book.bid | `DSCALP_TP1` | Yes (tp1_done) |
| TP2 | PnL >= +7.0c | 25% of position | book.bid | `DSCALP_TP2` | Yes (tp2_done) |
| TP3 | PnL >= +10.0c | 25% of position | book.bid | `DSCALP_TP3` | Yes (tp3_done) |
| TP4 | PnL >= +12.0c | 15% (runner) | book.bid | `DSCALP_TP4` | Yes (tp4_done) |

Each tier fires exactly once per position. After TP3, the remaining 15% becomes
the "runner" with special protection rules.

#### Stop Loss

| Trigger | Condition | Action | Overrides Min Hold? | Event |
|---------|-----------|--------|---------------------|-------|
| Hard stop | PnL <= -4.0c | Sell 100% at bid | YES | `DSCALP_STOP` |
| Loss cap | unrealized_pnl <= -1.25x invested | Sell 100% at bid | YES | `DSCALP_LOSS_CAP` |

- **Post-stop:** Slug gets 90s cooldown (`STOP_COOLDOWN_SEC`)

#### Early Exit (Before 60s Min Hold)

Requires ALL THREE simultaneously:
1. Profit >= +4.0c (`EARLY_EXIT_MIN_PROFIT_CENTS`)
2. Velocity reversal >= 6.0 bps/min against position (`EARLY_EXIT_VEL_REVERSAL_BPS`)
3. Spread >= 5.0c (`EARLY_EXIT_SPREAD_THRESHOLD_CENTS`)

Event: `DSCALP_EARLY_EXIT`

#### Runner Fallback (After TP3)

Fires when EITHER:
- Hold time >= 600s (`RUNNER_MAX_HOLD_SEC`)
- Velocity reversal >= 8 bps/min against position (`RUNNER_VEL_REVERSAL_BPS`)

Event: `DSCALP_RUNNER_FALLBACK`

#### Timeout

- After 600s total hold with no TP trigger: `DSCALP_TIMEOUT`

### B. Settlement Flatten

| Phase | Time | Action | Maker/Taker | Event |
|-------|------|--------|-------------|-------|
| Soft flatten | t >= 57.0 min | Stop new entries, start trimming | Maker preferred | `FLATTEN_TRIM` |
| Parity flatten | t >= 58.5 min | Sell locked + unpaired parity | Maker, step-based | `PARITY_FLATTEN` |
| Hard flatten | t >= 59.0 min | Cancel all, escalating IOC sells | Taker (IOC) | `HARD_FLATTEN_START/DONE` |
| Parity hard | t >= 59.1 min | Force-close parity inventory | Taker | `PARITY_HARD_FLATTEN` |
| Trade hard stop | t >= 59.25 min | All remaining sold at mid/bid | Taker | `HARD_STOP` |

**Hard flatten escalation:** -2c, -3c, -5c below bid in 3 steps with 300ms waits.

### C. Other Sell Triggers

| Trigger | Function | Condition | Action | Event |
|---------|----------|-----------|--------|-------|
| Inventory pressure | `_manage_exits()` | Position > 250 shares | Sell 50% | `INVENTORY_PRESSURE_SELL` |
| Lean exit | `_directional_lean_exits()` | Excess on wrong side of drift | Sell 25% of excess | `LEAN_EXIT` |
| Hedge mismatch | `_check_hedge_mismatch()` | Net exposure > limit for > max unhedged time | Flatten excess side | `HEDGE_MISMATCH_FLATTEN` |
| Wide spread trim | soft flatten path | Spread > 5c during flatten | Sell at midpoint | `FLATTEN_TRIM_WIDE_SPREAD` |
| RUN_ONE_HOUR | `_run_one_hour_end()` | One hour elapsed (env flag) | Cancel all + hard flatten + shutdown | `RUN_ONE_HOUR_END` |

### D. What Overrides What

```
HARD STOP (-4c)     → overrides minimum hold timer
EARLY EXIT          → overrides minimum hold timer (needs 3 conditions)
HARD FLATTEN (59m)  → overrides everything, cancels all orders first
CLOSE IMMINENT      → blocks new orders, does NOT force sells
SAFE MODE           → blocks buys only, all sells proceed normally
DESYNC_HARD_STOP    → blocks buys only, all sells proceed normally
```

---

## 3. EXPOSURE & BUDGET CALCULATION

### How Exposure Is Calculated

```
_total_exposure_usd() =
    SUM(position.cost_usdc for all positions where qty >= 0.001)
    + SUM(_reserved_usd[order_id] for all pending orders)
```

- **Positions:** Uses `cost_usdc` (historical cost basis = vwap * qty), NOT current mid
- **Open orders:** Uses reserved notional (order_price * order_qty)
- **Single source of truth** for the always-on cap

### Where MAX_TOTAL_EXPOSURE_USD Is Enforced

| Location | Check | Default |
|----------|-------|---------|
| `_buys_allowed_common()` | `_total_exposure_usd() >= MAX_TOTAL_EXPOSURE_USD` | $100 |
| `_inventory_cap_ok()` | Per-slug: positions + dscalp_invested + reserved >= MAX_POSITION_USD_PER_SLUG | $50/slug |
| `ExposureTracker.can_open()` | Total + per-engine budget (HYBRID mode) | $100 total, $35 DIR, $15 SCALP |
| Burst entry loop | `_total_exposure_usd() + this_usd >= MAX_TOTAL_EXPOSURE_USD` | $100 |

### Budget Reservation System

```
ON BUY SUBMIT:
    _reserve_budget(order_id, price * qty)     ← locks USD
    → cash_usdc is NOT reduced yet

ON FILL:
    _release_reservation(order_id)             ← frees reserved USD
    → position.cost_usdc increases instead
    → cash_usdc reduced by actual fill cost

ON CANCEL / TIMEOUT:
    _release_reservation(order_id)             ← frees reserved USD
    → no position change

STUCK ORDER CLEANUP:
    _audit_reservations() runs every tick
    → releases orders older than 25s (RESERVATION_STUCK_TIMEOUT_SEC)
    → clears negative sums (invariant violation)
```

### How Selling Frees Capacity

```
_live_sell(st, outcome, price, qty):
    proceeds = price * qty
    cost_basis = vwap * qty
    pnl = proceeds - cost_basis

    pos.qty -= qty
    pos.cost_usdc = max(0, pos.cost_usdc - cost_basis)   ← IMMEDIATE reduction
    cash_usdc += proceeds
    realized_pnl_usdc += pnl
```

**Selling immediately frees capacity.** The next call to `_total_exposure_usd()`
sees the reduced `cost_usdc`. There is no delay — the next order can use the
freed space within the same tick.

### Hour Budget (Reporting Only)

```python
HOURLY_BUDGET_USD = 50.0                     # NOT ENFORCED — reporting only
_hour_budget_spent += usdc_cost              # incremented on each buy
# This value is NEVER checked for gating. It's only logged in balance summaries.
# Actual enforcement uses MAX_TOTAL_EXPOSURE_USD (positions + reserved).
```

### Per-Slug Tracking (Directional Scalp)

```
_dscalp_invested_usd[slug]:
    ON BUY:  += actual_cost (fill_qty * fill_price)
    ON FULL EXIT:  deleted entirely (pop from dict)
    ON PARTIAL TP:  NOT decremented — stays at original entry cost

    Capped by: DSCALP_MAX_USD_PER_SLUG ($25 default)
```

---

## 4. SAFE MODE TRIGGERS

Seven independent safety mechanisms. ALL block new BUYs. NONE block SELLs.

### Summary Table

| Mechanism | Trigger | Scope | Auto-Clear? | Duration | Event Type |
|-----------|---------|-------|-------------|----------|------------|
| **Truth Safe Mode** | Critical position mismatch in reconciliation | Global buys | Yes — next clean reconciliation | ~60s | `TRUTH_SAFE_MODE_ENTER/EXIT` |
| **Ledger Safe Mode** | Fill/wallet mismatch (infrastructure exists, not currently activated) | Global buys | Manual | Indefinite | `LEDGER_SAFE_MODE_ENTER/EXIT` |
| **Desync Hard Stop** | Balance drift >$1 OR reconciler critical desync OR 3x position drift | Global buys | **NO — requires restart** | Indefinite | `DESYNC_HARD_STOP` |
| **Kill Switch** | API errors or orphan cancels exceed threshold/min | Global buys | Yes — timer expiry | `OM_KILL_PAUSE_SEC` | `KILL_SWITCH_ACTIVATED/EXPIRED` |
| **Loss Tail Pause** | N consecutive negative exits on a slug | Per-slug buys | Yes — lookback window | `LOSS_TAIL_PAUSE_SEC` | `SLUG_PAUSED_NEGATIVE_EXITS` |
| **Hedge Circuit Breaker** | 3+ hedge failures in 10-min window | Global buys | Yes — timer expiry | `HEDGE_FAIL_PAUSE_SEC` | `HEDGE_FAIL_CIRCUIT_BREAKER` |
| **Close Imminent** | 30s before hour close | Global orders | Yes — new hour | ~30s | `CLOSE_IMMINENT_SET` |

### Detailed Trigger Conditions

#### Truth Safe Mode (`src/positions/truth_capture.py`)

**Activated when:**
- Reconciliation detects CRITICAL mismatch: `abs(wallet_qty - truth_qty) > 10 * tolerance`
- OR: truth ledger shows 0 but wallet has shares (or vice versa)
- OR: bot sets it after 3 consecutive position drift checks

**Tolerance:** 0.01 shares (so CRITICAL = >0.1 shares drift)

**Auto-clears:** Next reconciliation with zero mismatches calls `exit_safe_mode()`.

#### Desync Hard Stop (3 independent triggers)

```
TRIGGER 1: Balance Sync Drift
    _sync_balances_from_wallet() detects total_drift_usd > $1.00
    → _desync_hard_stop = True
    → cancel_all_orders()
    Event: DESYNC_HARD_STOP (source=balance_sync)

TRIGGER 2: Reconciler Critical Desync
    _run_reconciliation() → reconciler.is_desynced() = True
    Condition: bot thinks flat (qty < MIN_QTY) but wallet has shares
    → _desync_hard_stop = True
    → cancel_all_orders()
    Event: DESYNC_HARD_STOP (source=reconciliation)

TRIGGER 3: Position Invariant Drift (3 consecutive)
    _check_position_invariant() detects mismatch 3 times in a row
    Escalation: drift #1 = diagnostic, drift #2 = attempt dedup fix,
                drift #3+ = hard stop
    → _desync_hard_stop = True
    → clear all reservations
    → cancel_all_orders()
    → enter truth safe mode
    Event: DESYNC_FROM_INVARIANT (source=position_invariant)
```

**Never auto-clears.** Requires manual restart.

#### Hedge Circuit Breaker (`src/execution/live_safety.py`)

**Hedge failure = any of:**
- Kill-switch timeout (leg 2 not filled within timeout)
- Slippage violation (intended vs actual price diverged)
- Insufficient depth (book too thin)
- Spread explosion (>3c normal, >5c final 2 min)

**Threshold:** 3 failures within 600s window → pause for 900s.

#### Kill Switch (`src/execution/safety_caps.py`)

**Triggers:**
- `api_errors_this_min >= OM_KILL_API_ERROR_THRESHOLD_PER_MIN`
- `orphan_cancels_this_min >= OM_KILL_ORPHAN_THRESHOLD_PER_MIN`

**Duration:** `OM_KILL_PAUSE_SEC`, then auto-expires.

---

## 5. TRUTH RECONCILIATION FLOW

### Fill Deduplication (Triple Layer)

```
LAYER 1: Truth Capture (earliest barrier)
    Key: trade_id (primary) or order_id+action+qty+price+ts (fallback)
    Storage: _seen_ids set + _seen_order_fills set
    Event on dup: DUP_SKIP or TRUTH_DEDUP_HIT

LAYER 2: Bot-Level (_apply_fill)
    Key: (order_id, side, round(fill_qty, 2))
    Storage: _applied_fills set
    Event on dup: FILL_APPLY_DEDUP

LAYER 3: Watcher Notification
    On immediate fill: notify_fill(order_id, fill_qty) updates watcher's
    cumulative_filled so poll doesn't re-discover it
```

### Fill Sources and Unification

All fills converge through `_apply_fill(slug, outcome, side, qty, price, order_id, reason, source)`:

| Source | Origin | source= parameter | Path |
|--------|--------|-------------------|------|
| Immediate | Direct API response from order placement | `"immediate"` | place_order → _apply_fill |
| Poll recovery | Truth capture order watchers (1s polling) | `"poll"` | poll_watchers → _process_truth_recovered_fills → _apply_fill |
| Lifecycle | LiveOrderTracker pending order check | `"lifecycle_recovered"` | poll_pending_orders → _process_lifecycle_recovered_fills → _apply_fill |
| Wallet scan | Incremental wallet trade scan (60s) | `"external_scan"` | run_wallet_scan → record_fill |

### `_apply_fill` Complete Flow

```
_apply_fill(slug, outcome, side, fill_qty, fill_price, order_id, source)
│
├─ 1. DEDUP CHECK
│  dedup_key = (order_id, side, round(qty, 2))
│  If in _applied_fills → emit FILL_APPLY_DEDUP, return 0.0
│  Else → add to _applied_fills
│
├─ 2. CAPTURE PRE-FILL STATE
│  _pre_exposure = _total_exposure_usd()
│  _pre_qty = position.qty
│
├─ 3. RELEASE RESERVATION
│  released = _release_reservation(order_id)
│
├─ 4. PROCESS FILL
│  ├─ BUY: cash += released, _live_buy(), _ledger_record_buy_fill(), _clear_buy_inflight()
│  └─ SELL: pnl = _live_sell(), _ledger_record_sell_fill()
│
├─ 5. UPDATE ENGINE SUB-LEDGER (HYBRID only)
│  engine = exposure_tracker.get_order_engine(order_id)
│  exposure_tracker.record_buy/sell(engine, slug, outcome, qty, price)
│  If SCALP SELL → notify scalp_engine.on_exit_fill(slug)
│
├─ 6. CAPTURE POST-FILL STATE
│  _post_exposure = _total_exposure_usd()
│  _post_qty = position.qty
│
├─ 7. EMIT ORDER TRACE + FILL_APPLIED
│  DiagReporter.order_trace("FILL", ...)
│  write_jsonl({event_type: "FILL_APPLIED", source, slug, outcome, side, ...})
│
└─ RETURN pnl (SELL) or 0.0 (BUY)
```

### Position Drift Detection

```
_check_position_invariant()   [runs every 10s]
│
├─ For each (slug, outcome):
│  bot_qty = market_states[slug].positions[outcome].qty
│  truth_qty = truth.get_position(token_id).net_qty
│  If abs(bot_qty - truth_qty) > 0.01 → record drift
│
├─ If NO drifts: _pos_drift_consecutive = 0, return
│
├─ If drifts detected:
│  _pos_drift_consecutive += 1
│  │
│  ├─ Count = 1: Diagnostic only
│  │  Fetch CLOB balances, emit DRIFT_DIAGNOSTIC_CLOB_BALANCES
│  │
│  ├─ Count = 2: Attempt auto-heal
│  │  truth.dedup_fills() — remove duplicate fills
│  │  If fixed → reset counter to 0
│  │
│  └─ Count >= 3: ESCALATE
│     _desync_hard_stop = True
│     Clear all reservations
│     Cancel all orders
│     Enter truth safe mode
│     Emit DESYNC_FROM_INVARIANT
```

### Reconciliation Schedule

| System | Interval | What It Compares | Can Correct? |
|--------|----------|------------------|-------------|
| Position invariant | 10s | Bot positions vs truth positions | No (detects only) |
| Position reconciler | 60s | Bot positions vs CLOB wallet balances | Yes (overwrites bot qty) |
| Ledger reconciler | 60s | Fills ledger vs unrealized PnL vs book | Partial (PnL only) |
| Wallet scan | 60s | Recent trades API vs truth fills | Yes (records missed fills) |
| Order watchers | 1s/order | Order status API vs expected fills | Yes (records delayed fills) |

---

## 6. SCALP ENGINE (HYBRID_COPYWALLET)

The scalp engine is an optional micro-scalp module activated only in the
`HYBRID_COPYWALLET` profile. It runs a separate fast loop parallel to the
directional engine.

### Architecture

```
Main Bot Loop (every tick)
│
├─ Directional Engine (_dscalp_entries)      ← slow, conviction-based
│  Budget: DIR_BUDGET_USD = $35
│  Size: $7/order
│  Hold: 60s min, 600s max
│  Exits: 4-tier TP ladder (-4c SL)
│
├─ Scalp Engine (_execute_scalp_action)      ← fast, imbalance-based
│  Budget: SCALP_BUDGET_USD = $15
│  Size: $1.50/order
│  Hold: 0-60s max
│  Exits: +2c TP, -2c SL, 60s time stop
│
└─ Shared: MAX_TOTAL_EXPOSURE_USD = $50 (HYBRID default)
           ExposureTracker enforces both engine + total caps
```

### Scalp Entry Logic (`scalp_engine.py: _scan_entry`)

```
For each outcome (Up, Down):
│
├─ 1. SPREAD CHECK
│  spread_cents = book.spread * 100
│  Require: spread >= SCALP_MIN_SPREAD_CENTS (2.0c)
│
├─ 2. ORDERBOOK IMBALANCE
│  micro_imb = book.bid_sz / (book.bid_sz + book.ask_sz)
│  For Up:   require micro_imb >= 0.62 (SCALP_IMB_LONG_THRESHOLD)
│  For Down: require micro_imb <= 0.38 (SCALP_IMB_SHORT_THRESHOLD)
│
├─ 3. EDGE AFTER FEES
│  taker_fee_cents = book.ask * TAKER_FEE_BPS / 100 * 100
│  net_edge = spread_cents / 2 - taker_fee_cents
│  Require: net_edge >= SCALP_MIN_EDGE_CENTS (1.0c)
│
├─ 4. EXPOSURE CHECK
│  exposure_tracker.can_open("SCALP", SCALP_SIZE_USD)
│  Checks: total < $50 AND scalp_engine < $15
│
├─ 5. NO EXISTING POSITION
│  One scalp position per slug max
│
└─ 6. MINIMUM QTY
    qty = SCALP_SIZE_USD / ask_price
    Require: qty >= 5 (CLOB minimum)

→ Output: {action: "BUY", slug, outcome, qty, price: ask, engine: "SCALP"}
```

### Scalp Exit Logic (`scalp_engine.py: _manage_exits`)

```
PnL = (current_mid - entry_price) * 100   [in cents]

EXIT 1: Take Profit
    Condition: pnl_cents >= +2.0c (SCALP_TAKE_PROFIT_CENTS)
    Price: book.bid
    Event: SCALP_EXIT_TP
    Resets consecutive stop counter

EXIT 2: Stop Loss
    Condition: pnl_cents <= -2.0c (SCALP_STOP_LOSS_CENTS)
    Price: book.bid - 0.01
    Event: SCALP_EXIT_STOP
    Increments consecutive stop counter
    If 3+ consecutive stops → pause slug for 10s

EXIT 3: Time Stop
    Condition: hold_sec >= 60s (SCALP_MAX_HOLD_SEC)
    Price: book.bid - 0.01
    Event: SCALP_EXIT_TIME
    Resets consecutive stop counter
```

### Inventory Separation (ExposureTracker)

```python
ExposureTracker._positions = {
    "DIR":   {(slug, outcome): SubPosition(qty, vwap, cost_usd, realized_pnl)},
    "SCALP": {(slug, outcome): SubPosition(qty, vwap, cost_usd, realized_pnl)},
}
```

- Each engine has its own sub-ledger with independent `SubPosition` entries
- Both engines CAN hold positions in the same slug simultaneously
- Directional can pyramid; scalp is limited to one position per slug
- Orders tagged via `tag_order(order_id, "DIR"|"SCALP")`
- On fill, `get_order_engine(order_id)` determines which sub-ledger to update

### Scalp vs Directional Gating Comparison

| Gate | Directional | Scalp |
|------|------------|-------|
| Safe mode / desync | Blocked | Blocked |
| Total exposure cap | Blocked | Blocked |
| Flatten phase | Blocked | Blocked |
| Early window only | Blocked if past | **Not checked** |
| Delta/velocity conviction | Required (15+ bps) | **Not required** (uses imbalance instead) |
| Spread minimum | Max 2c | Min 2c (wants spread for edge) |
| Signal type | Delta + spot move | Orderbook imbalance ratio |
| Order type | Maker at bid | IOC taker at ask |
| Position sizing | $7/order | $1.50/order |
| Engine budget | $35 | $15 |

### Scalp Integration in Main Loop

```
Every 300ms (SCALP_LOOP_INTERVAL_MS):
    For each market:
        If buys_allowed_scalp() OR has existing scalp position:
            actions = scalp_engine.tick(slug, books, safe_mode, desync, t_min)
            For each action:
                _execute_scalp_action(action, market, state)
                    BUY:  → IOC order → _apply_fill(source="scalp_ioc")
                    SELL: → IOC order → _apply_fill(source="scalp_ioc")
```

---

## APPENDIX: Key Constants Quick Reference

| Constant | Default | HYBRID Override | Purpose |
|----------|---------|----------------|---------|
| `MAX_TOTAL_EXPOSURE_USD` | $100 | $50 | Global cap (positions + reserved) |
| `MAX_POSITION_USD_PER_SLUG` | $50 | $25 | Per-slug position cap |
| `MAX_POSITION_SHARES_PER_OUTCOME` | 40 | 60 | Per-outcome shares cap |
| `MAX_NET_IMBALANCE_SHARES` | 12 | 25 | Up/Down imbalance cap |
| `DSCALP_MAX_USD_PER_SLUG` | $25 | — | Directional per-slug max |
| `DSCALP_STEP_USD` | $7 | — | Directional order size |
| `DSCALP_COOLDOWN_MS` | 4000 | 2000 | Between entries |
| `POST_FILL_COOLDOWN_MS` | 800 | 150 | After fill before re-entry |
| `TRADE_STOP_ADD_MIN` | 55.0 | 58.0 | Stop new entries |
| `FLATTEN_START_MIN` | 57.0 | 59.0 | Begin trimming |
| `HARD_FLATTEN_MIN` | 59.0 | 59.4 | Force-close all |
| `TRADE_HARD_STOP_MIN` | 59.25 | 59.4 | Settlement transition |
| `DIR_BUDGET_USD` | — | $35 | DIR engine budget (HYBRID) |
| `SCALP_BUDGET_USD` | — | $15 | SCALP engine budget (HYBRID) |
| `SCALP_SIZE_USD` | — | $1.50 | Scalp order size |
| `SCALP_TAKE_PROFIT_CENTS` | — | 2.0c | Scalp TP target |
| `SCALP_STOP_LOSS_CENTS` | — | 2.0c | Scalp SL threshold |
| `SCALP_MAX_HOLD_SEC` | — | 60s | Scalp time stop |
| `SCALP_IMB_LONG_THRESHOLD` | — | 0.62 | Imbalance for Up signal |
| `SCALP_IMB_SHORT_THRESHOLD` | — | 0.38 | Imbalance for Down signal |

## APPENDIX: Event Type Reference

### Entry Gates
```
GATE_TRACE (DIAG_MODE)     STALE_HOUR_START_DETECTED    SKIP_NO_PRICE
GATE_SPREAD_BLOCK          GATE_VELOCITY_BLOCK          GATE_INVENTORY_CAP
GATE_TOTAL_EXPOSURE_CAP    LIVE_SAFETY_BLOCK            SETTLEMENT_TRANSITION_DIAG
```

### Fill Events
```
FILL_APPLIED               FILL_APPLY_DEDUP             ORDER_TRACE (DIAG_MODE)
DSCALP_ENTRY               IOC_BUY_OK                   FOK_BUY_OK
```

### Exit Events
```
DSCALP_TP1/TP2/TP3/TP4    DSCALP_STOP                  DSCALP_LOSS_CAP
DSCALP_EARLY_EXIT          DSCALP_TIMEOUT               DSCALP_RUNNER_FALLBACK
FLATTEN_TRIM               FLATTEN_TRIM_WIDE_SPREAD     HARD_FLATTEN_START/DONE
HARD_STOP                  LEAN_EXIT                    INVENTORY_PRESSURE_SELL
PARITY_FLATTEN             PARITY_HARD_FLATTEN          RUN_ONE_HOUR_END
```

### Scalp Events
```
SCALP_ENTRY                SCALP_EXIT_TP                SCALP_EXIT_STOP
SCALP_EXIT_TIME            SCALP_CONSEC_STOP_PAUSE
```

### Safety Events
```
TRUTH_SAFE_MODE_ENTER/EXIT     DESYNC_HARD_STOP          DESYNC_FROM_INVARIANT
KILL_SWITCH_ACTIVATED/EXPIRED  HEDGE_FAIL_CIRCUIT_BREAKER CLOSE_IMMINENT_SET
POSITION_DRIFT                 STATE_DESYNC_DETECTED      DUP_SKIP
RESERVATION_STUCK_RELEASED     BUG_EXPOSURE_OVER_CAP      DIAG_SNAPSHOT
```
