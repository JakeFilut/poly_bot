"""
Arbitrage Bot - Kalshi vs Polymarket (15m Crypto Up/Down)

Features:
- Logging mode: Run forever, log all opportunities
- Trading mode: Execute until 10 successful trades
- Polymarket-first execution with auto-unwind
- Clean logging with P&L tracking
"""

import datetime
import os
import sys
import time
import requests

from dotenv import load_dotenv

from config import (
    KEYS_DIR,
    LOG_DIR,
    DEBUG,
    TARGET_SUCCESSFUL_TRADES,
    MAX_LOG_OPPORTUNITIES,
    MIN_BET_DOLLARS,
    MAX_TRADE_QTY,
    MIN_EDGE_PCT,
    MIN_EDGE_PCT_FOREVER,
    MIN_EDGE_PCT_TESTING,
    MIN_EXECUTE_EDGE_PCT,
    MAX_NET_EDGE_TO_TRADE_PCT,
    SCAN_INTERVAL_SECONDS,
    PRICE_BUFFER_START,
    PRICE_BUFFER_INCREMENT,
    PRICE_BUFFER_MAX,
    # Strike timing config
    WAIT_BEFORE_TRADING_SECONDS,
    # TESTING mode config
    TESTING_MIN_SPREAD_PCT,
    TESTING_LATE_SPREAD_PCT,
    TESTING_MIN_EDGE_NORMAL,
    TESTING_MIN_EDGE_LATE,
    TESTING_MIN_EDGE_FINAL,
    TESTING_LATE_WINDOW_START,
    TESTING_FINAL_WINDOW_START,
    TESTING_FINAL_QTY_MULTIPLIER,
    TESTING_NO_TRADE_FINAL_WINDOW,
    TESTING_MAX_COST_PER_CRYPTO_MARKET,
    TESTING_MAX_DRAWDOWN,
    TESTING_MAX_UNWINDS,
    TESTING_MAX_CONSECUTIVE_LOSSES,
    TESTING_COOLDOWN_MINUTES,
    calc_kalshi_fee,
    calc_poly_fee,
    GAS_FEE_DOLLARS,
    MAX_LOSS_PCT,
    MIN_AVG_PROFIT_PCT,
    MIN_TRADES_BEFORE_AVG_CHECK,
    KALSHI_SETUP_SECONDS,
    # No-threshold mode config
    NO_THRESHOLD_WAIT_SECONDS,
    NO_THRESHOLD_MIN_EDGE_PCT,
    NO_THRESHOLD_MAX_QTY,
    NO_THRESHOLD_MAX_TRADES_PER_CRYPTO,
    NO_THRESHOLD_FIRST_MINUTE_PROGRESS,
    NO_THRESHOLD_LAST_MINUTE_PROGRESS,
    ENABLE_1H_TRADING,
    ENABLE_5M_CROSS_STRIKE,
    # Last 2 minutes confirmation
    BINANCE_LAST_TWO_MIN_ENABLED,
    CHAINLINK_LAST_TWO_MIN_ENABLED,
    # Max contracts per window
    MAX_CONTRACTS_PER_WINDOW_ENABLED,
    MAX_CONTRACTS_PER_WINDOW,
)
from kalshi_client import KalshiClient
from polymarket_client import PolymarketClient
from fetcher import SnapshotFetcher, fetch_rtds_chainlink_price
from trader import ArbTrader

# WebSocket imports (optional - for faster mode)
try:
    from ws_fetcher import WebSocketFetcher
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False
from edge_calculator import pick_best_direction, calculate_edge, DEFAULT_QTY_FOR_EDGE
from logging_utils import (
    write_summary, set_log_mode, get_current_log_file,
    start_session, write_session_summary, get_session_file, get_session_stats,
    check_market_window_rotation, log_trade_row, log_potential_trade,
    update_early_trades_threshold_status, update_trade_settlements,
    get_settlement_risk,
    buffer_scan_line, clear_scan_buffer, log_highlight, flush_scan_buffer_to_log,
    get_current_1h_market_window,
)
from historical_logger import HistoricalLogger
from utils import now_iso, now_mst, get_window_progress, calc_spread_pct

def fmt_strike(v):
    """Format strike price with appropriate decimal precision."""
    if v < 10:
        return f"${v:,.5f}"
    elif v < 100:
        return f"${v:,.4f}"
    return f"${v:,.2f}"

def poly_round(price):
    """Round strike price to match Polymarket's resolution precision.
    Polymarket rounds: <$10 to 4 decimals, >=$10 to 2 decimals."""
    if price < 10:
        return round(price, 4)
    return round(price, 2)

BINANCE_SYMBOLS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}

def get_binance_price(crypto: str) -> float:
    """Fetch fresh current price from Binance (CF Benchmarks proxy for Kalshi settlement).
    Always makes a live API call — no caching."""
    symbol = BINANCE_SYMBOLS.get(crypto)
    if not symbol:
        return None
    try:
        r = requests.get(
            f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
            timeout=2,
        )
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception:
        pass
    return None


def should_skip_cross_strike(k_strike, p_strike, crypto, ws_fetcher):
    """Check if cross-strike trade should be skipped based on price feed positions.

    NOTE: Binance and Chainlink price checks are DISABLED.
    Always allows the trade without fetching external prices.

    Direction rule: ALWAYS trade UP on the lower strike, DOWN on the higher strike.
    Kalshi settles on CF Benchmarks (Binance proxy), Polymarket settles on Chainlink.
    """
    # Binance and Chainlink price feed checks disabled — always allow trade
    return False, None, None


MIN_BALANCE_TO_TRADE = 10.0  # Minimum $10 on each platform
AUTO_REDEEM_BALANCE_THRESHOLD = 20.0  # Trigger auto-redeem when Poly balance < $20

# Pushover notification settings
PUSHOVER_USER_KEY = "ustw2kk4tb256b5npzxa1bgjfkvdqs"
PUSHOVER_APP_TOKEN = "avu84oow7pcc45q9sx21izf284vq88"


def send_pushover_notification(title: str, message: str, priority: int = 0):
    """
    Send a Pushover notification.

    Args:
        title: Notification title
        message: Notification message
        priority: -2 to 2 (-2=lowest, 0=normal, 2=emergency)
    """
    try:
        url = "https://api.pushover.net/1/messages.json"
        data = {
            "token": PUSHOVER_APP_TOKEN,
            "user": PUSHOVER_USER_KEY,
            "title": title,
            "message": message,
            "priority": priority,
        }
        response = requests.post(url, data=data, timeout=10)
        if response.status_code == 200:
            print(f"  [PUSHOVER] Notification sent: {title}")
        else:
            print(f"  [PUSHOVER] Failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"  [PUSHOVER] Error: {e}")


# Separate Pushover app token for Balance notifications (creates separate thread)
# To use a separate Pushover thread, create a new app at https://pushover.net/apps
# and set this to the new token. Otherwise it uses the same Arb_bot thread.
PUSHOVER_BALANCE_APP_TOKEN = os.getenv("PUSHOVER_BALANCE_APP_TOKEN", PUSHOVER_APP_TOKEN)


def send_balance_notification(kalshi_client, poly_client):
    """
    Send hourly balance update via Pushover (separate "Balance" thread).

    Reports:
      - Kalshi account balance
      - Polymarket positions value (open positions still held)
      - USDC.e wallet balance (EOA + proxy)
    """
    try:
        # 1. Kalshi balance
        kalshi_bal = kalshi_client.get_balance()
        kalshi_str = f"${kalshi_bal:,.2f}" if kalshi_bal is not None else "N/A"

        # 2. USDC.e balance (Polymarket tradable balance - EOA + proxy)
        usdc_bal = poly_client.get_balance()
        usdc_str = f"${usdc_bal:,.2f}" if usdc_bal is not None else "N/A"

        # 3. Polymarket open positions value
        positions_value = 0.0
        positions_count = 0
        redeemable_value = 0.0
        redeemable_count = 0
        try:
            url = "https://data-api.polymarket.com/positions"
            all_positions = []
            offset = 0
            while True:
                params = {"user": poly_client.wallet_address.lower(), "limit": 100, "offset": offset}
                r = requests.get(url, params=params, timeout=15)
                if r.status_code != 200:
                    break
                page = r.json() if isinstance(r.json(), list) else []
                if not page:
                    break
                all_positions.extend(page)
                if len(page) < 100:
                    break
                offset += 100

            for p in all_positions:
                val = float(p.get("currentValue", 0) or 0)
                if val > 0:
                    if p.get("redeemable"):
                        redeemable_value += val
                        redeemable_count += 1
                    else:
                        positions_value += val
                        positions_count += 1
        except Exception as e:
            print(f"  [BALANCE] Error fetching positions: {e}")

        mst = datetime.timezone(datetime.timedelta(hours=-7))
        now_str = datetime.datetime.now(mst).strftime("%I:%M %p MST")

        # Calculate totals
        k_bal = kalshi_bal or 0
        u_bal = usdc_bal or 0
        cash_total = k_bal + u_bal
        est_total = cash_total + positions_value + redeemable_value

        # Build clear message with sections
        lines = []
        lines.append("CASH (available to trade):")
        lines.append(f"  Kalshi:    {kalshi_str}")
        lines.append(f"  Poly USDC: {usdc_str}")
        lines.append(f"  Total:     ${cash_total:,.2f}")

        if positions_count > 0:
            lines.append(f"\nOPEN BETS ({positions_count} unsettled):")
            lines.append(f"  Est. value: ${positions_value:,.2f}")

        if redeemable_count > 0:
            lines.append(f"\nREADY TO REDEEM ({redeemable_count}):")
            lines.append(f"  Value: ${redeemable_value:,.2f}")

        lines.append(f"\n{'─' * 20}")
        if positions_count > 0 or redeemable_count > 0:
            lines.append(f"Cash: ${cash_total:,.2f}")
            if positions_count > 0:
                lines.append(f"+ Open bets: ${positions_value:,.2f}")
            if redeemable_count > 0:
                lines.append(f"+ Redeemable: ${redeemable_value:,.2f}")
            lines.append(f"= Est. Total: ${est_total:,.2f}")
        else:
            lines.append(f"Total: ${cash_total:,.2f}")

        message = "\n".join(lines)

        # Send via Balance thread (separate app token)
        url = "https://api.pushover.net/1/messages.json"
        data = {
            "token": PUSHOVER_BALANCE_APP_TOKEN,
            "user": PUSHOVER_USER_KEY,
            "title": f"Balance ({now_str}) | ${est_total:,.2f}",
            "message": message,
            "priority": -1,  # Low priority - no sound/vibration
        }
        response = requests.post(url, data=data, timeout=10)
        if response.status_code == 200:
            print(f"  [BALANCE] Pushover sent: Cash=${cash_total:,.2f} Open=${positions_value:,.2f} Redeem=${redeemable_value:,.2f} Est=${est_total:,.2f}")
        else:
            print(f"  [BALANCE] Pushover failed: {response.status_code}")

    except Exception as e:
        print(f"  [BALANCE] Error sending balance notification: {e}")


def send_window_summary_notification(window_label: str, trades: list, kalshi_settlement_prices: dict, poly_settlement_prices: dict):
    """
    Send a single Pushover notification summarizing all trades in a completed window.

    Shows each trade with Kalshi and Poly sides, whether each side won/lost,
    and the net P&L. Replaces per-trade notifications.

    Args:
        window_label: e.g., "10:00-10:15 UTC"
        trades: list of trade dicts from window_trades
        kalshi_settlement_prices: dict of {crypto: end_price} from Kalshi strike (CF Benchmarks)
        poly_settlement_prices: dict of {crypto: end_price} from Chainlink t+0s
    """
    if not trades:
        return

    # Aggregate trades by crypto + direction
    # Key: (crypto, direction, market_type) -> aggregated data
    aggregated = {}
    for t in trades:
        key = (t["crypto"], t["direction"], t.get("market_type", "15m"))
        if key not in aggregated:
            aggregated[key] = {
                "crypto": t["crypto"],
                "direction": t["direction"],
                "market_type": t.get("market_type", "15m"),
                "kalshi_side": t["kalshi_side"],
                "total_qty": 0,
                "kalshi_cost_total": 0.0,
                "poly_cost_total": 0.0,
                "total_fees": 0.0,
                "kalshi_fee_total": 0.0,
                "poly_fee_total": 0.0,
                "kalshi_strike": t.get("kalshi_strike"),
                "poly_strike": t.get("poly_strike"),
                "unwind_trades": 0,
                "unwind_loss_total": 0.0,
                "success_trades": 0,
            }
        agg = aggregated[key]
        qty = t.get("qty", 0) or 0
        agg["total_qty"] += qty
        if t.get("was_unwound"):
            agg["unwind_trades"] += 1
            agg["unwind_loss_total"] += t.get("unwind_loss", 0) or 0
        else:
            agg["success_trades"] += 1
            agg["kalshi_cost_total"] += (t.get("kalshi_fill_price", 0) or 0) * qty
            agg["poly_cost_total"] += (t.get("poly_fill_price", 0) or 0) * qty
            agg["total_fees"] += t.get("fees", 0) or 0
            agg["kalshi_fee_total"] += t.get("kalshi_fee", 0) or 0
            agg["poly_fee_total"] += t.get("poly_fee", 0) or 0

    lines = [f"{window_label}"]
    total_pnl = 0.0

    for key, agg in aggregated.items():
        crypto = agg["crypto"]
        direction = agg["direction"]
        market_type = agg["market_type"]
        kalshi_side = agg["kalshi_side"]
        qty = agg["total_qty"]
        k_strike = agg["kalshi_strike"]
        p_strike = agg["poly_strike"]
        k_final = kalshi_settlement_prices.get(crypto)
        p_final = poly_settlement_prices.get(crypto)

        type_tag = f" ({market_type})" if market_type != "15m" else ""
        lines.append(f"\n{crypto}{type_tag} {qty}x:")

        # Handle unwound trades
        if agg["unwind_trades"] > 0 and agg["success_trades"] == 0:
            loss = agg["unwind_loss_total"]
            total_pnl -= loss
            lines.append(f"  UNWOUND: -{f'${loss:.2f}'}")
            continue

        # Calculate average prices for successful trades
        success_qty = sum(
            t.get("qty", 0) or 0
            for t in trades
            if (t["crypto"], t["direction"], t.get("market_type", "15m")) == key
            and not t.get("was_unwound")
        )
        if success_qty > 0:
            k_avg = agg["kalshi_cost_total"] / success_qty
            p_avg = agg["poly_cost_total"] / success_qty
        else:
            k_avg = 0
            p_avg = 0

        k_total = agg["kalshi_cost_total"]
        p_total = agg["poly_cost_total"]
        fees = agg["total_fees"]
        k_fee = agg["kalshi_fee_total"]
        p_fee = agg["poly_fee_total"]

        # Determine Kalshi side label
        if direction == "K_UP+P_DOWN":
            k_label = "UP"
            p_label = "DOWN"
        else:
            k_label = "DOWN"
            p_label = "UP"

        # Helper to format crypto prices
        def _fmt_price(v):
            try:
                v = float(v)
                if v >= 100:
                    return f"${v:,.2f}"
                elif v >= 1:
                    return f"${v:,.4f}"
                else:
                    return f"${v:,.5f}"
            except (ValueError, TypeError):
                return "N/A"

        # Determine win/loss using separate end prices for each exchange
        # Kalshi settles on CF Benchmarks (Kalshi strike price of new window)
        # Polymarket settles on Chainlink t+0s price
        if k_final is not None and p_final is not None and k_strike is not None and p_strike is not None:
            try:
                k_s = float(k_strike)
                p_s = float(p_strike)
                k_end = float(k_final)
                p_end = float(p_final)

                # K_UP (yes) wins if kalshi_end >= k_strike
                # K_DOWN (no) wins if kalshi_end < k_strike
                # P_UP wins if poly_end >= p_strike
                # P_DOWN wins if poly_end < p_strike
                if direction == "K_UP+P_DOWN":
                    k_won = k_end >= k_s
                    p_won = p_end < p_s
                else:  # K_DOWN+P_UP
                    k_won = k_end < k_s
                    p_won = p_end >= p_s

                k_payout = 1.0 * success_qty if k_won else 0.0
                p_payout = 1.0 * success_qty if p_won else 0.0
                total_cost = k_total + p_total + fees
                trade_pnl = (k_payout + p_payout) - total_cost

                k_result = f"WON +${k_payout:.2f}" if k_won else "LOST"
                p_result = f"WON +${p_payout:.2f}" if p_won else "LOST"

                # Kalshi: qty, direction, avg price, cost, fees, strike, end price (CF Benchmarks), result
                k_fee_str = f" (+${k_fee:.2f} fee)" if k_fee > 0 else ""
                lines.append(f"  Kalshi: {success_qty}x {k_label} avg ${k_avg:.2f} = ${k_total:.2f}{k_fee_str}")
                lines.append(f"    Strike {_fmt_price(k_s)} → End {_fmt_price(k_end)} (CF) → {k_result}")

                # Poly: qty, direction, avg price, cost, fees, strike, end price (Chainlink), result
                p_fee_str = f" (+${p_fee:.2f} fee)" if p_fee > 0 else ""
                lines.append(f"  Poly: {success_qty}x {p_label} avg ${p_avg:.2f} = ${p_total:.2f}{p_fee_str}")
                lines.append(f"    Strike {_fmt_price(p_s)} → End {_fmt_price(p_end)} (CL) → {p_result}")

                if not k_won and not p_won:
                    lines.append(f"  ** DOUBLE LOSS **")

                # Include unwind losses for this crypto
                trade_pnl -= agg["unwind_loss_total"]
                if agg["unwind_trades"] > 0:
                    lines.append(f"  Unwinds: {agg['unwind_trades']}x = -${agg['unwind_loss_total']:.2f}")

                lines.append(f"  Net: ${trade_pnl:+.2f}")
                total_pnl += trade_pnl
            except (ValueError, TypeError):
                k_fee_str = f" (+${k_fee:.2f} fee)" if k_fee > 0 else ""
                p_fee_str = f" (+${p_fee:.2f} fee)" if p_fee > 0 else ""
                lines.append(f"  Kalshi: {success_qty}x {k_label} avg ${k_avg:.2f} = ${k_total:.2f}{k_fee_str}")
                lines.append(f"  Poly: {success_qty}x {p_label} avg ${p_avg:.2f} = ${p_total:.2f}{p_fee_str}")
                lines.append(f"  Settlement: UNKNOWN")
        else:
            # Missing one or both settlement prices - show what we have
            k_fee_str = f" (+${k_fee:.2f} fee)" if k_fee > 0 else ""
            p_fee_str = f" (+${p_fee:.2f} fee)" if p_fee > 0 else ""
            lines.append(f"  Kalshi: {success_qty}x {k_label} avg ${k_avg:.2f} = ${k_total:.2f}{k_fee_str}")
            if k_strike is not None:
                k_end_str = f"End {_fmt_price(k_final)} (CF)" if k_final is not None else "End: pending"
                lines.append(f"    Strike {_fmt_price(k_strike)} → {k_end_str}")
            lines.append(f"  Poly: {success_qty}x {p_label} avg ${p_avg:.2f} = ${p_total:.2f}{p_fee_str}")
            if p_strike is not None:
                p_end_str = f"End {_fmt_price(p_final)} (CL)" if p_final is not None else "End: pending"
                lines.append(f"    Strike {_fmt_price(p_strike)} → {p_end_str}")
            payout = 1.0 * success_qty
            total_cost = k_total + p_total + fees
            theoretical = payout - total_cost - agg["unwind_loss_total"]
            lines.append(f"  Theoretical: ${theoretical:+.2f} (no settlement yet)")
            total_pnl += theoretical

    lines.append(f"\n{'─' * 20}")
    lines.append(f"TOTAL P&L: ${total_pnl:+.2f}")

    message = "\n".join(lines)

    # Send notification
    try:
        pnl_emoji = "+" if total_pnl >= 0 else ""
        send_pushover_notification(
            f"Window Summary | ${total_pnl:+.2f}",
            message,
            priority=-1 if total_pnl >= 0 else 0
        )
        print(f"  [SUMMARY] Window summary notification sent: ${total_pnl:+.2f}")
    except Exception as e:
        print(f"  [SUMMARY] Error sending window summary: {e}")


def fetch_poly_orderbook(token_id: str):
    """
    Fetch fresh Polymarket orderbook to get actual ask prices.

    Returns:
        List of (price, size) tuples for asks, sorted by price ascending (best first).
        Returns empty list on error.
    """
    try:
        url = "https://clob.polymarket.com/book"
        params = {"token_id": token_id}
        r = requests.get(url, params=params, timeout=3)
        if r.status_code == 200:
            book = r.json()
            asks = book.get("asks", [])
            # Sort asks by price ascending (lowest/best first for buyer)
            sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 999)))
            return [(float(a.get("price", 0)), float(a.get("size", 0))) for a in sorted_asks]
    except Exception as e:
        print(f"  [POLY] Orderbook fetch error: {e}")
    return []


def fetch_fresh_kalshi_prices(kalshi_client, ticker: str, debug: bool = False):
    """
    Fetch fresh Kalshi orderbook to get actual YES and NO ask prices.

    Returns:
        (yes_ask, no_ask, yes_ask_depth, no_ask_depth, yes_depth_buffered, no_depth_buffered) tuple in dollars, or (None, None, 0, 0, 0, 0) on error.
        yes_ask_depth = contracts available to BUY YES at yes_ask price (best level only)
        no_ask_depth = contracts available to BUY NO at no_ask price (best level only)
        yes_depth_buffered = cumulative contracts to BUY YES at +1 cent (includes best + next level)
        no_depth_buffered = cumulative contracts to BUY NO at +1 cent (includes best + next level)
    """
    try:
        ob = kalshi_client.get_orderbook(ticker)
        if not ob:
            return None, None, 0, 0, 0, 0

        orderbook = ob.get("orderbook", {}) or {}
        yes_orders = orderbook.get("yes", []) or []
        no_orders = orderbook.get("no", []) or []

        # Need orders on BOTH sides
        if not yes_orders or not no_orders:
            return None, None, 0, 0, 0, 0

        # Kalshi orderbook: "yes" and "no" are BIDS (people wanting to buy)
        # Format: [[price_cents, quantity], ...]
        # To BUY YES: we pay 100 - best_no_bid (this is the implied YES ask)
        # To BUY NO: we pay 100 - best_yes_bid (this is the implied NO ask)

        best_yes_bid = max(order[0] for order in yes_orders)
        best_no_bid = max(order[0] for order in no_orders)

        yes_ask = (100 - best_no_bid) / 100.0
        no_ask = (100 - best_yes_bid) / 100.0

        # Calculate depth - how many contracts you can BUY at each ask price
        # To BUY YES, you match NO bidders → depth = NO bid depth
        # To BUY NO, you match YES bidders → depth = YES bid depth
        yes_ask_depth = sum(qty for price, qty in no_orders if price == best_no_bid)  # Liquidity to buy YES
        no_ask_depth = sum(qty for price, qty in yes_orders if price == best_yes_bid)  # Liquidity to buy NO

        # Calculate cumulative depth at buffered price (+1 cent)
        # To buy YES at (yes_ask + 0.01), we match NO bids at (best_no_bid - 1) or higher
        # To buy NO at (no_ask + 0.01), we match YES bids at (best_yes_bid - 1) or higher
        yes_depth_buffered = sum(qty for price, qty in no_orders if price >= best_no_bid - 1)
        no_depth_buffered = sum(qty for price, qty in yes_orders if price >= best_yes_bid - 1)

        # Only skip if BOTH sides have extreme asks (market hasn't opened yet)
        if yes_ask >= 0.97 and no_ask >= 0.97:
            return None, None, 0, 0, 0, 0

        return yes_ask, no_ask, yes_ask_depth, no_ask_depth, yes_depth_buffered, no_depth_buffered
    except Exception as e:
        if debug:
            print(f"  [KALSHI] Orderbook error {ticker}: {e}")
    return None, None, 0, 0, 0, 0


def get_dynamic_qty(crypto: str, strike_gap_pct: float, kalshi_strike: float = None,
                    poly_strike: float = None, direction: str = None) -> int:
    """
    Calculate contract quantity based on strike price gap.
    Tighter gap = more contracts (less risk of different settlements).

    SAFE SPREAD: If buying UP on lower strike and DOWN on higher strike,
    there's a price range where BOTH bets win - this is very low risk,
    so we allow much higher quantity (50 contracts).

    Args:
        crypto: BTC, ETH, or SOL
        strike_gap_pct: Percentage difference between Kalshi and Poly strikes
        kalshi_strike: Kalshi strike price (optional, for safe spread check)
        poly_strike: Poly strike price (optional, for safe spread check)
        direction: Trade direction "K_UP+P_DOWN" or "K_DOWN+P_UP" (optional)

    Returns:
        Number of contracts to trade
    """
    # Check for SAFE SPREAD: buying into the gap between strikes
    # If K strike < P strike and direction is K_UP+P_DOWN: safe (wins if price between strikes)
    # If K strike > P strike and direction is K_DOWN+P_UP: safe (wins if price between strikes)
    # IMPORTANT: Gap must be meaningful (at least 0.25% or $5) for this to be reliable
    if kalshi_strike and poly_strike and direction:
        strike_gap_abs = abs(kalshi_strike - poly_strike)
        strike_gap_pct_check = strike_gap_abs / kalshi_strike * 100

        # Only consider safe spread if gap is meaningful (>= 0.25% AND >= $5)
        min_gap_pct = 0.25  # 0.25% minimum gap
        min_gap_dollars = 5.0  # $5 minimum gap

        is_safe_spread = False
        if strike_gap_pct_check >= min_gap_pct and strike_gap_abs >= min_gap_dollars:
            if kalshi_strike < poly_strike and direction == "K_UP+P_DOWN":
                # Betting price >= kalshi_strike AND price < poly_strike
                # Wins if price ends up between the two strikes
                is_safe_spread = True
            elif kalshi_strike > poly_strike and direction == "K_DOWN+P_UP":
                # Betting price < kalshi_strike AND price >= poly_strike
                # Wins if price ends up between the two strikes
                is_safe_spread = True

        if is_safe_spread:
            print(f"    ★ SAFE SPREAD: Gap ${strike_gap_abs:,.2f} ({strike_gap_pct_check:.2f}%) between K={fmt_strike(kalshi_strike)} and P={fmt_strike(poly_strike)}")
            return 50  # Much higher quantity for safe spreads

    # Fixed 7 contracts per trade for all cryptos
    return MAX_TRADE_QTY


def show_vpn_reminder():
    """Show VPN reminder at startup (disabled)."""
    pass


def select_cryptos() -> list:
    """Always trade all 4 cryptos."""
    return ["XRP", "ETH", "SOL", "BTC"]


def select_mode() -> str:
    """Select logging, trading, or redemption mode."""
    print("\n" + "=" * 65)
    print("  ARBITRAGE BOT - Kalshi vs Polymarket")
    print("=" * 65)
    print("\n  [1] LOGGING  - Log opportunities forever (no trades)")
    print(f"  [2] TRADING  - Execute real trades ({MIN_EDGE_PCT}% edge, 10 trades)")
    print(f"  [3] FOREVER  - Live trading forever ({MIN_EDGE_PCT_FOREVER}% edge)")
    print("  [4] REDEEM   - Redeem all settled Polymarket positions")
    print(f"  [5] TESTING  - Smart rules ({TESTING_MIN_EDGE_NORMAL}%/+{TESTING_MIN_EDGE_LATE}% edge, spread gates, kill-switches)")
    print(f"  [6] TESTING LOGS - Log with TESTING rules ({MIN_EDGE_PCT_FOREVER}% edge, $50/crypto, no last 3min)")
    if HAS_WEBSOCKET:
        print(f"  [7] WEBSOCKET - Real-time WebSocket mode (~10x faster prices & orders)")
    print()

    # Auto-select WEBSOCKET mode
    print("  Auto-selecting: [7] WEBSOCKET\n")
    return "websocket"


def run_redemption_mode():
    """Run the redemption mode to collect winnings from settled markets."""
    print("\n" + "=" * 65)
    print("  MODE: REDEEM SETTLED POSITIONS")
    print("=" * 65)

    # Load environment from keys directory
    env_path = os.path.join(KEYS_DIR, ".env")
    load_dotenv(env_path)

    # Get Polymarket credentials
    poly_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()

    if not poly_key:
        print(f"\n  [X] Missing Polymarket private key in {env_path}")
        raise SystemExit(1)

    # Initialize Polymarket client
    print("\n  [INIT]")
    print("  Connecting to Polymarket...")
    poly = PolymarketClient(poly_key)

    # Show current balance
    balance_before = poly.get_balance()
    print(f"\n  [BALANCE BEFORE]")
    print(f"    USDC.e: ${balance_before:.2f}" if balance_before else "    USDC.e: Unknown")

    # Confirm redemption
    print("\n  This will scan for and redeem all settled positions.")
    confirm = input("  Continue? (yes/no): ").strip().lower()

    if confirm != "yes":
        print("  Cancelled.")
        return

    # Run redemption
    redeemed = poly.redeem_all_positions()

    # Show final balance
    balance_after = poly.get_balance()
    print(f"\n  [BALANCE AFTER]")
    print(f"    USDC.e: ${balance_after:.2f}" if balance_after else "    USDC.e: Unknown")

    if balance_before is not None and balance_after is not None:
        change = balance_after - balance_before
        print(f"    Change: ${change:+.2f}")

    print("\n  Done!")


def redeem_during_wait(poly_client, wait_seconds, deadline_window_seconds=55):
    """
    Instead of just sleeping during the 1-minute startup wait, try to redeem
    up to 1 Polymarket position. Stops attempting redemptions if we reach
    deadline_window_seconds into the current 15-minute window, leaving a buffer
    to get ready for fetching.

    Args:
        poly_client: PolymarketClient instance
        wait_seconds: Total seconds we need to wait
        deadline_window_seconds: Stop redemptions at this many seconds into
            the 15-minute window (default 55, leaving 5s buffer before 60s fetch)

    Returns:
        Number of positions successfully redeemed.
    """
    from datetime import datetime, timezone

    MAX_REDEMPTIONS = 1
    start_time = time.time()
    redeemed = 0

    # Quick check: if wait is very short, just sleep
    if wait_seconds < 5:
        print(f"  [REDEEM] Wait too short ({wait_seconds:.0f}s) - skipping redemptions")
        time.sleep(max(0, wait_seconds))
        return 0

    print(f"  [REDEEM] Attempting up to {MAX_REDEMPTIONS} redemptions during {wait_seconds:.0f}s wait...")

    # Check if we're already past the deadline
    now = datetime.now(timezone.utc)
    secs_into_window = (now.minute % 15) * 60 + now.second
    if secs_into_window >= deadline_window_seconds:
        print(f"  [REDEEM] Already at {secs_into_window}s into window (deadline={deadline_window_seconds}s) - skipping")
        remaining = wait_seconds - (time.time() - start_time)
        if remaining > 0:
            time.sleep(remaining)
        return 0

    # Fetch redeemable positions
    try:
        redeemables = poly_client.get_redeemable_positions()
    except Exception as e:
        print(f"  [REDEEM] Error fetching positions: {e}")
        redeemables = []

    if not redeemables:
        print(f"  [REDEEM] No redeemable positions found - sleeping remaining time...")
        remaining = wait_seconds - (time.time() - start_time)
        if remaining > 0:
            time.sleep(remaining)
        return 0

    print(f"  [REDEEM] Found {len(redeemables)} redeemable position(s), will try up to {MAX_REDEMPTIONS}...")

    for i, pos in enumerate(redeemables[:MAX_REDEMPTIONS]):
        # Check deadline: stop at 55 seconds into window
        now = datetime.now(timezone.utc)
        secs_into_window = (now.minute % 15) * 60 + now.second
        if secs_into_window >= deadline_window_seconds:
            print(f"  [REDEEM] At {secs_into_window}s into window (deadline={deadline_window_seconds}s) - stopping")
            break

        # Also check total elapsed time (leave 5s buffer)
        elapsed = time.time() - start_time
        if elapsed >= wait_seconds - 5:
            print(f"  [REDEEM] Wait time nearly up ({elapsed:.0f}s/{wait_seconds:.0f}s) - stopping")
            break

        condition_id = pos.get('conditionId')
        title = pos.get('title', 'Unknown')[:50]
        value = float(pos.get('currentValue', 0))

        if not condition_id:
            continue

        print(f"  [REDEEM] [{i+1}/{MAX_REDEMPTIONS}] Redeeming: {title}... (${value:.2f})")

        try:
            tx_hash, gas_fee = poly_client.redeem_positions(condition_id)
            if tx_hash:
                redeemed += 1
                print(f"  [REDEEM] Redeemed successfully (gas: {gas_fee:.4f} MATIC)")
            else:
                print(f"  [REDEEM] Failed to redeem")
        except Exception as e:
            print(f"  [REDEEM] Error: {e}")

    if redeemed > 0:
        print(f"  [REDEEM] Redeemed {redeemed} position(s) during wait")

    # Sleep any remaining time
    elapsed = time.time() - start_time
    remaining = wait_seconds - elapsed
    if remaining > 0:
        print(f"  [REDEEM] Sleeping remaining {remaining:.1f}s...")
        time.sleep(remaining)
    else:
        print(f"  [REDEEM] Wait time elapsed during redemptions")

    return redeemed


def run_websocket_mode(cryptos: list):
    """
    Run WebSocket mode for ultra-fast trading.

    Uses WebSocket connections for:
    - Real-time price streaming (~50-100ms latency vs 3-5s scraping)
    - Fast order placement via WebSocket (Kalshi) and SDK (Polymarket)
    """
    print("\n" + "=" * 65)
    print("  MODE: WEBSOCKET (Real-time Fast Trading)")
    print("=" * 65)

    # Load environment from keys directory
    env_path = os.path.join(KEYS_DIR, ".env")
    load_dotenv(env_path)
    os.makedirs(LOG_DIR, exist_ok=True)

    # Credentials
    kalshi_key_id = os.getenv("KALSHI_API_KEY_ID", "").strip()
    kalshi_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()
    poly_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()

    if not kalshi_key_id or not kalshi_key_path:
        print(f"\n  [X] Missing Kalshi credentials in {env_path}")
        raise SystemExit(1)

    if not poly_key:
        print(f"\n  [X] Missing Polymarket private key in {env_path}")
        raise SystemExit(1)

    # Initialize WebSocket fetcher
    print("\n  [INIT]")
    print("  Starting WebSocket connections...")
    ws_fetcher = WebSocketFetcher(kalshi_key_id, kalshi_key_path, poly_key, skip_chainlink=False)

    # Also need regular clients for balance checks and some operations
    kalshi = KalshiClient(kalshi_key_id, kalshi_key_path)
    poly = PolymarketClient(poly_key)
    trader = ArbTrader(kalshi, poly)
    historical = HistoricalLogger(log_dir=LOG_DIR)

    # Balances
    k_bal, p_bal = trader.get_balances()

    print(f"\n  [BALANCES]")
    print(f"    Kalshi:       ${k_bal:.2f}")
    print(f"    Polymarket:   ${p_bal:.2f}")
    print(f"    Total:        ${k_bal + p_bal:.2f}")

    if k_bal < MIN_BALANCE_TO_TRADE or p_bal < MIN_BALANCE_TO_TRADE:
        print(f"\n  [X] Balance below ${MIN_BALANCE_TO_TRADE:.0f} minimum")
        raise SystemExit(1)

    # Config
    print(f"\n  [CONFIG]")
    print(f"    Cryptos:      {', '.join(cryptos)}")
    print(f"    Edge thresholds: All cryptos ≥6% (max 27%)")
    print(f"    Price buffer: ${PRICE_BUFFER_START:.3f} (1 cent on Kalshi)")
    print(f"    Mode:         WebSocket (real-time, Kalshi-first)")

    log_file = set_log_mode(False)  # Trading mode
    print(f"\n  [LOG] {log_file}")

    # Show dynamic contract sizing thresholds
    print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
    print(f"  ║  CONTRACT SIZING                                               ║")
    print(f"  ╠════════════════════════════════════════════════════════════════╣")
    print(f"  ║  All cryptos: Fixed {MAX_TRADE_QTY} contracts per trade                      ║")
    print(f"  ║  (Safe bets: 20% of thinner book, max 30, min 15 depth)       ║")
    print(f"  ╠════════════════════════════════════════════════════════════════╣")
    print(f"  ║  EDGE: All cryptos ≥6%  (skip if >27%)                        ║")
    print(f"  ║  TIME: Kalshi@60s, Chainlink strike, no trading last 1.5min    ║")
    print(f"  ║  Strike thresholds: BTC/ETH/SOL ≤0.02%                        ║")
    print(f"  ║  PER-CRYPTO: 3 guaranteed, then avg≥4.5% to continue, max 6   ║")
    print(f"  ║  Unwinds count as trades toward the 6-trade limit              ║")
    print(f"  ╚════════════════════════════════════════════════════════════════╝")
    print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
    print(f"  ║  SELECT TRADING MODE                                          ║")
    print(f"  ╠════════════════════════════════════════════════════════════════╣")
    print(f"  ║  [1] THRESHOLDS    - Full trading with strike thresholds      ║")
    print(f"  ║  [2] NO THRESHOLDS - Trade without strike price checks        ║")
    print(f"  ║  [3] NO LOSSES     - Only risk-free trades (safe bets only)   ║")
    print(f"  ╚════════════════════════════════════════════════════════════════╝")
    # Auto-select NO LOSSES mode
    print("  Auto-selecting: [3] NO LOSSES\n")
    use_strike_thresholds = True
    no_losses_mode = True

    if no_losses_mode:
        print(f"  [MODE] NO LOSSES - Risk-free trades only")
    elif use_strike_thresholds:
        print(f"  [MODE] THRESHOLDS ENABLED")
    else:
        print(f"  [MODE] NO THRESHOLDS")

    if no_losses_mode:
        print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
        print(f"  ║  NO LOSSES MODE                                               ║")
        print(f"  ╠════════════════════════════════════════════════════════════════╣")
        print(f"  ║  ONLY trades risk-free bets:                                  ║")
        print(f"  ║    Penny strike (BTC/ETH: diff ≤ $0.01)                       ║")
        print(f"  ║    Cross-strike arb (UP on lower, DOWN on higher)             ║")
        print(f"  ║    SOL/XRP: cross-strike only (no same-strike trades)         ║")
        print(f"  ║  Qty: 30% of thinner book, max 10, skip if <15 depth         ║")
        print(f"  ║  All other trades are skipped                                 ║")
        print(f"  ╚════════════════════════════════════════════════════════════════╝")
    elif use_strike_thresholds:
        print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
        print(f"  ║  THRESHOLD MODE RULES                                          ║")
        print(f"  ╠════════════════════════════════════════════════════════════════╣")
        print(f"  ║  2-Phase startup per window:                                   ║")
        print(f"  ║    Phase 1 @ {KALSHI_SETUP_SECONDS}s: Kalshi connections + strike prices        ║")
        print(f"  ║    Phase 2: 1s after Kalshi done → Chainlink strike, then trade  ║")
        print(f"  ║  Strike thresholds: BTC/ETH/SOL ≤0.02%                        ║")
        print(f"  ║  No trading in last 1.5 min of window (90%+ progress)          ║")
        print(f"  ║  Min edge: All cryptos 6%                                      ║")
        print(f"  ║  Max edge: 35% (skip suspicious outliers)                      ║")
        print(f"  ║  Fixed {MAX_TRADE_QTY} contracts per trade                                  ║")
        print(f"  ║  Max 6 trades per crypto per window (unwinds count)            ║")
        print(f"  ║    First 3 guaranteed, then avg edge ≥4.5% to continue         ║")
        print(f"  ║  No block on consecutive unwinds                                ║")
        print(f"  ║  Unwinds count toward trade limit                              ║")
        print(f"  ╠════════════════════════════════════════════════════════════════╣")
        print(f"  ║  SAFE BET MODE (overrides above when detected):                ║")
        print(f"  ║    Qty = 30% of thinner book side, max 10, skip if <15 depth    ║")
        print(f"  ║    SOL/XRP: cross-strike only (UP lower, DOWN higher)          ║")
        print(f"  ║    BTC/ETH: ≤$0.01 diff → 4% edge, unlimited                  ║")
        print(f"  ║    Cross-strike: UP on lower, DOWN on higher → same rules      ║")
        print(f"  ║    Bypasses: trade limit, last-minute block, strike threshold   ║")
        print(f"  ╚════════════════════════════════════════════════════════════════╝")
    else:
        print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
        print(f"  ║  NO-THRESHOLD MODE RULES                                      ║")
        print(f"  ╠════════════════════════════════════════════════════════════════╣")
        print(f"  ║  Skip strike price lookups                                     ║")
        print(f"  ║  No trading first 1 min or last 1.5 min of window              ║")
        print(f"  ║  Minimum edge: {NO_THRESHOLD_MIN_EDGE_PCT}%                                         ║")
        print(f"  ║  Max {NO_THRESHOLD_MAX_QTY} contracts per trade                                    ║")
        print(f"  ║  Max {NO_THRESHOLD_MAX_TRADES_PER_CRYPTO} trades per crypto per 15-min window (then restart)  ║")
        print(f"  ║  Unwinds count toward trade limit                              ║")
        print(f"  ╚════════════════════════════════════════════════════════════════╝")

    # Start session and capture the window BEFORE long Phase 1/Phase 2 setup
    session_file = start_session()
    print(f"  [SESSION] {session_file}")
    from logging_utils import get_current_market_window
    _startup_window = get_current_market_window()  # Capture now, used for last_ws_window later

    # Start WebSocket connections (start() now waits for connection)
    ws_fetcher.start()

    # Check how far into the current window we are
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    seconds_into_window = (now.minute % 15) * 60 + now.second

    import concurrent.futures
    failed_threshold_cryptos_startup = set()  # Initialize - will be populated after wait

    if use_strike_thresholds:
        # 2-Phase startup: Kalshi at 60s, then Poly RTDS right after
        KALSHI_SETUP_TIME = KALSHI_SETUP_SECONDS  # 60s into window

        # --- Phase 1: Kalshi connections + strike prices ---
        if seconds_into_window < KALSHI_SETUP_TIME:
            kalshi_wait = KALSHI_SETUP_TIME - seconds_into_window
            print(f"\n  [PHASE 1] {seconds_into_window}s into window - {kalshi_wait:.0f}s until {KALSHI_SETUP_TIME}s mark for Kalshi setup...")
            redeem_during_wait(poly, kalshi_wait)
        else:
            print(f"\n  [PHASE 1] Already {seconds_into_window}s into window - setting up Kalshi now...")

        print(f"  [KALSHI SETUP] Getting Kalshi connections and strike prices...")

        def subscribe_kalshi_phase(crypto):
            ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)
            return crypto

        # Retry market subscription with backoff (Kalshi may take time to create new markets)
        for subscribe_attempt in range(6):
            missing_before = [c for c in cryptos if not ws_fetcher._ticker_cache.get(c)]
            if not missing_before and subscribe_attempt > 0:
                break
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                targets = missing_before if missing_before else cryptos
                futures = [executor.submit(subscribe_kalshi_phase, c) for c in targets]
                concurrent.futures.wait(futures, timeout=30)
            missing_after = [c for c in cryptos if not ws_fetcher._ticker_cache.get(c)]
            if not missing_after:
                break
            if subscribe_attempt < 5:
                wait = 15
                print(f"  [KALSHI] Markets not found for {', '.join(missing_after)} - retrying in {wait}s ({subscribe_attempt+1}/6)...")
                time.sleep(wait)
            else:
                print(f"  [KALSHI] Still missing markets for {', '.join(missing_after)} after 6 attempts")

        ws_fetcher.flush_subscriptions(cryptos)
        ws_fetcher.enable_price_logs()

        # Show Kalshi strikes obtained so far
        print(f"\n  [PHASE 1] Kalshi strike prices:")
        missing_kalshi = []
        for c in cryptos:
            k_strike = ws_fetcher._kalshi_strikes.get(c)
            if k_strike:
                print(f"    {c}: {fmt_strike(k_strike)}")
            else:
                print(f"    {c}: N/A (will retry)")
                missing_kalshi.append(c)

        # Retry missing Kalshi strikes every 5s until ALL are found
        if missing_kalshi:
            retry_round = 0
            while missing_kalshi:
                retry_round += 1
                print(f"\n  [PHASE 1] Retrying missing Kalshi strikes (round {retry_round}): {', '.join(missing_kalshi)}")
                still_missing = []
                for c in missing_kalshi:
                    k_strike, _ = ws_fetcher._fetch_strike_with_retry(c, max_retries=2)
                    if k_strike:
                        ws_fetcher._kalshi_strikes[c] = k_strike
                        print(f"    {c}: {fmt_strike(k_strike)} (recovered)")
                    else:
                        print(f"    {c}: Still N/A")
                        still_missing.append(c)
                missing_kalshi = still_missing
                if missing_kalshi:
                    print(f"  [PHASE 1] Waiting 5s before next retry...")
                    time.sleep(5)

        # --- Poly strikes: from Chainlink at t+0s ---
        # At startup, Chainlink may have missed the boundary (started late).
        # Display whatever we have — missing strikes are handled by threshold check.
        now_p2 = datetime.now(timezone.utc)
        secs_p2 = (now_p2.minute % 15) * 60 + now_p2.second
        print(f"\n  [POLY] Poly strikes (Chainlink, t+{secs_p2}s)")

        for c in cryptos:
            p_strike = ws_fetcher._poly_strikes.get(c)
            if p_strike and p_strike > 0:
                pfmt = f"${p_strike:,.5f}" if p_strike < 10 else f"${p_strike:,.4f}" if p_strike < 100 else f"${p_strike:,.2f}"
                print(f"    {c}: {pfmt} (Chainlink)")
            else:
                print(f"    {c}: No Chainlink strike — will skip until next window")

        # Display full strike comparison with threshold check
        STRIKE_THRESHOLDS_STARTUP = {
            "BTC": 0.02,  # 0.02% max difference
            "ETH": 0.02,  # 0.02% max difference
            "SOL": 0.02,  # 0.02% max difference
            "XRP": 0.02,  # 0.02% max difference
        }

        strike_now = datetime.now(timezone.utc)
        strike_date_str = strike_now.strftime("%Y-%m-%d %H:%M:%S UTC")
        strike_lines = []
        strike_lines.append(f"  ╔══════════════════════════════════════════════════════════════════════════════════╗")
        strike_lines.append(f"  ║  STRIKE PRICES  |  {strike_date_str}  |  Poly@t+0s Chainlink + Kalshi@t+60s API  ║")
        strike_lines.append(f"  ╠══════════════════════════════════════════════════════════════════════════════════╣")
        for c in cryptos:
            k_strike = ws_fetcher._kalshi_strikes.get(c)
            p_strike = ws_fetcher._poly_strikes.get(c)
            if k_strike and p_strike:
                diff_pct = abs(k_strike - p_strike) / k_strike * 100
                threshold = STRIKE_THRESHOLDS_STARTUP.get(c, 0.10)
                if diff_pct <= threshold:
                    status = "GOOD"
                else:
                    status = "BAD - SKIP"
                    failed_threshold_cryptos_startup.add(c)
                strike_lines.append(f"  ║  {c}: K={fmt_strike(k_strike)}  P={fmt_strike(p_strike)}  (diff: {diff_pct:.3f}%) [{status}]")
            else:
                k_display = f"{fmt_strike(k_strike)}" if k_strike else "N/A"
                p_display = f"{fmt_strike(p_strike)}" if p_strike else "N/A"
                strike_lines.append(f"  ║  {c}: K={k_display}  P={p_display}  (MISSING!) [BAD - SKIP]")
                failed_threshold_cryptos_startup.add(c)
        strike_lines.append(f"  ╚══════════════════════════════════════════════════════════════════════════════════╝")
        for line in strike_lines:
            print(line)
        log_highlight("\n".join(strike_lines) + "\n\n")

        # Log strikes to Historical_Data
        for c in cryptos:
            k_strike = ws_fetcher._kalshi_strikes.get(c)
            p_strike = ws_fetcher._poly_strikes.get(c)
            if k_strike and p_strike:
                historical.record_prices(c, k_strike, p_strike)

        strikes_fetched_at_startup = True
    else:
        # No-threshold mode: single wait of 60s, no scraping
        if seconds_into_window >= NO_THRESHOLD_WAIT_SECONDS:
            print(f"\n  [FULL SUBSCRIBE] Already {seconds_into_window}s into window - skipping strikes (thresholds off)...")

            def subscribe_no_thresh(crypto):
                ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)
                return crypto

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(subscribe_no_thresh, c) for c in cryptos]
                concurrent.futures.wait(futures, timeout=30)
        else:
            wait_remaining = NO_THRESHOLD_WAIT_SECONDS - seconds_into_window
            print(f"\n  [WAITING] {seconds_into_window}s into window - {wait_remaining}s until first minute buffer ends...")
            redeem_during_wait(poly, wait_remaining)

            print(f"\n  [SUBSCRIBING] Wait complete - subscribing for prices (no strikes)...")

            def subscribe_no_thresh(crypto):
                ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)
                return crypto

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(subscribe_no_thresh, c) for c in cryptos]
                concurrent.futures.wait(futures, timeout=30)

        # Flush and pre-fetch
        ws_fetcher.flush_subscriptions(cryptos)
        ws_fetcher.enable_price_logs()
        print(f"  [READY] Thresholds OFF - skipping strike price display")

        strikes_fetched_at_startup = True

    # Quick check for prices (should already have them from REST pre-fetch)
    print("\n  [PRICES] Checking prices from pre-fetch...")
    missing_prices = []
    have_prices = []
    for crypto in cryptos:
        kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
        poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)

        missing = []
        if not kalshi_price:
            missing.append("K")
        if not poly_price:
            missing.append("P")

        if missing:
            missing_prices.append(f"{crypto}({','.join(missing)})")
        else:
            have_prices.append(crypto)

    if not missing_prices:
        print(f"  [READY] All prices received: {', '.join(have_prices)}")
    else:
        # Brief retry if any missing (just 3 quick attempts)
        print(f"  [RETRY] Missing: {', '.join(missing_prices)} - quick retry...")
        for retry in range(3):
            time.sleep(0.5)  # Very brief wait
            missing_prices = []
            for crypto in cryptos:
                kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                missing = []
                if not kalshi_price:
                    missing.append("K")
                if not poly_price:
                    missing.append("P")
                if missing:
                    missing_prices.append(f"{crypto}({','.join(missing)})")
            if not missing_prices:
                print(f"  [READY] All prices received after retry {retry + 1}")
                break
        if missing_prices:
            print(f"  [WARN] Still missing after retries: {', '.join(missing_prices)} - continuing anyway")

    # Strikes are always fetched before starting (after 2 min wait or immediately if past)
    # No "fast start" mode anymore - we always have strikes before trading

    # Clear the missing logged set so we can log new misses during trading
    if hasattr(ws_fetcher, '_missing_logged'):
        ws_fetcher._missing_logged.clear()

    scan_num = 0
    last_prices = {}  # Track last prices to detect updates
    last_prices_1h = {}  # Track 1h prices for dedup
    last_log_state = {}  # Track last logged state per crypto to avoid repetitive logging
    traded_cryptos = {}  # Track trades per crypto this window: {crypto: count}
    crypto_edges = {}  # Track edges per crypto: {crypto: [edge1, edge2, ...]}
    stopped_cryptos = set()  # Cryptos stopped after avg edge < 2% (checked after 3 trades)
    window_contracts_traded = {}  # Per-crypto contracts traded this window: {crypto: count}
    MIN_TRADES_BEFORE_CHECK = 3  # Always allow first 3 trades
    MIN_AVG_EDGE_TO_CONTINUE = 4.5  # After 3 trades, continue if avg edge >= 4.5%
    MAX_TRADES_PER_CRYPTO = NO_THRESHOLD_MAX_TRADES_PER_CRYPTO if not use_strike_thresholds else 6  # per crypto (no-threshold) or 6 (with thresholds)

    # Track consecutive unwinds per crypto (informational only, no blocking)
    consecutive_unwinds = {}  # {crypto: count}

    # Track trades per window for summary notification (replaces per-trade Pushover)
    # Each entry: {crypto, market_type, direction, qty, kalshi_side, kalshi_fill_price,
    #              poly_fill_price, total_cost, fees, kalshi_strike, poly_strike,
    #              was_unwound, unwind_loss, status}
    window_trades = []

    # Hourly balance notification tracking
    last_balance_hour = -1  # Track which hour we last sent balance notification

    # 5-minute market tracking (BTC only - Kalshi 15m vs Poly 5m cross-strike arb)
    five_min_data = None  # Cached 5-min market data for this window
    five_min_fetched_this_window = False  # Whether we've fetched the 5-min market
    five_min_chainlink_tried = False  # Whether we tried Chainlink at t+601s

    # 1-hour market tracking
    last_1h_window = get_current_1h_market_window()  # Track current 1h window
    traded_cryptos_1h = {}  # Track 1h trades per crypto: {crypto: count}
    stopped_cryptos_1h = set()  # Cryptos that hit trade limit for 1h this hour
    one_hour_subscribed = False  # Whether we've subscribed to 1h markets this hour
    one_hour_1h_strikes_ready = False  # Whether 1h Chainlink strikes are captured

    # Strike thresholds (defined here so it's always available)
    STRIKE_THRESHOLDS = {
        "BTC": 0.02,  # 0.02% max difference
        "ETH": 0.02,  # 0.02% max difference
        "SOL": 0.02,  # 0.02% max difference
        "XRP": 0.02,  # 0.02% max difference
    }

    # Track current window for re-subscription
    # Use the window captured BEFORE Phase 1/Phase 2 setup (which can take 2+ minutes)
    # If we called get_current_market_window() here, the window may have already changed
    # during setup, causing the first window rotation to be silently missed
    last_ws_window = _startup_window

    # Track window timing (strikes are always fetched before starting now)
    window_start_time = datetime.now(timezone.utc)
    strikes_fetched_this_window = True  # Always true now - strikes fetched before trading
    failed_threshold_cryptos = failed_threshold_cryptos_startup.copy()

    # Track previous window data for double loss detection at window rotation
    prev_window_start_time = window_start_time.isoformat()
    prev_window_traded_cryptos = set()

    if use_strike_thresholds:
        print(f"\n  [READY] Strike thresholds active - trading enabled")
    else:
        print(f"\n  [READY] Strike thresholds DISABLED - trading ALL cryptos")
    print(f"  [GO] Starting to scan for opportunities...")

    try:
        while True:  # Run forever
            # Check if all cryptos are stopped (had a trade with edge < 1%)
            all_stopped = all(c in stopped_cryptos for c in cryptos)
            if all_stopped:
                current_ws_window = get_current_market_window()
                if current_ws_window == last_ws_window:
                    # Still same window, wait
                    print(f"\r  [WAITING] All cryptos traded. Waiting for next 15-min window...", end="", flush=True)
                    time.sleep(2)
                    continue
                # Window changed, will be handled below

            # Check market window rotation
            check_market_window_rotation()

            # ═══════════════════════════════════════════════════════
            # 1-HOUR WINDOW ROTATION CHECK
            # Separate from 15-min: only fires on hour boundaries.
            # Clears 1h caches and re-subscribes to new 1h markets.
            # ═══════════════════════════════════════════════════════
            current_1h_window = get_current_1h_market_window()
            if current_1h_window != last_1h_window:
                print(f"\n  ═══════════════════════════════════════════════════")
                print(f"  [1H WINDOW] {last_1h_window} -> {current_1h_window}")
                print(f"  ═══════════════════════════════════════════════════")
                last_1h_window = current_1h_window
                traded_cryptos_1h.clear()
                stopped_cryptos_1h.clear()
                ws_fetcher._ticker_cache_1h.clear()
                ws_fetcher._poly_token_cache_1h.clear()
                ws_fetcher._kalshi_strikes_1h.clear()
                ws_fetcher._poly_strikes_1h.clear()
                one_hour_subscribed = False
                one_hour_1h_strikes_ready = False

            # Subscribe to 1h markets (once per hour, after 15-min setup is done)
            if ENABLE_1H_TRADING and not one_hour_subscribed and strikes_fetched_this_window:
                print(f"\n  [1H SETUP] Subscribing to 1-hour markets...")
                for crypto in cryptos:
                    ws_fetcher.subscribe_crypto_1h(crypto)
                one_hour_subscribed = True

                # Capture 1h Chainlink strikes (at hour boundary they match 15-min strikes)
                if ws_fetcher.ws_manager and ws_fetcher.ws_manager.chainlink_feed:
                    feed = ws_fetcher.ws_manager.chainlink_feed
                    for c in cryptos:
                        strike_1h = feed.get_strike_1h(c)
                        if strike_1h and strike_1h > 0:
                            ws_fetcher._poly_strikes_1h[c] = poly_round(strike_1h)
                            if not one_hour_1h_strikes_ready:
                                print(f"  [1H STRIKES] Chainlink 1h strikes available")
                                one_hour_1h_strikes_ready = True

                # Show 1h market summary
                print(f"  [1H SETUP] 1-hour markets:")
                for c in cryptos:
                    k_data = ws_fetcher._ticker_cache_1h.get(c)
                    p_data = ws_fetcher._poly_token_cache_1h.get(c)
                    k_strike = ws_fetcher._kalshi_strikes_1h.get(c)
                    p_strike = ws_fetcher._poly_strikes_1h.get(c)
                    k_ticker = k_data[0] if k_data else "N/A"
                    p_slug = p_data[2] if p_data else "N/A"
                    print(f"    {c}: K={k_ticker} P={p_slug}")
                    if k_strike or p_strike:
                        print(f"         K_strike={fmt_strike(k_strike)}" if k_strike else "         K_strike=N/A", end="")
                        print(f"  P_strike={fmt_strike(p_strike)}" if p_strike else "  P_strike=N/A")

            # Check if window changed - need to re-subscribe to new markets
            current_ws_window = get_current_market_window()
            if current_ws_window != last_ws_window:
                print(f"\n  ═══════════════════════════════════════════════════")
                print(f"  [WINDOW CHANGE] {last_ws_window} -> {current_ws_window}")
                print(f"  ═══════════════════════════════════════════════════")

                # Clear old caches and WebSocket subscriptions
                ws_fetcher._ticker_cache.clear()
                ws_fetcher._poly_token_cache.clear()
                ws_fetcher._kalshi_strikes.clear()
                ws_fetcher._poly_strikes.clear()
                ws_fetcher._cache_window.clear()
                ws_fetcher.ws_manager.clear_all_subscriptions()
                last_prices.clear()
                last_log_state.clear()  # Reset log states for new window

                # Note: Do NOT clear chainlink_feed.window_strikes here.
                # The feed handles its own clearing during rotate_if_needed().
                # Clearing externally can delete already-captured boundary strikes
                # that can never be recaptured (exact timestamp has passed).

                # Save previous window start time for double loss checking
                prev_window_start_time = window_start_time.isoformat() if hasattr(window_start_time, 'isoformat') else str(window_start_time)
                prev_window_traded_cryptos = set(traded_cryptos.keys())

                # Save previous window trades for summary notification
                prev_window_trades = list(window_trades)
                prev_window_label = last_ws_window  # e.g., "2026-02-14_22-15"

                # Reset tracking for new window
                last_ws_window = current_ws_window
                window_trades.clear()
                traded_cryptos.clear()
                crypto_edges.clear()
                stopped_cryptos.clear()
                consecutive_unwinds.clear()
                window_contracts_traded.clear()
                five_min_data = None
                five_min_fetched_this_window = False
                five_min_chainlink_tried = False
                failed_threshold_cryptos.clear()
                ws_fetcher._strikes_pre_cleared = False  # Reset pre-clear flag for new window
                window_start_time = datetime.now(timezone.utc)  # Reset for new window
                scan_num = 0

                # ═══════════════════════════════════════════════════════
                # STEP 1: CAPTURE CHAINLINK t+0s PRICE (Poly strike)
                # Must get exact boundary price for ALL cryptos.
                # If ANY are missing → skip the entire window.
                # ═══════════════════════════════════════════════════════
                print(f"\n  [STEP 1] Capturing Chainlink t+0s prices...")
                poly_strikes_from_feed = {}
                chainlink_ok = False
                if ws_fetcher.ws_manager and ws_fetcher.ws_manager.chainlink_feed:
                    feed = ws_fetcher.ws_manager.chainlink_feed
                    # Calculate the expected window in the feed's format (YYYYMMDD_HHMM)
                    now_step1 = datetime.now(timezone.utc)
                    expected_feed_window = f"{now_step1.strftime('%Y%m%d_%H')}{(now_step1.minute // 15) * 15:02d}"

                    # Force feed to rotate to current window based on system clock.
                    # This ensures the feed is on the correct window even if no
                    # WebSocket messages have arrived since the boundary.
                    feed.rotate_if_needed()

                    # Verify rotation succeeded
                    if feed._current_window != expected_feed_window:
                        print(f"    [ERROR] Chainlink feed rotation failed (expected={expected_feed_window}, got={getattr(feed, '_current_window', 'N/A')})")
                        print(f"    Cannot use stale strikes — SKIPPING ENTIRE WINDOW")
                        continue

                    # Wait up to 5s for all cryptos to get their t+0s strike (exact match only)
                    for wait_i in range(10):
                        all_have_strikes = True
                        for c in cryptos:
                            strike = feed.get_strike(c)
                            if strike and strike > 0:
                                poly_strikes_from_feed[c] = strike
                            else:
                                all_have_strikes = False
                        if all_have_strikes:
                            break
                        time.sleep(0.5)
                    captured = [c for c in cryptos if poly_strikes_from_feed.get(c)]
                    missing = [c for c in cryptos if not poly_strikes_from_feed.get(c)]
                    # Write captured Chainlink strikes to Poly strikes
                    for c in captured:
                        sv = poly_strikes_from_feed[c]
                        ws_fetcher._poly_strikes[c] = poly_round(sv)
                        sfmt = f"${sv:,.5f}" if sv < 10 else f"${sv:,.4f}" if sv < 100 else f"${sv:,.2f}"
                        print(f"    {c}: {sfmt} (Chainlink t+0s)")
                    if missing:
                        print(f"    MISSING: {', '.join(missing)} — SKIPPING ENTIRE WINDOW")
                        print(f"    (Chainlink did not capture exact boundary price)")
                    else:
                        chainlink_ok = True
                else:
                    print(f"    [ERROR] No Chainlink feed available — SKIPPING ENTIRE WINDOW")

                # GATE: If Chainlink missed any strike, skip to next window
                if not chainlink_ok:
                    # Still try to send summary even if Chainlink missed (no settlement data)
                    if prev_window_trades:
                        try:
                            # Format window label from prev_window_label (e.g., "2026-02-14_22-15")
                            parts = prev_window_label.split("_")
                            wl = f"{parts[0]} {parts[1].replace('-', ':')} UTC" if len(parts) >= 2 else prev_window_label
                            send_window_summary_notification(wl, prev_window_trades, {}, {})
                        except Exception as e:
                            print(f"  [SUMMARY] Error: {e}")
                    print(f"\n  [SKIP] Waiting for next 15-min window...")
                    continue

                # (Window summary notification is sent after Kalshi strikes
                # are fetched at t+60s so we have correct end prices for both
                # exchanges: CF Benchmarks for Kalshi, Chainlink for Poly)

                # ═══════════════════════════════════════════════════════
                # STEP 2: REDEEM ONE TRADE
                # Quick single-position redemption before anything else.
                # ═══════════════════════════════════════════════════════
                print(f"\n  [STEP 2] Attempting to redeem 1 position...")
                try:
                    redeemables = poly.get_redeemable_positions()
                    if redeemables:
                        pos = redeemables[0]
                        condition_id = pos.get('conditionId')
                        title = pos.get('title', 'Unknown')[:50]
                        value = float(pos.get('currentValue', 0))
                        print(f"    Redeeming: {title}... (${value:.2f})")
                        tx_hash, gas_fee = poly.redeem_positions(condition_id)
                        if tx_hash:
                            print(f"    Redeemed successfully (gas: {gas_fee:.4f} MATIC)")
                        else:
                            print(f"    Failed to redeem")
                    else:
                        print(f"    No redeemable positions")
                except Exception as e:
                    print(f"    Redeem error: {e}")

                # ═══════════════════════════════════════════════════════
                # STEP 3: BALANCE NOTIFICATION (every 15-min window)
                # ═══════════════════════════════════════════════════════
                now_balance = datetime.now(timezone.utc)
                print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
                print(f"  ║  BALANCE UPDATE ({now_balance.strftime('%H:%M UTC')})                                ║")
                print(f"  ╚════════════════════════════════════════════════════════════════╝")
                try:
                    send_balance_notification(kalshi, poly)
                except Exception as e:
                    print(f"  [BALANCE] Error: {e}")

                # ═══════════════════════════════════════════════════════
                # STEP 4: KALSHI SETUP (at t+60s)
                # Wait until 60s into the window so Kalshi has time to
                # create new markets. Retry for up to 5 minutes if needed.
                # ═══════════════════════════════════════════════════════
                if use_strike_thresholds:
                    KALSHI_ROTATION_WAIT = 60  # Seconds into window before fetching Kalshi
                    KALSHI_MAX_RETRY_SECS = 300  # Give up after 5 minutes
                    now_wc = datetime.now(timezone.utc)
                    secs_into_new_window = (now_wc.minute % 15) * 60 + now_wc.second
                    if secs_into_new_window < KALSHI_ROTATION_WAIT:
                        kalshi_wait = KALSHI_ROTATION_WAIT - secs_into_new_window
                        print(f"\n  [KALSHI] t+{secs_into_new_window}s — waiting {kalshi_wait:.0f}s for Kalshi markets to be created...")
                        time.sleep(kalshi_wait)
                        now_wc = datetime.now(timezone.utc)
                        secs_into_new_window = (now_wc.minute % 15) * 60 + now_wc.second
                    print(f"\n  [KALSHI] Fetching Kalshi strike prices at t+{secs_into_new_window}s...")

                    print(f"  [KALSHI] Getting Kalshi connections and strike prices...")
                    kalshi_start_time = time.time()
                    kalshi_found = False
                    attempt = 0
                    while (time.time() - kalshi_start_time) < KALSHI_MAX_RETRY_SECS:
                        attempt += 1
                        for crypto in cryptos:
                            if not ws_fetcher._ticker_cache.get(crypto):
                                ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)
                        # Check which cryptos still need markets
                        missing_markets = [c for c in cryptos if not ws_fetcher._ticker_cache.get(c)]
                        if not missing_markets:
                            kalshi_found = True
                            break
                        elapsed = time.time() - kalshi_start_time
                        remaining = KALSHI_MAX_RETRY_SECS - elapsed
                        if remaining <= 0:
                            break
                        wait = min(15, remaining)
                        print(f"  [KALSHI] Markets not found for {', '.join(missing_markets)} - retrying in {wait:.0f}s (attempt {attempt}, {remaining:.0f}s left)...")
                        time.sleep(wait)

                    if not kalshi_found:
                        missing_markets = [c for c in cryptos if not ws_fetcher._ticker_cache.get(c)]
                        print(f"  [KALSHI] FAILED: Still missing markets for {', '.join(missing_markets)} after 5 minutes — skipping window")
                        continue

                    # Flush subscriptions and pre-fetch prices
                    ws_fetcher.flush_subscriptions(cryptos)

                    # Verify prices are available
                    all_have_prices = True
                    missing_prices = []
                    for crypto in cryptos:
                        kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                        poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                        if not kalshi_price:
                            missing_prices.append(f"{crypto} Kalshi")
                            all_have_prices = False
                        if not poly_price:
                            missing_prices.append(f"{crypto} Poly")
                            all_have_prices = False

                    if not all_have_prices:
                        print(f"  [MISSING] {', '.join(missing_prices)} - retrying...")
                        for _ in range(5):
                            time.sleep(1)
                            ws_fetcher.ws_manager.prefetch_all_prices(cryptos)
                            all_have_prices = True
                            for crypto in cryptos:
                                kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                                poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                                if not kalshi_price or not poly_price:
                                    all_have_prices = False
                                    break
                            if all_have_prices:
                                break

                    # Show Kalshi strikes obtained so far
                    print(f"\n  [PHASE 1] Kalshi strike prices:")
                    missing_kalshi = []
                    for c in cryptos:
                        k_strike = ws_fetcher._kalshi_strikes.get(c)
                        if k_strike:
                            print(f"    {c}: {fmt_strike(k_strike)}")
                        else:
                            print(f"    {c}: N/A (will retry)")
                            missing_kalshi.append(c)

                    # Retry missing Kalshi strikes using full retry logic (market detail API)
                    if missing_kalshi:
                        print(f"\n  [PHASE 1] Retrying missing Kalshi strikes: {', '.join(missing_kalshi)}")
                        for c in missing_kalshi:
                            k_strike, _ = ws_fetcher._fetch_strike_with_retry(c, max_retries=2)
                            if k_strike:
                                ws_fetcher._kalshi_strikes[c] = k_strike
                                print(f"    {c}: {fmt_strike(k_strike)} (recovered)")
                            else:
                                print(f"    {c}: Still N/A after retry")

                    # --- Poly strikes already captured at STEP 1 (Chainlink t+0s) ---
                    now_p2r = datetime.now(timezone.utc)
                    secs_p2r = (now_p2r.minute % 15) * 60 + now_p2r.second
                    print(f"\n  [POLY] Poly strikes (from Chainlink at t+0s, now t+{secs_p2r}s)")
                    for c in cryptos:
                        p_strike = ws_fetcher._poly_strikes.get(c)
                        pfmt = f"${p_strike:,.5f}" if p_strike < 10 else f"${p_strike:,.4f}" if p_strike < 100 else f"${p_strike:,.2f}"
                        print(f"    {c}: {pfmt} (Chainlink t+0s)")

                    # Display full strike comparison with threshold check
                    STRIKE_THRESHOLDS = {
                        "BTC": 0.03,  # 0.03% max difference
                        "ETH": 0.03,  # 0.03% max difference
                        "SOL": 0.03,  # 0.03% max difference
                        "XRP": 0.03,  # 0.03% max difference
                    }

                    strike_now = datetime.now(timezone.utc)
                    strike_date_str = strike_now.strftime("%Y-%m-%d %H:%M:%S UTC")
                    strike_lines = []
                    strike_lines.append(f"  ╔══════════════════════════════════════════════════════════════════════════════════╗")
                    strike_lines.append(f"  ║  STRIKE PRICES  |  {strike_date_str}  |  Poly@t+0s Chainlink + Kalshi@t+60s API  ║")
                    strike_lines.append(f"  ╠══════════════════════════════════════════════════════════════════════════════════╣")
                    for c in cryptos:
                        k_strike = ws_fetcher._kalshi_strikes.get(c)
                        p_strike = ws_fetcher._poly_strikes.get(c)
                        if k_strike and p_strike:
                            diff_pct = abs(k_strike - p_strike) / k_strike * 100
                            threshold = STRIKE_THRESHOLDS.get(c, 0.10)
                            if diff_pct <= threshold:
                                status = "GOOD"
                            else:
                                status = "BAD - SKIP"
                                failed_threshold_cryptos.add(c)
                            strike_lines.append(f"  ║  {c}: K={fmt_strike(k_strike)}  P={fmt_strike(p_strike)}  (diff: {diff_pct:.3f}%) [{status}]")
                        else:
                            k_display = f"{fmt_strike(k_strike)}" if k_strike else "N/A"
                            p_display = f"{fmt_strike(p_strike)}" if p_strike else "N/A"
                            strike_lines.append(f"  ║  {c}: K={k_display}  P={p_display}  (MISSING!) [BAD - SKIP]")
                            failed_threshold_cryptos.add(c)
                    strike_lines.append(f"  ╚══════════════════════════════════════════════════════════════════════════════════╝")
                    for line in strike_lines:
                        print(line)
                    log_highlight("\n".join(strike_lines) + "\n\n")

                    # Log strikes to Historical_Data
                    for c in cryptos:
                        k_strike = ws_fetcher._kalshi_strikes.get(c)
                        p_strike = ws_fetcher._poly_strikes.get(c)
                        if k_strike and p_strike:
                            # Finalize previous window's historical data
                            historical._finalize_crypto(c, k_strike, p_strike)
                            # Record new window's strikes
                            historical.record_prices(c, k_strike, p_strike)

                    # ═══════════════════════════════════════════════════════
                    # WINDOW SUMMARY NOTIFICATION
                    # Sent after Kalshi strikes are fetched at t+60s so we
                    # have correct end prices for both exchanges:
                    #   Kalshi end price  = new window's Kalshi strike (CF Benchmarks)
                    #   Poly end price    = Chainlink t+0s price
                    # ═══════════════════════════════════════════════════════
                    if prev_window_trades:
                        try:
                            # Kalshi settlement = new window's Kalshi strike (CF Benchmarks)
                            kalshi_settlement = {}
                            for c in cryptos:
                                k_s = ws_fetcher._kalshi_strikes.get(c)
                                if k_s:
                                    kalshi_settlement[c] = k_s

                            # Poly settlement = Chainlink t+0s price (captured at STEP 1)
                            poly_settlement = {}
                            for c in cryptos:
                                if c in poly_strikes_from_feed:
                                    poly_settlement[c] = poly_strikes_from_feed[c]

                            parts = prev_window_label.split("_")
                            wl = f"{parts[0]} {parts[1].replace('-', ':')} UTC" if len(parts) >= 2 else prev_window_label

                            send_window_summary_notification(wl, prev_window_trades, kalshi_settlement, poly_settlement)
                        except Exception as e:
                            print(f"  [SUMMARY] Error sending window summary: {e}")

                    # Settle previous window's trades with end prices
                    # Kalshi end = new Kalshi strike (CF Benchmarks)
                    # Poly end = Chainlink t+0s price
                    if prev_window_traded_cryptos:
                        print(f"\n  [SETTLEMENT] Settling previous window trades...")
                        for c in prev_window_traded_cryptos:
                            k_end = ws_fetcher._kalshi_strikes.get(c)
                            p_end = poly_strikes_from_feed.get(c)
                            if k_end and p_end:
                                settled = update_trade_settlements(c, prev_window_start_time, k_end, p_end)
                                if settled > 0:
                                    print(f"  [SETTLEMENT] {c}: Settled {settled} trade(s) (K end ${k_end:,.2f}, P end ${p_end:,.2f})")
                                else:
                                    print(f"  [SETTLEMENT] {c}: No unsettled trades found")
                            else:
                                missing = []
                                if not k_end:
                                    missing.append("Kalshi strike")
                                if not p_end:
                                    missing.append("Chainlink price")
                                print(f"  [SETTLEMENT] {c}: Cannot settle - missing {', '.join(missing)}")
                        prev_window_traded_cryptos.clear()  # Done checking
                else:
                    # No-threshold mode: send window summary with Chainlink as Poly settlement
                    # (no Kalshi strikes available in no-threshold mode)
                    if prev_window_trades:
                        try:
                            poly_settlement = {}
                            if ws_fetcher.ws_manager and ws_fetcher.ws_manager.chainlink_feed:
                                feed = ws_fetcher.ws_manager.chainlink_feed
                                for c in cryptos:
                                    strike = feed.get_strike(c)
                                    if strike and strike > 0:
                                        poly_settlement[c] = strike
                            parts = prev_window_label.split("_")
                            wl = f"{parts[0]} {parts[1].replace('-', ':')} UTC" if len(parts) >= 2 else prev_window_label
                            send_window_summary_notification(wl, prev_window_trades, {}, poly_settlement)
                        except Exception as e:
                            print(f"  [SUMMARY] Error: {e}")

                    # No-threshold mode: subscribe immediately, no wait
                    print(f"  [SUBSCRIBING] Getting market data (no strikes)...")
                    for crypto in cryptos:
                        ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)

                    ws_fetcher.flush_subscriptions(cryptos)

                    # Verify prices are available
                    all_have_prices = True
                    missing_prices = []
                    for crypto in cryptos:
                        kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                        poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                        if not kalshi_price:
                            missing_prices.append(f"{crypto} Kalshi")
                            all_have_prices = False
                        if not poly_price:
                            missing_prices.append(f"{crypto} Poly")
                            all_have_prices = False

                    if not all_have_prices:
                        print(f"  [MISSING] {', '.join(missing_prices)} - retrying...")
                        for _ in range(5):
                            time.sleep(1)
                            ws_fetcher.ws_manager.prefetch_all_prices(cryptos)
                            all_have_prices = True
                            for crypto in cryptos:
                                kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                                poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                                if not kalshi_price or not poly_price:
                                    all_have_prices = False
                                    break
                            if all_have_prices:
                                break

                    print(f"  [READY] Thresholds OFF - skipping strike price display")

                # Mark strikes as fetched - ready to trade
                from datetime import datetime, timezone
                window_start_time = datetime.now(timezone.utc)
                strikes_fetched_this_window = True

                print(f"\n  [READY] {'Strikes fetched' if use_strike_thresholds else 'Prices loaded'} - trading enabled!")
                print(f"  [GO] Starting to scan for opportunities...")
                continue

            # Check balances with specific thresholds
            k_bal, p_bal = trader.get_balances()

            # If Kalshi drops below $20 - STOP completely
            if k_bal < 20.0:
                print(f"\n  [STOPPED] Kalshi balance ${k_bal:.2f} is below $20 minimum - stopping program")
                break

            # If Polymarket drops below $20 - try to redeem positions and continue
            if p_bal < 20.0:
                print(f"\n  [LOW BALANCE] Polymarket balance ${p_bal:.2f} is below $20")
                print(f"  [REDEEM] Attempting to redeem all available positions...")

                try:
                    # Use the existing poly client from trader, or create new one
                    poly_client = trader._poly_client if hasattr(trader, '_poly_client') else poly

                    # Redeem all positions using the batch method
                    redeemed_count, gas_fees = poly_client.redeem_all_positions()
                    if redeemed_count > 0:
                        print(f"  [REDEEM] ✓ Redeemed {redeemed_count} positions (gas: {gas_fees:.4f} MATIC)")

                        # Re-check balance after redemption
                        time.sleep(5)  # Wait for blockchain
                        new_p_bal = poly_client.get_balance()
                        if new_p_bal:
                            p_bal = new_p_bal
                            print(f"  [REDEEM] New Polymarket balance: ${p_bal:.2f}")
                    else:
                        print(f"  [REDEEM] No positions redeemed")
                except Exception as e:
                    print(f"  [REDEEM] Error during redemption: {e}")

                # If still below minimum after redemption, wait and continue (don't stop)
                if p_bal < MIN_BALANCE_TO_TRADE:
                    print(f"  [WAIT] Polymarket balance still low (${p_bal:.2f}), waiting for deposits...")
                    time.sleep(30)
                    continue

            # Check WebSocket status
            status = ws_fetcher.is_connected()
            if not status.get("kalshi") or not status.get("polymarket"):
                print(f"\n  [WARN] WebSocket disconnected: {status}")
                time.sleep(2)
                continue

            scan_num += 1

            # Retry subscribing cryptos that don't have a valid market (every 10 scans ~5s)
            if scan_num % 10 == 0:
                any_missing = False
                for crypto in cryptos:
                    ticker_data = ws_fetcher._ticker_cache.get(crypto)
                    if not ticker_data or not ticker_data[0]:
                        if not any_missing:
                            any_missing = True
                            print(f"  [RETRY] Retrying market subscription...")
                        ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)
                if any_missing:
                    # Give Kalshi time to create markets
                    time.sleep(5)

            # Get snapshots from WebSocket cache (instant - no API calls)
            snapshots = ws_fetcher.fetch_all(cryptos)

            opportunities = []
            scanned_any = False

            # Check window progress for time-based restrictions
            window_progress = get_window_progress()
            secs_left_in_window = (1.0 - window_progress) * 15 * 60

            # Hard cutoff: no trades at all in last 15 seconds (ALL modes, including safe bets)
            # This ensures we have time to capture the t+0s Chainlink price for the next window
            in_last_15s = secs_left_in_window <= 15

            # Pre-clear strike prices 15 seconds before next window to guarantee fresh data
            if in_last_15s and not getattr(ws_fetcher, '_strikes_pre_cleared', False):
                print(f"\n  [PRE-CLEAR] {secs_left_in_window:.0f}s left — clearing strike caches for next window")
                ws_fetcher._poly_strikes.clear()
                ws_fetcher._kalshi_strikes.clear()
                if ws_fetcher.ws_manager and ws_fetcher.ws_manager.chainlink_feed:
                    ws_fetcher.ws_manager.chainlink_feed.window_strikes.clear()
                ws_fetcher._strikes_pre_cleared = True

            if use_strike_thresholds:
                # Thresholds ON: no trading in last 1.5 minutes
                in_first_minute = False  # Handled by WAIT_BEFORE_TRADING_SECONDS (2 min wait)
                in_last_minute = window_progress >= 0.90  # 13.5/15 = 0.90 = last 1.5 minutes
            else:
                # Thresholds OFF: no trading in first 1 min or last 1.5 min
                in_first_minute = window_progress < NO_THRESHOLD_FIRST_MINUTE_PROGRESS  # First 60 seconds
                in_last_minute = window_progress >= NO_THRESHOLD_LAST_MINUTE_PROGRESS  # Last 1.5 minutes (90%+)

            # Block trading in first minute (no-threshold mode only)
            if in_first_minute:
                secs_in = window_progress * 15 * 60
                if scan_num % 20 == 0:
                    print(f"\r  [FIRST MIN] {secs_in:.0f}s into window - waiting until 60s...", end="", flush=True)
                scan_num += 1
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Last-2-min confirmation mode flag: when active, trades in the final
            # 2 minutes require external price confirmation (see LAST 2 MINUTES
            # CONFIRMATION block below).  Scanning and trading proceed normally
            # for the first 13 minutes — only the confirmation gate is applied
            # during the last 2 minutes.
            _last2_only = (
                (CHAINLINK_LAST_TWO_MIN_ENABLED and not BINANCE_LAST_TWO_MIN_ENABLED)
                or (BINANCE_LAST_TWO_MIN_ENABLED and not CHAINLINK_LAST_TWO_MIN_ENABLED)
            )

            # Hard block: no trades in last 15 seconds of window (ALL modes)
            if in_last_15s:
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Show periodic status every 10 scans
            if scan_num % 10 == 0:
                print(f"\n  ─── Scan #{scan_num} ───")

            # Clear scan buffer at start of each scan cycle
            clear_scan_buffer()

            for crypto_idx, crypto in enumerate(cryptos):
                # Rate limit buffer: small delay between crypto fetches to avoid API rate limits
                if crypto_idx > 0:
                    time.sleep(0.1)  # 100ms buffer between cryptos

                snap = snapshots.get(crypto)
                if not snap:
                    # Log missing prices explicitly
                    kalshi_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                    poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                    missing = []
                    if not kalshi_price:
                        missing.append("Kalshi")
                    if not poly_price:
                        missing.append("Poly")
                    if scan_num % 10 == 0:  # Only log missing every 10 scans
                        print(f"  [MISSING] {crypto}: No prices from {', '.join(missing) if missing else 'unknown'}")
                    continue

                # FETCH FRESH KALSHI ORDERBOOK PRICES (WebSocket prices can be stale!)
                kalshi_ticker = ws_fetcher._ticker_cache.get(crypto, (None,))[0]
                fresh_k_yes, fresh_k_no = None, None
                kalshi_yes_depth, kalshi_no_depth = 0, 0
                kalshi_yes_depth_buffered, kalshi_no_depth_buffered = 0, 0
                kalshi_fresh = False
                if kalshi_ticker:
                    # Fetch fresh Kalshi orderbook
                    result = fetch_fresh_kalshi_prices(
                        ws_fetcher._kalshi_client, kalshi_ticker, debug=False
                    )
                    fresh_k_yes, fresh_k_no, kalshi_yes_depth, kalshi_no_depth, kalshi_yes_depth_buffered, kalshi_no_depth_buffered = result
                else:
                    print(f"  [{crypto}] No Kalshi ticker in cache!")

                # Use fresh Kalshi prices if available, otherwise fall back to WebSocket
                if fresh_k_yes is not None and fresh_k_no is not None:
                    snap.k_up = fresh_k_yes
                    snap.k_down = fresh_k_no
                    kalshi_fresh = True
                    # Clear failed state so we log if it fails again
                    if last_log_state.get(crypto) == "kalshi_fetch_failed":
                        last_log_state[crypto] = "ok"
                else:
                    # Fresh fetch failed - DO NOT use stale WS prices
                    # Clear them so we skip this crypto until fresh data available
                    # Only log once per crypto until state changes
                    if last_log_state.get(crypto) != "kalshi_fetch_failed":
                        print(f"  [{crypto}] Fresh Kalshi fetch failed - skipping")
                        last_log_state[crypto] = "kalshi_fetch_failed"
                    continue

                # Validate Kalshi prices - only skip if BOTH sides are extreme (market not ready)
                k_yes = snap.k_up
                k_no = snap.k_down
                if k_yes is None or k_no is None:
                    if scan_num % 10 == 0:
                        print(f"  [{crypto}] Kalshi price missing (YES={k_yes}, NO={k_no}) - waiting...")
                    continue
                # Allow extreme prices on ONE side (valid market state near settlement)
                # Only skip if BOTH are extreme (market hasn't opened properly)
                if k_yes >= 0.98 and k_no >= 0.98:
                    if scan_num % 10 == 0:
                        print(f"  [{crypto}] Kalshi prices invalid (YES={k_yes:.2f}, NO={k_no:.2f}) - waiting...")
                    continue

                # ALSO FETCH FRESH POLYMARKET ORDERBOOK (WebSocket is often stale!)
                poly_fresh = False
                poly_tokens = ws_fetcher.ws_manager._poly_tokens.get(crypto)
                if poly_tokens:
                    up_token, down_token = poly_tokens[0], poly_tokens[1]
                    # Fetch both UP and DOWN orderbooks in parallel
                    import concurrent.futures
                    try:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                            up_future = executor.submit(fetch_poly_orderbook, up_token)
                            down_future = executor.submit(fetch_poly_orderbook, down_token)
                            up_asks = up_future.result(timeout=3)
                            down_asks = down_future.result(timeout=3)

                        # Update snap with fresh Poly prices AND cache orderbook depth
                        if up_asks:
                            snap.p_up = up_asks[0][0]  # Best ask for UP
                            snap.poly_up_asks = up_asks  # Cache full orderbook
                            poly_fresh = True
                        if down_asks:
                            snap.p_down = down_asks[0][0]  # Best ask for DOWN
                            snap.poly_down_asks = down_asks  # Cache full orderbook
                            poly_fresh = True
                    except Exception as e:
                        if DEBUG:
                            print(f"  [{crypto}] Poly orderbook fetch error: {e}")

                # Get Poly prices from WebSocket as fallback
                poly_price = ws_fetcher.ws_manager.get_polymarket_price(crypto)

                # Build price key using FRESH prices from both exchanges
                k_key = f"k_{snap.k_up:.3f}_{snap.k_down:.3f}" if snap.k_up and snap.k_down else "k_none"
                p_key = f"p_{snap.p_up:.3f}_{snap.p_down:.3f}" if snap.p_up and snap.p_down else "p_none"
                price_key = f"{crypto}_{k_key}_{p_key}"

                if price_key == last_prices.get(crypto):
                    # Prices unchanged - skip but show heartbeat every 10 scans
                    if scan_num % 10 == 0:
                        print(f"  [{crypto}] No price change")
                    continue
                last_prices[crypto] = price_key

                scanned_any = True

                # Calculate edge with FRESH prices
                best_dir, edge_pct, other_edge, breakdown = pick_best_direction(snap)
                raw_edge = edge_pct

                # Force direction based on strike prices: lower strike = UP, higher strike = DOWN
                # This is critical for cross-strike arbs where the gap between strikes
                # must work FOR us (price between strikes = both bets win)
                allowed_dir = snap.allowed_direction()
                if allowed_dir in ("K_UP+P_DOWN", "K_DOWN+P_UP") and allowed_dir != best_dir:
                    best_dir = allowed_dir
                    edge_pct = other_edge
                    raw_edge = other_edge
                    # Recalculate breakdown for the correct direction
                    if allowed_dir == "K_UP+P_DOWN":
                        _, breakdown = calculate_edge(DEFAULT_QTY_FOR_EDGE, snap.k_up, snap.p_down)
                    else:
                        _, breakdown = calculate_edge(DEFAULT_QTY_FOR_EDGE, snap.k_down, snap.p_up)

                # Detect SAFE BET conditions (same/penny strikes or cross-strike arb)
                is_safe_bet = False
                safe_bet_reason = ""
                if use_strike_thresholds and snap.kalshi_strike and snap.poly_strike:
                    strike_diff = abs(snap.kalshi_strike - snap.poly_strike)
                    if crypto in ("BTC", "ETH") and strike_diff <= 0.01:
                        is_safe_bet = True
                        safe_bet_reason = "penny strike"
                    elif strike_diff > 0:
                        # Cross-strike: check for price feed divergence
                        skip, bin_px, cl_px = should_skip_cross_strike(
                            snap.kalshi_strike, snap.poly_strike, crypto, ws_fetcher
                        )
                        if skip:
                            if scan_num % 20 == 0:
                                print(f"    [CROSS-SKIP] Price feeds diverge: Binance=${bin_px:,.2f} Chainlink=${cl_px:,.2f} K{fmt_strike(snap.kalshi_strike)} P{fmt_strike(snap.poly_strike)}")
                        else:
                            is_safe_bet = True
                            px_info = ""
                            if bin_px and cl_px:
                                px_info = f" Bin=${bin_px:,.0f} CL=${cl_px:,.0f}"
                            safe_bet_reason = f"cross-strike (K{fmt_strike(snap.kalshi_strike)} P{fmt_strike(snap.poly_strike)}){px_info}"

                # Clear price display showing both exchanges with depth
                k_yes_ask = f"{snap.k_up:.2f}" if snap.k_up else "N/A"
                k_no_ask = f"{snap.k_down:.2f}" if snap.k_down else "N/A"
                k_yes_depth_str = f"({kalshi_yes_depth})" if kalshi_yes_depth > 0 else ""
                k_no_depth_str = f"({kalshi_no_depth})" if kalshi_no_depth > 0 else ""

                # Polymarket depth from cached orderbooks
                poly_up_depth = int(snap.poly_up_asks[0][1]) if snap.poly_up_asks else 0
                poly_down_depth = int(snap.poly_down_asks[0][1]) if snap.poly_down_asks else 0
                p_up_ask = f"{snap.p_up:.2f}" if snap.p_up else "N/A"
                p_down_ask = f"{snap.p_down:.2f}" if snap.p_down else "N/A"
                p_up_depth_str = f"({poly_up_depth})" if poly_up_depth > 0 else ""
                p_down_depth_str = f"({poly_down_depth})" if poly_down_depth > 0 else ""
                kalshi_status = "(fresh)" if kalshi_fresh else "(WS-STALE!)"
                poly_status = "(fresh)" if poly_fresh else "(WS)"

                # Print scan box to console
                scan_box = []
                market_label = f"{crypto}/15"
                scan_box.append(f"  ┌─ {market_label} ─────────────────────────────────")
                scan_box.append(f"  │ KALSHI 15m: YES={k_yes_ask}{k_yes_depth_str}  NO={k_no_ask}{k_no_depth_str} {kalshi_status}")
                scan_box.append(f"  │ POLY 15m:   UP={p_up_ask}{p_up_depth_str}   DOWN={p_down_ask}{p_down_depth_str} {poly_status}")
                safe_label = f" ★SAFE({safe_bet_reason})" if is_safe_bet else ""
                scan_box.append(f"  │ EDGE: {raw_edge:.1f}% via {best_dir}{safe_label}")
                scan_box.append(f"  └──────────────────────────────────────────")
                for line in scan_box:
                    print(line)

                # Note: Early window skip handled by in_first_minute check (no-threshold) or WAIT_BEFORE_TRADING_SECONDS (thresholds)

                # NO LOSSES MODE: Only trade safe bets
                if no_losses_mode and not is_safe_bet:
                    continue

                # Skip trading near end of window (last 1.5min) - UNLESS safe bet
                # Bypass when last-2-min confirmation mode is active (trades go through to confirmation check)
                if in_last_minute and not is_safe_bet and not _last2_only:
                    if raw_edge >= 2.0:
                        secs_left = (1.0 - window_progress) * 15 * 60
                        label = "Last 1.5min"
                        print(f"    → {label} ({secs_left:.0f}s left) - NO TRADING (logging only)")
                        # Log the potential trade
                        k_yes_log = f"{snap.k_up:.4f}({kalshi_yes_depth})" if snap.k_up else "N/A"
                        k_no_log = f"{snap.k_down:.4f}({kalshi_no_depth})" if snap.k_down else "N/A"
                        p_up_log = f"{snap.p_up:.4f}({poly_up_depth})" if snap.p_up else "N/A"
                        p_down_log = f"{snap.p_down:.4f}({poly_down_depth})" if snap.p_down else "N/A"
                        log_potential_trade({
                            "timestamp": now_mst(),
                            "crypto": crypto,
                            "direction": best_dir,
                            "qty": MAX_TRADE_QTY,
                            "k_yes_price": k_yes_log,
                            "k_no_price": k_no_log,
                            "p_up_price": p_up_log,
                            "p_down_price": p_down_log,
                            "raw_edge_pct": raw_edge,
                            "traded": "last_1.5min",
                        })
                    continue

                # Skip if this crypto is stopped - UNLESS safe bet (trade forever)
                if crypto in stopped_cryptos and not is_safe_bet:
                    trades_done = traded_cryptos.get(crypto, 0)
                    edges = crypto_edges.get(crypto, [])
                    avg_edge = sum(edges) / len(edges) if edges else 0
                    print(f"    → {crypto} stopped after {trades_done} trades (avg edge {avg_edge:.1f}% < {MIN_AVG_EDGE_TO_CONTINUE}%)")
                    continue

                # Skip if per-crypto window contract cap reached
                crypto_contracts = window_contracts_traded.get(crypto, 0)
                if MAX_CONTRACTS_PER_WINDOW_ENABLED and crypto_contracts >= MAX_CONTRACTS_PER_WINDOW:
                    print(f"    → {crypto} CONTRACT CAP: {crypto_contracts}/{MAX_CONTRACTS_PER_WINDOW} contracts traded this window — skipping")
                    continue

                # Per-crypto minimum edge thresholds
                SAFE_BET_MIN_EDGE = 5.0
                WS_MIN_EDGE_MAP = {"BTC": 6.0, "ETH": 6.0, "SOL": 6.0, "XRP": 6.0}
                if is_safe_bet:
                    WS_MIN_EDGE = SAFE_BET_MIN_EDGE
                elif use_strike_thresholds:
                    WS_MIN_EDGE = WS_MIN_EDGE_MAP.get(crypto, 4.0)
                else:
                    # No-threshold mode: flat 12% edge, 1 trade per crypto
                    WS_MIN_EDGE = NO_THRESHOLD_MIN_EDGE_PCT
                WS_MAX_EDGE = 35.0  # Skip suspicious edges above 35%

                # Skip if edge too high (likely bad data)
                if raw_edge > WS_MAX_EDGE:
                    continue

                if raw_edge < WS_MIN_EDGE:
                    # Log all 2%+ edge opportunities (not tried - edge below threshold)
                    if raw_edge >= 2.0:
                        # Format prices with depth: "0.56(353)"
                        k_yes_log = f"{snap.k_up:.4f}({kalshi_yes_depth})" if snap.k_up else "N/A"
                        k_no_log = f"{snap.k_down:.4f}({kalshi_no_depth})" if snap.k_down else "N/A"
                        p_up_log = f"{snap.p_up:.4f}({poly_up_depth})" if snap.p_up else "N/A"
                        p_down_log = f"{snap.p_down:.4f}({poly_down_depth})" if snap.p_down else "N/A"
                        log_potential_trade({
                            "timestamp": now_mst(),
                            "crypto": crypto,
                            "direction": best_dir,
                            "qty": MAX_TRADE_QTY,
                            "k_yes_price": k_yes_log,
                            "k_no_price": k_no_log,
                            "p_up_price": p_up_log,
                            "p_down_price": p_down_log,
                            "raw_edge_pct": raw_edge,
                            "traded": "not_tried",
                        })
                    continue

                # Check if crypto failed threshold at startup (still log but don't trade)
                # Safe bets bypass this - cross-strike arbs will have different strikes
                if crypto in failed_threshold_cryptos and not is_safe_bet:
                    threshold = STRIKE_THRESHOLDS.get(crypto, 0.06)
                    if snap.kalshi_strike and snap.poly_strike:
                        diff_pct = abs(snap.kalshi_strike - snap.poly_strike) / snap.kalshi_strike * 100
                        print(f"    → {crypto} failed threshold ({diff_pct:.3f}% > {threshold}%) - logging only")
                        # Mark in historical data
                        historical.mark_failed_threshold(crypto, diff_pct, threshold)
                    else:
                        print(f"    → {crypto} failed threshold (missing strikes) - logging only")
                    # Log to potential trades
                    if raw_edge >= 2.0:
                        k_yes_log = f"{snap.k_up:.4f}({kalshi_yes_depth})" if snap.k_up else "N/A"
                        k_no_log = f"{snap.k_down:.4f}({kalshi_no_depth})" if snap.k_down else "N/A"
                        p_up_log = f"{snap.p_up:.4f}({poly_up_depth})" if snap.p_up else "N/A"
                        p_down_log = f"{snap.p_down:.4f}({poly_down_depth})" if snap.p_down else "N/A"
                        log_potential_trade({
                            "timestamp": now_mst(),
                            "crypto": crypto,
                            "direction": best_dir,
                            "qty": MAX_TRADE_QTY,
                            "k_yes_price": k_yes_log,
                            "k_no_price": k_no_log,
                            "p_up_price": p_up_log,
                            "p_down_price": p_down_log,
                            "raw_edge_pct": raw_edge,
                            "traded": f"failed_threshold ({diff_pct:.3f}% > {threshold}%)",
                        })
                    continue

                # Check strikes match with crypto-specific thresholds (if enabled)
                # Safe bets bypass threshold check (cross-strike arbs have different strikes by definition)
                if use_strike_thresholds and not is_safe_bet:
                    # Crypto-specific thresholds
                    threshold_map = {"BTC": 0.03, "ETH": 0.03, "SOL": 0.03, "XRP": 0.03}
                    max_strike_diff = threshold_map.get(crypto, 0.06)

                    if snap.kalshi_strike is None or snap.poly_strike is None:
                        # Skip trades if we don't have both strikes
                        k_str = f"${snap.kalshi_strike:.2f}" if snap.kalshi_strike else "N/A"
                        p_str = f"${snap.poly_strike:.2f}" if snap.poly_strike else "N/A"
                        print(f"    → Missing strike price (K={k_str} vs P={p_str}) - skipping")
                        continue
                    else:
                        # Calculate strike difference
                        strike_diff_pct = abs(snap.kalshi_strike - snap.poly_strike) / snap.kalshi_strike * 100
                        if strike_diff_pct > max_strike_diff:
                            # Mark in historical data
                            historical.mark_failed_threshold(crypto, strike_diff_pct, max_strike_diff)
                            # Log to potential trades
                            if raw_edge >= 2.0:
                                k_yes_log = f"{snap.k_up:.4f}({kalshi_yes_depth})" if snap.k_up else "N/A"
                                k_no_log = f"{snap.k_down:.4f}({kalshi_no_depth})" if snap.k_down else "N/A"
                                p_up_log = f"{snap.p_up:.4f}({poly_up_depth})" if snap.p_up else "N/A"
                                p_down_log = f"{snap.p_down:.4f}({poly_down_depth})" if snap.p_down else "N/A"
                                log_potential_trade({
                                    "timestamp": now_mst(),
                                    "crypto": crypto,
                                    "direction": best_dir,
                                    "qty": MAX_TRADE_QTY,
                                    "k_yes_price": k_yes_log,
                                    "k_no_price": k_no_log,
                                    "p_up_price": p_up_log,
                                    "p_down_price": p_down_log,
                                    "raw_edge_pct": raw_edge,
                                    "traded": f"failed_threshold ({strike_diff_pct:.3f}% > {max_strike_diff}%)",
                                })
                            continue

                # Good opportunity! Include depth info for liquidity check
                opportunities.append((crypto, snap, best_dir, edge_pct, breakdown, raw_edge, kalshi_yes_depth, kalshi_no_depth, kalshi_yes_depth_buffered, kalshi_no_depth_buffered, is_safe_bet, safe_bet_reason))

            # Sort by edge and trade best
            opportunities.sort(key=lambda x: x[5], reverse=True)

            if opportunities:
                crypto, snap, best_dir, edge_pct, breakdown, raw_edge, kalshi_yes_depth, kalshi_no_depth, kalshi_yes_depth_buffered, kalshi_no_depth_buffered, is_safe_bet, safe_bet_reason = opportunities[0]

                # Execute via WebSocket/SDK
                if best_dir == "K_UP+P_DOWN":
                    kalshi_side = "yes"
                    poly_direction = "down"
                    k_price = snap.k_up
                    p_price = snap.p_down
                    poly_token = snap.poly_down_token
                else:
                    kalshi_side = "no"
                    poly_direction = "up"
                    k_price = snap.k_down
                    p_price = snap.p_up
                    poly_token = snap.poly_up_token

                # Calculate dynamic qty based on strike gap
                # Tighter gap = more contracts (less settlement risk)
                # SAFE BET: liquidity-based sizing (30% of thinner book, min 15 depth, max 10 qty)
                if is_safe_bet:
                    # Get depth on the side we're buying for each platform
                    k_buy_depth = kalshi_no_depth if kalshi_side.lower() == "no" else kalshi_yes_depth
                    p_buy_depth = poly_down_depth if poly_direction == "down" else poly_up_depth
                    thin_side = min(k_buy_depth, p_buy_depth)
                    thin_label = "Kalshi" if k_buy_depth <= p_buy_depth else "Poly"

                    if k_buy_depth < 15 or p_buy_depth < 15:
                        print(f"    ★ SAFE BET ({safe_bet_reason}): SKIP - thin book (K={k_buy_depth}, P={p_buy_depth}, need 15+)")
                        continue

                    if raw_edge >= 3.0:
                        pct = 0.25
                        max_qty = MAX_TRADE_QTY
                    else:
                        pct = 0.15  # 1.5%-3% edge: smaller size
                        max_qty = MAX_TRADE_QTY
                    qty = min(int(thin_side * pct), max_qty)
                    # If over 75 contracts, scale back to 20% of book
                    if qty > 75:
                        qty = min(int(thin_side * 0.20), max_qty)
                    qty = max(qty, 1)  # at least 1 contract
                    print(f"    ★ SAFE BET ({safe_bet_reason}): {qty} contracts ({int(pct*100)}% of {thin_label}={thin_side}, K={k_buy_depth}/P={p_buy_depth}), edge {raw_edge:.1f}%")
                elif not use_strike_thresholds:
                    # No-threshold mode: always use fixed max qty
                    qty = NO_THRESHOLD_MAX_QTY
                elif snap.kalshi_strike and snap.poly_strike:
                    strike_gap_pct = abs(snap.kalshi_strike - snap.poly_strike) / snap.kalshi_strike * 100
                    qty = get_dynamic_qty(crypto, strike_gap_pct, snap.kalshi_strike, snap.poly_strike, best_dir)
                else:
                    qty = MAX_TRADE_QTY  # Fallback if no strike data

                # Skip if prices are None or clearly invalid (e.g., YES=0.01 with NO=None)
                if k_price is None or p_price is None:
                    print(f"  [SKIP] Missing price data: K={k_price} P={p_price}")
                    continue

                # Skip if prices are suspiciously low (likely incomplete data)
                if k_price < 0.05 or p_price < 0.02:
                    print(f"  [SKIP] Price too low (bad data?): K=${k_price:.2f} P=${p_price:.2f}")
                    continue

                # Check if either side costs more than 88 cents - skip if so
                if k_price > 0.88 or p_price > 0.88:
                    print(f"  [SKIP] Price too high: K=${k_price:.2f} P=${p_price:.2f} (max 88¢)")
                    continue

                # ─── LAST 2 MINUTES CONFIRMATION ─────────────────────────────
                # When exactly one of BINANCE/CHAINLINK_LAST_TWO_MIN_ENABLED is
                # true, confirm trade direction using the live external price
                # during the final 2 minutes (>=13 min into the 15-min window).
                # If both flags are true, skip this check (use existing logic).
                in_last_two_min = window_progress >= (13.0 / 15.0)  # ~0.8667
                use_binance_confirm = BINANCE_LAST_TWO_MIN_ENABLED and not CHAINLINK_LAST_TWO_MIN_ENABLED
                use_chainlink_confirm = CHAINLINK_LAST_TWO_MIN_ENABLED and not BINANCE_LAST_TWO_MIN_ENABLED

                if in_last_two_min and (use_binance_confirm or use_chainlink_confirm) and snap.poly_strike:
                    # Get the confirmation price from the selected source
                    if use_binance_confirm:
                        confirm_price = get_binance_price(crypto)
                        confirm_source = "Binance"
                    else:
                        confirm_price = (
                            ws_fetcher.ws_manager.get_chainlink_price(crypto)
                            if ws_fetcher.ws_manager else None
                        )
                        confirm_source = "Chainlink"

                    if confirm_price is not None:
                        poly_is_lower = snap.poly_strike < (snap.kalshi_strike or 0)
                        poly_is_higher = snap.poly_strike > (snap.kalshi_strike or 0)

                        direction_ok = True
                        if poly_is_lower and confirm_price <= snap.poly_strike:
                            # Poly is lower → confirm price must be HIGHER than Poly strike
                            direction_ok = False
                        elif poly_is_higher and confirm_price >= snap.poly_strike:
                            # Poly is higher → confirm price must be LOWER than Poly strike
                            direction_ok = False

                        if not direction_ok:
                            secs_left = (1.0 - window_progress) * 15 * 60
                            print(
                                f"  [LAST-2MIN] {confirm_source} ${confirm_price:,.2f} does NOT confirm "
                                f"direction (Poly strike ${snap.poly_strike:,.2f}, "
                                f"K strike ${snap.kalshi_strike:,.2f}) — skipping trade "
                                f"({secs_left:.0f}s left)"
                            )
                            log_potential_trade({
                                "timestamp": now_mst(),
                                "crypto": crypto,
                                "direction": best_dir,
                                "qty": qty,
                                "k_yes_price": f"{snap.k_up:.4f}" if snap.k_up else "N/A",
                                "k_no_price": f"{snap.k_down:.4f}" if snap.k_down else "N/A",
                                "p_up_price": f"{snap.p_up:.4f}" if snap.p_up else "N/A",
                                "p_down_price": f"{snap.p_down:.4f}" if snap.p_down else "N/A",
                                "raw_edge_pct": raw_edge,
                                "traded": f"last_2min_{confirm_source.lower()}_reject",
                            })
                            continue
                        else:
                            print(
                                f"  [LAST-2MIN] {confirm_source} ${confirm_price:,.2f} CONFIRMS "
                                f"direction (Poly strike ${snap.poly_strike:,.2f}) — proceeding"
                            )
                    else:
                        print(f"  [LAST-2MIN] Could not fetch {confirm_source} price — skipping trade")
                        continue
                # ─── END LAST 2 MINUTES CONFIRMATION ─────────────────────────

                # Show all prices clearly before trading
                # Flush buffered scan lines to highlights log (edge boxes that led to this trade)
                flush_scan_buffer_to_log()

                exec_lines = []
                exec_lines.append(f"  ╔═══════════════════════════════════════════════════════╗")
                exec_lines.append(f"  ║  EXECUTING 15m TRADE: {crypto}/15                        ║")
                exec_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                exec_lines.append(f"  ║  Strategy: {best_dir}")
                exec_lines.append(f"  ║  Edge: {raw_edge:.1f}%")
                exec_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                exec_lines.append(f"  ║  KALSHI (ask prices - what we pay to buy):")
                k_up_str = f"${snap.k_up:.2f}" if snap.k_up is not None else "N/A"
                k_down_str = f"${snap.k_down:.2f}" if snap.k_down is not None else "N/A"
                k_yes_depth_info = f" (depth: {kalshi_yes_depth})" if kalshi_yes_depth > 0 else ""
                k_no_depth_info = f" (depth: {kalshi_no_depth})" if kalshi_no_depth > 0 else ""
                exec_lines.append(f"  ║    YES: {k_up_str}{k_yes_depth_info}    NO: {k_down_str}{k_no_depth_info}")
                # Calculate actual order price (fresh + 1c buffer)
                actual_order_cents = int(k_price * 100) + 1
                k_depth_for_order = kalshi_no_depth if kalshi_side.lower() == "no" else kalshi_yes_depth
                k_depth_buffered = kalshi_no_depth_buffered if kalshi_side.lower() == "no" else kalshi_yes_depth_buffered
                exec_lines.append(f"  ║    → Buying {kalshi_side.upper()}: ${k_price:.2f}({k_depth_for_order}) → ${actual_order_cents/100:.2f}({k_depth_buffered})")
                exec_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                exec_lines.append(f"  ║  POLYMARKET (ask prices - what we pay to buy):")
                p_up_str = f"${snap.p_up:.2f}" if snap.p_up is not None else "N/A"
                p_down_str = f"${snap.p_down:.2f}" if snap.p_down is not None else "N/A"
                exec_lines.append(f"  ║    UP: {p_up_str}    DOWN: {p_down_str}")
                # Show depth from cached orderbook
                if poly_direction == "down" and snap.poly_down_asks:
                    depth_at_best = snap.poly_down_asks[0][1] if snap.poly_down_asks else 0
                    total_depth = sum(s for p, s in snap.poly_down_asks[:5]) if snap.poly_down_asks else 0
                    exec_lines.append(f"  ║    → Buying DOWN @ ${p_price:.2f} (depth: {depth_at_best:.0f} @ best, {total_depth:.0f} total)")
                elif poly_direction == "up" and snap.poly_up_asks:
                    depth_at_best = snap.poly_up_asks[0][1] if snap.poly_up_asks else 0
                    total_depth = sum(s for p, s in snap.poly_up_asks[:5]) if snap.poly_up_asks else 0
                    exec_lines.append(f"  ║    → Buying UP @ ${p_price:.2f} (depth: {depth_at_best:.0f} @ best, {total_depth:.0f} total)")
                else:
                    exec_lines.append(f"  ║    → Need to buy {poly_direction.upper()} @ ${p_price:.2f} or better")
                exec_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                exec_lines.append(f"  ║  Combined: ${actual_order_cents/100:.2f} + ${p_price:.2f} = ${actual_order_cents/100 + p_price:.2f}")
                exec_lines.append(f"  ║  Expected profit (with fees/gas/buffer): {raw_edge:.1f}%")
                exec_lines.append(f"  ╚═══════════════════════════════════════════════════════╝")
                for line in exec_lines:
                    print(line)
                print(f"")
                log_highlight("\n".join(exec_lines) + "\n")

                kalshi_ticker = ws_fetcher._ticker_cache.get(crypto, (None,))[0]
                if not kalshi_ticker:
                    print(f"  [FAIL] No Kalshi ticker for {crypto}")
                    continue

                # Check Polymarket depth for logging
                cached_asks = snap.poly_down_asks if poly_direction == "down" else snap.poly_up_asks
                if cached_asks:
                    depth_at_best = cached_asks[0][1]
                    total_depth = sum(s for p, s in cached_asks[:5])
                    print(f"  [DEPTH] {len(cached_asks)} levels, {depth_at_best:.0f} @ best, {total_depth:.0f} in top 5")

                # Use scan price directly + 1 cent buffer (scan already fetches fresh REST API data)
                k_price_cents_with_buffer = int(k_price * 100) + 1

                print(f"  [STEP 1] KALSHI: Placing {qty}x {kalshi_side.upper()} @ {k_price_cents_with_buffer}c...")

                # Check position BEFORE placing order
                pos_before = ws_fetcher._kalshi_client.get_position_for_ticker(kalshi_ticker)
                pos_before_qty = 0
                if pos_before:
                    position_val = pos_before.get("position", 0)
                    # YES positions are positive, NO positions are negative
                    if kalshi_side.lower() == "yes":
                        pos_before_qty = position_val if position_val > 0 else 0
                    else:
                        pos_before_qty = abs(position_val) if position_val < 0 else 0

                is_filled = False
                kalshi_filled_qty = 0

                # Place order at fresh price + 1 cent buffer
                kalshi_result = ws_fetcher._kalshi_client.place_order(
                    kalshi_ticker, kalshi_side, qty, k_price_cents_with_buffer
                )

                if not kalshi_result:
                    print(f"  [KALSHI] Order failed - no response")
                else:
                    kalshi_order = kalshi_result.get("order", {})
                    order_id = kalshi_order.get("order_id")
                    kalshi_status = kalshi_order.get("status", "").lower()
                    kalshi_filled_qty = kalshi_order.get("filled_count", 0)
                    remaining = kalshi_order.get("remaining_count", qty)

                    print(f"  [KALSHI] Response: id={order_id[:8] if order_id else 'N/A'}, status={kalshi_status}, filled={kalshi_filled_qty}, remaining={remaining}")

                    # Check if filled instantly
                    if kalshi_filled_qty > 0:
                        print(f"  [KALSHI] FILLED {kalshi_filled_qty}x instantly!")
                        is_filled = True
                    elif kalshi_status in ["executed", "filled"]:
                        print(f"  [KALSHI] Status={kalshi_status} - FILLED!")
                        kalshi_filled_qty = qty
                        is_filled = True
                    else:
                        # Quick recheck order status
                        time.sleep(0.1)
                        recheck = ws_fetcher._kalshi_client.get_order_status(order_id)
                        if recheck:
                            recheck_order = recheck.get("order", {})
                            kalshi_filled_qty = recheck_order.get("filled_count", 0)
                            recheck_status = recheck_order.get("status", "").lower()
                            print(f"  [KALSHI] Recheck: status={recheck_status}, filled={kalshi_filled_qty}")

                            if kalshi_filled_qty > 0 or recheck_status in ["executed", "filled"]:
                                print(f"  [KALSHI] FILLED {kalshi_filled_qty or qty}x!")
                                kalshi_filled_qty = kalshi_filled_qty or qty
                                is_filled = True

                        if not is_filled:
                            # Cancel the order immediately
                            ws_fetcher._kalshi_client.cancel_order(order_id)
                            time.sleep(0.1)

                            # Check order status after cancel
                            final = ws_fetcher._kalshi_client.get_order_status(order_id)
                            if final:
                                final_order = final.get("order", {})
                                final_filled = final_order.get("filled_count", 0)
                                final_status = final_order.get("status", "").lower()
                                print(f"  [KALSHI] After cancel: status={final_status}, filled={final_filled}")
                                if final_filled > 0:
                                    print(f"  [KALSHI] FILLED {final_filled}x!")
                                    kalshi_filled_qty = final_filled
                                    is_filled = True

                        # CRITICAL: Check actual position to catch fills the API missed
                        if not is_filled:
                            time.sleep(0.1)
                            pos_after = ws_fetcher._kalshi_client.get_position_for_ticker(kalshi_ticker)
                            if pos_after:
                                position_val = pos_after.get("position", 0)
                                # YES positions are positive, NO positions are negative
                                if kalshi_side.lower() == "yes":
                                    pos_after_qty = position_val if position_val > 0 else 0
                                else:
                                    pos_after_qty = abs(position_val) if position_val < 0 else 0

                                new_position = pos_after_qty - pos_before_qty
                                print(f"  [KALSHI] Position check: before={pos_before_qty}, after={pos_after_qty}, new={new_position}")

                                if new_position > 0:
                                    print(f"  [KALSHI] POSITION CONFIRMS FILL: {new_position}x!")
                                    kalshi_filled_qty = new_position
                                    is_filled = True

                if not is_filled:
                    print(f"  [FAIL] No fill detected (checked order status AND position)")
                    print(f"  [EDGE] {crypto}: {raw_edge:.1f}% edge missed")
                    print(f"  [CONTINUE] Looking for next opportunity...")
                    # Log to potential trades with traded=failed (Kalshi didn't fill)
                    k_yes_log = f"{snap.k_up:.4f}({kalshi_yes_depth})" if snap.k_up else "N/A"
                    k_no_log = f"{snap.k_down:.4f}({kalshi_no_depth})" if snap.k_down else "N/A"
                    p_up_log = f"{snap.p_up:.4f}({poly_up_depth})" if snap.p_up else "N/A"
                    p_down_log = f"{snap.p_down:.4f}({poly_down_depth})" if snap.p_down else "N/A"
                    log_potential_trade({
                        "timestamp": now_mst(),
                        "crypto": crypto,
                        "direction": best_dir,
                        "qty": qty,
                        "k_yes_price": k_yes_log,
                        "k_no_price": k_no_log,
                        "p_up_price": p_up_log,
                        "p_down_price": p_down_log,
                        "raw_edge_pct": raw_edge,
                        "traded": "failed",
                    })
                    continue

                # Use requested qty if filled_count is 0 but status shows filled
                actual_filled_qty = kalshi_filled_qty if kalshi_filled_qty > 0 else qty
                print(f"  [STEP 1] KALSHI: FILLED {actual_filled_qty}x @ {k_price_cents_with_buffer}c ✓")

                # STEP 2: Kalshi filled - now try to fill on Polymarket
                print(f"")
                print(f"  [STEP 2] POLYMARKET: Attempting to fill with FRESH orderbook...")
                actual_k_cost = k_price_cents_with_buffer / 100.0
                MIN_ACCEPTABLE_EDGE = -(MAX_LOSS_PCT * 100)  # Stop at -3% loss (from config)

                # Get the token ID for this direction
                poly_tokens = ws_fetcher.ws_manager._poly_tokens.get(crypto)
                if poly_tokens:
                    if poly_direction == "down":
                        poly_token_id = poly_tokens[1]  # down token
                    else:
                        poly_token_id = poly_tokens[0]  # up token
                else:
                    poly_token_id = None

                # Fetch FRESH orderbook to get actual ask prices
                poly_asks = []
                if poly_token_id:
                    poly_asks = fetch_poly_orderbook(poly_token_id)
                    if poly_asks:
                        print(f"  [POLY] Fresh orderbook: {len(poly_asks)} ask levels")
                        # Show top 3 levels
                        for i, (price, size) in enumerate(poly_asks[:3]):
                            print(f"         Level {i+1}: ${price:.3f} x {size:.1f}")
                    else:
                        print(f"  [POLY] No orderbook data, using WebSocket price")

                # Fallback to WebSocket price if no orderbook
                if not poly_asks:
                    poly_price_update = ws_fetcher.ws_manager.get_polymarket_price(crypto)
                    if poly_price_update:
                        if poly_direction == "down":
                            fallback_price = poly_price_update.no_ask or poly_price_update.no_price or p_price
                        else:
                            fallback_price = poly_price_update.yes_ask or poly_price_update.yes_price or p_price
                    else:
                        fallback_price = p_price
                    # Create fake asks from fallback - LIMITED to 3 levels max
                    poly_asks = [(fallback_price + i * 0.01, 100) for i in range(3)]

                poly_filled = False
                poly_fill_price = None
                poly_total_filled = 0
                poly_total_cost = 0.0
                poly_fills = []  # Track fills at each level: [(price, qty), ...]

                # Check FIRST if best Poly price gives at least -3% edge (including fees)
                # If not, skip directly to unwind - don't waste time trying
                if poly_asks:
                    best_poly_ask = poly_asks[0][0]
                    best_k_fee = calc_kalshi_fee(actual_k_cost, 1)
                    best_p_fee = calc_poly_fee(best_poly_ask, 1)
                    best_total_cost = actual_k_cost + best_poly_ask + best_k_fee + best_p_fee + GAS_FEE_DOLLARS
                    best_edge = (1.0 - best_total_cost) / best_total_cost * 100 if best_total_cost > 0 else -100

                    if best_edge < MIN_ACCEPTABLE_EDGE:
                        print(f"  [POLY] Best ask ${best_poly_ask:.3f} gives {best_edge:.1f}% edge (w/ fees) - BELOW {MIN_ACCEPTABLE_EDGE:.0f}% minimum")
                        print(f"  [POLY] Cannot fill within {MAX_LOSS_PCT*100:.0f}% loss limit - UNWIND IMMEDIATELY")
                        poly_asks = []  # Clear asks to skip

                # Fill Polymarket - NO BUFFER, get cheapest price possible
                remaining_to_fill = actual_filled_qty
                max_poly_attempts = 5
                poly_attempt = 0

                while remaining_to_fill > 0 and poly_attempt < max_poly_attempts and poly_asks:
                    poly_attempt += 1

                    # Refresh orderbook on every retry to get current best price
                    if poly_attempt > 1 and poly_token_id:
                        poly_asks = fetch_poly_orderbook(poly_token_id)
                        if poly_asks:
                            print(f"  [POLY] Retry orderbook: {len(poly_asks)} levels, best=${poly_asks[0][0]:.3f}")
                        else:
                            print(f"  [POLY] Retry orderbook failed - no data")
                            break

                    if not poly_asks:
                        break

                    best_ask_price = poly_asks[0][0]
                    # NO BUFFER - order at exact best ask price
                    order_price = best_ask_price

                    # Check edge threshold (including fees for accurate stop-loss)
                    retry_k_fee = calc_kalshi_fee(actual_k_cost, 1)
                    retry_p_fee = calc_poly_fee(order_price, 1)
                    potential_total = actual_k_cost + order_price + retry_k_fee + retry_p_fee + GAS_FEE_DOLLARS
                    potential_edge = (1.0 - potential_total) / potential_total * 100 if potential_total > 0 else -100
                    if potential_edge < MIN_ACCEPTABLE_EDGE:
                        print(f"  [POLY] ${order_price:.2f} = {potential_edge:.1f}% edge (w/ fees) - stopping (limit: {MIN_ACCEPTABLE_EDGE:.0f}%)")
                        break

                    if poly_attempt == 1:
                        print(f"  [POLY] {remaining_to_fill}x @ ${order_price:.3f}...")
                    else:
                        print(f"  [POLY] Retry {poly_attempt}: {remaining_to_fill}x @ ${order_price:.3f}...")

                    try:
                        poly_result = ws_fetcher.ws_manager.place_poly_order(
                            crypto, poly_direction, remaining_to_fill, order_price
                        )

                        if poly_result.success:
                            filled_qty = poly_result.filled_qty if hasattr(poly_result, 'filled_qty') and poly_result.filled_qty else remaining_to_fill
                            fill_price = best_ask_price

                            poly_fills.append((fill_price, filled_qty))
                            poly_total_filled += filled_qty
                            poly_total_cost += fill_price * filled_qty
                            remaining_to_fill -= filled_qty

                            if remaining_to_fill > 0:
                                print(f"  [POLY] ✓ {filled_qty}x @ ${fill_price:.3f}, {remaining_to_fill} remaining...")
                            else:
                                print(f"  [POLY] ✓ Filled {filled_qty}x @ ${fill_price:.3f}")
                        else:
                            error_short = str(poly_result.error)[:30] if poly_result.error else "no fill"
                            print(f"  [POLY] ✗ {error_short}")
                    except Exception as poly_ex:
                        print(f"  [POLY] ✗ {str(poly_ex)[:30]}")

                if remaining_to_fill > 0 and poly_total_filled > 0:
                    print(f"  [POLY] ⚠️ {poly_total_filled}/{actual_filled_qty} filled after {poly_attempt} tries")

                # Check if we got any fills
                if poly_total_filled > 0:
                    poly_filled = True
                    poly_fill_price = poly_total_cost / poly_total_filled  # Weighted average

                    # Log fills across levels
                    if len(poly_fills) > 1:
                        print(f"  [POLY] Filled across {len(poly_fills)} levels: {poly_fills}")
                    print(f"  [POLY] Total: {poly_total_filled}x @ avg ${poly_fill_price:.3f}")

                    # If partial fill, update qty for profit calculation
                    if poly_total_filled < actual_filled_qty:
                        print(f"  [POLY] Partial fill: {poly_total_filled}/{actual_filled_qty}")
                        actual_filled_qty = poly_total_filled

                if poly_filled:
                    # Calculate actual edge after both fills WITH FEES
                    actual_total_cost = actual_k_cost + poly_fill_price

                    # Calculate fees on actual fill prices
                    actual_kalshi_fee = calc_kalshi_fee(actual_k_cost, actual_filled_qty, is_maker=False)
                    actual_poly_fee = calc_poly_fee(poly_fill_price, actual_filled_qty)
                    actual_gas = GAS_FEE_DOLLARS
                    total_fees = actual_kalshi_fee + actual_poly_fee + actual_gas

                    # Gross edge (before fees)
                    gross_edge = (1.0 - actual_total_cost) / actual_total_cost * 100

                    # Net edge (after fees) - this is what we actually make
                    total_cost_with_fees = (actual_total_cost * actual_filled_qty) + total_fees
                    payout = 1.0 * actual_filled_qty
                    profit = payout - total_cost_with_fees
                    actual_edge = profit / total_cost_with_fees * 100  # Net edge after fees

                    # Print detailed trade summary
                    trade_time = now_mst()
                    result_lines = []
                    result_lines.append(f"  ╔═══════════════════════════════════════════════════════╗")
                    result_lines.append(f"  ║  ✓ TRADE COMPLETE                                     ║")
                    result_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                    result_lines.append(f"  ║  Time:     {trade_time}")
                    result_lines.append(f"  ║  Crypto:   {crypto}")
                    result_lines.append(f"  ║  Strategy: {best_dir}")
                    result_lines.append(f"  ║  Qty:      {actual_filled_qty} contracts")
                    result_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                    result_lines.append(f"  ║  PRICES SEEN:")
                    result_lines.append(f"  ║    Kalshi:  YES=${snap.k_up:.2f}  NO=${snap.k_down:.2f}")
                    result_lines.append(f"  ║    Poly:    UP=${snap.p_up:.2f}   DOWN=${snap.p_down:.2f}")
                    result_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                    result_lines.append(f"  ║  FILL PRICES:")
                    result_lines.append(f"  ║    Kalshi:  ${actual_k_cost:.2f}")
                    result_lines.append(f"  ║    Poly:    ${poly_fill_price:.2f}")
                    result_lines.append(f"  ║    Total:   ${actual_total_cost:.2f} x {actual_filled_qty} = ${actual_total_cost * actual_filled_qty:.2f}")
                    result_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                    result_lines.append(f"  ║  FEES:")
                    result_lines.append(f"  ║    Kalshi:  ${actual_kalshi_fee:.2f}")
                    result_lines.append(f"  ║    Poly:    ${actual_poly_fee:.2f}")
                    result_lines.append(f"  ║    Gas:     ${actual_gas:.2f}")
                    result_lines.append(f"  ║    Total:   ${total_fees:.2f}")
                    result_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                    result_lines.append(f"  ║  EDGE:")
                    result_lines.append(f"  ║    Expected (scan): {raw_edge:.1f}%")
                    result_lines.append(f"  ║    Actual (fills+fees): {actual_edge:.1f}%")
                    result_lines.append(f"  ╠═══════════════════════════════════════════════════════╣")
                    result_lines.append(f"  ║  PROFIT: ${profit:.2f} ({actual_edge:.1f}%)")
                    result_lines.append(f"  ║  Unwound: NO")
                    result_lines.append(f"  ╚═══════════════════════════════════════════════════════╝")
                    print(f"")
                    for line in result_lines:
                        print(line)
                    print(f"")
                    log_highlight("\n".join(result_lines) + "\n\n")

                    # Compute fill totals (used by window_trades and CSV log)
                    kalshi_fill_total = actual_k_cost * actual_filled_qty
                    poly_fill_total = poly_fill_price * actual_filled_qty

                    # Record trade for window summary notification
                    window_trades.append({
                        "crypto": crypto,
                        "market_type": "15m",
                        "direction": best_dir,
                        "qty": actual_filled_qty,
                        "kalshi_side": kalshi_side,
                        "kalshi_fill_price": actual_k_cost,
                        "poly_fill_price": poly_fill_price,
                        "total_cost": kalshi_fill_total + poly_fill_total,
                        "fees": total_fees,
                        "kalshi_fee": actual_kalshi_fee,
                        "poly_fee": actual_poly_fee,
                        "kalshi_strike": snap.kalshi_strike,
                        "poly_strike": snap.poly_strike,
                        "was_unwound": False,
                        "unwind_loss": 0.0,
                        "status": "SUCCESS",
                    })

                    trader.stats.successful_trades += 1

                    # Reset consecutive unwind counter on success
                    consecutive_unwinds[crypto] = 0

                    # Count trades and track edge for this crypto
                    traded_cryptos[crypto] = traded_cryptos.get(crypto, 0) + 1
                    trade_num = traded_cryptos[crypto]
                    if crypto not in crypto_edges:
                        crypto_edges[crypto] = []
                    crypto_edges[crypto].append(actual_edge)

                    # Track per-crypto contracts this window
                    window_contracts_traded[crypto] = window_contracts_traded.get(crypto, 0) + actual_filled_qty
                    if MAX_CONTRACTS_PER_WINDOW_ENABLED:
                        print(f"  [WINDOW CONTRACTS] {crypto}: {window_contracts_traded[crypto]}/{MAX_CONTRACTS_PER_WINDOW} contracts traded this window")

                    if is_safe_bet:
                        # Safe bet mode (penny/cross-strike): trade forever, no limit
                        print(f"  [SAFE BET] {crypto} trade #{trade_num} - edge {actual_edge:.1f}% - {safe_bet_reason} (no trade limit)")
                    elif not use_strike_thresholds:
                        # No-threshold mode: 1 trade per crypto, then stop
                        stopped_cryptos.add(crypto)
                        print(f"  [DONE] {crypto} trade #{trade_num} - edge {actual_edge:.1f}% - STOPPING (1 trade per crypto)")
                    else:
                        # Threshold mode: max 6 trades, after 3 check avg edge >= 4.5%
                        if trade_num >= MAX_TRADES_PER_CRYPTO:
                            stopped_cryptos.add(crypto)
                            avg_edge = sum(crypto_edges[crypto]) / len(crypto_edges[crypto])
                            print(f"  [DONE] {crypto} trade #{trade_num} - reached max {MAX_TRADES_PER_CRYPTO} trades (avg edge {avg_edge:.1f}%) - STOPPING this crypto for this window")
                        elif trade_num >= MIN_TRADES_BEFORE_CHECK:
                            avg_edge = sum(crypto_edges[crypto]) / len(crypto_edges[crypto])
                            if avg_edge < MIN_AVG_EDGE_TO_CONTINUE:
                                stopped_cryptos.add(crypto)
                                print(f"  [DONE] {crypto} trade #{trade_num} - avg edge {avg_edge:.1f}% < {MIN_AVG_EDGE_TO_CONTINUE}% - STOPPING this crypto")
                            else:
                                print(f"  [DONE] {crypto} trade #{trade_num} - avg edge {avg_edge:.1f}% >= {MIN_AVG_EDGE_TO_CONTINUE}% - continuing at 6% edge (max {MAX_TRADES_PER_CRYPTO} trades)")
                        else:
                            print(f"  [DONE] {crypto} trade #{trade_num}/{MIN_TRADES_BEFORE_CHECK} - edge {actual_edge:.1f}% - {MIN_TRADES_BEFORE_CHECK - trade_num} more guaranteed")

                    # Log trade to CSV
                    poly_side_label = "DOWN" if best_dir == "K_UP+P_DOWN" else "UP"
                    total_cost_for_log = kalshi_fill_total + poly_fill_total
                    total_bet = total_cost_for_log + total_fees
                    log_trade_row({
                        "timestamp": trade_time,
                        "crypto": crypto,
                        "direction": best_dir,
                        "qty": actual_filled_qty,
                        "kalshi_side": kalshi_side.upper(),
                        "kalshi_fill": actual_k_cost,
                        "poly_side": poly_side_label,
                        "poly_fill": poly_fill_price,
                        "kalshi_fill_total": kalshi_fill_total,
                        "poly_fill_total": poly_fill_total,
                        "total_cost": total_cost_for_log,
                        "kalshi_fee": actual_kalshi_fee,
                        "poly_fee": actual_poly_fee,
                        "total_fees": total_fees,
                        "total_bet": total_bet,
                        "edge_expected_pct": raw_edge,
                        "edge_actual_pct": actual_edge,
                        "was_unwound": "NO",
                        "unwind_loss": "",
                        "kalshi_strike": snap.kalshi_strike,
                        "poly_strike": snap.poly_strike,
                        "kalshi_end_price": "",
                        "poly_end_price": "",
                        "kalshi_result": "",
                        "poly_result": "",
                        "pnl": "",
                        "status": "SUCCESS",
                    })

                    # Mark this crypto as traded in historical data
                    historical.mark_traded(crypto)

                    # Also log to potential trades with traded=completed
                    # Get depth from cached orderbooks
                    log_k_yes_depth = kalshi_yes_depth if kalshi_yes_depth > 0 else 0
                    log_k_no_depth = kalshi_no_depth if kalshi_no_depth > 0 else 0
                    log_p_up_depth = int(snap.poly_up_asks[0][1]) if snap.poly_up_asks else 0
                    log_p_down_depth = int(snap.poly_down_asks[0][1]) if snap.poly_down_asks else 0
                    k_yes_log = f"{snap.k_up:.4f}({log_k_yes_depth})" if snap.k_up else "N/A"
                    k_no_log = f"{snap.k_down:.4f}({log_k_no_depth})" if snap.k_down else "N/A"
                    p_up_log = f"{snap.p_up:.4f}({log_p_up_depth})" if snap.p_up else "N/A"
                    p_down_log = f"{snap.p_down:.4f}({log_p_down_depth})" if snap.p_down else "N/A"
                    log_potential_trade({
                        "timestamp": trade_time,
                        "crypto": crypto,
                        "direction": best_dir,
                        "qty": actual_filled_qty,
                        "k_yes_price": k_yes_log,
                        "k_no_price": k_no_log,
                        "p_up_price": p_up_log,
                        "p_down_price": p_down_log,
                        "raw_edge_pct": raw_edge,
                        "traded": "completed",
                    })

                    # Show remaining cryptos (those not stopped)
                    remaining = []
                    for c in cryptos:
                        if c not in stopped_cryptos:
                            trades = traded_cryptos.get(c, 0)
                            edges = crypto_edges.get(c, [])
                            avg = sum(edges) / len(edges) if edges else 0
                            remaining.append(f"{c}({trades} trades, avg {avg:.1f}%)")
                    if remaining:
                        print(f"  [ACTIVE] Can still trade: {', '.join(remaining)}")
                    else:
                        print(f"  [COMPLETE] All cryptos stopped (avg edge < {MIN_AVG_EDGE_TO_CONTINUE}% after {MIN_TRADES_BEFORE_CHECK}+ trades)")

                    # No-threshold mode: per-crypto limit handled by MAX_TRADES_PER_CRYPTO (3)
                    # Bot continues running and resets each 15-min window

                else:
                    # Polymarket failed even after retries - unwind Kalshi position
                    print(f"  [UNWIND] Could not fill Polymarket - selling Kalshi {kalshi_side.upper()} position...")

                    unwind_success = False
                    unwind_sell_price = None
                    unwind_loss = 0.0
                    max_unwind_attempts = 5  # More attempts
                    remaining_to_sell = actual_filled_qty
                    total_sold = 0
                    total_sell_proceeds = 0.0

                    for unwind_attempt in range(max_unwind_attempts):
                        if remaining_to_sell <= 0:
                            unwind_success = True
                            break

                        print(f"  [UNWIND] Attempt {unwind_attempt + 1}/{max_unwind_attempts} - Selling {remaining_to_sell}x remaining...")

                        # Fetch FRESH Kalshi orderbook to get actual best bid
                        fresh_ob = ws_fetcher._kalshi_client.get_orderbook(kalshi_ticker)
                        sell_price_cents = None

                        if fresh_ob:
                            orderbook = fresh_ob.get("orderbook", {}) or {}
                            # Get bids for the side we're selling
                            if kalshi_side == "yes":
                                bids = orderbook.get("yes", []) or []
                                if bids:
                                    best_bid = max(b[0] for b in bids)
                                    sell_price_cents = best_bid
                                    print(f"  [UNWIND] Fresh orderbook: best YES bid = {best_bid}c")
                            else:
                                bids = orderbook.get("no", []) or []
                                if bids:
                                    best_bid = max(b[0] for b in bids)
                                    sell_price_cents = best_bid
                                    print(f"  [UNWIND] Fresh orderbook: best NO bid = {best_bid}c")

                        # Fallback to WebSocket price if orderbook failed
                        if sell_price_cents is None:
                            kalshi_unwind_price = ws_fetcher.ws_manager.get_kalshi_price(crypto)
                            if kalshi_unwind_price:
                                if kalshi_side == "yes":
                                    sell_price_cents = int((kalshi_unwind_price.yes_price or 0.01) * 100) - 1
                                else:
                                    sell_price_cents = int((kalshi_unwind_price.no_price or 0.01) * 100) - 1
                                print(f"  [UNWIND] Using WebSocket fallback price: {sell_price_cents}c")

                        if sell_price_cents and sell_price_cents > 0:
                            sell_price_cents = max(1, sell_price_cents)
                            print(f"  [UNWIND] SELL {remaining_to_sell}x {kalshi_side.upper()} @ {sell_price_cents}c...")

                            # Try to sell at best bid
                            unwind_result = ws_fetcher._kalshi_client.sell_position(
                                kalshi_ticker, kalshi_side, remaining_to_sell, sell_price_cents,
                                gtc=False
                            )

                            if unwind_result:
                                unwind_order = unwind_result.get("order", {})
                                unwind_status = unwind_order.get("status", "").lower()
                                unwind_filled = unwind_order.get("filled_count", 0)
                                unwind_order_id = unwind_order.get("order_id")

                                # Check for fills (partial or complete)
                                if unwind_filled > 0 or unwind_status in ["executed", "filled"]:
                                    filled_this_attempt = unwind_filled if unwind_filled > 0 else remaining_to_sell
                                    fill_price = sell_price_cents / 100.0
                                    total_sold += filled_this_attempt
                                    total_sell_proceeds += fill_price * filled_this_attempt
                                    remaining_to_sell -= filled_this_attempt
                                    print(f"  [UNWIND] Filled {filled_this_attempt}x @ {sell_price_cents}c (total sold: {total_sold}/{actual_filled_qty})")

                                    if remaining_to_sell <= 0:
                                        unwind_success = True
                                        print(f"  [UNWIND] SUCCESS - All {total_sold}x sold!")
                                    elif unwind_attempt < max_unwind_attempts - 1:
                                        print(f"  [UNWIND] Partial fill - {remaining_to_sell}x remaining, retrying...")
                                        time.sleep(0.3)
                                elif unwind_status == "resting":
                                    # Order didn't fill - cancel and retry
                                    if unwind_order_id:
                                        print(f"  [UNWIND] Order resting, cancelling...")
                                        ws_fetcher._kalshi_client.cancel_order(unwind_order_id)
                                    if unwind_attempt < max_unwind_attempts - 1:
                                        print(f"  [UNWIND] Didn't fill at {sell_price_cents}c, will retry...")
                                        time.sleep(0.5)
                                else:
                                    print(f"  [UNWIND] Order status: {unwind_status}")
                                    if unwind_attempt < max_unwind_attempts - 1:
                                        time.sleep(0.5)
                        else:
                            print(f"  [UNWIND] Could not determine sell price")
                            if unwind_attempt < max_unwind_attempts - 1:
                                time.sleep(0.5)

                    # Calculate final unwind stats
                    if total_sold > 0:
                        unwind_sell_price = total_sell_proceeds / total_sold  # Weighted average
                        buy_cost = actual_k_cost * actual_filled_qty
                        unwind_loss = buy_cost - total_sell_proceeds
                        if remaining_to_sell > 0:
                            print(f"  [UNWIND] Partial success: sold {total_sold}/{actual_filled_qty}, still holding {remaining_to_sell}x")

                    # Print detailed unwind summary
                    trade_time = now_mst()
                    print(f"")
                    print(f"  ╔═══════════════════════════════════════════════════════╗")
                    print(f"  ║  ✗ TRADE UNWOUND                                      ║")
                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                    print(f"  ║  Time:     {trade_time}")
                    print(f"  ║  Crypto:   {crypto}")
                    print(f"  ║  Strategy: {best_dir}")
                    print(f"  ║  Qty:      {actual_filled_qty} contracts (sold: {total_sold})")
                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                    print(f"  ║  PRICES SEEN:")
                    print(f"  ║    Kalshi:  YES=${snap.k_up:.2f}  NO=${snap.k_down:.2f}")
                    print(f"  ║    Poly:    UP=${snap.p_up:.2f}   DOWN=${snap.p_down:.2f}")
                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                    print(f"  ║  KALSHI FILL:")
                    print(f"  ║    Bought @ ${actual_k_cost:.2f}")
                    if total_sold > 0:
                        print(f"  ║    Sold   @ ${unwind_sell_price:.2f} (avg)")
                    else:
                        print(f"  ║    Sold   @ FAILED (still holding!)")
                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                    print(f"  ║  EDGE:")
                    print(f"  ║    Raw (before):  {raw_edge:.1f}%")
                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                    if total_sold > 0:
                        # unwind_loss can be negative (profit) if sold higher than bought
                        if unwind_loss > 0:
                            print(f"  ║  LOSS: -${unwind_loss:.2f}")
                        else:
                            print(f"  ║  PROFIT: +${-unwind_loss:.2f}")
                    else:
                        print(f"  ║  P&L: UNKNOWN (position still held)")
                    if remaining_to_sell > 0:
                        print(f"  ║  Still holding: {remaining_to_sell}x")
                    print(f"  ║  Unwound: {'YES' if unwind_success else 'PARTIAL'}")
                    print(f"  ╚═══════════════════════════════════════════════════════╝")
                    print(f"")

                    # Record unwind for window summary notification
                    window_trades.append({
                        "crypto": crypto,
                        "market_type": "15m",
                        "direction": best_dir,
                        "qty": actual_filled_qty,
                        "kalshi_side": kalshi_side,
                        "kalshi_fill_price": actual_k_cost,
                        "poly_fill_price": 0,
                        "total_cost": actual_k_cost * actual_filled_qty,
                        "fees": 0,
                        "kalshi_fee": 0,
                        "poly_fee": 0,
                        "kalshi_strike": snap.kalshi_strike,
                        "poly_strike": snap.poly_strike,
                        "was_unwound": True,
                        "unwind_loss": unwind_loss if total_sold > 0 else actual_k_cost * actual_filled_qty,
                        "status": "UNWIND" if total_sold > 0 else "UNWIND_FAILED",
                    })

                    if remaining_to_sell > 0:
                        print(f"  [WARN] Still holding {remaining_to_sell}x Kalshi {kalshi_side.upper()} position!")

                    # Log unwind to CSV
                    poly_side_label = "DOWN" if best_dir == "K_UP+P_DOWN" else "UP"
                    kalshi_fill_total_uw = actual_k_cost * actual_filled_qty
                    log_trade_row({
                        "timestamp": trade_time,
                        "crypto": crypto,
                        "direction": best_dir,
                        "qty": actual_filled_qty,
                        "kalshi_side": kalshi_side.upper(),
                        "kalshi_fill": actual_k_cost,
                        "poly_side": poly_side_label,
                        "poly_fill": "",
                        "kalshi_fill_total": kalshi_fill_total_uw,
                        "poly_fill_total": "",
                        "total_cost": kalshi_fill_total_uw,
                        "kalshi_fee": "",
                        "poly_fee": "",
                        "total_fees": "",
                        "total_bet": kalshi_fill_total_uw,
                        "edge_expected_pct": raw_edge,
                        "edge_actual_pct": "",
                        "was_unwound": "YES",
                        "unwind_loss": unwind_loss if unwind_success else "UNKNOWN",
                        "kalshi_strike": snap.kalshi_strike,
                        "poly_strike": snap.poly_strike,
                        "kalshi_end_price": "",
                        "poly_end_price": "",
                        "kalshi_result": "",
                        "poly_result": "",
                        "pnl": -unwind_loss if unwind_success else "",
                        "status": "UNWIND" if unwind_success else "UNWIND_FAILED",
                    })

                    # Track unwind loss in session stats
                    if unwind_success and unwind_loss > 0:
                        trader.stats.unwind_losses += unwind_loss

                    # Mark this crypto as traded in historical data (even for unwound trades)
                    historical.mark_traded(crypto)

                    # Also log to potential trades with traded=unwind (Kalshi filled but Poly failed)
                    # Get depth from cached orderbooks
                    log_k_yes_depth = kalshi_yes_depth if kalshi_yes_depth > 0 else 0
                    log_k_no_depth = kalshi_no_depth if kalshi_no_depth > 0 else 0
                    log_p_up_depth = int(snap.poly_up_asks[0][1]) if snap.poly_up_asks else 0
                    log_p_down_depth = int(snap.poly_down_asks[0][1]) if snap.poly_down_asks else 0
                    k_yes_log = f"{snap.k_up:.4f}({log_k_yes_depth})" if snap.k_up else "N/A"
                    k_no_log = f"{snap.k_down:.4f}({log_k_no_depth})" if snap.k_down else "N/A"
                    p_up_log = f"{snap.p_up:.4f}({log_p_up_depth})" if snap.p_up else "N/A"
                    p_down_log = f"{snap.p_down:.4f}({log_p_down_depth})" if snap.p_down else "N/A"
                    log_potential_trade({
                        "timestamp": trade_time,
                        "crypto": crypto,
                        "direction": best_dir,
                        "qty": actual_filled_qty,
                        "k_yes_price": k_yes_log,
                        "k_no_price": k_no_log,
                        "p_up_price": p_up_log,
                        "p_down_price": p_down_log,
                        "raw_edge_pct": raw_edge,
                        "traded": "unwind",
                    })

                    # Track consecutive unwinds for this crypto
                    consecutive_unwinds[crypto] = consecutive_unwinds.get(crypto, 0) + 1
                    unwind_count = consecutive_unwinds[crypto]

                    # Unwinds count toward the trade-per-crypto limit (both modes)
                    traded_cryptos[crypto] = traded_cryptos.get(crypto, 0) + 1
                    trade_num = traded_cryptos[crypto]
                    print(f"  [TRADE COUNT] {crypto}: {trade_num}/{MAX_TRADES_PER_CRYPTO} (unwind counted)")
                    if trade_num >= MAX_TRADES_PER_CRYPTO:
                        stopped_cryptos.add(crypto)
                        print(f"  [DONE] {crypto} reached max {MAX_TRADES_PER_CRYPTO} trades (incl. unwinds) - STOPPING this crypto for this window")

                    print(f"  [UNWIND COUNT] {crypto}: {unwind_count} consecutive unwinds")

                # NOTE: Removed trader.log_opportunity() - was causing duplicate LOGGED rows

            # ═══════════════════════════════════════════════════════════════
            # 1-HOUR MARKET SCANS (optimized: batch fetch, parallel, dedup)
            # Combination 2: K_1h + P_1h (anytime during the hour)
            # Combination 3: K_1h + P_15m (final 15 min of hour only)
            # Combination 4: K_15m + P_1h (final 15 min of hour only)
            # ═══════════════════════════════════════════════════════════════
            if ENABLE_1H_TRADING and one_hour_subscribed and no_losses_mode:
                now_utc_1h = datetime.now(timezone.utc)
                in_final_15m = (now_utc_1h.minute >= 45)
                MAX_TRADES_PER_CRYPTO_1H = 3
                trade_executed_1h = False

                for crypto in cryptos:
                    if trade_executed_1h:
                        break
                    if crypto in stopped_cryptos_1h:
                        continue

                    # Get 1h market data from caches
                    k1h_data = ws_fetcher._ticker_cache_1h.get(crypto)
                    p1h_data = ws_fetcher._poly_token_cache_1h.get(crypto)

                    k1h_ticker = k1h_data[0] if k1h_data else None
                    k1h_strike = ws_fetcher._kalshi_strikes_1h.get(crypto)
                    p1h_strike = ws_fetcher._poly_strikes_1h.get(crypto)
                    p1h_up_token = p1h_data[0] if p1h_data else None
                    p1h_down_token = p1h_data[1] if p1h_data else None

                    # Skip if no data at all for this crypto
                    if not k1h_data and not p1h_data:
                        continue

                    # ─── BATCH FETCH: All prices once per crypto ───
                    # K1h prices (reused by Combo 2 and 3)
                    k1h_yes, k1h_no, k1h_yes_depth, k1h_no_depth = None, None, 0, 0
                    if k1h_ticker:
                        k1h_r = fetch_fresh_kalshi_prices(ws_fetcher._kalshi_client, k1h_ticker, debug=False)
                        k1h_yes, k1h_no, k1h_yes_depth, k1h_no_depth = k1h_r[0], k1h_r[1], k1h_r[2], k1h_r[3]

                    # P1h prices (reused by Combo 2 and 4) — parallel UP+DOWN fetch
                    p1h_up_ask, p1h_down_ask = None, None
                    p1h_up_depth, p1h_down_depth = 0, 0
                    if p1h_up_token and p1h_down_token:
                        p1h_prices = ws_fetcher.fetch_1h_poly_prices(p1h_up_token, p1h_down_token)
                        if p1h_prices:
                            p1h_up_ask = p1h_prices.get("up_ask")
                            p1h_down_ask = p1h_prices.get("down_ask")
                            p1h_up_depth = p1h_prices.get("up_depth", 0)
                            p1h_down_depth = p1h_prices.get("down_depth", 0)

                    # Price dedup — skip if K1h + P1h unchanged (only outside final 15m;
                    # in final 15m, cross-timeframe combos use K15m/P15m which change independently)
                    if not in_final_15m:
                        k1h_k = f"{k1h_yes:.3f}_{k1h_no:.3f}" if k1h_yes is not None and k1h_no is not None else "N"
                        p1h_k = f"{p1h_up_ask:.3f}_{p1h_down_ask:.3f}" if p1h_up_ask is not None and p1h_down_ask is not None else "N"
                        price_key_1h = f"{crypto}_{k1h_k}_{p1h_k}"
                        if price_key_1h == last_prices_1h.get(crypto):
                            if scan_num % 10 == 0:
                                print(f"  [{crypto}/1h] No price change")
                            continue
                        last_prices_1h[crypto] = price_key_1h

                    # ─── COMBINATION 2: K_1h + P_1h (same-timeframe, anytime) ───
                    if (k1h_yes is not None and k1h_no is not None
                            and p1h_up_ask is not None and p1h_down_ask is not None):

                        # Calculate edge for K_1h + P_1h (same logic as 15-min)
                        cost_k_up_p_down_1h = k1h_yes + p1h_down_ask
                        cost_k_down_p_up_1h = k1h_no + p1h_up_ask
                        edge_k_up_p_down_1h = ((1.0 - cost_k_up_p_down_1h) / cost_k_up_p_down_1h * 100) if cost_k_up_p_down_1h > 0 else -100
                        edge_k_down_p_up_1h = ((1.0 - cost_k_down_p_up_1h) / cost_k_down_p_up_1h * 100) if cost_k_down_p_up_1h > 0 else -100

                        if edge_k_up_p_down_1h >= edge_k_down_p_up_1h:
                            best_dir_1h = "K_UP+P_DOWN"
                            raw_edge_1h = edge_k_up_p_down_1h
                            k_price_1h = k1h_yes
                            p_price_1h = p1h_down_ask
                            kalshi_side_1h = "yes"
                            poly_token_1h = p1h_down_token
                            p1h_depth = p1h_down_depth
                        else:
                            best_dir_1h = "K_DOWN+P_UP"
                            raw_edge_1h = edge_k_down_p_up_1h
                            k_price_1h = k1h_no
                            p_price_1h = p1h_up_ask
                            kalshi_side_1h = "no"
                            poly_token_1h = p1h_up_token
                            p1h_depth = p1h_up_depth

                        # Detect safe bet (cross-strike arb between K_1h and P_1h)
                        is_safe_bet_1h = False
                        safe_reason_1h = ""
                        if k1h_strike and p1h_strike:
                            strike_diff_1h = abs(k1h_strike - p1h_strike)
                            if crypto in ("BTC", "ETH") and strike_diff_1h <= 0.01:
                                is_safe_bet_1h = True
                                safe_reason_1h = "penny strike"
                            elif strike_diff_1h > 0:
                                skip_1h, bin_px_1h, cl_px_1h = should_skip_cross_strike(
                                    k1h_strike, p1h_strike, crypto, ws_fetcher
                                )
                                if skip_1h:
                                    if scan_num % 20 == 0:
                                        print(f"    [1h CROSS-SKIP] Price feeds diverge: Binance=${bin_px_1h:,.2f} Chainlink=${cl_px_1h:,.2f} K{fmt_strike(k1h_strike)} P{fmt_strike(p1h_strike)}")
                                else:
                                    is_safe_bet_1h = True
                                    px_info_1h = ""
                                    if bin_px_1h and cl_px_1h:
                                        px_info_1h = f" Bin=${bin_px_1h:,.0f} CL=${cl_px_1h:,.0f}"
                                    safe_reason_1h = f"cross-strike (K{fmt_strike(k1h_strike)} P{fmt_strike(p1h_strike)}){px_info_1h}"

                        # Log scan box
                        k1h_yes_str = f"{k1h_yes:.2f}" if k1h_yes else "N/A"
                        k1h_no_str = f"{k1h_no:.2f}" if k1h_no else "N/A"
                        k1h_yes_d = f"({k1h_yes_depth})" if k1h_yes_depth > 0 else ""
                        k1h_no_d = f"({k1h_no_depth})" if k1h_no_depth > 0 else ""
                        p1h_up_str = f"{p1h_up_ask:.2f}" if p1h_up_ask else "N/A"
                        p1h_down_str = f"{p1h_down_ask:.2f}" if p1h_down_ask else "N/A"
                        p1h_up_d = f"({p1h_up_depth})" if p1h_up_depth > 0 else ""
                        p1h_down_d = f"({p1h_down_depth})" if p1h_down_depth > 0 else ""
                        safe_label_1h = f" ★SAFE({safe_reason_1h})" if is_safe_bet_1h else ""

                        scan_box_1h = []
                        scan_box_1h.append(f"  ┌─ {crypto}/1h ─────────────────────────────────")
                        scan_box_1h.append(f"  │ KALSHI 1h:  YES={k1h_yes_str}{k1h_yes_d}  NO={k1h_no_str}{k1h_no_d} (fresh)")
                        scan_box_1h.append(f"  │ POLY 1h:    UP={p1h_up_str}{p1h_up_d}   DOWN={p1h_down_str}{p1h_down_d} (fresh)")
                        if k1h_strike and p1h_strike:
                            scan_box_1h.append(f"  │ STRIKES: K1h={fmt_strike(k1h_strike)} vs P1h={fmt_strike(p1h_strike)}")
                        scan_box_1h.append(f"  │ EDGE: {raw_edge_1h:.1f}% via {best_dir_1h}{safe_label_1h}")
                        scan_box_1h.append(f"  └──────────────────────────────────────────")
                        for line in scan_box_1h:
                            print(line)

                        # Only trade safe bets in NO LOSSES mode
                        SAFE_BET_MIN_EDGE_1H = 1.5
                        if not is_safe_bet_1h:
                            if raw_edge_1h >= 2.0:
                                print(f"    → Not a safe bet - skipping (NO LOSSES mode)")
                        elif (raw_edge_1h >= SAFE_BET_MIN_EDGE_1H
                                and k_price_1h and p_price_1h
                                and 0.05 <= k_price_1h <= 0.88
                                and 0.02 <= p_price_1h <= 0.88
                                and p1h_depth >= 15):

                            qty_1h = min(int(p1h_depth * 0.25), 10)
                            qty_1h = max(qty_1h, 1)

                            print(f"\n  ╔═══════════════════════════════════════════════════════╗")
                            print(f"  ║  EXECUTING 1H TRADE: {crypto}/1h                       ║")
                            print(f"  ╠═══════════════════════════════════════════════════════╣")
                            print(f"  ║  Strategy: {best_dir_1h} ({safe_reason_1h})")
                            print(f"  ║  Edge: {raw_edge_1h:.1f}%")
                            if k1h_strike and p1h_strike:
                                print(f"  ║  K1h strike: {fmt_strike(k1h_strike)}  P1h strike: {fmt_strike(p1h_strike)}")
                            print(f"  ║  KALSHI: {kalshi_side_1h.upper()} @ ${k_price_1h:.2f}  POLY: {'DOWN' if kalshi_side_1h == 'yes' else 'UP'} @ ${p_price_1h:.2f}")
                            print(f"  ║  Qty: {qty_1h} contracts")
                            print(f"  ╚═══════════════════════════════════════════════════════╝")

                            trade_executed_1h = True  # Stop scanning — jump into trade
                            # Place Kalshi 1h order
                            k_price_cents_1h = int(k_price_1h * 100) + 1
                            print(f"  [1H STEP 1] KALSHI 1h: {qty_1h}x {kalshi_side_1h.upper()} @ {k_price_cents_1h}c...")
                            k_result_1h = ws_fetcher._kalshi_client.place_order(
                                k1h_ticker, kalshi_side_1h, qty_1h, k_price_cents_1h
                            )

                            kalshi_filled_1h = 0
                            if k_result_1h:
                                k_order_1h = k_result_1h.get("order", {})
                                kalshi_filled_1h = k_order_1h.get("filled_count", 0)
                                k_status_1h = k_order_1h.get("status", "").lower()
                                k_id_1h = k_order_1h.get("order_id", "")
                                print(f"  [1H KALSHI] status={k_status_1h}, filled={kalshi_filled_1h}")

                                if kalshi_filled_1h == 0 and k_status_1h not in ["executed", "filled"]:
                                    if k_id_1h:
                                        ws_fetcher._kalshi_client.cancel_order(k_id_1h)
                                    print(f"  [1H KALSHI] Not filled - cancelled")
                                else:
                                    kalshi_filled_1h = kalshi_filled_1h or qty_1h

                            if kalshi_filled_1h > 0:
                                # Place Polymarket 1h order
                                print(f"  [1H STEP 2] POLY 1h: {kalshi_filled_1h}x {'DOWN' if kalshi_side_1h == 'yes' else 'UP'} @ ${p_price_1h:.2f}...")
                                p_result_1h = ws_fetcher.ws_manager.poly_ws.place_order(
                                    poly_token_1h, "BUY", kalshi_filled_1h, p_price_1h, immediate=True
                                )

                                if p_result_1h and p_result_1h.success:
                                    print(f"  [1H POLY] FILLED! {p_result_1h.filled_qty or kalshi_filled_1h}x")
                                    print(f"  [1H COMPLETE] {crypto}/1h trade executed!")

                                    # Track trade
                                    traded_cryptos_1h[crypto] = traded_cryptos_1h.get(crypto, 0) + 1
                                    trade_num_1h = traded_cryptos_1h[crypto]
                                    print(f"  [1H TRADE COUNT] {crypto}: {trade_num_1h}/{MAX_TRADES_PER_CRYPTO_1H}")
                                    if trade_num_1h >= MAX_TRADES_PER_CRYPTO_1H:
                                        stopped_cryptos_1h.add(crypto)
                                        print(f"  [1H DONE] {crypto} reached max {MAX_TRADES_PER_CRYPTO_1H} trades")

                                    # Log trade
                                    trade_time = datetime.now(timezone.utc).isoformat()
                                    actual_filled_1h = p_result_1h.filled_qty or kalshi_filled_1h
                                    # Record trade for window summary
                                    k_fee_1h = calc_kalshi_fee(k_price_1h, actual_filled_1h, is_maker=False)
                                    p_fee_1h = calc_poly_fee(p_price_1h, actual_filled_1h)
                                    fees_1h = k_fee_1h + p_fee_1h + GAS_FEE_DOLLARS
                                    poly_side_1h = "DOWN" if best_dir_1h == "K_UP+P_DOWN" else "UP"
                                    k_fill_total_1h = k_price_1h * actual_filled_1h
                                    p_fill_total_1h = p_price_1h * actual_filled_1h
                                    total_cost_1h = k_fill_total_1h + p_fill_total_1h
                                    total_bet_1h = total_cost_1h + fees_1h
                                    log_trade_row({
                                        "timestamp": trade_time,
                                        "crypto": crypto,
                                        "direction": best_dir_1h,
                                        "qty": actual_filled_1h,
                                        "kalshi_side": kalshi_side_1h.upper(),
                                        "kalshi_fill": k_price_1h,
                                        "poly_side": poly_side_1h,
                                        "poly_fill": p_price_1h,
                                        "kalshi_fill_total": k_fill_total_1h,
                                        "poly_fill_total": p_fill_total_1h,
                                        "total_cost": total_cost_1h,
                                        "kalshi_fee": k_fee_1h,
                                        "poly_fee": p_fee_1h,
                                        "total_fees": fees_1h,
                                        "total_bet": total_bet_1h,
                                        "edge_expected_pct": raw_edge_1h,
                                        "edge_actual_pct": "",
                                        "was_unwound": "NO",
                                        "unwind_loss": "",
                                        "kalshi_strike": k1h_strike,
                                        "poly_strike": p1h_strike,
                                        "kalshi_end_price": "",
                                        "poly_end_price": "",
                                        "kalshi_result": "",
                                        "poly_result": "",
                                        "pnl": "",
                                        "status": "SUCCESS",
                                    })
                                    flush_scan_buffer_to_log()
                                    window_trades.append({
                                        "crypto": crypto,
                                        "market_type": "1h",
                                        "direction": best_dir_1h,
                                        "qty": actual_filled_1h,
                                        "kalshi_side": kalshi_side_1h,
                                        "kalshi_fill_price": k_price_1h,
                                        "poly_fill_price": p_price_1h,
                                        "total_cost": (k_price_1h + p_price_1h) * actual_filled_1h,
                                        "fees": fees_1h,
                                        "kalshi_fee": k_fee_1h,
                                        "poly_fee": p_fee_1h,
                                        "kalshi_strike": k1h_strike,
                                        "poly_strike": p1h_strike,
                                        "was_unwound": False,
                                        "unwind_loss": 0.0,
                                        "status": "SUCCESS",
                                    })
                                else:
                                    print(f"  [1H POLY] FAILED - need to unwind Kalshi 1h")
                                    # Unwind Kalshi 1h position
                                    remaining_1h = kalshi_filled_1h
                                    for unwind_att in range(5):
                                        if remaining_1h <= 0:
                                            break
                                        fresh_ob_1h = ws_fetcher._kalshi_client.get_orderbook(k1h_ticker)
                                        sell_price_1h = None
                                        if fresh_ob_1h:
                                            ob_1h = fresh_ob_1h.get("orderbook", {}) or {}
                                            side_key = "yes" if kalshi_side_1h == "yes" else "no"
                                            bids_1h = ob_1h.get(side_key, []) or []
                                            if bids_1h:
                                                sell_price_1h = max(b[0] for b in bids_1h)
                                        if sell_price_1h and sell_price_1h > 0:
                                            print(f"  [1H UNWIND] SELL {remaining_1h}x {kalshi_side_1h.upper()} @ {sell_price_1h}c...")
                                            uw_result = ws_fetcher._kalshi_client.sell_position(
                                                k1h_ticker, kalshi_side_1h, remaining_1h, sell_price_1h, gtc=False
                                            )
                                            if uw_result:
                                                uw_order = uw_result.get("order", {})
                                                uw_filled = uw_order.get("filled_count", 0)
                                                if uw_filled > 0 or uw_order.get("status", "").lower() in ["executed", "filled"]:
                                                    filled_this = uw_filled if uw_filled > 0 else remaining_1h
                                                    remaining_1h -= filled_this
                                                    print(f"  [1H UNWIND] Sold {filled_this}x, remaining={remaining_1h}")
                                        time.sleep(0.3)
                                    if remaining_1h > 0:
                                        print(f"  [1H UNWIND] Incomplete - still holding {remaining_1h}x")
                                    else:
                                        print(f"  [1H UNWIND] Complete")

                    if trade_executed_1h:
                        break

                    # ─── COMBINATION 3: K_1h + P_15m (cross-timeframe, final 15 min only) ───
                    if in_final_15m and k1h_ticker and k1h_yes is not None and k1h_no is not None and k1h_strike:
                        # Use existing 15-min Poly data from the current scan
                        p15_strike = ws_fetcher._poly_strikes.get(crypto)
                        p15_tokens = ws_fetcher.ws_manager._poly_tokens.get(crypto)

                        if p15_strike and p15_tokens and k1h_strike != p15_strike:
                            p15_up_token, p15_down_token = p15_tokens[0], p15_tokens[1]

                            # Fresh fetch both sides
                            # Kalshi 1h already fetched above (k1h_yes, k1h_no)
                            # Need fresh Poly 15m prices
                            import concurrent.futures
                            try:
                                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                                    up_f = executor.submit(fetch_poly_orderbook, p15_up_token)
                                    down_f = executor.submit(fetch_poly_orderbook, p15_down_token)
                                    p15_up_asks = up_f.result(timeout=3)
                                    p15_down_asks = down_f.result(timeout=3)

                                p15_up_ask = p15_up_asks[0][0] if p15_up_asks else None
                                p15_down_ask = p15_down_asks[0][0] if p15_down_asks else None
                                p15_up_depth = int(p15_up_asks[0][1]) if p15_up_asks else 0
                                p15_down_depth = int(p15_down_asks[0][1]) if p15_down_asks else 0
                            except Exception:
                                p15_up_ask, p15_down_ask = None, None
                                p15_up_depth, p15_down_depth = 0, 0

                            if k1h_yes is not None and k1h_no is not None and p15_up_ask and p15_down_ask:
                                # Cross-strike direction
                                if k1h_strike < p15_strike:
                                    dir_c3 = "K_UP+P_DOWN"
                                    k_price_c3 = k1h_yes
                                    p_price_c3 = p15_down_ask
                                    kalshi_side_c3 = "yes"
                                    poly_token_c3 = p15_down_token
                                    p_depth_c3 = p15_down_depth
                                else:
                                    dir_c3 = "K_DOWN+P_UP"
                                    k_price_c3 = k1h_no
                                    p_price_c3 = p15_up_ask
                                    kalshi_side_c3 = "no"
                                    poly_token_c3 = p15_up_token
                                    p_depth_c3 = p15_up_depth

                                cost_c3 = k_price_c3 + p_price_c3
                                edge_c3 = ((1.0 - cost_c3) / cost_c3 * 100) if cost_c3 > 0 else -100

                                # Log scan
                                print(f"  ┌─ {crypto}/1h×15 ─────────────────────────────────")
                                print(f"  │ KALSHI 1h:  YES={k1h_yes:.2f}  NO={k1h_no:.2f} (fresh)")
                                print(f"  │ POLY 15m:   UP={p15_up_ask:.2f}({p15_up_depth})  DOWN={p15_down_ask:.2f}({p15_down_depth}) (fresh)")
                                print(f"  │ STRIKES: K1h={fmt_strike(k1h_strike)} vs P15={fmt_strike(p15_strike)}")
                                print(f"  │ EDGE: {edge_c3:.1f}% via {dir_c3} ★SAFE(cross-strike)")
                                print(f"  └──────────────────────────────────────────")

                                SAFE_BET_MIN_EDGE_CROSS = 1.5
                                skip_c3, bin_c3, cl_c3 = should_skip_cross_strike(
                                    k1h_strike, p15_strike, crypto, ws_fetcher
                                )
                                if skip_c3:
                                    if scan_num % 20 == 0:
                                        print(f"    [1h×15 CROSS-SKIP] Price feeds diverge: Binance=${bin_c3:,.2f} Chainlink=${cl_c3:,.2f}")
                                elif (edge_c3 >= SAFE_BET_MIN_EDGE_CROSS
                                        and 0.05 <= k_price_c3 <= 0.88
                                        and 0.02 <= p_price_c3 <= 0.88
                                        and p_depth_c3 >= 15):

                                    qty_c3 = min(int(p_depth_c3 * 0.25), 10)
                                    qty_c3 = max(qty_c3, 1)

                                    print(f"\n  ╔═══════════════════════════════════════════════════════╗")
                                    print(f"  ║  EXECUTING CROSS TRADE: {crypto}/1h×15                 ║")
                                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                                    print(f"  ║  Strategy: {dir_c3} (cross-strike 1h×15m)")
                                    print(f"  ║  Edge: {edge_c3:.1f}%")
                                    print(f"  ║  K1h strike: {fmt_strike(k1h_strike)}  P15 strike: {fmt_strike(p15_strike)}")
                                    print(f"  ║  Qty: {qty_c3} contracts")
                                    print(f"  ╚═══════════════════════════════════════════════════════╝")

                                    trade_executed_1h = True  # Stop scanning — jump into trade
                                    # Execute: Kalshi 1h first, then Poly 15m
                                    k_cents_c3 = int(k_price_c3 * 100) + 1
                                    print(f"  [1h×15 STEP 1] KALSHI 1h: {qty_c3}x {kalshi_side_c3.upper()} @ {k_cents_c3}c...")
                                    k_result_c3 = ws_fetcher._kalshi_client.place_order(
                                        k1h_ticker, kalshi_side_c3, qty_c3, k_cents_c3
                                    )
                                    k_filled_c3 = 0
                                    if k_result_c3:
                                        ko = k_result_c3.get("order", {})
                                        k_filled_c3 = ko.get("filled_count", 0)
                                        ks = ko.get("status", "").lower()
                                        kid = ko.get("order_id", "")
                                        if k_filled_c3 == 0 and ks not in ["executed", "filled"]:
                                            if kid:
                                                ws_fetcher._kalshi_client.cancel_order(kid)
                                            print(f"  [1h×15 KALSHI] Not filled - cancelled")
                                        else:
                                            k_filled_c3 = k_filled_c3 or qty_c3

                                    if k_filled_c3 > 0:
                                        print(f"  [1h×15 STEP 2] POLY 15m: {k_filled_c3}x @ ${p_price_c3:.2f}...")
                                        p_result_c3 = ws_fetcher.ws_manager.poly_ws.place_order(
                                            poly_token_c3, "BUY", k_filled_c3, p_price_c3, immediate=True
                                        )
                                        if p_result_c3 and p_result_c3.success:
                                            actual_c3 = p_result_c3.filled_qty or k_filled_c3
                                            print(f"  [1h×15 COMPLETE] {crypto} cross-trade executed! {actual_c3}x")
                                            traded_cryptos_1h[crypto] = traded_cryptos_1h.get(crypto, 0) + 1
                                            if traded_cryptos_1h[crypto] >= MAX_TRADES_PER_CRYPTO_1H:
                                                stopped_cryptos_1h.add(crypto)
                                            # Record trade for window summary
                                            k_fee_c3 = calc_kalshi_fee(k_price_c3, actual_c3, is_maker=False)
                                            p_fee_c3 = calc_poly_fee(p_price_c3, actual_c3)
                                            fees_c3 = k_fee_c3 + p_fee_c3 + GAS_FEE_DOLLARS
                                            poly_side_c3 = "DOWN" if dir_c3 == "K_UP+P_DOWN" else "UP"
                                            k_fill_total_c3 = k_price_c3 * actual_c3
                                            p_fill_total_c3 = p_price_c3 * actual_c3
                                            total_cost_c3 = k_fill_total_c3 + p_fill_total_c3
                                            total_bet_c3 = total_cost_c3 + fees_c3
                                            log_trade_row({
                                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                                "crypto": crypto,
                                                "direction": dir_c3,
                                                "qty": actual_c3,
                                                "kalshi_side": kalshi_side_c3.upper(),
                                                "kalshi_fill": k_price_c3,
                                                "poly_side": poly_side_c3,
                                                "poly_fill": p_price_c3,
                                                "kalshi_fill_total": k_fill_total_c3,
                                                "poly_fill_total": p_fill_total_c3,
                                                "total_cost": total_cost_c3,
                                                "kalshi_fee": k_fee_c3,
                                                "poly_fee": p_fee_c3,
                                                "total_fees": fees_c3,
                                                "total_bet": total_bet_c3,
                                                "edge_expected_pct": edge_c3,
                                                "edge_actual_pct": "",
                                                "was_unwound": "NO",
                                                "unwind_loss": "",
                                                "kalshi_strike": k1h_strike,
                                                "poly_strike": p15_strike,
                                                "kalshi_end_price": "",
                                                "poly_end_price": "",
                                                "kalshi_result": "",
                                                "poly_result": "",
                                                "pnl": "",
                                                "status": "SUCCESS",
                                            })
                                            flush_scan_buffer_to_log()
                                            window_trades.append({
                                                "crypto": crypto,
                                                "market_type": "1h×15m",
                                                "direction": dir_c3,
                                                "qty": actual_c3,
                                                "kalshi_side": kalshi_side_c3,
                                                "kalshi_fill_price": k_price_c3,
                                                "poly_fill_price": p_price_c3,
                                                "total_cost": (k_price_c3 + p_price_c3) * actual_c3,
                                                "fees": fees_c3,
                                                "kalshi_fee": k_fee_c3,
                                                "poly_fee": p_fee_c3,
                                                "kalshi_strike": k1h_strike,
                                                "poly_strike": p15_strike,
                                                "was_unwound": False,
                                                "unwind_loss": 0.0,
                                                "status": "SUCCESS",
                                            })
                                        else:
                                            print(f"  [1h×15 POLY] FAILED - unwinding Kalshi 1h")
                                            # Unwind
                                            rem_c3 = k_filled_c3
                                            for ua in range(5):
                                                if rem_c3 <= 0:
                                                    break
                                                fob = ws_fetcher._kalshi_client.get_orderbook(k1h_ticker)
                                                sp = None
                                                if fob:
                                                    ob = fob.get("orderbook", {}) or {}
                                                    bids = ob.get(kalshi_side_c3, []) or []
                                                    if bids:
                                                        sp = max(b[0] for b in bids)
                                                if sp and sp > 0:
                                                    ur = ws_fetcher._kalshi_client.sell_position(k1h_ticker, kalshi_side_c3, rem_c3, sp, gtc=False)
                                                    if ur:
                                                        uo = ur.get("order", {})
                                                        uf = uo.get("filled_count", 0)
                                                        if uf > 0 or uo.get("status", "").lower() in ["executed", "filled"]:
                                                            rem_c3 -= (uf if uf > 0 else rem_c3)
                                                time.sleep(0.3)

                    if trade_executed_1h:
                        break

                    # ─── COMBINATION 4: K_15m + P_1h (cross-timeframe, final 15 min only) ───
                    if in_final_15m and p1h_up_ask is not None and p1h_down_ask is not None and p1h_strike:
                        k15_strike = ws_fetcher._kalshi_strikes.get(crypto)
                        k15_ticker = ws_fetcher._ticker_cache.get(crypto, (None,))[0]

                        if k15_strike and k15_ticker and k15_strike != p1h_strike:
                            # Fresh fetch Kalshi 15m prices
                            k15_result = fetch_fresh_kalshi_prices(
                                ws_fetcher._kalshi_client, k15_ticker, debug=False
                            )
                            k15_yes, k15_no, k15_yes_depth, k15_no_depth, _, _ = k15_result

                            if k15_yes is not None and k15_no is not None:

                                # Reuse P1h prices from batch fetch (already fetched this cycle)
                                p1h_up_c4 = p1h_up_ask
                                p1h_down_c4 = p1h_down_ask
                                p1h_up_depth_c4 = p1h_up_depth
                                p1h_down_depth_c4 = p1h_down_depth

                                # Cross-strike direction
                                if k15_strike < p1h_strike:
                                    dir_c4 = "K_UP+P_DOWN"
                                    k_price_c4 = k15_yes
                                    p_price_c4 = p1h_down_c4
                                    kalshi_side_c4 = "yes"
                                    poly_token_c4 = p1h_down_token
                                    p_depth_c4 = p1h_down_depth_c4
                                else:
                                    dir_c4 = "K_DOWN+P_UP"
                                    k_price_c4 = k15_no
                                    p_price_c4 = p1h_up_c4
                                    kalshi_side_c4 = "no"
                                    poly_token_c4 = p1h_up_token
                                    p_depth_c4 = p1h_up_depth_c4

                                cost_c4 = k_price_c4 + p_price_c4
                                edge_c4 = ((1.0 - cost_c4) / cost_c4 * 100) if cost_c4 > 0 else -100

                                # Log scan
                                print(f"  ┌─ {crypto}/15×1h ─────────────────────────────────")
                                print(f"  │ KALSHI 15m: YES={k15_yes:.2f}  NO={k15_no:.2f} (fresh)")
                                print(f"  │ POLY 1h:    UP={p1h_up_c4:.2f}({p1h_up_depth_c4})  DOWN={p1h_down_c4:.2f}({p1h_down_depth_c4}) (fresh)")
                                print(f"  │ STRIKES: K15={fmt_strike(k15_strike)} vs P1h={fmt_strike(p1h_strike)}")
                                print(f"  │ EDGE: {edge_c4:.1f}% via {dir_c4} ★SAFE(cross-strike)")
                                print(f"  └──────────────────────────────────────────")

                                SAFE_BET_MIN_EDGE_CROSS = 1.5
                                skip_c4, bin_c4, cl_c4 = should_skip_cross_strike(
                                    k15_strike, p1h_strike, crypto, ws_fetcher
                                )
                                if skip_c4:
                                    if scan_num % 20 == 0:
                                        print(f"    [15×1h CROSS-SKIP] Price feeds diverge: Binance=${bin_c4:,.2f} Chainlink=${cl_c4:,.2f}")
                                elif (edge_c4 >= SAFE_BET_MIN_EDGE_CROSS
                                        and 0.05 <= k_price_c4 <= 0.88
                                        and 0.02 <= p_price_c4 <= 0.88
                                        and p_depth_c4 >= 15):

                                    qty_c4 = min(int(p_depth_c4 * 0.25), 10)
                                    qty_c4 = max(qty_c4, 1)

                                    print(f"\n  ╔═══════════════════════════════════════════════════════╗")
                                    print(f"  ║  EXECUTING CROSS TRADE: {crypto}/15×1h                 ║")
                                    print(f"  ╠═══════════════════════════════════════════════════════╣")
                                    print(f"  ║  Strategy: {dir_c4} (cross-strike 15m×1h)")
                                    print(f"  ║  Edge: {edge_c4:.1f}%")
                                    print(f"  ║  K15 strike: {fmt_strike(k15_strike)}  P1h strike: {fmt_strike(p1h_strike)}")
                                    print(f"  ║  Qty: {qty_c4} contracts")
                                    print(f"  ╚═══════════════════════════════════════════════════════╝")

                                    trade_executed_1h = True  # Stop scanning — jump into trade
                                    # Execute: Kalshi 15m first, then Poly 1h
                                    k_cents_c4 = int(k_price_c4 * 100) + 1
                                    print(f"  [15×1h STEP 1] KALSHI 15m: {qty_c4}x {kalshi_side_c4.upper()} @ {k_cents_c4}c...")
                                    k_result_c4 = ws_fetcher._kalshi_client.place_order(
                                        k15_ticker, kalshi_side_c4, qty_c4, k_cents_c4
                                    )
                                    k_filled_c4 = 0
                                    if k_result_c4:
                                        ko = k_result_c4.get("order", {})
                                        k_filled_c4 = ko.get("filled_count", 0)
                                        ks = ko.get("status", "").lower()
                                        kid = ko.get("order_id", "")
                                        if k_filled_c4 == 0 and ks not in ["executed", "filled"]:
                                            if kid:
                                                ws_fetcher._kalshi_client.cancel_order(kid)
                                            print(f"  [15×1h KALSHI] Not filled - cancelled")
                                        else:
                                            k_filled_c4 = k_filled_c4 or qty_c4

                                    if k_filled_c4 > 0:
                                        print(f"  [15×1h STEP 2] POLY 1h: {k_filled_c4}x @ ${p_price_c4:.2f}...")
                                        p_result_c4 = ws_fetcher.ws_manager.poly_ws.place_order(
                                            poly_token_c4, "BUY", k_filled_c4, p_price_c4, immediate=True
                                        )
                                        if p_result_c4 and p_result_c4.success:
                                            actual_c4 = p_result_c4.filled_qty or k_filled_c4
                                            print(f"  [15×1h COMPLETE] {crypto} cross-trade executed! {actual_c4}x")
                                            traded_cryptos_1h[crypto] = traded_cryptos_1h.get(crypto, 0) + 1
                                            if traded_cryptos_1h[crypto] >= MAX_TRADES_PER_CRYPTO_1H:
                                                stopped_cryptos_1h.add(crypto)
                                            # Record trade for window summary
                                            k_fee_c4 = calc_kalshi_fee(k_price_c4, actual_c4, is_maker=False)
                                            p_fee_c4 = calc_poly_fee(p_price_c4, actual_c4)
                                            fees_c4 = k_fee_c4 + p_fee_c4 + GAS_FEE_DOLLARS
                                            poly_side_c4 = "DOWN" if dir_c4 == "K_UP+P_DOWN" else "UP"
                                            k_fill_total_c4 = k_price_c4 * actual_c4
                                            p_fill_total_c4 = p_price_c4 * actual_c4
                                            total_cost_c4 = k_fill_total_c4 + p_fill_total_c4
                                            total_bet_c4 = total_cost_c4 + fees_c4
                                            log_trade_row({
                                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                                "crypto": crypto,
                                                "direction": dir_c4,
                                                "qty": actual_c4,
                                                "kalshi_side": kalshi_side_c4.upper(),
                                                "kalshi_fill": k_price_c4,
                                                "poly_side": poly_side_c4,
                                                "poly_fill": p_price_c4,
                                                "kalshi_fill_total": k_fill_total_c4,
                                                "poly_fill_total": p_fill_total_c4,
                                                "total_cost": total_cost_c4,
                                                "kalshi_fee": k_fee_c4,
                                                "poly_fee": p_fee_c4,
                                                "total_fees": fees_c4,
                                                "total_bet": total_bet_c4,
                                                "edge_expected_pct": edge_c4,
                                                "edge_actual_pct": "",
                                                "was_unwound": "NO",
                                                "unwind_loss": "",
                                                "kalshi_strike": k15_strike,
                                                "poly_strike": p1h_strike,
                                                "kalshi_end_price": "",
                                                "poly_end_price": "",
                                                "kalshi_result": "",
                                                "poly_result": "",
                                                "pnl": "",
                                                "status": "SUCCESS",
                                            })
                                            flush_scan_buffer_to_log()
                                            window_trades.append({
                                                "crypto": crypto,
                                                "market_type": "15m×1h",
                                                "direction": dir_c4,
                                                "qty": actual_c4,
                                                "kalshi_side": kalshi_side_c4,
                                                "kalshi_fill_price": k_price_c4,
                                                "poly_fill_price": p_price_c4,
                                                "total_cost": (k_price_c4 + p_price_c4) * actual_c4,
                                                "fees": fees_c4,
                                                "kalshi_fee": k_fee_c4,
                                                "poly_fee": p_fee_c4,
                                                "kalshi_strike": k15_strike,
                                                "poly_strike": p1h_strike,
                                                "was_unwound": False,
                                                "unwind_loss": 0.0,
                                                "status": "SUCCESS",
                                            })
                                        else:
                                            print(f"  [15×1h POLY] FAILED - unwinding Kalshi 15m")
                                            rem_c4 = k_filled_c4
                                            for ua in range(5):
                                                if rem_c4 <= 0:
                                                    break
                                                fob = ws_fetcher._kalshi_client.get_orderbook(k15_ticker)
                                                sp = None
                                                if fob:
                                                    ob = fob.get("orderbook", {}) or {}
                                                    bids = ob.get(kalshi_side_c4, []) or []
                                                    if bids:
                                                        sp = max(b[0] for b in bids)
                                                if sp and sp > 0:
                                                    ur = ws_fetcher._kalshi_client.sell_position(k15_ticker, kalshi_side_c4, rem_c4, sp, gtc=False)
                                                    if ur:
                                                        uo = ur.get("order", {})
                                                        uf = uo.get("filled_count", 0)
                                                        if uf > 0 or uo.get("status", "").lower() in ["executed", "filled"]:
                                                            rem_c4 -= (uf if uf > 0 else rem_c4)
                                                time.sleep(0.3)

            # ═══════════════════════════════════════════════════════════════
            # 5-MINUTE MARKET CHECK (BTC only - Kalshi 15m vs Poly 5m)
            # DISABLED: Kalshi and Polymarket use different resolution prices,
            # so cross-strike arb can double-lose. Set ENABLE_5M_CROSS_STRIKE
            # in config.py to re-enable.
            # ═══════════════════════════════════════════════════════════════
            secs_into_window_5m = get_window_progress() * 15 * 60
            if (secs_into_window_5m >= 605 and no_losses_mode and ENABLE_5M_CROSS_STRIKE):
                # Try Chainlink persistent feed, retry for up to 10s
                if not five_min_chainlink_tried:
                    if ws_fetcher.ws_manager and ws_fetcher.ws_manager.chainlink_feed:
                        strike_5m = ws_fetcher.ws_manager.chainlink_feed.get_strike_5m("BTC")
                        if strike_5m and strike_5m > 0:
                            five_min_chainlink_tried = True
                            # Got the Chainlink price — fetch tokens from Gamma API only (skip scraper)
                            five_min_data = ws_fetcher.fetch_5m_market("BTC", skip_scraper=True)
                            five_min_fetched_this_window = True
                            if five_min_data:
                                five_min_data["strike"] = strike_5m
                                pfmt_5m = f"${strike_5m:,.5f}" if strike_5m < 10 else f"${strike_5m:,.4f}" if strike_5m < 100 else f"${strike_5m:,.2f}"
                                print(f"  [5M] BTC: Chainlink 5-min strike at t+{secs_into_window_5m:.0f}s: {pfmt_5m}")
                            else:
                                pfmt_5m = f"${strike_5m:,.5f}" if strike_5m < 10 else f"${strike_5m:,.4f}" if strike_5m < 100 else f"${strike_5m:,.2f}"
                                print(f"  [5M] BTC: Chainlink strike found ({pfmt_5m}) but Gamma API failed")
                                five_min_fetched_this_window = False
                        elif secs_into_window_5m >= 615:
                            # Give up after t+615s (15s of retrying)
                            five_min_chainlink_tried = True
                            print(f"  [5M] BTC: Chainlink 5-min strike not available after t+{secs_into_window_5m:.0f}s — skipping 5-min trade this window")

                # Scan for cross-strike arb: Kalshi 15m vs Poly 5m
                if five_min_data and five_min_data.get("strike"):
                    kalshi_strike_15m = ws_fetcher._kalshi_strikes.get("BTC")
                    five_min_strike = five_min_data["strike"]

                    if not kalshi_strike_15m:
                        if scan_num % 20 == 0:
                            print(f"  [5M-SKIP] Kalshi 15m strike not available for BTC - cannot compare")
                    elif kalshi_strike_15m == five_min_strike:
                        if scan_num % 20 == 0:
                            print(f"  [5M-SKIP] Strikes identical: K15={fmt_strike(kalshi_strike_15m)} == P5={fmt_strike(five_min_strike)} - no cross-strike arb")
                    elif kalshi_strike_15m and five_min_strike and kalshi_strike_15m != five_min_strike:
                        # Get fresh 5-min prices via REST
                        prices_5m = ws_fetcher.fetch_5m_prices(
                            five_min_data["up_token"], five_min_data["down_token"]
                        )

                        # Get FRESH Kalshi 15m prices (don't rely on snapshot which may be stale)
                        btc_snap = snapshots.get("BTC")
                        kalshi_fresh_ok = False
                        kalshi_ticker_5m_check = ws_fetcher._ticker_cache.get("BTC", (None,))[0]
                        if kalshi_ticker_5m_check and btc_snap:
                            fresh_result = fetch_fresh_kalshi_prices(
                                ws_fetcher._kalshi_client, kalshi_ticker_5m_check
                            )
                            if fresh_result:
                                fresh_yes, fresh_no = fresh_result[0], fresh_result[1]
                                if fresh_yes is not None and fresh_no is not None:
                                    btc_snap.k_up = fresh_yes
                                    btc_snap.k_down = fresh_no
                                    kalshi_fresh_ok = True

                        if not prices_5m:
                            if scan_num % 20 == 0:
                                print(f"  [5M-SKIP] Could not fetch Poly 5m orderbook prices")
                        elif not kalshi_fresh_ok:
                            if scan_num % 20 == 0:
                                print(f"  [5M-SKIP] Fresh Kalshi 15m fetch failed - refusing stale prices")
                        elif prices_5m and btc_snap and btc_snap.k_up and btc_snap.k_down:
                            p5_up_ask = prices_5m.get("up_ask")
                            p5_down_ask = prices_5m.get("down_ask")
                            p5_up_depth = prices_5m.get("up_depth", 0)
                            p5_down_depth = prices_5m.get("down_depth", 0)

                            if p5_up_ask and p5_down_ask:
                                # Determine cross-strike direction
                                # If Kalshi strike < 5m strike: buy K_UP + P5_DOWN
                                # If Kalshi strike > 5m strike: buy K_DOWN + P5_UP
                                if kalshi_strike_15m < five_min_strike:
                                    direction_5m = "K_UP+P5_DOWN"
                                    k_price_5m = btc_snap.k_up
                                    p_price_5m = p5_down_ask
                                    kalshi_side_5m = "yes"
                                    poly_token_5m = five_min_data["down_token"]
                                    p5_depth = p5_down_depth
                                else:
                                    direction_5m = "K_DOWN+P5_UP"
                                    k_price_5m = btc_snap.k_down
                                    p_price_5m = p5_up_ask
                                    kalshi_side_5m = "no"
                                    poly_token_5m = five_min_data["up_token"]
                                    p5_depth = p5_up_depth

                                # Calculate edge (same as normal: cost = k + p, profit if both win)
                                combined_cost = k_price_5m + p_price_5m
                                raw_edge_5m = ((1.0 - combined_cost) / combined_cost) * 100 if combined_cost > 0 else 0

                                # Log the scan
                                print(f"  ┌─ BTC/5 ─────────────────────────────────")
                                print(f"  │ KALSHI 15m: YES={btc_snap.k_up:.2f}  NO={btc_snap.k_down:.2f}")
                                print(f"  │ POLY 5m:    UP={p5_up_ask:.2f}({p5_up_depth})  DOWN={p5_down_ask:.2f}({p5_down_depth})")
                                print(f"  │ STRIKES: K15={fmt_strike(kalshi_strike_15m)} vs P5={fmt_strike(five_min_strike)}")
                                print(f"  │ EDGE: {raw_edge_5m:.1f}% via {direction_5m} ★SAFE(cross-strike)")
                                print(f"  └──────────────────────────────────────────")

                                # Check if tradeable
                                SAFE_BET_MIN_EDGE_5M = 1.5
                                skip_5m, bin_5m, cl_5m = should_skip_cross_strike(
                                    kalshi_strike_15m, five_min_strike, "BTC", ws_fetcher
                                )
                                if skip_5m:
                                    if scan_num % 20 == 0:
                                        print(f"    [5M CROSS-SKIP] Price feeds diverge: Binance=${bin_5m:,.2f} Chainlink=${cl_5m:,.2f}")
                                elif (raw_edge_5m >= SAFE_BET_MIN_EDGE_5M
                                        and k_price_5m and p_price_5m
                                        and 0.05 <= k_price_5m <= 0.88
                                        and 0.02 <= p_price_5m <= 0.88
                                        and p5_depth >= 15):

                                    # Sizing: 25% of Poly 5m depth, max 10
                                    thin_side_5m = p5_depth
                                    if thin_side_5m >= 15:
                                        qty_5m = min(int(thin_side_5m * 0.25), 10)
                                        qty_5m = max(qty_5m, 1)

                                        print(f"\n  ╔═══════════════════════════════════════════════════════╗")
                                        print(f"  ║  EXECUTING 5-MIN TRADE: BTC/5                         ║")
                                        print(f"  ╠═══════════════════════════════════════════════════════╣")
                                        print(f"  ║  Strategy: {direction_5m} (cross-strike)")
                                        print(f"  ║  Edge: {raw_edge_5m:.1f}%")
                                        print(f"  ║  K15 strike: {fmt_strike(kalshi_strike_15m)}  P5 strike: {fmt_strike(five_min_strike)}")
                                        print(f"  ║  KALSHI: {kalshi_side_5m.upper()} @ ${k_price_5m:.2f}  POLY5: {'DOWN' if kalshi_side_5m == 'yes' else 'UP'} @ ${p_price_5m:.2f}")
                                        print(f"  ║  Qty: {qty_5m} contracts")
                                        print(f"  ╚═══════════════════════════════════════════════════════╝")

                                        # Place Kalshi order (15m market)
                                        kalshi_ticker_5m = ws_fetcher._ticker_cache.get("BTC", (None,))[0]
                                        if kalshi_ticker_5m:
                                            k_price_cents_5m = int(k_price_5m * 100) + 1
                                            print(f"  [5M STEP 1] KALSHI: {qty_5m}x {kalshi_side_5m.upper()} @ {k_price_cents_5m}c...")
                                            k_result_5m = ws_fetcher._kalshi_client.place_order(
                                                kalshi_ticker_5m, kalshi_side_5m, qty_5m, k_price_cents_5m
                                            )

                                            kalshi_filled_5m = 0
                                            if k_result_5m:
                                                k_order_5m = k_result_5m.get("order", {})
                                                kalshi_filled_5m = k_order_5m.get("filled_count", 0)
                                                k_status_5m = k_order_5m.get("status", "").lower()
                                                k_id_5m = k_order_5m.get("order_id", "")
                                                print(f"  [5M KALSHI] status={k_status_5m}, filled={kalshi_filled_5m}")

                                                if kalshi_filled_5m == 0 and k_status_5m not in ["executed", "filled"]:
                                                    # Cancel unfilled order
                                                    if k_id_5m:
                                                        ws_fetcher._kalshi_client.cancel_order(k_id_5m)
                                                    print(f"  [5M KALSHI] Not filled - cancelled")
                                                else:
                                                    kalshi_filled_5m = kalshi_filled_5m or qty_5m

                                            if kalshi_filled_5m > 0:
                                                # Place Polymarket 5-min order
                                                print(f"  [5M STEP 2] POLY 5m: {kalshi_filled_5m}x {'DOWN' if kalshi_side_5m == 'yes' else 'UP'} @ ${p_price_5m:.2f}...")
                                                p_result_5m = ws_fetcher.ws_manager.poly_ws.place_order(
                                                    poly_token_5m, "BUY", kalshi_filled_5m, p_price_5m, immediate=True
                                                )

                                                if p_result_5m and p_result_5m.success:
                                                    poly_filled_5m = p_result_5m.filled_qty or kalshi_filled_5m
                                                    poly_fill_price_5m = getattr(p_result_5m, 'avg_price', p_price_5m) or p_price_5m
                                                    print(f"  [5M POLY] FILLED! {poly_filled_5m}x @ ${poly_fill_price_5m:.3f}")

                                                    # Calculate actual edge with fees
                                                    actual_k_cost_5m = k_price_cents_5m / 100.0
                                                    actual_total_cost_5m = actual_k_cost_5m + poly_fill_price_5m
                                                    actual_kalshi_fee_5m = calc_kalshi_fee(actual_k_cost_5m, poly_filled_5m, is_maker=False)
                                                    actual_poly_fee_5m = calc_poly_fee(poly_fill_price_5m, poly_filled_5m)
                                                    actual_gas_5m = GAS_FEE_DOLLARS
                                                    total_fees_5m = actual_kalshi_fee_5m + actual_poly_fee_5m + actual_gas_5m
                                                    total_cost_with_fees_5m = (actual_total_cost_5m * poly_filled_5m) + total_fees_5m
                                                    payout_5m = 1.0 * poly_filled_5m
                                                    profit_5m = payout_5m - total_cost_with_fees_5m
                                                    actual_edge_5m = profit_5m / total_cost_with_fees_5m * 100 if total_cost_with_fees_5m > 0 else 0

                                                    # Detailed trade completion box
                                                    trade_time_5m = now_mst()
                                                    poly_side_5m = 'DOWN' if kalshi_side_5m == 'yes' else 'UP'
                                                    r5 = []
                                                    r5.append(f"  ╔═══════════════════════════════════════════════════════╗")
                                                    r5.append(f"  ║  ✓ 5-MIN TRADE COMPLETE                               ║")
                                                    r5.append(f"  ╠═══════════════════════════════════════════════════════╣")
                                                    r5.append(f"  ║  Time:     {trade_time_5m}")
                                                    r5.append(f"  ║  Crypto:   BTC/5 (cross-strike)")
                                                    r5.append(f"  ║  Strategy: {direction_5m}")
                                                    r5.append(f"  ║  Qty:      {poly_filled_5m} contracts")
                                                    r5.append(f"  ╠═══════════════════════════════════════════════════════╣")
                                                    r5.append(f"  ║  STRIKES:")
                                                    r5.append(f"  ║    K15 strike: {fmt_strike(kalshi_strike_15m)}")
                                                    r5.append(f"  ║    P5 strike:  {fmt_strike(five_min_strike)}")
                                                    r5.append(f"  ╠═══════════════════════════════════════════════════════╣")
                                                    r5.append(f"  ║  FILL PRICES:")
                                                    r5.append(f"  ║    Kalshi:  {kalshi_side_5m.upper()} @ ${actual_k_cost_5m:.2f}")
                                                    r5.append(f"  ║    Poly5:   {poly_side_5m} @ ${poly_fill_price_5m:.3f}")
                                                    r5.append(f"  ║    Total:   ${actual_total_cost_5m:.2f} x {poly_filled_5m} = ${actual_total_cost_5m * poly_filled_5m:.2f}")
                                                    r5.append(f"  ╠═══════════════════════════════════════════════════════╣")
                                                    r5.append(f"  ║  FEES:")
                                                    r5.append(f"  ║    Kalshi:  ${actual_kalshi_fee_5m:.2f}")
                                                    r5.append(f"  ║    Poly:    ${actual_poly_fee_5m:.2f}")
                                                    r5.append(f"  ║    Gas:     ${actual_gas_5m:.2f}")
                                                    r5.append(f"  ║    Total:   ${total_fees_5m:.2f}")
                                                    r5.append(f"  ╠═══════════════════════════════════════════════════════╣")
                                                    r5.append(f"  ║  EDGE:")
                                                    r5.append(f"  ║    Expected (scan): {raw_edge_5m:.1f}%")
                                                    r5.append(f"  ║    Actual (fills+fees): {actual_edge_5m:.1f}%")
                                                    r5.append(f"  ╠═══════════════════════════════════════════════════════╣")
                                                    r5.append(f"  ║  PROFIT: ${profit_5m:.2f} ({actual_edge_5m:.1f}%)")
                                                    r5.append(f"  ║  Unwound: NO")
                                                    r5.append(f"  ╚═══════════════════════════════════════════════════════╝")
                                                    print(f"")
                                                    for line in r5:
                                                        print(line)
                                                    print(f"")
                                                    log_highlight("\n".join(r5) + "\n\n")

                                                    # Record trade for window summary
                                                    window_trades.append({
                                                        "crypto": "BTC",
                                                        "market_type": "5m",
                                                        "direction": direction_5m,
                                                        "qty": poly_filled_5m,
                                                        "kalshi_side": kalshi_side_5m,
                                                        "kalshi_fill_price": actual_k_cost_5m,
                                                        "poly_fill_price": poly_fill_price_5m,
                                                        "total_cost": (actual_k_cost_5m + poly_fill_price_5m) * poly_filled_5m,
                                                        "fees": total_fees_5m,
                                                        "kalshi_fee": actual_kalshi_fee_5m,
                                                        "poly_fee": actual_poly_fee_5m,
                                                        "kalshi_strike": kalshi_strike_15m,
                                                        "poly_strike": five_min_strike,
                                                        "was_unwound": False,
                                                        "unwind_loss": 0.0,
                                                        "status": "SUCCESS",
                                                    })
                                                else:
                                                    print(f"  [5M POLY] FAILED - need to unwind Kalshi")
                                                    # Unwind using proper orderbook-based process
                                                    unwind_success_5m = False
                                                    remaining_5m = kalshi_filled_5m
                                                    total_sold_5m = 0
                                                    total_sell_proceeds_5m = 0.0
                                                    max_unwind_attempts_5m = 5

                                                    for unwind_attempt_5m in range(max_unwind_attempts_5m):
                                                        if remaining_5m <= 0:
                                                            unwind_success_5m = True
                                                            break

                                                        print(f"  [5M UNWIND] Attempt {unwind_attempt_5m + 1}/{max_unwind_attempts_5m} - Selling {remaining_5m}x remaining...")

                                                        # Fetch fresh Kalshi orderbook to get actual best bid
                                                        fresh_ob_5m = ws_fetcher._kalshi_client.get_orderbook(kalshi_ticker_5m)
                                                        sell_price_cents_5m = None

                                                        if fresh_ob_5m:
                                                            orderbook_5m = fresh_ob_5m.get("orderbook", {}) or {}
                                                            if kalshi_side_5m == "yes":
                                                                bids_5m = orderbook_5m.get("yes", []) or []
                                                                if bids_5m:
                                                                    best_bid_5m = max(b[0] for b in bids_5m)
                                                                    sell_price_cents_5m = best_bid_5m
                                                                    print(f"  [5M UNWIND] Fresh orderbook: best YES bid = {best_bid_5m}c")
                                                            else:
                                                                bids_5m = orderbook_5m.get("no", []) or []
                                                                if bids_5m:
                                                                    best_bid_5m = max(b[0] for b in bids_5m)
                                                                    sell_price_cents_5m = best_bid_5m
                                                                    print(f"  [5M UNWIND] Fresh orderbook: best NO bid = {best_bid_5m}c")

                                                        # Fallback to WebSocket price
                                                        if sell_price_cents_5m is None:
                                                            kalshi_unwind_price_5m = ws_fetcher.ws_manager.get_kalshi_price("BTC")
                                                            if kalshi_unwind_price_5m:
                                                                if kalshi_side_5m == "yes":
                                                                    sell_price_cents_5m = int((kalshi_unwind_price_5m.yes_price or 0.01) * 100) - 1
                                                                else:
                                                                    sell_price_cents_5m = int((kalshi_unwind_price_5m.no_price or 0.01) * 100) - 1
                                                                print(f"  [5M UNWIND] Using WebSocket fallback price: {sell_price_cents_5m}c")

                                                        if sell_price_cents_5m and sell_price_cents_5m > 0:
                                                            sell_price_cents_5m = max(1, sell_price_cents_5m)
                                                            print(f"  [5M UNWIND] SELL {remaining_5m}x {kalshi_side_5m.upper()} @ {sell_price_cents_5m}c...")

                                                            unwind_result_5m = ws_fetcher._kalshi_client.sell_position(
                                                                kalshi_ticker_5m, kalshi_side_5m, remaining_5m, sell_price_cents_5m,
                                                                gtc=False
                                                            )

                                                            if unwind_result_5m:
                                                                unwind_order_5m = unwind_result_5m.get("order", {})
                                                                unwind_status_5m = unwind_order_5m.get("status", "").lower()
                                                                unwind_filled_5m = unwind_order_5m.get("filled_count", 0)
                                                                unwind_order_id_5m = unwind_order_5m.get("order_id")

                                                                if unwind_filled_5m > 0 or unwind_status_5m in ["executed", "filled"]:
                                                                    filled_this_5m = unwind_filled_5m if unwind_filled_5m > 0 else remaining_5m
                                                                    fill_price_5m = sell_price_cents_5m / 100.0
                                                                    total_sold_5m += filled_this_5m
                                                                    total_sell_proceeds_5m += fill_price_5m * filled_this_5m
                                                                    remaining_5m -= filled_this_5m
                                                                    print(f"  [5M UNWIND] Filled {filled_this_5m}x @ {sell_price_cents_5m}c (total sold: {total_sold_5m}/{kalshi_filled_5m})")

                                                                    if remaining_5m <= 0:
                                                                        unwind_success_5m = True
                                                                        print(f"  [5M UNWIND] SUCCESS - All {total_sold_5m}x sold!")
                                                                    elif unwind_attempt_5m < max_unwind_attempts_5m - 1:
                                                                        print(f"  [5M UNWIND] Partial fill - {remaining_5m}x remaining, retrying...")
                                                                        time.sleep(0.3)
                                                                elif unwind_status_5m == "resting":
                                                                    if unwind_order_id_5m:
                                                                        print(f"  [5M UNWIND] Order resting, cancelling...")
                                                                        ws_fetcher._kalshi_client.cancel_order(unwind_order_id_5m)
                                                                    if unwind_attempt_5m < max_unwind_attempts_5m - 1:
                                                                        print(f"  [5M UNWIND] Didn't fill at {sell_price_cents_5m}c, will retry...")
                                                                        time.sleep(0.5)
                                                                else:
                                                                    print(f"  [5M UNWIND] Order status: {unwind_status_5m}")
                                                                    if unwind_attempt_5m < max_unwind_attempts_5m - 1:
                                                                        time.sleep(0.5)
                                                        else:
                                                            print(f"  [5M UNWIND] Could not determine sell price")
                                                            if unwind_attempt_5m < max_unwind_attempts_5m - 1:
                                                                time.sleep(0.5)

                                                    if unwind_success_5m:
                                                        print(f"  [5M UNWIND] Complete - sold all {total_sold_5m}x")
                                                    else:
                                                        print(f"  [5M UNWIND] Incomplete - sold {total_sold_5m}/{kalshi_filled_5m}, still holding {remaining_5m}x")

                                                    # Calculate final unwind stats
                                                    unwind_sell_price_5m = None
                                                    unwind_loss_5m = 0.0
                                                    if total_sold_5m > 0:
                                                        unwind_sell_price_5m = total_sell_proceeds_5m / total_sold_5m
                                                        buy_cost_5m = actual_k_cost_5m * kalshi_filled_5m
                                                        unwind_loss_5m = buy_cost_5m - total_sell_proceeds_5m

                                                    # Record unwind for window summary
                                                    window_trades.append({
                                                        "crypto": "BTC",
                                                        "market_type": "5m",
                                                        "direction": direction_5m,
                                                        "qty": kalshi_filled_5m,
                                                        "kalshi_side": kalshi_side_5m,
                                                        "kalshi_fill_price": actual_k_cost_5m,
                                                        "poly_fill_price": 0,
                                                        "total_cost": actual_k_cost_5m * kalshi_filled_5m,
                                                        "fees": 0,
                                                        "kalshi_fee": 0,
                                                        "poly_fee": 0,
                                                        "kalshi_strike": kalshi_strike_15m,
                                                        "poly_strike": five_min_strike,
                                                        "was_unwound": True,
                                                        "unwind_loss": unwind_loss_5m if total_sold_5m > 0 else actual_k_cost_5m * kalshi_filled_5m,
                                                        "status": "UNWIND",
                                                    })
                                elif scan_num % 20 == 0:
                                    # Log why we're not trading (every 20 scans to avoid spam)
                                    reasons = []
                                    if raw_edge_5m < SAFE_BET_MIN_EDGE_5M:
                                        reasons.append(f"edge {raw_edge_5m:.1f}% < {SAFE_BET_MIN_EDGE_5M}%")
                                    if not (0.05 <= k_price_5m <= 0.88):
                                        reasons.append(f"k_price {k_price_5m:.2f} out of [0.05,0.88]")
                                    if not (0.02 <= p_price_5m <= 0.88):
                                        reasons.append(f"p_price {p_price_5m:.2f} out of [0.02,0.88]")
                                    if p5_depth < 15:
                                        reasons.append(f"depth {p5_depth} < 15")
                                    print(f"  [5M-NO-TRADE] {', '.join(reasons)}")

            # Show heartbeat every 10 scans (5 seconds) with current prices
            if scan_num % 10 == 0:
                now = datetime.now().strftime("%H:%M:%S")
                print(f"\n  ─── [{now}] Scan #{scan_num} ───")
                for crypto in cryptos:
                    snap = snapshots.get(crypto)
                    k_strike = ws_fetcher._kalshi_strikes.get(crypto)
                    p_strike = ws_fetcher._poly_strikes.get(crypto)
                    if snap and snap.k_up and snap.k_down and snap.p_up and snap.p_down:
                        best_dir, edge_pct, _, _ = pick_best_direction(snap)
                        strike_str = f"K=${k_strike:,.0f} P=${p_strike:,.0f}" if k_strike and p_strike else "strikes:N/A"
                        print(f"  {crypto}: K[{snap.k_up:.2f}/{snap.k_down:.2f}] P[{snap.p_up:.2f}/{snap.p_down:.2f}] edge={edge_pct:+.1f}% {strike_str}")
                    else:
                        print(f"  {crypto}: waiting for prices...")

            # Short sleep - WebSocket provides real-time updates
            time.sleep(0.5)  # 500ms between checks (vs 2s for REST)

    except KeyboardInterrupt:
        print("\n\n  [STOPPED] User interrupted (Ctrl+C)")
    except Exception as e:
        import traceback
        print(f"\n\n  [CRASH] Unexpected error: {e}")
        print(f"  [CRASH] Traceback:")
        traceback.print_exc()
        print(f"\n  [CRASH] The program crashed - check the error above!")

    # Cleanup
    ws_fetcher.stop()

    # Summary
    trader.print_summary()
    write_session_summary()

    k_bal, p_bal = trader.get_balances()
    print(f"\n  Final balances: Kalshi ${k_bal:.2f}, Poly ${p_bal:.2f}")


def main():
    """Main entry point."""
    # Step 1: VPN Reminder
    show_vpn_reminder()

    # Step 2: Select mode (logging, trading, or redeem)
    mode = select_mode()

    # Handle redemption mode separately
    if mode == "redeem":
        run_redemption_mode()
        return

    # Step 3: Select cryptocurrencies to trade
    cryptos = select_cryptos()

    # Handle WebSocket mode separately
    if mode == "websocket":
        run_websocket_mode(cryptos)
        return

    logging_only = (mode == "logging")
    forever_mode = (mode == "forever")
    testing_mode = (mode == "testing")
    testing_logs_mode = (mode == "testing_logs")

    # Testing mode uses 10 trades, normal mode uses TARGET_SUCCESSFUL_TRADES (5)
    target_trades = 10 if testing_mode else TARGET_SUCCESSFUL_TRADES

    # Mode-specific edge thresholds from config
    if testing_mode:
        active_min_edge = MIN_EDGE_PCT_TESTING  # 5%
    elif forever_mode:
        active_min_edge = MIN_EDGE_PCT_FOREVER  # 11%
    elif testing_logs_mode:
        active_min_edge = MIN_EDGE_PCT_FOREVER  # 11% - same as forever mode
    else:
        active_min_edge = MIN_EDGE_PCT  # 7%

    # Set up the correct log file based on mode
    # testing_logs_mode also uses logging mode file setup
    log_file = set_log_mode(logging_only or testing_logs_mode)

    print("\n" + "=" * 65)
    if logging_only:
        print("  MODE: LOGGING ONLY (runs forever)")
    elif testing_logs_mode:
        print(f"  MODE: TESTING LOGS ({active_min_edge}% edge, TESTING rules, $50/crypto limit, no last 3min)")
    elif forever_mode:
        print(f"  MODE: LIVE TRADING FOREVER ({active_min_edge}% edge, stops if balance < $20)")
    elif testing_mode:
        print(f"  MODE: TESTING (smart rules: {TESTING_MIN_EDGE_NORMAL}%/{TESTING_MIN_EDGE_LATE}% edge, spread gates, {target_trades} trades)")
    else:
        print(f"  MODE: LIVE TRADING ({active_min_edge}% edge, stops after {target_trades} trades)")
    print("=" * 65)

    # Load environment from keys directory
    env_path = os.path.join(KEYS_DIR, ".env")
    load_dotenv(env_path)
    os.makedirs(LOG_DIR, exist_ok=True)

    # Credentials
    kalshi_key_id = os.getenv("KALSHI_API_KEY_ID", "").strip()
    kalshi_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()
    poly_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()

    if not kalshi_key_id or not kalshi_key_path:
        print(f"\n  [X] Missing Kalshi credentials in {env_path}")
        raise SystemExit(1)

    if not poly_key:
        print(f"\n  [X] Missing Polymarket private key in {env_path}")
        raise SystemExit(1)

    # Initialize
    print("\n  [INIT]")
    print("  Connecting to exchanges...")
    kalshi = KalshiClient(kalshi_key_id, kalshi_key_path)
    poly = PolymarketClient(poly_key)
    fetcher = SnapshotFetcher(kalshi, poly)
    trader = ArbTrader(kalshi, poly)
    historical = HistoricalLogger(log_dir=LOG_DIR)

    # Balances
    k_bal, p_bal = trader.get_balances()

    print(f"\n  [BALANCES]")
    print(f"    Kalshi:       ${k_bal:.2f}")
    print(f"    Polymarket:   ${p_bal:.2f}")
    print(f"    Total:        ${k_bal + p_bal:.2f}")

    # Check minimum balance - STOP if either is below $10
    if k_bal < MIN_BALANCE_TO_TRADE:
        print(f"\n  [X] STOPPED: Kalshi balance (${k_bal:.2f}) is below ${MIN_BALANCE_TO_TRADE:.0f} minimum")
        raise SystemExit(1)

    if p_bal < MIN_BALANCE_TO_TRADE:
        print(f"\n  [X] STOPPED: Polymarket balance (${p_bal:.2f}) is below ${MIN_BALANCE_TO_TRADE:.0f} minimum")
        raise SystemExit(1)

    # Config
    print(f"\n  [CONFIG]")
    print(f"    Cryptos:      {', '.join(cryptos)}")
    print(f"    GO threshold: {active_min_edge}% edge (w/fees+gas+buffer)")
    print(f"    Exec threshold: {MIN_EXECUTE_EDGE_PCT}% edge (minimum to execute)")
    print(f"    Price buffer: ${PRICE_BUFFER_START:.3f} -> ${PRICE_BUFFER_MAX:.3f} (+${PRICE_BUFFER_INCREMENT:.3f}/retry)")
    print(f"    Trade size:   ${MIN_BET_DOLLARS}/side min, {MAX_TRADE_QTY} contracts max")
    print(f"    Scan interval: {SCAN_INTERVAL_SECONDS}s")
    print(f"    Min balance:  ${MIN_BALANCE_TO_TRADE:.0f} per platform")
    if logging_only or testing_logs_mode or forever_mode:
        print(f"    Stop: Never (Ctrl+C to stop)")
    else:
        print(f"    Stop: After {target_trades} successful trades")

    print(f"\n  [LOG] {log_file}")

    # Start a new logging session (creates timestamped session file)
    session_file = start_session()
    print(f"  [SESSION] {session_file}")

    scan_num = 0
    start_time = time.time()

    # TESTING mode kill-switch tracking
    testing_peak_balance = k_bal + p_bal  # Track session peak for drawdown
    testing_consecutive_losses = 0
    testing_unwind_count = 0
    testing_cooldown_until = None  # Timestamp when cooldown ends
    testing_trades_complete = False  # Track if TESTING mode reached target (but keep running)
    testing_complete_window = None  # Track which window we completed in (to exit on next window)

    # Spread check cache - skip cryptos with low spread for entire window
    spread_failed_cryptos = {}  # {crypto: window_id} - cryptos that failed spread check this window
    all_skipped_logged = False  # Track if we've shown "all skipped" message for this window

    # Per-crypto market spend tracking - max $50 per crypto per 15-min window
    crypto_market_spend = {}  # {crypto: dollars_spent} - resets each window

    # Per-crypto window contract cap
    window_contracts_traded = {}

    try:
        while True:
            # Check if we've crossed into a new 15-minute market window
            check_market_window_rotation()

            # Check stop condition - successful trades (not in forever mode or TESTING mode)
            # TESTING mode continues running after reaching target to allow position monitoring
            if not logging_only and not testing_logs_mode and not forever_mode and not testing_mode and (trader.stats.successful_trades + trader.stats.partial_fills) >= target_trades:
                print(f"\n  [DONE] Reached {target_trades} trades (incl. unwinds)!")
                break

            # TESTING mode: Mark as complete but keep running until window closes
            if testing_mode and not testing_trades_complete and (trader.stats.successful_trades + trader.stats.partial_fills) >= target_trades:
                testing_trades_complete = True
                # Record current window to detect when it changes
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                testing_complete_window = f"{now.hour:02d}:{(now.minute // 15) * 15:02d}"
                print(f"\n  [TESTING] Reached {target_trades} trades - monitoring until {testing_complete_window} window closes...")

            # TESTING mode: Exit when window changes after completing trades
            if testing_mode and testing_trades_complete and testing_complete_window:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                current_window = f"{now.hour:02d}:{(now.minute // 15) * 15:02d}"
                if current_window != testing_complete_window:
                    print(f"\n  [TESTING] Window closed ({testing_complete_window} -> {current_window}) - exiting")
                    break

            if logging_only and MAX_LOG_OPPORTUNITIES > 0:
                if trader.stats.opportunities_found >= MAX_LOG_OPPORTUNITIES:
                    print(f"\n  [DONE] Logged {MAX_LOG_OPPORTUNITIES} opportunities")
                    break

            # Check stop condition - minimum balance
            k_bal, p_bal = trader.get_balances()
            if k_bal < MIN_BALANCE_TO_TRADE:
                print(f"\n  [STOPPED] Kalshi balance (${k_bal:.2f}) dropped below ${MIN_BALANCE_TO_TRADE:.0f}")
                break

            # Auto-redeem when Polymarket balance drops below threshold
            # This happens BEFORE stop checks so we can recover funds
            if p_bal < AUTO_REDEEM_BALANCE_THRESHOLD:
                print(f"\n  [AUTO-REDEEM] Poly balance ${p_bal:.2f} < $20, attempting to redeem positions...")
                try:
                    redeemed_amount, gas_fees = poly.try_auto_redeem()
                    if redeemed_amount > 0:
                        print(f"  [AUTO-REDEEM] Redeemed ${redeemed_amount:.2f} (gas: {gas_fees:.6f} MATIC)")
                        # Track redemption stats
                        trader.stats.redeemed_amount += redeemed_amount
                        trader.stats.redemption_gas_fees += gas_fees
                        # Re-check balance after redemption
                        _, p_bal = trader.get_balances()
                        print(f"  [AUTO-REDEEM] New Poly balance: ${p_bal:.2f}")
                    else:
                        print(f"  [AUTO-REDEEM] No positions to redeem")
                except Exception as e:
                    print(f"  [AUTO-REDEEM] Error: {e}")

            # Now check Poly minimum balance (after redemption attempt)
            # In forever mode, don't stop for Poly - just keep trying to redeem
            if not forever_mode and p_bal < MIN_BALANCE_TO_TRADE:
                print(f"\n  [STOPPED] Polymarket balance (${p_bal:.2f}) dropped below ${MIN_BALANCE_TO_TRADE:.0f}")
                break

            # Check average profit percentage - stop if it drops below threshold after minimum trades
            total_trades = trader.stats.successful_trades + trader.stats.partial_fills
            if not logging_only and not testing_logs_mode and total_trades >= MIN_TRADES_BEFORE_AVG_CHECK:
                if trader.stats.total_invested > 0:
                    avg_profit_pct = (trader.stats.realized_pnl / trader.stats.total_invested) * 100
                    if avg_profit_pct < MIN_AVG_PROFIT_PCT:
                        print(f"\n  [STOPPED] Average profit {avg_profit_pct:.2f}% < {MIN_AVG_PROFIT_PCT:.1f}% threshold after {total_trades} trades (incl. unwinds)")
                        break

            # Forever mode: Kalshi must stay above $20 threshold
            if forever_mode:
                from config import MIN_KALSHI_BALANCE_FOREVER
                if k_bal < MIN_KALSHI_BALANCE_FOREVER:
                    print(f"\n  [STOPPED] Forever mode: Kalshi balance (${k_bal:.2f}) dropped below ${MIN_KALSHI_BALANCE_FOREVER:.0f}")
                    break
                # For Poly, auto-redeem above handles it - keep running regardless of balance

            # TESTING mode kill-switches
            if testing_mode:
                current_balance = k_bal + p_bal
                # Update peak balance
                if current_balance > testing_peak_balance:
                    testing_peak_balance = current_balance

                # Check drawdown kill-switch
                drawdown = testing_peak_balance - current_balance
                if drawdown >= TESTING_MAX_DRAWDOWN:
                    print(f"\n  [KILL-SWITCH] Drawdown ${drawdown:.2f} >= ${TESTING_MAX_DRAWDOWN:.0f} limit - STOPPING")
                    break

                # Check unwind limit kill-switch
                if testing_unwind_count >= TESTING_MAX_UNWINDS:
                    print(f"\n  [KILL-SWITCH] Unwind count {testing_unwind_count} >= {TESTING_MAX_UNWINDS} limit - STOPPING")
                    break

                # Check cooldown (after consecutive losses)
                if testing_cooldown_until and time.time() < testing_cooldown_until:
                    remaining = int(testing_cooldown_until - time.time())
                    mins_left = remaining // 60
                    secs_left = remaining % 60
                    print(f"\n  [COOLDOWN] {mins_left}m {secs_left}s remaining after {TESTING_MAX_CONSECUTIVE_LOSSES} consecutive losses")
                    time.sleep(min(30, remaining))  # Sleep in 30-second chunks
                    continue

                # Reset cooldown if it expired
                if testing_cooldown_until and time.time() >= testing_cooldown_until:
                    testing_cooldown_until = None
                    testing_consecutive_losses = 0
                    print(f"\n  [COOLDOWN] Cooldown ended - resuming trading")

            # Check if we're in the final 1.5 minutes - just wait, no scanning
            if testing_mode or testing_logs_mode:
                window_progress = get_window_progress()
                if window_progress >= TESTING_FINAL_WINDOW_START:
                    # Initialize waiting dots counter if needed
                    if not hasattr(main, '_waiting_dots'):
                        main._waiting_dots = 1
                        main._waiting_started = False

                    # Print newline only on first wait message
                    if not main._waiting_started:
                        print()  # Start on new line
                        main._waiting_started = True

                    # Format time nicely (Xm Ys)
                    secs_left = (1.0 - window_progress) * 15 * 60
                    mins = int(secs_left // 60)
                    secs = int(secs_left % 60)
                    dots = "." * main._waiting_dots

                    # Use \r to overwrite line, pad with spaces to clear old text
                    msg = f"  Waiting ({mins}m {secs}s){dots}"
                    print(f"\r{msg:<40}", end="", flush=True)

                    # Cycle dots: 1 -> 2 -> 3 -> 1...
                    main._waiting_dots = (main._waiting_dots % 3) + 1

                    time.sleep(1)
                    continue  # Skip the rest of the scan loop
                else:
                    # Reset waiting state when not in final window
                    if hasattr(main, '_waiting_started') and main._waiting_started:
                        print()  # Move to new line after waiting ends
                    main._waiting_dots = 1
                    main._waiting_started = False

            scan_num += 1
            trader.stats.scans = scan_num

            # Scan header
            print(f"\n  --- SCAN #{scan_num} | {now_mst()} ---")
            if not logging_only and not testing_logs_mode:
                if forever_mode:
                    print(f"  Successful: {trader.stats.successful_trades} (forever mode)")
                else:
                    print(f"  Successful: {trader.stats.successful_trades}/{target_trades}")
            elif testing_logs_mode:
                print(f"  Opportunities logged: {trader.stats.opportunities_found} (testing logs mode)")

            # Track market window changes
            from logging_utils import get_current_market_window
            current_window = get_current_market_window()

            # If window changed, wait for markets to update before fetching
            if not hasattr(main, '_last_market_window'):
                main._last_market_window = current_window
                main._last_kalshi_strikes = {}  # Track previous window's Kalshi strikes
            elif current_window != main._last_market_window:
                print(f"\n  [MARKET SWITCH] New window detected, waiting 60s for markets to update...")
                time.sleep(60)  # Wait 1 minute for markets to have new prices

                # Save previous Kalshi strikes to detect stale data
                previous_strikes = main._last_kalshi_strikes.copy()

                main._last_market_window = current_window
                # Clear spread cache and reset flags for new window
                spread_failed_cryptos.clear()
                crypto_market_spend.clear()  # Reset per-crypto spend for new window
                window_contracts_traded.clear()  # Reset per-crypto contract cap for new window
                all_skipped_logged = False

                # Try to fetch prices, with faster retries (Chainlink fallback available)
                retry_wait = 3  # seconds between retries (faster with Chainlink fallback)
                max_retries = 10
                attempt = 0

                while attempt < max_retries:
                    attempt += 1
                    print(f"  [MARKET SWITCH] Fetching prices (attempt {attempt}/{max_retries})...")
                    snapshots = fetcher.fetch_all(cryptos)

                    # Check if we got all VALID prices (both exist and within 3% of each other)
                    all_prices_ready = True
                    missing = []
                    stale = []
                    for crypto in cryptos:
                        snap = snapshots.get(crypto)
                        if not snap or not snap.kalshi_strike or not snap.poly_strike:
                            all_prices_ready = False
                            missing.append(crypto)
                        else:
                            # Check if Kalshi price is exactly the same as last window (stale data)
                            prev_strike = previous_strikes.get(crypto)
                            if prev_strike is not None and snap.kalshi_strike == prev_strike:
                                all_prices_ready = False
                                stale.append(f"{crypto}(K${snap.kalshi_strike:.2f}=STALE)")
                            # Also check if prices are within 3% (otherwise likely bad data)
                            else:
                                avg_price = (snap.kalshi_strike + snap.poly_strike) / 2
                                pct_diff = abs(snap.kalshi_strike - snap.poly_strike) / avg_price * 100
                                if pct_diff > 3.0:
                                    all_prices_ready = False
                                    missing.append(f"{crypto}(K${snap.kalshi_strike:.0f}/P${snap.poly_strike:.0f}={pct_diff:.0f}%)")

                    if all_prices_ready:
                        print(f"  [MARKET SWITCH] All prices ready!")
                        # Update stored Kalshi strikes for next window comparison
                        for crypto in cryptos:
                            snap = snapshots.get(crypto)
                            if snap and snap.kalshi_strike:
                                main._last_kalshi_strikes[crypto] = snap.kalshi_strike
                        break
                    else:
                        issues = missing + stale
                        if attempt < max_retries:
                            print(f"  [MARKET SWITCH] Issues: {issues}, retrying in {retry_wait}s...")
                            time.sleep(retry_wait)
                        else:
                            print(f"  [MARKET SWITCH] Some prices still have issues after {max_retries} attempts, proceeding anyway...")
                            # Still update strikes even if not perfect
                            for crypto in cryptos:
                                snap = snapshots.get(crypto)
                                if snap and snap.kalshi_strike:
                                    main._last_kalshi_strikes[crypto] = snap.kalshi_strike

                # Log prices - new prices are both:
                # 1. The previous window's FINAL price (current start = previous end)
                # 2. The current window's price to beat
                for crypto in cryptos:
                    snap = snapshots.get(crypto)
                    if snap and snap.kalshi_strike and snap.poly_strike:
                        # Update previous window's finals with these new prices
                        historical._finalize_crypto(crypto, snap.kalshi_strike, snap.poly_strike)
                        # Log the new window's prices for historical tracking
                        historical.record_prices(crypto, snap.kalshi_strike, snap.poly_strike)

                # Continue to next scan iteration
                continue

            # Normal fetch for non-transition scans
            # TESTING mode and TESTING LOGS: Skip fetching cryptos that failed spread check this window
            if testing_mode or testing_logs_mode:
                skipped_spread = [c for c in cryptos if c in spread_failed_cryptos and spread_failed_cryptos[c] == current_window]
                cryptos_to_fetch = [c for c in cryptos if c not in spread_failed_cryptos or spread_failed_cryptos[c] != current_window]
            else:
                cryptos_to_fetch = cryptos

            # If all cryptos are skipped, show waiting message instead of full scan
            if not cryptos_to_fetch:
                # Show "all skipped" message only once
                if not all_skipped_logged:
                    print(f"\n  [SPREAD] All cryptos below threshold - waiting for next window")
                    all_skipped_logged = True

                # Calculate time until next window
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                mins_in_window = now.minute % 15
                secs_in_window = mins_in_window * 60 + now.second
                secs_until_next = (15 * 60) - secs_in_window
                mins_left = secs_until_next // 60
                secs_left = secs_until_next % 60

                # Show compact waiting message (overwrites previous line)
                dots = "." * ((scan_num % 3) + 1)
                print(f"\r  Waiting ({mins_left}m {secs_left}s){dots}   ", end="", flush=True)
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # We have cryptos to scan - print full header
            print()  # New line after any waiting dots

            snapshots = fetcher.fetch_all(cryptos_to_fetch)

            # Analyze all cryptos first, collect GO opportunities
            opportunities = []  # List of (crypto, snap, dir, edge, breakdown, raw_edge)

            for crypto in cryptos:
                # TESTING mode and TESTING LOGS: Skip cryptos that failed spread check this window
                if (testing_mode or testing_logs_mode) and crypto in spread_failed_cryptos:
                    if spread_failed_cryptos[crypto] == current_window:
                        continue  # Already logged once, skip silently

                snap = snapshots.get(crypto)
                if not snap:
                    continue

                # TESTING mode and TESTING LOGS: Always use best edge direction (like logging mode)
                # Other modes: Use allowed direction based on strike prices
                if testing_mode or testing_logs_mode:
                    # TESTING doesn't care about up/down - just find best edge
                    best_dir, edge_pct, other_edge, breakdown = pick_best_direction(snap)
                    # raw_edge is now the FULL edge with fees+gas+buffer (the real edge)
                    raw_edge = edge_pct
                    if best_dir == "K_UP+P_DOWN":
                        k_price, p_price = snap.k_up, snap.p_down
                    else:
                        k_price, p_price = snap.k_down, snap.p_up
                    allowed_dir = best_dir
                else:
                    # Get allowed direction based on strike prices
                    # Lower strike = bet UP, Higher strike = bet DOWN
                    allowed_dir = snap.allowed_direction()

                    # Calculate edge ONLY for the allowed direction
                    if allowed_dir == "K_UP+P_DOWN":
                        # Kalshi UP (lower strike) + Poly DOWN (higher strike)
                        k_price, p_price = snap.k_up, snap.p_down
                    elif allowed_dir == "K_DOWN+P_UP":
                        # Kalshi DOWN (higher strike) + Poly UP (lower strike)
                        k_price, p_price = snap.k_down, snap.p_up
                    else:
                        # ANY or NONE - use best direction as fallback
                        best_dir, edge_pct, other_edge, breakdown = pick_best_direction(snap)
                        raw_edge = edge_pct  # Full edge with fees+gas+buffer
                        if best_dir == "K_UP+P_DOWN":
                            k_price, p_price = snap.k_up, snap.p_down
                        else:
                            k_price, p_price = snap.k_down, snap.p_up
                        allowed_dir = best_dir  # Use best for display

                # Get full breakdown for trading (edge_pct now includes fees+gas+buffer)
                if allowed_dir in ["K_UP+P_DOWN", "K_DOWN+P_UP"]:
                    best_dir, edge_pct, other_edge, breakdown = pick_best_direction(snap)
                    # raw_edge is now the FULL edge with fees+gas+buffer
                    raw_edge = edge_pct

                best_dir = allowed_dir  # Force use of allowed direction

                # Record prices for historical analysis
                historical.record_prices(crypto, snap.kalshi_strike, snap.poly_strike)

                # TESTING mode and TESTING LOGS: Check spread FIRST (before edge)
                # This allows us to cache failed cryptos and skip fetching on future scans
                if testing_mode or testing_logs_mode:
                    window_progress = get_window_progress()
                    is_final_window = window_progress >= TESTING_FINAL_WINDOW_START
                    is_late_window = window_progress >= TESTING_LATE_WINDOW_START

                    # SPECIAL CASE: Penny spread (strikes within $0.01) - NO RESTRICTIONS, just 11% edge
                    strike_diff = abs(snap.kalshi_strike - snap.poly_strike)
                    is_penny_spread = strike_diff <= 0.01

                    if is_penny_spread:
                        # Penny spread: Only check 11% edge, no other restrictions
                        if raw_edge < active_min_edge:
                            print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [PENNY SPREAD - EDGE<{active_min_edge}%]")
                            continue
                        # Penny spread with good edge - GO with no limits!
                        status = "GO"
                        is_final = False  # No half-qty for penny spreads
                        opportunities.append((crypto, snap, best_dir, edge_pct, breakdown, raw_edge, is_final))
                        print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [PENNY SPREAD - GO NO LIMITS]")
                        continue

                    # Normal case: Strike spread check FIRST (difference between Kalshi and Poly strike prices)
                    spread_pct = calc_spread_pct(snap.kalshi_strike, snap.poly_strike)
                    if is_late_window:
                        min_spread = TESTING_LATE_SPREAD_PCT.get(crypto, 0.001)
                    else:
                        min_spread = TESTING_MIN_SPREAD_PCT.get(crypto, 0.0005)

                    if spread_pct >= min_spread:
                        # Cache this crypto as failed for this window - won't check again
                        spread_failed_cryptos[crypto] = current_window
                        print(f"  {crypto}: Strike spread {spread_pct*100:.3f}% >= {min_spread*100:.2f}% - SKIPPING for window")
                        continue

                    # No trading in last 3 minutes (final window)
                    if TESTING_NO_TRADE_FINAL_WINDOW and is_final_window:
                        mins_left = (1.0 - window_progress) * 15
                        print(f"  {crypto}: NO TRADING - Last {mins_left:.1f} min of window")
                        continue

                    # Check per-crypto market spend limit ($50 max per crypto per window)
                    current_spend = crypto_market_spend.get(crypto, 0.0)
                    if current_spend >= TESTING_MAX_COST_PER_CRYPTO_MARKET:
                        print(f"  {crypto}: LIMIT REACHED - ${current_spend:.2f}/${TESTING_MAX_COST_PER_CRYPTO_MARKET:.0f} spent this window")
                        continue

                    # Check per-crypto window contract cap
                    crypto_contracts = window_contracts_traded.get(crypto, 0)
                    if MAX_CONTRACTS_PER_WINDOW_ENABLED and crypto_contracts >= MAX_CONTRACTS_PER_WINDOW:
                        print(f"  {crypto}: CONTRACT CAP - {crypto_contracts}/{MAX_CONTRACTS_PER_WINDOW} contracts traded this window")
                        continue

                    # Edge check (after spread passes)
                    if testing_logs_mode:
                        # TESTING LOGS mode: use 11% edge threshold
                        if raw_edge < active_min_edge:
                            print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [EDGE<{active_min_edge}%]")
                            continue
                    else:
                        # TESTING mode: Minimum edge based on window progress (3 tiers)
                        if is_final_window:
                            if raw_edge < TESTING_MIN_EDGE_FINAL:
                                print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [EDGE<{TESTING_MIN_EDGE_FINAL}% (final)]")
                                continue
                        elif is_late_window:
                            if raw_edge < TESTING_MIN_EDGE_LATE:
                                print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [EDGE<{TESTING_MIN_EDGE_LATE}%]")
                                continue
                        else:
                            if raw_edge < TESTING_MIN_EDGE_NORMAL:
                                print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [EDGE<{TESTING_MIN_EDGE_NORMAL}%]")
                                continue

                    # All checks passed - this is a GO opportunity
                    status = "GO"
                    is_final = is_final_window if testing_mode else False
                    opportunities.append((crypto, snap, best_dir, edge_pct, breakdown, raw_edge, is_final))
                    print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}% [GO]")
                    continue  # Already printed, skip the print at end

                # Non-TESTING modes
                if raw_edge < active_min_edge:
                    status = "skip"
                elif raw_edge >= MAX_NET_EDGE_TO_TRADE_PCT:
                    status = "outlier"
                elif not snap.strikes_match(max_diff_pct=3.0):
                    if snap.kalshi_strike is None or snap.poly_strike is None:
                        status = "NO_STRIKE"
                    else:
                        status = "STRIKE>3%"
                elif allowed_dir == "NONE":
                    status = "NO_STRIKE"
                else:
                    status = "GO"
                    is_final = False
                    opportunities.append((crypto, snap, best_dir, edge_pct, breakdown, raw_edge, is_final))

                print(f"  {crypto}: K({snap.k_up:.2f}/{snap.k_down:.2f}) P({snap.p_up:.2f}/{snap.p_down:.2f}) Edge:{raw_edge:.1f}%")

            # Sort opportunities by edge (best first)
            opportunities.sort(key=lambda x: x[5], reverse=True)

            # Trade best opportunity, then re-scan all cryptos fresh
            for i, (crypto, snap, best_dir, edge_pct, breakdown, raw_edge, is_final) in enumerate(opportunities):
                # Check stop condition
                if not logging_only and not testing_logs_mode and not forever_mode and not testing_mode and (trader.stats.successful_trades + trader.stats.partial_fills) >= target_trades:
                    break

                # TESTING mode: Skip new trades after reaching target (but keep scanning)
                if testing_trades_complete:
                    break

                if len(opportunities) > 1:
                    if logging_only or testing_logs_mode:
                        print(f"\n  [BEST EDGE] Logging {crypto} ({raw_edge:.1f}% edge)")
                    elif is_final:
                        print(f"\n  [BEST EDGE] Trading {crypto} ({raw_edge:.1f}% edge) [FINAL WINDOW - half qty]")
                    else:
                        print(f"\n  [BEST EDGE] Trading {crypto} ({raw_edge:.1f}% edge)")

                # Execute
                if logging_only or testing_logs_mode:
                    trader.log_opportunity(snap, best_dir, edge_pct, breakdown)
                    # TESTING LOGS: Track simulated $50 per crypto spend limit (skip for penny spread)
                    if testing_logs_mode:
                        strike_diff = abs(snap.kalshi_strike - snap.poly_strike)
                        is_penny_spread = strike_diff <= 0.01
                        if is_penny_spread:
                            print(f"  [PENNY SPREAD] {crypto}: No spend limit - trade as much as you want!")
                        else:
                            # Estimate trade cost based on prices (similar to TESTING_MAX_COST_PER_TRADE = $25)
                            if best_dir == "K_UP+P_DOWN":
                                k_price, p_price = snap.k_up, snap.p_down
                            else:
                                k_price, p_price = snap.k_down, snap.p_up
                            # Simulate a trade at ~$25 (max cost per trade)
                            simulated_cost = min(TESTING_MAX_COST_PER_CRYPTO_MARKET - crypto_market_spend.get(crypto, 0.0), 25.0)
                            if simulated_cost > 0:
                                crypto_market_spend[crypto] = crypto_market_spend.get(crypto, 0.0) + simulated_cost
                                print(f"  [SIMULATED SPEND] {crypto}: ${simulated_cost:.2f} this trade, ${crypto_market_spend[crypto]:.2f}/${TESTING_MAX_COST_PER_CRYPTO_MARKET:.0f} total this window")
                else:
                    # Track unwinds before trade for kill-switch
                    unwinds_before = trader.stats.partial_fills
                    # Track invested amount before trade for per-crypto spend tracking
                    invested_before = trader.stats.total_invested

                    success, actual_edge, last_p_buffer = trader.try_trade(snap, best_dir, edge_pct, breakdown, testing_mode=testing_mode, final_window=is_final)

                    # Update per-crypto market spend tracking (skip for penny spread)
                    if testing_mode:
                        strike_diff = abs(snap.kalshi_strike - snap.poly_strike)
                        is_penny_spread = strike_diff <= 0.01
                        if is_penny_spread:
                            print(f"  [PENNY SPREAD] {crypto}: No spend limit tracked")
                        else:
                            trade_cost = trader.stats.total_invested - invested_before
                            if trade_cost > 0:
                                crypto_market_spend[crypto] = crypto_market_spend.get(crypto, 0.0) + trade_cost
                                print(f"  [SPEND] {crypto}: ${trade_cost:.2f} this trade, ${crypto_market_spend[crypto]:.2f}/${TESTING_MAX_COST_PER_CRYPTO_MARKET:.0f} total this window")

                    # Track per-crypto window contract cap
                    if success and MAX_CONTRACTS_PER_WINDOW_ENABLED:
                        window_contracts_traded[crypto] = window_contracts_traded.get(crypto, 0) + trader.stats.last_filled_qty
                        print(f"  [WINDOW CONTRACTS] {crypto}: {window_contracts_traded[crypto]}/{MAX_CONTRACTS_PER_WINDOW} contracts traded this window")

                    # TESTING mode: Update kill-switch tracking
                    if testing_mode:
                        unwinds_after = trader.stats.partial_fills
                        if unwinds_after > unwinds_before:
                            testing_unwind_count += (unwinds_after - unwinds_before)
                            testing_consecutive_losses += 1
                            print(f"  [TESTING] Unwind count: {testing_unwind_count}, Consecutive losses: {testing_consecutive_losses}")

                            # Check consecutive loss limit
                            if testing_consecutive_losses >= TESTING_MAX_CONSECUTIVE_LOSSES:
                                testing_cooldown_until = time.time() + (TESTING_COOLDOWN_MINUTES * 60)
                                print(f"  [TESTING] {TESTING_MAX_CONSECUTIVE_LOSSES} consecutive losses - entering {TESTING_COOLDOWN_MINUTES}min cooldown")
                        elif success:
                            # Reset consecutive losses on success
                            testing_consecutive_losses = 0

                    if success:
                        if forever_mode:
                            print(f"\n  [SUCCESS] Trade #{trader.stats.successful_trades} complete!")
                        else:
                            print(f"\n  [SUCCESS] Trade {trader.stats.successful_trades}/{target_trades} complete!")
                        # Break to re-scan all cryptos immediately
                        break
                    else:
                        # Trade failed (NO FILL or unwind) - re-scan same crypto and retry once if edge still good
                        retry_snap = fetcher.fetch(crypto)
                        if retry_snap:
                            retry_dir, retry_edge, _, retry_breakdown = pick_best_direction(retry_snap)
                            retry_raw_edge = retry_breakdown.get("raw_edge_pct", retry_edge)

                            if retry_raw_edge >= MIN_EDGE_PCT and retry_dir == best_dir:
                                print(f"\n  [RETRY] Edge still {retry_raw_edge:.1f}% >= {MIN_EDGE_PCT}% - trying once more...")
                                unwinds_before_retry = trader.stats.partial_fills
                                invested_before_retry = trader.stats.total_invested
                                retry_success, _, _ = trader.try_trade(retry_snap, retry_dir, retry_edge, retry_breakdown, testing_mode=testing_mode)

                                # Update per-crypto market spend tracking for retry
                                if testing_mode:
                                    retry_trade_cost = trader.stats.total_invested - invested_before_retry
                                    if retry_trade_cost > 0:
                                        crypto_market_spend[crypto] = crypto_market_spend.get(crypto, 0.0) + retry_trade_cost
                                        print(f"  [SPEND] {crypto}: ${retry_trade_cost:.2f} this retry, ${crypto_market_spend[crypto]:.2f}/${TESTING_MAX_COST_PER_CRYPTO_MARKET:.0f} total this window")

                                # TESTING mode: Track retry results
                                if testing_mode:
                                    unwinds_after_retry = trader.stats.partial_fills
                                    if unwinds_after_retry > unwinds_before_retry:
                                        testing_unwind_count += (unwinds_after_retry - unwinds_before_retry)
                                        testing_consecutive_losses += 1
                                        if testing_consecutive_losses >= TESTING_MAX_CONSECUTIVE_LOSSES:
                                            testing_cooldown_until = time.time() + (TESTING_COOLDOWN_MINUTES * 60)
                                    elif retry_success:
                                        testing_consecutive_losses = 0

                                if retry_success:
                                    if forever_mode:
                                        print(f"\n  [RETRY SUCCESS] Trade #{trader.stats.successful_trades} complete!")
                                    else:
                                        print(f"\n  [RETRY SUCCESS] Trade {trader.stats.successful_trades}/{target_trades} complete!")
                        # After any trade attempt, break to re-scan all cryptos fresh
                        break

            # Wait before next scan
            # TESTING mode keeps running after reaching target (for position monitoring)
            if not logging_only and not testing_logs_mode and not forever_mode and not testing_mode and (trader.stats.successful_trades + trader.stats.partial_fills) >= target_trades:
                break

            time.sleep(SCAN_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\n  [STOPPED] User interrupted (Ctrl+C)")
    except Exception as e:
        import traceback
        print(f"\n\n  [CRASH] Unexpected error: {e}")
        print(f"  [CRASH] Traceback:")
        traceback.print_exc()
        print(f"\n  [CRASH] The program crashed - check the error above!")

    # Finalize historical data logging (wrapped to handle second Ctrl+C)
    try:
        historical.force_finalize_all()
    except KeyboardInterrupt:
        print("  [SKIP] Skipping finalization due to interrupt")
    except Exception as e:
        print(f"  [SKIP] Finalization error: {e}")

    # Write session summary to session file
    write_session_summary()

    # Summary
    trader.print_summary()

    # Calculate stats
    k_bal, p_bal = trader.get_balances()
    start_total = trader.stats.kalshi_start_balance + trader.stats.poly_start_balance
    end_total = k_bal + p_bal
    net_pnl = trader.stats.realized_pnl - trader.stats.unwind_losses

    # Simple summary
    summary = f"""
================================================================================
                        SESSION SUMMARY
================================================================================
Finished: {now_mst()}

Trades: {trader.stats.successful_trades} success, {trader.stats.partial_fills} unwinds
Net P&L: ${net_pnl:.2f}

Balances:
  Kalshi:     ${trader.stats.kalshi_start_balance:.2f} -> ${k_bal:.2f} ({k_bal - trader.stats.kalshi_start_balance:+.2f})
  Polymarket: ${trader.stats.poly_start_balance:.2f} -> ${p_bal:.2f} ({p_bal - trader.stats.poly_start_balance:+.2f})
  Total:      ${start_total:.2f} -> ${end_total:.2f} ({end_total - start_total:+.2f})

Log: {log_file}
================================================================================
"""
    write_summary(summary)


if __name__ == "__main__":
    main()
