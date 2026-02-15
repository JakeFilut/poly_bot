"""
Trade logging utilities - CSV format optimized for Google Sheets.
Includes:
- Cumulative all-time log (Profit_Loss_Total.txt)
- Per-session logs with timestamps
- Hourly rate calculations
"""

import csv
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

from config import TRADES_LOG_LIVE, TRADES_LOG_LOG, SUMMARY_LOG, LOG_DIR, CONSOLE_HIGHLIGHTS_LOG


# Current active log file - set by set_log_mode()
_current_log_file = TRADES_LOG_LIVE

# Session tracking
_session_start_time: Optional[float] = None
_session_log_file: Optional[str] = None
_session_pnl: float = 0.0
_session_trades: int = 0
_current_market_window: Optional[str] = None  # e.g., "2026-02-01_22:15"


# CSV header - clean columns for easy Google Sheets import
TRADE_LOG_HEADER = [
    "timestamp",
    "crypto",
    "direction",           # K_UP+P_DOWN or K_DOWN+P_UP
    "qty",
    # Which side we actually bet on each exchange
    "kalshi_side",         # YES or NO (the side we bought)
    "kalshi_fill",         # Per-contract fill price on Kalshi
    "poly_side",           # UP or DOWN (the side we bought)
    "poly_fill",           # Per-contract fill price on Poly
    # Cost breakdown
    "kalshi_fill_total",   # kalshi_fill × qty
    "poly_fill_total",     # poly_fill × qty
    "total_cost",          # kalshi_fill_total + poly_fill_total
    # Fees
    "kalshi_fee",          # Kalshi taker fee
    "poly_fee",            # Polymarket fee
    "total_fees",          # kalshi_fee + poly_fee + gas
    "total_bet",           # total_cost + total_fees (total amount risked)
    # Edge analysis
    "edge_expected_pct",   # Edge calculated before trade (at scan time)
    "edge_actual_pct",     # Edge achieved with actual fills
    # Unwind info
    "was_unwound",         # YES/NO
    "unwind_loss",         # How much we lost on unwind
    # Strike prices at time of bet
    "kalshi_strike",       # Kalshi strike price (CF Benchmarks)
    "poly_strike",         # Poly strike price (Chainlink)
    # End prices (filled at next window boundary)
    "kalshi_end_price",    # CF Benchmarks price at window end (blank until settled)
    "poly_end_price",      # Chainlink price at window end (blank until settled)
    # Results (filled at next window boundary)
    "kalshi_result",       # WON or LOST (blank until settled)
    "poly_result",         # WON or LOST (blank until settled)
    "pnl",                 # Actual profit/loss after settlement (blank until settled)
    # Status
    "status",              # SUCCESS, UNWIND, DOUBLE_LOSS, UNWIND_FAILED
]


def get_settlement_risk(direction: str, kalshi_strike, poly_strike) -> str:
    """
    Determine settlement risk for a trade based on direction and strike prices.

    Returns:
        SAFE_SAME   - Same strikes, no double loss possible
        SAFE_PENNY  - Strikes within $0.01, negligible risk
        SAFE_CROSS  - Betting into the gap (both can win if price between strikes)
        AT_RISK     - Betting against the gap (both can lose if price between strikes)
    """
    if kalshi_strike is None or poly_strike is None:
        return "UNKNOWN"

    try:
        k = float(kalshi_strike)
        p = float(poly_strike)
    except (ValueError, TypeError):
        return "UNKNOWN"

    diff = abs(k - p)

    if diff == 0:
        return "SAFE_SAME"
    if diff <= 0.01:
        return "SAFE_PENNY"

    # Check if direction bets INTO the gap (safe) or AGAINST it (at risk)
    if direction == "K_UP+P_DOWN":
        # K_UP wins if final >= k_strike, P_DOWN wins if final < p_strike
        # Safe if k_strike < p_strike (gap between = both win zone)
        if k < p:
            return "SAFE_CROSS"
        else:
            return "AT_RISK"
    elif direction == "K_DOWN+P_UP":
        # K_DOWN wins if final < k_strike, P_UP wins if final >= p_strike
        # Safe if p_strike < k_strike (gap between = both win zone)
        if p < k:
            return "SAFE_CROSS"
        else:
            return "AT_RISK"

    return "UNKNOWN"


def set_log_mode(logging_only: bool) -> str:
    """
    Set the log mode (logging vs trading).
    Returns the path to the log file being used.
    """
    global _current_log_file
    if logging_only:
        _current_log_file = TRADES_LOG_LOG
    else:
        _current_log_file = TRADES_LOG_LIVE
    return _current_log_file


def get_current_log_file() -> str:
    """Get the current log file path."""
    return _current_log_file


def _format_value(v: Any) -> str:
    """Format value for CSV - numbers without excessive decimals."""
    if v is None:
        return ""
    if isinstance(v, float):
        # Use 4 decimals for prices, 2 for costs/balances
        if abs(v) < 1:
            return f"{v:.4f}"
        return f"{v:.2f}"
    return str(v)


def _ensure_header(filepath: str, header: list) -> None:
    """Ensure CSV file has header row."""
    try:
        if not os.path.exists(filepath):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(header)
        else:
            # Check if empty
            with open(filepath, "r", encoding="utf-8") as f:
                if not f.read(1):
                    with open(filepath, "w", newline="", encoding="utf-8") as fw:
                        csv.writer(fw).writerow(header)
    except PermissionError:
        print(f"[LOG] Warning: Cannot access {filepath} - file may be open in another program")


def log_trade_row(row: Dict[str, Any], retries: int = 3) -> bool:
    """
    Log a trade row to CSV file.
    Uses the current log file set by set_log_mode().
    Returns True if successful, False otherwise.
    """
    filepath = _current_log_file

    for attempt in range(retries):
        try:
            _ensure_header(filepath, TRADE_LOG_HEADER)
            with open(filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([_format_value(row.get(h, "")) for h in TRADE_LOG_HEADER])
            return True
        except PermissionError:
            if attempt < retries - 1:
                print(f"[LOG] File locked, retrying in 1s... ({attempt + 1}/{retries})")
                time.sleep(1)
            else:
                print(f"[LOG] ERROR: Cannot write to {filepath}")
                print(f"[LOG] Please close the file if it's open in Excel/another program")
                # Try to log to a backup file
                backup_file = filepath.replace(".csv", f"_backup_{int(time.time())}.csv")
                try:
                    _ensure_header(backup_file, TRADE_LOG_HEADER)
                    with open(backup_file, "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([_format_value(row.get(h, "")) for h in TRADE_LOG_HEADER])
                    print(f"[LOG] Wrote to backup: {backup_file}")
                    return True
                except Exception as e:
                    print(f"[LOG] Backup also failed: {e}")
                return False
        except Exception as e:
            print(f"[LOG] Error writing to log: {e}")
            return False
    return False


def write_summary(text: str) -> None:
    """Write summary text to file."""
    os.makedirs(os.path.dirname(SUMMARY_LOG), exist_ok=True)
    with open(SUMMARY_LOG, "w", encoding="utf-8") as f:
        f.write(text)


# =============================================================================
# SESSION MANAGEMENT (15-minute market windows)
# =============================================================================

def get_current_market_window() -> str:
    """
    Get the current 15-minute market window identifier.
    Markets run at :00, :15, :30, :45 each hour.
    Returns format: "2026-02-01_22-15" (filesystem safe)
    """
    now = datetime.now()
    # Round down to nearest 15 minutes
    minute_window = (now.minute // 15) * 15
    window_time = now.replace(minute=minute_window, second=0, microsecond=0)
    return window_time.strftime("%Y-%m-%d_%H-%M")


def get_current_1h_market_window() -> str:
    """
    Get the current 1-hour market window identifier.
    1-hour markets run at :00 each hour.
    Returns format: "2026-02-01_22-00" (filesystem safe)
    """
    now = datetime.now()
    window_time = now.replace(minute=0, second=0, microsecond=0)
    return window_time.strftime("%Y-%m-%d_%H-00")


def start_session() -> str:
    """
    Start a new logging session for the current 15-minute market window.
    NOTE: No longer creates market_*.txt files - disabled per user request.
    Returns empty string (kept for compatibility).
    """
    global _session_start_time, _session_log_file, _session_pnl, _session_trades, _current_market_window

    _session_start_time = time.time()
    _session_pnl = 0.0
    _session_trades = 0
    _current_market_window = get_current_market_window()
    _session_log_file = None  # Disabled - no more market_*.txt files

    return ""


def check_market_window_rotation() -> Optional[str]:
    """
    Check if we've crossed into a new 15-minute market window.
    If so, close the current session and start a new one.
    Returns the new session file path if rotated, None otherwise.
    """
    global _current_market_window

    current_window = get_current_market_window()

    # If no session yet or same window, no rotation needed
    if _current_market_window is None:
        return start_session()

    if current_window == _current_market_window:
        return None

    # New market window - rotate session
    print(f"\n  [NEW MARKET] Window changed: {_current_market_window} -> {current_window}")

    # Write summary to old session
    write_session_summary()

    # Start new session
    new_file = start_session()
    print(f"  [SESSION] {new_file}")

    return new_file


def get_session_file() -> Optional[str]:
    """Get current session log file path."""
    return _session_log_file


def get_session_duration_hours() -> float:
    """Get session duration in hours."""
    if _session_start_time is None:
        return 0.0
    return (time.time() - _session_start_time) / 3600


def get_session_hourly_rate() -> float:
    """Calculate hourly earnings rate for this session."""
    hours = get_session_duration_hours()
    if hours < 0.01:  # Less than 36 seconds
        return 0.0
    return _session_pnl / hours


def update_session_stats(pnl: float) -> None:
    """Update session statistics with a new trade."""
    global _session_pnl, _session_trades
    _session_pnl += pnl
    _session_trades += 1


def get_session_stats() -> dict:
    """Get current session statistics."""
    hours = get_session_duration_hours()
    hourly_rate = get_session_hourly_rate() if hours >= 0.01 else 0.0

    return {
        "pnl": _session_pnl,
        "trades": _session_trades,
        "hours": hours,
        "hourly_rate": hourly_rate,
    }


def log_to_session(text: str) -> None:
    """Append text to the session log file."""
    if _session_log_file is None:
        return

    try:
        with open(_session_log_file, "a", encoding="utf-8") as f:
            f.write(text)
    except Exception as e:
        print(f"[LOG] Error writing to session file: {e}")


def write_session_summary() -> None:
    """Write final summary to session log file."""
    if _session_log_file is None:
        return

    stats = get_session_stats()
    hours = stats["hours"]
    minutes = hours * 60

    summary = "\n" + "=" * 70 + "\n"
    summary += "  SESSION SUMMARY\n"
    summary += "=" * 70 + "\n"
    summary += f"  Duration:     {int(hours)}h {int(minutes % 60)}m\n"
    summary += f"  Trades:       {stats['trades']}\n"
    summary += f"  Net P&L:      ${stats['pnl']:+.2f}\n"
    if hours >= 0.01:
        summary += f"  Hourly Rate:  ${stats['hourly_rate']:+.2f}/hr\n"
    summary += "=" * 70 + "\n"

    log_to_session(summary)


def get_last_running_total() -> float:
    """
    Read the last running total from the Profit_Loss_Total.txt file.
    Returns 0.0 if file doesn't exist or can't be parsed.
    """
    pnl_file = os.path.join(os.path.dirname(SUMMARY_LOG), "Profit_Loss_Total.txt")
    if not os.path.exists(pnl_file):
        return 0.0

    try:
        with open(pnl_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Find all "Running Total: $X.XX" patterns
        import re
        matches = re.findall(r"Running Total: \$([+-]?\d+\.?\d*)", content)
        if matches:
            return float(matches[-1])  # Return the last one
        return 0.0
    except Exception:
        return 0.0


def get_pnl_stats() -> dict:
    """
    Parse the Profit_Loss_Total.txt file and calculate summary statistics.
    Returns dict with total_traded, total_earned, trade_count, avg_pct, hours, hourly_rate
    """
    import re
    pnl_file = os.path.join(os.path.dirname(SUMMARY_LOG), "Profit_Loss_Total.txt")
    if not os.path.exists(pnl_file):
        return {"total_traded": 0.0, "total_earned": 0.0, "trade_count": 0, "avg_pct": 0.0, "hours": 0.0, "hourly_rate": 0.0}

    try:
        with open(pnl_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Find all "Cost: $X.XX" entries
        costs = re.findall(r"Cost: \$(\d+\.?\d*)", content)
        total_traded = sum(float(c) for c in costs)

        # Find all "P&L: $X.XXX" entries (including negative)
        pnls = re.findall(r"P&L: \$([+-]?\d+\.?\d*)", content)
        total_earned = sum(float(p) for p in pnls)

        trade_count = len(costs)

        # Calculate average % return per trade
        if total_traded > 0 and trade_count > 0:
            avg_pct = (total_earned / total_traded) * 100
        else:
            avg_pct = 0.0

        # Calculate hours from first to last timestamp
        # Try new MST format first: [Feb 06 2026 03 10]
        timestamps = re.findall(r'\[([A-Z][a-z]{2} \d{2} \d{4} \d{2} \d{2})\]', content)
        hours = 0.0
        hourly_rate = 0.0
        if len(timestamps) >= 2:
            try:
                first_ts = datetime.strptime(timestamps[0], "%b %d %Y %H %M")
                last_ts = datetime.strptime(timestamps[-1], "%b %d %Y %H %M")
                hours = (last_ts - first_ts).total_seconds() / 3600
                if hours >= 0.01:
                    hourly_rate = total_earned / hours
            except Exception:
                pass

        # Fallback: try old ISO format [2026-02-05T14:32:45]
        if hours < 0.01:
            iso_timestamps = re.findall(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', content)
            if len(iso_timestamps) >= 2:
                try:
                    first_ts = datetime.fromisoformat(iso_timestamps[0])
                    last_ts = datetime.fromisoformat(iso_timestamps[-1])
                    hours = (last_ts - first_ts).total_seconds() / 3600
                    if hours >= 0.01:
                        hourly_rate = total_earned / hours
                except Exception:
                    pass

        return {
            "total_traded": total_traded,
            "total_earned": total_earned,
            "trade_count": trade_count,
            "avg_pct": avg_pct,
            "hours": hours,
            "hourly_rate": hourly_rate
        }
    except Exception:
        return {"total_traded": 0.0, "total_earned": 0.0, "trade_count": 0, "avg_pct": 0.0, "hours": 0.0, "hourly_rate": 0.0}


def update_pnl_header() -> None:
    """
    Update the header of Profit_Loss_Total.txt with current statistics.
    """
    import re
    pnl_file = os.path.join(os.path.dirname(SUMMARY_LOG), "Profit_Loss_Total.txt")
    if not os.path.exists(pnl_file):
        return

    stats = get_pnl_stats()

    try:
        with open(pnl_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Build new header
        new_header = "=" * 70 + "\n"
        new_header += "  ARBITRAGE BOT - PROFIT & LOSS TOTAL\n"
        new_header += "=" * 70 + "\n"
        new_header += f"  Total Trades:    {stats['trade_count']}\n"
        new_header += f"  Money Traded:    ${stats['total_traded']:.2f}\n"
        new_header += f"  Money Earned:    ${stats['total_earned']:+.2f}\n"
        new_header += f"  Avg % Per Trade: {stats['avg_pct']:+.2f}%\n"
        if stats['hours'] >= 0.01:
            new_header += f"  Total Hours:     {stats['hours']:.1f}h\n"
            new_header += f"  Hourly Rate:     ${stats['hourly_rate']:+.2f}/hr\n"
        new_header += "=" * 70 + "\n\n"

        # Find where the header ends (after the first set of === lines and stats)
        # Look for the pattern that marks start of trade entries [202X-
        trade_start = re.search(r'\n\[202\d-', content)
        if trade_start:
            # Keep everything from the first trade entry onwards
            trades_content = content[trade_start.start():]
            new_content = new_header + trades_content.lstrip('\n')
        else:
            # No trades yet, just update header
            new_content = new_header

        with open(pnl_file, "w", encoding="utf-8") as f:
            f.write(new_content)

    except Exception as e:
        print(f"[LOG] Error updating pnl header: {e}")


def append_pnl_summary(
    timestamp: str,
    crypto: str,
    direction: str,
    status: str,
    qty: int,
    cost: float,
    pnl: float,
    loss_reason: str = None,
    running_total: float = None,
    raw_edge: float = None,
    actual_edge: float = None,
    kalshi_strike: float = None,
    poly_strike: float = None,
) -> None:
    """
    Append a trade result to BOTH the cumulative P&L summary file AND the session log.
    Also updates session statistics for hourly rate calculation.

    Args:
        timestamp: When the trade happened
        crypto: BTC, ETH, etc.
        direction: K_UP+P_DOWN or K_DOWN+P_UP
        status: SUCCESS, FAILED, UNWIND_LOSS, UNSOLD_LOSS
        qty: Number of contracts
        cost: Total cost of the trade
        pnl: Profit/loss from this trade (negative for losses)
        loss_reason: Why it failed (if applicable)
        running_total: Cumulative P&L so far
        raw_edge: Raw edge % before buffers/fees
        actual_edge: Actual edge % achieved
        kalshi_strike: Kalshi strike price for this trade
        poly_strike: Poly strike price for this trade
    """
    # Update session stats
    update_session_stats(pnl)

    pnl_file = os.path.join(os.path.dirname(SUMMARY_LOG), "Profit_Loss_Total.txt")
    os.makedirs(os.path.dirname(pnl_file), exist_ok=True)

    # Create header if file doesn't exist
    if not os.path.exists(pnl_file):
        with open(pnl_file, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("  ARBITRAGE BOT - PROFIT & LOSS TOTAL\n")
            f.write("=" * 70 + "\n\n")

    # Calculate running total from file (persists across sessions)
    last_total = get_last_running_total()
    new_running_total = last_total + pnl

    # Format the entry
    pnl_str = f"${pnl:+.3f}" if pnl else "$0.000"
    # Show ❌ for losses (negative P&L), ✅ for profits
    if pnl < 0:
        status_emoji = "❌"
    else:
        status_emoji = "✅"

    # Build the log entry
    entry = f"[{timestamp}] {status_emoji} {status}\n"
    entry += f"  {crypto} | {direction} | {qty} contracts | Cost: ${cost:.2f}\n"

    # Add strike prices and gap (danger zone for "in between" losses)
    if kalshi_strike is not None and poly_strike is not None:
        low_strike = min(kalshi_strike, poly_strike)
        high_strike = max(kalshi_strike, poly_strike)
        gap = high_strike - low_strike
        entry += f"  Strikes: K${kalshi_strike:,.2f} / P${poly_strike:,.2f} | Gap: ${gap:,.2f} (loss zone)\n"

    entry += f"  P&L: {pnl_str}"

    # Add edge info if provided
    if raw_edge is not None and actual_edge is not None:
        entry += f" | Raw Edge: {raw_edge:.1f}% | Actual Edge: {actual_edge:.1f}%"
    elif actual_edge is not None:
        entry += f" | Actual Edge: {actual_edge:.1f}%"

    if loss_reason:
        entry += f" ({loss_reason})"

    entry += f" | Running Total: ${new_running_total:+.2f}\n\n"

    # Write to cumulative Profit_Loss_Total.txt
    with open(pnl_file, "a", encoding="utf-8") as f:
        f.write(entry)

    # Also write to session log with session-specific stats
    session_stats = get_session_stats()
    session_entry = f"[{timestamp}] {status_emoji} {status}\n"
    session_entry += f"  {crypto} | {direction} | {qty}x | Cost: ${cost:.2f} | P&L: {pnl_str}"
    if raw_edge is not None and actual_edge is not None:
        session_entry += f" | Raw: {raw_edge:.1f}% Actual: {actual_edge:.1f}%"
    session_entry += "\n"
    session_entry += f"  Session: ${session_stats['pnl']:+.2f}"
    if session_stats['hours'] >= 0.01:
        session_entry += f" | Rate: ${session_stats['hourly_rate']:+.2f}/hr"
    session_entry += f" | All-Time: ${new_running_total:+.2f}\n\n"
    log_to_session(session_entry)

    # Update the header with current stats
    update_pnl_header()


def log_unsold_position_loss(
    timestamp: str,
    crypto: str,
    token_id: str,
    qty: int,
    buy_price: float,
) -> float:
    """
    Log an unsold position as a total loss.
    Returns the loss amount.
    """
    loss = buy_price * qty  # Consider entire cost as loss

    pnl_file = os.path.join(os.path.dirname(SUMMARY_LOG), "Profit_Loss_Total.txt")
    os.makedirs(os.path.dirname(pnl_file), exist_ok=True)

    with open(pnl_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] ❌ UNSOLD_POSITION_LOSS\n")
        f.write(f"  {crypto} | Could not sell {qty} contracts @ ${buy_price:.4f}\n")
        f.write(f"  Token: ...{token_id[-16:]}\n")
        f.write(f"  TOTAL LOSS: ${-loss:.2f} (position abandoned)\n\n")

    return loss


# =============================================================================
# POTENTIAL TRADES LOGGING (2%-4.9% edge opportunities)
# =============================================================================

# Header for potential trades CSV
POTENTIAL_TRADES_HEADER = [
    "timestamp",
    "crypto",
    "direction",
    "qty",
    "k_yes_price",
    "k_no_price",
    "p_up_price",
    "p_down_price",
    "raw_edge_pct",
    "traded",
]


def log_potential_trade(row: Dict[str, Any], retries: int = 3) -> bool:
    """
    Log a potential trade (2%-4.9% edge) to CSV file.
    Returns True if successful, False otherwise.
    """
    from config import POTENTIAL_TRADES_LOG

    filepath = POTENTIAL_TRADES_LOG

    for attempt in range(retries):
        try:
            _ensure_header(filepath, POTENTIAL_TRADES_HEADER)
            with open(filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([_format_value(row.get(h, "")) for h in POTENTIAL_TRADES_HEADER])
            return True
        except PermissionError:
            if attempt < retries - 1:
                print(f"[LOG] Potential trades file locked, retrying in 1s... ({attempt + 1}/{retries})")
                time.sleep(1)
            else:
                print(f"[LOG] ERROR: Cannot write to {filepath}")
                return False
        except Exception as e:
            print(f"[LOG] Error writing to potential trades log: {e}")
            return False
    return False


def update_early_trades_threshold_status(cryptos: set, window_start_time: str) -> int:
    """
    Update SUCCESS status to SUCCESS/THRESHOLD for trades made during early window
    for cryptos that later failed the strike threshold check.

    Args:
        cryptos: Set of crypto symbols that failed threshold (e.g., {"BTC", "ETH"})
        window_start_time: Timestamp string of when the window started (for filtering)

    Returns:
        Number of entries updated
    """
    if not cryptos:
        return 0

    # Update the main trades log
    updated_count = 0

    try:
        # Read the current log file
        log_file = get_current_log_file()
        if not os.path.exists(log_file):
            return 0

        lines = []
        with open(log_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return 0
            lines.append(header)

            # Find indices
            try:
                crypto_idx = header.index('crypto')
                status_idx = header.index('status')
                timestamp_idx = header.index('timestamp')
            except ValueError:
                return 0  # Missing columns

            for row in reader:
                if len(row) > max(crypto_idx, status_idx, timestamp_idx):
                    # Check if this row should be updated
                    if (row[crypto_idx] in cryptos and
                        row[status_idx] == 'SUCCESS' and
                        row[timestamp_idx] >= window_start_time):
                        row[status_idx] = 'SUCCESS/THRESHOLD'
                        updated_count += 1
                lines.append(row)

        # Write back
        if updated_count > 0:
            with open(log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(lines)
            print(f"  [LOG] Updated {updated_count} early trades to SUCCESS/THRESHOLD for {cryptos}")

    except Exception as e:
        print(f"[LOG] Error updating threshold status: {e}")

    return updated_count


def update_trade_settlements(crypto: str, window_start_time: str,
                             kalshi_end_price: float, poly_end_price: float) -> int:
    """
    Fill in end prices, win/loss results, and actual P&L for trades from a
    completed window. Called at the next window boundary once both end prices
    are available (Kalshi strike from CF Benchmarks, Chainlink t+0s for Poly).

    For each unsettled SUCCESS trade of this crypto in the window:
      - Sets kalshi_end_price and poly_end_price
      - Determines kalshi_result and poly_result (WON/LOST)
      - Calculates actual pnl based on which sides won
      - Updates status to DOUBLE_LOSS if both sides lost

    Args:
        crypto: Crypto symbol (e.g., "BTC")
        window_start_time: Timestamp string of the window start
        kalshi_end_price: CF Benchmarks price (new window's Kalshi strike)
        poly_end_price: Chainlink t+0s price at window boundary

    Returns:
        Number of entries updated
    """
    updated_count = 0

    try:
        log_file = get_current_log_file()
        if not os.path.exists(log_file):
            return 0

        lines = []
        with open(log_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return 0
            lines.append(header)

            # Find column indices
            try:
                crypto_idx = header.index('crypto')
                direction_idx = header.index('direction')
                qty_idx = header.index('qty')
                status_idx = header.index('status')
                timestamp_idx = header.index('timestamp')
                kalshi_strike_idx = header.index('kalshi_strike')
                poly_strike_idx = header.index('poly_strike')
                kalshi_end_idx = header.index('kalshi_end_price')
                poly_end_idx = header.index('poly_end_price')
                kalshi_result_idx = header.index('kalshi_result')
                poly_result_idx = header.index('poly_result')
                pnl_idx = header.index('pnl')
                total_cost_idx = header.index('total_cost')
                total_fees_idx = header.index('total_fees')
                unwind_loss_idx = header.index('unwind_loss')
            except ValueError:
                return 0  # Missing columns

            all_rows = list(reader)

            for row in all_rows:
                if len(row) <= max(crypto_idx, direction_idx, qty_idx, status_idx,
                                   timestamp_idx, kalshi_strike_idx, poly_strike_idx,
                                   kalshi_end_idx, poly_end_idx, kalshi_result_idx,
                                   poly_result_idx, pnl_idx):
                    lines.append(row)
                    continue

                # Only update SUCCESS trades for this crypto in this window
                # that haven't been settled yet (kalshi_end_price is blank)
                if (row[crypto_idx] != crypto or
                    row[status_idx] not in ('SUCCESS', 'SUCCESS/THRESHOLD') or
                    row[timestamp_idx] < window_start_time or
                    row[kalshi_end_idx].strip() != ''):
                    lines.append(row)
                    continue

                # Parse trade data
                try:
                    direction = row[direction_idx]
                    qty = float(row[qty_idx])
                    k_strike = float(row[kalshi_strike_idx])
                    p_strike = float(row[poly_strike_idx])
                    total_cost = float(row[total_cost_idx]) if row[total_cost_idx] else 0
                    total_fees = float(row[total_fees_idx]) if row[total_fees_idx] else 0
                    u_loss = float(row[unwind_loss_idx]) if row[unwind_loss_idx] else 0
                except (ValueError, TypeError):
                    lines.append(row)
                    continue

                # Fill end prices
                row[kalshi_end_idx] = _format_value(kalshi_end_price)
                row[poly_end_idx] = _format_value(poly_end_price)

                # Determine win/loss for each side
                # Kalshi settles on CF Benchmarks (kalshi_end_price)
                # Poly settles on Chainlink (poly_end_price)
                if direction == "K_UP+P_DOWN":
                    k_won = kalshi_end_price >= k_strike
                    p_won = poly_end_price < p_strike
                else:  # K_DOWN+P_UP
                    k_won = kalshi_end_price < k_strike
                    p_won = poly_end_price >= p_strike

                row[kalshi_result_idx] = "WON" if k_won else "LOST"
                row[poly_result_idx] = "WON" if p_won else "LOST"

                # Calculate actual P&L
                k_payout = 1.0 * qty if k_won else 0.0
                p_payout = 1.0 * qty if p_won else 0.0
                actual_pnl = (k_payout + p_payout) - total_cost - total_fees - u_loss
                row[pnl_idx] = _format_value(actual_pnl)

                # Update status if double loss
                if not k_won and not p_won:
                    row[status_idx] = 'DOUBLE_LOSS'
                    print(f"  [DOUBLE LOSS] {crypto} trade at {row[timestamp_idx]}: "
                          f"K end ${kalshi_end_price:,.2f} vs strike ${k_strike:,.2f}, "
                          f"P end ${poly_end_price:,.2f} vs strike ${p_strike:,.2f} "
                          f"({direction}) - P&L ${actual_pnl:+.2f}")
                else:
                    result_str = []
                    if k_won:
                        result_str.append(f"K WON (+${k_payout:.2f})")
                    else:
                        result_str.append("K LOST")
                    if p_won:
                        result_str.append(f"P WON (+${p_payout:.2f})")
                    else:
                        result_str.append("P LOST")
                    print(f"  [SETTLED] {crypto}: {' | '.join(result_str)} → P&L ${actual_pnl:+.2f}")

                updated_count += 1
                lines.append(row)

        # Write back
        if updated_count > 0:
            with open(log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(lines)
            print(f"  [LOG] Settled {updated_count} trades for {crypto}")

    except Exception as e:
        print(f"[LOG] Error updating trade settlements: {e}")

    return updated_count


# =============================================================================
# CONSOLE HIGHLIGHTS LOG (strikes, edge scans before trades, trade results)
# =============================================================================

# Buffer for edge scan lines - written to log only when a trade happens
_scan_buffer: list = []


def buffer_scan_line(line: str) -> None:
    """Buffer a scan output line. Written to log only if a trade executes."""
    _scan_buffer.append(line)


def clear_scan_buffer() -> None:
    """Clear the scan buffer (called at start of each scan cycle)."""
    global _scan_buffer
    _scan_buffer = []


def log_highlight(text: str) -> None:
    """Append text directly to the console highlights log."""
    try:
        os.makedirs(os.path.dirname(CONSOLE_HIGHLIGHTS_LOG), exist_ok=True)
        with open(CONSOLE_HIGHLIGHTS_LOG, "a", encoding="utf-8") as f:
            f.write(text)
    except Exception as e:
        print(f"[LOG] Error writing highlight: {e}")


def flush_scan_buffer_to_log() -> None:
    """Write buffered scan lines to the highlights log (called when a trade executes)."""
    global _scan_buffer
    if not _scan_buffer:
        return
    try:
        os.makedirs(os.path.dirname(CONSOLE_HIGHLIGHTS_LOG), exist_ok=True)
        with open(CONSOLE_HIGHLIGHTS_LOG, "a", encoding="utf-8") as f:
            f.write("\n".join(_scan_buffer) + "\n")
    except Exception as e:
        print(f"[LOG] Error flushing scan buffer: {e}")
    _scan_buffer = []
