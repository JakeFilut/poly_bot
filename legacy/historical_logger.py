"""
Historical Data Logger - Records prices and final prices.
Always appends, never deletes data.
"""

import os
import csv
import requests
from datetime import datetime, timezone
from typing import Dict, Optional


class HistoricalLogger:
    """Logs market prices and final prices for each window."""

    LOG_FILE = "Historical_Data.csv"
    HEADERS = ['timestamp_MST', 'crypto', 'kalshi_strike', 'poly_strike', 'kalshi_final', 'poly_final', 'k_in_between', 'p_in_between', 'traded', 'record_time']

    def __init__(self, log_dir: str = None):
        if log_dir:
            self.log_path = os.path.join(log_dir, self.LOG_FILE)
        else:
            self.log_path = self.LOG_FILE

        self._pending: Dict[str, dict] = {}
        self._last_window: str = ""
        self._logged_windows: set = set()
        self._ensure_file_exists()
        self._load_existing_entries()

    def _load_existing_entries(self):
        """Load existing entries from CSV to prevent duplicates on restart.
        Also restores pending entries that need finalization (missing finals)."""
        if not os.path.exists(self.log_path):
            return

        try:
            with open(self.log_path, 'r', newline='') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header

                line_num = 0  # Start at 1 (after header)
                for row in reader:
                    line_num += 1
                    if len(row) >= 2:
                        # row[0] = timestamp (csv.reader already removes quotes)
                        # row[1] = crypto like "BTC"
                        timestamp_str = row[0].strip()
                        crypto = row[1].strip()

                        # Extract window from timestamp, rounded to 15-min boundary
                        # Handles both formats:
                        #   New ISO: "2026-02-05 12:15:00"
                        #   Old format: "Feb 05 2026 12 15"
                        try:
                            if ':' in timestamp_str:
                                # New ISO format: "2026-02-05 12:15:00"
                                parts = timestamp_str.split(' ')
                                if len(parts) >= 2:
                                    time_part = parts[1]  # "12:15:00"
                                    time_parts = time_part.split(':')
                                    if len(time_parts) >= 2:
                                        hour = int(time_parts[0])
                                        minute = int(time_parts[1])
                            else:
                                # Old format: "Feb 05 2026 12 15"
                                parts = timestamp_str.split(' ')
                                if len(parts) >= 5:
                                    hour = int(parts[3])
                                    minute = int(parts[4])
                                else:
                                    continue

                            # Round down to 15-minute boundary
                            window_minute = (minute // 15) * 15
                            window = f"{hour:02d}:{window_minute:02d}"
                            key = (crypto, window)
                            self._logged_windows.add(key)

                            # Check if this entry is missing finals (needs finalization)
                            # row[4] = kalshi_final, row[5] = poly_final
                            kalshi_final = row[4].strip() if len(row) > 4 else ""
                            poly_final = row[5].strip() if len(row) > 5 else ""

                            if not kalshi_final or not poly_final:
                                # This entry needs finalization - add to pending
                                kalshi_strike = float(row[2].strip()) if len(row) > 2 and row[2].strip() else None
                                poly_strike = float(row[3].strip()) if len(row) > 3 and row[3].strip() else None

                                if kalshi_strike and poly_strike:
                                    self._pending[crypto] = {
                                        "window": window,
                                        "line_num": line_num,
                                        "kalshi_strike": kalshi_strike,
                                        "poly_strike": poly_strike,
                                    }
                                    print(f"[historical] Restored pending {crypto} from {window} (needs finals)")
                        except:
                            pass

                if self._logged_windows:
                    print(f"[historical] Loaded {len(self._logged_windows)} existing entries from CSV")
                if self._pending:
                    print(f"[historical] {len(self._pending)} entries need finalization")
        except Exception as e:
            print(f"[historical] Error loading existing entries: {e}")

    def _ensure_file_exists(self):
        """Create CSV with headers only if file doesn't exist."""
        if not os.path.exists(self.log_path):
            try:
                with open(self.log_path, 'w', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(self.HEADERS)
                print(f"[historical] Created new {self.LOG_FILE}")
            except Exception as e:
                print(f"[historical] Error creating file: {e}")

    def _get_mst_now(self):
        """Get current time in Mountain Standard Time (UTC-7)."""
        from datetime import timedelta
        return datetime.utcnow() - timedelta(hours=7)

    def _get_current_window(self) -> str:
        """Get current 15-minute window in MST."""
        now = self._get_mst_now()
        minute_window = (now.minute // 15) * 15
        return f"{now.hour:02d}:{minute_window:02d}"

    def _get_previous_window(self) -> str:
        """Get the previous 15-minute window identifier in MST."""
        from datetime import timedelta
        now = self._get_mst_now()
        prev_time = now - timedelta(minutes=15)
        minute_window = (prev_time.minute // 15) * 15
        return f"{prev_time.hour:02d}:{minute_window:02d}"

    def _get_window_timestamp(self) -> str:
        """Get timestamp rounded to start of current 15-minute window in MST.
        Format: Feb 05 2026 08 45 (Mountain Time)
        """
        now = self._get_mst_now()
        minute_window = (now.minute // 15) * 15
        window_time = now.replace(minute=minute_window, second=0, microsecond=0)
        return window_time.strftime("%b %d %Y %H %M")

    def _fetch_price(self, crypto: str) -> Optional[float]:
        """Fetch current price from CoinGecko."""
        ids = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple"}
        coin_id = ids.get(crypto)
        if not coin_id:
            return None
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.json().get(coin_id, {}).get("usd")
        except Exception:
            pass
        return None

    def _now_mst(self) -> str:
        """Get current Mountain Standard Time in format: Feb 06 2026 03 10"""
        mst = self._get_mst_now()
        return mst.strftime("%b %d %Y %H %M")

    def _append_row(self, timestamp, crypto, kalshi_strike, poly_strike,
                    kalshi_final="", poly_final="", k_in_between="", p_in_between="", traded=""):
        """Safely append a row to the CSV file."""
        try:
            record_time = self._now_mst()
            with open(self.log_path, 'a', newline='') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                writer.writerow([
                    timestamp,
                    crypto,
                    f"{kalshi_strike:.2f}" if kalshi_strike else "",
                    f"{poly_strike:.2f}" if poly_strike else "",
                    f"{kalshi_final:.2f}" if kalshi_final else "",
                    f"{poly_final:.2f}" if poly_final else "",
                    k_in_between,
                    p_in_between,
                    traded,
                    record_time,
                ])
            return True
        except Exception as e:
            print(f"[historical] Error appending row: {e}")
            return False

    def log_previous_window(self, crypto: str, kalshi_price: Optional[float],
                           poly_price: Optional[float],
                           kalshi_final: Optional[float] = None,
                           poly_final: Optional[float] = None):
        """Log the previous window's data at program startup."""
        prev_window = self._get_previous_window()
        key = (crypto, prev_window)

        if key in self._logged_windows:
            return

        # Need at least one price to log
        if poly_price is None and kalshi_price is None:
            return

        # Use provided finals or fetch from CoinGecko
        if kalshi_final is None:
            kalshi_final = self._fetch_price(crypto)
        if poly_final is None:
            poly_final = self._fetch_price(crypto)

        if self._append_row(kalshi_price, poly_price, kalshi_final, poly_final):
            self._logged_windows.add(key)
            k_str = f"K${kalshi_price:.2f}" if kalshi_price else "K$--"
            p_str = f"P${poly_price:.2f}" if poly_price else "P$--"
            kf_str = f"${kalshi_final:.2f}" if kalshi_final else "$--"
            pf_str = f"${poly_final:.2f}" if poly_final else "$--"
            print(f"[historical] Logged PREVIOUS {prev_window} {crypto}: {k_str} / {p_str} -> Finals K{kf_str} / P{pf_str}")

    def _is_valid_price(self, crypto: str, price: float) -> bool:
        """Check if price is valid/reasonable for the crypto."""
        if price is None or price <= 0:
            return False
        min_prices = {"BTC": 10000, "ETH": 500, "SOL": 10, "XRP": 0.50}
        return price >= min_prices.get(crypto, 0.50)

    def record_prices(self, crypto: str, kalshi_strike: Optional[float],
                     poly_strike: Optional[float]):
        """
        Record prices when window starts.

        When window changes, the NEW prices become the finals for the previous row.
        """
        current_window = self._get_current_window()

        # Check if THIS CRYPTO has a pending entry from a PREVIOUS window
        # If so, finalize it with the NEW prices (which are the finals for the old window)
        if crypto in self._pending:
            pending_window = self._pending[crypto].get("window")
            if pending_window and pending_window != current_window:
                # This crypto's pending entry is from a previous window - finalize it
                self._finalize_crypto(crypto, kalshi_strike, poly_strike)

        self._last_window = current_window

        key = (crypto, current_window)

        # Skip if already recorded
        if key in self._logged_windows:
            return
        if crypto in self._pending and self._pending[crypto].get("window") == current_window:
            return

        # Need BOTH strike prices - no fallbacks
        if kalshi_strike is None or poly_strike is None:
            return

        # Validate both prices are reasonable
        if not self._is_valid_price(crypto, kalshi_strike):
            return
        if not self._is_valid_price(crypto, poly_strike):
            return

        # Skip if prices are more than 3% apart (likely a fetch error)
        avg_price = (kalshi_strike + poly_strike) / 2
        percent_diff = abs(kalshi_strike - poly_strike) / avg_price * 100
        if percent_diff > 3.0:
            print(f"[historical] Skipping {crypto}: prices {percent_diff:.1f}% apart (K${kalshi_strike:.2f} vs P${poly_strike:.2f})")
            return

        # Generate timestamp rounded to start of 15-min window in MST
        timestamp = self._get_window_timestamp()

        # Append row with empty final prices (will be filled when next window starts)
        if self._append_row(timestamp, crypto, kalshi_strike, poly_strike):
            self._logged_windows.add(key)

            # Track line number and prices for updating finals later
            try:
                with open(self.log_path, 'r') as f:
                    line_count = sum(1 for _ in f)
                self._pending[crypto] = {
                    "window": current_window,
                    "line_num": line_count - 1,  # 0-indexed (last line we just added)
                    "kalshi_strike": kalshi_strike,
                    "poly_strike": poly_strike,
                }
            except:
                pass

            print(f"[historical] Logged {crypto}: K${kalshi_strike:.2f} / P${poly_strike:.2f}")

    def mark_traded(self, crypto: str):
        """Mark the current window's entry as traded (YES)."""
        if crypto not in self._pending:
            return

        data = self._pending[crypto]
        line_num = data.get("line_num", 0)

        try:
            # Read all rows using csv module
            with open(self.log_path, 'r', newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)

            if 0 < line_num < len(rows):
                row = rows[line_num]

                # Ensure row has enough columns (10 now with record_time)
                while len(row) < 10:
                    row.append("")

                row[8] = "traded"  # traded is column 8
                rows[line_num] = row

                # Write back using csv module
                with open(self.log_path, 'w', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(rows)

                print(f"[historical] Marked {crypto} as TRADED")
        except Exception as e:
            print(f"[historical] Error marking {crypto} traded: {e}")

    def mark_failed_threshold(self, crypto: str, strike_diff_pct: float, threshold_pct: float):
        """Mark the current window's entry as failed threshold check."""
        if crypto not in self._pending:
            return

        data = self._pending[crypto]
        line_num = data.get("line_num", 0)

        try:
            # Read all rows using csv module
            with open(self.log_path, 'r', newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)

            if 0 < line_num < len(rows):
                row = rows[line_num]

                # Ensure row has enough columns (10 now with record_time)
                while len(row) < 10:
                    row.append("")

                # Only update if not already marked as traded
                if row[8] != "traded":
                    row[8] = f"failed ({strike_diff_pct:.3f}% > {threshold_pct}%)"
                    rows[line_num] = row

                    # Write back using csv module
                    with open(self.log_path, 'w', newline='') as f:
                        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                        writer.writerows(rows)

                    print(f"[historical] Marked {crypto} as FAILED THRESHOLD ({strike_diff_pct:.3f}% > {threshold_pct}%)")
        except Exception as e:
            print(f"[historical] Error marking {crypto} failed threshold: {e}")

    def _finalize_crypto(self, crypto: str, kalshi_final: float, poly_final: float):
        """
        Fill in finals for a specific crypto's pending entry using the NEW window's prices.

        Current start price = Previous end price, so the new prices ARE the finals.
        Also calculates if each final price was between the two starting prices.
        """
        # Check if this crypto has a pending entry
        if crypto not in self._pending:
            return

        # Need BOTH finals - no fallbacks
        if not kalshi_final or not poly_final:
            print(f"[historical] Missing {crypto} finals - need both Kalshi and Poly")
            return  # Don't clear pending - try again next time

        # Skip if final prices are more than 3% apart (likely a fetch error)
        avg_final = (kalshi_final + poly_final) / 2
        percent_diff = abs(kalshi_final - poly_final) / avg_final * 100
        if percent_diff > 3.0:
            print(f"[historical] {crypto} finals skipped: prices {percent_diff:.1f}% apart (K${kalshi_final:.2f} vs P${poly_final:.2f})")
            return  # Don't clear pending - try again next time

        # Read all rows using csv module
        try:
            with open(self.log_path, 'r', newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)
        except Exception as e:
            print(f"[historical] Error reading file: {e}")
            return

        data = self._pending[crypto]
        line_num = data.get("line_num", 0)
        kalshi_price = data.get("kalshi_strike")  # Use correct key name
        poly_price = data.get("poly_strike")  # Use correct key name

        if 0 < line_num < len(rows):
            row = rows[line_num]

            # Ensure row has enough columns (10: timestamp, crypto, k_price, p_price, k_final, p_final, k_in_between, p_in_between, traded, record_time)
            while len(row) < 10:
                row.append("")

            row[4] = f"{kalshi_final:.2f}"  # kalshi_final is column 4
            row[5] = f"{poly_final:.2f}"  # poly_final is column 5

            # Calculate if each final is between the two starting prices
            k_in_between = ""
            p_in_between = ""
            if kalshi_price and poly_price:
                low_price = min(kalshi_price, poly_price)
                high_price = max(kalshi_price, poly_price)

                # Check if Kalshi final is between
                if low_price < kalshi_final < high_price:
                    k_in_between = "YES"
                else:
                    k_in_between = "NO"

                # Check if Poly final is between
                if low_price < poly_final < high_price:
                    p_in_between = "YES"
                else:
                    p_in_between = "NO"

            row[6] = k_in_between  # k_in_between is column 6
            row[7] = p_in_between  # p_in_between is column 7
            rows[line_num] = row

            # Write back using csv module
            try:
                with open(self.log_path, 'w', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(rows)
                k_str = f"K${kalshi_final:.2f}"
                p_str = f"P${poly_final:.2f}"
                k_btw = f"K={k_in_between}" if k_in_between else ""
                p_btw = f"P={p_in_between}" if p_in_between else ""
                print(f"[historical] Updated {crypto} finals: {k_str} / {p_str} [{k_btw} {p_btw}]")
            except Exception as e:
                print(f"[historical] Error updating {crypto} finals: {e}")

            # Remove only this crypto from pending
            del self._pending[crypto]

    def _finalize_all_pending(self):
        """Fetch final prices and update pending entries (fallback using CoinGecko)."""
        if not self._pending:
            return

        # Read all rows using csv module
        try:
            with open(self.log_path, 'r', newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)
        except Exception as e:
            print(f"[historical] Error reading file: {e}")
            self._pending.clear()
            return

        # Update rows with final prices
        updated = False
        for crypto, data in self._pending.items():
            line_num = data.get("line_num", 0)
            if 0 < line_num < len(rows):
                final_price = self._fetch_price(crypto)
                if final_price:
                    row = rows[line_num]
                    # Ensure row has enough columns
                    while len(row) < 10:
                        row.append("")
                    row[4] = f"{final_price:.2f}"  # kalshi_final
                    row[5] = f"{final_price:.2f}"  # poly_final
                    rows[line_num] = row
                    updated = True
                    print(f"[historical] Updated {crypto} final: ${final_price:.2f}")

        # Write back only if we updated something
        if updated:
            try:
                with open(self.log_path, 'w', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(rows)
            except Exception as e:
                print(f"[historical] Error updating finals: {e}")

        self._pending.clear()

    def check_window_change(self, cryptos: list):
        """Check if window changed and finalize pending entries."""
        current_window = self._get_current_window()
        if self._last_window and current_window != self._last_window:
            self._finalize_all_pending()
        self._last_window = current_window

    def force_finalize_all(self):
        """Finalize all pending entries on shutdown."""
        self._finalize_all_pending()
