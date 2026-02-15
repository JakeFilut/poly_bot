"""
logger.py  --  Bounded-growth, analysis-ready Logger for pm_hourly_clone_bot.

Outputs:
    bot_log_<run_id>.csv              compact, fixed-schema, analysis-ready table
    bot_events_<run_id>.jsonl         detailed telemetry, size-controlled

Key guarantees:
    * JSONL bounded via SNAPSHOT_COMPACT interval + SNAPSHOT_ON_CHANGE + rotation
    * No full SNAPSHOT every tick -- only compact / on-change / order lifecycle
    * CSV has a FIXED header that never varies
    * Every record has run_id, schema_version, bot_version
    * event_type (never "type") everywhere
    * SELL/EXIT events with qty <= 0 or qty < MIN_QTY are suppressed
    * Correlation keys: decision_id, client_order_id, position_id, parent_order_id
    * Lifecycle: ORDER_INTENT -> ORDER_SUBMIT -> ORDER_ACK -> ORDER_FILL -> ORDER_CANCEL
    * reason field always populated
"""
from __future__ import annotations
import json, os, time, uuid, io
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA / VERSION CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
SCHEMA_VERSION = 6
BOT_VERSION    = "2.0.0"

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING POLICY CONSTANTS  (tuneable via env)
# ═══════════════════════════════════════════════════════════════════════════════
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "5.0"))
MID_MOVE_CENTS        = float(os.getenv("MID_MOVE_CENTS", "0.01"))
IMB_DELTA             = float(os.getenv("IMB_DELTA", "0.15"))
FLUSH_INTERVAL_SEC    = float(os.getenv("FLUSH_INTERVAL_SEC", "1.0"))
MAX_JSONL_BYTES       = int(os.getenv("MAX_JSONL_BYTES", str(200 * 1024 * 1024)))
MAX_CSV_BYTES         = int(os.getenv("MAX_CSV_BYTES",   str(500 * 1024 * 1024)))
STATE_HIST_MAX        = int(os.getenv("STATE_HIST_MAX", "120"))
MIN_QTY               = float(os.getenv("MIN_QTY", "0.001"))

# ═══════════════════════════════════════════════════════════════════════════════
# FIXED CSV SCHEMA  (NEVER vary columns)
# ═══════════════════════════════════════════════════════════════════════════════
CSV_COLUMNS: List[str] = [
    "ts","run_id","schema_version","bot_version","mode","profile",
    "event_type","engine",
    "position_id","decision_id","client_order_id","exchange_order_id",
    "parent_order_id",
    "crypto","slug","hour_start_utc","t_min",
    "spot","hour_open","delta_bps","abs_delta_bps",
    "delta_vel_bps_per_min","zscore",
    "phase","seconds_to_close",
    "outcome","side","qty","target_price","fill_price","usdc_cost",
    "fees_usdc","maker_taker","did_cross",
    "ref_bid","ref_ask","ref_mid","ref_spread","ref_imb",
    "up_bid","up_ask","up_mid","up_spread","up_imb",
    "dn_bid","dn_ask","dn_mid","dn_spread","dn_imb",
    "cap_used","thr_used","size_mult",
    "exit_reason","vwap",
    "unrealized_pnl_usdc","realized_pnl_usdc","net_pnl_usdc",
    "notes",
]

_CSV_PRECISION: Dict[str, int] = {
    "qty":10,"fill_price":6,"target_price":6,"usdc_cost":4,
    "delta_bps":3,"abs_delta_bps":3,"delta_vel_bps_per_min":3,
    "zscore":3,"t_min":3,"seconds_to_close":1,
    "spot":2,"hour_open":2,"fees_usdc":4,"vwap":6,
    "unrealized_pnl_usdc":4,"realized_pnl_usdc":4,"net_pnl_usdc":4,
    "cap_used":4,"thr_used":1,"size_mult":2,
    "ref_bid":4,"ref_ask":4,"ref_mid":4,"ref_spread":4,"ref_imb":3,
    "up_bid":4,"up_ask":4,"up_mid":4,"up_spread":4,"up_imb":3,
    "dn_bid":4,"dn_ask":4,"dn_mid":4,"dn_spread":4,"dn_imb":3,
}

def _csv_fmt(key: str, val: Any) -> str:
    if val is None or val == "":
        return ""
    prec = _CSV_PRECISION.get(key)
    if prec is not None and isinstance(val, (int, float)):
        s = f"{val:.{prec}f}"
    elif isinstance(val, bool):
        s = "true" if val else "false"
    else:
        s = str(val)
    if "," in s or '"' in s or "\n" in s:
        return '"' + s.replace('"', '""') + '"'
    return s

def _spread_bucket(spread: float) -> str:
    if spread <= 0.01 + 1e-9: return "1c"
    if spread <= 0.02 + 1e-9: return "2c"
    return "3c+"

def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGER CLASS
# ═══════════════════════════════════════════════════════════════════════════════
class Logger:
    """
    Bounded-growth logger.

    Call sites in the bot loop:
        1. snapshot_compact()       -- every SNAPSHOT_INTERVAL_SEC per market
        2. snapshot_on_change()     -- when significant state changes detected
        3. log_order_intent()       -- decision triggers an order
        4. log_order_submit()       -- order sent to exchange / paper engine
        5. log_order_ack()          -- exchange acknowledges order
        6. log_order_fill()         -- full or partial fill
        7. log_order_cancel()       -- order cancelled or replaced
        8. log_event()              -- generic (START, STOP, HOUR_RESOLVED …)
        9. log_csv() / log_jsonl()  -- direct low-level writes
    """

    def __init__(self, run_id: str, log_dir: str, mode: str, profile: str):
        self.run_id   = run_id
        self.log_dir  = log_dir
        self.mode     = mode
        self.profile  = profile
        os.makedirs(log_dir, exist_ok=True)

        self._csv_base  = os.path.join(log_dir, f"bot_log_{run_id}")
        self._jsonl_base = os.path.join(log_dir, f"bot_events_{run_id}")
        self._csv_part  = 0
        self._jsonl_part = 0

        self._csv_path  = f"{self._csv_base}.csv"
        self._jsonl_path = f"{self._jsonl_base}.jsonl"
        self._csv_f: Optional[io.TextIOWrapper]   = None
        self._jsonl_f: Optional[io.TextIOWrapper]  = None
        self._open_csv()
        self._open_jsonl()

        self._last_flush_ts = time.time()
        # snapshot throttle: market_key -> last compact time
        self._last_compact_ts: Dict[str, float] = {}
        # snapshot change detection: market_key -> prev snapshot dict
        self._prev_snapshots: Dict[str, Dict[str, Any]] = {}

    # ── file management ──────────────────────────────────────────────────
    def _open_csv(self):
        exists = os.path.exists(self._csv_path) and os.path.getsize(self._csv_path) > 0
        self._csv_f = open(self._csv_path, "a", encoding="utf-8", buffering=8192)
        if not exists:
            self._csv_f.write(",".join(CSV_COLUMNS) + "\n")

    def _open_jsonl(self):
        self._jsonl_f = open(self._jsonl_path, "a", encoding="utf-8", buffering=8192)

    def _rotate_csv_if_needed(self):
        if self._csv_f is None: return
        try:    pos = self._csv_f.tell()
        except: pos = 0
        if pos >= MAX_CSV_BYTES:
            self._csv_f.flush(); self._csv_f.close()
            self._csv_part += 1
            self._csv_path = f"{self._csv_base}_part{self._csv_part:02d}.csv"
            self._csv_f = open(self._csv_path, "a", encoding="utf-8", buffering=8192)
            self._csv_f.write(",".join(CSV_COLUMNS) + "\n")

    def _rotate_jsonl_if_needed(self):
        if self._jsonl_f is None: return
        try:    pos = self._jsonl_f.tell()
        except: pos = 0
        if pos >= MAX_JSONL_BYTES:
            self._jsonl_f.flush(); self._jsonl_f.close()
            self._jsonl_part += 1
            self._jsonl_path = f"{self._jsonl_base}_part{self._jsonl_part:02d}.jsonl"
            self._jsonl_f = open(self._jsonl_path, "a", encoding="utf-8", buffering=8192)

    def rotate_files_if_needed(self):
        self._rotate_csv_if_needed()
        self._rotate_jsonl_if_needed()

    def _maybe_flush(self, force: bool = False):
        now = time.time()
        if force or (now - self._last_flush_ts >= FLUSH_INTERVAL_SEC):
            if self._csv_f:   self._csv_f.flush()
            if self._jsonl_f: self._jsonl_f.flush()
            self._last_flush_ts = now

    def close(self):
        for f in (self._csv_f, self._jsonl_f):
            if f:
                try: f.flush(); f.close()
                except: pass
        self._csv_f = self._jsonl_f = None

    # ── core write methods ───────────────────────────────────────────────
    def log_csv(self, row: Dict[str, Any]):
        row.setdefault("run_id", self.run_id)
        row.setdefault("schema_version", SCHEMA_VERSION)
        row.setdefault("bot_version", BOT_VERSION)
        row.setdefault("mode", self.mode)
        row.setdefault("profile", self.profile)
        row.setdefault("ts", _iso_z(_utc_now()))
        self._rotate_csv_if_needed()
        if self._csv_f:
            self._csv_f.write(",".join(_csv_fmt(k, row.get(k, "")) for k in CSV_COLUMNS) + "\n")
        self._maybe_flush()

    def log_jsonl(self, event: Dict[str, Any], *, force_flush: bool = False):
        event.setdefault("ts", _iso_z(_utc_now()))
        event.setdefault("run_id", self.run_id)
        event.setdefault("schema_version", SCHEMA_VERSION)
        event.setdefault("bot_version", BOT_VERSION)
        self._rotate_jsonl_if_needed()
        if self._jsonl_f:
            self._jsonl_f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
        self._maybe_flush(force=force_flush)
        self._console_print(event)

    # ── snapshot throttle helpers ────────────────────────────────────────
    def should_log_snapshot_compact(self, now_ts: float, market_key: str) -> bool:
        last = self._last_compact_ts.get(market_key, 0.0)
        if now_ts - last >= SNAPSHOT_INTERVAL_SEC:
            self._last_compact_ts[market_key] = now_ts
            return True
        return False

    def should_log_snapshot_on_change(
        self, market_key: str, new_snap: Dict[str, Any],
    ) -> Tuple[bool, str]:
        prev = self._prev_snapshots.get(market_key)
        if prev is None:
            self._prev_snapshots[market_key] = new_snap.copy()
            return True, "INITIAL"
        triggers: List[str] = []
        for side in ("up_mid", "dn_mid"):
            if abs(new_snap.get(side,0.0) - prev.get(side,0.0)) >= MID_MOVE_CENTS:
                triggers.append(f"MID_MOVE_{side}")
        for side in ("up_spread", "dn_spread"):
            if _spread_bucket(prev.get(side,0.0)) != _spread_bucket(new_snap.get(side,0.0)):
                triggers.append(f"SPREAD_BUCKET_{side}")
        for side in ("up_imb", "dn_imb"):
            oi, ni = prev.get(side,0.0), new_snap.get(side,0.0)
            if (oi - 1.0) * (ni - 1.0) < 0:
                triggers.append(f"IMB_FLIP_{side}")
            elif abs(ni - oi) >= IMB_DELTA:
                triggers.append(f"IMB_DELTA_{side}")
        od = abs(prev.get("delta_bps",0.0))
        nd = abs(new_snap.get("delta_bps",0.0))
        thr = new_snap.get("entry_thr_bps", 10.0)
        if (od < thr) != (nd < thr):
            triggers.append("DELTA_THR_CROSS")
        for key in ("pos_qty_up", "pos_qty_down"):
            if abs(new_snap.get(key,0.0) - prev.get(key,0.0)) >= MIN_QTY:
                triggers.append(f"POS_QTY_{key}")
        if triggers:
            self._prev_snapshots[market_key] = new_snap.copy()
            return True, "|".join(triggers)
        return False, ""

    # ── high-level logging calls ─────────────────────────────────────────
    def log_snapshot_compact(self, *, slug, crypto, t_min, spot, hour_open,
                             delta_bps, abs_delta_bps, vel, z,
                             up_bid, up_ask, up_mid, up_spread, up_imb,
                             dn_bid, dn_ask, dn_mid, dn_spread, dn_imb,
                             pos_qty_up, pos_qty_down, cash_usdc, equity_usdc):
        self.log_jsonl({
            "event_type":"SNAPSHOT_COMPACT",
            "slug":slug,"crypto":crypto,
            "t_min":round(t_min,3),
            "spot":round(spot,2),"hour_open":round(hour_open,2),
            "delta_bps":round(delta_bps,3),"abs_delta_bps":round(abs_delta_bps,3),
            "vel":round(vel,3),"z":round(z,3),
            "up_bid":round(up_bid,4),"up_ask":round(up_ask,4),
            "up_mid":round(up_mid,4),"up_spread":round(up_spread,4),"up_imb":round(up_imb,3),
            "dn_bid":round(dn_bid,4),"dn_ask":round(dn_ask,4),
            "dn_mid":round(dn_mid,4),"dn_spread":round(dn_spread,4),"dn_imb":round(dn_imb,3),
            "pos_qty_up":round(pos_qty_up,10),"pos_qty_down":round(pos_qty_down,10),
            "cash_usdc":round(cash_usdc,2),"equity_usdc":round(equity_usdc,2),
        })

    def log_snapshot_on_change(self, *, slug, crypto, trigger, t_min, delta_bps,
                               up_mid, up_spread, up_imb,
                               dn_mid, dn_spread, dn_imb,
                               pos_qty_up, pos_qty_down, cash_usdc, equity_usdc):
        self.log_jsonl({
            "event_type":"SNAPSHOT_ON_CHANGE",
            "slug":slug,"crypto":crypto,"trigger":trigger,
            "t_min":round(t_min,3),"delta_bps":round(delta_bps,3),
            "up_mid":round(up_mid,4),"up_spread":round(up_spread,4),"up_imb":round(up_imb,3),
            "dn_mid":round(dn_mid,4),"dn_spread":round(dn_spread,4),"dn_imb":round(dn_imb,3),
            "pos_qty_up":round(pos_qty_up,10),"pos_qty_down":round(pos_qty_down,10),
            "cash_usdc":round(cash_usdc,2),"equity_usdc":round(equity_usdc,2),
        })

    def log_decision(self, event: Dict[str, Any]):
        event["event_type"] = "DECISION"
        self.log_jsonl(event)

    def log_order_intent(self, *, engine, reason, decision_id, position_id,
                         parent_order_id="", crypto, slug, outcome, side, qty,
                         target_price, usdc_cost=0.0, ctx, book_fields,
                         extra=None):
        if side in ("SELL","EXIT") and qty < MIN_QTY: return
        row = self._build_order_row(
            event_type="ORDER_INTENT", engine=engine, reason=reason,
            decision_id=decision_id, position_id=position_id,
            parent_order_id=parent_order_id,
            crypto=crypto, slug=slug, outcome=outcome,
            side=side, qty=qty, target_price=target_price,
            fill_price=0.0, usdc_cost=usdc_cost,
            ctx=ctx, book_fields=book_fields)
        if extra: row.update(extra)
        self.log_csv(row); self.log_jsonl(row, force_flush=True)

    def log_order_submit(self, *, engine, reason, decision_id, position_id,
                         client_order_id, parent_order_id="",
                         crypto, slug, outcome, side, qty, target_price,
                         usdc_cost=0.0, ctx, book_fields, extra=None):
        if side in ("SELL","EXIT") and qty < MIN_QTY: return
        row = self._build_order_row(
            event_type="ORDER_SUBMIT", engine=engine, reason=reason,
            decision_id=decision_id, position_id=position_id,
            client_order_id=client_order_id,
            parent_order_id=parent_order_id,
            crypto=crypto, slug=slug, outcome=outcome,
            side=side, qty=qty, target_price=target_price,
            fill_price=0.0, usdc_cost=usdc_cost,
            ctx=ctx, book_fields=book_fields)
        if extra: row.update(extra)
        self.log_csv(row); self.log_jsonl(row, force_flush=True)

    def log_order_ack(self, *, decision_id, client_order_id,
                      exchange_order_id="", position_id,
                      crypto, slug, outcome, side, notes=""):
        row = {"event_type":"ORDER_ACK","decision_id":decision_id,
               "client_order_id":client_order_id,
               "exchange_order_id":exchange_order_id,
               "position_id":position_id,"crypto":crypto,"slug":slug,
               "outcome":outcome,"side":side,"notes":notes}
        self.log_csv(row); self.log_jsonl(row, force_flush=True)

    def log_order_fill(self, *, engine, reason, decision_id, client_order_id,
                       exchange_order_id="", position_id, parent_order_id="",
                       crypto, slug, outcome, side, qty, fill_price,
                       usdc_cost=0.0, fees_usdc=0.0, maker_taker="",
                       did_cross="", realized_pnl_usdc=0.0,
                       unrealized_pnl_usdc=0.0, net_pnl_usdc=0.0,
                       vwap=0.0, ctx, book_fields, extra=None):
        if side in ("SELL","EXIT") and qty < MIN_QTY: return
        row = self._build_order_row(
            event_type="ORDER_FILL", engine=engine, reason=reason,
            decision_id=decision_id, position_id=position_id,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            parent_order_id=parent_order_id,
            crypto=crypto, slug=slug, outcome=outcome,
            side=side, qty=qty, target_price=0.0,
            fill_price=fill_price, usdc_cost=usdc_cost,
            ctx=ctx, book_fields=book_fields)
        row.update({"fees_usdc":fees_usdc,"maker_taker":maker_taker,
                     "did_cross":did_cross,"vwap":vwap,
                     "realized_pnl_usdc":realized_pnl_usdc,
                     "unrealized_pnl_usdc":unrealized_pnl_usdc,
                     "net_pnl_usdc":net_pnl_usdc})
        if extra: row.update(extra)
        self.log_csv(row); self.log_jsonl(row, force_flush=True)

    def log_order_cancel(self, *, decision_id="", client_order_id,
                         exchange_order_id="", position_id="",
                         crypto, slug, outcome, side, reason, notes=""):
        row = {"event_type":"ORDER_CANCEL","decision_id":decision_id,
               "client_order_id":client_order_id,
               "exchange_order_id":exchange_order_id,
               "position_id":position_id,"crypto":crypto,"slug":slug,
               "outcome":outcome,"side":side,"exit_reason":reason,"notes":notes}
        self.log_csv(row); self.log_jsonl(row, force_flush=True)

    def log_event(self, event: Dict[str, Any], *, also_csv: bool = False):
        self.log_jsonl(event)
        if also_csv: self.log_csv(event)

    # ── internal ─────────────────────────────────────────────────────────
    def _build_order_row(self, *, event_type, engine, reason,
                         decision_id, position_id,
                         client_order_id="", exchange_order_id="",
                         parent_order_id="",
                         crypto, slug, outcome, side, qty,
                         target_price, fill_price, usdc_cost,
                         ctx, book_fields) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "event_type":event_type,"engine":engine,
            "decision_id":decision_id,"position_id":position_id,
            "client_order_id":client_order_id,
            "exchange_order_id":exchange_order_id,
            "parent_order_id":parent_order_id,
            "crypto":crypto,"slug":slug,"outcome":outcome,
            "side":side,"qty":qty,
            "target_price":target_price if target_price else "",
            "fill_price":fill_price if fill_price else "",
            "usdc_cost":usdc_cost,"exit_reason":reason,
            "hour_start_utc":ctx.get("hour_start_utc",""),
            "t_min":ctx.get("t_min",""),
            "spot":ctx.get("spot",""),"hour_open":ctx.get("hour_open",""),
            "delta_bps":ctx.get("delta_bps",""),
            "abs_delta_bps":ctx.get("abs_delta_bps",""),
            "delta_vel_bps_per_min":ctx.get("vel",""),
            "zscore":ctx.get("z",""),
            "phase":ctx.get("phase",""),
            "seconds_to_close":ctx.get("seconds_to_close",""),
            "cap_used":ctx.get("cap_used",""),
            "thr_used":ctx.get("thr_used",""),
            "size_mult":ctx.get("size_mult",""),
        }
        row.update(book_fields)
        return row

    def _console_print(self, event: Dict[str, Any]):
        etype = event.get("event_type", "")
        if etype == "SNAPSHOT_COMPACT":
            print(f"  {event.get('crypto',''):4s}  delta={event.get('delta_bps',0):+7.1f}bps  "
                  f"vel={event.get('vel',0):+6.1f}  "
                  f"up_ask={event.get('up_ask',0):.3f}  "
                  f"dn_ask={event.get('dn_ask',0):.3f}  "
                  f"t={event.get('t_min',0):.1f}m")
        elif etype == "ORDER_INTENT":
            print(f"  >>> {event.get('engine','')} {event.get('side','BUY')} "
                  f"{event.get('crypto','')} {event.get('outcome','')} "
                  f"qty={event.get('qty',0):.1f} "
                  f"px={event.get('target_price',event.get('fill_price',0)):.3f} "
                  f"${event.get('usdc_cost',0):.2f} "
                  f"reason={event.get('exit_reason','')}")
        elif etype == "ORDER_FILL":
            pnl = event.get("realized_pnl_usdc", 0)
            side = event.get("side", "BUY")
            pnl_s = f"pnl={'+' if pnl>=0 else ''}{pnl:.2f}  " if side=="SELL" else ""
            print(f"  $$$ FILL {event.get('engine',''):10s} {side:4s} "
                  f"{event.get('crypto',''):4s} {event.get('outcome',''):4s} "
                  f"qty={event.get('qty',0):.1f} @ ${event.get('fill_price',0):.3f}  "
                  f"{pnl_s}reason={event.get('exit_reason','')}")
        elif etype == "HOUR_RESOLVED":
            print(f"\n  === HOUR RESOLVED: {event.get('crypto','')} {event.get('slug','')} ===")
            print(f"      Winner: {event.get('winner','')}  |  "
                  f"close_proxy={event.get('close_proxy',0):.2f}  "
                  f"open={event.get('hour_open',0):.2f}")
            for pi in event.get("positions", []):
                print(f"      {pi['outcome']}: qty={pi['qty']:.1f}  "
                      f"payout=${pi['payout']:.2f}  "
                      f"pnl={'+' if pi['pnl']>=0 else ''}{pi['pnl']:.2f}")
            print(f"      Cash: ${event.get('cash',0):.2f}  "
                  f"Equity: ${event.get('equity',0):.2f}\n")
        elif etype == "LOOP_ERROR":
            print(f"  ERROR: {event.get('err','')}")
        elif etype == "SKIP_NO_PRICE":
            print(f"  SKIP {event.get('crypto','')}: no price data")
        elif etype in ("DECISION","SNAPSHOT_ON_CHANGE","ORDER_SUBMIT",
                        "ORDER_ACK","ORDER_CANCEL"):
            pass
        elif etype not in ("SNAPSHOT_COMPACT","SNAPSHOT_ON_CHANGE",
                           "ORDER_INTENT","ORDER_FILL","ORDER_SUBMIT",
                           "ORDER_ACK","ORDER_CANCEL","DECISION"):
            filt = {k:v for k,v in event.items()
                    if k not in ("event_type","ts","run_id","schema_version","bot_version")}
            print(f"[{etype}] {json.dumps(filt, default=str)}")

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS: book fields, correlation IDs, maker/taker
# ═══════════════════════════════════════════════════════════════════════════════
def build_book_fields(up_book, dn_book, outcome=None) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "up_bid":up_book.bid,"up_ask":up_book.ask,"up_mid":up_book.mid,
        "up_spread":up_book.spread,"up_imb":up_book.imb,
        "dn_bid":dn_book.bid,"dn_ask":dn_book.ask,"dn_mid":dn_book.mid,
        "dn_spread":dn_book.spread,"dn_imb":dn_book.imb,
    }
    if outcome:
        ref = up_book if outcome == "Up" else dn_book
        d.update({"ref_bid":ref.bid,"ref_ask":ref.ask,"ref_mid":ref.mid,
                   "ref_spread":ref.spread,"ref_imb":ref.imb})
    return d

def new_decision_id() -> str:
    return uuid.uuid4().hex

def new_order_id() -> str:
    return uuid.uuid4().hex[:16]

def new_position_id() -> str:
    return uuid.uuid4().hex

def infer_maker_taker(side: str, fill_price: float, book) -> str:
    if side == "BUY":
        if fill_price <= book.bid + 1e-6: return "maker"
        if fill_price >= book.ask - 1e-6: return "taker"
        return "mid"
    else:
        if fill_price >= book.ask - 1e-6: return "maker"
        if fill_price <= book.bid + 1e-6: return "taker"
        return "mid"

def spread_capture_fields(side: str, fill_price: float, book) -> Dict[str, Any]:
    if side == "BUY":
        did_cross = fill_price >= book.ask - 1e-6
    else:
        did_cross = fill_price <= book.bid + 1e-6
    return {"did_cross": did_cross}
