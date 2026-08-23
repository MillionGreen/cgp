"""
MARKETOS INSTITUTIONAL v12.1 — FYERS EVIDENCE / EDGE ENGINE
Evidence-first / replay-first architecture
==========================================================
WHAT'S NEW:
  Uses Fyers TBT WebSocket for true 50-level market depth
  WebSocket URL: wss://rtsocket-api.fyers.in/versova
  Protobuf binary protocol — fastest available feed
  Price/100 decoded correctly
  Up to 50 bid + 50 ask levels in real-time
  Threading lock for race condition safety
  Falls back to FyersDataSocket if TBT fails
  Falls back to REST polling as last resort
  Full 50-level truth recorder with transport/data-quality metadata
  Raw protobuf path is opt-in diagnostic (disabled by default after observed 403)
  Persistent 5-point liquidity map: NIFTY ±500 / BANKNIFTY ±1000 search envelope
  Map coverage is reported from observed data; unobserved prices are never invented
  Trade targets are sourced from directional mapped liquidity before rail/fallback targets
  Trade Card separates visible book, persistent map, liquidity-path state, risk and empirical edge

SETUP:
  pip install fyers-apiv3 websockets protobuf
  Set FYERS_CLIENT_ID and FYERS_SECRET_KEY environment variables
  Place msg_pb2.py in same folder as this file
  python orderbook_engine.py

IMPORTANT v11.1 policy:
  TBQ/TSQ are exchange-reported book totals, not CVD.
  Execution absorption/iceberg/CVD remain unavailable unless the supported
  callback exposes a genuine classified trade stream.
  The persistent liquidity map is observational, not a guarantee of future price.

FILES NEEDED (same folder):
  orderbook_engine.py  — this file
  msg_pb2.py           — protobuf parser (included in download)
"""
import traceback
import os, sys, time, json, threading, webbrowser, calendar, asyncio
from datetime import datetime, date, timedelta
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ============================================================
# CREDENTIALS
# ============================================================
# Primary: explicit environment variables. Legacy fallback is retained so the
# existing v11 edge-engine installation continues to start without re-entering
# credentials. Replace/rotate these credentials if they have ever been exposed.
CLIENT_ID  = os.environ.get("FYERS_CLIENT_ID", "YLD8YJ8FW7-100").strip()
SECRET_KEY = os.environ.get("FYERS_SECRET_KEY", "Q32PCCFL0I").strip()
REDIRECT   = "https://trade.fyers.in/api-login/redirect-uri/index.html"
TOKEN_FILE = "fyers_token.txt"
PORT       = 8766

TBT_URL    = "wss://rtsocket-api.fyers.in/versova"

# v6 operating policy. Raw protobuf is diagnostic-only because the deployed
# endpoint has returned HTTP 403 in live tests. The supported FyersTbtSocket
# remains the production transport. Truth recording is ON by default so the
# engine builds the replay dataset required for empirical edge discovery.
RAW_TBT_ENABLED = os.environ.get("MARKETOS_RAW_TBT", "0").strip().lower() in ("1", "true", "yes", "on")
TRUTH_RECORD_ENABLED = os.environ.get("MARKETOS_TRUTH_RECORD", "1").strip().lower() in ("1", "true", "yes", "on")
TRUTH_RECORD_DIR = os.environ.get("MARKETOS_TRUTH_DIR", ".")
TRUTH_SCHEMA = "marketos.fyers.truth.v1"
REPLAY_MODE = False

# ============================================================
# SYMBOL
# ============================================================
NSE_EXPIRY_SHIFTS = {
    "2025-10-21": "2025-10-20",
    "2025-03-14": "2025-03-13",
}
def holiday_adjust(d):
    from datetime import datetime as _dt
    key = d.strftime("%Y-%m-%d")
    if key in NSE_EXPIRY_SHIFTS:
        adj = _dt.strptime(NSE_EXPIRY_SHIFTS[key],"%Y-%m-%d").date()
        print("[EXPIRY] Holiday shift: {} -> {}".format(d,adj)); return adj
    return d

def monthly_sym(base):
    """
    NSE Index Futures expiry (updated 2025):
      NIFTY     = last TUESDAY of month
      BANKNIFTY = last TUESDAY of month
      Both changed to Tuesday.
    """
    today = date.today()
    expiry_weekday = 1  # 1 = Tuesday for both

    def last_tuesday(y, m):
        d = date(y, m, calendar.monthrange(y, m)[1])
        while d.weekday() != expiry_weekday:
            d -= timedelta(days=1)
        return d

    exp = last_tuesday(today.year, today.month)
    if today > exp:
        m2 = today.month % 12 + 1
        y2 = today.year + (1 if today.month == 12 else 0)
        exp = last_tuesday(y2, m2)
    exp = holiday_adjust(exp)
    return "NSE:{}{}{}FUT".format(base, exp.strftime('%y'), exp.strftime('%b').upper())

NF_SYM  = monthly_sym("NIFTY")
BNF_SYM = monthly_sym("BANKNIFTY")

def get_active_sym(base="NIFTY"):
    """
    Always returns the currently active futures contract.
    Recalculates on every call — automatic expiry rollover.
    NIFTY expiry    = last Tuesday  of month
    BANKNIFTY expiry = last Tuesday of month
    """
    return monthly_sym(base)

# ============================================================
# THREAD LOCK
# ============================================================
S_LOCK = threading.Lock()

# ============================================================
# FYERS AUTH CONFIGURATION VALIDATION
# ============================================================
def _load_runtime_fyers_credentials():
    """Legacy credential mode. Keep the working v11 behavior:
    environment variables may override the configured credentials, but there
    is no interactive prompt in this production build.
    """
    global CLIENT_ID, SECRET_KEY
    CLIENT_ID = os.environ.get("FYERS_CLIENT_ID", "YLD8YJ8FW7-100").strip()
    SECRET_KEY = os.environ.get("FYERS_SECRET_KEY", "Q32PCCFL0I").strip()
    return CLIENT_ID, SECRET_KEY


def validate_fyers_credentials():
    """Fail closed before any FYERS auth URL can be generated."""
    missing = []
    if not CLIENT_ID:
        missing.append("FYERS_CLIENT_ID")
    if not SECRET_KEY:
        missing.append("FYERS_SECRET_KEY")

    if missing:
        raise RuntimeError(
            "Missing FYERS API credential(s): {}. "
            "Set the environment variables before starting MarketOS."
            .format(", ".join(missing))
        )

    placeholder_ids = {"YOUR_CLIENT_ID", "YOUR_APP_ID", "YOUR_FYERS_CLIENT_ID"}
    placeholder_secrets = {"YOUR_SECRET", "YOUR_SECRET_KEY", "YOUR_FYERS_SECRET_KEY"}
    if CLIENT_ID.upper() in placeholder_ids:
        raise RuntimeError("FYERS_CLIENT_ID is still a placeholder.")
    if SECRET_KEY.upper() in placeholder_secrets:
        raise RuntimeError("FYERS_SECRET_KEY is still a placeholder.")

    print("[FYERS AUTH] API credentials detected; secret not displayed.")


# ============================================================
# TOKEN
# ============================================================
def get_token():
    validate_fyers_credentials()

    if os.path.exists(TOKEN_FILE):
        try:
            d, t = open(TOKEN_FILE).read().strip().split("|", 1)
            if d == date.today().strftime("%Y-%m-%d"):
                try:
                    from fyers_apiv3 import fyersModel
                    tc = fyersModel.FyersModel(client_id=CLIENT_ID, token=t, is_async=False, log_path="")
                    r  = tc.get_profile()
                    if r.get("code") == 200 or r.get("s") == "ok":
                        print("Token validated OK.")
                        return t
                    else:
                        print("Token rejected ({}), regenerating.".format(r.get("code", r.get("s"))))
                        os.remove(TOKEN_FILE)
                except Exception as ve:
                    print("Token validation error: " + str(ve) + " — regenerating.")
                    try: os.remove(TOKEN_FILE)
                    except: pass
            else:
                print("Token expired, regenerating.")
                os.remove(TOKEN_FILE)
        except Exception as e:
            print("Token cache error: " + str(e))
            try: os.remove(TOKEN_FILE)
            except: pass

    from fyers_apiv3 import fyersModel
    session = fyersModel.SessionModel(
        client_id=CLIENT_ID, secret_key=SECRET_KEY,
        redirect_uri=REDIRECT, response_type="code",
        grant_type="authorization_code")
    url = session.generate_authcode()
    print("\nOpen this URL:\n" + url)
    webbrowser.open(url)
    redirected = input("\nPaste redirect URL or auth_code:\n> ").strip()
    code = None
    if redirected.startswith("eyJ"): code = redirected
    if not code:
        try: code = parse_qs(urlparse(redirected).query).get("auth_code",[None])[0]
        except: pass
    if not code:
        for p in redirected.replace("&","?").split("?"):
            if "auth_code=" in p: code = p.split("auth_code=")[1].split("&")[0]
    if not code and redirected and " " not in redirected: code = redirected
    if not code: raise ValueError("Could not extract auth_code")
    session.set_token(code)
    resp = session.generate_token()
    tok  = resp.get("access_token")
    if not tok: raise ValueError("Token generation failed: " + str(resp))
    open(TOKEN_FILE,"w").write(date.today().strftime("%Y-%m-%d") + "|" + tok)
    print("Token saved.")
    return tok

# ============================================================
# SHARED STATE
# ============================================================
S = {
    "sym":"NIFTY","sym_str":NF_SYM,"live":False,"feed":"--",
    "spot":None,"bids":[],"asks":[],
    "bp":50,"ap":50,"tb":0,"ta":0,"sig":"BALANCED","dirn":"NEUTRAL",
    "nbp":50,"nap":50,"nsig":"BALANCED",
    "dom":None,"wsig":"NONE","bw":[],"aw":[],
    "absorb":{"active":False,"side":"NONE","signal":"NONE","price":0},
    "iceberg":{"detected":False,"side":"NONE","signal":"NONE","price":0},
    "delta":0,"sess_delta":0,"cum_delta":0,
    "delta_trend":"NEUTRAL","delta_hist":deque(maxlen=20),
    "vb":None,"va":None,"dr":1.0,"conc":50,"sup":[],"res":[],
    "dsig":"NEUTRAL","dc":"neutral","dst":0,"bull":0,"bear":0,"sigs":[],
    "prev_bids":{},"prev_asks":{},
    "tot_buy_qty":0,"tot_sell_qty":0,
    # Flow integrity: actual trade-flow is used only when trade prints are observed.
    # tbq/tsq are retained as exchange-reported depth totals, NEVER as CVD.
    "cvd_hist":deque(maxlen=200),
    "proxy_cvd_hist":deque(maxlen=200),
    "cvd_session":0, "proxy_cvd_session":0,
    "actual_buy_volume":0, "actual_sell_volume":0,
    "trade_count":0, "trade_total_qty":0, "trade_classified_qty":0, "trade_unclassified":0,
    "last_trade_key":None, "last_trade_ltt":None, "last_trade_price":None,
     "last_trade":None, "last_vtt":None, "last_vtt_diff":None,
     "prev_vtt":None, "vtt_delta_qty":0, "ltq_sum":0, "vtt_recon_samples":0, "vtt_recon_abs_error":0,
    "prev_tbq":0,"prev_tsq":0,
    "last_feed_ts":0, "last_sequence":None,
    "local_update_seq":0, "data_quality":{}, "book_span":{},
    "quote_available":False, "quote_fields_seen":[],
    "quote_probe_ticks":0, "quote_probe_populated_ticks":0,
    "quote_probe_ltp":0, "quote_probe_ltt":0, "quote_probe_ltq":0,
    "quote_probe_vtt":0, "quote_probe_vtt_diff":0,
    "last":"--","err":None,"alerts":deque(maxlen=100),
    "tick_count":0,"depth_levels":0,"level_memory":[],"spoof_count":0,
    "sweep":{"detected":False,"side":"NONE","signal":"","levels":0,"volume":0,"confirmed":False},
    "vacuum":{"detected":False,"side":"NONE","signal":"","pct":0},
    "market_status":"Checking...",
    "liquidity_path":{}, "liquidity_path_alerts":deque(maxlen=50),
    "decision":{}, "session_risk":{}, "trade_stats":{},
    "sup_hist5": deque(maxlen=3), "res_hist5": deque(maxlen=3),  # 5-min history snapshots
    "sup_hist30": deque(maxlen=3), "res_hist30": deque(maxlen=3),  # 30-min history
    "sup_hist5_ts": 0, "res_hist5_ts": 0,
    "sup_hist30_ts": 0, "res_hist30_ts": 0,
}

WALL_NF=1500; WALL_BNF=800

# Measurement integrity:
# tbq/tsq are exchange-reported depth totals. Their changes are NOT CVD.
# Actual CVD is enabled only when ltp/ltt/ltq (or an equivalent trade print) is observed.
# Until then all flow-derived conclusions remain explicitly PROXY.
FLOW_QUALITY = "PROXY"
RAW_BOOKS = {}
RAW_TBT_STOP = None

# ============================================================
# ANALYTICS
# ============================================================

# ============================================================
# MODULE 3: ROLLING STATISTICS
# Replaces all fixed thresholds with adaptive percentiles
# Self-calibrates every tick to current market conditions
# ============================================================
import collections, math, dataclasses
from typing import Optional

class RollingStats:
    """
    Maintains rolling window of values.
    Provides: mean, std, percentile, z-score.
    Used by all detectors instead of fixed constants.
    """
    def __init__(self, maxlen=200):
        self.data   = collections.deque(maxlen=maxlen)
        self.maxlen = maxlen

    def add(self, v):
        if v is not None and not math.isnan(float(v)):
            self.data.append(float(v))

    def mean(self):
        return sum(self.data)/len(self.data) if self.data else 0.0

    def std(self):
        if len(self.data) < 2: return 1.0
        m = self.mean()
        return math.sqrt(sum((x-m)**2 for x in self.data)/len(self.data)) or 1.0

    def percentile(self, p):
        if not self.data: return 0.0
        s = sorted(self.data)
        k = (len(s)-1) * p/100
        f,c = int(k), math.ceil(k)
        return s[f] if f==c else s[f]*(c-k)+s[c]*(k-f)

    def zscore(self, v):
        s = self.std()
        return (v - self.mean())/s if s>0 else 0.0

    def ready(self, min_samples=30):
        return len(self.data) >= min_samples

# Global rolling stats instances
RS = {
    "bid_pct":      RollingStats(200),   # imbalance history
    "qty_drop":     RollingStats(200),   # qty drop magnitudes
    "wall_qty":     RollingStats(200),   # wall sizes
    "cvd_delta":    RollingStats(200),   # REAL CVD per tick only
    "spread":       RollingStats(100),   # bid-ask spread
    "total_bid":    RollingStats(100),   # total bid depth
    "total_ask":    RollingStats(100),   # total ask depth
    "level_count":  RollingStats(100),   # active levels count
}

def adaptive_wall_threshold(bnf=False):
    """Wall threshold = 85th percentile of recent wall qtys."""
    base = 800 if bnf else 1500
    if RS["wall_qty"].ready():
        return max(base, RS["wall_qty"].percentile(85))
    return base

def adaptive_absorption_threshold():
    """Absorption = qty drop > 80th percentile of recent drops."""
    if RS["qty_drop"].ready():
        return RS["qty_drop"].percentile(80)
    return 0.50  # fallback

def adaptive_imbalance_threshold():
    """Strong imbalance = above 80th percentile of recent bid%."""
    if RS["bid_pct"].ready(20):
        return RS["bid_pct"].percentile(80)
    return 62.0  # fallback


# ============================================================
# MODULE 4: PRICE LEVEL STATE
# Full lifecycle tracking per price level
# Replaces crude _level_memory dict
# ============================================================

class PriceLevelState:
    """
    Tracks the complete lifecycle of a price level.
    States: NEW → ACTIVE → HIT → REFILLED → CANCELLED → ABSORBED
    Distinguishes execution from cancellation using:
      - executed_vol (ACTUAL classified trades)
      - times_hit (significant qty drops)
      - refill_count (qty rebuilt after drops)
      - lifetime (age in seconds)
    """
    STATES = ["NEW","ACTIVE","HIT","REFILLED","CANCELLED","ABSORBED","REMOVED"]

    def __init__(self, price, qty, side, ts):
        self.price         = price
        self.side          = side
        self.born          = ts
        self.last_seen     = ts
        self.current_qty   = qty
        self.prev_qty      = qty
        self.peak_qty      = qty
        self.peak_time     = ts
        self.lowest_qty    = qty
        self.times_hit     = 0
        self.times_refilled= 0
        self.times_cancelled=0
        self.executed_vol  = 0      # ACTUAL classified trade volume at this exact price
        self.depletion_events = 0
        self.depleted_volume = 0
        self.depletion_events_at_last_refill = 0
        self.last_execution_qty = 0
        self.last_execution_side = "UNKNOWN"
        self.state         = "NEW"
        self.ticks_alive   = 1
        self.evidence_count= 0      # how many ticks confirmed this level
        self.qty_history   = deque(maxlen=20)  # qty samples for velocity calc
        self.velocity      = 0.0    # qty change per second (growing=+, draining=-)

    @property
    def lifetime(self):
        import time as _t
        return max(0.0, self.last_seen - self.born)

    @property
    def lifetime_class(self):
        lt = self.lifetime
        if lt < 5:    return "FLASH"
        if lt < 30:   return "SHORT"
        if lt < 120:  return "NORMAL"
        return "INSTITUTIONAL"

    @property
    def age_str(self):
        lt = int(self.lifetime)
        return "{}s".format(lt) if lt<60 else "{}m{}s".format(lt//60,lt%60)

    def update(self, qty, ts):
        """Update displayed liquidity only. A quantity drop is NOT an execution."""
        self.prev_qty = self.current_qty
        self.current_qty = qty
        self.last_seen = ts
        self.ticks_alive += 1
        if qty > self.peak_qty:
            self.peak_qty = qty
            self.peak_time = ts
        if qty < self.lowest_qty:
            self.lowest_qty = qty

        self.qty_history.append((ts, qty))
        if len(self.qty_history) >= 2:
            dt = ts - self.qty_history[0][0]
            self.velocity = (qty - self.qty_history[0][1]) / dt if dt > 0 else 0.0

        change_pct = (qty - self.prev_qty) / self.prev_qty if self.prev_qty > 0 else 0.0
        if change_pct < -0.20 and self.prev_qty >= adaptive_wall_threshold():
            drop_vol = self.prev_qty - qty
            RS["qty_drop"].add(abs(change_pct))
            self.depletion_events = getattr(self, "depletion_events", 0) + 1
            self.depleted_volume = getattr(self, "depleted_volume", 0) + int(drop_vol)
            # Do NOT classify this as HIT or cancellation. Without a trade print
            # the cause of a displayed-quantity decrease is unobservable.
            self.state = "DEPLETING"
        elif change_pct > 0.20:
            if getattr(self, "depletion_events", 0) > getattr(self, "depletion_events_at_last_refill", 0):
                self.times_refilled += 1
                self.depletion_events_at_last_refill = getattr(self, "depletion_events", 0)
                self.state = "REFILLED"
            else:
                self.state = "ACTIVE"
        elif abs(change_pct) <= 0.10 and self.state in ("NEW", "REFILLED", "DEPLETING"):
            self.state = "ACTIVE"

    def register_trade(self, qty, trade_side):
        """Attach an ACTUAL classified trade to this exact displayed price level."""
        q = int(max(0, qty or 0))
        if q <= 0:
            return
        self.executed_vol += q
        self.times_hit += 1
        self.evidence_count += 1
        self.last_execution_qty = q
        self.last_execution_side = trade_side
        self.state = "HIT"

    def is_iceberg(self):
        """Consistent refill pattern = iceberg."""
        return self.times_refilled >= 2 and self.times_hit >= 2

    def is_spoof(self):
        """Conservative: a vanished level is not called spoof without cancellation evidence."""
        return False

    def is_genuine_wall(self):
        """Long-lived, stable, not spoof."""
        return self.lifetime > 30 and self.times_cancelled == 0

    def absorption_confidence(self):
        """0-100 confidence this level is being absorbed."""
        if self.times_hit == 0: return 0
        score = 0
        score += min(40, self.times_hit * 10)       # execution count
        score += min(20, int(self.executed_vol/100)) # executed volume
        score += 20 if self.lifetime > 10 else 0    # persisted long enough
        score += 20 if self.times_refilled > 0 else 0  # iceberg = high conviction
        return min(100, score)

    @property
    def velocity_indicator(self):
        """Returns (symbol, css_class) for velocity display."""
        v = self.velocity
        if v > 50:   return "▲▲", "bull"
        if v > 10:   return "▲", "mild-bull"
        if v < -50:  return "▼▼", "bear"
        if v < -10:  return "▼", "mild-bear"
        return "→", "neut"

    def zone_qty(self, all_levels, tolerance=2.5):
        """Cumulative qty of all levels within tolerance of this price."""
        total = self.current_qty
        for lv in all_levels:
            if lv is self: continue
            lp = lv.price if hasattr(lv, "price") else lv.get("price", 0)
            lq = lv.current_qty if hasattr(lv, "current_qty") else lv.get("qty", 0)
            if abs(lp - self.price) <= tolerance:
                total += lq
        return total


class PriceLevelBook:
    """
    Manages PriceLevelState objects for all active levels.
    Single source of truth for level state.
    """
    def __init__(self):
        self.levels = {}   # price_key -> PriceLevelState

    def update(self, bids, asks, event_ts=None):
        now = float(event_ts) if event_ts is not None else time.time()
        current_keys = set()

        for b in bids:
            k = round(b["price"],1)
            current_keys.add(k)
            RS["wall_qty"].add(b["qty"])
            if k not in self.levels:
                self.levels[k] = PriceLevelState(k, b["qty"], "BID", now)
            else:
                self.levels[k].update(b["qty"], now)

        for a in asks:
            k = round(a["price"],1)
            current_keys.add(k)
            RS["wall_qty"].add(a["qty"])
            if k not in self.levels:
                self.levels[k] = PriceLevelState(k, a["qty"], "ASK", now)
            else:
                self.levels[k].update(a["qty"], now)

        # Handle departed levels
        for k in list(self.levels.keys()):
            if k not in current_keys:
                lv = self.levels[k]
                if lv.state not in ("CANCELLED","ABSORBED","REMOVED"):
                    lv.state = "REMOVED"
                # Keep for 5s then purge
                import time as _t2
                if _t2.time() - lv.last_seen > 5:
                    del self.levels[k]

    def register_trade(self, price, qty, aggressor_side):
        """Attribute an actual classified trade only to the exact matching book level."""
        if price is None or qty is None or aggressor_side not in ("BUY", "SELL"):
            return False
        lv = self.get(price)
        if not lv:
            return False
        expected_side = "ASK" if aggressor_side == "BUY" else "BID"
        if lv.side != expected_side:
            return False
        lv.register_trade(qty, aggressor_side)
        return True

    def significant_levels(self, spot, bnf=False):
        """Return significant levels sorted by distance from spot."""
        wq = adaptive_wall_threshold(bnf)
        sig = [lv for lv in self.levels.values()
               if lv.current_qty >= wq or lv.state in ("ABSORBED","HIT","REFILLED")]
        sig.sort(key=lambda x: abs(x.price - spot) if spot else 0)
        return sig[:10]

    def get(self, price):
        return self.levels.get(round(price,1))


# Global price level book
PLB = PriceLevelBook()


# ============================================================
# MODULE 2: EVIDENCE OBJECTS
# Replaces raw alert strings with structured evidence
# ============================================================
import dataclasses

@dataclasses.dataclass
class Evidence:
    """Structured evidence from a detector."""
    detector:    str        # ABSORPTION/ICEBERG/SWEEP/VACUUM/WALL
    side:        str        # BULLISH/BEARISH/NEUTRAL
    price:       float      # price level
    confidence:  float      # 0-100
    signal:      str        # human readable
    timestamp:   float      # unix timestamp
    cvd_confirmed: bool = False
    extra:       dict = dataclasses.field(default_factory=dict)

    def __str__(self):
        conf_str = " [{:.0f}%]".format(self.confidence) if self.confidence>0 else ""
        cvd_str  = " (CVD✓)" if self.cvd_confirmed else ""
        return "{}{}{}: {}".format(self.detector, conf_str, cvd_str, self.signal)


# ============================================================
# MODULE 7: EVENT ENGINE (simplified)
# Clusters Evidence objects into institutional events
# Same level + same detector within 30s = one event
# Eliminates repeated alerts for ongoing events
# ============================================================
import time as _evt_t

class InstitutionalEvent:
    """
    A cluster of Evidence objects representing one institutional activity.
    """
    def __init__(self, ev: Evidence):
        self.detector    = ev.detector
        self.side        = ev.side
        self.price       = ev.price
        self.started     = ev.timestamp
        self.last_update = ev.timestamp
        self.evidence    = [ev]
        self.state       = "BUILDING"   # BUILDING/CONFIRMED/WEAKENING/DONE
        self.max_confidence = ev.confidence
        self.cvd_ticks   = 1 if ev.cvd_confirmed else 0

    def add_evidence(self, ev: Evidence):
        self.evidence.append(ev)
        self.last_update = ev.timestamp
        self.max_confidence = max(self.max_confidence, ev.confidence)
        if ev.cvd_confirmed: self.cvd_ticks += 1
        # Update state
        n = len(self.evidence)
        if n >= 5 and self.cvd_ticks >= 2:  self.state = "CONFIRMED"
        elif n >= 3:                          self.state = "BUILDING"

    @property
    def duration(self):
        return max(0.0, self.last_update - self.started)

    @property
    def duration_str(self):
        d = int(self.duration)
        return "{}s".format(d) if d<60 else "{}m{}s".format(d//60,d%60)

    @property
    def is_stale(self):
        return _evt_t.time() - self.last_update > 30

    def summary(self):
        return "[{}] {} | {} | {}conf | {}ticks | CVD:{} | {}".format(
            self.state, self.detector, self.side,
            int(self.max_confidence), len(self.evidence),
            self.cvd_ticks, self.duration_str)


class EventEngine:
    """
    Maintains active institutional events.
    Prevents alert flooding by clustering repeated evidence.
    """
    MERGE_WINDOW   = 15.0   # seconds — same level within 30s = same event
    PRICE_TOLERANCE= 2.0    # points — levels within 2pts = same zone
    MAX_EVENTS     = 20

    def __init__(self):
        self.events = []   # list of InstitutionalEvent

    def process(self, ev: Evidence) -> Optional[InstitutionalEvent]:
        """
        Process Evidence. Returns event if it changed state (new/confirmed).
        Returns None if evidence merged into existing event silently.
        """
        now = ev.timestamp

        # Purge stale events
        self.events = [e for e in self.events if not e.is_stale]

        # Find matching existing event
        for event in self.events:
            if (event.detector == ev.detector and
                abs(event.price - ev.price) <= self.PRICE_TOLERANCE and
                now - event.last_update <= self.MERGE_WINDOW):
                prev_state = event.state
                event.add_evidence(ev)
                # Only return if state changed (BUILDING→CONFIRMED)
                if event.state != prev_state:
                    return event
                return None  # merged silently — no new alert

        # New event
        new_event = InstitutionalEvent(ev)
        self.events.append(new_event)
        if len(self.events) > self.MAX_EVENTS:
            self.events = self.events[-self.MAX_EVENTS:]
        return new_event   # always return new events

    def active_events(self):
        return [e for e in self.events if not e.is_stale]

    def confirmed_events(self):
        return [e for e in self.events if e.state=="CONFIRMED" and not e.is_stale]


# Global event engine
EV_ENGINE = EventEngine()

def emit_evidence(detector, side, price, confidence, signal,
                  cvd_confirmed=False, extra=None):
    """
    Create Evidence and process through Event Engine.
    Returns (evidence, event_or_None)
    """
    import time as _t
    ev = Evidence(
        detector=detector, side=side, price=price,
        confidence=float(confidence), signal=signal,
        timestamp=float(S.get("last_feed_ts") or _t.time()), cvd_confirmed=cvd_confirmed,
        extra=extra or {}
    )
    event = EV_ENGINE.process(ev)
    return ev, event

def _get_age(p):
    """Get age from the event clock; never use wall-clock time for replay state."""
    lv = PLB.get(round(p, 1))
    if lv:
        return int(max(0.0, lv.last_seen - lv.born))
    return 0

# Session-level age cache for levels before PLB tracks them
_level_born = {}

def rolling_cvd(window=5):
    """Rolling REAL CVD only. Empty means unavailable, not zero-flow."""
    h = list(S.get("cvd_hist", []))
    return sum(h[-window:]) if h else 0


def rolling_proxy_flow(window=5):
    """Book-total flow proxy derived from positive tbq/tsq increments. Never CVD."""
    h = list(S.get("proxy_cvd_hist", []))
    return sum(h[-window:]) if h else 0


def _safe_num(v, default=0):
    try:
        if v is None: return default
        # Fyers protobuf variants may expose scalar wrappers with .value.
        if hasattr(v, "value") and not isinstance(v, (int, float, str, bytes)):
            v = v.value
        return float(v)
    except Exception:
        return default


def _get_field(obj, *names):
    """Read a field from an SDK/protobuf object or mapping."""
    for name in names:
        try:
            if isinstance(obj, dict) and name in obj:
                return obj[name]
            if hasattr(obj, name):
                return getattr(obj, name)
        except Exception:
            pass
    return None


# QUOTE PROBE / TRADE-FLOW VALIDATION
# ------------------------------------
# This layer is deliberately non-invasive. It records Quote fields when the
# selected Fyers transport exposes them. It never treats TBQ/TSQ as CVD and
# never invents a trade when LTQ/LTP are absent. Actual CVD remains dependent
# on observed/classifiable trade prints.
def extract_trade_fields(obj):
    """Best-effort extraction of Fyers quote/trade fields.

    The current FyersTbtSocket callback is documented by the supplied engine as
    delivering a Depth object. Some SDK/protobuf variants expose quote fields
    either on that object or as a nested quote/message object. We probe both.
    We do NOT manufacture a trade when these fields are absent.
    """
    candidates = [obj]
    for child_name in ("quote", "Quote", "quote_data", "market_feed", "feed", "message"):
        child = _get_field(obj, child_name)
        if child is not None: candidates.append(child)
    out = {}
    aliases = {
        "ltp": ("ltp", "last_price", "last_traded_price"),
        "ltt": ("ltt", "last_traded_time", "last_trade_time"),
        "ltq": ("ltq", "last_traded_quantity", "last_trade_qty"),
        "vtt": ("vtt", "volume_traded_today", "volume"),
        "vtt_diff": ("vtt_diff", "volume_traded_today_diff", "volume_diff"),
        "sequence_no": ("sequence_no", "seq_no", "sequence"),
        "feed_time": ("feed_time", "feed_ts"),
        "send_time": ("send_time", "send_ts"),
    }
    for key, names in aliases.items():
        for c in candidates:
            v = _get_field(c, *names)
            if v is not None:
                out[key] = v
                break
    return out


def _classify_trade(price, bid, ask, prev_price=None):
    """Quote-test + tick-rule trade classification.

    Returns BUY, SELL or UNKNOWN. Trades inside the spread are not forced into
    a side unless the tick rule can resolve them.
    """
    p, b, a = _safe_num(price), _safe_num(bid), _safe_num(ask)
    if p <= 0: return "UNKNOWN"
    if a > 0 and p >= a: return "BUY"
    if b > 0 and p <= b: return "SELL"
    pp = _safe_num(prev_price)
    if pp > 0:
        if p > pp: return "BUY"
        if p < pp: return "SELL"
    return "UNKNOWN"


def _event_timestamp(feed_meta=None, trade=None):
    """Normalize Fyers feed/send timestamps to epoch seconds when possible."""
    for raw in ((feed_meta or {}).get("feed_time"), (trade or {}).get("ltt"),
                (feed_meta or {}).get("send_time")):
        x = _safe_num(raw, 0)
        if x > 0:
            # Fyers timestamps are commonly seconds or milliseconds.
            return x / 1000.0 if x > 10_000_000_000 else x
    return time.time()


def process_trade_fields(trade, best_bid=0, best_ask=0):
    """Process an observed Fyers Quote trade print.

    CVD is constructed ONLY from classified ltq trade events. tbq/tsq and
    vtt/vtt_diff are never used as aggressor-side flow. Quote fields are
    retained for reconciliation diagnostics.
    """
    global FLOW_QUALITY
    if not trade:
        return 0
    price = _safe_num(trade.get("ltp"))
    qty = int(max(0, round(_safe_num(trade.get("ltq")))))
    ltt = trade.get("ltt")
    vtt = _safe_num(trade.get("vtt"))
    vtt_diff = _safe_num(trade.get("vtt_diff"))
    seq = trade.get("sequence_no")
    if price <= 0 or qty <= 0:
        return 0

    # De-duplicate identical quote/trade updates.
    key = (ltt, round(price, 4), qty, seq)
    if key == S.get("last_trade_key"):
        return 0

    side = _classify_trade(price, best_bid, best_ask, S.get("last_trade_price"))
    S["last_trade_key"] = key
    S["last_trade_ltt"] = ltt
    S["last_trade_price"] = price
    S["quote_available"] = True
    if seq is not None:
        S["last_sequence"] = seq
    S["trade_count"] = S.get("trade_count", 0) + 1
    S["trade_total_qty"] = S.get("trade_total_qty", 0) + qty
    S["ltq_sum"] = S.get("ltq_sum", 0) + qty

    # VTT reconciliation: when VTT is monotonic, compare its increment to LTQ.
    prev_vtt = S.get("prev_vtt")
    if vtt > 0 and prev_vtt is not None and vtt >= prev_vtt:
        dv = int(vtt - prev_vtt)
        S["vtt_delta_qty"] = S.get("vtt_delta_qty", 0) + dv
        S["vtt_recon_samples"] = S.get("vtt_recon_samples", 0) + 1
        S["vtt_recon_abs_error"] = S.get("vtt_recon_abs_error", 0) + abs(dv - qty)
    if vtt > 0:
        S["prev_vtt"] = vtt

    if side == "BUY":
        delta = qty
        S["actual_buy_volume"] = S.get("actual_buy_volume", 0) + qty
        S["trade_classified_qty"] = S.get("trade_classified_qty", 0) + qty
    elif side == "SELL":
        delta = -qty
        S["actual_sell_volume"] = S.get("actual_sell_volume", 0) + qty
        S["trade_classified_qty"] = S.get("trade_classified_qty", 0) + qty
    else:
        S["trade_unclassified"] = S.get("trade_unclassified", 0) + qty
        delta = 0

    S["last_trade_side"] = side
    S["last_actual_delta"] = delta
    S["_pending_level_trade"] = None
    if side != "UNKNOWN":
        S["cvd_hist"].append(delta)
        S["cvd_session"] = S.get("cvd_session", 0) + delta
        S["trade_classified_events"] = S.get("trade_classified_events", 0) + 1
        FLOW_QUALITY = "REAL"
        # Exact-price attribution is performed after the current book snapshot
        # has been loaded into PLB.
        S["_pending_level_trade"] = {"price": price, "qty": qty, "side": side}

    S["last_vtt"] = vtt
    S["last_vtt_diff"] = vtt_diff
    S["last_trade"] = {"price":price,"qty":qty,"side":side,"ltt":ltt,
                        "vtt":vtt,"vtt_diff":vtt_diff,"sequence_no":seq}
    return delta

def update_proxy_flow(tbq, tsq):
    """Update tbq/tsq proxy without ever labelling it CVD."""
    with S_LOCK:
        prev_tbq = S.get("prev_tbq", 0)
        prev_tsq = S.get("prev_tsq", 0)
        proxy_delta = max(0, int(tbq or 0) - int(prev_tbq)) - max(0, int(tsq or 0) - int(prev_tsq))
        S["prev_tbq"] = int(tbq or 0)
        S["prev_tsq"] = int(tsq or 0)
        if proxy_delta:
            S["proxy_cvd_hist"].append(proxy_delta)
            S["proxy_cvd_session"] = S.get("proxy_cvd_session", 0) + proxy_delta
        return proxy_delta



# ============================================================
# BOOK-NATIVE MICROSTRUCTURE v1
# Primary signal layer when Fyers TBT exposes depth but no trade tape.
# No TBQ/TSQ-as-CVD. No execution inference from displayed-qty changes.
# ============================================================
class BookMicrostructure:
    """Persistent, replayable microstructure state derived ONLY from depth.

    Observable facts:
      - spread / mid / microprice
      - top-N depth imbalance
      - depth concentration and slope
      - displayed liquidity depletion/replenishment
      - liquidity migration between adjacent zones
      - current visible liquidity vs historical/session liquidity

    It deliberately does NOT call depletion an execution, support a wall,
    or replenishment an iceberg. Those are hypotheses emitted with explicit
    evidence labels later in the pipeline.
    """
    def __init__(self):
        self.prev_bids = {}; self.prev_asks = {}
        self.prev_mid = None; self.prev_micro = None
        self.prev_ts = None
        self.hist = {"spread":deque(maxlen=2000), "imb":deque(maxlen=2000),
                     "micro_move":deque(maxlen=2000), "mid_move":deque(maxlen=2000)}
        self.level_stats = {}  # (side, rounded price) -> stats
        self.last = {}

    @staticmethod
    def _map(levels):
        return {round(float(x["price"]),2): x for x in levels if x.get("price",0)>0 and x.get("qty",0)>0}

    def update(self, bids, asks, now):
        b=self._map(bids); a=self._map(asks)
        if not b or not a:
            return self.last
        bb=max(b); ba=min(a); mid=(bb+ba)/2.0
        bq=sum(x.get("qty",0) for x in bids[:10]); aq=sum(x.get("qty",0) for x in asks[:10])
        imb=(bq-aq)/max(1.0,bq+aq)
        # Microprice weighted by opposite-side top quantity.
        micro=(bb*aq + ba*bq)/max(1.0,bq+aq)
        spread=ba-bb
        dmid=0.0 if self.prev_mid is None else mid-self.prev_mid
        dmicro=0.0 if self.prev_micro is None else micro-self.prev_micro
        self.hist["spread"].append(spread); self.hist["imb"].append(imb)
        self.hist["mid_move"].append(dmid); self.hist["micro_move"].append(dmicro)

        depleted=[]; replenished=[]; migrated=[]
        for side,cur,prev in (("BID",b,self.prev_bids),("ASK",a,self.prev_asks)):
            for p,lv in cur.items():
                old=prev.get(p,{}).get("qty",0)
                q=lv.get("qty",0)
                if old>0 and q<old*0.5:
                    depleted.append({"side":side,"price":p,"before":old,"after":q,"ratio":q/old})
                elif old>0 and q>old*1.5:
                    replenished.append({"side":side,"price":p,"before":old,"after":q,"ratio":q/old})
            # Migration proxy: liquidity disappeared here while a nearby level
            # on the same side increased. This is not cancellation/execution.
            for p,oldlv in prev.items():
                if p not in cur and oldlv.get("qty",0)>0:
                    near=[q for q in cur if abs(q-p)<=5.0]
                    if near: migrated.append({"side":side,"from":p,"to":min(near,key=lambda q:abs(q-p))})

        # Persistent per-price observations.
        for side,levels in (("BID",bids),("ASK",asks)):
            for lv in levels:
                key=(side,round(float(lv["price"]),2)); z=self.level_stats.setdefault(key,{
                    "first":now,"last":now,"samples":0,"max_qty":0,"sum_qty":0,
                    "touches":0,"depletions":0,"replenishments":0})
                z["last"]=now; z["samples"]+=1; z["max_qty"]=max(z["max_qty"],lv["qty"]); z["sum_qty"]+=lv["qty"]
        for x in depleted:
            z=self.level_stats.get((x["side"],x["price"]));
            if z: z["depletions"]+=1
        for x in replenished:
            z=self.level_stats.get((x["side"],x["price"]));
            if z: z["replenishments"]+=1

        self.prev_bids=b; self.prev_asks=a; self.prev_mid=mid; self.prev_micro=micro; self.prev_ts=now
        self.last={
            "mid":mid,"best_bid":bb,"best_ask":ba,"spread":spread,"microprice":micro,
            "top10_bid_qty":bq,"top10_ask_qty":aq,"imbalance":imb,
            "mid_move":dmid,"micro_move":dmicro,
            "depleted":depleted[-20:],"replenished":replenished[-20:],"migrated":migrated[-20:],
            "visible_support":[{"price":x["price"],"qty":x["qty"]} for x in bids[:5]],
            "visible_resistance":[{"price":x["price"],"qty":x["qty"]} for x in asks[:5]],
        }
        return self.last

    def snapshot(self):
        return dict(self.last)

BOOK_MS={"NIFTY":BookMicrostructure(),"BANKNIFTY":BookMicrostructure()}

class OutcomeTracker:
    """Replay/live forward-outcome recorder with event-aware sampling.

    Lessons from the first full-session replay:
      * high-frequency depletion/replenishment events can otherwise crowd out
        rarer events such as sweeps/vacuums in one global deque;
      * the same price event can be emitted on consecutive book updates;
      * therefore outcome storage is partitioned by event family and uses a
        short same-price cooldown.

    This remains an observation/outcome dataset. It does NOT imply that an
    event is a trading signal or that a positive forward move is causal.
    """
    HORIZONS=(5,15,30,60,180)
    EVENT_CAPS={
        "VISIBLE_LIQUIDITY_DEPLETION":12000,
        "VISIBLE_REPLENISHMENT":12000,
        "BOOK_SWEEP":6000,
        "BOOK_VACUUM":6000,
        "TRADE_SETUP":6000,
        "BOOK_PRESSURE":8000,
        "DEFAULT":6000,
    }
    def __init__(self):
        self.pending=deque(maxlen=12000)
        self.completed=deque(maxlen=50000)
        self.completed_by_event={}
        self.last_sample={}
        self.gap_threshold_s=2.0
        self.cooldown_s={
            "VISIBLE_LIQUIDITY_DEPLETION":1.0,
            "VISIBLE_REPLENISHMENT":1.0,
            "BOOK_SWEEP":2.0,
            "BOOK_VACUUM":2.0,
            "TRADE_SETUP":15.0,
            "BOOK_PRESSURE":3.0,
        }
        self.dropped_duplicates=0
        self.dropped_capacity=0

    def add(self,event_name,side,price,now,evidence=None):
        if not price or side not in ("BID","ASK","BUY","SELL","LONG","SHORT"): return False
        try: price=float(price); now=float(now)
        except Exception: return False
        if isinstance(evidence,dict) and evidence.get("stale_gap"):
            return False
        direction=1 if side in ("BID","BUY","LONG") else -1
        # Same event/side/price on adjacent depth updates is not a new event.
        key=(event_name,side,round(price,1))
        cd=self.cooldown_s.get(event_name,0.75)
        prev=self.last_sample.get(key)
        if prev is not None and now-prev < cd:
            self.dropped_duplicates += 1
            return False
        self.last_sample[key]=now
        self.pending.append({"event":event_name,"side":side,"dir":direction,
                             "p0":price,"t0":now,"max_fav":0.0,"max_adv":0.0,
                             "done":set(),"evidence":dict(evidence or {})})
        return True

    def update(self,spot,now):
        if not spot: return
        keep=deque(maxlen=self.pending.maxlen)
        for e in self.pending:
            dt=float(now)-e["t0"]
            move=(float(spot)-e["p0"])*e["dir"]
            e["max_fav"]=max(e["max_fav"],move)
            e["max_adv"]=min(e["max_adv"],move)
            for h in self.HORIZONS:
                if h in e["done"] or dt<h: continue
                e["done"].add(h)
                row={"event":e["event"],"side":e["side"],"t0":e["t0"],
                     "horizon_s":h,"entry":e["p0"],"spot":float(spot),
                     "forward_move":move,"mfe":e["max_fav"],"mae":e["max_adv"],
                     "evidence":e["evidence"]}
                bucket=self.completed_by_event.setdefault(e["event"],deque(maxlen=self.EVENT_CAPS.get(e["event"],self.EVENT_CAPS["DEFAULT"])))
                if len(bucket)>=bucket.maxlen:
                    self.dropped_capacity += 1
                else:
                    bucket.append(row)
                    self.completed.append(row)
            if dt<max(self.HORIZONS): keep.append(e)
        self.pending=keep

    def recent(self,n=100): return list(self.completed)[-n:]

    def event_rows(self,event_name=None,horizon=60,n=None):
        if event_name is None:
            rows=[x for x in self.completed if x["horizon_s"]==horizon]
        else:
            rows=[x for x in self.completed_by_event.get(event_name,()) if x["horizon_s"]==horizon]
        return rows[-n:] if n else rows

    def stats(self,event_name=None,horizon=60,n=None):
        rows=self.event_rows(event_name,horizon,n)
        if not rows: return {"n":0}
        vals=[x["forward_move"] for x in rows]
        wins=sum(1 for v in vals if v>0)
        return {"n":len(rows),"win_rate":wins/len(rows),
                "mean_move":sum(vals)/len(vals),
                "median_move":sorted(vals)[len(vals)//2],
                "mean_mfe":sum(x["mfe"] for x in rows)/len(rows),
                "mean_mae":sum(x["mae"] for x in rows)/len(rows),
                "sample_dropped_duplicates":self.dropped_duplicates,
                "sample_dropped_capacity":self.dropped_capacity}


class OutcomeStore:
    """Durable derived outcome dataset; raw Fyers truth remains separate."""
    def __init__(self): self.path=os.environ.get("MARKETOS_OUTCOME_FILE",""); self._seen=0
    def _path(self):
        if self.path: return self.path
        return os.path.join(TRUTH_RECORD_DIR,"marketos_edge_outcomes_{}_{}.jsonl".format(datetime.now().strftime("%Y%m%d"),S.get("sym","NIFTY")))
    def flush(self,rows):
        if len(rows)<=self._seen: return
        try:
            path=self._path(); os.makedirs(os.path.dirname(os.path.abspath(path)) or ".",exist_ok=True)
            with open(path,"a",encoding="utf-8",buffering=1) as f:
                for r in rows[self._seen:]: f.write(json.dumps(r,separators=(",",":"),ensure_ascii=False)+"\n")
            self._seen=len(rows)
        except Exception as e: S["err"]="outcome store: "+str(e)
OUTCOME_STORE=OutcomeStore()

class EmpiricalEdgeGate:
    """No live GO is treated as validated until forward outcomes support it."""
    def __init__(self):
        self.min_n=int(os.environ.get("MARKETOS_EDGE_MIN_N","100")); self.horizon=int(os.environ.get("MARKETOS_EDGE_HORIZON","60"))
        self.min_ev=float(os.environ.get("MARKETOS_EDGE_MIN_MEAN_MOVE","0.8")); self.min_win=float(os.environ.get("MARKETOS_EDGE_MIN_WIN","0.54"))
    def stats(self,event,side):
        rows=[r for r in OUTCOMES.event_rows(event,self.horizon) if r.get("side")==side]
        if not rows: return {"n":0}
        v=[float(r.get("forward_move",0)) for r in rows]
        return {"n":len(v),"win_rate":sum(x>0 for x in v)/len(v),"mean_move":sum(v)/len(v),"mean_mfe":sum(float(r.get("mfe",0)) for r in rows)/len(rows),"mean_mae":sum(float(r.get("mae",0)) for r in rows)/len(rows)}
    def evaluate(self,event,side):
        st=self.stats(event,side)
        if st.get("n",0)<self.min_n: return False,st,"INSUFFICIENT_SAMPLE"
        ok=st["win_rate"]>=self.min_win and st["mean_move"]>=self.min_ev
        return ok,st,"EMPIRICALLY_SUPPORTED" if ok else "NO_DEMONSTRATED_EDGE"
EDGE_GATE=EmpiricalEdgeGate()

class RiskEngine:
    """Risk envelope only; never sends broker orders."""
    def __init__(self):
        self.max_daily_loss=float(os.environ.get("MARKETOS_MAX_DAILY_LOSS","5000")); self.risk_per_trade=float(os.environ.get("MARKETOS_RISK_PER_TRADE","1000")); self.daily_pnl=0.0; self.last={}
    def update(self,spot,side,trigger,profile):
        if not spot or side not in ("LONG","SHORT"): self.last={"ready":False,"reason":"NO_SIDE"}; return self.last
        bms=BOOK_MS.get(S.get("sym","NIFTY")); moves=list(bms.hist.get("mid_move",[]))[-100:] if bms else []
        vol=sum(abs(x) for x in moves)/len(moves) if moves else 0.0; stop=max(2.0,vol*8.0); entry=float(trigger.get("entry") or spot)
        top=[x.get("price") for x in (profile or {}).get("top",[]) if x.get("price") is not None]
        if side=="LONG":
            stop_px=entry-stop; ups=[x for x in top if x>entry]; target=min(ups) if ups else entry+1.5*stop
        else:
            stop_px=entry+stop; downs=[x for x in top if x<entry]; target=max(downs) if downs else entry-1.5*stop
        rr=abs(target-entry)/max(.01,abs(entry-stop_px)); size=int(max(0,self.risk_per_trade/max(.01,abs(entry-stop_px))))
        self.last={"ready":bool(rr>=1.25 and self.daily_pnl>-self.max_daily_loss),"entry":round(entry,2),"stop":round(stop_px,2),"target":round(target,2),"stop_points":round(abs(entry-stop_px),2),"rr":round(rr,2),"size_units":size,"daily_pnl":round(self.daily_pnl,2),"reason":"OK" if rr>=1.25 else "POOR_RR"}
        return self.last
RISK=RiskEngine()

OUTCOMES=OutcomeTracker()

# ============================================================
# INSTITUTIONAL DETECTOR MODULE v1.0
# Shared architecture for ALL orchestration detectors.
#   candidates -> FSM -> evidence -> confidence(+bounds) -> event
# Principles (kept from the design lessons):
#   - Never detect from raw DOM: read market memory (PLB, RS).
#   - Absorbed/iceberg = behaviour over time, not a snapshot.
#   - Refill does NOT imply iceberg.  Volume does NOT imply absorption.
#   - Candidates are cheap, evidence is expensive -> O(candidates).
#   - State transitions require EVIDENCE, never time alone.
#   - Negative evidence REDUCES confidence.
#   - Confirmed events show ONCE, persist "ongoing", update live, then FINISH.
#   - Thresholds are adaptive (percentiles), never hardcoded.
# ============================================================

class DetectorCandidate:
    """One price level under observation for institutional behaviour."""
    __slots__ = ("idx","price","side","born","last_seen","state",
                 "queue_hits","executed","refills","lifetime","peak_qty",
                 "current_qty","velocity",
                 "disp_hist","refill_delays","last_refill_t",
                 "hidden_est","disp_stability","efficiency",
                 "evidence","confidence","conf_lo","conf_hi","class_name",
                 "neg_hits","exhaustion","acceptance","order_sig",
                 "market_ctx","raw_levels","raw_collapse")
    _ctr = 0
    def __init__(self, price, side, ts):
        DetectorCandidate._ctr += 1
        self.idx           = DetectorCandidate._ctr
        self.price         = round(float(price),1)
        self.side          = side
        self.born          = ts
        self.last_seen     = ts
        self.state         = "WATCHING"
        self.queue_hits    = 0
        self.executed      = 0
        self.refills       = 0
        self.lifetime      = 0
        self.peak_qty      = 0
        self.current_qty   = 0
        self.velocity      = 0.0
        self.disp_hist     = deque(maxlen=40)
        self.refill_delays = deque(maxlen=20)
        self.last_refill_t = None
        self.hidden_est    = 0
        self.disp_stability= 0.0
        self.efficiency    = 0.0
        self.evidence      = {}
        self.confidence    = 0.0
        self.conf_lo       = 0.0
        self.conf_hi       = 0.0
        self.class_name    = "NONE"
        self.neg_hits      = 0
        self.exhaustion    = 0.0
        self.acceptance    = 0.5
        self.order_sig     = 0.0
        self.market_ctx    = deque(maxlen=60)
        self.raw_levels    = 0
        self.raw_collapse  = 0
    def age(self):
        return max(0.0, self.last_seen - self.born)
    def to_dict(self):
        return {"id":self.idx,"side":self.side,"price":self.price,
                "state":self.state,"conf":round(self.confidence),
                "lo":round(self.conf_lo),"hi":round(self.conf_hi),
                "hidden":int(self.hidden_est),"class":self.class_name,
                "exec":int(self.executed),"disp":int(self.peak_qty),
                "refill":self.refills,"age":round(self.age(),1),
                "accept":round(self.acceptance,2),"neg":self.neg_hits,
                "lvl":self.raw_levels,"col":int(self.raw_collapse),
                "ev":[{"k":k,"v":round(v)} for k,v in self.evidence.items()]}


class DetectorBase:
    NAME            = "DETECTOR"
    MAX_CANDIDATES  = 3
    ZONE            = 2.5     # points — drift within a zone = ONE event (Lesson: region, not price)
    RATE_LIMIT      = 8.0     # seconds — max one NEW confirmed alert per detector per window
    def __init__(self):
        self.candidates   = {}
        self._announced   = set()
        self._pending_alerts = []
        self._last_announce = 0.0

    # ---- adaptive baselines from PLB (no new RS feed needed) ----
    def _exec_base(self, pct=80):
        vals = sorted(lv.executed_vol for lv in PLB.levels.values() if lv.executed_vol > 0)
        return max(400, vals[min(int(len(vals)*pct/100), len(vals)-1)]) if vals else 500
    def _wall_base(self, pct=85):
        vals = sorted(lv.peak_qty for lv in PLB.levels.values() if lv.peak_qty > 0)
        base = 800 if S.get("sym")=="BANKNIFTY" else 1500
        return max(base, vals[min(int(len(vals)*pct/100), len(vals)-1)]) if vals else base

    # ---- candidate admittance ----
    def admit(self, price, side, ts):
        k = round(float(price),1)
        if k in self.candidates: return
        # zone-based dedup: a drifting wall at 24600.0/24600.7/24601.4 is ONE event
        zk = round(float(price)/self.ZONE)*self.ZONE
        if any(c.side==side and abs(c.price-zk)<=self.ZONE for c in self.candidates.values()):
            return
        if len(self.candidates) >= self.MAX_CANDIDATES:
            wk = min([(c.confidence,kk) for kk,c in self.candidates.items() if c.state=="WATCHING"],
                      default=(None,None), key=lambda x:(x[0] if x[0] is not None else 999))
            if wk[1] is not None:
                self.candidates.pop(wk[1], None)
            else:
                return
        self.candidates[k] = DetectorCandidate(price, side, ts)

    # ---- pull queue-persistence memory from PLB into the candidate ----
    def sync_plb(self, cand):
        lv = PLB.get(cand.price)
        if not lv: return False
        cand.last_seen  = lv.last_seen
        cand.queue_hits = lv.times_hit
        cand.executed   = max(cand.executed, lv.executed_vol)
        cand.refills    = lv.times_refilled
        cand.lifetime   = lv.lifetime
        cand.peak_qty   = max(cand.peak_qty, lv.peak_qty)
        cand.current_qty= lv.current_qty
        cand.velocity   = lv.velocity
        # refill timing (Lesson 48: fast/consistent refills stronger)
        if lv.times_refilled > len(cand.refill_delays):
            if cand.last_refill_t is not None:
                cand.refill_delays.append(max(0.0, lv.last_seen - cand.last_refill_t))
            cand.last_refill_t = lv.last_seen
        cand.disp_hist.append((lv.last_seen, lv.current_qty))
        # display stability (Lesson 56)
        if len(cand.disp_hist) >= 5:
            qs = [q for _,q in cand.disp_hist]
            mu = sum(qs)/len(qs)
            sds = (sum((x-mu)**2 for x in qs)/len(qs))**0.5
            cand.disp_stability = 1.0 - min(1.0, sds/(mu+1))
        # acceptance: did price HOLD despite executions? (Lesson 42/49)
        if cand.market_ctx:
            pass
        return True

    def _acceptance(self, cand, spot):
        if not spot: return cand.acceptance
        cand.market_ctx.append(spot)
        if len(cand.market_ctx) < 5: return cand.acceptance
        r = list(cand.market_ctx)
        span = max(r)-min(r)
        band = max(0.001, spot*0.004)
        cand.acceptance = cand.acceptance*0.85 + (1.0-min(1.0,span/band))*0.15
        return cand.acceptance

    def _exhaustion(self, cand):
        e = 0.0
        refill_slow = False
        if len(cand.refill_delays) >= 3:
            d = list(cand.refill_delays)
            if d[-1] > d[0]*1.6 and d[-1] > d[-2]*1.25: refill_slow = True
        keep = (cand.current_qty/cand.peak_qty) if cand.peak_qty>0 else 1.0
        queue_gone = keep < 0.15
        queue_weak = keep < 0.40
        if queue_gone: e = 1.0                      # queue structurally destroyed
        elif refill_slow and queue_weak: e = 0.5    # BOTH signs, not either
        cand.exhaustion = max(cand.exhaustion, e)
        return cand.exhaustion

    # ---- mutual exclusion between detectors (same level = ONE owning detector) ----
    def _should_defer(self, cand):
        """Return True when another detector owns this level's signature.
        A stable-display, single-order, repeatedly-hit level is an ICEBERG —
        absorption must NOT announce it too (Lesson 51: iceberber is hidden
        order refilling, not generic price-holding)."""
        return cand.order_sig >= 0.7 and cand.disp_stability >= 0.6

    # ---- FSM: transitions require evidence (Lesson 50) ----
    def transition(self, cand):
        n_com = len([v for v in cand.evidence.values() if v>=50])
        conf, st = cand.confidence, cand.state
        if st=="WATCHING" and n_com>=2: cand.state="SUSPECTED"
        if st in ("WATCHING","SUSPECTED") and conf>=40: cand.state="BUILDING"
        if st=="BUILDING" and conf>=60 and n_com>=4 and cand.age()>=20: cand.state="CONFIRMED"   # 4 comps + 20s persistence
        if st=="CONFIRMED" and conf>=84 and cand.disp_stability>=0.6 and n_com>=5: cand.state="DOMINANT"
        if st in ("CONFIRMED","DOMINANT") and cand.exhaustion>=0.9: cand.state="EXHAUSTING"
        if st=="EXHAUSTING" and (cand.exhaustion>=1.0 or cand.neg_hits>=2): cand.state="FINISHED"
        if st not in ("WATCHING","SUSPECTED") and cand.neg_hits>=3: cand.state="FINISHED"

    def score(self, cand):
        # CALIBRATED confidence (was: sums of 0-100 components pinned the cap -> always 100).
        # Evidence components sum to at most ~600, so map raw -> 0-100 such that 100 is
        # effectively unreachable and typical confirmed events land ~60-80 (they discriminate).
        raw = sum(cand.evidence.values())
        conf = min(100.0, raw / 6.2)
        cand.conf_lo = max(0.0, conf*0.82 - 5)
        cand.conf_hi = min(100.0, conf*1.15 + 5)
        cand.confidence = max(0.0, conf - cand.neg_hits*8)
        cand.conf_lo    = max(0.0, cand.conf_lo - cand.neg_hits*8)

    def cleanup(self, cand, spot, now):
        if now - cand.last_seen > 20: return True
        if cand.state=="FINISHED" and now-cand.last_seen>5: return True
        if spot and cand.lifetime>10:
            drift = spot*0.004
            if cand.side=="BID" and spot < cand.price-drift: return True
            if cand.side=="ASK" and spot > cand.price+drift: return True
        return False

    # ---- main tick ----
    def update(self, bids, asks, spot, cvd_roll, now):
        self._pending_alerts=[]
        self.candidate_builder(bids, asks, spot, now)
        for k in list(self.candidates.keys()):
            cand = self.candidates[k]
            if not self.sync_plb(cand):
                cand.neg_hits += 1
                if cand.state not in ("WATCHING","SUSPECTED"): cand.state="FINISHED"
                if self.cleanup(cand, spot, now): self.candidates.pop(k,None)
                continue
            # order-count signature for ALL detectors (Lesson 51: orders==1 = hidden order)
            _s=0.0; _pool = asks if cand.side=="ASK" else bids
            for _p in _pool:
                if abs(_p["price"]-cand.price)<0.6 and _p.get("orders")==1: _s+=1.0
            cand.order_sig=min(1.0,_s)
            self.evidence_components(cand, bids, asks, spot, cvd_roll, now)
            self.score(cand)
            self.transition(cand)
            if cand.state in ("CONFIRMED","DOMINANT","EXHAUSTING"):
                if self._should_defer(cand):
                    continue   # another detector owns this level — no duplicate
                # Institutional model: ONCE per (side, 2.5pt zone) per session.
                # A drifting wall 24600.0/24600.7/24601.4 = ONE event.
                zk = round(cand.price/self.ZONE)*self.ZONE
                sig=(cand.side, round(zk,1))
                if sig not in self._announced:
                    self._announced.add(sig)
                    self._pending_alerts.append(cand)
            if self.cleanup(cand, spot, now): self.candidates.pop(k,None)
        return [c.to_dict() for c in self.candidates.values()]

    def candidate_builder(self, bids, asks, spot, now): raise NotImplementedError
    def evidence_components(self, cand, bids, asks, spot, cvd, now): raise NotImplementedError


# ============================================================
# ABSORPTION — an execution process, not a volume event
#   Core signal: executions happen but price REFUSES to move.
# ============================================================
class AbsorptionDetector(DetectorBase):
    NAME="ABSORPTION"; MAX_CANDIDATES=3
    def candidate_builder(self, bids, asks, spot, now):
        wb = self._wall_base(85)
        eb = self._exec_base(85)
        for k, lv in PLB.levels.items():
            if lv.side not in ("BID","ASK"): continue
            # STRICT: real execution AND a meaningful wall AND survived 3s+
            size_ok = lv.peak_qty >= wb*0.7
            exec_ok = lv.executed_vol >= eb
            time_ok = lv.lifetime >= 3
            if size_ok and exec_ok and time_ok:
                self.admit(lv.price, lv.side, now)

    def _should_defer(self, cand):
        # a hidden single-order refilling wall is an ICEBERG, not absorption
        return cand.order_sig >= 0.7 and cand.disp_stability >= 0.6

    def _classify(self, cand, spot):
        a = cand.acceptance
        if cand.state in ("CONFIRMED","DOMINANT") and a>=0.72 and cand.exhaustion<0.5:
            return "PASSIVE DEFENSE" if cand.disp_stability>=0.6 else "PASSIVE ACCUMULATION"
        if cand.exhaustion>=0.5: return "EXHAUSTION"
        if a<0.35:
            cand.neg_hits+=1; return "BREAKOUT/TRAP"
        return "ABSORPTION"

    def evidence_components(self, cand, bids, asks, spot, cvd, now):
        wb=self._wall_base(85); eb=self._exec_base(80)
        acc=self._acceptance(cand, spot)
        cand.evidence["accept"]=acc*100                      # price held (biggest)
        cand.evidence["queue"] =min(100,cand.executed/eb*55) # execution vs baseline
        age=cand.age(); dens=cand.executed/age if age>0.5 else 0
        cand.evidence["exec"]  =min(100,dens/(wb*3)*70)      # exec density (lots/s)
        if len(cand.refill_delays)>=2:
            d=list(cand.refill_delays); mu=sum(d)/len(d); cv=((sum((x-mu)**2 for x in d)/len(d))**0.5)/mu if mu>0 else 0
            cand.evidence["refill"]=max(0,100-cv*80)         # refill consistency
        else:
            cand.evidence["refill"]=25
        cand.evidence["life"] =min(100,cand.age()*6)         # persistence
        cand.hidden_est = max(0,cand.executed-cand.peak_qty)+cand.refills*200+cand.queue_hits*100
        self._exhaustion(cand)
        cand.class_name=self._classify(cand, spot)


# ============================================================
# ICEBERG — hidden execution pattern; requires REAL execution evidence
#   Core signal: DISPLAY STABILITY despite continuous executions.
# ============================================================
class IcebergDetector(DetectorBase):
    NAME="ICEBERG"; MAX_CANDIDATES=3
    def candidate_builder(self, bids, asks, spot, now):
        wb=self._wall_base(85)
        for k, lv in PLB.levels.items():
            if lv.side not in ("BID","ASK"): continue
            # STRICT: meaningful size OR execution, PLUS repeated hits, PLUS 5s survival
            big = lv.peak_qty>=wb; ex = lv.executed_vol>=wb*0.5; rh = lv.times_hit>=2
            if (big or ex) and rh and lv.lifetime>=5:
                self.admit(lv.price, lv.side, now)

    def _classify(self, cand, spot):
        a=cand.acceptance; hr=cand.hidden_est/(cand.peak_qty+1)
        if a>=0.7 and cand.order_sig>=0.7 and hr>1.2: return "PASSIVE"
        if a>=0.7 and cand.queue_hits>=2 and hr>1.0: return "DEFENSIVE"
        if cand.refills>=2 and cand.hidden_est>cand.peak_qty: return "RELOADING"
        if cand.exhaustion>=0.5: return "EXHAUSTING"
        if cand.disp_stability<0.4 and cand.queue_hits>=2: return "EXECUTION"
        return "ICEBERG"

    def evidence_components(self, cand, bids, asks, spot, cvd, now):
        wb=self._wall_base(85)
        cand.evidence["disp_stab"]=cand.disp_stability*100    # strongest evidence
        ratio=cand.executed/(cand.peak_qty+1)
        cand.evidence["exec_disp"]=min(100,ratio*60)          # hidden-liquidity evidence
        cand.hidden_est=max(0,cand.executed-cand.peak_qty)+cand.refills*220+cand.queue_hits*90
        # order-count signature: orders==1 = one large hidden order (Lesson 51)
        s=0.0; pool=asks if cand.side=="ASK" else bids
        for p in pool:
            if abs(p["price"]-cand.price)<0.6 and p.get("orders")==1: s+=1.0
        cand.order_sig=min(1.0,s)
        cand.evidence["order_sig"]=cand.order_sig*80
        if len(cand.refill_delays)>=2:
            d=list(cand.refill_delays); mu=sum(d)/len(d); cv=((sum((x-mu)**2 for x in d)/len(d))**0.5)/mu if mu>0 else 0
            cand.evidence["refill_t"]=max(0,100-cv*80)
        else:
            cand.evidence["refill_t"]=30
        cand.evidence["queue"]=min(100,cand.queue_hits*30+(cand.executed/(wb+1))*50)
        cand.evidence["life"]=min(100,cand.age()*6)
        self._acceptance(cand, spot)
        self._exhaustion(cand)
        cand.class_name=self._classify(cand, spot)


# Singleton detectors (mirror PLB / EV_ENGINE pattern)
ABS_DETECTOR = AbsorptionDetector()
ICE_DETECTOR = IcebergDetector()

# ---- LIQUIDITY TOPOLOGY + SWEEP + VACUUM (region-based) ----
# ============================================================
# LIQUIDITY TOPOLOGY + VACUUM + SWEEP (v1.2, clean)
# Shared region model recalculates liquidity structure ONCE per tick,
# then BOTH vacuum & sweep query it (design's shared LiquidityTopology).
#
#   LESSONS HELD:
#   61/62/67 migration ruled out before vacuum declared (never raw cancel-count)
#   65 structural collapse only (>=2 consecutive zones >70%), not isolated cancels
#   66 collapse RATIO, not absolute volume
#   68/79 microprice (weighted-mid) leads traded price
#   69/80 slow-recovery test over a WINDOW (not a 1-tick blip)
#   70/63 vacuum & 80/63 sweep have their own lifecycle + intent classification
#   74-76 execution clusters / levels crossed, not prints / not head volume
#   77-78 efficiency = observed/expected displacement, baseline SELF-CALIBRATED
#   60 negative evidence (migration / fast-recovery) reduces confidence
# ============================================================

class LiquidityTopology:
    """Region-based book model: collapse / migration / window-recovery / microprice."""
    ZONE = 2.5
    def __init__(self):
        self.prev_bids={}; self.prev_asks={}
        self.now_bids={};  self.now_asks={}
        self._have_prev=False
        self._hist={}                    # (side,zone) -> deque of recent qty
    def _regions(self, levels):
        out={}
        for l in levels:
            zk=round(l["price"]/self.ZONE)*self.ZONE
            r=out.setdefault(zk,{"qty":0,"levels":0,"center":zk})
            r["qty"]+=l["qty"]; r["levels"]+=1
        return out
    def snapshot(self, bids, asks):
        if self.now_bids or self.now_asks:
            self.prev_bids=self.now_bids; self.prev_asks=self.now_asks; self._have_prev=True
        self.now_bids=self._regions(bids); self.now_asks=self._regions(asks)
        for side,m in (("BID",self.now_bids),("ASK",self.now_asks)):
            for zk,r in m.items():
                h=self._hist.setdefault((side,zk), deque(maxlen=12)); h.append(r["qty"])
    def has_prev(self): return self._have_prev
    def collapse(self, side, min_ratio=0.60, min_qty=400):
        """regions that dropped > min_ratio vs previous tick (Lesson 66, 65)."""
        out=[]
        if not self._have_prev: return out
        p=self.prev_bids if side=="BID" else self.prev_asks
        n=self.now_bids  if side=="BID" else self.now_asks
        for zk,pr in p.items():
            before=pr["qty"]; after=n.get(zk,{}).get("qty",0)
            if before<min_qty: continue
            ratio=1.0-after/before
            if ratio>=min_ratio: out.append({"zone":zk,"ratio":ratio,"before":before,"after":after})
        return out
    def migration(self, price, side, tol=2.5):
        """0..1: fraction of lost volume that reappeared at an ADJACENT zone (Lesson 67)."""
        zk=round(float(price)/self.ZONE)*self.ZONE
        p=self.prev_bids if side=="BID" else self.prev_asks
        n=self.now_bids  if side=="BID" else self.now_asks
        before=p.get(zk,{}).get("qty",0); after=n.get(zk,{}).get("qty",0)
        lost=max(0,before-after)
        if lost<=0: return 0.0
        nearby=sum(r["qty"] for z,r in n.items() if abs(z-zk)<=tol and z!=zk)
        return min(1.0,nearby/lost)
    def recovery(self, price, side):
        """0..1: how ABSENT the zone still is vs its recent window PEAK (Lesson 69).
        High value = level stays collapsed = SLOW recovery = genuine vacuum/continuation.
        A 1-tick blip that bounces back shows here as LOW recovery (not a real vacuum)."""
        zk=round(float(price)/self.ZONE)*self.ZONE
        h=self._hist.get((side,zk))
        if not h: return 0.0
        peak=max(h); cur=h[-1]
        if peak<=0: return 0.0
        return min(1.0,(peak-cur)/peak)
    def microprice(self, bids, asks):
        """Volume-weighted mid of top5 — leads last-traded-price (Lesson 68/79)."""
        b=sorted(bids,key=lambda x:-x["price"])[:5]; a=sorted(asks,key=lambda x:x["price"])[:5]
        bq=sum(x["qty"] for x in b) or 1; aq=sum(x["qty"] for x in a) or 1
        bp=sum(x["price"]*x["qty"] for x in b)/bq; ap=sum(x["price"]*x["qty"] for x in a)/aq
        return (bp*aq+ap*bq)/(bq+aq)


TOPOLOGY = LiquidityTopology()   # ONE shared book-region source

class AdaptiveImpact:
    """Self-calibrating expected-price-impact baseline (Lesson 77/78).
    efficiency = observed/expected; expected = recent median displacement/tick."""
    def __init__(self, window=200):
        self._deltas=deque(maxlen=window)
    def add(self, d): self._deltas.append(abs(d))
    def expected(self, fallback=0.6):
        if not self._deltas: return fallback
        s=sorted(self._deltas); return s[len(s)//2]*1.3 if len(s)>1 else fallback


# ============================================================
# VACUUM — structural liquidity collapse, migration ruled out + slow-recovery
# ============================================================
class VacuumDetector(DetectorBase):
    NAME="VACUUM"; MAX_CANDIDATES=2
    def __init__(self):
        super().__init__()
        self.topo=TOPOLOGY
        self._prev_micro=None; self._micro=None
    def update(self, bids, asks, spot, cvd_roll, now):
        self.topo.snapshot(bids, asks)
        _prev = self._micro
        _cur  = self.topo.microprice(bids, asks)
        self._micro = _cur; self._prev_micro = _prev   # shift = cur-prev (real delta)
        r=super().update(bids, asks, spot, cvd_roll, now)
        return r
    def _should_defer(self, cand):
        # migrated liquidity is NOT a vacuum (Lesson 67)
        return self.topo.migration(cand.price, cand.side)>=0.7
    def sync_plb(self, cand):
        # vacuum is REGION-based (topology), not a single PLB price level
        cand.lifetime=max(cand.lifetime,cand.age())
        return True
    def candidate_builder(self, bids, asks, spot, now):
        wb=self._wall_base(85)
        for side in ("BID","ASK"):
            for cl in self.topo.collapse(side, min_ratio=0.60, min_qty=wb*0.4):
                self.admit(cl["zone"], side, now)
    def _classify(self, cand, spot):
        cvd=cand.evidence.get("exec_ctx",0); spr=cand.evidence.get("spread",0)
        if cvd<25 and spr<40 and cand.evidence.get("recovery",0)<30: return "SPOOF COLLAPSE"
        if spr>=55 and cvd<25: return "MARKET-MAKER RETREAT"
        if cvd>=55: return "EXECUTION VACUUM"
        return "LIQUIDITY PULL"
    def evidence_components(self, cand, bids, asks, spot, cvd_roll, now):
        side=cand.side
        ratio=next((c["ratio"] for c in self.topo.collapse(side,min_ratio=0,min_qty=1)
                    if c["zone"]==cand.price),0.0)
        mig=self.topo.migration(cand.price, side)
        rec=self.topo.recovery(cand.price, side)          # window slow-recovery (L69)
        # retain PEAK collapse — an event stays a vacuum even once it stops shrinking
        cand.evidence["collapse"]=max(cand.evidence.get("collapse",0), max(0,ratio*100))
        cand.raw_collapse=int(cand.evidence["collapse"])
        cand.evidence["migration"]=(1-mig)*45             # penalty if migrated (L67)
        cand.evidence["recovery"]=rec*50                  # HIGH=still absent=true vacuum
        cand.evidence["exec_ctx"]=min(100,abs(cvd_roll)/8) # exec vs pure cancel (L63)
        cand.evidence["spread"]=30+(20 if abs(cvd_roll)>800 else 0)
        # microprice shift leads move (L68)
        cand.evidence["micro"]=min(100,abs(self._micro-self._prev_micro)/(spot*0.0004)*60) \
            if (self._prev_micro is not None and self._micro is not None) else 25
        # directional bias from CVD sign (which side collapsed + who drove it)
        cand.evidence["dir"]=60 if ((side=="BID" and cvd_roll<0) or (side=="ASK" and cvd_roll>0)) else 20
        if mig>=0.7: cand.neg_hits+=1                    # migrated -> not a vacuum
        cand.class_name=self._classify(cand, spot)
    def transition(self, cand):
        # lifecycle (L70): COLLAPSING -> CONFIRMED -> RECOVERING -> FINISHED
        n_com=len([v for v in cand.evidence.values() if v>=50])
        conf, st=cand.confidence, cand.state
        if st=="WATCHING" and n_com>=2: cand.state="COLLAPSING"
        if st in ("WATCHING","COLLAPSING") and conf>=42: cand.state="CONFIRMED"
        if cand.state in ("CONFIRMED","DOMINANT") and cand.evidence.get("recovery",0)<=28:
            cand.state="RECOVERING"
        if cand.state=="RECOVERING" and (cand.evidence.get("recovery",0)<=12 or cand.neg_hits>=2):
            cand.state="FINISHED"
        if cand.state not in ("WATCHING","COLLAPSING") and cand.neg_hits>=3: cand.state="FINISHED"
        if cand.state not in ("WATCHING","COLLAPSING","CONFIRMED","DOMINANT","RECOVERING"):
            cand.state="CONFIRMED"


# ============================================================
# SWEEP — aggressive multi-level removal + price displacement
# ============================================================
class SweepDetector(DetectorBase):
    NAME="SWEEP"; MAX_CANDIDATES=2
    def __init__(self):
        super().__init__()
        self.topo=TOPOLOGY
        self._prev_micro=None; self._micro=None
        self.impact=AdaptiveImpact()
        self._persist={}
    def update(self, bids, asks, spot, cvd_roll, now):
        self.topo.snapshot(bids, asks)
        _prev = self._micro
        _cur  = self.topo.microprice(bids, asks)
        self._micro = _cur; self._prev_micro = _prev
        if self._prev_micro is not None:
            self.impact.add(self._micro-self._prev_micro)
        r=super().update(bids, asks, spot, cvd_roll, now)
        return r
    def _should_defer(self, cand):
        # a stable, single-order, refilling level is an iceberg (owned elsewhere)
        return cand.order_sig>=0.7 and cand.disp_stability>=0.6
    def sync_plb(self, cand):
        # sweep is REGION-based (topology), not a single PLB price level
        cand.lifetime=max(cand.lifetime,cand.age())
        return True
    def candidate_builder(self, bids, asks, spot, now):
        wb=self._wall_base(85)
        for side in ("BID","ASK"):
            cols=self.topo.collapse(side, min_ratio=0.70, min_qty=wb*0.4)
            if len(cols)<2: continue
            zs=sorted(c["zone"] for c in cols); runs=[]; cur=[zs[0]]
            for z in zs[1:]:
                if z-cur[-1]<=self.topo.ZONE*1.2: cur.append(z)
                else: runs.append(cur); cur=[z]
            runs.append(cur)
            for run in runs:
                if len(run)>=2:
                    self.admit(round(sum(run)/len(run),1), side, now)
    def _classify(self, cand, spot):
        eff=cand.evidence.get("eff",50); cont=cand.evidence.get("cont",50)
        rec=cand.evidence.get("recovery",0); micro=cand.evidence.get("micro",25)
        if rec>=0.6 and cont<40: return "STOP HUNT"       # reversal + liquidity back
        if cont>=45 and cont<70: return "INITIATION"
        if cont>=70 and micro>=40: return "BREAKOUT"
        if eff<30: return "EXHAUSTION"
        if eff>=70: return "LIQUIDATION"
        return "SWEEP"
    def evidence_components(self, cand, bids, asks, spot, cvd_roll, now):
        side=cand.side
        nbr=[c for c in self.topo.collapse(side,min_ratio=0.70,min_qty=200)
             if abs(c["zone"]-cand.price)<=self.topo.ZONE*1.2]
        n_levels=len(nbr)
        consumed=sum(c["before"]-c["after"] for c in nbr)
        prev_disp=sum(r["qty"] for r in self.topo.prev_bids.values())+sum(r["qty"] for r in self.topo.prev_asks.values())
        cand.evidence["levels"]=max(cand.evidence.get("levels",0), min(100,n_levels*22))   # L76 peak
        cand.raw_levels=n_levels
        cand.evidence["consump"]=max(cand.evidence.get("consump",0),
                                     (consumed/max(1,prev_disp)*100 if prev_disp>0 else 0))  # L72 peak
        # efficiency vs SELF-CALIBRATED expected impact (L77/78, no hardcode)
        obs=abs(self._micro-self._prev_micro) if (self._prev_micro is not None and self._micro is not None) else 0.0
        exp=max(0.05,self.impact.expected())
        cand.evidence["eff"]=min(100,max(0,obs/(exp*max(1,n_levels))*100))
        cand.evidence["density"]=max(cand.evidence.get("density",0),
                                     min(100,consumed/max(1,now-cand.last_seen)/10))   # L75 peak
        # microprice confirms direction (L79)
        if self._prev_micro is not None and self._micro is not None:
            g=self._micro-self._prev_micro
            cand.evidence["micro"]=70 if ((side=="ASK" and g>0) or (side=="BID" and g<0)) else 25
        else: cand.evidence["micro"]=30
        # window recovery (L80): true sweep CONTINUES; blip reverses -> stop-hunt
        rec=self.topo.recovery(cand.price, side)
        cand.evidence["recovery"]=(1-rec)*60
        # directional persistence (L79/80) - post-sweep flow keeps pushing
        # Continuation is book/price-native when no trade tape exists.
        # Positive microprice displacement through an ASK collapse is bullish;
        # negative displacement through a BID collapse is bearish.
        g = (self._micro-self._prev_micro) if (self._prev_micro is not None and self._micro is not None) else 0.0
        k=(cand.side, round(cand.price/self.topo.ZONE))
        pq=self._persist.setdefault(k, deque(maxlen=20)); pq.append(g)
        sgn=1 if cand.side=="ASK" else -1
        cand.evidence["cont"]=(sum(1 for v in pq if v*sgn>0)/len(pq)*100) if pq else 50
        if rec>=0.6 and abs(g)<max(0.01,self.impact.expected()*0.25): cand.neg_hits+=1  # failed sweep
        cand.class_name=self._classify(cand, spot)
    def transition(self, cand):
        # lifecycle (L80): BUILDING -> CONFIRMED -> CONTINUING -> EXHAUSTING -> FINISHED
        n_com=len([v for v in cand.evidence.values() if v>=50])
        conf, st=cand.confidence, cand.state
        if st=="WATCHING" and n_com>=2: cand.state="BUILDING"
        if st in ("WATCHING","BUILDING") and conf>=42: cand.state="CONFIRMED"
        if cand.state=="CONFIRMED" and cand.evidence.get("cont",0)>=60: cand.state="CONTINUING"
        if cand.state in ("CONFIRMED","CONTINUING") and cand.evidence.get("recovery",0)>=70: cand.state="EXHAUSTING"
        if cand.state=="EXHAUSTING" and (cand.evidence.get("recovery",0)>=90 or cand.neg_hits>=2): cand.state="FINISHED"
        if cand.state not in ("WATCHING","BUILDING") and cand.neg_hits>=3: cand.state="FINISHED"
        if cand.state not in ("WATCHING","BUILDING","CONFIRMED","CONTINUING","EXHAUSTING"): cand.state="CONFIRMED"


SWEEP_DETECTOR=SweepDetector()
VACUUM_DETECTOR=VacuumDetector()

# ---- TOXICITY + MARKET INTELLIGENCE (Modules 12-15, v2 design-aligned) ----
# ============================================================
# CORE INTELLIGENCE LAYER v2.0 (design-aligned)
#   Lesson 119  one shared intelligence object, no direct module coupling
#   Lesson 120  enums not strings for market states
#   Lesson 121  snapshot-typed sub-objects
#   Lesson 122  risk is multidimensional
#   Lesson 123  MarketIntelligence is detector-agnostic
#   Lesson 124  prev/current/delta on confidence
# Layers: ToxicityEngine (M12) + MarketIntelligence (M13-15 core object)
# ============================================================
from enum import Enum

class MarketPhase(Enum):
    UNKNOWN="UNKNOWN"; ACCUMULATION="ACCUMULATION"; DISTRIBUTION="DISTRIBUTION"
    MARKUP="MARKUP"; MARKDOWN="MARKDOWN"; BALANCE="BALANCE"
    COMPRESSION="COMPRESSION"; EXPANSION="EXPANSION"; DISCOVERY="DISCOVERY"
    EXHAUSTION="EXHAUSTION"; PANIC="PANIC"; REPAIR="REPAIR"

class MarketIntent(Enum):
    UNKNOWN="UNKNOWN"; ACCUMULATING="ACCUMULATING"; DISTRIBUTING="DISTRIBUTING"
    DEFENDING="DEFENDING"; ATTACKING="ATTACKING"; LIQUIDATING="LIQUIDATING"
    ABSORBING="ABSORBING"; DISCOVERING="DISCOVERING"; REBALANCING="REBALANCING"

class MarketHealth(Enum):
    UNKNOWN="UNKNOWN"; HEALTHY="HEALTHY"; NORMAL="NORMAL"
    STRESSED="STRESSED"; TOXIC="TOXIC"; DISLOCATED="DISLOCATED"

class Opportunity(Enum):
    NONE="NONE"; VERY_LOW="VERY_LOW"; LOW="LOW"
    MODERATE="MODERATE"; HIGH="HIGH"; VERY_HIGH="VERY_HIGH"


class ToxicityEngine:
    """Market-wide liquidity/toxicity context (Lesson 83). Adaptive z-combo (Lesson 90)."""
    def __init__(self):
        self.BUCKET_VOL=500
        self._buy=0; self._sell=0
        self._imb=deque(maxlen=100); self._kyle=deque(maxlen=300)
        self._amihud=deque(maxlen=300); self._ofiv=deque(maxlen=200)
        self._adv=deque(maxlen=200); self._spread=deque(maxlen=200)
        self.prev_mid=None; self._last_tbq=0; self._last_tsq=0
        self.prev_bids={}; self.prev_asks={}
        self.reset()
    def reset(self):
        self.vpin=0.0; self.kyle=0.0; self.amihud=0.0; self.ofi=0.0
        self.adverse=0.0; self.spread=0.0; self.stress=0.0
        self.regime="NORMAL"; self.z={}
    @staticmethod
    def _z(win,x):
        if not win: return 0.0
        m=sum(win)/len(win); v=sum((i-m)**2 for i in win)/len(win)
        return (x-m)/(v**0.5) if v>0 else 0.0
    def update(self, bids, asks, mid, cvd_delta, tbq, tsq):
        self.reset()
        vol=abs(cvd_delta) if FLOW_QUALITY == "REAL" else 0.0
        if FLOW_QUALITY == "REAL":
            self._buy+=max(0,cvd_delta); self._sell+=max(0,-cvd_delta)
        while (self._buy+self._sell)>=self.BUCKET_VOL:
            tot=self._buy+self._sell
            self._imb.append(abs(self._buy-self._sell)/tot); self._buy=self._sell=0
        self.vpin=sum(self._imb)/len(self._imb) if self._imb else 0.0
        if self.prev_mid is not None and vol>0:
            self._kyle.append(abs(mid-self.prev_mid)/vol)
        self.kyle=sum(self._kyle)/len(self._kyle) if self._kyle else 0.0
        dvol=abs(tbq-self._last_tbq)+abs(tsq-self._last_tsq)
        if self.prev_mid is not None and self.prev_mid>0 and dvol>0:
            self._amihud.append(abs(mid-self.prev_mid)/self.prev_mid/max(1,dvol))
        self.amihud=sum(self._amihud)/len(self._amihud) if self._amihud else 0.0
        cur_b={round(b["price"],1):b["qty"] for b in bids}
        cur_a={round(a["price"],1):a["qty"] for a in asks}
        ofi=0.0
        if self.prev_bids or self.prev_asks:
            for p,q in cur_b.items(): ofi+=q-self.prev_bids.get(p,0)
            for p,q in cur_a.items(): ofi-=q-self.prev_asks.get(p,0)
            for p in self.prev_bids:
                if p not in cur_b: ofi-=self.prev_bids[p]
            for p in self.prev_asks:
                if p not in cur_a: ofi+=self.prev_asks[p]
        self._ofiv.append(ofi); self.ofi=ofi
        if bids and asks:
            s=asks[0]["price"]-bids[0]["price"]; self._spread.append(s); self.spread=s
        if (tsq-self._last_tsq)>0 and self.prev_mid is not None:
            self._adv.append(1.0 if (mid-self.prev_mid)<0 else 0.0)
        if (tbq-self._last_tbq)>0 and self.prev_mid is not None:
            self._adv.append(1.0 if (mid-self.prev_mid)>0 else 0.0)
        self.adverse=sum(self._adv)/len(self._adv) if self._adv else 0.0
        self._last_tbq=tbq; self._last_tsq=tsq; self.prev_mid=mid
        self.prev_bids=cur_b; self.prev_asks=cur_a
        self.z={"vpin":self._z(list(self._imb),self.vpin),"kyle":self._z(list(self._kyle),self.kyle),
                "amihud":self._z(list(self._amihud),self.amihud),"ofi":self._z(list(self._ofiv),abs(self.ofi)),
                "adverse":self._z(list(self._adv),self.adverse),"spread":self._z(list(self._spread),self.spread)}
        raw=0.30*self.z["vpin"]+0.20*self.z["kyle"]+0.10*self.z["amihud"]+0.15*self.z["ofi"]+0.15*self.z["adverse"]+0.10*self.z["spread"]
        self.stress=min(100,max(0,raw*22+35))
        self.regime=("DISLOCATED" if self.stress>=80 else "HIGHLY TOXIC" if self.stress>=60
                     else "STRESSED" if self.stress>=45 else "ELEVATED" if self.stress>=30 else "NORMAL")
        return self.to_dict()
    def to_dict(self):
        return {"vpin":round(self.vpin,3),"kyle":round(self.kyle,6),"amihud":round(self.amihud,8),
                "ofi":round(self.ofi),"adverse":round(self.adverse,2),"spread":round(self.spread,2),
                "stress":round(self.stress),"regime":self.regime,
                "z":{k:round(v,2) for k,v in self.z.items()}}


class RiskSnapshot:
    def __init__(self):
        self.liquidity=0.0; self.execution=0.0; self.volatility=0.0
        self.toxicity=0.0; self.regime=0.0; self.narrative=0.0; self.overall=0.0
    def compute(self, tox, vac_active, flip):
        self.toxicity=min(100,tox.get("stress",0))
        self.liquidity=80 if vac_active else 20
        self.execution=max(self.toxicity,100 if tox.get("adverse",0)>0.5 else 40)
        self.volatility=min(100,self.toxicity*0.6+(60 if vac_active else 30))
        self.regime=80 if flip>=2 else 50
        self.narrative=25
        self.overall=min(100,round(0.25*self.liquidity+0.20*self.execution+0.15*self.volatility
                                   +0.20*self.toxicity+0.10*self.regime+0.10*self.narrative,1))
    def to_dict(self):
        return {"liquidity":int(self.liquidity),"execution":int(self.execution),
                "volatility":int(self.volatility),"toxicity":int(self.toxicity),
                "regime":int(self.regime),"narrative":int(self.narrative),"overall":int(self.overall)}

class ScenarioSnapshot:
    def __init__(self,name,probability,invalidation):
        self.name=name; self.probability=probability; self.invalidation=invalidation
    def to_dict(self):
        return {"name":self.name,"prob":self.probability,"invalid":self.invalidation}


class MarketIntelligence:
    """Detector-agnostic central object (Lesson 123). prev/current/delta (Lesson 124)."""
    SCHEMA_VERSION=2
    def __init__(self):
        self._phase=MarketPhase.UNKNOWN
        self._intent=MarketIntent.UNKNOWN
        self._health=MarketHealth.NORMAL
        self._prev_phase=MarketPhase.UNKNOWN
        self.confidence=0.0; self._prev_confidence=0.0
        self.opportunity=Opportunity.NONE
        self.evidence0=0.0
        self.risk=RiskSnapshot()
        self.scenarios=[]
        self.invalidations=[]
        self.ready=False
        self.story=""; self.implication="--"; self.destination="--"; self.dom_side="NONE"
        self._flip_count=0; self._last_regime=None

    @staticmethod
    def _side(arr, inv=False):
        b=sum(c["conf"] for c in arr if c["side"]=="BID")/100.0
        s=sum(c["conf"] for c in arr if c["side"]=="ASK")/100.0
        # SWEEP/VACUUM are INVERSE: ASK side consumed (offers eaten) = BULLISH,
        # BID side consumed (bids eaten) = BEARISH.
        return (s,b) if inv else (b,s)

    def update(self, spot, delta, tox, inst_abs, inst_ice, inst_sweep, inst_vacuum):
        ab,as_=self._side(inst_abs); ib,is_=self._side(inst_ice)
        sb,ss=self._side(inst_sweep, True); vb,vs2=self._side(inst_vacuum, True)
        hidden_b=0.65*ab+0.35*ib; hidden_s=0.65*as_+0.35*is_     # one stream (Lesson102)
        bull=hidden_b+sb+vb; bear=hidden_s+ss+vs2
        if delta>0: bull+=0.5
        elif delta<0: bear+=0.5
        toxn=tox.get("stress",0)/100.0
        bull*=(1+0.3*toxn); bear*=(1+0.3*toxn)
        # health enum (Lesson120)
        if tox.get("regime")=="DISLOCATED": h=MarketHealth.DISLOCATED
        elif tox.get("stress",0)>=60: h=MarketHealth.TOXIC
        elif tox.get("stress",0)>=45: h=MarketHealth.STRESSED
        elif tox.get("stress",0)<25: h=MarketHealth.HEALTHY
        else: h=MarketHealth.NORMAL
        self._health=h
        # phase enum + transition count
        if bull>bear+1.2 and (sb+ss)>0: ph=MarketPhase.ACCUMULATION
        elif bear>bull+1.2 and (sb+ss)>0: ph=MarketPhase.DISTRIBUTION
        elif abs(bull-bear)<0.4: ph=MarketPhase.BALANCE
        elif bull>bear: ph=MarketPhase.MARKUP
        else: ph=MarketPhase.MARKDOWN
        if tox.get("regime")=="DISLOCATED": ph=MarketPhase.PANIC
        self._prev_phase=self._phase; self._phase=ph
        # intent
        self._intent=(MarketIntent.ACCUMULATING if self._phase in (MarketPhase.ACCUMULATION,MarketPhase.MARKUP)
                      else MarketIntent.DISTRIBUTING if self._phase in (MarketPhase.DISTRIBUTION,MarketPhase.MARKDOWN)
                      else MarketIntent.REBALANCING)
        # confidence + delta (Lesson 124), validation (Lesson: clamp 0-1)
        self._prev_confidence=self.confidence
        self.confidence=min(0.99,max(0.05,round(0.22*(bull-bear)+0.28*(1-toxn),2)))
        self.opportunity=(Opportunity.HIGH if self.confidence>=0.6
                          else Opportunity.MODERATE if self.confidence>=0.45
                          else Opportunity.LOW if self.confidence>=0.25 else Opportunity.NONE)
        # scenarios sum to 100
        if self._phase in (MarketPhase.ACCUMULATION,MarketPhase.MARKUP): r1,r2,r3=60,24,16
        elif self._phase in (MarketPhase.DISTRIBUTION,MarketPhase.MARKDOWN): r1,r2,r3=60,24,16
        else: r1,r2,r3=38,34,28
        if toxn>0.6: r1=max(20,r1-12); r3=min(50,r3+12)
        tot=r1+r2+r3; r1=round(r1/tot*100); r2=round(r2/tot*100); r3=100-r1-r2
        cont="CONTINUATION" if self._phase in (MarketPhase.ACCUMULATION,MarketPhase.MARKUP) else "DECLINE"
        self.scenarios=[ScenarioSnapshot(cont,r1,"trend intact"),
                        ScenarioSnapshot("COUNTER-MOVE",r2,"overhead wall appears"),
                        ScenarioSnapshot("BALANCE",r3,"liquidity returns")]
        # risk (Lesson 122) + readiness (Lesson 116)
        vac_active=bool(vb+vs2)
        if self._last_regime is not None and self._last_regime!=self._phase.value:
            self._flip_count+=1
        self._last_regime=self._phase.value
        self.risk.compute(tox, vac_active, self._flip_count)
        good_env=self._health in (MarketHealth.HEALTHY,MarketHealth.NORMAL) and self.risk.execution<60
        self.ready=bool(good_env and abs(bull-bear)>=0.6)
        # invalidation (Lesson 98)
        bull_side=self._phase in (MarketPhase.ACCUMULATION,MarketPhase.MARKUP)
        self.invalidations=["support below broken" if bull_side else "resistance above broken",
                            "toxicity spikes to DISLOCATED"]
        # story (Lesson 92/96)
        self.story=("Institutional "+self._intent.value.lower()+" while market health reads "
                    +self._health.value.lower()+".")
        # ---- trading implication (plain, actionable) ----
        _toxn = tox.get("stress",0) or 0
        if not self.ready:
            self.implication = "STAND DOWN" + (" (toxic " + str(int(_toxn)) + ")" if _toxn>=50 else " (execution not ready)")
        elif self._phase in (MarketPhase.ACCUMULATION,MarketPhase.MARKUP):
            self.implication = "BREAKOUT IMMINENT" if (sb+ss)>0 else "BULLISH BIAS - retest bid support"
        elif self._phase in (MarketPhase.DISTRIBUTION,MarketPhase.MARKDOWN):
            self.implication = "BREAKOUT IMMINENT" if (sb+ss)>0 else "BEARISH BIAS - reject ask resistance"
        elif self._phase == MarketPhase.BALANCE:
            self.implication = "RANGE - no edge, wait"
        else:
            self.implication = "NEUTRAL"
        # ---- destination: next likely liquidity target (the "WHERE") ----
        _imp = LEVELS.important_levels(top=6) or []
        _above = sorted([x["price"] for x in _imp if x["price"] > (spot or 0)])
        _below = sorted([x["price"] for x in _imp if x["price"] < (spot or 0)])
        if bull >= bear:
            self.dom_side = "BULL"
            self.destination = "UP " + "{:.0f}".format(_above[0]) if _above else "UP (no mapped target - explore)"
        else:
            self.dom_side = "BEAR"
            self.destination = "DOWN " + "{:.0f}".format(_below[-1]) if _below else "DOWN (no mapped target - explore)"
        return self.to_dict()

    def _int(self,e):
        return e.value

    @property
    def phase(self): return self._phase.value
    @property
    def phase_delta(self): return self._prev_phase.value if self._prev_phase else ""

    def to_dict(self):
        return {"schema":self.SCHEMA_VERSION,"phase":self._phase.value,
                "intent":self._intent.value,"health":self._health.value,
                "confidence":round(self.confidence,2),
                "conf_delta":round(self.confidence-self._prev_confidence,3),
                "opportunity":self.opportunity.value,
                "evidence":"","risk":self.risk.to_dict() if isinstance(self.risk,RiskSnapshot) else {},
                "scenarios":[s.to_dict() for s in self.scenarios],
                "invalidations":self.invalidations,"ready":self.ready,"story":self.story,
                "implication":self.implication,"destination":self.destination,"dom_side":self.dom_side}

    def _int(self,e): return e.value


TOX=ToxicityEngine()
INTEL=MarketIntelligence()

# ---- FLOW SURGE + TRIGGER ENGINE (GO/NO-GO) ----
# ============================================================
# FLOW SURGE + TRIGGER ENGINE v1.0
#   FlowTracker  — ACTUAL trade-flow acceleration when REAL CVD is available.
#                  A sudden |delta| vs its own rolling median = large participant
#                  transacting now -> "FLOW SURGE" (the early move signal).
#   TriggerEngine— turns state into a DEFINITIVE GO / NO-GO with entry + reason
#                  (evidence-attributed, Lesson 153) + invalidation + duration.
#                  GO requires: phase agrees AND ready=YES AND low stress AND a
#                  price/flow trigger (wall cleared OR sweep/vacuum in direction
#                  OR flow surge) — never a bare context call.
# ============================================================

class FlowTracker:
    """Rolling-median-based buy/sell delta surge detector (Lesson: acceleration > level)."""
    def __init__(self, window=60):
        self._deltas=deque(maxlen=window)
        self._state={"surge":0,"dirn":"FLAT","mag":0}
    def update(self, cvd_delta):
        self._deltas.append(cvd_delta)
        s=self._state
        if len(self._deltas)>=8:
            med=sorted(abs(d) for d in self._deltas)[len(self._deltas)//2] or 1
            last=abs(cvd_delta)
            if last > med*2.5:
                s["surge"]=min(100, round(last/med*18,0))
                s["dirn"]="BUY SURGE" if cvd_delta>0 else "SELL SURGE"
                s["mag"]=int(cvd_delta)
            else:
                s["surge"]=0; s["dirn"]="FLAT"; s["mag"]=0
        return s
    def to_dict(self):
        return dict(self._state)

class TriggerEngine:
    """GO/NO-GO with evidence-attributed reason (Lesson 153) + invalidation + duration."""
    def __init__(self):
        self._state={"go":"NO-GO","side":"NONE","entry":0,"reason":[],"invalidation":[],"duration":"--","last":0}
    def _top_wall(self, walls, side):
        arr=[c for c in walls if c["side"]==side]
        return max(arr, key=lambda c:c["conf"], default=None)
    def _act(self, arr):
        return [c for c in arr if c["state"] in ("CONFIRMED","CONTINUING","COLLAPSING","EXHAUSTING")]
    def update(self, spot, intel, inst_abs, inst_ice, inst_sweep, inst_vacuum, tox, flow, now, div=None, book_ms=None, liquidity_path=None):
        go="NO-GO"; side="NONE"; entry=0; reason=[]; inval=[]; dur="--"
        phase=intel.get("phase","BALANCE")
        ready=bool(intel.get("ready"))
        stress=tox.get("stress",0) or 0
        bull_phase=phase in ("ACCUMULATION","MARKUP")
        bear_phase=phase in ("DISTRIBUTION","MARKDOWN")
        walls=list(inst_abs)+list(inst_ice)
        support   =self._top_wall(walls,"BID")
        resistance=self._top_wall(walls,"ASK")
        fs=flow.get("dirn","FLAT")
        bull_sw=self._act([c for c in inst_sweep if c["side"]=="ASK"])
        bull_vac=self._act([c for c in inst_vacuum if c["side"]=="ASK"])
        bear_sw=self._act([c for c in inst_sweep if c["side"]=="BID"])
        bear_vac=self._act([c for c in inst_vacuum if c["side"]=="BID"])
        lp = liquidity_path or {}
        lp_long = (lp.get("LONG") or {}) if isinstance(lp, dict) else {}
        lp_short = (lp.get("SHORT") or {}) if isinstance(lp, dict) else {}

        # ---- LONG candidate ----
        if bull_phase and ready and stress<50:
            reason.append("phase bullish")
            if bull_sw: reason.append("ask being swept ({}L)".format(bull_sw[0].get("lvl",0)))
            if bull_vac: reason.append("ask vacuum ({:.0f}%)".format(bull_vac[0].get("col",0)))
            if fs=="BUY SURGE": reason.append("buy flow surge +{}".format(flow.get("mag",0)))
            cleared = resistance and spot and spot>=resistance["price"]
            if cleared:
                reason.append("price cleared ask wall {}".format(resistance["price"]))
                go="GO"; side="LONG"; entry=resistance["price"]
                inval=["back below {}".format(resistance["price"]),"sell iceberg reload","toxicity spike"]
                dur="1-5 min"
            elif (bull_sw or bull_vac) and fs=="BUY SURGE":
                reason.append("no ask wall overhead; flow-proxy confirms")
                go="GO"; side="LONG"; entry=round(spot or 0,1)
                inval=["sell wall forms above","buy flow reverses"]
                dur="1-5 min"
        # ---- SHORT candidate ----
        if go=="NO-GO" and bear_phase and ready and stress<50:
            reason.append("phase bearish")
            if bear_sw: reason.append("bids being swept")
            if bear_vac: reason.append("bid vacuum")
            if fs=="SELL SURGE": reason.append("sell flow surge -{}".format(abs(flow.get("mag",0))))
            broke = support and spot and spot<=support["price"]
            if broke:
                reason.append("price broke below bid wall {}".format(support["price"]))
                go="GO"; side="SHORT"; entry=support["price"]
                inval=["back above {}".format(support["price"]),"buy iceberg reload","toxicity spike"]
                dur="1-5 min"
            elif (bear_sw or bear_vac) and fs=="SELL SURGE":
                reason.append("no bid wall below; flow-proxy confirms")
                go="GO"; side="SHORT"; entry=round(spot or 0,1)
                inval=["bid wall forms below","sell flow reverses"]
                dur="1-5 min"
        # ---- LIQUIDITY-PATH trigger (map -> interaction -> response) ----
        # This is book-native. It does not require trade prints and never calls
        # displayed-quantity changes executions. A path trigger is provisional
        # until the empirical edge gate validates the event family.
        if go=="NO-GO" and FLOW_QUALITY!="REAL" and stress<50:
            if lp_long.get("entry_ready") and lp_long.get("phase") in ("CLEARING","ACCEPTED","EXTENDING") and lp_long.get("failure_risk",0) < 70 and lp_long.get("setup_state") not in ("FAILING","EXHAUSTING"):
                go="GO"; side="LONG"; entry=round(float(spot or 0),1)
                reason.append("LIQUIDITY PATH: mapped ask cleared + price accepted")
                inval=["target rejection", "ask liquidity reloads", "microprice loses lead", "toxicity spike"]
                dur="1-5 min"
            elif lp_short.get("entry_ready") and lp_short.get("phase") in ("CLEARING","ACCEPTED","EXTENDING") and lp_short.get("failure_risk",0) < 70 and lp_short.get("setup_state") not in ("FAILING","EXHAUSTING"):
                go="GO"; side="SHORT"; entry=round(float(spot or 0),1)
                reason.append("LIQUIDITY PATH: mapped bid cleared + price accepted")
                inval=["target rejection", "bid liquidity reloads", "microprice loses lead", "toxicity spike"]
                dur="1-5 min"

        # Path exhaustion is an entry blocker. Position exit management remains
        # separate because the engine does not assume a live broker position.
        if go=="GO" and side=="LONG" and lp_long.get("exit_ready"):
            go="NO-GO"; side="NONE"; entry=0; reason=["blocked: mapped liquidity path is exhausting"]
        elif go=="GO" and side=="SHORT" and lp_short.get("exit_ready"):
            go="NO-GO"; side="NONE"; entry=0; reason=["blocked: mapped liquidity path is exhausting"]

        # A mapped target that is actively rejecting is an entry blocker in that
        # direction. This prevents buying directly into the same liquidity failure
        # that is already reversing.
        if go=="GO" and side=="LONG" and lp_long.get("phase")=="REJECTING":
            go="NO-GO"; side="NONE"; entry=0; reason=["blocked: mapped overhead liquidity is rejecting"]
        elif go=="GO" and side=="SHORT" and lp_short.get("phase")=="REJECTING":
            go="NO-GO"; side="NONE"; entry=0; reason=["blocked: mapped downside liquidity is rejecting"]

        # ---- BOOK-ONLY trigger path (no trade tape / no fabricated CVD) ----
        if go=="NO-GO" and FLOW_QUALITY!="REAL" and ready and stress<50:
            bm=book_ms or {}; micro=float(bm.get("microprice") or 0); mid=float(bm.get("mid") or spot or 0)
            micro_up=bool(mid and micro>mid+0.25); micro_dn=bool(mid and micro<mid-0.25)
            if bull_phase and (bull_sw or bull_vac) and micro_up:
                go="GO"; side="LONG"; entry=round(float(spot or 0),1); reason.append("BOOK-ONLY: offer liquidity removed + microprice leads up"); inval=["microprice loses lead","bid support fails","toxicity spike"]; dur="1-5 min"
            elif bear_phase and (bear_sw or bear_vac) and micro_dn:
                go="GO"; side="SHORT"; entry=round(float(spot or 0),1); reason.append("BOOK-ONLY: bid liquidity removed + microprice leads down"); inval=["microprice loses lead","ask resistance fails","toxicity spike"]; dur="1-5 min"

        # ---- price/flow divergence refinement (the self-filtering edge) ----
        if div is not None and div.get("div") not in (None,"NEUTRAL"):
            dv=div.get("div")
            if go=="GO" and side=="LONG" and dv=="FAKE RALLY":
                go="NO-GO"; side="NONE"; entry=0; reason=["blocked: price up but flow down (fake rally)"]
            elif go=="GO" and side=="SHORT" and dv=="ABSORPTION":
                go="NO-GO"; side="NONE"; entry=0; reason=["blocked: buyers absorbing the dip"]
            elif go=="GO":
                reason.append("flow-proxy confirms " + ("bull" if side=="LONG" else "bear"))
            elif dv=="ABSORPTION": reason.append("note: buyers absorbing dip")
            elif dv=="FAKE RALLY": reason.append("note: rally lacks real buyers")
        if go=="NO-GO":
            if not ready: reason.append("waiting: execution not ready")
            elif stress>=50: reason.append("waiting: toxic tape ({:.0f})".format(stress))
            elif not (bull_phase or bear_phase): reason.append("waiting: phase balance")
            elif not any("blocked" in r or "note:" in r for r in reason):
                reason.append("waiting: no trigger (need wall-break / sweep / flow surge)")
        self._state={"go":go,"side":side,"entry":entry,"reason":reason,
                     "invalidation":inval,"duration":dur,"last":now}
        return dict(self._state)
    def to_dict(self): return dict(self._state)


FLOW = FlowTracker()
TRIGGER = TriggerEngine()

class DivergenceTracker:
    """Price-vs-flow divergence over a rolling window (the self-filtering edge).
    Compares net CVD over the window vs net spot move. Only acts on DISAGREEMENT
    (rare + meaningful), which cancels the per-tick noise automatically."""
    def __init__(self, window=120, min_samples=20):
        self._data=deque(maxlen=600)
        self.window=window; self.min_samples=min_samples
        self._state={"div":"NEUTRAL","detail":"no clear price/flow trend"}
    def update(self, spot, cvd_delta, now):
        self._data.append((now, spot, cvd_delta))
        d=self._state
        if spot is None:
            return d
        recent=[x for x in self._data if now-x[0]<=self.window]
        if len(recent)<self.min_samples:
            d["div"]="NEUTRAL"; d["detail"]="gathering data ({}/{} ticks)".format(len(recent),self.min_samples)
            return d
        p0=recent[0][1]; p1=recent[-1][1]
        move=p1-p0; net=sum(x[2] for x in recent)
        p_thr=max(1.0, spot*0.0001)     # ~2.5 pts on NIFTY
        n_thr=400.0
        if abs(move)<p_thr or abs(net)<n_thr:
            d["div"]="NEUTRAL"; d["detail"]="no clear price/flow trend"
        elif move>0 and net>0:  d["div"]="CONFIRMED"; d["detail"]="price up + flow up (real buy)"
        elif move>0 and net<0:  d["div"]="FAKE RALLY"; d["detail"]="price up but flow down (no real buyers)"
        elif move<0 and net<0:  d["div"]="CONFIRMED"; d["detail"]="price down + flow down (real sell)"
        elif move<0 and net>0:  d["div"]="ABSORPTION"; d["detail"]="price down but flow up (buyers absorbing dip)"
        else:                   d["div"]="NEUTRAL"; d["detail"]="flat"
        return d
    def to_dict(self):
        return dict(self._state)

DIV = DivergenceTracker()

# ---- LEVEL MEMORY (cross-session importance) ----
# ============================================================
# LEVEL MEMORY — "has price level X become important?"
# Traces which price ZONES are becoming liquidity-relevant and
# PERSISTS them across sessions (the missing cross-session memory).
#   per zone:  touches  (times spot sat there)
#              volume   (cumulative |flow| traded near it)
#              first/last seen, age
#   importance = f(touches, volume, recency)  -> 0-100
# Persisted to JSON so yesterday's important zones load today.
# ============================================================
import os, json, time as _t

class LevelMemory:
    ZONE = 2.5
    def __init__(self, file="level_memory.json", decay_sec=3600):
        self.file = file
        self.decay = decay_sec
        self.zones = {}          # zone_key(float) -> stats
        self._load()
    def _load(self):
        try:
            if os.path.exists(self.file):
                with open(self.file, encoding="utf-8") as f:
                    self.zones = json.load(f)
        except Exception:
            self.zones = {}
    def save(self):
        try:
            with open(self.file, "w", encoding="utf-8") as f:
                json.dump(self.zones, f)
        except Exception:
            pass
    def update(self, spot, cvd_delta=None, now=None, depth_mass=0):
        if not spot: return
        now=now if now is not None else _t.time()
        zk=str(round(spot/self.ZONE)*self.ZONE)
        z=self.zones.setdefault(zk,{"t":0,"depth":0,"first":now,"last":now})
        # Backward-compatible migration: older level_memory.json files may
        # contain zones created before the depth field existed.  setdefault()
        # does not repair an existing dict, so normalize persisted records here.
        if not isinstance(z, dict):
            z={"t":0,"depth":0,"first":now,"last":now}
            self.zones[zk]=z
        z.setdefault("t", 0)
        z.setdefault("depth", 0)
        z.setdefault("first", now)
        z.setdefault("last", now)
        z.pop("v",None)
        z["t"]+=1
        z["depth"]+=max(0,int(depth_mass or 0))
        z["last"]=now
    def importance(self, zone_key, now=None):
        now = now if now is not None else _t.time()
        z = self.zones.get(str(zone_key))
        if not z: return 0.0
        recency = max(0.0, 1.0 - (now - z["last"]) / self.decay)     # 1 fresh -> 0 stale
        touch = min(1.0, z["t"] / 20.0)                              # 20+ touches = full
        depth=min(1.0,z.get("depth",0)/5000000.0)
        return min(100.0,(0.45*touch+0.35*depth+0.20*recency)*100)
    def important_levels(self, top=4, now=None, min_imp=45):
        """Top price zones by importance score — the 'levels that matter'."""
        now = now if now is not None else _t.time()
        scored = []
        for k, z in self.zones.items():
            imp = self.importance(k, now)
            if imp >= min_imp:
                scored.append({"price": float(k), "imp": round(imp),
                               "touches": z["t"], "depth": int(z.get("depth",0))})
        scored.sort(key=lambda x: -x["imp"])
        return scored[:top]
    def is_important(self, price, now=None, min_imp=45):
        return self.importance(price, now) >= min_imp

LEVELS = LevelMemory()

# ---- LIQUIDITY PROFILE (full-session liquidity map) ----
# ============================================================
# LIQUIDITY PROFILE — the full-session "where is liquidity" map.
# The 50-level feed only shows depth NEAR price, so we build the
# profile by ACCUMULATING every observed depth sample per price zone
# as price travels. Over the day this gives a genuine liquidity
# histogram: the zones that repeatedly held big depth become S/R.
# Reliable inputs only: depth qty, order-counts, time. NO tbq/tsq.
#
#   POC        = zone of peak observed liquidity (point of control)
#   VALUE AREA = zones covering ~70% of cumulative density
#   HVN / LVN  = zones denser / emptier than average
# ============================================================

class LiquidityProfile:
    ZONE = 5.0
    def __init__(self, min_ticks=8):
        self.zones = {}            # zone_key -> {t, bs, as, bp, ap, orders, last}
        self.min_ticks = min_ticks
    def _zk(self, p): return round(float(p)/self.ZONE)*self.ZONE
    def update(self, bids, asks, now):
        for l in bids:
            z = self.zones.setdefault(self._zk(l["price"]), {"t":0,"bs":0,"as":0,"bp":0,"ap":0,"orders":0,"last":now,"first":now,"active_s":0.0})
            q = l.get("qty",0); o = l.get("orders",0) or 1
            z["t"]+=1; z["bs"]+=q; z["bp"]=max(z["bp"],q); z["orders"]+=o; z["last"]=now
        for l in asks:
            z = self.zones.setdefault(self._zk(l["price"]), {"t":0,"bs":0,"as":0,"bp":0,"ap":0,"orders":0,"last":now,"first":now,"active_s":0.0})
            q = l.get("qty",0); o = l.get("orders",0) or 1
            z["t"]+=1; z["as"]+=q; z["ap"]=max(z["ap"],q); z["orders"]+=o; z["last"]=now
    def density(self, zk):
        z = self.zones.get(zk)
        if not z or z["t"] < self.min_ticks: return 0.0
        time_w  = min(1.0, z["t"]/120.0)                 # how long liquidity stood here
        depth_w = min(1.0, ((z["bs"]+z["as"])/z["t"])/4000.0)   # avg depth (0-4000 -> 1)
        return min(100.0, (0.55*time_w + 0.45*depth_w)*100)
    def _ranked(self):
        return sorted(self.zones.items(), key=lambda kv: -self.density(kv[0]))
    def poc(self):
        r=self._ranked()
        return r[0][0] if r and self.density(r[0][0])>0 else None
    def value_area(self, pct=70):
        r=[kv for kv in self._ranked() if self.density(kv[0])>0]
        if not r: return []
        tot=sum(self.density(k) for k,_ in r)
        acc=0.0; out=[]
        for k,_ in r:
            acc+=self.density(k); out.append(k)
            if acc/tot >= pct/100.0: break
        return sorted(out)
    def hvn_lvn(self, top=3):
        dens=[self.density(k) for k in self.zones if self.density(k)>0]
        if not dens: return [], []
        med=sorted(dens)[len(dens)//2]
        r=self._ranked()
        hvn=[round(k) for k,_ in r if self.density(k)>med][:top]
        lvn=[round(k) for k,_ in r[::-1] if 0<self.density(k)<=med][:top]
        return hvn, lvn
    def to_dict(self, spot=None):
        va=self.value_area()
        hvn,lvn=self.hvn_lvn()
        return {"poc":self.poc(),"va_lo":min(va) if va else None,"va_hi":max(va) if va else None,
                "hvn":hvn,"lvn":lvn,
                "top":[{ "price":k, "d":round(self.density(k))} for k,_ in self._ranked()[:3]]}

PROFILE = {"NIFTY": LiquidityProfile(), "BANKNIFTY": LiquidityProfile()}


# ---- SESSION RAILS (PDH/PDL/PDC + liquidity-center map) ----
# ============================================================
# SESSION RAILS — the permanent MARKET MAP rails
#   PDH / PDL / PDC   previous day high/low/close
#   VWAP              session volume-weighted average (from tbq+tsq volume)
#   today's H/L/C     live session range
# Self-tracked; on session rollover the current H/L/C becomes PDH/PDL/PDC.
# Best-effort seed from Fyers history API on startup (degrades gracefully).
# ============================================================

class SessionRails:
    def __init__(self):
        self.pdh=None; self.pdl=None; self.pdc=None
        self.t_open=None; self.t_high=None; self.t_low=None; self.t_close=None
        self.vwap=None
        self._pv=0.0; self._v=0.0; self._pv=0.0; self._v=0.0; self._prev_tb=None; self._prev_ts=None
        self._day=None
    def update(self, spot, tbq, tsq, vol=None):
        d = str(date.today())
        if self._day is None:
            self._day = d
        elif d != self._day:
            # promote current session -> previous rails
            self.pdh=self.t_high; self.pdl=self.t_low; self.pdc=self.t_close
            self.t_open=spot; self.t_high=spot; self.t_low=spot; self.t_close=spot
            self._pv=0.0; self._v=0.0; self._prev_tb=None; self._prev_ts=None
            self._day=d
        if self.t_high is None or (spot and spot > self.t_high): self.t_high=spot
        if self.t_low  is None or (spot and spot < self.t_low):  self.t_low=spot
        if spot: self.t_close=spot
        # No trade tape was exposed by the supported TBT callback in the observed
        # session. This is displayed-depth liquidity center, NOT execution VWAP.
        if (vol or 0) > 0 and spot:
            self._pv += spot*vol; self._v += vol
            if self._v > 0: self.vwap = self._pv/self._v
    def to_dict(self):
        return {"pdh":self.pdh,"pdl":self.pdl,"pdc":self.pdc,
                "t_open":self.t_open,"t_high":self.t_high,"t_low":self.t_low,
                "vwap":self.vwap,"vwap_status":"LIQUIDITY_WEIGHTED_PROXY_NOT_EXECUTION_VWAP"}
    def save(self, file="rails_memory.json"):
        try:
            import json as _j
            with open(file, "w", encoding="utf-8") as f:
                _j.dump(self.to_dict(), f)
        except Exception:
            pass
    def load(self, file="rails_memory.json"):
        try:
            import json as _j, os as _os
            if _os.path.exists(file):
                with open(file, encoding="utf-8") as f:
                    d = _j.load(f)
                self.pdh=d.get("pdh"); self.pdl=d.get("pdl"); self.pdc=d.get("pdc")
        except Exception:
            pass

SESSION = SessionRails()
SESSION.load()
# optional manual rails via env (fallback if history seed fails)
if SESSION.pdh is None:
    SESSION.pdh = float(os.environ["FYERS_PDH"]) if os.environ.get("FYERS_PDH") else None
    SESSION.pdl = float(os.environ["FYERS_PDL"]) if os.environ.get("FYERS_PDL") else None
    SESSION.pdc = float(os.environ["FYERS_PDC"]) if os.environ.get("FYERS_PDC") else None

# ---- LEVEL INTERACTION (break vs reject at the key level) ----
# ============================================================
# LEVEL INTERACTION — "is the key level breaking or rejecting?"
# Finds the nearest reference level (PDH/PDL / important zone / wall)
# and classifies price's interaction with it, so the market itself
# reveals whether the level breaks or rejects (no foretelling).
#
#   RANGE    price far from any reference           (no scenario)
#   APPROACH price moving toward the level
#   TESTING  price at the level, unresolved
#   BREAK    price through + flow agrees + stays    -> trade with the break
#   FADE     price through then rejected           -> level flips / stand down
# ============================================================

class LevelInteraction:
    BAND_FRAC = 0.0008          # interaction band ~0.08% of spot (~20 pts NIFTY)
    def __init__(self):
        self.state="RANGE"; self.key=None; self.ktype=""; self.brk_dir=0
        self.side=""; self.flow=""
        self.held=0
    def to_dict(self):
        return {"has":self.key is not None, "state":self.state,
                "key":self.key, "ktype":self.ktype, "side":self.side, "flow":self.flow,
                "held":self.held}
    def update(self, spot, rails, imp_levels, support, resistance, flow_dirn, now):
        spot = spot or 0
        refs = []
        if rails.get("pdh"): refs.append(("PDH", float(rails["pdh"])))
        if rails.get("pdl"): refs.append(("PDL", float(rails["pdl"])))
        for il in (imp_levels or [])[:2]:
            if il.get("price"): refs.append(("IMP", float(il["price"])))
        if resistance and resistance.get("price"): refs.append(("RES", float(resistance["price"])))
        if support and support.get("price"):       refs.append(("SUP", float(support["price"])))
        self.flow = flow_dirn
        band = max(2.0, spot * self.BAND_FRAC)
        if not refs:
            self.key=None; self.state="RANGE"; self.side=""; self.brk_dir=0; return self.to_dict()
        kt, kv = min(refs, key=lambda r: abs(r[1]-spot))
        if abs(kv-spot) > band*4:
            self.key=None; self.state="RANGE"; self.side=""; self.brk_dir=0; return self.to_dict()
        if self.key != kv:                       # new level entered play -> lock break direction
            self.key=kv; self.ktype=kt; self.state="TESTING"; self.held=0
            self.brk_dir = 1 if spot < kv else -1          # +1 -> break UP, -1 -> break DOWN
            self.side = "RESISTANCE" if self.brk_dir==1 else "SUPPORT"
        above = spot > kv + band
        below = spot < kv - band
        break_side = above if self.brk_dir==1 else below     # price THROUGH in the break direction
        if self.state in ("RANGE","APPROACH"):
            self.state = "TESTING" if not (above or below) else "APPROACH"
        if self.state == "TESTING" and break_side:
            agreeing = ("BUY" in flow_dirn) if self.brk_dir==1 else ("SELL" in flow_dirn)
            self.state = "BREAK" if agreeing else "PROBING"
        if self.state == "BREAK":
            self.held += 1
        if self.state in ("BREAK","PROBING") and not break_side:
            self.state = "FADE"                                # returned through the level
        return self.to_dict()

INTERACT = LevelInteraction()


def seed_previous_ohlc(client, sym):
    """Best-effort: fetch last completed daily candle from Fyers history -> PDH/PDL/PDC.
    Tries both Fyers date formats + common response shapes; saves on success."""
    from datetime import date as _d_, timedelta as _t_
    to = _d_.today(); frm = to - _t_(days=12)
    for dfmt in ("0", "1"):
        try:
            range_from = str(frm) if dfmt == "0" else frm.strftime("%d-%m-%Y")
            range_to   = str(to)  if dfmt == "0" else to.strftime("%d-%m-%Y")
            resp = client.history(data={"symbol": sym, "resolution": "D", "date_format": dfmt,
                                         "range_from": range_from, "range_to": range_to, "cont_flag": "1"})
            candles = resp.get("candles") or resp.get("data") or (resp.get("d") or {}).get("candles") or []
            if candles:
                c = candles[-2] if len(candles) >= 2 else candles[-1]   # last completed day
                SESSION.pdh=float(c[2]); SESSION.pdl=float(c[3]); SESSION.pdc=float(c[4])
                SESSION.save()
                print("[RAILS] seeded PDH={:.1f} PDL={:.1f} PDC={:.1f}".format(c[2],c[3],c[4]))
                return
        except Exception:
            continue
    print("[RAILS] history seed failed -> self-tracked / env rails only")





# =============================================================
# ANALYZE — institutional-grade orderbook analysis
# Evidence is detector-specific. CVD is required only for execution-dependent
# detectors; book-only detectors remain valid without trade-flow data.
# =============================================================
def analyze(bids, asks, spot, pb, pa, bnf=False):
    # Adaptive thresholds — self-calibrate from rolling stats
    wq = adaptive_wall_threshold(bnf)   # 85th-pctile wall qty
    # Absorption: level must have been >adaptive wall size before dropping
    absorb_min_size = max(wq, RS["wall_qty"].percentile(75)) if RS["wall_qty"].ready(30) else (WALL_BNF if bnf else WALL_NF)
    # CVD threshold: require meaningful trade activity (500-lot rolling 3-tick)
    cvd_thr = max(200, abs(RS["cvd_delta"].percentile(60))) if RS["cvd_delta"].ready(20) else 500
    # Iceberg: requires PLB-confirmed refill pattern, not just qty refill
    # Sweep: min 4 adjacent levels collapsed + price moved
    sweep_min_levels = 4
    sweep_cooldown = 10   # seconds between sweep alerts

    # Exchange totals for imbalance (most reliable)
    ex_tb = S.get("tot_buy_qty", 0)
    ex_ts = S.get("tot_sell_qty", 0)
    tb = ex_tb if ex_tb > 0 else sum(b["qty"] for b in bids)
    ta = ex_ts if ex_ts > 0 else sum(a["qty"] for a in asks)

    # Flow: actual CVD only when trade prints were observed. Otherwise keep the
    # book-total proxy completely separate and do not feed it into execution labels.
    if FLOW_QUALITY == "REAL" and S.get("trade_count", 0) > 0:
        cvd_roll = rolling_cvd(3)
        delta = S.get("last_actual_delta", 0)
    else:
        cvd_roll = 0
        delta = 0

    # Imbalance % — 50L local depth
    local_tb = sum(b["qty"] for b in bids)
    local_ta = sum(a["qty"] for a in asks)
    tot = local_tb + local_ta or 1
    bp = round(local_tb / tot * 100, 1)
    ap = round(local_ta / tot * 100, 1)
    RS["bid_pct"].add(bp)
    RS["total_bid"].add(local_tb)
    RS["total_ask"].add(local_ta)

    # Classification — raise bars for 50-level data (more noise)
    def cls(b, a):
        if b >= 75: return "STRONG BID", "BULLISH"
        if a >= 75: return "STRONG ASK", "BEARISH"
        if b >= 62: return "MILD BID", "MILD BULL"
        if a >= 62: return "MILD ASK", "MILD BEAR"
        return "BALANCED", "NEUTRAL"

    sig, dirn = cls(bp, ap)

    # Near-zone (spot ±0.3%)
    nb = [b for b in bids if spot and b["price"] >= spot * 0.997]
    na = [a for a in asks if spot and a["price"] <= spot * 1.003]
    nt = (sum(b["qty"] for b in nb) + sum(a["qty"] for a in na)) or 1
    nbp = round(sum(b["qty"] for b in nb) / nt * 100, 1)
    nap = round(sum(a["qty"] for a in na) / nt * 100, 1)
    nsig, ndirn = cls(nbp, nap)

    # ---- Detect execution absorption ----
    # A displayed quantity decrease is not enough. We require actual classified
    # trades at the exact level and a subsequent failure of price to continue.
    absorb = {"active": False, "side": "NONE", "signal": "NONE", "price": 0}
    if FLOW_QUALITY == "REAL" and pb and pa:
        # SELL aggressors hitting a BID, while the bid remains/replenishes and
        # price does not continue lower => bullish execution absorption.
        for b in bids:
            k = b["price"]
            lv = PLB.get(k)
            prev_qty = pb.get(k, b["qty"])
            if not lv or lv.side != "BID" or lv.times_hit < 2:
                continue
            if lv.last_execution_side != "SELL" or lv.executed_vol < max(100, lv.peak_qty * 0.50):
                continue
            consumed = max(0, prev_qty - b["qty"])
            replenished = lv.times_refilled >= 1 or b["qty"] >= lv.peak_qty * 0.70
            if replenished and (consumed > 0 or lv.executed_vol >= lv.peak_qty):
                absorb = {"active": True, "side": "BULLISH",
                          "signal": "BULL EXECUTION ABSORPTION {} exec={:,} hits={} refill={}".format(
                              k, lv.executed_vol, lv.times_hit, lv.times_refilled),
                          "price": k}
                break

        # BUY aggressors hitting an ASK, while the ask remains/replenishes and
        # price does not continue higher => bearish execution absorption.
        if not absorb["active"]:
            for a in asks:
                k = a["price"]
                lv = PLB.get(k)
                prev_qty = pa.get(k, a["qty"])
                if not lv or lv.side != "ASK" or lv.times_hit < 2:
                    continue
                if lv.last_execution_side != "BUY" or lv.executed_vol < max(100, lv.peak_qty * 0.50):
                    continue
                consumed = max(0, prev_qty - a["qty"])
                replenished = lv.times_refilled >= 1 or a["qty"] >= lv.peak_qty * 0.70
                if replenished and (consumed > 0 or lv.executed_vol >= lv.peak_qty):
                    absorb = {"active": True, "side": "BEARISH",
                              "signal": "BEAR EXECUTION ABSORPTION {} exec={:,} hits={} refill={}".format(
                                  k, lv.executed_vol, lv.times_hit, lv.times_refilled),
                              "price": k}
                    break

    # ---- Detect iceberg from PLB (institutional pattern) ----
    iceberg = {"detected": False, "side": "NONE", "signal": "NONE", "price": 0}
    if pb and pa:
        # Look for PLB-confirmed refill pattern
        for price_key, level_obj in PLB.levels.items():
            if level_obj.side != "ASK" or level_obj.times_refilled < 2:
                continue
            prev_p = pa.get(price_key)
            if prev_p and level_obj.times_hit >= 2:
                refill_ratio = level_obj.current_qty / level_obj.peak_qty if level_obj.peak_qty > 0 else 0
                if refill_ratio >= 0.75 and level_obj.lifetime > 20:
                    # PLB confirmed: hits + refills + lasted >20s = iceberg
                    iceberg = {"detected": True, "side": "BEARISH",
                               "signal": "SELL ICEBERG {} {:,} [{}× refill, {}s old]".format(
                                   price_key, level_obj.current_qty, level_obj.times_refilled, int(level_obj.lifetime)),
                               "price": price_key}
                    break
        if not iceberg["detected"]:
            for price_key, level_obj in PLB.levels.items():
                if level_obj.side != "BID" or level_obj.times_refilled < 2:
                    continue
                prev_p = pb.get(price_key)
                if prev_p and level_obj.times_hit >= 2:
                    refill_ratio = level_obj.current_qty / level_obj.peak_qty if level_obj.peak_qty > 0 else 0
                    if refill_ratio >= 0.75 and level_obj.lifetime > 20:
                        iceberg = {"detected": True, "side": "BULLISH",
                                   "signal": "BUY ICEBERG {} {:,} [{}× refill, {}s old]".format(
                                       price_key, level_obj.current_qty, level_obj.times_refilled, int(level_obj.lifetime)),
                                   "price": price_key}
                        break

    # Liquidity-weighted price center — local visible book (NOT trade VWAP)
    vb = round(sum(b["price"] * b["qty"] for b in bids) / local_tb, 2) if local_tb > 0 else None
    va = round(sum(a["price"] * a["qty"] for a in asks) / local_ta, 2) if local_ta > 0 else None
    dr = round(local_tb / local_ta, 2) if local_ta > 0 else 1.0
    n5b = sum(b["qty"] for b in bids[:5])
    n5a = sum(a["qty"] for a in asks[:5])
    conc = round(((n5b / local_tb * 100 if local_tb > 0 else 50) +
                  (n5a / local_ta * 100 if local_ta > 0 else 50)) / 2, 1)

    # Wall detection — institutional only (NORMAL/INSTITUTIONAL lifetime)
    avg_bq = local_tb / len(bids) if bids else 500
    dyn_wq = max(wq, avg_bq * 3)
    # Build wall list from ALL 50 bid/ask levels, track age from PLB
    bw = [dict(b, dist=round(spot - b["price"], 1),
               st="LARGE" if b["qty"] >= dyn_wq * 3 else "MED",
               age=_get_age(b["price"]))
          for b in bids if b["orders"] >= 1]
    bw = sorted(bw, key=lambda x: x["qty"], reverse=True)[:3]
    aw = [dict(a, dist=round(a["price"] - spot, 1),
               st="LARGE" if a["qty"] >= dyn_wq * 3 else "MED",
               age=_get_age(a["price"]))
          for a in asks if a["orders"] >= 1]
    aw = sorted(aw, key=lambda x: x["qty"], reverse=True)[:3]

    # ---- ZONE-BASED S/R: cluster adjacent levels into price zones ----
    def cluster_zones(levels, tolerance=2.5):
        """Cluster levels within tolerance into zones, return top zones by cum_qty."""
        if not levels:
            return []
        zones = []
        used = set()
        for lv in sorted(levels, key=lambda x: x["qty"], reverse=True):
            p = round(lv["price"], 1)
            if p in used:
                continue
            cluster = [lv]
            used.add(p)
            for other in levels:
                op = round(other["price"], 1)
                if op not in used and abs(op - p) <= tolerance:
                    cluster.append(other)
                    used.add(op)
            cum_qty = sum(l["qty"] for l in cluster)
            age_max = max(l.get("age", 0) for l in cluster)
            prices = [l["price"] for l in cluster]
            zones.append({
                "center": round(sum(prices) / len(prices), 1),
                "range": "{:.0f}-{:.0f}".format(min(prices), max(prices)),
                "qty": cum_qty,
                "levels": len(cluster),
                "age": age_max,
            })
        zones.sort(key=lambda z: z["qty"], reverse=True)
        return zones[:3]

    bid_zones = cluster_zones(bw)
    ask_zones = cluster_zones(aw)

    all_w = [(w, "BID") for w in bw] + [(w, "ASK") for w in aw]
    dom, wsig = None, "NONE"
    if all_w:
        big = max(all_w, key=lambda x: x[0]["qty"])
        # Only flag as wall if INSTITUTIONAL or NORMAL age (>30s)
        big_age = big[0].get("age", 0)
        if big_age >= 30:    # only institutional walls
            dom = dict(big[0], side=big[1])
            wsig = "BUY WALL" if big[1] == "BID" else "SELL WALL"
        else:
            dom = None
            wsig = "NONE"

    sup = []
    for w in bw[:3]:
        lv = PLB.get(w["price"])
        vel_sym, vel_cls = ("→", "neut")
        zone = w["qty"]
        if lv:
            vel_sym, vel_cls = lv.velocity_indicator
            # Zone qty: cumulative of levels within 2.5 pts
            all_bid_lv = [l for l in PLB.levels.values() if l.side == "BID"]
            zone = lv.zone_qty(all_bid_lv)
        sup.append({
            "price": w["price"], "qty": w["qty"], "st": w["st"],
            "age": w.get("age", 0), "vel_sym": vel_sym, "vel_cls": vel_cls, "zone": zone
        })

    res = []
    for w in aw[:3]:
        lv = PLB.get(w["price"])
        vel_sym, vel_cls = ("→", "neut")
        zone = w["qty"]
        if lv:
            vel_sym, vel_cls = lv.velocity_indicator
            all_ask_lv = [l for l in PLB.levels.values() if l.side == "ASK"]
            zone = lv.zone_qty(all_ask_lv)
        res.append({
            "price": w["price"], "qty": w["qty"], "st": w["st"],
            "age": w.get("age", 0), "vel_sym": vel_sym, "vel_cls": vel_cls, "zone": zone
        })

    # ---- Institutional signal scoring: EVIDENCE GROUPS with caps (no double-counting) ----
    # Correlated measures share ONE capped bucket, so 5 ways of reading the same book
    # (wall+depth+near-zone+ratio) can't stack into a fake STRONG BULL. Groups:
    #   FLOW (cap25)  = CVD + exchange cumulative direction   (trade-flow)
    #   LIQUIDITY (cap20) = depth + near-zone + ratio + wall   (one book, pooled)
    #   EXECUTION (cap25) = absorption + iceberg               (hidden-liquidity execution)
    def _cap(x, c): return min(c, max(0, x))
    bull = bear = 0
    sigs = []

    # FLOW / EXECUTION (cap 25): only genuine classified trade flow may enter
    # the CVD bucket. TBQ/TSQ are exchange-reported BOOK totals and are never
    # interpreted as aggressive buy/sell volume. When trade prints are absent,
    # this bucket is intentionally empty; book dynamics are scored separately.
    fl_b = fl_s = 0
    if FLOW_QUALITY == "REAL":
        if delta > cvd_thr:        fl_b += 8; sigs.append("REAL CVD +{}".format(delta))
        elif delta < -cvd_thr:     fl_s += 8; sigs.append("REAL CVD {}".format(delta))
    bull += _cap(fl_b, 25); bear += _cap(fl_s, 25)

    # BOOK DYNAMICS (cap 25): legitimate information available without a trade
    # tape. Uses microprice lead, local imbalance, and observed depletion /
    # replenishment. This is structural pressure, NOT CVD.
    bm = BOOK_MS.get(S.get("sym", "NIFTY"))
    bms = bm.snapshot() if bm else {}
    micro = float(bms.get("microprice") or 0)
    mid = float(bms.get("mid") or spot or 0)
    spread = float(bms.get("spread") or 0)
    bp_score = 0.0
    if mid and spread >= 0:
        lead = (micro-mid) / max(0.5, spread/2.0)
        bp_score += max(-8.0, min(8.0, lead*4.0))
    bp_score += max(-5.0, min(5.0, (bp-50.0)/4.0))
    for ev in (bms.get("depleted") or []):
        if abs(float(ev.get("price", spot))-spot) <= 10:
            bp_score += -2.5 if ev.get("side")=="BID" else 2.5
    for ev in (bms.get("replenished") or []):
        if abs(float(ev.get("price", spot))-spot) <= 10:
            bp_score += 2.0 if ev.get("side")=="BID" else -2.0
    bp_score=max(-25.0,min(25.0,bp_score))
    if bp_score >= 4: sigs.append("BOOK PRESSURE +{:.1f}".format(bp_score))
    elif bp_score <= -4: sigs.append("BOOK PRESSURE {:.1f}".format(bp_score))
    bull += _cap(bp_score,25); bear += _cap(-bp_score,25)

    # LIQUIDITY (cap 20): ALL book-depth measures pooled (correlated -> one bucket)
    liq_b = liq_s = 0
    if dirn == "BULLISH":      liq_b += 2; sigs.append("Depth {}% bid".format(bp))
    elif dirn == "BEARISH":    liq_s += 2; sigs.append("Depth {}% ask".format(ap))
    if ndirn == "BULLISH":     liq_b += 2; sigs.append("Near {}% bid".format(nbp))
    elif ndirn == "BEARISH":   liq_s += 2; sigs.append("Near {}% ask".format(nap))
    if dr > 1.5:               liq_b += 1; sigs.append("Depth {:.2f}x bid".format(dr))
    elif dr < 0.67:            liq_s += 1; sigs.append("Depth {:.2f}x ask".format(dr))
    if dom:
        sz = 2 if dom["st"] == "LARGE" else 1
        if dom["side"] == "BID": liq_b += sz; sigs.append("{} wall {:,}@{}".format(dom["st"], dom["qty"], dom["price"]))
        else:                    liq_s += sz; sigs.append("{} wall {:,}@{}".format(dom["st"], dom["qty"], dom["price"]))
    bull += _cap(liq_b, 20); bear += _cap(liq_s, 20)

    # EXECUTION (cap 25): absorption + iceberg pooled (both hidden-liquidity execution)
    ex_b = ex_s = 0
    if absorb["active"]:
        if absorb["side"] == "BULLISH": ex_b += 6; sigs.append(absorb["signal"])
        else:                            ex_s += 6; sigs.append(absorb["signal"])
    if iceberg["detected"]:
        if iceberg["side"] == "BULLISH": ex_b += 5; sigs.append(iceberg["signal"])
        else:                             ex_s += 5; sigs.append(iceberg["signal"])
    bull += _cap(ex_b, 25); bear += _cap(ex_s, 25)

    net = bull - bear
    if net >= 10:   dsig, dc, dst = "STRONG BULL", "bull",     min(bull * 8, 100)
    elif net >= 6:  dsig, dc, dst = "BULL BIAS",   "mild-bull", min(bull * 7, 80)
    elif net >= 3:  dsig, dc, dst = "MILD BULL",   "mild-bull", min(bull * 5, 60)
    elif net <= -10: dsig, dc, dst = "STRONG BEAR", "bear",      min(bear * 8, 100)
    elif net <= -6: dsig, dc, dst = "BEAR BIAS",   "mild-bear", min(bear * 7, 80)
    elif net <= -3: dsig, dc, dst = "MILD BEAR",   "mild-bear", min(bear * 5, 60)
    else:           dsig, dc, dst = "NEUTRAL", "neutral", 0

    return {"bp": bp, "ap": ap, "tb": local_tb, "ta": local_ta,
            "sig": sig, "dirn": dirn,
            "nbp": nbp, "nap": nap, "nsig": nsig, "ndirn": ndirn,
            "bw": bw, "aw": aw, "dom": dom, "wsig": wsig,
            "absorb": absorb, "iceberg": iceberg,
            "delta": delta, "vb": vb, "va": va, "dr": dr, "conc": conc,
            "sup": sup, "res": res,
            "bid_zones": bid_zones, "ask_zones": ask_zones,
            "dsig": dsig, "dc": dc, "dst": dst,
            "bull": bull, "bear": bear, "sigs": sigs,
            "book_pressure": round(bp_score,2),
            "flow_quality": FLOW_QUALITY,
            "cvd_valid": FLOW_QUALITY == "REAL"}


_WALL_HIST = deque(maxlen=100)   # shared wall-size history for sweep/vacuum

def detect_iceberg_from_plb(bids, asks, pb, pa, spot):
    """PLB-backed iceberg detection — requires confirmed hit+refill cycles."""
    detected = {"detected": False, "side": "NONE", "signal": "NONE", "price": 0}
    for price_key, level_obj in PLB.levels.items():
        if level_obj.times_refilled < 2 or level_obj.times_hit < 2:
            continue
        refill_ratio = level_obj.current_qty / level_obj.peak_qty if level_obj.peak_qty > 0 else 0
        if refill_ratio < 0.70 or level_obj.lifetime < 20:
            continue
        age_str = "{}s".format(int(level_obj.lifetime)) if level_obj.lifetime < 60 else "{}m".format(int(level_obj.lifetime // 60))
        if level_obj.side == "ASK":
            detected = {"detected": True, "side": "BEARISH",
                        "signal": "SELL ICEBERG {} {:,} [{}× refill, {} old]".format(
                            price_key, level_obj.current_qty, level_obj.times_refilled, age_str),
                        "price": price_key}
            break
        elif level_obj.side == "BID":
            detected = {"detected": True, "side": "BULLISH",
                        "signal": "BUY ICEBERG {} {:,} [{}× refill, {} old]".format(
                            price_key, level_obj.current_qty, level_obj.times_refilled, age_str),
                        "price": price_key}
            break
    return detected


_ICE_CD = 0   # iceberg cooldown

def detect_sweep(bids, asks, spot):
    """Detect institutional sweep: 4+ adjacent levels vaporized + price moved."""
    global _sweep_cd, _prev_asks_snap, _prev_bids_snap
    import time as _t; now = _t.time()
    if now - _sweep_cd < 10:
        _c = dict(S.get("sweep", {"detected": False})); _c["detected"]=False; _c["confirmed"]=False
        return _c
    ca = {round(a["price"], 1): a["qty"] for a in asks}
    cb = {round(b["price"], 1): b["qty"] for b in bids}
    sw = {"detected": False, "side": "NONE", "signal": "", "levels": 0, "volume": 0, "confirmed": False}
    cvd_roll = rolling_cvd(5) if FLOW_QUALITY == "REAL" else 0
    prev_spot = S.get("prev_spot", spot) or spot
    # Check ask sweep (buyers eating through levels = bullish sweep)
    if _prev_asks_snap:
        avg_q = max(500, (sum(ca.values()) + sum(cb.values())) / (len(ca) + len(cb)))
        min_q = avg_q * 1.5
        sa = []
        for p in sorted(_prev_asks_snap):
            pq = _prev_asks_snap[p]
            cq = ca.get(p, 0)
            # Level vaporized: any level that collapsed >75% in one tick
            # (institutional sweep = consecutive band collapse, size-agnostic)
            if pq > 100 and cq < pq * 0.25:
                sa.append({"price": p, "vol": pq - cq})
        if len(sa) >= 4:
            gaps = sum(1 for i in range(1, len(sa))
                       if sa[i]["price"] - sa[i-1]["price"] <= 2.5)
            if gaps >= 3:
                tv = sum(x["vol"] for x in sa)
                tv_ratio = tv / (avg_q * max(1, len(sa)))
                # Institutional floor: meaningful absolute volume consumed
                # (4+ consecutive levels >75% collapse + adjacency is the core signal;
                #  tv_ratio just confirms magnitude vs book depth)
                if tv >= 4000 and tv_ratio >= 0.35:
                    cvd_ok = abs(cvd_roll) >= 500
                    price_ok = spot > prev_spot + 0.5
                    confirmed = cvd_ok and price_ok
                    sig_type = "CVD✓" if (confirmed and FLOW_QUALITY == "REAL") else "?"
                    sw = {"detected": True, "side": "BUY",
                          "signal": "BUY SWEEP {} levels {:,} ({})".format(len(sa), int(tv), sig_type),
                          "levels": len(sa), "volume": int(tv), "confirmed": confirmed,
                          "cum_cvd": cvd_roll, "tv_ratio": round(tv_ratio, 1)}
                    _sweep_cd = now
    # Check bid sweep (sellers eating through levels = bearish sweep)
    if not sw["detected"] and _prev_bids_snap:
        avg_q = max(500, (sum(ca.values()) + sum(cb.values())) / (len(ca) + len(cb)))
        min_q = avg_q * 1.5
        sb = []
        for p in sorted(_prev_bids_snap, reverse=True):
            pq = _prev_bids_snap[p]
            cq = cb.get(p, 0)
            if pq > 100 and cq < pq * 0.25:
                sb.append({"price": p, "vol": pq - cq})
        if len(sb) >= 4:
            gaps = sum(1 for i in range(1, len(sb))
                       if sb[i-1]["price"] - sb[i]["price"] <= 2.5)
            if gaps >= 3:
                tv = sum(x["vol"] for x in sb)
                tv_ratio = tv / (avg_q * max(1, len(sb)))
                if tv >= 4000 and tv_ratio >= 0.35:
                    cvd_ok = abs(cvd_roll) >= 500
                    price_ok = spot < prev_spot - 0.5
                    confirmed = cvd_ok and price_ok
                    sig_type = "CVD✓" if (confirmed and FLOW_QUALITY == "REAL") else "?"
                    sw = {"detected": True, "side": "SELL",
                          "signal": "SELL SWEEP {} levels {:,} ({})".format(len(sb), int(tv), sig_type),
                          "levels": len(sb), "volume": int(tv), "confirmed": confirmed,
                          "cum_cvd": cvd_roll, "tv_ratio": round(tv_ratio, 1)}
                    _sweep_cd = now
    _prev_asks_snap = ca
    _prev_bids_snap = cb
    with S_LOCK: S["sweep"] = sw
    return sw


_prev_bids_snap = {}
_prev_asks_snap = {}
_sweep_cd = 0
_vac_cd = 0
_prev_bid_tot = 0
_prev_ask_tot = 0


def detect_vacuum(bids, asks, spot):
    """
    INSTITUTIONAL-GRADE VACUUM v2.0
    - Adaptive threshold: use rolling stats for what "high" evaporation means
    - CVD direction correlation added (real vs cancel)
    - Per-side weighting: threshold scales with book size
    - Tracks both magnitude and residual so brief fluctuations don't fire
    """
    global _vac_cd, _prev_bid_tot, _prev_ask_tot
    import time as _t; now = _t.time()
    if now - _vac_cd < 8:
        _v = dict(S.get("vacuum", {"detected": False, "side": "NONE"})); _v["detected"]=False
        return _v

    cb = sum(b["qty"] for b in bids)
    ca = sum(a["qty"] for a in asks)
    vac = {"detected": False, "side": "NONE", "signal": "", "pct": 0}
    if _prev_bid_tot > 0 and _prev_ask_tot > 0:
        bd = (_prev_bid_tot - cb) / _prev_bid_tot * 100
        ad = (_prev_ask_tot - ca) / _prev_ask_tot * 100
        cvd_roll = rolling_cvd(3) if FLOW_QUALITY == "REAL" else 0

        # Adaptive threshold: high evap for this market = 60th percentile of recent drops
        cvd_abs = abs(cvd_roll)
        # Institutional: need BIG absolute liquidation, not just normal flow
        big_cvd = cvd_abs >= 500  # raised from 200 — 500+ = institutional

        # BID side vanishes (ask pressure, bearish) — liquidity evaporates under bids
        if bd >= 55 and big_cvd:
            vac = {"detected": True, "side": "BID_VACUUM",
                   "signal": "BID VACUUM {:.0f}% evaporated [CVD {}]".format(bd, cvd_roll),
                   "pct": round(bd, 1), "side_qty": int(_prev_bid_tot - cb)}
            _vac_cd = now
        # ASK side vanishes (bid pressure, bullish) — offers pulled, book thin
        elif ad >= 55 and big_cvd:
            vac = {"detected": True, "side": "ASK_VACUUM",
                   "signal": "ASK VACUUM {:.0f}% evaporated [CVD {}]".format(ad, cvd_roll),
                   "pct": round(ad, 1), "side_qty": int(_prev_ask_tot - ca)}
            _vac_cd = now
    _prev_bid_tot = cb
    _prev_ask_tot = ca
    with S_LOCK: S["vacuum"] = vac
    return vac


DEFAULT_RECORD = os.environ.get("FYERS_RECORD", "session.jsonl")
RECORD_FILE = DEFAULT_RECORD if os.environ.get("FYERS_RECORD") else ""

def _truth_path(symbol=None):
    """Return a date/symbol-specific raw evidence file unless explicitly overridden."""
    if os.environ.get("FYERS_RECORD"):
        return os.environ.get("FYERS_RECORD")
    sym = (symbol or S.get("sym", "NIFTY")).replace(":", "_")
    d = datetime.now().strftime("%Y%m%d")
    return os.path.join(TRUTH_RECORD_DIR, "marketos_truth_{}_{}.jsonl".format(d, sym))

def set_record(on):
    global RECORD_FILE
    RECORD_FILE = _truth_path() if on else ""
    print("[REC] " + ("Recording ON -> " + RECORD_FILE if on else "Recording OFF"))

if TRUTH_RECORD_ENABLED and not RECORD_FILE:
    RECORD_FILE = _truth_path()
    print("[REC] Truth recorder default ON -> {}".format(RECORD_FILE))

def flow_integrity_snapshot():
    """Machine-readable proof state for the trade-flow pipeline."""
    total = S.get("trade_total_qty", 0)
    classified = S.get("trade_classified_qty", 0)
    coverage = (classified / total) if total else 0.0
    return {
        "flow_quality": FLOW_QUALITY,
        "quote_available": bool(S.get("quote_available", False)),
        "quote_fields_seen": list(S.get("quote_fields_seen", [])),
        "trade_events": int(S.get("trade_count", 0)),
        "classified_events": int(S.get("trade_classified_events", 0)),
        "trade_total_qty": int(total),
        "classified_qty": int(classified),
        "unclassified_qty": int(S.get("trade_unclassified", 0)),
        "classification_coverage": round(coverage, 6),
        "actual_buy_volume": int(S.get("actual_buy_volume", 0)),
        "actual_sell_volume": int(S.get("actual_sell_volume", 0)),
        "actual_cvd": int(S.get("cvd_session", 0)),
        "book_flow_proxy": int(S.get("proxy_cvd_session", 0)),
        "last_trade": S.get("last_trade"),
        "last_sequence": S.get("last_sequence"),
        "last_feed_ts": S.get("last_feed_ts"),
        "last_vtt": S.get("last_vtt"),
        "last_vtt_diff": S.get("last_vtt_diff"),
        "quote_probe_ticks": int(S.get("quote_probe_ticks", 0)),
        "quote_probe_populated_ticks": int(S.get("quote_probe_populated_ticks", 0)),
        "quote_field_population": {
            "ltp": int(S.get("quote_probe_ltp", 0)),
            "ltt": int(S.get("quote_probe_ltt", 0)),
            "ltq": int(S.get("quote_probe_ltq", 0)),
            "vtt": int(S.get("quote_probe_vtt", 0)),
            "vtt_diff": int(S.get("quote_probe_vtt_diff", 0)),
        },
        "vtt_delta_qty": int(S.get("vtt_delta_qty", 0)),
        "ltq_sum": int(S.get("ltq_sum", 0)),
        "vtt_recon_samples": int(S.get("vtt_recon_samples", 0)),
        "vtt_recon_abs_error": int(S.get("vtt_recon_abs_error", 0)),
    }

def _record_tick(bids, asks, spot, tbq, tsq, trade=None, feed_meta=None, raw_bids=None, raw_asks=None):
    """Append one lossless-enough canonical Fyers evidence record.

    This is deliberately descriptive, not interpretive. It preserves all
    non-zero book levels (up to the 50 received by TBT), original rank, order
    count, TBQ/TSQ, quote/trade fields when available, transport metadata,
    and a deterministic book checksum.
    """
    global RECORD_FILE
    if not TRUTH_RECORD_ENABLED and not RECORD_FILE:
        return
    try:
        path = RECORD_FILE or _truth_path()
        event_t = _event_timestamp(feed_meta, trade)
        raw_bids = raw_bids if raw_bids is not None else bids
        raw_asks = raw_asks if raw_asks is not None else asks
        b_sorted = sorted(bids, key=lambda x: x.get("price", 0), reverse=True)
        a_sorted = sorted(asks, key=lambda x: x.get("price", 0))
        book_b = [[round(float(x["price"]),2), int(x["qty"]), x.get("orders"), int(x.get("level", i))] for i,x in enumerate(b_sorted[:50])]
        book_a = [[round(float(x["price"]),2), int(x["qty"]), x.get("orders"), int(x.get("level", i))] for i,x in enumerate(a_sorted[:50])]
        canonical = json.dumps({"b":book_b,"a":book_a}, separators=(",",":"), sort_keys=True).encode("utf-8")
        import hashlib
        with S_LOCK:
            local_seq = int(S.get("local_update_seq",0))
            dq = dict(S.get("data_quality",{}))
            sym = S.get("sym","NIFTY")
        rec = {
            "schema": TRUTH_SCHEMA,
            "t": round(event_t,3),
            "receive_ts": round(time.time(),3),
            "symbol": sym,
            "symbol_fyers": S.get("sym_str"),
            "local_update_seq": local_seq,
            "spot": round(float(spot),2) if spot else None,
            "tbq": int(tbq or 0),
            "tsq": int(tsq or 0),
            "bids": book_b,
            "asks": book_a,
            "raw_bids": [[round(float(x.get("price",0)),2),int(x.get("qty",0)),x.get("orders"),int(x.get("level",i))] for i,x in enumerate(raw_bids[:50])],
            "raw_asks": [[round(float(x.get("price",0)),2),int(x.get("qty",0)),x.get("orders"),int(x.get("level",i))] for i,x in enumerate(raw_asks[:50])],
            "book_normalization":{"raw_bid_entries":len(raw_bids),"raw_ask_entries":len(raw_asks),
                                   "unique_bid_levels":len(bids),"unique_ask_levels":len(asks),
                                   "duplicate_bid_entries":max(0,len(raw_bids)-len(bids)),
                                   "duplicate_ask_entries":max(0,len(raw_asks)-len(asks))},
            "best_bid": book_b[0][0] if book_b else None,
            "best_ask": book_a[0][0] if book_a else None,
            "book_span": {
                "low": min([x[0] for x in book_b+book_a], default=None),
                "high": max([x[0] for x in book_b+book_a], default=None),
                "points": (max([x[0] for x in book_b+book_a])-min([x[0] for x in book_b+book_a])) if (book_b or book_a) else None
            },
            "trade": trade or None,
            # Fixed-shape Quote probe: preserves absent fields as null so a
            # session can be audited for Quote-field population without
            # confusing "field absent" with zero. This is diagnostic only;
            # it does not alter trading logic or CVD.
            "quote_probe": {
                "ltp": (trade or {}).get("ltp"),
                "ltt": (trade or {}).get("ltt"),
                "ltq": (trade or {}).get("ltq"),
                "vtt": (trade or {}).get("vtt"),
                "vtt_diff": (trade or {}).get("vtt_diff"),
                "present": {
                    k: (k in (trade or {}))
                    for k in ("ltp", "ltt", "ltq", "vtt", "vtt_diff")
                },
            },
            "feed_meta": feed_meta or {},
            "data_quality": dq,
            "flow_integrity": flow_integrity_snapshot(),
            "book_sha256": hashlib.sha256(canonical).hexdigest(),
        }
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "a", encoding="utf-8", buffering=1) as f:
            f.write(json.dumps(rec, separators=(",",":"), ensure_ascii=False) + "\n")
    except Exception as e:
        with S_LOCK:
            S["err"] = "truth recorder: " + str(e)

def _normalize_book_levels(levels, side):
    """Aggregate duplicate Fyers depth entries at the same price.

    The 2026-08-14 recording showed repeated prices inside the 50-entry arrays.
    Analytics therefore use unique price levels while the truth recorder keeps
    the raw SDK entries. This is aggregation only; it never infers execution.
    """
    agg = {}
    for i, x in enumerate(levels or []):
        try:
            p = round(float(x.get("price", 0)), 2)
            q = int(max(0, x.get("qty", 0)))
        except Exception:
            continue
        if p <= 0 or q <= 0:
            continue
        if p in agg:
            z = agg[p]
            z["qty"] += q
            ov = x.get("orders")
            if ov is not None:
                z["orders"] = int(z.get("orders", 0) or 0) + int(ov)
            z["source_entries"] += 1
        else:
            ov = x.get("orders")
            agg[p] = {"price":p,"qty":q,"orders":int(ov) if ov is not None else None,
                      "level":int(x.get("level",i)) if x.get("level") is not None else i,
                      "source_entries":1}
    out = list(agg.values())
    out.sort(key=lambda z:z["price"], reverse=(side=="BID"))
    return out


def _book_quality_snapshot(bids, asks, raw_bids=None, raw_asks=None):
    rb = raw_bids if raw_bids is not None else bids
    ra = raw_asks if raw_asks is not None else asks
    prices=[float(x["price"]) for x in list(bids or [])+list(asks or []) if x.get("price",0)>0]
    span=(max(prices)-min(prices)) if prices else None
    return {"raw_bid_entries":len(rb or []),"raw_ask_entries":len(ra or []),
            "unique_bid_levels":len(bids or []),"unique_ask_levels":len(asks or []),
            "duplicate_bid_entries":max(0,len(rb or [])-len(bids or [])),
            "duplicate_ask_entries":max(0,len(ra or [])-len(asks or [])),
            "visible_span_points":round(span,2) if span is not None else None,
            "depth_complete_raw_50":bool(len(rb or [])>=50 and len(ra or [])>=50),
            "depth_complete_unique_50":bool(len(bids or [])>=50 and len(asks or [])>=50)}


def push_update(bids, asks, spot, feed_name, n_levels, tbq=0, tsq=0, trade=None, feed_meta=None):
    # Preserve SDK entries for audit; analytics operate on unique price levels.
    raw_bids=[dict(x) for x in (bids or [])]
    raw_asks=[dict(x) for x in (asks or [])]
    bids=_normalize_book_levels(raw_bids,"BID")
    asks=_normalize_book_levels(raw_asks,"ASK")
    n_levels=max(len(bids),len(asks))
    if bids and asks and bids[0]["price"] >= asks[0]["price"]: return
    if spot and spot <= 0: return
    # Local monotonic sequence exists even when the supported SDK callback
    # exposes no exchange sequence number. It is explicitly NOT exchange seq.
    with S_LOCK:
        S["local_update_seq"] = int(S.get("local_update_seq",0)) + 1
        _lu = S["local_update_seq"]
    # Basic transport/book quality flags are stored with every truth record.
    _bq=_book_quality_snapshot(bids,asks,raw_bids,raw_asks)
    _quality={"bid_levels":len(bids),"ask_levels":len(asks),
        "depth_complete_50":bool(len(raw_bids)>=50 and len(raw_asks)>=50),
        "depth_complete_unique_50":bool(len(bids)>=50 and len(asks)>=50),
        "crossed":bool(bids and asks and bids[0]["price"]>=asks[0]["price"]),
        "tbq_present":bool(tbq),"tsq_present":bool(tsq),
        "trade_fields_present":bool(trade and any(k in trade for k in ("ltp","ltt","ltq","vtt","vtt_diff"))),
        "exchange_sequence_present":bool(feed_meta and feed_meta.get("sequence_no") is not None),**_bq}
    with S_LOCK:
        _prev_t = S.get("last_event_timestamp")
        _gap_s = (float(_event_timestamp(feed_meta, trade)) - float(_prev_t)) if _prev_t is not None else 0.0
        _quality["inter_update_gap_s"] = round(max(0.0, _gap_s), 3)
        _quality["stale_gap"] = bool(_gap_s > 2.0)
        S["last_event_timestamp"] = _event_timestamp(feed_meta, trade)
        S["data_quality"] = _quality
        S["quote_probe_ticks"] = int(S.get("quote_probe_ticks", 0)) + 1
        if trade:
            qkeys = ("ltp", "ltt", "ltq", "vtt", "vtt_diff")
            populated = False
            for _k in qkeys:
                if _k in trade:
                    S["quote_probe_" + _k] = int(S.get("quote_probe_" + _k, 0)) + 1
                    populated = True
            if populated:
                S["quote_probe_populated_ticks"] = int(S.get("quote_probe_populated_ticks", 0)) + 1
    # per-symbol glitch guard: NIFTY->BNF toggle is a legit jump, so compare
    # only against the last spot of the SAME symbol.
    _sym = S.get("sym","NIFTY")
    _lsm = S.get("_last_spot") or {}
    if isinstance(_lsm, float): _lsm = {"NIFTY": _lsm}      # migrate old value
    if _lsm.get(_sym) and spot and abs(spot-_lsm[_sym])/_lsm[_sym] > 0.05:
        return
    _lsm[_sym] = spot; S["_last_spot"] = _lsm
    # ---- Flow tracking ----
    # tbq/tsq are NEVER CVD. They are retained only as an exchange-depth proxy.
    proxy_delta = update_proxy_flow(tbq, tsq)
    actual_delta = process_trade_fields(trade or {},
                                        best_bid=bids[0]["price"] if bids else 0,
                                        best_ask=asks[0]["price"] if asks else 0)
    # Record AFTER trade processing so flow_integrity reflects this event.
    _record_tick(bids, asks, spot, tbq, tsq, trade=trade, feed_meta=feed_meta, raw_bids=raw_bids, raw_asks=raw_asks)
    if FLOW_QUALITY == "REAL" and S.get("trade_count", 0) > 0:
        cvd_delta = actual_delta
        cvd_roll = rolling_cvd(3)
        flow_for_detectors = cvd_roll
    else:
        cvd_delta = 0
        cvd_roll = None
        flow_for_detectors = None

    # Primary book-native state. This remains valid when trade data is absent.
    inst_now = _event_timestamp(feed_meta, trade)
    _bms = BOOK_MS.setdefault(S.get("sym","NIFTY"), BookMicrostructure())
    book_ms = _bms.update(bids, asks, inst_now)
    with S_LOCK:
        S["book_micro"] = book_ms

    # Update displayed-liquidity state independently of trade flow.
    # A depth decrease is NEVER treated as execution.
    inst_now = _event_timestamp(feed_meta, trade)
    PLB.update(bids, asks, event_ts=inst_now)
    pending_trade = S.get("_pending_level_trade")
    if pending_trade:
        PLB.register_trade(pending_trade["price"], pending_trade["qty"], pending_trade["side"])
    RS["cvd_delta"].add(cvd_delta if FLOW_QUALITY == "REAL" else 0)

    # ---- INSTITUTIONAL DETECTORS: sweep + vacuum (behaviour over time) ----
    inst_now = _event_timestamp(feed_meta, trade)
    swe = SWEEP_DETECTOR.update(bids, asks, spot, flow_for_detectors, inst_now)
    vacd = VACUUM_DETECTOR.update(bids, asks, spot, flow_for_detectors, inst_now)
    with S_LOCK:
        S["inst_sweep"] = swe
        S["inst_vacuum"] = vacd
    for det_name, det in (("SWEEP", SWEEP_DETECTOR), ("VACUUM", VACUUM_DETECTOR)):
        for a in det._pending_alerts:
            m = "[{}] {} {}@{} [{} \u2014 {:.0f}%]".format(
                datetime.now().strftime("%H:%M:%S"), det_name, a.side, a.price, a.class_name, a.confidence)
            print(m)
    for det_name,det in (("BOOK_SWEEP",SWEEP_DETECTOR),("BOOK_VACUUM",VACUUM_DETECTOR)):
        for a in det._pending_alerts:
            if a.state in ("CONFIRMED","CONTINUING","DOMINANT","EXHAUSTING"):
                ev_side="LONG" if a.side=="ASK" else "SHORT"
                OUTCOMES.add(det_name,ev_side,a.price,inst_now,anchor_spot=spot,
                    evidence={"confidence":round(a.confidence,2),"class":a.class_name,
                              "levels":a.raw_levels,"flow_quality":FLOW_QUALITY})

    pb = S.get("prev_bids", {})
    pa = S.get("prev_asks", {})
    with S_LOCK: bnf = S.get("sym") == "BANKNIFTY"
    r = analyze(bids, asks, spot, pb, pa, bnf)

    # Track exchange imbalance for scoring
    if tbq > 0 or tsq > 0:
        RS["bid_pct"].add(0.0)   # no-op — exchange pct handled in analyze

    with S_LOCK:
        S["delta_hist"].append(r["delta"])
        S["sess_delta"] = S.get("sess_delta", 0) + r["delta"]

    h = list(S["delta_hist"])
    if len(h) >= 5:
        l5 = h[-5:]
        if all(d > 0 for d in l5):   dt = "SUSTAINED BUY"
        elif all(d < 0 for d in l5): dt = "SUSTAINED SELL"
        elif sum(l5) > 0:            dt = "MILD BUY"
        elif sum(l5) < 0:            dt = "MILD SELL"
        else:                         dt = "NEUTRAL"
    else:
        dt = "NEUTRAL"

    now = datetime.now().strftime("%H:%M:%S")
    new_alerts = []

    # ---- INSTITUTIONAL DETECTORS: absorption + iceberg (behaviour over time) ----
    # Confirmed events show ONCE, persist live in S["inst_abs"]/S["inst_ice"].
    with S_LOCK:
        inst_now = _event_timestamp(feed_meta, trade)
        if FLOW_QUALITY == "REAL":
            abs_active = ABS_DETECTOR.update(bids, asks, spot, cvd_roll, inst_now)
            ice_active = ICE_DETECTOR.update(bids, asks, spot, cvd_roll, inst_now)
        else:
            # No execution tape: do not label depletion/replenishment as
            # absorption or iceberg. Book-native replenishment is reported separately.
            abs_active = []
            ice_active = []
        S["inst_abs"] = abs_active
        S["inst_ice"] = ice_active
        for det_name, det in (("ABSORPTION", ABS_DETECTOR), ("ICEBERG", ICE_DETECTOR)):
            for a in det._pending_alerts:
                m = "[{}] {} {}@{} [{} — {:.0f}%]".format(
                    now, det_name, a.side, a.price, a.class_name, a.confidence)
                new_alerts.append(m); print(m)

    # ---- TOXICITY + MARKET INTELLIGENCE (market-wide context, Modules 12-15) ----
    tox = TOX.update(bids, asks, spot, cvd_roll or 0.0, tbq, tsq)
    intel = INTEL.update(spot, cvd_roll or 0.0, tox,
                         S.get("inst_abs",[]), S.get("inst_ice",[]),
                         S.get("inst_sweep",[]), S.get("inst_vacuum",[]))
    with S_LOCK:
        S["toxicity"] = tox
        S["intel"] = intel
        S["dataq"] = {"flow_quality": FLOW_QUALITY, "book_levels": n_levels,
                      "feed": feed_name, "flow_proxy": FLOW_QUALITY!="REAL",
                      "trade_count": S.get("trade_count",0),
                      "trade_total_qty": S.get("trade_total_qty",0),
                      "trade_classified_qty": S.get("trade_classified_qty",0),
                      "classification_coverage": (S.get("trade_classified_qty",0) / S.get("trade_total_qty",1)) if S.get("trade_total_qty",0) else 0,
                      "unclassified_trade_qty": S.get("trade_unclassified",0),
                      "proxy_flow_session": S.get("proxy_cvd_session",0),
                      "actual_cvd_session": S.get("cvd_session",0),
                       "cvd_status":"REAL" if FLOW_QUALITY=="REAL" else "UNAVAILABLE",
                       "tbq_tsq_semantics":"EXCHANGE_REPORTED_BOOK_TOTALS_NOT_CVD",
                       "book_flow_status":"AVAILABLE",
                       "book_event_status":"AVAILABLE_WITHOUT_TRADE_TAPE",
                       "visible_book_span_points":S.get("visible_book_span_points"),
                       "book_micro": S.get("book_micro",{}),
                       "session_liquidity_profile": S.get("profile",{}), "empirical_edge":S.get("empirical_edge",{}), "risk":S.get("risk",{})}

    # ---- PERSISTENT LIQUIDITY MAP: update BEFORE risk/target calculation ----
    # This is intentionally upstream of Trigger/Risk. A target must be derived
    # from the current tick's observed map, not from the previous tick's profile.
    _pf = PROFILE.get(S.get("sym","NIFTY"))
    if _pf is None:
        _pf = LiquidityProfileV11(S.get("sym","NIFTY"))
        PROFILE[S.get("sym","NIFTY")] = _pf
    _pf.update(bids, asks, inst_now)
    if inst_now - float(S.get("_profile_save_ts",0) or 0) >= 300:
        try: _pf.save_history()
        except Exception: pass
        S["_profile_save_ts"] = inst_now
    with S_LOCK:
        S["profile"] = _pf.to_dict(spot)

    # ---- LIQUIDITY PATH / INTERACTION ----
    # Map-derived target becomes a stateful interaction object. It is upstream
    # of trigger/risk so the same observed target drives both entry evidence and
    # later target-rejection/continuation research.
    _pe = PATH_ENGINE.setdefault(S.get("sym","NIFTY"), LiquidityPathEngine(S.get("sym","NIFTY")))
    liquidity_path = _pe.update(S.get("profile",{}), bids, asks, spot, S.get("book_micro",{}), inst_now)
    path_events = list(liquidity_path.get("events",[]) or [])
    with S_LOCK:
        S["liquidity_path"] = {"LONG":liquidity_path.get("LONG",{}),
                                "SHORT":liquidity_path.get("SHORT",{}),
                                "events":path_events}
        for _ev in path_events[-6:]:
            _tag="[PATH] {} {} @{}".format(_ev.get("event"),_ev.get("side"),_ev.get("target"))
            if not any(_tag in x for x in list(S.get("liquidity_path_alerts",[]))):
                S["liquidity_path_alerts"].appendleft(_tag)

    # Record path events as separate forward-outcome families. The anchor is the
    # live spot when the path transition is observed, not the target price.
    for _pev in path_events[-6:]:
        if float(_pev.get("ts",0) or 0) < inst_now-0.5:
            continue
        _pev_ev=_pev.get("event")
        _pev_side=_pev.get("side")
        if _pev_ev in ("LIQUIDITY_DEPLETION","LIQUIDITY_RELOAD","LIQUIDITY_DEFENSE","LIQUIDITY_CLEARANCE","LIQUIDITY_ACCEPTANCE","TARGET_CONTINUATION","TARGET_REJECTION") and _pev_side in ("LONG","SHORT"):
            OUTCOMES.add(_pev_ev,_pev_side,float(_pev.get("target") or spot),inst_now,
                         anchor_spot=spot,evidence=dict(_pev.get("evidence") or {}))

    # ---- FLOW SURGE + TRIGGER + DECISION GATE ----
    # TriggerEngine answers: did the structural condition fire?
    # DecisionEngine answers: is the setup eligible to act on today?
    fl = FLOW.update(cvd_roll or 0.0)
    dv = DIV.update(spot, cvd_roll or 0.0, inst_now)
    trig = TRIGGER.update(spot, intel,
                          S.get("inst_abs",[]), S.get("inst_ice",[]),
                          S.get("inst_sweep",[]), S.get("inst_vacuum",[]),
                          tox, fl, inst_now, dv, book_ms=S.get("book_micro",{}),
                          liquidity_path=S.get("liquidity_path",{}))
    risk = RISK.update(spot, trig.get("side","NONE"), trig, S.get("profile",{}))
    empirical={"status":"RESEARCH_ONLY","event":"","n":0}
    if trig.get("side") in ("LONG","SHORT"):
        evname = DECISION_EVENT_MAP.pick(S.get("inst_sweep",[]), S.get("inst_vacuum",[]), S.get("book_micro",{}), S.get("liquidity_path",{}))
        ok,st,why=EDGE_GATE.evaluate(evname,trig.get("side"))
        empirical={"status":why,"event":evname,"side":trig.get("side"),"supported":ok,"stats":st}
    decision = DECISION.update(spot, trig, risk, empirical, intel, tox,
                               S.get("book_micro",{}), S.get("data_quality",{}),
                               S.get("rails",{}), S.get("profile",{}), inst_now)
    # Record every actionable setup separately from generic book events. This is
    # the bridge from "event edge" to "actual trading decision edge".
    if decision.get("state") in ("PROVISIONAL-LONG","PROVISIONAL-SHORT","VALIDATED-LONG","VALIDATED-SHORT"):
        _ds=decision.get("side")
        _ep=float(decision.get("entry") or spot)
        OUTCOMES.add("TRADE_SETUP",_ds,_ep,inst_now,anchor_spot=_ep,
                     evidence={"state":decision.get("state"),"event":decision.get("event"),
                               "stop":decision.get("stop"),"target":decision.get("target"),
                               "rr":decision.get("net_rr"),"empirical":decision.get("empirical_status"),
                               "toxicity":tox.get("stress"),"regime":intel.get("phase"),
                               "flow_quality":FLOW_QUALITY})
    try:
        _edge_rows = {}
        for _ev in ("BOOK_SWEEP","BOOK_VACUUM","BOOK_PRESSURE","VISIBLE_LIQUIDITY_DEPLETION","VISIBLE_REPLENISHMENT",
                    "LIQUIDITY_CLEARANCE","LIQUIDITY_ACCEPTANCE","TARGET_CONTINUATION","TARGET_REJECTION","TRADE_SETUP"):
            _edge_rows[_ev] = {"long": EDGE_GATE.stats(_ev,"LONG"), "short": EDGE_GATE.stats(_ev,"SHORT")}
    except Exception:
        _edge_rows = {}
    with S_LOCK:
        S["edge_event_stats"] = _edge_rows
        S["flow"] = fl; S["div"] = dv; S["trigger"] = trig; S["risk"] = risk
        S["empirical_edge"] = empirical; S["decision"] = decision

    # ---- LEVEL MEMORY: trace + persist important price zones ----
    _local_depth_mass=sum(x.get("qty",0) for x in bids if abs(x["price"]-spot)<=2.5)+sum(x.get("qty",0) for x in asks if abs(x["price"]-spot)<=2.5)
    LEVELS.update(spot,None,inst_now,depth_mass=_local_depth_mass)
    if (inst_now // 300) != (S.get("_lm_ts",0)):
        S["_lm_ts"] = int(inst_now // 300); LEVELS.save()      # persist ~every 5 min; SESSION.save()
    with S_LOCK:
        S["imp_levels"] = LEVELS.important_levels(top=4)

    # ---- SESSION RAILS: PDH/PDL/PDC + VWAP + today H/L ----
    _bkvol = sum(b["qty"] for b in bids) + sum(a["qty"] for a in asks) if (bids or asks) else 0
    SESSION.update(spot, tbq, tsq, vol=_bkvol)
    with S_LOCK:
        S["rails"] = SESSION.to_dict()

    # ---- LEVEL INTERACTION: which reference is in play, breaking or rejecting? ----
    _il  = LEVELS.important_levels(top=4)
    _wr  = list(S.get("inst_abs", [])) + list(S.get("inst_ice", []))
    _sup_w = max([w for w in _wr if w["side"]=="BID"], key=lambda w: w["conf"], default=None)
    _res_w = max([w for w in _wr if w["side"]=="ASK"], key=lambda w: w["conf"], default=None)
    itx = INTERACT.update(spot, S.get("rails",{}), _il, _sup_w, _res_w, fl.get("dirn",""), inst_now)
    with S_LOCK:
        S["interact"] = itx

    # Forward-outcome dataset. These are observations, not trade signals.
    OUTCOMES.update(spot, inst_now)
    OUTCOME_STORE.flush(list(OUTCOMES.completed))
    # Sample structural events only when they are actually observable from book.
    # Research sampling: only local book changes are eligible as event anchors.
    # Far-away level changes are observations of the book, not immediate trade
    # hypotheses. Keep the raw book for later analysis, but avoid contaminating
    # forward-outcome statistics with thousands of irrelevant levels.
    _local_radius = max(5.0, min(12.0, abs(SESSION.to_dict().get("vwap") or spot)*0.00025))
    _deps=[x for x in (book_ms.get("depleted") or []) if abs(x.get("price",spot)-spot)<=_local_radius]
    _refs=[x for x in (book_ms.get("replenished") or []) if abs(x.get("price",spot)-spot)<=_local_radius]
    if _deps:
        x=min(_deps,key=lambda z:(abs(z["price"]-spot), z.get("ratio",1.0)))
        OUTCOMES.add("VISIBLE_LIQUIDITY_DEPLETION", x["side"], x["price"], inst_now,anchor_spot=spot, evidence={"ratio":x["ratio"],"qty_before":x["before"],"qty_after":x["after"],
                      "distance":round(abs(x["price"]-spot),2),"sample_scope":"LOCAL_BOOK","stale_gap":bool(S.get("data_quality",{}).get("stale_gap"))})
    if _refs:
        x=min(_refs,key=lambda z:(abs(z["price"]-spot), -z.get("ratio",1.0)))
        OUTCOMES.add("VISIBLE_REPLENISHMENT", x["side"], x["price"], inst_now,anchor_spot=spot, evidence={"ratio":x["ratio"],"qty_before":x["before"],"qty_after":x["after"],
                      "distance":round(abs(x["price"]-spot),2),"sample_scope":"LOCAL_BOOK","stale_gap":bool(S.get("data_quality",{}).get("stale_gap"))})
    if abs(float(r.get("book_pressure",0.0) or 0.0)) >= 8.0:
        bp_side = "LONG" if float(r.get("book_pressure",0.0)) > 0 else "SHORT"
        OUTCOMES.add("BOOK_PRESSURE", bp_side, spot, inst_now, anchor_spot=spot,
                     evidence={"pressure":round(float(r.get("book_pressure",0.0)),2),
                               "microprice":S.get("book_micro",{}).get("microprice"),
                               "mid":S.get("book_micro",{}).get("mid"),
                               "spread":S.get("book_micro",{}).get("spread"),
                               "flow_quality":FLOW_QUALITY})

    # Strong DOM signal
    if r["dst"] >= 80:
        new_alerts.append("[{}] DOM: {} ({}%)".format(now, r["dsig"], r["dst"]))

    if S.get("tick_count",0) % 50 == 0:
        with S_LOCK:
            S["outcome_stats_60s"] = {
                "depletion": OUTCOMES.stats("VISIBLE_LIQUIDITY_DEPLETION",60),
                "replenishment": OUTCOMES.stats("VISIBLE_REPLENISHMENT",60)}

    # ---- Historical comparison: track sup/res changes over 5/30 min windows ----
    import time as _hist_t; _now = inst_now if REPLAY_MODE else _hist_t.time()
    sup5 = S.get("sup_hist5_ts", 0)
    res5 = S.get("res_hist5_ts", 0)
    sup30 = S.get("sup_hist30_ts", 0)
    res30 = S.get("res_hist30_ts", 0)
    if _now - sup5 >= 300:   # 5 min snapshot
        S["sup_hist5"].append({"ts": _now, "levels": r["sup"]})
        S["sup_hist5_ts"] = _now
    if _now - res5 >= 300:
        S["res_hist5"].append({"ts": _now, "levels": r["res"]})
        S["res_hist5_ts"] = _now
    if _now - sup30 >= 1800:  # 30 min snapshot
        S["sup_hist30"].append({"ts": _now, "levels": r["sup"]})
        S["sup_hist30_ts"] = _now
    if _now - res30 >= 1800:
        S["res_hist30"].append({"ts": _now, "levels": r["res"]})
        S["res_hist30_ts"] = _now

    # Compute strength: compare current top1 qty vs 5-min/30-min ago
    def compare_strength(curr, hist_key, attr="qty"):
        if not S.get(hist_key): return ""
        snap = S[hist_key][-1]["levels"]
        if not snap: return ""
        curr_top = curr[0].get(attr, 0) if curr else 0
        past_top = snap[0].get(attr, 0) if snap else 0
        if past_top > 0:
            chg = (curr_top - past_top) / past_top * 100
            if chg > 10:   return "▲{:.0f}%".format(chg)
            if chg < -10:  return "▼{:.0f}%".format(abs(chg))
        return "→"

    sup5_str  = compare_strength(r["sup"],  "sup_hist5")
    res5_str  = compare_strength(r["res"],  "res_hist5")
    sup30_str = compare_strength(r["sup"],  "sup_hist30")
    res30_str = compare_strength(r["res"],  "res_hist30")

    with S_LOCK:
        S.update({
            "live": True, "spot": spot, "bids": bids[:10], "asks": asks[:10],
            "bp": r["bp"], "ap": r["ap"], "tb": r["tb"], "ta": r["ta"],
            "sig": r["sig"], "dirn": r["dirn"],
            "nbp": r["nbp"], "nap": r["nap"], "nsig": r["nsig"], "ndirn": r["ndirn"],
            "dom": r["dom"], "wsig": r["wsig"], "bw": r["bw"], "aw": r["aw"],
            "absorb": r["absorb"], "iceberg": r["iceberg"],
            "delta": r["delta"], "delta_trend": dt,
            "vb": r["vb"], "va": r["va"], "dr": r["dr"], "conc": r["conc"],
            "sup": r["sup"], "res": r["res"],
            "bid_zones": r["bid_zones"], "ask_zones": r["ask_zones"],
            "sup5s": sup5_str, "res5s": res5_str,
            "sup30s": sup30_str, "res30s": res30_str,
            "dsig": r["dsig"], "dc": r["dc"], "dst": r["dst"],
            "bull": r["bull"], "bear": r["bear"], "sigs": r["sigs"],
            "book_pressure": r.get("book_pressure",0.0),
            "cvd_valid": r.get("cvd_valid",False),
            "feed": feed_name, "depth_levels": n_levels,
            "last": now, "err": None,
            "tick_count": S.get("tick_count", 0) + 1,
            "sweep":swe,"vacuum":vacd,
            "book_normalization":_quality,
            "visible_book_span_points":_quality.get("visible_span_points"),
            "level_mem":S.get("level_memory",[]),
            "prev_spot": spot,
        })
        S["prev_bids"] = {b["price"]: b["qty"] for b in bids}
        S["prev_asks"] = {a["price"]: a["qty"] for a in asks}
        for m in new_alerts: S["alerts"].appendleft(m)



# ============================================================
# MARKETOS v11 EXTENSIONS — LIVE DECISION + EMPIRICAL LEARNING
# ============================================================
# These modules sit on top of the v10 detectors. They do not remove the
# existing evidence engines; they separate structural detection from the
# final trade-eligibility decision and make the outcome dataset mathematically
# usable as it grows.

class OutcomeTrackerV11(OutcomeTracker):
    """Outcome tracker with correct event-direction semantics and spot anchors.

    Critical correction: forward return is measured from the market spot at the
    instant the event was observed, NOT from the event price itself. The event
    price remains a separate feature. Direction is event-family specific:
      depletion: BID -> bearish, ASK -> bullish
      replenishment: BID -> bullish, ASK -> bearish
      sweep/vacuum: supplied LONG/SHORT direction is preserved.
    """
    EVENT_DIRECTION = {
        "VISIBLE_LIQUIDITY_DEPLETION": {"BID": -1, "ASK": 1},
        "VISIBLE_REPLENISHMENT": {"BID": 1, "ASK": -1},
        "LIQUIDITY_CLEARANCE": {"LONG": 1, "SHORT": -1},
        "LIQUIDITY_ACCEPTANCE": {"LONG": 1, "SHORT": -1},
        "TARGET_CONTINUATION": {"LONG": 1, "SHORT": -1},
        "TARGET_REJECTION": {"LONG": -1, "SHORT": 1},
    }
    def __init__(self):
        super().__init__()
        self.EVENT_CAPS.update({
            "LIQUIDITY_CLEARANCE":8000, "LIQUIDITY_ACCEPTANCE":8000,
            "TARGET_CONTINUATION":6000, "TARGET_REJECTION":6000})
        self.cooldown_s.update({"TRADE_SETUP":15.0,
                                "LIQUIDITY_CLEARANCE":3.0,
                                "LIQUIDITY_ACCEPTANCE":5.0,
                                "TARGET_CONTINUATION":5.0,
                                "TARGET_REJECTION":5.0})
    def add(self,event_name,side,price,now,anchor_spot=None,evidence=None):
        if not price or side not in ("BID","ASK","BUY","SELL","LONG","SHORT"): return False
        try:
            price=float(price); now=float(now)
            anchor=float(anchor_spot) if anchor_spot is not None else price
        except Exception:
            return False
        if isinstance(evidence,dict) and evidence.get("stale_gap"):
            return False
        mapping=self.EVENT_DIRECTION.get(event_name,{})
        if side in mapping:
            direction=mapping[side]
        else:
            direction=1 if side in ("BUY","LONG") else -1
        key=(event_name,side,round(price,1))
        cd=self.cooldown_s.get(event_name,0.75)
        prev=self.last_sample.get(key)
        if prev is not None and now-prev < cd:
            self.dropped_duplicates += 1
            return False
        self.last_sample[key]=now
        # Avoid an explosion of simultaneous nearly-identical local events.
        active_same=sum(1 for e in self.pending if e["event"]==event_name and
                        e["side"]==side and abs(e["p0"]-price)<=2.5)
        if active_same>=3:
            self.dropped_duplicates += 1
            return False
        self.pending.append({"event":event_name,"side":side,"dir":direction,
                             "p0":price,"anchor_spot":anchor,"t0":now,
                             "max_fav":0.0,"max_adv":0.0,"done":set(),
                             "evidence":dict(evidence or {})})
        return True
    def update(self,spot,now):
        if not spot: return
        keep=deque(maxlen=self.pending.maxlen)
        for e in self.pending:
            dt=float(now)-e["t0"]
            move=(float(spot)-e["anchor_spot"])*e["dir"]
            e["max_fav"]=max(e["max_fav"],move)
            e["max_adv"]=min(e["max_adv"],move)
            for h in self.HORIZONS:
                if h in e["done"] or dt<h: continue
                e["done"].add(h)
                row={"event":e["event"],"side":e["side"],"t0":e["t0"],
                     "horizon_s":h,"event_price":e["p0"],"entry_spot":e["anchor_spot"],
                     "spot":float(spot),"forward_move":move,"mfe":e["max_fav"],
                     "mae":e["max_adv"],"direction":e["dir"],"evidence":e["evidence"]}
                bucket=self.completed_by_event.setdefault(
                    e["event"],deque(maxlen=self.EVENT_CAPS.get(e["event"],self.EVENT_CAPS["DEFAULT"])))
                if len(bucket)>=bucket.maxlen:
                    self.dropped_capacity += 1
                else:
                    bucket.append(row); self.completed.append(row)
            if dt<max(self.HORIZONS): keep.append(e)
        self.pending=keep

    def load_persisted(self, pattern="marketos_edge_outcomes_*_*.jsonl", max_rows=100000):
        import glob
        rows=[]
        for fn in sorted(glob.glob(pattern)):
            try:
                with open(fn,encoding="utf-8") as f:
                    for line in f:
                        try: rows.append(json.loads(line))
                        except Exception: continue
            except Exception: continue
        rows=rows[-max_rows:]
        loaded=0
        for r in rows:
            ev=r.get("event")
            if not ev or r.get("horizon_s") not in self.HORIZONS: continue
            if "direction" not in r:
                side=r.get("side")
                if ev=="VISIBLE_LIQUIDITY_DEPLETION": r["direction"]={"BID":-1,"ASK":1}.get(side,0)
                elif ev=="VISIBLE_REPLENISHMENT": r["direction"]={"BID":1,"ASK":-1}.get(side,0)
                else: r["direction"]=1 if side in ("LONG","BUY") else -1
            b=self.completed_by_event.setdefault(ev,deque(maxlen=self.EVENT_CAPS.get(ev,self.EVENT_CAPS["DEFAULT"])))
            if len(b)<b.maxlen:
                b.append(r); self.completed.append(r); loaded+=1
        return loaded

class EmpiricalEdgeGateV11(EmpiricalEdgeGate):
    """Growing-sample edge statistics with uncertainty diagnostics.

    This gate never turns a heuristic score into a probability. It reports
    empirical win rate, mean move, MFE/MAE, and a Wilson interval for win rate.
    A sample can be PROVISIONAL before the long-run validation threshold is met.
    """
    def __init__(self):
        super().__init__()
        self.min_n=int(os.environ.get("MARKETOS_EDGE_MIN_N","100"))
        self.provisional_n=int(os.environ.get("MARKETOS_EDGE_PROVISIONAL_N","0"))
        self.horizon=int(os.environ.get("MARKETOS_EDGE_HORIZON","60"))
        self.min_ev=float(os.environ.get("MARKETOS_EDGE_MIN_MEAN_MOVE","0.8"))
        self.min_win=float(os.environ.get("MARKETOS_EDGE_MIN_WIN","0.54"))
    @staticmethod
    def _wilson(w,n,z=1.96):
        if n<=0:return (0.0,0.0)
        p=w/n; den=1+z*z/n; cen=(p+z*z/(2*n))/den
        half=z*math.sqrt(max(0,p*(1-p)/n+z*z/(4*n*n)))/den
        return max(0,cen-half),min(1,cen+half)
    def stats(self,event,side):
        rows=[r for r in OUTCOMES.event_rows(event,self.horizon) if r.get("direction",0)==(1 if side=="LONG" else -1)]
        if not rows:return {"n":0}
        vals=[float(r.get("forward_move",0)) for r in rows]
        wins=sum(v>0 for v in vals); lo,hi=self._wilson(wins,len(vals))
        sv=sorted(vals); mae=sorted(float(r.get("mae",0)) for r in rows)
        return {"n":len(vals),"win_rate":wins/len(vals),"win_ci_lo":lo,"win_ci_hi":hi,
                "mean_move":sum(vals)/len(vals),"median_move":sv[len(sv)//2],
                "mean_mfe":sum(float(r.get("mfe",0)) for r in rows)/len(rows),
                "median_mfe":sorted(float(r.get("mfe",0)) for r in rows)[len(rows)//2],
                "mean_mae":sum(mae)/len(mae),"median_mae":mae[len(mae)//2]}
    def evaluate(self,event,side):
        st=self.stats(event,side); n=st.get("n",0)
        if n<self.min_n:return False,st,"INSUFFICIENT_SAMPLE"
        ok=st.get("win_rate",0)>=self.min_win and st.get("mean_move",0)>=self.min_ev and st.get("win_ci_lo",0)>=0.50
        return ok,st,"EMPIRICALLY_SUPPORTED" if ok else "NO_DEMONSTRATED_EDGE"

class DecisionEventMap:
    @staticmethod
    def pick(sweeps,vacuums,book_ms,liquidity_path=None):
        lp=liquidity_path or {}
        for _side in ("LONG","SHORT"):
            _p=lp.get(_side,{}) if isinstance(lp,dict) else {}
            if _p.get("phase") in ("CLEARING","ACCEPTED","EXTENDING") and _p.get("entry_ready"):
                return "LIQUIDITY_ACCEPTANCE" if _p.get("accepted") else "LIQUIDITY_CLEARANCE"
            if _p.get("phase")=="REJECTING":
                return "TARGET_REJECTION"
        if any(c.get("state") in ("CONFIRMED","CONTINUING","COLLAPSING","EXHAUSTING") for c in sweeps):
            return "BOOK_SWEEP"
        if any(c.get("state") in ("CONFIRMED","COLLAPSING","RECOVERING","EXHAUSTING") for c in vacuums):
            return "BOOK_VACUUM"
        bm=book_ms or {}
        if bm.get("depleted"): return "VISIBLE_LIQUIDITY_DEPLETION"
        if bm.get("replenished"): return "VISIBLE_REPLENISHMENT"
        if abs(float(bm.get("microprice",0) or 0)-float(bm.get("mid",0) or 0)) >= 0.25:
            return "BOOK_PRESSURE"
        return "BOOK_SWEEP"
DECISION_EVENT_MAP=DecisionEventMap()

class RiskEngineV11(RiskEngine):
    """Risk envelope with lot-aware sizing, costs, daily lockout and map targets.

    It still does not place broker orders. It produces an execution plan that a
    separate broker adapter can consume after explicit enablement.
    """
    def __init__(self):
        super().__init__()
        self.max_daily_loss=float(os.environ.get("MARKETOS_MAX_DAILY_LOSS","5000"))
        self.risk_per_trade=float(os.environ.get("MARKETOS_RISK_PER_TRADE","1000"))
        self.max_trades=int(os.environ.get("MARKETOS_MAX_TRADES_DAY","8"))
        self.trades_today=0; self.realized_pnl=0.0; self.cooldown_until=0.0
        self.lot_nifty=int(os.environ.get("MARKETOS_LOT_NIFTY","65"))
        self.lot_bnf=int(os.environ.get("MARKETOS_LOT_BANKNIFTY","35"))
        self.slippage=float(os.environ.get("MARKETOS_ASSUMED_SLIPPAGE_POINTS","0.8"))
        self.cost_per_unit=float(os.environ.get("MARKETOS_COST_PER_UNIT","0.0"))
    def _lot(self): return self.lot_bnf if S.get("sym")=="BANKNIFTY" else self.lot_nifty
    def update(self,spot,side,trigger,profile):
        if not spot or side not in ("LONG","SHORT"):
            self.last={"ready":False,"reason":"NO_SIDE"}; return self.last
        bm=BOOK_MS.get(S.get("sym","NIFTY")); moves=list(bm.hist.get("micro_move",[]))[-120:] if bm else []
        abs_moves=[abs(x) for x in moves if x]
        med=sorted(abs_moves)[len(abs_moves)//2] if abs_moves else 1.0
        stop=max(3.0,med*6.0,self.slippage*3.0)
        entry=float(trigger.get("entry") or spot)
        p=(profile or {})
        direction=1 if side=="LONG" else -1

        # PRIMARY TARGET SOURCE: persistent observed liquidity map.
        # The search envelope is 500 points for NIFTY / 1000 for BANKNIFTY,
        # but only zones actually observed in the TBT stream can qualify.
        min_dist=stop*1.05
        # Use the profile's directional target selection first; it prefers ask
        # liquidity for longs and bid liquidity for shorts. Fall back to the raw
        # directional map only if no preferred target survives the stop envelope.
        map_rows=list(p.get("target_up" if direction>0 else "target_down",[]) or [])
        map_rows=[x for x in map_rows
                  if x.get("price") is not None and float(x.get("distance",0) or 0)>=min_dist]
        if not map_rows:
            map_rows=list(p.get("up_map" if direction>0 else "down_map",[]) or [])
            map_rows=[x for x in map_rows
                      if x.get("price") is not None and float(x.get("distance",0) or 0)>=min_dist]
            map_rows.sort(key=lambda x:(float(x.get("distance",999999)), -float(x.get("target_score",0))))

        target_source="NONE"
        target_meta={}
        if map_rows:
            # Prefer the nearest sufficiently persistent mapped liquidity. If a
            # materially stronger directional-side zone is close behind it, keep
            # that as a secondary reference for the UI but do not skip the next
            # observed liquidity automatically.
            primary=map_rows[0]
            target=float(primary["price"])
            target_source="SESSION_MAP" if primary.get("source")=="SESSION" else "PRIOR_SESSION_MAP"
            target_meta={"price":round(target,1),"density":primary.get("density"),
                         "relevant_density":primary.get("relevant_density"),
                         "role":primary.get("role"),"source":primary.get("source"),
                         "distance":primary.get("distance"),
                         "observations":primary.get("observations",0),
                         "target_score":primary.get("target_score",0)}
        else:
            # Secondary context only: rails can provide a target when the
            # persistent map has not yet observed a qualifying zone. This is
            # explicitly labelled so the dashboard never presents it as mapped
            # liquidity.
            rails=[SESSION.pdh,SESSION.pdl,SESSION.pdc,SESSION.t_high,SESSION.t_low]
            rails=[float(x) for x in rails if x is not None]
            if direction>0:
                rr=[x for x in rails if x>entry+min_dist]
                target=min(rr) if rr else entry+stop*1.5
            else:
                rr=[x for x in rails if x<entry-min_dist]
                target=max(rr) if rr else entry-stop*1.5
            target_source="RAIL_CONTEXT" if rr else "RISK_FALLBACK"
            target_meta={"source":target_source}

        if side=="LONG":
            stop_px=entry-stop
            if target<=entry+min_dist:
                target=entry+stop*1.5; target_source="RISK_FALLBACK"; target_meta={"source":target_source}
        else:
            stop_px=entry+stop
            if target>=entry-min_dist:
                target=entry-stop*1.5; target_source="RISK_FALLBACK"; target_meta={"source":target_source}

        rr=abs(target-entry)/max(0.01,stop)
        round_trip_cost=self.cost_per_unit*2+self.slippage
        net_target=max(0.0,abs(target-entry)-round_trip_cost)
        net_stop=stop+round_trip_cost
        net_rr=net_target/max(0.01,net_stop)
        lot=self._lot(); per_lot_risk=net_stop*lot
        lots=int(max(0,min(10,self.risk_per_trade/max(0.01,per_lot_risk))))
        blocked=(self.realized_pnl<=-self.max_daily_loss or self.trades_today>=self.max_trades or time.time()<self.cooldown_until)
        ready=bool(net_rr>=1.25 and not blocked)
        self.last={"ready":ready,"entry":round(entry,2),"stop":round(stop_px,2),"target":round(target,2),
                   "stop_points":round(stop,2),"rr":round(rr,2),"net_rr":round(net_rr,2),
                   "target_source":target_source,"target_meta":target_meta,
                   "map_target":target_meta if "MAP" in target_source else None,
                   "lot_size":lot,"lots":lots,"quantity":lots*lot,"risk_per_trade":round(self.risk_per_trade,2),
                   "estimated_risk":round(per_lot_risk*lots,2),"slippage_points":self.slippage,
                   "daily_pnl":round(self.realized_pnl,2),"trades_today":self.trades_today,
                   "map_up_coverage":round(float(p.get("coverage_up_points",0) or 0),1),
                   "map_down_coverage":round(float(p.get("coverage_down_points",0) or 0),1),
                   "map_limit":round(float(p.get("map_max_distance_points",1000 if S.get("sym")=="BANKNIFTY" else 500)),1),
                   "daily_lock":blocked,"reason":"OK" if ready else ("DAILY_LOCK" if blocked else "POOR_RR")}
        return self.last
    def register_trade(self,pnl):
        self.realized_pnl+=float(pnl); self.trades_today+=1
        self.cooldown_until=time.time()+float(os.environ.get("MARKETOS_TRADE_COOLDOWN","30"))
RISK=RiskEngineV11()


class LiquidityProfileV11(LiquidityProfile):
    """Persistent observed-liquidity map built from the moving 50-level window.

    The live TBT book is a *local* view. Every observed level is accumulated into
    a 5-point price zone as price travels.  The resulting session map can therefore
    cover much more than the currently visible 50 levels, but it never claims that
    unobserved prices contain liquidity.

    Semantics:
      CURRENT BOOK      = visible right now
      SESSION MAP       = liquidity actually observed during this session
      PRIOR SESSION     = historical context only
      TARGET            = next sufficiently persistent mapped liquidity zone

    The 500-point NIFTY / 1000-point BANKNIFTY limits are search envelopes, not
    guarantees of coverage. Coverage is reported explicitly in the dashboard.
    """
    SCHEMA = 2
    ZONE = 5.0

    def __init__(self, symbol, min_ticks=8):
        self.symbol=symbol
        self.file="marketos_liquidity_{}_history.json".format(symbol)
        super().__init__(min_ticks=min_ticks)
        self.session_day=str(date.today())
        self.prior={}
        self._load_history()
        self._replay_density_cache={}
        self._replay_pct_cache={}

    @property
    def max_distance(self):
        return 1000.0 if self.symbol == "BANKNIFTY" else 500.0

    def _load_history(self):
        try:
            if os.path.exists(self.file):
                d=json.load(open(self.file,encoding="utf-8"))
                self.prior=d.get("prior",{}) or {}
                # Backward compatible with the v11 single-prior format.
                if isinstance(self.prior,list):
                    merged={}
                    for day in self.prior:
                        for k,v in (day.get("zones",{}) or {}).items():
                            if k not in merged or float(v.get("d",0)) > float(merged[k].get("d",0)):
                                merged[k]=v
                    self.prior=merged
        except Exception:
            self.prior={}

    def _compact_current(self):
        out={}
        for k,v in self.zones.items():
            d=self.density(k)
            if d<=0:
                continue
            t=max(1,int(v.get("t",0)))
            out[str(k)]={
                "d":round(d,2),
                "t":t,
                "bs":int(v.get("bs",0)),
                "as":int(v.get("as",0)),
                "bp":int(v.get("bp",0)),
                "ap":int(v.get("ap",0)),
                "orders":int(v.get("orders",0)),
                "first":v.get("first"),"last":v.get("last"),
                "active_s":round(float(v.get("active_s",0) or 0),2)
            }
        return out

    def save_history(self):
        try:
            payload={"schema":self.SCHEMA,"symbol":self.symbol,"day":self.session_day,
                     "prior":self.prior,"current":self._compact_current()}
            with open(self.file,"w",encoding="utf-8") as f:
                json.dump(payload,f,separators=(",",":"))
        except Exception:
            pass

    def roll_day(self):
        today=str(date.today())
        if today==self.session_day:
            return
        if self.zones:
            # Keep the strongest observed zones from the completed session as
            # context. They are never promoted to current support/resistance.
            self.prior={str(k):{
                "d":round(self.density(k),2),
                "t":int(v.get("t",0)),
                "bs":int(v.get("bs",0)),
                "as":int(v.get("as",0)),
                "bp":int(v.get("bp",0)),
                "ap":int(v.get("ap",0)),
                "first":v.get("first"),"last":v.get("last"),
                "active_s":round(float(v.get("active_s",0) or 0),2)
            } for k,v in self.zones.items() if self.density(k)>0}
        self.zones={}
        self.session_day=today
        self.save_history()

    def update(self,bids,asks,now):
        self._replay_density_cache.clear(); self._replay_pct_cache.clear()
        self.roll_day()
        now=float(now)
        # Track observed presence in seconds, not raw callback count. Fyers can
        # deliver many callbacks per second; counting ticks would make every
        # frequently sampled zone look "persistent" within seconds.
        touched=set()
        for l in list(bids)+list(asks):
            try: touched.add(self._zk(l["price"]))
            except Exception: pass
        prev_last={k:float(v.get("last",now) or now) for k,v in self.zones.items() if k in touched}
        super().update(bids,asks,now)
        for k in touched:
            z=self.zones.get(k)
            if not z: continue
            z.setdefault("first",now)
            z.setdefault("active_s",0.0)
            dt=max(0.0,now-prev_last.get(k,now))
            # Cap a single gap so a zone is not credited with persistence while
            # price was away from it. Continuous re-observation earns time.
            z["active_s"] += min(dt,2.0)
            z["last"]=now

    def _depth_percentile(self, zk, side=None):
        cache_key=side or "BOTH"
        mp=self._replay_pct_cache.get(cache_key)
        if mp is None:
            vals=[]
            for k,v in self.zones.items():
                if int(v.get("t",0) or 0) < self.min_ticks:
                    continue
                t=max(1.0,float(v.get("t",0) or 0))
                q=float(v.get("bs" if side=="BID" else "as",0) if side else (v.get("bs",0)+v.get("as",0)))
                if q>0: vals.append((k,q/t))
            vals.sort(key=lambda x:x[1])
            mp={}
            if len(vals)==1:
                mp[vals[0][0]]=100.0
            elif vals:
                den=float(len(vals)-1)
                for i,(k,_) in enumerate(vals): mp[k]=100.0*i/den
            self._replay_pct_cache[cache_key]=mp
        return float(mp.get(zk,0.0))

    def density(self, zk):
        if zk in self._replay_density_cache:
            return self._replay_density_cache[zk]
        z=self.zones.get(zk)
        if not z or int(z.get("t",0) or 0) < self.min_ticks:
            self._replay_density_cache[zk]=0.0
            return 0.0
        time_w=min(1.0,float(z.get("active_s",0.0) or 0.0)/60.0)
        depth_pct=self._depth_percentile(zk)/100.0
        d=min(100.0,(0.45*time_w+0.55*depth_pct)*100.0)
        self._replay_density_cache[zk]=d
        return d

    def _side_density_from_record(self, v, side, zk=None):
        t=max(1,float(v.get("t",0) or 0))
        q=float(v.get("bs" if side=="BID" else "as",0) or 0)
        if t<=0 or q<=0:
            return 0.0
        # Side-specific density uses the side's depth percentile plus persistence.
        # For prior-session rows the stored density is the only reliable total
        # context, so fall back to that when active_s is unavailable.
        time_w=min(1.0,float(v.get("active_s",0) or 0)/60.0)
        if zk is not None and float(zk) in self.zones:
            depth_w=self._depth_percentile(float(zk),side)/100.0
        else:
            depth_w=min(1.0,float(v.get("d",0) or 0)/100.0)
        return min(100.0,(0.45*time_w+0.55*depth_w)*100.0)

    def _records(self, include_prior=True):
        rows=[]
        for k,v in self.zones.items():
            d=self.density(k)
            if d<=0:
                continue
            rows.append((float(k),dict(v),d,"SESSION"))
        if include_prior:
            for k,v in self.prior.items():
                try: price=float(k); d=float(v.get("d",0) or 0)
                except Exception: continue
                if d<=0: continue
                rows.append((price,dict(v),d,"PRIOR_SESSION"))
        # Same price can be present in current + prior; current session wins.
        merged={}
        for price,v,d,source in rows:
            key=round(price,1)
            if key not in merged or (source=="SESSION" and merged[key][3]!="SESSION") or d>merged[key][2]:
                merged[key]=(price,v,d,source)
        return list(merged.values())

    def map_points(self,spot,direction,max_distance=None,min_density=25,include_prior=True,limit=24):
        if not spot:
            return []
        max_distance=float(max_distance or self.max_distance)
        out=[]
        for price,v,d,source in self._records(include_prior=include_prior):
            dist=price-float(spot)
            if not ((direction>0 and dist>0) or (direction<0 and dist<0)):
                continue
            if abs(dist)>max_distance or d<min_density:
                continue
            bid_d=self._side_density_from_record(v,"BID",price)
            ask_d=self._side_density_from_record(v,"ASK",price)
            relevant=ask_d if direction>0 else bid_d
            other=bid_d if direction>0 else ask_d
            role=("ASK_LIQUIDITY" if ask_d>=bid_d*1.15 else
                  "BID_LIQUIDITY" if bid_d>=ask_d*1.15 else "TWO_SIDED")
            source_weight=1.0 if source=="SESSION" else 0.65
            out.append({
                "price":round(price,1),"distance":round(abs(dist),1),
                "density":round(d,1),"relevant_density":round(relevant,1),
                "other_density":round(other,1),"bid_density":round(bid_d,1),
                "ask_density":round(ask_d,1),"role":role,"source":source,
                "observations":int(v.get("t",0) or 0),
                "peak_qty":int(max(v.get("bp",0) or 0,v.get("ap",0) or 0)),
                "target_score":round(min(100.0,0.55*relevant+0.30*d+15.0*source_weight),1)
            })
        out.sort(key=lambda x:(x["distance"],-x["target_score"]))
        return out[:limit]

    def target_candidates(self,spot,direction,min_distance=0.0,min_density=25):
        """Return the next mapped liquidity targets in price order.

        The first acceptable target is the nearest observed zone beyond the stop
        envelope.  A stronger mapped zone is retained as the secondary target.
        """
        rows=self.map_points(spot,direction,self.max_distance,min_density,True,limit=24)
        rows=[x for x in rows if x["distance"]>=float(min_distance)]
        if not rows:
            return []
        # For a long, the most useful mapped destination is normally an observed
        # ask-liquidity node overhead; for a short it is a bid-liquidity node below.
        # Two-sided nodes are acceptable. Pure opposite-side nodes remain context
        # rather than becoming the primary target when a relevant-side node exists.
        preferred_role="ASK_LIQUIDITY" if direction>0 else "BID_LIQUIDITY"
        preferred=[x for x in rows if x.get("role") in (preferred_role,"TWO_SIDED") and x["relevant_density"]>=35]
        primary=preferred[0] if preferred else rows[0]
        secondary=next((x for x in rows if x["price"]!=primary["price"] and x["distance"]>primary["distance"]),None)
        return [x for x in (primary,secondary) if x]

    def to_dict(self,spot=None):
        if REPLAY_MODE:
            ranked=self._ranked()[:3]
            top=[{"price":k,"d":round(self.density(k))} for k,_ in ranked]
            up_all=self.map_points(spot,1,self.max_distance,25,True,limit=24) if spot else []
            dn_all=self.map_points(spot,-1,self.max_distance,25,True,limit=24) if spot else []
            def _targets(rows,direction):
                preferred_role="ASK_LIQUIDITY" if direction>0 else "BID_LIQUIDITY"
                preferred=[x for x in rows if x.get("role") in (preferred_role,"TWO_SIDED") and float(x.get("relevant_density",x.get("density",0)) or 0)>=35]
                primary=preferred[0] if preferred else (rows[0] if rows else None)
                secondary=next((x for x in rows if primary and x["price"]!=primary["price"] and x["distance"]>primary["distance"]),None)
                return [x for x in (primary,secondary) if x]
            return {
                "poc":self.poc(),"va_lo":None,"va_hi":None,"hvn":[],"lvn":[],"top":top,
                "map_max_distance_points":self.max_distance,"map_zone_points":self.ZONE,"map_min_density":25,
                "up_map":up_all,"down_map":dn_all,
                "coverage_up_points":round(max([x["distance"] for x in up_all],default=0),1),
                "coverage_down_points":round(max([x["distance"] for x in dn_all],default=0),1),
                "target_up":_targets(up_all,1),"target_down":_targets(dn_all,-1),
                "observed_session_zones":sum(1 for k in self.zones if self.density(k)>=25),
                "session_map_low":None,"session_map_high":None,"session_map_span_points":0}
        d=super().to_dict(spot)
        d["map_max_distance_points"]=self.max_distance; d["map_zone_points"]=self.ZONE; d["map_min_density"]=25
        if spot:
            up_all=self.map_points(spot,1,self.max_distance,25,True,limit=10000); dn_all=self.map_points(spot,-1,self.max_distance,25,True,limit=10000)
            d["up_map"]=up_all[:24]; d["down_map"]=dn_all[:24]
            d["coverage_up_points"]=round(max([x["distance"] for x in up_all],default=0),1); d["coverage_down_points"]=round(max([x["distance"] for x in dn_all],default=0),1)
            d["target_up"]=self.target_candidates(spot,1,0,25); d["target_down"]=self.target_candidates(spot,-1,0,25)
            d["observed_session_zones"]=sum(1 for k in self.zones if self.density(k)>=25)
            eligible=[float(k) for k in self.zones if self.density(k)>=25]
            d["session_map_low"]=min(eligible) if eligible else None; d["session_map_high"]=max(eligible) if eligible else None
            d["session_map_span_points"]=round((max(eligible)-min(eligible)) if eligible else 0,1)
        else:
            d["up_map"]=[]; d["down_map"]=[]; d["target_up"]=[]; d["target_down"]=[]
            d["coverage_up_points"]=0; d["coverage_down_points"]=0; d["observed_session_zones"]=0
            d["session_map_low"]=None; d["session_map_high"]=None; d["session_map_span_points"]=0
        return d

PROFILE={"NIFTY":LiquidityProfileV11("NIFTY"),"BANKNIFTY":LiquidityProfileV11("BANKNIFTY")}


# ============================================================
# ============================================================
# LIQUIDITY PATH / INTERACTION ENGINE v1.4
# ============================================================
# Purpose:
#   Turn the persistent observed-liquidity map into a stateful, replayable path.
#   The engine is deliberately price-agnostic: it discovers the active node from
#   the map at runtime and never hard-codes a historical level.
#
# Lifecycle:
#   WATCH -> BUILDING -> CLEARING -> ACCEPTED -> EXTENDING -> EXHAUSTING
#                                              |                     |
#                                              +--> REJECTING <------+
#
# A path is built from observable book evidence only:
#   persistence / density, repeated tests, displayed depletion, reload/recovery,
#   migration, microprice alignment, top-50 imbalance, price acceptance and
#   adverse response.
#
# IMPORTANT:
#   displayed quantity falling is NOT execution;
#   displayed quantity returning is NOT automatically an iceberg;
#   this engine never manufactures CVD from TBQ/TSQ.
#
class LiquidityPathEngine:
    """Stateful interaction/path engine for persistent mapped liquidity."""
    ZONE=5.0
    MAX_HISTORY=240
    MAX_EVENTS=240
    TEST_WINDOW=12.5
    MIN_DENSITY=25.0
    # v12.3.2: distinguish a fresh interaction regime from long-lived node memory.
    REGIME_GAP_S=8.0
    FRESH_WINDOW_S=6.0
    REGIME_SHIFT_THRESHOLD=0.22
    FRESH_MIN_SCORE=0.55
    # v12.3.3: a regime may emit at most one structural clearance. A new
    # clearance requires a genuine re-arm after sustained separation from the node.
    REGIME_REARM_GAP_S=8.0
    REGIME_REARM_AWAY_MULT=2.5
    REGIME_MIN_HOLD_S=2.0

    def __init__(self,symbol):
        self.symbol=symbol
        self.active={"LONG":None,"SHORT":None}
        self.qty_hist={}
        self.spot_hist=deque(maxlen=self.MAX_HISTORY)
        self.events=deque(maxlen=self.MAX_EVENTS)
        self.last_emit={}
        self.last={"LONG":{"phase":"IDLE"},"SHORT":{"phase":"IDLE"}}
        # v12.3.1: completed/rejected liquidity nodes are retired temporarily so
        # stale interaction history cannot contaminate a new regime.
        self.retired_nodes={"LONG":{}, "SHORT":{}}
        self._now=0.0

    def _direction_role(self,direction):
        return "ASK_LIQUIDITY" if direction>0 else "BID_LIQUIDITY"

    def _prune_retired(self, key, now):
        book=self.retired_nodes.setdefault(key,{})
        for price, expiry in list(book.items()):
            if now >= float(expiry):
                book.pop(price, None)

    def _retire_node(self, key, price, now, ttl=30.0):
        self.retired_nodes.setdefault(key,{})[round(float(price),1)] = float(now) + float(ttl)

    def _decay_candidate(self, c, key, now, dist, band):
        """Regime decay and clean lifecycle reset.

        Old reload/defence observations are evidence about an old interaction
        regime, not permanent properties of a price. When price leaves the node
        for a sustained interval, interaction counts decay and completed/rejected
        nodes are retired temporarily.
        """
        far = abs(float(dist)) > max(self.ZONE, band*2.5)
        if far:
            c["away_s"] = float(c.get("away_s",0.0)) + max(0.0, now-float(c.get("decay_ts",now)))
        else:
            c["away_s"] = 0.0
        c["decay_ts"] = now

        if far and c.get("away_s",0.0) >= 5.0:
            last=float(c.get("last_decay",0.0))
            if now-last >= 5.0:
                # Half-life style decay: recent interactions matter more than
                # stale observations. Keep the raw counters for auditability.
                for fld in ("tests","disappearance_count","reappearance_count",
                            "reload_count","defense_count","clearance_count",
                            "cross_count","adverse_count","exhaustion_count",
                            "reversal_count"):
                    c[fld]=int(round(float(c.get(fld,0))*0.5))
                c["reload"]=float(c.get("reload",0.0))*0.5
                c["pressure"]=float(c.get("pressure",0.0))*0.5
                c["last_decay"]=now

        # Once a node has failed/exhausted and price has moved away, do not
        # recycle it as the active entry node. Let the map choose the next node.
        if far and c.get("phase") in ("REJECTING","EXHAUSTING") and c.get("away_s",0.0)>=5.0:
            self._retire_node(key,c["target"],now,ttl=30.0)
            return True
        return False

    def _target_rows(self,profile,spot,direction):
        p=profile or {}
        key="LONG" if direction>0 else "SHORT"
        self._prune_retired(key, self._now)
        rows=list(p.get("target_up" if direction>0 else "target_down",[]) or [])
        retired=set(self.retired_nodes.get(key,{}).keys())
        rows=[x for x in rows if round(float(x.get("price",0) or 0),1) not in retired]
        rows=[x for x in rows if x.get("price") is not None and float(x.get("distance",0) or 0)>0]
        role=self._direction_role(direction)
        preferred=[x for x in rows if x.get("role") in (role,"TWO_SIDED")]
        preferred=[x for x in preferred if float(x.get("relevant_density",x.get("density",0)) or 0)>=self.MIN_DENSITY]
        rows=preferred or rows
        if not rows:
            rows=list(p.get("up_map" if direction>0 else "down_map",[]) or [])
            rows=[x for x in rows if x.get("price") is not None]
        rows.sort(key=lambda x:(float(x.get("distance",1e9) or 1e9),-float(x.get("target_score",0) or 0)))
        return rows[:6]

    def _target(self,profile,spot,direction):
        rows=self._target_rows(profile,spot,direction)
        return rows[0] if rows else None

    @staticmethod
    def _side_qty(bids,asks,target,direction,half=2.5):
        side=asks if direction>0 else bids
        return sum(float(x.get("qty",0) or 0) for x in side
                   if abs(float(x.get("price",0))-target)<=half)

    @staticmethod
    def _nearby_qty(bids,asks,target,direction,band=12.5):
        side=asks if direction>0 else bids
        return sum(float(x.get("qty",0) or 0) for x in side
                   if 2.5 < abs(float(x.get("price",0))-target)<=band)

    @staticmethod
    def _opposing_qty(bids,asks,spot,direction,band=12.5):
        # For a LONG path, nearby bid liquidity is supportive and nearby ask is
        # opposing only after price has moved through the current node. For SHORT
        # the inverse applies. This is deliberately local, not a support/resistance
        # classifier.
        side=bids if direction>0 else asks
        return sum(float(x.get("qty",0) or 0) for x in side
                   if abs(float(x.get("price",0))-spot)<=band)

    @staticmethod
    def _micro_alignment(book_ms,direction):
        bm=book_ms or {}; mid=float(bm.get("mid") or 0); micro=float(bm.get("microprice") or 0)
        spread=max(0.01,float(bm.get("spread") or 0.01))
        if not mid or not micro: return 0.0
        # Normalize by half-spread, then cap. Positive = directionally aligned.
        raw=((micro-mid)/(spread/2.0))*direction
        return max(-2.0,min(2.0,raw))

    @staticmethod
    def _norm(v,lo=0.0,hi=1.0):
        return max(lo,min(hi,float(v)))

    def _adaptive_band(self,book_ms):
        bm=book_ms or {}
        spread=max(0.5,float(bm.get("spread") or 0.5))
        mm=abs(float(bm.get("mid_move") or 0))
        return max(self.ZONE/2.0,min(12.5,max(spread*2.0,mm*4.0)))

    def _emit(self,event,direction,target,spot,now,evidence):
        key=(event,direction,round(float(target),1))
        if now-self.last_emit.get(key,-1e9)<3.0: return None
        self.last_emit[key]=now
        row={"event":event,"direction":direction,"side":"LONG" if direction>0 else "SHORT",
             "target":round(float(target),1),"spot":round(float(spot),2),"ts":float(now),
             "evidence":dict(evidence or {})}
        self.events.append(row)
        return row

    def _update_interaction_regime(self, c, now, qty, baseline, dist, band, book_ms, direction):
        """v12.3.2: segment repeated interaction into fresh regimes.

        Persistent-map history remains intact for auditability, but decision
        evidence is calculated primarily from the current interaction regime.
        A regime changes when the node has been away long enough or its recent
        quantity/depletion/reload/micro structure changes materially.
        """
        prev_qty=float(c.get("regime_prev_qty",0.0) or 0.0)
        prev_dep=float(c.get("regime_prev_dep",0.0) or 0.0)
        prev_reload=float(c.get("regime_prev_reload",0.0) or 0.0)
        prev_micro=float(c.get("regime_prev_micro",0.0) or 0.0)
        last_i=float(c.get("last_interaction_ts",now) or now)
        dep=1.0-float(qty)/max(1.0,float(baseline))
        micro=self._micro_alignment(book_ms,direction)
        reload_ratio=float(c.get("reload",0.0) or 0.0)
        away=float(c.get("away_s",0.0) or 0.0)
        gap=(now-last_i) if c.get("regime_seen") else 0.0
        shift=False
        if c.get("regime_seen"):
            # A regime is not a tick-to-tick label. Preserve the v12.3.2
            # segmentation ideas, but require either a true temporal gap or a
            # compound structural change sustained beyond a short hold period.
            hold_ok=(now-float(c.get("regime_born",now) or now))>=self.REGIME_MIN_HOLD_S
            changes=0
            changes += int(abs(dep-prev_dep)>=self.REGIME_SHIFT_THRESHOLD)
            changes += int(abs(micro-prev_micro)>=0.55)
            changes += int(prev_qty>0 and abs(qty-prev_qty)/max(prev_qty,1.0)>=0.45)
            changes += int(abs(reload_ratio-prev_reload)>=0.45)
            if gap>=self.REGIME_GAP_S or (hold_ok and changes>=2) or (away>=3.0 and hold_ok):
                shift=True

        if shift:
            c["regime_id"]=int(c.get("regime_id",0))+1
            c["regime_born"]=now
            c["regime_tests"]=0
            c["regime_disappearances"]=0
            c["regime_reloads"]=0
            c["regime_defense"]=0
            c["regime_adverse"]=0
            c["regime_clearance"]=0
            c["regime_crosses"]=0
            c["regime_peak_fav"]=0.0
            c["regime_fresh_score"]=1.0
            # A regime shift alone is NOT a re-arm. The old regime stays
            # committed until the candidate has genuinely separated from the
            # node. This prevents repeated CLEARING events at the same price.
            if c.get("regime_closed") and away>=self.REGIME_REARM_AWAY_MULT*band and c.get("away_s",0.0)>=self.REGIME_REARM_GAP_S:
                c["regime_committed"]=False
                c["regime_clearance_emitted"]=False
                c["regime_closed"]=False
                c["clearance"]=False
                c["entry_ready"]=False
                c["cross_count"]=0
                c["entry_state"]="NOT_READY"
                c["rearm_count"]=int(c.get("rearm_count",0))+1
                c["last_rearm_ts"]=now
                c["phase"]="WATCH"
                c["last_transition"]="REGIME_REARM"
            elif not c.get("regime_committed") and not c.get("accepted"):
                c["clearance"]=False
                c["entry_ready"]=False
                c["cross_count"]=0
                c["entry_state"]="NOT_READY"
                if c.get("phase") in ("CLEARING","REJECTING","WATCH","BUILDING"):
                    c["phase"]="BUILDING"
            else:
                c["last_transition"]="REGIME_SHIFT_COMMITTED"
        else:
            age=max(0.0,now-float(c.get("regime_born",now) or now))
            fresh=max(0.0,1.0-age/max(1.0,self.FRESH_WINDOW_S))
            if gap>0:
                fresh=max(fresh,1.0-min(1.0,gap/self.REGIME_GAP_S))
            c["regime_fresh_score"]=max(float(c.get("regime_fresh_score",0.0) or 0.0)*0.92,fresh)

        c["regime_seen"]=True
        c["regime_prev_qty"]=float(qty)
        c["regime_prev_dep"]=float(dep)
        c["regime_prev_reload"]=float(reload_ratio)
        c["regime_prev_micro"]=float(micro)
        c["regime_last_ts"]=now
        c["regime_distance"]=abs(float(dist))
        return shift

    def _new_candidate(self,target,direction,now):
        return {
            "target":float(target.get("price")),"direction":direction,
            "source":target.get("source"),
            "density":float(target.get("relevant_density",target.get("density",0)) or 0),
            "role":target.get("role"),"observations":int(target.get("observations",0) or 0),
            "born":now,"phase":"WATCH","tests":0,
            "clearance":False,"accepted":False,"rejected":False,"continued":False,
            "entry_ready":False,"exit_ready":False,
            "peak_fav":0.0,"peak_adv":0.0,"baseline_qty":0.0,"current_qty":0.0,
            "depletion":0.0,"reload":0.0,"micro":0.0,"migration":0.0,
            "disappearance_count":0,"reappearance_count":0,"reload_count":0,
            "defense_count":0,"clearance_count":0,"cross_count":0,"adverse_count":0,
            "exhaustion_count":0,"reversal_count":0,
            "low_seen":False,"last_qty":0.0,"last_spot":None,"last_ts":now,
            "last_transition":"WATCH","class":"WATCH",
            "qty_velocity":0.0,"reload_decay":0.0,"pressure":0.0,
            "next_target":None,"next_target_distance":None,"next_density":0.0,
            "last_event":None,
            "away_s":0.0,"decay_ts":now,"last_decay":now,
            "last_interaction_ts":now,"entry_state":"NOT_READY",
            "build_state":"WATCH","risk_state":"NORMAL",
            # v12.3.2 regime-local evidence; lifetime counters remain above.
            "regime_id":0,"regime_born":now,"regime_seen":False,"regime_last_ts":now,
            "regime_tests":0,"regime_disappearances":0,"regime_reloads":0,
            "regime_defense":0,"regime_adverse":0,"regime_clearance":0,"regime_crosses":0,
            "regime_peak_fav":0.0,"regime_fresh_score":1.0,
            "regime_prev_qty":0.0,"regime_prev_dep":0.0,"regime_prev_reload":0.0,"regime_prev_micro":0.0,
            "regime_distance":0.0,
            # v12.3.3 lifecycle controls
            "regime_committed":False,"regime_clearance_emitted":False,
            "regime_closed":False,"rearm_count":0,"last_rearm_ts":0.0,
        }

    def _state_metrics(self,c,dist,band,book_ms,direction):
        bm=book_ms or {}
        imb=float(bm.get("imbalance") or 0.0)
        micro=float(c.get("micro",0.0) or 0.0)
        dep=self._norm(c.get("depletion",0.0))
        rel=self._norm(c.get("reload",0.0)/1.5)
        mig=self._norm(c.get("migration",0.0))
        age=max(0.0,float(c.get("last_ts",0))-float(c.get("born",0)))
        tests=float(c.get("regime_tests",c.get("tests",0)) or 0)
        dis=float(c.get("regime_disappearances",c.get("disappearance_count",0)) or 0)
        reloads=float(c.get("regime_reloads",c.get("reload_count",0)) or 0)
        defense=float(c.get("regime_defense",c.get("defense_count",0)) or 0)
        adverse=float(c.get("regime_adverse",c.get("adverse_count",0)) or 0)
        prox=0.0 if dist<=0 else self._norm(1.0-dist/max(0.5,band*2.5))
        imb_align=self._norm(imb*direction,0,1)
        micro_align=self._norm((micro+1.0)/2.0,0,1)
        persistence=self._norm(age/30.0)
        repeat=self._norm((dis+reloads)/5.0)
        test_score=self._norm(tests/5.0)
        pressure=self._norm(c.get("pressure",0.0))
        # Early BUILD score: proximity + repeated interaction + directional pressure
        # can build the alert BEFORE clearance. Depletion is important but not the
        # sole gate, preventing the engine from waking only after the move begins.
        build=100*(0.15*prox + 0.18*dep + 0.16*micro_align + 0.13*imb_align +
                   0.10*persistence + 0.10*repeat + 0.08*test_score + 0.10*pressure)
        reload_risk=self._norm(rel)
        defense_risk=self._norm(defense/4.0)
        adverse_risk=self._norm(adverse/4.0)
        against=self._norm(-micro/1.0)
        migration_risk=self._norm(mig)
        # A strong directional build lowers failure risk; repeated reload/defense
        # and adverse price response raise it.
        fail=100*(0.25*reload_risk + 0.23*defense_risk + 0.22*adverse_risk +
                  0.20*against + 0.10*migration_risk)
        if c.get("accepted"):
            fail=max(fail,100*self._norm(0.7*adverse_risk+0.3*against))
        if c.get("rejected"):
            fail=max(fail,85.0)

        exhaustion=self._norm(
            0.30*self._norm(c.get("peak_fav",0)/(max(band,self.ZONE))) +
            0.20*against + 0.20*adverse_risk + 0.15*reload_risk +
            0.15*self._norm(c.get("reversal_count",0)/3.0)
        ) if c.get("accepted") else 0.0

        # v12.3.1: BUILDING and ENTRY are separate states. A setup may be
        # structurally building while still explicitly NOT entry-ready.
        if c.get("rejected"):
            setup="FAILING"
            entry_state="INVALIDATED"
        elif c.get("exit_ready") or exhaustion>=0.70:
            setup="EXHAUSTING"
            entry_state="EXIT_WATCH"
        elif c.get("entry_ready") or c.get("accepted"):
            setup="READY"
            entry_state="READY"
        elif build>=48 and (dep>=0.20 or pressure>=0.45 or tests>=2 or dis>=1):
            setup="BUILDING"
            entry_state="NOT_READY"
        elif prox>=0.25:
            setup="WATCH"
            entry_state="NOT_READY"
        else:
            setup="IDLE"
            entry_state="NOT_READY"

        if setup=="BUILDING":
            risk_state="HIGH" if fail>=70 else ("ELEVATED" if fail>=50 else "NORMAL")
        else:
            risk_state="HIGH" if fail>=70 else ("ELEVATED" if fail>=50 else "NORMAL")
        c["entry_state"]=entry_state
        c["build_state"]=setup
        c["risk_state"]=risk_state
        return {
            "build_score":round(self._norm(build,0,100),1),
            "failure_risk":round(self._norm(fail,0,100),1),
            "exhaustion_score":round(self._norm(exhaustion)*100,1),
            "setup_state":setup,
            "build_state":setup,
            "entry_state":entry_state,
            "risk_state":risk_state,
            "entry_ready":bool(c.get("entry_ready",False)),
            "micro_alignment":round(micro,2),
            "imbalance_alignment":round(imb_align,2),
            "proximity":round(prox,2),"age_s":round(age,1),
        }

    def _update_direction(self,direction,profile,bids,asks,spot,book_ms,now):
        key="LONG" if direction>0 else "SHORT"
        target_rows=self._target_rows(profile,spot,direction)
        target=target_rows[0] if target_rows else None
        c=self.active.get(key)
        band=self._adaptive_band(book_ms)
        self._now=now
        if c is not None:
            if self._decay_candidate(c,key,now,(c["target"]-spot)*direction,band):
                c=None
                self.active[key]=None
        if c is None:
            if target is None:
                self.last[key]={"phase":"IDLE","direction":key,"target":None,"entry_ready":False}; return
            c=self._new_candidate(target,direction,now); self.active[key]=c
        else:
            # Retire a completed node only when price has clearly moved beyond it;
            # otherwise keep the same node so reload/rejection/exhaustion history is
            # not lost during the critical interaction window.
            far_beyond=(direction>0 and spot>c["target"]+max(self.ZONE,band*3.0)) or (direction<0 and spot<c["target"]-max(self.ZONE,band*3.0))
            if far_beyond and target is not None and abs(float(target.get("price"))-c["target"])>self.ZONE:
                c=self._new_candidate(target,direction,now); self.active[key]=c
            elif target is not None and c.get("phase") in ("WATCH","BUILDING") and abs(float(target.get("price"))-c["target"])>self.ZONE*2:
                c=self._new_candidate(target,direction,now); self.active[key]=c

        if c is None:
            self.last[key]={"phase":"IDLE","direction":key,"target":None,"entry_ready":False}; return

        t=c["target"]; dist=(t-spot)*direction
        qty=self._side_qty(bids,asks,t,direction)
        nearby=self._nearby_qty(bids,asks,t,direction)
        qkey=(key,round(t,1)); hist=self.qty_hist.setdefault(qkey,deque(maxlen=80)); hist.append(qty)
        vals=sorted(hist); med=vals[len(vals)//2] if vals else qty
        baseline=max(1.0,med,c.get("baseline_qty",0.0)) if len(hist)>=8 else max(1.0,qty)
        c["baseline_qty"]=baseline; c["current_qty"]=qty
        c["depletion"]=self._norm(1.0-qty/max(1.0,baseline))
        self._update_interaction_regime(c, now, qty, baseline, (t-spot)*direction, band, book_ms, direction)
        c["micro"]=self._micro_alignment(book_ms,direction)
        c["migration"]=self._norm(nearby/max(1.0,baseline*c["depletion"])) if c["depletion"]>0.05 else 0.0

        # Quantity velocity and pressure build are intentionally independent of
        # execution inference. They measure how quickly visible liquidity changes.
        if len(hist)>=3:
            recent=list(hist)[-3:]
            dt=max(0.01,now-c.get("last_ts",now))
            c["qty_velocity"]=(recent[-1]-recent[0])/max(0.01,dt)
        prev_qty=c.get("last_qty",0.0)
        if prev_qty>0:
            drop_ratio=max(0.0,(prev_qty-qty)/prev_qty)
            rise_ratio=max(0.0,(qty-prev_qty)/prev_qty)
        else:
            drop_ratio=rise_ratio=0.0

        if prev_qty>0 and qty<prev_qty*0.55 and prev_qty>=baseline*0.65:
            c["low_seen"]=True; c["disappearance_count"]+=1; c["regime_disappearances"]+=1; c["last_interaction_ts"]=now
            c["last_transition"]="DEPLETING"; c["phase"]="BUILDING"
            ev=self._emit("LIQUIDITY_DEPLETION",direction,t,spot,now,{
                "before_qty":round(prev_qty,0),"current_qty":round(qty,0),
                "depletion":round(c["depletion"],3),"baseline_qty":round(baseline,0),
                "migration":round(c["migration"],3),"density":round(c["density"],1)})
            if ev: c["last_event"]=ev

        if c.get("low_seen") and qty>=max(baseline*0.75,prev_qty*1.45):
            c["reappearance_count"]+=1; c["reload_count"]+=1; c["regime_reloads"]+=1; c["low_seen"]=False; c["last_interaction_ts"]=now
            c["reload"]=qty/max(1.0,baseline); c["last_transition"]="RELOADED"
            ev=self._emit("LIQUIDITY_RELOAD",direction,t,spot,now,{
                "current_qty":round(qty,0),"baseline_qty":round(baseline,0),
                "reload_ratio":round(c["reload"],2),"reappearance_count":c["reappearance_count"],
                "migration":round(c["migration"],3),"density":round(c["density"],1)})
            if ev: c["last_event"]=ev

        # Reload decay: repeated reloads which become progressively weaker support
        # a genuine clearing path; stable/strong reloads support the failure case.
        if c["reload_count"]:
            c["reload_decay"]=self._norm(1.0-c["reload"]/max(1.0,1.5))
        else:
            c["reload_decay"]=0.0

        # Price progress and test count.
        if c["last_spot"] is not None:
            fav=(spot-c["last_spot"])*direction
            c["peak_fav"]=max(c["peak_fav"],fav)
            c["peak_adv"]=min(c["peak_adv"],fav)
        c["last_spot"]=spot; c["last_ts"]=now; c["last_qty"]=qty

        if dist>band:
            c["phase"]="WATCH" if c["phase"] not in ("ACCEPTED","EXTENDING","EXHAUSTING","REJECTING") else c["phase"]
        elif abs(dist)<=band:
            c["tests"]+=1; c["regime_tests"]+=1
            c["last_interaction_ts"]=now
            c["away_s"]=0.0
            if c["phase"] in ("WATCH","IDLE"): c["phase"]="BUILDING"

            # Pressure is the early-warning component. It rises from repeated tests,
            # depletion, directional microprice, imbalance and weak reloads.
            imb=float((book_ms or {}).get("imbalance") or 0.0)
            directional_imb=self._norm(imb*direction)
            test_pressure=self._norm(c["tests"]/5.0)
            c["pressure"]=(0.35*self._norm(c["depletion"])+0.25*self._norm((c["micro"]+1)/2)+
                            0.20*directional_imb+0.10*test_pressure+0.10*c["reload_decay"])

            # Clearance is only declared when the visible node has materially weakened.
            # Migration is retained as an explanatory qualifier, not silently treated as
            # cancellation/consumption.
            if len(hist)>=8 and c["depletion"]>=0.55 and (c["disappearance_count"]>=1 or c["tests"]>=3):
                if not c["clearance"] and not c.get("regime_clearance_emitted",False):
                    c["clearance_count"]+=1
                    c["regime_clearance_emitted"]=True
                    c["regime_committed"]=True
                    ev=self._emit("LIQUIDITY_CLEARANCE",direction,t,spot,now,{
                        "depletion":round(c["depletion"],3),"baseline_qty":round(baseline,0),
                        "current_qty":round(qty,0),"density":round(c["density"],1),
                        "distance":round(abs(dist),2),"micro_alignment":round(c["micro"],2),
                        "migration":round(c["migration"],3),"disappearance_count":c["disappearance_count"]})
                    if ev: c["last_event"]=ev
                c["clearance"]=True; c["phase"]="CLEARING"; c["last_transition"]="CLEARING"
                c["class"]="MIGRATION" if c["migration"]>=0.70 else "CLEARANCE"

            crossed=(spot>=t+band*0.25) if direction>0 else (spot<=t-band*0.25)
            if c["reload_count"]>0 and not crossed:
                # Defense requires reload plus failure to establish beyond the node.
                c["defense_count"]+=1; c["regime_defense"]+=1; c["class"]="DEFENSE/RELOAD"; c["last_transition"]="DEFENDING"
                if c["defense_count"] in (1,3,5):
                    ev=self._emit("LIQUIDITY_DEFENSE",direction,t,spot,now,{
                        "reload_count":c["reload_count"],"defense_count":c["defense_count"],
                        "reload_ratio":round(c["reload"],2),"micro_alignment":round(c["micro"],2),
                        "distance":round(abs(dist),2),"migration":round(c["migration"],3)})
                    if ev: c["last_event"]=ev

            # Acceptance requires repeated crossings plus directional microprice.
            if c["clearance"] and crossed and c["micro"]>-0.75:
                c["cross_count"]+=1; c["regime_crosses"]+=1
                if c["cross_count"]>=2:
                    if not c["accepted"]:
                        c["accepted"]=True; c["entry_ready"]=True; c["phase"]="ACCEPTED"
                        c["class"]="ACCEPTANCE"; c["last_transition"]="ACCEPTED"
                        ev=self._emit("LIQUIDITY_ACCEPTANCE",direction,t,spot,now,{
                            "cross_count":c["cross_count"],"depletion":round(c["depletion"],3),
                            "micro_alignment":round(c["micro"],2),"density":round(c["density"],1),
                            "disappearance_count":c["disappearance_count"],"reappearance_count":c["reappearance_count"]})
                        if ev: c["last_event"]=ev
                    else:
                        c["phase"]="ACCEPTED"

        # Continuation and next-node discovery are map-driven. This is what turns a
        # single break into a liquidity path rather than a one-off wall signal.
        rows=self._target_rows(profile,spot,direction)
        next_rows=[r for r in rows if float(r.get("price",0) or 0)!=round(t,1)]
        if direction>0:
            next_rows=[r for r in next_rows if float(r.get("price",0))>t]
        else:
            next_rows=[r for r in next_rows if float(r.get("price",0))<t]
        next_rows.sort(key=lambda x:float(x.get("distance",1e9) or 1e9))
        if next_rows:
            nr=next_rows[0]
            c["next_target"]=round(float(nr.get("price")),1)
            c["next_target_distance"]=round(abs(float(nr.get("price"))-spot),1)
            c["next_density"]=round(float(nr.get("relevant_density",nr.get("density",0)) or 0),1)
        else:
            c["next_target"]=None; c["next_target_distance"]=None; c["next_density"]=0.0

        if c["accepted"]:
            beyond=(spot>=t+band) if direction>0 else (spot<=t-band)
            back=(spot<t-band*0.25) if direction>0 else (spot>t+band*0.25)
            if beyond:
                c["phase"]="EXTENDING"; c["class"]="CONTINUATION"
                if not c["continued"] and c["peak_fav"]>=max(band,self.ZONE/2):
                    c["continued"]=True
                    ev=self._emit("TARGET_CONTINUATION",direction,t,spot,now,{
                        "extension":round(c["peak_fav"],2),"next_target":c.get("next_target"),
                        "next_density":c.get("next_density"),"reload_ratio":round(c["reload"],2),
                        "micro_alignment":round(c["micro"],2),"density":round(c["density"],1)})
                    if ev: c["last_event"]=ev

            # Exhaustion is different from outright rejection. It is an early exit
            # warning when the move has travelled, directional microprice deteriorates,
            # opposing liquidity returns, or the node response starts reversing.
            micro_weak=c["micro"]<0.0
            reload_bad=c["reload_count"]>=2 and c["reload"]>=0.75
            adverse_bad=c["adverse_count"]>=1
            if beyond and (micro_weak or reload_bad or adverse_bad):
                c["exhaustion_count"]+=1
            else:
                c["exhaustion_count"]=max(0,c["exhaustion_count"]-1)

            if c["exhaustion_count"]>=2:
                c["exit_ready"]=True
                c["phase"]="EXHAUSTING"
                c["regime_closed"]=True
                c["class"]="EXHAUSTION"
                c["last_transition"]="EXHAUSTING"
                ev=self._emit("PATH_EXHAUSTION",direction,t,spot,now,{
                    "peak_fav":round(c["peak_fav"],2),"micro_alignment":round(c["micro"],2),
                    "reload_ratio":round(c["reload"],2),"reload_count":c["reload_count"],
                    "adverse_count":c["adverse_count"],"next_target":c.get("next_target")})
                if ev: c["last_event"]=ev

            if back:
                c["adverse_count"]+=1; c["regime_adverse"]+=1
                if c["adverse_count"]>=3 or (c["reload"]>=0.8 and c["micro"]<-0.5):
                    if not c["rejected"]:
                        c["rejected"]=True; c["entry_ready"]=False; c["exit_ready"]=True
                        c["entry_state"]="INVALIDATED"
                        c["phase"]="REJECTING"; c["regime_closed"]=True; c["class"]="REJECTION"; c["last_transition"]="REJECTING"
                        c["reversal_count"]+=1
                        ev=self._emit("TARGET_REJECTION",direction,t,spot,now,{
                            "reload_ratio":round(c["reload"],2),"micro_alignment":round(c["micro"],2),
                            "adverse_count":c["adverse_count"],"max_fav":round(c["peak_fav"],2),
                            "density":round(c["density"],1)})
                        if ev: c["last_event"]=ev

        c.update(self._state_metrics(c,max(0.0,dist),band,book_ms,direction))
        out=dict(c)
        out["age_s"]=round(max(0.0,now-c["born"]),1)
        out["target_distance"]=round(abs(dist),1)
        out["target_role"]=c.get("role"); out["source"]=c.get("source")
        out["entry_state"]=c.get("entry_state","NOT_READY")
        out["build_state"]=c.get("build_state",out.get("setup_state","WATCH"))
        out["risk_state"]=c.get("risk_state","NORMAL")
        out["retired_nodes"]=len(self.retired_nodes.get(key,{}))
        out["interaction_regime"]=int(c.get("regime_id",0))
        out["regime_freshness"]=round(float(c.get("regime_fresh_score",0.0) or 0.0),2)
        out["regime_committed"]=bool(c.get("regime_committed",False))
        out["regime_clearance_emitted"]=bool(c.get("regime_clearance_emitted",False))
        out["regime_closed"]=bool(c.get("regime_closed",False))
        out["rearm_count"]=int(c.get("rearm_count",0) or 0)
        out["regime_tests"]=int(c.get("regime_tests",0) or 0)
        out["regime_reloads"]=int(c.get("regime_reloads",0) or 0)
        out["regime_defense"]=int(c.get("regime_defense",0) or 0)
        out["event_count"]=sum(1 for e in self.events if e["direction"]==key)
        self.last[key]=out

    def update(self,profile,bids,asks,spot,book_ms,now):
        if not spot: return self.last
        self._update_direction(1,profile,bids,asks,spot,book_ms,now)
        self._update_direction(-1,profile,bids,asks,spot,book_ms,now)
        out={k:dict(v) for k,v in self.last.items()}
        out["events"]=list(self.events)[-16:]
        return out

PATH_ENGINE={"NIFTY":LiquidityPathEngine("NIFTY"),"BANKNIFTY":LiquidityPathEngine("BANKNIFTY")}

# Correct the outcome/edge globals after the v10 definitions.
OUTCOMES=OutcomeTrackerV11()
try:
    _loaded_edge_rows=OUTCOMES.load_persisted()
    if _loaded_edge_rows:
        print("[EDGE DATA] loaded {} persisted outcome rows".format(_loaded_edge_rows))
except Exception as _le:
    _loaded_edge_rows=0
try: OUTCOME_STORE._seen=len(OUTCOMES.completed)
except Exception: pass
EDGE_GATE=EmpiricalEdgeGateV11()

class DecisionEngine:
    """Final state machine: structural trigger -> safety -> provisional/validated.

    Empirical edge is a modifier and learning signal, not a prerequisite for
    collecting data. This allows daily operation from day one while clearly
    separating provisional setups from statistically validated ones.
    """
    def __init__(self):
        self.require_edge=os.environ.get("MARKETOS_REQUIRE_EMPIRICAL_EDGE","0").lower() in ("1","true","yes","on")
        self.allow_provisional=os.environ.get("MARKETOS_ALLOW_PROVISIONAL","1").lower() in ("1","true","yes","on")
        self.max_stale=float(os.environ.get("MARKETOS_MAX_STALE_GAP","2.0"))
        self.max_toxicity=float(os.environ.get("MARKETOS_MAX_TRADE_TOXICITY","65"))
        self.max_spread_n=float(os.environ.get("MARKETOS_MAX_SPREAD_NIFTY","6.0"))
        self.max_spread_b=float(os.environ.get("MARKETOS_MAX_SPREAD_BANKNIFTY","12.0"))
        self._last={}
    def update(self,spot,trig,risk,empirical,intel,tox,book_ms,dataq,rails,profile,now):
        side=trig.get("side","NONE"); structural=trig.get("go") in ("GO","PROVISIONAL")
        reasons=list(trig.get("reason",[])); blocks=[]
        if side not in ("LONG","SHORT") or not structural: state="NO-GO"
        else:
            if not dataq.get("depth_complete_50",False): blocks.append("incomplete 50L book")
            if dataq.get("stale_gap",False) or float(dataq.get("inter_update_gap_s",0))>self.max_stale: blocks.append("stale feed gap")
            spread=float((book_ms or {}).get("spread") or 0)
            max_sp=self.max_spread_b if S.get("sym")=="BANKNIFTY" else self.max_spread_n
            if spread>max_sp: blocks.append("spread {:.1f} > {:.1f}".format(spread,max_sp))
            if float(tox.get("stress",0))>self.max_toxicity: blocks.append("toxicity {:.0f}".format(float(tox.get("stress",0))))
            if not risk.get("ready",False): blocks.append(risk.get("reason","risk not ready"))
            status=empirical.get("status","INSUFFICIENT_SAMPLE")
            supported=bool(empirical.get("supported"))
            if supported: state="VALIDATED-LONG" if side=="LONG" else "VALIDATED-SHORT"
            else:
                if self.require_edge or not self.allow_provisional: blocks.append("empirical edge not yet demonstrated")
                state="PROVISIONAL-LONG" if side=="LONG" else "PROVISIONAL-SHORT"
            if blocks: state="NO-GO"
        # Map target / rails are context, not fabricated certainty.
        target=risk.get("target") if isinstance(risk,dict) else None
        self._last={"state":state,"side":side if state!="NO-GO" else "NONE",
                    "entry":risk.get("entry",trig.get("entry",spot)),"stop":risk.get("stop"),
                    "target":target,"rr":risk.get("rr"),"net_rr":risk.get("net_rr"),
                    "quantity":risk.get("quantity",0),"lots":risk.get("lots",0),
                    "empirical_status":empirical.get("status","RESEARCH_ONLY"),
                    "empirical":empirical.get("stats",{}),"event":empirical.get("event",""),
                    "blocks":blocks,"reasons":reasons,
                    "mode":"LIVE-DECISION" if os.environ.get("MARKETOS_EXECUTION_MODE","PAPER").upper()=="LIVE" else "PAPER-DECISION",
                    "trade_enabled":state!="NO-GO","timestamp":now}
        return dict(self._last)
DECISION=DecisionEngine()

# ---- old update_level_memory/detect_sweep/detect_vacuum moved inline ----
# ---- kept for reference only ----
def _raw_level_to_dict(level):
    """Decode MarketLevel from the supplied Fyers protobuf schema."""
    def val(x, default=0):
        try:
            return x.value if x is not None else default
        except Exception:
            return default
    return {
        "price": float(val(level.price, 0)) / 100.0,
        "qty": int(val(level.qty, 0)),
        "orders": int(val(level.nord, 0)),
        "level": int(val(level.num, 0)),
    }


def _raw_quote_to_dict(feed):
    """Extract Quote + transport metadata from MarketFeed protobuf."""
    q = getattr(feed, "quote", None)
    out = {}
    if q is not None:
        for name in ("ltp", "ltt", "ltq", "vtt", "vtt_diff"):
            try:
                obj = getattr(q, name)
                if obj is not None and getattr(obj, "value", None) is not None:
                    out[name] = obj.value
            except Exception:
                pass
    for name in ("sequence_no", "feed_time", "send_time"):
        try:
            obj = getattr(feed, name)
            if name == "sequence_no":
                out[name] = int(obj)
            elif obj is not None and getattr(obj, "value", None) is not None:
                out[name] = obj.value
        except Exception:
            pass
    if "ltp" in out:
        out["ltp"] = float(out["ltp"]) / 100.0
    return out


def _raw_apply_book(ticker, depth, snapshot):
    """Maintain a deterministic 50-level book from MarketFeed depth updates."""
    book = RAW_BOOKS.setdefault(ticker, {
        "bids": {i: {"price":0.0,"qty":0,"orders":0,"level":i} for i in range(50)},
        "asks": {i: {"price":0.0,"qty":0,"orders":0,"level":i} for i in range(50)},
        "initialized": False,
    })
    if snapshot or not book["initialized"]:
        for i in range(50):
            book["bids"][i] = {"price":0.0,"qty":0,"orders":0,"level":i}
            book["asks"][i] = {"price":0.0,"qty":0,"orders":0,"level":i}
    for src, dst in ((getattr(depth, "bids", []), book["bids"]),
                     (getattr(depth, "asks", []), book["asks"])):
        for lvl in src:
            x = _raw_level_to_dict(lvl)
            i = x["level"]
            if not (0 <= i < 50):
                continue
            old = dst[i]
            if x["price"] == 0.0 and x["qty"] > 0 and old["price"] > 0:
                x["price"] = old["price"]
            elif x["price"] == 0.0 and x["qty"] == 0 and old["price"] > 0:
                x["price"] = old["price"]
            dst[i] = x
    book["initialized"] = True
    bids = [x.copy() for x in book["bids"].values() if x["price"] > 0 and x["qty"] > 0]
    asks = [x.copy() for x in book["asks"].values() if x["price"] > 0 and x["qty"] > 0]
    bids.sort(key=lambda x:x["price"], reverse=True)
    asks.sort(key=lambda x:x["price"])
    for i,x in enumerate(bids): x["level"] = i
    for i,x in enumerate(asks): x["level"] = i
    return bids[:50], asks[:50]


async def _raw_tbt_loop(access_token):
    """Direct protobuf TBT path.

    This is an OPTIONAL enhancement for Quote + Depth capture.  A valid REST
    token can still receive HTTP 403 from the raw TBT WebSocket edge/entitlement
    layer.  In that case the caller must fall back to FyersTbtSocket.
    """
    import websockets
    try:
        import msg_pb2
    except ImportError as e:
        raise RuntimeError("msg_pb2.py is required for raw TBT decoding") from e

    auth_header = "{}:{}".format(CLIENT_ID, access_token)
    sub = {"type":1,"data":{"subs":1,"symbols":[S["sym_str"]],"mode":"depth","channel":"1"}}
    resume = {"type":2,"data":{"resumeChannels":["1"],"pauseChannels":[]}}

    print("[RAW TBT] Connecting {}".format(TBT_URL))
    handshake_variants = [
        {"Authorization": auth_header},
        {"Authorization": auth_header, "User-Agent": "Mozilla/5.0", "Origin": "https://trade.fyers.in"},
    ]
    ws = None
    ws_ctx = None
    last_error = None
    try:
        for headers in handshake_variants:
            try:
                try:
                    ws_ctx = websockets.connect(TBT_URL, additional_headers=headers, open_timeout=10)
                except TypeError:
                    ws_ctx = websockets.connect(TBT_URL, extra_headers=headers, open_timeout=10)
                ws = await ws_ctx.__aenter__()
                print("[RAW TBT] WebSocket handshake accepted")
                break
            except Exception as e:
                last_error = e
                print("[RAW TBT HANDSHAKE ERR] {}".format(e))
                try:
                    if ws_ctx is not None:
                        await ws_ctx.__aexit__(type(e), e, e.__traceback__)
                except Exception:
                    pass
                ws = None
                ws_ctx = None

        if ws is None:
            raise RuntimeError("HTTP/WebSocket handshake rejected: {}".format(last_error))

        await ws.send(json.dumps(sub))
        await ws.send(json.dumps(resume))
        print("[RAW TBT] Connected; subscribed depth channel 1")
        with S_LOCK:
            S["feed"] = "RAW-TBT/50L"
            S["live"] = True

        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if isinstance(msg, str):
                if msg.lower() == "ping":
                    await ws.send("pong")
                continue

            sm = msg_pb2.SocketMessage()
            sm.ParseFromString(msg)
            if sm.error:
                print("[RAW TBT] feed error: {}".format(sm.msg))
                continue

            for ticker, feed in sm.feeds.items():
                depth = feed.depth
                tbq = int(depth.tbq.value) if depth.tbq else 0
                tsq = int(depth.tsq.value) if depth.tsq else 0
                snap = bool(sm.snapshot or getattr(feed, "snapshot", False))
                bids, asks = _raw_apply_book(ticker, depth, snap)
                trade = _raw_quote_to_dict(feed)
                meta = {
                    "feed_time": trade.get("feed_time"),
                    "send_time": trade.get("send_time"),
                    "sequence_no": trade.get("sequence_no"),
                    "snapshot": snap,
                }
                with S_LOCK:
                    S["quote_fields_seen"] = sorted(set(S.get("quote_fields_seen", [])) | set(trade.keys()))
                    if meta["feed_time"] is not None: S["last_feed_ts"] = meta["feed_time"]
                    if meta["sequence_no"] is not None: S["last_sequence"] = meta["sequence_no"]
                    if trade.get("ltp") is not None: S["quote_available"] = True
                if not (bids and asks):
                    continue
                spot = trade.get("ltp") or ((bids[0]["price"] + asks[0]["price"]) / 2.0)
                push_update(
                    bids, asks, round(float(spot),2),
                    "RAW-TBT/{}L".format(max(len(bids),len(asks))),
                    max(len(bids),len(asks)), tbq=tbq, tsq=tsq,
                    trade=trade, feed_meta=meta
                )
    finally:
        try:
            if ws_ctx is not None:
                await ws_ctx.__aexit__(None, None, None)
        except Exception:
            pass
        with S_LOCK:
            S["live"] = False


def _start_raw_tbt(access_token):
    """Start optional raw protobuf path and automatically fall back to SDK TBT."""
    def runner():
        try:
            asyncio.run(_raw_tbt_loop(access_token))
        except Exception as e:
            import traceback
            print("[RAW TBT ERR] {}".format(e))
            print(traceback.format_exc()[:1000])
            try:
                from fyers_apiv3.FyersWebsocket.tbt_ws import FyersTbtSocket, SubscriptionModes
                print("[TBT FALLBACK] Starting official FyersTbtSocket (50-level)")
                _start_fyers_tbt(access_token, FyersTbtSocket, SubscriptionModes)
            except Exception as fb:
                print("[TBT FALLBACK ERR] {}".format(fb))
                print("[TBT FALLBACK] Starting FyersDataSocket (5-level)")
                try:
                    start_data_socket(access_token)
                except Exception as ds:
                    print("[DATA SOCKET ERR] {}".format(ds))
    th = threading.Thread(target=runner, name="fyers-raw-tbt", daemon=True)
    th.start()
    return th


def start_tbt(access_token):
    """Start supported Fyers 50-level TBT; raw protobuf is diagnostic opt-in.

    Live testing established that the direct raw endpoint currently returns
    HTTP 403. Re-attempting it on every production start adds no information.
    Set MARKETOS_RAW_TBT=1 only when deliberately testing that transport.
    """
    if RAW_TBT_ENABLED:
        try:
            import msg_pb2  # noqa: F401
            import websockets  # noqa: F401
            print("[TBT] RAW_TBT diagnostic mode enabled; attempting Quote + Depth capture")
            _start_raw_tbt(access_token)
            return
        except Exception as raw_exc:
            print("[TBT RAW] diagnostic path unavailable/rejected: {}".format(raw_exc))
    else:
        print("[TBT] Using supported FyersTbtSocket (50-level); raw protobuf diagnostic is OFF")
        print("[TBT] Quote probe remains enabled: ltp/ltt/ltq/vtt/vtt_diff are recorded if exposed by the SDK callback")

    try:
        from fyers_apiv3.FyersWebsocket.tbt_ws import FyersTbtSocket, SubscriptionModes
        print("[TBT] FyersTbtSocket found in fyers-apiv3")
        _start_fyers_tbt(access_token, FyersTbtSocket, SubscriptionModes)
        return
    except ImportError as ie:
        print("[TBT] FyersTbtSocket import failed: " + str(ie))
        print("[TBT] Your fyers-apiv3 may be outdated. Run: pip install --upgrade fyers-apiv3")
    except Exception as e:
        import traceback
        print("[TBT] FyersTbtSocket error: " + str(e))
        print(traceback.format_exc()[:400])

    # Fallback to FyersDataSocket (5-level)
    print("[TBT] Falling back to FyersDataSocket (5-level)...")
    start_data_socket(access_token)

def _start_fyers_tbt(access_token, FyersTbtSocket, SubscriptionModes):
    """
    FULLY CONFIRMED from source inspection:

    Callback receives: on_depth_update(ticker: str, message: Depth)

    Depth object fields (50 levels, price already /100):
        depth.bidprice[0..49]  list of 50 bid prices
        depth.askprice[0..49]  list of 50 ask prices
        depth.bidqty[0..49]    list of 50 bid quantities
        depth.askqty[0..49]    list of 50 ask quantities
        depth.bidordn[0..49]   list of 50 bid order counts
        depth.askordn[0..49]   list of 50 ask order counts
        depth.tbq              total buy qty (exchange reported)
        depth.tsq              total sell qty (exchange reported)
    """
    sym = S["sym_str"]

    def on_depth_update(ticker, depth):
        try:
            bids = []
            asks = []

            # Read all 50 levels from Depth object
            for i in range(50):
                bp = depth.bidprice[i]
                bq = depth.bidqty[i]
                bo = depth.bidordn[i]
                if bp > 0 and bq > 0:
                    bids.append({"price": round(float(bp), 2),
                                 "qty":   int(bq),
                                 "orders":int(bo), "level": i})

                ap = depth.askprice[i]
                aq = depth.askqty[i]
                ao = depth.askordn[i]
                if ap > 0 and aq > 0:
                    asks.append({"price": round(float(ap), 2),
                                 "qty":   int(aq),
                                 "orders":int(ao), "level": i})

            # Sort correctly
            bids.sort(key=lambda x: x["price"], reverse=True)
            asks.sort(key=lambda x: x["price"])

            # Exchange total qty: depth statistics, NOT trade flow.
            tbq = int(depth.tbq)
            tsq = int(depth.tsq)
            trade = extract_trade_fields(depth)
            feed_meta = {
                "feed_time": trade.get("feed_time"),
                "send_time": trade.get("send_time"),
                "sequence_no": trade.get("sequence_no"),
            }
            with S_LOCK:
                S["quote_fields_seen"] = sorted(set(S.get("quote_fields_seen", [])) | set(trade.keys()))
                if trade.get("feed_time") is not None: S["last_feed_ts"] = trade.get("feed_time")
                if trade.get("sequence_no") is not None: S["last_sequence"] = trade.get("sequence_no")
            if tbq > 0:
                with S_LOCK:
                    S["tot_buy_qty"]  = tbq
                    S["tot_sell_qty"] = tsq

            # Spot = midpoint of best bid/ask
            spot = 0.0
            if bids and asks:
                spot = round((bids[0]["price"] + asks[0]["price"]) / 2, 2)

            n = max(len(bids), len(asks))

            # Log first 3 ticks
            tc = S.get("tick_count", 0)
            if tc < 3:
                print("[TBT tick={}] sym={} bids={} asks={} spot={} tbq={} tsq={} quote_fields={} trade={}".format(
                    tc, ticker, len(bids), len(asks), spot, tbq, tsq,
                    sorted(trade.keys()), bool(trade.get("ltq"))))

            if bids or asks:
                push_update(bids, asks, spot, "TBT/{}L".format(n), n, tbq=tbq, tsq=tsq, trade=trade, feed_meta=feed_meta)

        except Exception as e:
            import traceback
            print("[TBT ERR] " + str(e))
            print(traceback.format_exc()[:300])

    def onerror(msg):
        print("[TBT ERR] " + str(msg))

    def onclose(msg):
        print("[TBT CLOSED] " + str(msg))
        with S_LOCK: S["live"] = False

    def onopen():
        print("[TBT OPEN] Connected. Subscribing: " + sym)
        fyers_tbt.subscribe(
            symbol_tickers=[sym],
            channelNo="1",
            mode=SubscriptionModes.DEPTH
        )
        fyers_tbt.switchChannel(resume_channels=["1"], pause_channels=[])
        with S_LOCK:
            S["feed"] = "TBT/50L"
            S["live"] = True
            S["last"] = datetime.now().strftime("%H:%M:%S")
        _, status = is_market_open()
        print("[TBT] Subscribed | " + status)

    # on_open overwrites on_connect in __init__ — must use on_open param
    global _tbt_socket, _tbt_modes
    _tbt_modes  = SubscriptionModes
    fyers_tbt = FyersTbtSocket(
        access_token    = access_token,
        write_to_file   = False,
        log_path        = None,
        on_open         = onopen,
        on_close        = onclose,
        on_error        = onerror,
        on_depth_update = on_depth_update
    )

    _tbt_socket = fyers_tbt  # store for toggle resubscription

    def run():
        try:
            print("[TBT] keep_running()...")
            fyers_tbt.keep_running()   # sets __ws_run=True before connect
            print("[TBT] connect()...")
            fyers_tbt.connect()        # starts WS thread, fires onopen after 2s
            print("[TBT] running — waiting for depth data...")
            while True:
                time.sleep(1)
        except Exception as e:
            import traceback
            print("[TBT FAILED] " + str(e))
            print(traceback.format_exc()[:300])
            start_data_socket(access_token)

    threading.Thread(target=run, daemon=True).start()


def start_data_socket(access_token):
    """Fallback: FyersDataSocket DepthUpdate — 5 levels"""
    try:
        from fyers_apiv3.FyersWebsocket import data_ws

        def onmessage(msg):
            parse_data_socket_msg(msg)

        def onerror(msg):
            print("[DS ERR] " + str(msg))
            with S_LOCK: S["err"] = str(msg)

        def onclose(msg):
            print("[DS CLOSED] " + str(msg))
            with S_LOCK: S["live"] = False

        def onopen():
            sym = S["sym_str"]
            print("[DS OPEN] Subscribing DepthUpdate + SymbolUpdate for " + sym)
            fys_ws.subscribe(symbols=[sym], data_type="DepthUpdate")
            fys_ws.subscribe(symbols=[sym], data_type="SymbolUpdate")
            with S_LOCK: S["feed"] = "DataSocket/5-level"
            fys_ws.keep_running()

        fys_ws = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="", litemode=False,
            write_to_file=False, reconnect=True,
            on_connect=onopen, on_close=onclose,
            on_error=onerror, on_message=onmessage)

        print("[DS] Starting FyersDataSocket...")
        threading.Thread(target=fys_ws.connect, daemon=True).start()

    except Exception as e:
        import traceback
        print("[DS FAILED] " + str(e))
        print(traceback.format_exc())
        start_rest_fallback(access_token)

def parse_data_socket_msg(message):
    """Parse FyersDataSocket confirmed message formats"""
    try:
        if not isinstance(message, dict): return
        msg_type = message.get("type","")
        if msg_type in ("cn","ful","sub","ping","error"): return
        bids=[]; asks=[]; spot=0.0; tb=0; ts=0
        trade = {k: message.get(k) for k in
                 ("ltp","ltt","ltq","vtt","vtt_diff","sequence_no","feed_time","send_time")
                 if message.get(k) is not None}
        if "bid_price1" in message:
            for i in range(1,6):
                bp=message.get("bid_price{}".format(i),0); bs=message.get("bid_size{}".format(i),0)
                ap=message.get("ask_price{}".format(i),0); as_=message.get("ask_size{}".format(i),0)
                bo=message.get("bid_order{}".format(i),1); ao=message.get("ask_order{}".format(i),1)
                if bp and bs: bids.append({"price":float(bp),"qty":int(bs),"orders":int(bo)})
                if ap and as_: asks.append({"price":float(ap),"qty":int(as_),"orders":int(ao)})
            if bids and asks: spot=round((bids[0]["price"]+asks[0]["price"])/2,2)
            if message.get("ltp"): spot=float(message["ltp"])
        elif "ltp" in message:
            spot=float(message.get("ltp",0))
            tb=int(message.get("tot_buy_qty",0)); ts=int(message.get("tot_sell_qty",0))
            bp=float(message.get("bid_price",0)); bs=int(message.get("bid_size",0))
            ap=float(message.get("ask_price",0)); as_=int(message.get("ask_size",0))
            if bp and bs: bids.append({"price":bp,"qty":bs,"orders":1})
            if ap and as_: asks.append({"price":ap,"qty":as_,"orders":1})
            if tb>0 and ts>0:
                with S_LOCK: S["tot_buy_qty"]=tb; S["tot_sell_qty"]=ts
        if not spot and not bids and not asks: return
        bids.sort(key=lambda x:x["price"],reverse=True)
        asks.sort(key=lambda x:x["price"])
        if bids or asks:
            if not spot and bids and asks: spot=round((bids[0]["price"]+asks[0]["price"])/2,2)
            push_update(bids,asks,spot,"DataSocket/5-level",5,tbq=tb,tsq=ts, trade=trade,
                         feed_meta={"sequence_no":trade.get("sequence_no"),"feed_time":trade.get("feed_time"),"send_time":trade.get("send_time")})
        elif spot>100:
            with S_LOCK:
                S["spot"]=spot; S["last"]=datetime.now().strftime("%H:%M:%S")
                S["live"]=True; S["tick_count"]=S.get("tick_count",0)+1
    except Exception as e:
        print("[DS PARSE ERR] "+str(e))

# ============================================================
# ENGINE 3 — REST polling (last resort)
# ============================================================
def is_market_open():
    """Check if NSE market is currently open (9:15 AM to 3:30 PM IST Mon-Fri)"""
    from datetime import datetime
    import pytz
    try:
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
    except:
        # pytz not installed — use UTC+5:30 offset
        from datetime import timezone, timedelta
        ist_offset = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist_offset)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False, "Weekend — market closed"
    t = now.hour * 60 + now.minute
    if t < 9*60+15:
        opens_in = (9*60+15 - t)
        return False, "Pre-market — opens in {}m".format(opens_in)
    if t > 15*60+30:
        return False, "Market closed for today — opens tomorrow 9:15 AM IST"
    return True, "Market OPEN"

def start_rest_fallback(access_token):
    try:
        from fyers_apiv3 import fyersModel
        client=fyersModel.FyersModel(client_id=CLIENT_ID,token=access_token,is_async=False,log_path="")

        def poll():
            while True:
                # Auto-roll symbol on expiry day after 3:30 PM
                new_nf  = get_active_sym("NIFTY")
                new_bnf = get_active_sym("BANKNIFTY")
                with S_LOCK:
                    if S["sym"]=="NIFTY" and S["sym_str"] != new_nf:
                        print("[ROLL] NIFTY contract rolled: {} -> {}".format(S["sym_str"], new_nf))
                        S["sym_str"] = new_nf
                        S["prev_bids"]={};S["prev_asks"]={}
                    if S["sym"]=="BANKNIFTY" and S["sym_str"] != new_bnf:
                        print("[ROLL] BANKNIFTY contract rolled: {} -> {}".format(S["sym_str"], new_bnf))
                        S["sym_str"] = new_bnf
                        S["prev_bids"]={};S["prev_asks"]={}

                open_, status = is_market_open()
                with S_LOCK:
                    S["market_status"] = status
                if not open_:
                    with S_LOCK:
                        S["feed"] = "REST | " + status
                        S["live"] = False
                    time.sleep(30)
                    continue
                try:
                    with S_LOCK: S["feed"]="REST/5L"
                    sym=S["sym_str"]; bnf=S["sym"]=="BANKNIFTY"
                    resp=client.depth(data={"symbol":sym,"ohlcv_flag":1})
                    d=resp.get("d",{}); rec=d.get(sym) or (next(iter(d.values())) if d else None)
                    if rec:
                        spot=float(rec.get("ltp") or 0); bids=[]; asks=[]
                        for b in rec.get("bids",[]):
                            p=float(b.get("price",0)); q=int(b.get("volume",0)); o=int(b.get("ord",1))
                            if p>0 and q>0: bids.append({"price":p,"qty":q,"orders":o})
                        for a in rec.get("ask",[]):
                            p=float(a.get("price",0)); q=int(a.get("volume",0)); o=int(a.get("ord",1))
                            if p>0 and q>0: asks.append({"price":p,"qty":q,"orders":o})
                        bids.sort(key=lambda x:x["price"],reverse=True)
                        asks.sort(key=lambda x:x["price"])
                        if bids or asks: push_update(bids,asks,spot,"REST/5L",5,tbq=tb,tsq=ts)
                except Exception as e: print("[REST ERR] "+str(e))
                time.sleep(1)

        threading.Thread(target=poll,daemon=True).start()
        print("[REST] Polling started.")
    except Exception as e: print("[REST FAILED] "+str(e))

# ============================================================
# SYMBOL SWITCH
# ============================================================
_ws_ref = None

# Global reference to TBT socket for resubscription on toggle
_tbt_socket = None
_tbt_modes  = None

def switch_symbol(new_sym):
    global _tbt_socket, _tbt_modes
    new_str = get_active_sym("NIFTY") if new_sym=="NIFTY" else get_active_sym("BANKNIFTY")
    old_str = S.get("sym_str","")
    with S_LOCK:
        S["sym"]      = new_sym
        S["sym_str"]  = new_str
        S["sess_delta"]=0; S["prev_bids"]={}; S["prev_asks"]={}
        S["cvd_session"]=0; S["proxy_cvd_session"]=0
        S["cvd_hist"].clear(); S["proxy_cvd_hist"].clear()
        S["trade_count"]=0; S["trade_total_qty"]=0; S["trade_classified_qty"]=0; S["trade_unclassified"]=0
        S["actual_buy_volume"]=0; S["actual_sell_volume"]=0
        S["last_trade_key"]=None; S["last_trade_price"]=None
        S["_last_ice"]=""; S["_last_ice_ts"]=0
    # Re-subscribe TBT to new symbol
    if _tbt_socket is not None and new_str != old_str:
        try:
            print("[TOGGLE] Switching TBT: {} -> {}".format(old_str, new_str))
            # unsubscribe needs mode parameter too
            _tbt_socket.unsubscribe(symbol_tickers=[old_str], channelNo="1", mode=_tbt_modes.DEPTH)
            _tbt_socket.subscribe(symbol_tickers=[new_str], channelNo="1", mode=_tbt_modes.DEPTH)
            _tbt_socket.switchChannel(resume_channels=["1"], pause_channels=[])
            print("[TOGGLE] Resubscribed to " + new_str)
        except Exception as e:
            print("[TOGGLE ERR] " + str(e))

# ============================================================
# HTML DASHBOARD
# ============================================================
def col(d):
    d=str(d)
    if "BULL" in d: return "bull"
    if "BEAR" in d: return "bear"
    return "neut"

def build_page():
    with S_LOCK:
        s=dict(S); s["alerts"]=list(S["alerts"]); s["sigs"]=list(S.get("sigs",[]))
        s["bids"]=list(S.get("bids",[])); s["asks"]=list(S.get("asks",[]))
        s["sup"]=list(S.get("sup",[])); s["res"]=list(S.get("res",[]))
    sym=s["sym"]; ssym=s["sym_str"]; spot="{:,.2f}".format(s["spot"]) if s.get("spot") else "--"
    nfc="on" if sym=="NIFTY" else "off"; bnfc="on" if sym=="BANKNIFTY" else "off"
    wq=WALL_BNF if sym=="BANKNIFTY" else WALL_NF
    feed=s.get("feed","--"); levels=s.get("depth_levels",0); ticks=s.get("tick_count",0)
    ldot='<span style="color:#1D9E75;font-size:10px;">&#9679; LIVE</span>' if s["live"] else '<span style="color:#EF9F27;font-size:10px;">&#9679; CONNECTING</span>'
    mkt_open, mkt_status = is_market_open()
    mkt_col = "bull" if mkt_open else "amber"
    feed_str = s.get("feed","--")
    tbt_connected = "connected" in feed_str.lower() or "50L" in feed_str
    if not mkt_open and tbt_connected:
        warn = '<div style="background:#0a0a14;border-bottom:1px solid #1D9E75;padding:8px 14px;color:#1D9E75;font-size:11px;">&#10003; TBT WebSocket authenticated &amp; connected | {} | Depth data streams when market opens Mon 9:15 AM IST</div>'.format(mkt_status)
    elif not mkt_open and not s["live"]:
        warn = '<div style="background:#0a0a14;border-bottom:1px solid #3c3489;padding:8px 14px;color:#3c3489;font-size:11px;">&#9679; {} &mdash; Run during market hours for live data</div>'.format(mkt_status)
    elif not s["live"]:
        warn = '<div style="background:#1a1000;border-bottom:1px solid #EF9F27;padding:6px 14px;color:#EF9F27;font-size:11px;">&#9888; Connecting WebSocket...</div>'
    else:
        warn = ""
    err='<div style="background:#1a0a0a;padding:6px 14px;color:#E24B4A;font-size:11px;">ERROR: {}</div>'.format(s["err"]) if s.get("err") else ""
    alert_state="none"
    if s.get("absorb",{}).get("active"): alert_state="bull" if s["absorb"]["side"]=="BULLISH" else "bear"
    elif s.get("iceberg",{}).get("detected"): alert_state="ibull" if s["iceberg"]["side"]=="BULLISH" else "ibear"
    elif s.get("dsig","")=="STRONG BULL": alert_state="sbull"
    elif s.get("dsig","")=="STRONG BEAR": alert_state="sbear"
    bh=""; ah=""
    for b in s.get("bids",[]):
        w=b["qty"]>=wq; cls="br bid-r"+(" wall-r" if w else ""); wt='<span class="tl">W</span>' if w else ""
        bh+='<div class="{}"><span class="c5">{}</span><span class="bull">{:,}{}</span><span class="w">{}</span></div>'.format(cls,b["orders"],b["qty"],wt,b["price"])
    for a in s.get("asks",[]):
        w=a["qty"]>=wq; cls="br ask-r"+(" wall-r" if w else ""); wt='<span class="tl">W</span>' if w else ""
        ah+='<div class="{}"><span class="w">{}</span><span class="bear">{:,}{}</span><span class="c5">{}</span></div>'.format(cls,a["price"],a["qty"],wt,a["orders"])
    dom=s.get("dom"); wd=""
    if dom:
        sc="bull" if dom["side"]=="BID" else "bear"
        wd='<div class="row"><span class="lbl">Wall@{}</span><span class="val {}">{:,} [{}]</span></div>'.format(dom["price"],sc,dom["qty"],dom["st"])
    def lzh(zones, cls):
        """Zone HTML: center price, cum qty, level count, age."""
        if not zones: return '<div class="row" style="font-size:10px;color:#333">None</div>'
        h = ""
        for z in zones:
            age = z.get("age", 0)
            age_s = "{}s".format(age) if age < 60 else "{}m{}s".format(age//60, age%60)
            ac = "bull" if age >= 120 else "amber" if age >= 30 else "neut"
            h += '<div class="row" style="font-size:10px;"><span class="val {c}">{rng}</span><span class="val {c}">{q:,}q</span><span class="val neut">{lvls}L</span><span class="val {ag}">{age}</span></div>'.format(
                c=cls, rng=z["range"], q=z["qty"], lvls=z["levels"], age=age_s, ag=ac)
        return h

    def lh(lvls,c):
        if not lvls: return '<div class="row"><span class="lbl neut">None</span></div>'
        h=""
        for l in lvls:
            age=l.get("age",0)
            age_s="{}s".format(age) if age<60 else "{}m{}s".format(age//60,age%60)
            ac="bull" if age>=120 else "amber" if age>=30 else "neut"
            vel=l.get("vel_sym","→")
            vc=l.get("vel_cls","neut")
            zone=l.get("zone",l.get("qty",0))
            h+='<div class="row"><span class="lbl {c}">{p}</span><span class="val {c}">{q:,}q [{s}]</span><span class="val {vc}" style="font-size:12px;padding-left:4px">{vs}</span><span class="val {ag}" style="font-size:9px;padding-left:4px">{a}</span><span class="val neut" style="font-size:9px;padding-left:6px">Z:{z:,}</span></div>'.format(c=c,p=l["price"],q=l["qty"],s=l["st"],vs=vel,vc=vc,ag=ac,a=age_s,z=zone)
        return h
    dcc=s.get("dc","neutral"); ddir=col(s.get("dsig","")); dst=s.get("dst",0)
    strc="#1D9E75" if "BULL" in s.get("dsig","") else "#E24B4A" if "BEAR" in s.get("dsig","") else "#555"
    sgh="".join('<div class="si">{}</div>'.format(sg) for sg in s.get("sigs",[]))
    alh="".join('<div class="ai">{}</div>'.format(a) for a in s.get("alerts",[])[:20]) or '<div style="color:#333;font-size:10px;">No alerts</div>'
    bs=s.get("bull",0); rs=s.get("bear",0); bb=int(bs/(bs+rs)*100) if (bs+rs)>0 else 50
    conc=s.get("conc",50); cc="bull" if conc>60 else "amber" if conc>40 else "bear"; cl="TIGHT" if conc>60 else "NORMAL" if conc>40 else "LOOSE"
    dr=s.get("dr",1.0); dv=s.get("delta",0); sd=s.get("sess_delta",0)
    vb="{:,.2f}".format(s["vb"]) if s.get("vb") else "--"; va="{:,.2f}".format(s["va"]) if s.get("va") else "--"
    dt=s.get("delta_trend","NEUTRAL"); bp=s.get("bp",50); ap=s.get("ap",50); tb=s.get("tb",0); ta=s.get("ta",0)
    _prof=s.get("profile",{}) or {}
    _map_limit=float(_prof.get("map_max_distance_points",1000 if sym=="BANKNIFTY" else 500) or 0)
    _upcov=float(_prof.get("coverage_up_points",0) or 0); _dncov=float(_prof.get("coverage_down_points",0) or 0)
    def _mini_map(arr,col):
        if not arr: return '<div class="row"><span class="lbl neut">No observed mapped liquidity</span></div>'
        return "".join('<div class="row" style="font-size:10px;"><span class="val" style="color:{c};">{p:.0f}</span><span class="val neut">+{d:.0f}pt</span><span class="val amber">D{den:.0f}</span><span class="val neut">{src}</span></div>'.format(c=col,p=float(x.get("price",0)),d=float(x.get("distance",0)),den=float(x.get("relevant_density",x.get("density",0)) or 0),src="LIVE" if x.get("source")=="SESSION" else "PRIOR") for x in arr[:4])
    _map_up_html=_mini_map(_prof.get("up_map",[]) or [],"#1D9E75")
    _map_dn_html=_mini_map(_prof.get("down_map",[]) or [],"#E24B4A")
    _flow_real=(FLOW_QUALITY=="REAL")
    flow_quality_label="REAL TRADE FLOW" if _flow_real else "UNAVAILABLE (NO TRADE TAPE)"
    delta_display="{:+,}".format(dv) if _flow_real else "--"
    sess_delta_display="{:+,}".format(sd) if _flow_real else "--"
    drc="bull" if dr>1.2 else "bear" if dr<0.8 else "neut"
    ic=col(s["dirn"]); nc=col(s["nsig"])
    # Active institutional candidates (behaviour over time, NOT snapshot booleans)
    _abs_a = s.get("inst_abs", [])
    _ice_a = s.get("inst_ice", [])
    _atop = _abs_a[0] if _abs_a else None
    _itop = _ice_a[0] if _ice_a else None
    def _panel(cands):
        if not cands:
            return '<div class="row" style="font-size:10px;color:#333">None active</div>'
        h=""
        for c in cands:
            sc = "bull" if c["side"]=="BID" else "bear"
            stc = "amber" if c["state"] in ("CONFIRMED","DOMINANT") else "neut"
            # detector-appropriate metric: sweep->levels, vacuum->collapse, else hidden
            if c.get("lvl"):  mk, mv = "L", int(c["lvl"])
            elif c.get("col"): mk, mv = "C", int(c["col"])
            else:              mk, mv = "H", int(c.get("hidden", 0))
            h += '<div class="row"><span class="val {sc}">{s}@{p:.1f}</span><span class="val {stc}">{st}</span><span class="val neut">[{cl}]</span><span class="val {sc}">{cf:.0f}%</span><span class="val neut" style="font-size:9px">{mk}:{mv}</span></div>'.format(sc=sc,s=c["side"],p=c["price"],st=c["state"],stc=stc,cl=pcls(c["class"]),cf=c["conf"],mk=mk,mv=mv)
        return h
    abs_panel = _panel(_abs_a); ice_panel = _panel(_ice_a)
    atc = "bull" if _atop and _atop["side"]=="BID" else "bear" if _atop else "neut"
    itc = "bull" if _itop and _itop["side"]=="BID" else "bear" if _itop else "neut"
    absig = "{} {} {} [{}] {:.0f}%".format(_atop["side"],_atop["price"],pcls(_atop["state"]),pcls(_atop["class"]),_atop["conf"]) if _atop else "--"
    icsig = "{} {} {} [{}] {:.0f}%".format(_itop["side"],_itop["price"],pcls(_itop["state"]),pcls(_itop["class"]),_itop["conf"]) if _itop else "--"    # SWEEP live candidate (from DetectorCore)
    _sweep_a = s.get("inst_sweep", []); _stop = _sweep_a[0] if _sweep_a else None
    swig = "{} {} {} [{}] {:.0f}%".format(_stop["side"],_stop["price"],pcls(_stop["state"]),pcls(_stop["class"]),_stop["conf"]) if _stop else "--"
    # ASK sweep = offers eaten by buyers = BULLISH (inverse side)
    swc = "bull" if _stop and _stop["side"]=="ASK" else "bear" if _stop else "neut"
    _sev = {e["k"]:e["v"] for e in _stop.get("ev",[])} if _stop else {}
    swlvl = int(_sev.get("levels",0)) if _stop else "--"
    swqty = int(_sev.get("consump",0)) if _stop else 0
    swcvd = "REAL" if (FLOW_QUALITY == "REAL" and _stop) else "UNAVAILABLE"
    sweep_panel = _panel(_sweep_a)
    _vac_a = s.get("inst_vacuum", []); _vtop = _vac_a[0] if _vac_a else None
    vacSig = "{} {} {} [{}] {:.0f}%".format(_vtop["side"],_vtop["price"],pcls(_vtop["state"]),pcls(_vtop["class"]),_vtop["conf"]) if _vtop else "--"
    # ASK vacuum = offers gone = BULLISH
    vac = "bull" if _vtop and _vtop["side"]=="ASK" else "bear" if _vtop else "neut"
    _vev = {e["k"]:e["v"] for e in _vtop.get("ev",[])} if _vtop else {}
    vacPct = int(_vev.get("collapse",0)) if _vtop else "--"
    vacCVD = "REAL" if (FLOW_QUALITY == "REAL" and _vtop) else "UNAVAILABLE"
    vacuum_panel = _panel(_vac_a)
    # ---- MARKET INTELLIGENCE (design-aligned object) ----
    _int = s.get("intel", {}) or {}
    _tox = s.get("toxicity", {}) or {}
    def _mbar(lbl, val, cls):
        return '<div class="row"><span class="lbl">{0}</span><span class="val {2}">{1}</span></div>'.format(lbl, val, cls)
    phase_mi = _int.get("phase","--"); intent_mi = _int.get("intent","--"); health_mi = _int.get("health","--")
    conf_mi = "{:.0f}%".format(_int.get("confidence",0)*100) if _int.get("confidence") else "--"
    confd_mi = "{:+.0f}%".format(_int.get("conf_delta",0)*100) if _int.get("conf_delta") else ""
    opp_mi = _int.get("opportunity","--")
    ready_mi = bool(_int.get("ready",False))
    rdy_cls = "bull" if ready_mi else "bear"; rdy_txt = "YES" if ready_mi else "NO"
    story_mi = _int.get("story","--")
    inv_mi = " | ".join(_int.get("invalidations",[])) or "--"
    _risk = _int.get("risk", {}) or {}
    risk_html = "".join(_mbar(str(k).title(), v, "neut") for k,v in _risk.items()) if _risk else '<div class="row"><span class="val neut">--</span></div>'
    scen_html = ""
    for sc in _int.get("scenarios",[]):
        scen_html += '<div class="row"><span class="lbl">{0}</span><span class="val amber">{1}%</span></div>'.format(sc.get("name"), sc.get("prob"))
    if not scen_html: scen_html = '<div class="row"><span class="val neut">--</span></div>'
    tox_html = _mbar("Stress", _tox.get("stress","--"), "amber") +                _mbar("Regime", _tox.get("regime","--"), "amber") +                _mbar("VPIN", _tox.get("vpin","--"), "neut") +                _mbar("Adverse", _tox.get("adverse","--"), "neut")
    wsc="amber" if s["wsig"]!="NONE" else "neut"
    etb=s.get("tot_buy_qty",0); ets=s.get("tot_sell_qty",0)
    blink="ab" if alert_state in("bear","ibear","sbear") else "ag" if alert_state in("bull","ibull","sbull") else ""
    if RECORD_FILE:
        rec_btn = '<a href="/?rec=off&sym={s}" class="btn on" title="Recording to {rf} — click to STOP">REC &#9679;</a>'.format(s=sym, rf=RECORD_FILE)
    else:
        rec_btn = '<a href="/?rec=on&sym={s}" class="btn off" title="Not recording — click to START">REC &#9678;</a>'.format(s=sym)
    _dec=s.get("decision",{}) or {}
    _ds=_dec.get("state","NO-GO")
    _dc="#1D9E75" if "LONG" in _ds else "#E24B4A" if "SHORT" in _ds else "#888"
    _emp=_dec.get("empirical",{}) or {}
    decision_banner=('<div style="display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;">'
        + '<span style="font-size:24px;font-weight:bold;color:'+_dc+';">'+_ds+'</span>'
        + '<span style="color:#aaa;font-size:11px;">event '+str(_dec.get("event") or "--")+' | edge '+str(_dec.get("empirical_status") or "--")+' | entry '+str(_dec.get("entry") or "--")+' | stop '+str(_dec.get("stop") or "--")+' | target '+str(_dec.get("target") or "--")+' | qty '+str(_dec.get("quantity",0))+'</span>'
        + '<span style="color:#EF9F27;font-size:10px;">'+(" | ".join(_dec.get("blocks",[])) if _dec.get("blocks") else "no active blocks")+'</span></div>')

    return """<!DOCTYPE html><html><head>
<meta charset="UTF-8"><meta http-equiv="refresh" content="1;url=/?sym={sym}">
<title>DOM | {ssym}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:#0a0a0f;color:#d3d1c7;font-family:'Courier New',monospace;font-size:12px;}}
.hdr{{background:#111127;padding:9px 14px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #3c3489;}}
h1{{color:#fff;font-size:12px;letter-spacing:2px;}}
.btn{{text-decoration:none;padding:4px 12px;border-radius:3px;font-size:11px;font-weight:bold;border:1px solid;}}
.on{{color:#1D9E75;border-color:#1D9E75;}}.off{{color:#444;border-color:#444;}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#1a1a1a;}}
.g3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1px;background:#1a1a1a;}}
.p{{background:#0d0d12;padding:12px;}}
.pt{{font-size:9px;letter-spacing:3px;color:#3c3489;margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid #1a1a2e;}}
.row{{display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #0f0f1a;font-size:11px;}}
.lbl{{color:#666;}}.val{{font-weight:bold;text-align:right;}}
.bull{{color:#1D9E75;}}.bear{{color:#E24B4A;}}.amber{{color:#EF9F27;}}
.neut{{color:#555;}}.w{{color:#d3d1c7;}}.c5{{color:#555;}}.mild-bull{{color:#1D9E75;opacity:0.7;}}.mild-bear{{color:#E24B4A;opacity:0.7;}}
.bk{{display:grid;grid-template-columns:1fr 1fr;}}
.bh{{display:grid;grid-template-columns:1fr 1.5fr 1fr;padding:3px 5px;background:#111127;color:#555;font-size:9px;}}
.br{{display:grid;grid-template-columns:1fr 1.5fr 1fr;padding:3px 5px;border-bottom:1px solid #0f0f1a;font-size:11px;}}
.br:hover{{background:#111127;}}
.bid-r{{border-left:2px solid #1D9E75;}}.ask-r{{border-left:2px solid #E24B4A;}}
.wall-r{{background:rgba(239,159,39,0.08)!important;border-left:3px solid #EF9F27!important;}}
.ib{{height:7px;background:#E24B4A;border-radius:4px;overflow:hidden;margin:5px 0;}}
.ibf{{height:100%;background:#1D9E75;border-radius:4px;transition:width 0.2s;}}
.sb{{padding:10px;border-radius:4px;border-left:4px solid;margin:6px 0;}}
.sb.bull{{border-color:#1D9E75;background:rgba(29,158,117,0.08);}}
.sb.bear{{border-color:#E24B4A;background:rgba(226,75,74,0.08);}}
.sb.mild-bull{{border-color:#1D9E75;background:rgba(29,158,117,0.04);}}
.sb.mild-bear{{border-color:#E24B4A;background:rgba(226,75,74,0.04);}}
.sb.neutral{{border-color:#555;background:rgba(100,100,100,0.04);}}
.sdir{{font-size:17px;font-weight:bold;margin-bottom:4px;}}
.si{{font-size:11px;color:#888;padding:2px 0 2px 10px;position:relative;}}
.si:before{{content:"->";position:absolute;left:0;color:#3c3489;}}
.strb{{height:5px;background:#1a1a2e;border-radius:3px;overflow:hidden;margin:3px 0;}}
.strf{{height:100%;border-radius:3px;transition:width 0.2s;}}
.scb{{height:5px;background:#E24B4A;border-radius:3px;overflow:hidden;margin:3px 0;}}
.scf{{height:100%;background:#1D9E75;border-radius:3px;}}
.al{{max-height:90px;overflow-y:auto;}}
.ai{{font-size:10px;color:#EF9F27;padding:2px 0;border-bottom:1px solid #0f0f1a;}}
.tl{{display:inline-block;padding:0 3px;border-radius:2px;font-size:9px;margin-left:2px;background:rgba(239,159,39,0.2);color:#EF9F27;}}
.hl{{background:#111127;padding:1px 6px;border-radius:2px;}}
.ftr{{background:#0a0a0f;border-top:1px solid #1a1a2e;padding:5px 14px;font-size:10px;color:#333;display:flex;justify-content:space-between;}}
@keyframes pb{{0%{{background:#0a0a0f;}}50%{{background:#2a0000;}}100%{{background:#0a0a0f;}}}}
@keyframes pg{{0%{{background:#0a0a0f;}}50%{{background:#002a00;}}100%{{background:#0a0a0f;}}}}
.ab{{animation:pb 0.5s infinite;}}.ag{{animation:pg 0.5s infinite;}}
</style></head><body class="{blink}">
<div class="hdr">
  <h1>DOM ENGINE | ALL-WEATHER MATRIX v71.99 | {feed} | {levels}-LEVEL DEPTH</h1>
  <div style="display:flex;gap:8px;align-items:center;">
    <a href="/?sym=NIFTY"     class="btn {nfc}">NIFTY FUT</a>
    <a href="/?sym=BANKNIFTY" class="btn {bnfc}">BANKNIFTY FUT</a>
    <a href="/?view=trade&sym={sym}" class="btn" style="color:#EF9F27;border-color:#EF9F27;">TRADE CARD</a>
    {rec_btn}
    {ldot}
    <span style="color:#555;font-size:10px;">{ssym} | {last} | <span class="bull">{ticks}&#x2191;</span> | <span class="{mkt_col}">{mkt_status}</span> | Spot: <span class="w hl">{spot}</span></span>
  </div>
</div>
{warn}{err}
<div class="p" style="grid-column:1/-1;background:#0a0a0f;border:1px solid #3c3489;border-radius:4px;padding:10px;margin:4px 0;">
  <div class="pt" style="color:#EF9F27;">MARKET INTELLIGENCE &mdash; {phase_mi} | {intent_mi} | {health_mi}</div>
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;">
    <div>
      <div style="font-size:9px;color:#555;letter-spacing:1px;margin-bottom:3px;">STATE CONFIDENCE (HEURISTIC)</div>
      <div class="row"><span class="val amber" style="font-size:14px;">{conf_mi} <span style="font-size:9px">{confd_mi}</span></span></div>
      <div class="row"><span class="lbl">Opportunity</span><span class="val neut">{opp_mi}</span></div>
      <div class="row"><span class="lbl">Exec Ready</span><span class="val {rdy_cls}">{rdy_txt}</span></div>
    </div>
    <div>
      <div style="font-size:9px;color:#555;letter-spacing:1px;margin-bottom:3px;">RISK (multidim)</div>
      {risk_html}
    </div>
    <div>
      <div style="font-size:9px;color:#555;letter-spacing:1px;margin-bottom:3px;">TOXICITY</div>
      {tox_html}
    </div>
    <div>
      <div style="font-size:9px;color:#555;letter-spacing:1px;margin-bottom:3px;">SCENARIO WEIGHTS (HEURISTIC)</div>
      {scen_html}
    </div>
    <div>
      <div style="font-size:9px;color:#555;letter-spacing:1px;margin-bottom:3px;">STORY</div>
      <div style="font-size:10px;color:#d3d1c7;line-height:1.6;">{story_mi}</div>
      <div style="font-size:9px;color:#EF9F27;margin-top:3px;">Invalidates: {inv_mi}</div>
    </div>
  </div>
</div>
<div class="p" style="grid-column:1/-1;background:#0d0d12;border:1px solid #3c3489;border-radius:4px;padding:10px;margin:4px 0;">
  <div class="pt" style="color:#EF9F27;">MARKETOS DECISION ENGINE</div>
  {decision_banner}
</div>
<div class="g2">
<div class="p">
  <div class="pt">LIVE ORDER BOOK — {levels} LEVELS RECEIVED | TOP 10 SHOWN</div>
  <div class="bk">
    <div><div class="bh"><span>ORD</span><span>QTY</span><span>BID</span></div>{bh}</div>
    <div><div class="bh"><span>ASK</span><span>QTY</span><span>ORD</span></div>{ah}</div>
  </div>
  <div class="ib"><div class="ibf" style="width:{bp}%;"></div></div>
  <div style="display:flex;justify-content:space-between;font-size:10px;">
    <span class="bull">BID {bp}% | {tb:,}</span>
    <span class="{ic}">{sig}</span>
    <span class="bear">{ta:,} | {ap}% ASK</span>
  </div>
  <div style="font-size:9px;color:#555;margin-top:2px;display:flex;justify-content:space-between;">
    <span>Near: <span class="{nc}">{nsig} ({nbp}%/{nap}%)</span></span>
    <span>TBQ/TSQ — Buy:<span class="bull">{etb:,}</span> Sell:<span class="bear">{ets:,}</span></span>
  </div>
  <div style="font-size:9px;margin-top:2px;color:#555;">
    Depth ratio: <span class="{drc}">{dr:.2f}x</span>
  </div>
</div>
<div class="p">
  <div class="pt">DOM METRICS — 5 ENGINES | {levels} LEVELS</div>
  <div style="font-size:9px;color:#3c3489;letter-spacing:2px;margin-bottom:3px;">ENGINE 1 — IMBALANCE</div>
  <div class="row"><span class="lbl">50L Imbalance</span><span class="val {ic}">{sig}</span></div>
  <div class="row"><span class="lbl">Near-Zone</span><span class="val {nc}">{nsig}</span></div>
  <div class="row"><span class="lbl">TBQ / TSQ (book totals)</span><span class="val neut">{etb:,} / {ets:,}</span></div>
  <div style="font-size:9px;color:#3c3489;letter-spacing:2px;margin:5px 0 3px;">ENGINE 2 — WALL DETECTION</div>
  <div class="row"><span class="lbl">Wall Signal</span><span class="val {wsc}">{wsig}</span></div>
  {wd}
  <div style="font-size:9px;color:#3c3489;letter-spacing:2px;margin:5px 0 3px;">ENGINE 3 — ABSORPTION</div>
  <div class="row"><span class="lbl">Absorption</span><span class="val {atc}">{absig}</span></div>
  <div style="font-size:9px;color:#3c3489;letter-spacing:2px;margin:5px 0 3px;">ENGINE 4 — ICEBERG</div>
  <div class="row"><span class="lbl">Iceberg Print</span><span class="val {itc}">{icsig}</span></div>
  <div style="font-size:9px;color:#3c3489;letter-spacing:2px;margin:5px 0 3px;">ENGINE 5 — TRADE FLOW</div>
  <div class="row"><span class="lbl">Flow quality</span><span class="val neut">{flow_quality_label}</span></div>
  <div class="row"><span class="lbl">Verified delta</span><span class="val {dvc}">{delta_display}</span></div>
  <div class="row"><span class="lbl">Session delta</span><span class="val {sdc}">{sess_delta_display}</span></div>
  <div style="font-size:9px;color:#EF9F27;letter-spacing:2px;margin:5px 0 3px;">ENGINE 6 &mdash; SWEEP</div>
  <div class="row"><span class="lbl">Signal</span><span class="val {swc}" id="sweepRow">{swig}</span></div>
  <div class="row"><span class="lbl">Levels Hit</span><span class="val neut">{swlvl}</span></div>
  <div class="row"><span class="lbl">Volume</span><span class="val neut">{swqty:,}</span></div>
  <div class="row"><span class="lbl">Trade-flow</span><span class="val {swc}">{swcvd}</span></div>
  <div style="font-size:9px;color:#EF9F27;letter-spacing:2px;margin:5px 0 3px;">ENGINE 7 &mdash; VACUUM</div>
  <div class="row"><span class="lbl">Signal</span><span class="val {vac}" id="vacRow">{vacSig}</span></div>
  <div class="row"><span class="lbl">Evaporated</span><span class="val neut">{vacPct}%</span></div>
  <div class="row"><span class="lbl">Trade-flow</span><span class="val {vac}">{vacCVD}</span></div>
  <div style="display:flex;justify-content:space-between;font-size:10px;margin-top:7px;">
    <span class="bull">BULL {bs}</span><span class="neut">DOM SCORE</span><span class="bear">BEAR {rs}</span>
  </div>
  <div class="scb"><div class="scf" style="width:{bb}%;"></div></div>
</div>
</div>
<div class="g3">
<div class="p">
  <div class="pt">DEEP ANALYSIS</div>
  <div class="row"><span class="lbl">Bid Liquidity Center</span><span class="val bull">{vb}</span></div>
  <div class="row"><span class="lbl">Ask Liquidity Center</span><span class="val bear">{va}</span></div>
  <div class="row"><span class="lbl">Depth Ratio</span><span class="val {drc}">{dr:.2f}x</span></div>
  <div class="row"><span class="lbl">Concentration</span><span class="val {cc}">{conc}% {cl}</span></div>
  <div class="row"><span class="lbl">Depth Levels</span><span class="val w">{levels}</span></div>
</div>
<div class="p">
  <div class="pt">PERSISTENT LIQUIDITY MAP</div>
  <div style="font-size:8px;color:#555;margin-bottom:5px;">5pt zones | search envelope &plusmn;{map_limit:.0f} | observed coverage U:{upcov:.0f} D:{dncov:.0f}</div>
  <div style="font-size:9px;color:#1D9E75;letter-spacing:1px;margin-bottom:2px;">OVERHEAD — NEXT MAPPED LIQUIDITY</div>
  <div style="font-size:8px;color:#444;margin-bottom:2px;">PRICE........DIST....DENS....SOURCE</div>
  {map_up_html}
  <div style="font-size:9px;color:#E24B4A;letter-spacing:1px;margin:7px 0 2px;">DOWN-SIDE — NEXT MAPPED LIQUIDITY</div>
  <div style="font-size:8px;color:#444;margin-bottom:2px;">PRICE........DIST....DENS....SOURCE</div>
  {map_dn_html}
  <div style="font-size:8px;color:#444;margin-top:6px;border-top:1px solid #1a1a2e;padding-top:5px;">Visible 50L ≠ session map. Unobserved prices are never invented.</div>
</div>
<div class="p">
  <div class="pt">COMPOSITE DOM SIGNAL</div>
  <div class="sb {dcc}">
    <div class="sdir {ddir}">{dsig}</div>
    <div class="strb"><div class="strf" style="width:{dst}%;background:{strc};"></div></div>
    <div style="font-size:10px;color:#555;">Score: {dst} (0-100, <b>not</b> a probability)</div>
    {sgh}
  </div>
</div>
</div>
<div class="p" style="grid-column:1/-1;background:#0a0a0f;border:1px solid #1D9E75;border-radius:4px;padding:12px;margin:4px 0;">
  <div class="pt" style="color:#1D9E75;">ACTIVE INSTITUTIONAL ORDERS — LIVE (DetectorCore v1.0)</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:1px;">
    <div>
      <div style="font-size:9px;color:#1D9E75;margin-bottom:3px;letter-spacing:2px;">ABSORPTION (price acceptance)</div>
      <div id="instAbsPanel" style="font-size:11px;line-height:1.8;">{abs_panel}</div>
    </div>
    <div>
      <div style="font-size:9px;color:#3c3489;margin-bottom:3px;letter-spacing:2px;">ICEBERG (hidden liquidity)</div>
      <div id="instIcePanel" style="font-size:11px;line-height:1.8;">{ice_panel}</div>
    </div>
    <div>
      <div style="font-size:9px;color:#EF9F27;margin-bottom:3px;letter-spacing:2px;">SWEEP (liquidity removal)</div>
      <div id="instSweepPanel" style="font-size:11px;line-height:1.8;">{sweep_panel}</div>
    </div>
    <div>
      <div style="font-size:9px;color:#E24B4A;margin-bottom:3px;letter-spacing:2px;">VACUUM (collapse)</div>
      <div id="instVacPanel" style="font-size:11px;line-height:1.8;">{vacuum_panel}</div>
    </div>
  </div>
  <div style="border-top:1px solid #1a1a2e;margin-top:6px;padding-top:6px;">
    <div style="font-size:9px;color:#555;">Lifecycle: WATCHING &rarr; SUSPECTED &rarr; BUILDING &rarr; CONFIRMED &rarr; DOMINANT &rarr; EXHAUSTING &rarr; FINISHED | Confirmed once, live until FINISHED</div>
  </div>
  <div style="font-size:9px;color:#333;margin-top:6px;">
    Active events: <span id="evCount" style="color:#EF9F27;">0</span> |
    Confirmed = 5+ evidence ticks + CVD agreement |
    Merged within 30s window per level
  </div>
</div>

<div class="g2">
<div class="p">
  <div class="pt">HOW TO USE WITH ALL-WEATHER MATRIX v71.99</div>
  <div style="font-size:11px;line-height:2.1;color:#666;">
    <span class="bull">VALIDATED setup + mapped target</span> &rarr; eligible for execution review<br>
    <span class="amber">PROVISIONAL setup</span> &rarr; paper/research only until empirical edge is demonstrated<br>
    <span class="bear">NO-GO / risk block</span> &rarr; do not trade<br>
    <span style="color:#378ADD;">SESSION MAP</span> &rarr; target source; coverage is shown explicitly<br>
    <span style="color:#378ADD;">ABSORPTION / ICEBERG</span> &rarr; only when real trade flow is available
  </div>
</div>
<div class="p">
  <div class="pt">LIVE ALERTS</div>
  <div class="al">{alh}</div>
</div>
</div>
<div class="ftr">
  <span>DOM Engine v5.0 | Feed: {feed} | {NF_SYM} / {BNF_SYM}</span>
  <span>Evidence-first / replay-first architecture + v71.35 Dual Engine</span>
</div>
<script>
var st="{{alert_state}}",orig=document.title;
if(st==="bear"||st==="ibear"||st==="sbear"){{
  setInterval(function(){{document.title=document.title===orig?"\u26a0 ALERT | "+orig:orig;}},500);
}}
if(st==="bull"||st==="ibull"||st==="sbull"){{
  setInterval(function(){{document.title=document.title===orig?"\u26a1 ALERT | "+orig:orig;}},500);
}}
</script>
<script>
function c(d){{if(!d)return"neut";d=String(d);return d.indexOf("BULL")>=0?"bull":d.indexOf("BEAR")>=0?"bear":"neut";}}
function fmt(n,dec){{if(n===null||n===undefined)return"--";return Number(n).toLocaleString("en-IN",{{minimumFractionDigits:dec||0,maximumFractionDigits:dec||0}});}}
function set(id,txt,cls){{var e=document.getElementById(id);if(!e)return;if(txt!==undefined)e.textContent=txt;if(cls!==undefined)e.className=cls;}}
function poll(){{
    fetch("/data").then(function(r){{return r.json();}}).then(function(d){{
        set("spot",d.spot?fmt(d.spot,2):"--");
        set("lastT",d.last);
        var ld=document.getElementById("liveDot");
        if(ld){{ld.textContent=d.live?"● LIVE":"● OFF";ld.style.color=d.live?"#1D9E75":"#EF9F27";}}
        set("e1s",d.sig,"val "+c(d.sig));
        set("e1n",d.nsig,"val "+c(d.nsig));
        set("e1x",fmt(d.etb)+" / "+fmt(d.ets));
        set("e2w",d.wsig,"val "+(d.wsig!=="NONE"?"amber":"neut"));
        var ab=d.absorb&&d.absorb.active?d.absorb.signal:"--";
        set("e3a",ab,"val "+c(d.absorb?d.absorb.side:""));
        var ic=d.iceberg&&d.iceberg.detected?d.iceberg.signal:"--";
        set("e4i",ic,"val "+c(d.iceberg?d.iceberg.side:""));
        set("e5t",d.delta_trend,"val "+c(d.delta_trend));
        var sw=document.getElementById("sweepRow");
        if(sw&&d.sweep){{sw.textContent=d.sweep.detected?d.sweep.signal:"--";sw.className="val "+(d.sweep.side==="BUY"?"bull":d.sweep.side==="SELL"?"bear":d.sweep.side==="CANCEL"?"amber":"neut");}}
        var vac=document.getElementById("vacRow");
        if(vac&&d.vacuum){{vac.textContent=d.vacuum.detected?d.vacuum.signal:"--";vac.className="val "+(d.vacuum.side==="BID_VACUUM"?"bear":d.vacuum.side==="ASK_VACUUM"?"bull":"neut");}}
        var sc=document.getElementById("spoofCnt"); if(sc)sc.textContent=d.spoof_cnt||0;
        var ib=document.getElementById("imbBar"); if(ib)ib.style.width=d.bp+"%";
        set("imbBid","BID "+d.bp+"% | "+fmt(d.tb));
        set("imbAsk",fmt(d.ta)+" | "+d.ap+"% ASK");
        var bsc=d.bull+d.bear>0?Math.round(d.bull/(d.bull+d.bear)*100):50;
        var sb=document.getElementById("sBr"); if(sb)sb.style.width=bsc+"%";
        set("cSig",d.dsig,"sdir "+c(d.dsig));
        var cs=document.getElementById("cStr");
        if(cs){{cs.style.width=d.dst+"%";cs.style.background=d.dsig&&d.dsig.indexOf("BULL")>=0?"#1D9E75":d.dsig&&d.dsig.indexOf("BEAR")>=0?"#E24B4A":"#555";}}
        set("vB",d.vb?fmt(d.vb,2):"--"); set("vA",d.va?fmt(d.va,2):"--");
        var al=document.getElementById("alL");
        if(al)al.innerHTML=d.alerts&&d.alerts.length?d.alerts.map(function(a){{return"<div class='ai'>"+a+"</div>";}}).join(""):"<div style='color:#333'>No alerts</div>";
        var ast_="none";
        if(d.absorb&&d.absorb.active)ast_=d.absorb.side==="BULLISH"?"bull":"bear";
        else if(d.iceberg&&d.iceberg.detected)ast_=d.iceberg.side==="BULLISH"?"ibull":"ibear";
        else if(d.dsig==="STRONG BULL")ast_="sbull";
        else if(d.dsig==="STRONG BEAR")ast_="sbear";
        document.body.classList.toggle("ab",["bear","ibear","sbear"].indexOf(ast_)>=0);
        document.body.classList.toggle("ag",["bull","ibull","sbull"].indexOf(ast_)>=0);
        renderDet("instAbsPanel",d.inst_abs);renderDet("instIcePanel",d.inst_ice);renderDet("instSweepPanel",d.inst_sweep);renderDet("instVacPanel",d.inst_vacuum);
    }}).catch(function(){{}});
}}
function renderDet(id,arr){{var el=document.getElementById(id);if(!el)return;if(!arr||!arr.length){{el.innerHTML="None active";return;}}var h="";for(var i=0;i<arr.length;i++){{var c=arr[i];var cls=(c.side==="BID")?"bull":"bear";var stc=(c.state==="CONFIRMED"||c.state==="DOMINANT")?"amber":"neut";h+="<div class=row><span class=val "+cls+">"+c.side+"@"+c.price.toFixed(1)+"</span><span class=val "+stc+">"+c.state+"</span><span class=val neut>"+c["class"]+"</span><span class=val "+cls+">"+c.conf+"%</span><span class=val neut>H:"+c.hidden+"</span></div>";}}el.innerHTML=h;}}
poll(); setInterval(poll,1000);
</script>
</body></html>""".format(
        sym=sym,ssym=ssym,nfc=nfc,bnfc=bnfc,ldot=ldot,last=s["last"],
        ticks=ticks,feed=feed,levels=levels,spot=spot,warn=warn,err=err,
        bh=bh,ah=ah,bp=bp,ap=ap,tb=tb,ta=ta,ic=ic,sig=s["sig"],mkt_col=mkt_col,mkt_status=mkt_status,
        nc=nc,nsig=s["nsig"],nbp=s["nbp"],nap=s["nap"],drc=drc,dr=dr,
        wsc=wsc,wsig=s["wsig"],wd=wd,
        atc=atc,absig=absig,itc=itc,icsig=icsig,
        dtc=col(dt),dt=dt,
        abs_panel=abs_panel,ice_panel=ice_panel,
        dvc="bull" if dv>0 else "bear" if dv<0 else "neut",dv=dv,
        sdc="bull" if sd>0 else "bear" if sd<0 else "neut",sd=sd,
        flow_quality_label=flow_quality_label, delta_display=delta_display, sess_delta_display=sess_delta_display,
        map_limit=_map_limit, upcov=_upcov, dncov=_dncov, map_up_html=_map_up_html, map_dn_html=_map_dn_html,
        bs=bs,rs=rs,bb=bb,vb=vb,va=va,cc=cc,cl=cl,conc=conc,
        sup=lh(s.get("sup",[]),"bull"),res=lh(s.get("res",[]),"bear"),
        bz_html=lzh(s.get("bid_zones",[]),"bull"),az_html=lzh(s.get("ask_zones",[]),"bear"),
        sup5s=s.get("sup5s",""), res5s=s.get("res5s",""),
        sup30s=s.get("sup30s",""),res30s=s.get("res30s",""),
                # SWEEP using precomputed vars
        swig=swig,swc=swc,swlvl=swlvl,swqty=swqty,swcvd=swcvd,
        sweep_panel=sweep_panel,
        # VACUUM using precomputed vars
        vacSig=vacSig,vac=vac,vacPct=vacPct,vacCVD=vacCVD,
        vacuum_panel=vacuum_panel,
        phase_mi=phase_mi,intent_mi=intent_mi,health_mi=health_mi,
        conf_mi=conf_mi,confd_mi=confd_mi,opp_mi=opp_mi,rdy_cls=rdy_cls,rdy_txt=rdy_txt,
        risk_html=risk_html,tox_html=tox_html,scen_html=scen_html,
        story_mi=story_mi,inv_mi=inv_mi,
dcc=dcc,ddir=ddir,dsig=s.get("dsig","NEUTRAL"),dst=dst,strc=strc,sgh=sgh,alh=alh,
        rec_btn=rec_btn,
        NF_SYM=NF_SYM,BNF_SYM=BNF_SYM,blink=blink,alert_state=alert_state,
        etb=etb,ets=ets,decision_banner=decision_banner)

# ---- Plain-language level fusion (fused across absorption + iceberg at same level) ----
PCLS = {
    "PASSIVE DEFENSE":"HOLDING","PASSIVE ACCUMULATION":"ACCUMULATING",
    "EXHAUSTION":"WEAKENING","BREAKOUT/TRAP":"BROKEN","ABSORPTION":"ABSORBING",
    "PASSIVE":"STRONG HIDDEN","DEFENSIVE":"HOLDING","RELOADING":"REBUILDING",
    "EXHAUSTING":"FADING","EXECUTION":"VISIBLE","ICEBERG":"HIDDEN",
    "SPOOF COLLAPSE":"FAKE","MARKET-MAKER RETREAT":"MM WITHDRAW",
    "EXECUTION VACUUM":"CONSUMED","LIQUIDITY PULL":"PULLED",
    "STOP HUNT":"STOP HUNT","INITIATION":"INITIATING","BREAKOUT":"BREAKOUT",
    "LIQUIDATION":"LIQUIDATION","SWEEP":"SWEEP","NONE":"--",
}
def pcls(c): return PCLS.get(c, c)

def _level_status(cands):
    """Fused plain status for a level zone. Priority:
    REBUILDING > WEAKENING > SPENT > BROKEN > STRONG > FORMING."""
    if not cands: return "WATCHING"
    st=None
    for c in cands:
        cl=pcls(c.get("class","")); stt=c.get("state","")
        if cl=="REBUILDING": st="REBUILDING"
        elif cl in ("WEAKENING","FADING") and st!="REBUILDING": st="WEAKENING"
        elif cl=="BROKEN" and st not in ("REBUILDING","WEAKENING"): st="BROKEN"
        elif stt=="FINISHED" and st is None: st="SPENT"
        elif cl in ("HOLDING","STRONG HIDDEN","ACCUMULATING","ABSORBING","HIDDEN") and st is None:
            st="STRONG"
    return st or "FORMING"

def _fused_levels(walls):
    """Group absorption+iceberg by zone; one plain read per level (no double-voice)."""
    zones={}
    for c in walls:
        zk=round(c["price"]/2.5)*2.5
        zones.setdefault(zk,[]).append(c)
    out=[]
    for zk,arr in zones.items():
        top=max(arr,key=lambda c:c["conf"])
        out.append({"side":top["side"],"price":top["price"],
                    "status":_level_status(arr),"conf":top["conf"]})
    out.sort(key=lambda x:-x["conf"])
    return out

def build_trade_card():
    """Decision-first Trade Card.

    The card is deliberately small at the top and evidence-heavy at the bottom.
    The two research panels added here are the canonical live views of:
      1) observed forward-outcome edge by event/direction;
      2) the persistent 5-point liquidity map used by the target engine.

    Neither panel invents probabilities or unobserved liquidity.
    """
    with S_LOCK:
        s = dict(S)

    sym = s.get("sym_str", "--")
    symq = s.get("sym", "NIFTY")
    spot = s.get("spot")
    spot_s = "{:,.2f}".format(float(spot)) if spot is not None else "--"
    live = bool(s.get("live"))

    dec = s.get("decision", {}) or {}
    trig = s.get("trigger", {}) or {}
    risk = s.get("risk", {}) or {}
    intel = s.get("intel", {}) or {}
    tox = s.get("toxicity", {}) or {}
    bm = s.get("book_micro", {}) or {}
    profile = s.get("profile", {}) or {}
    emp = dec.get("empirical", {}) or {}
    dataq = s.get("dataq", s.get("data_quality", {})) or {}
    edge_events = s.get("edge_event_stats", {}) or {}
    liquidity_path = s.get("liquidity_path", {}) or {}

    state = str(dec.get("state") or "NO-GO")
    action = "WAIT"
    if state.endswith("LONG"):
        action = "LONG"
    elif state.endswith("SHORT"):
        action = "SHORT"

    estatus = str(dec.get("empirical_status") or emp.get("status") or "RESEARCH_ONLY")
    n = int(emp.get("n", 0) or 0)
    win = float(emp.get("win_rate", 0) or 0)
    ci_lo = float(emp.get("win_ci_lo", 0) or 0)
    mean_move = float(emp.get("mean_move", 0) or 0)
    mean_mfe = float(emp.get("mean_mfe", 0) or 0)
    mean_mae = float(emp.get("mean_mae", 0) or 0)

    if estatus == "EMPIRICALLY_SUPPORTED":
        edge_label, edge_col = "VALIDATED", "#1D9E75"
    elif n > 0:
        edge_label, edge_col = "PROVISIONAL", "#EF9F27"
    else:
        edge_label, edge_col = "INSUFFICIENT", "#888"

    mid = float(bm.get("mid") or spot or 0)
    micro = float(bm.get("microprice") or 0)
    spread = float(bm.get("spread") or 0)
    imb = float(bm.get("imbalance") or 0)
    pressure = float(s.get("book_pressure", 0) or 0)
    micro_lead = (micro - mid) if mid and micro else 0.0

    event = str(dec.get("event") or trig.get("event") or "")
    event_text = event if event else "NONE"
    flow_label = "REAL TRADE FLOW" if dataq.get("flow_quality") == "REAL" else "BOOK-ONLY"

    target = risk.get("target")
    target_source = str(risk.get("target_source") or "NONE")
    target_meta = risk.get("target_meta", {}) or {}
    target_note = "--"
    if target is not None:
        if target_source == "SESSION_MAP":
            target_note = "SESSION MAP | D{:.0f} | {} obs".format(
                float(target_meta.get("relevant_density", target_meta.get("density", 0)) or 0),
                int(target_meta.get("observations", 0) or 0))
        elif target_source == "PRIOR_SESSION_MAP":
            target_note = "PRIOR SESSION MAP | D{:.0f}".format(
                float(target_meta.get("relevant_density", target_meta.get("density", 0)) or 0))
        elif target_source == "RAIL_CONTEXT":
            target_note = "RAIL CONTEXT (not mapped liquidity)"
        else:
            target_note = "RISK FALLBACK"

    direction = 1 if action == "LONG" else -1 if action == "SHORT" else 1
    map_key = "up_map" if direction > 0 else "down_map"
    map_rows = [x for x in list(profile.get(map_key, []) or []) if x.get("price") is not None]
    map_rows.sort(key=lambda x: (float(x.get("distance", 1e9) or 1e9), -float(x.get("target_score", 0) or 0)))
    next_map = map_rows[0] if map_rows else None
    coverage_up = float(profile.get("coverage_up_points", 0) or 0)
    coverage_down = float(profile.get("coverage_down_points", 0) or 0)
    coverage = coverage_up if direction > 0 else coverage_down
    map_limit = float(profile.get("map_max_distance_points", 1000 if symq == "BANKNIFTY" else 500) or 0)
    observed_span = max(coverage_up, coverage_down)

    entry = risk.get("entry", dec.get("entry", trig.get("entry")))
    stop = risk.get("stop")
    rr = risk.get("net_rr", dec.get("net_rr"))
    qty = risk.get("quantity", dec.get("quantity", risk.get("size_units", 0)))
    est_risk = risk.get("estimated_risk", risk.get("risk_amount"))
    slippage = risk.get("slippage_points")

    blocks = list(dec.get("blocks", []) or [])
    if not blocks and not dec.get("trade_enabled", False):
        blocks = list(trig.get("reason", []) or [])
    if not blocks and not risk.get("ready", False) and risk.get("reason"):
        blocks = [str(risk.get("reason"))]

    dq_bits = []
    if dataq.get("flow_quality") and dataq.get("flow_quality") != "REAL":
        dq_bits.append("no trade tape")
    if dataq.get("book_levels") and int(dataq.get("book_levels") or 0) < 50:
        dq_bits.append("incomplete 50L")
    if dataq.get("stale_gap"):
        dq_bits.append("stale gap")
    dq_text = ", ".join(dq_bits) if dq_bits else "50L book OK"

    inval = list(dec.get("blocks", []) or [])
    if not inval:
        inval = list(trig.get("invalidation", []) or [])
    if not inval and stop is not None:
        inval = ["stop {:.2f}".format(float(stop))]

    action_col = "#1D9E75" if action == "LONG" else "#E24B4A" if action == "SHORT" else "#888"
    status_text = state.replace("VALIDATED-", "").replace("PROVISIONAL-", "") if state != "NO-GO" else "NO-GO"

    def num(x, d=2):
        if x is None:
            return "--"
        try:
            return ("{:,.%df}" % d).format(float(x))
        except Exception:
            return "--"

    def signed(x, d=2):
        if x is None:
            return "--"
        try:
            return ("{:+,.%df}" % d).format(float(x))
        except Exception:
            return "--"

    def esc(x):
        # Values originate from the engine, not arbitrary user HTML.  Escape
        # anyway so a malformed feed field cannot break the Trade Card DOM.
        import html
        return html.escape(str(x))

    def edge_stat(event_name, side_name):
        raw = edge_events.get(event_name, {}) if isinstance(edge_events, dict) else {}
        st = raw.get(side_name, {}) if isinstance(raw, dict) else {}
        if not isinstance(st, dict):
            return {"n": 0}
        return st

    edge_rows = []
    edge_event_labels = [
        ("BOOK_SWEEP", "BOOK_SWEEP"),
        ("BOOK_VACUUM", "BOOK_VACUUM"),
        ("BOOK_PRESSURE", "BOOK_PRESSURE"),
        ("VISIBLE_LIQUIDITY_DEPLETION", "VISIBLE_LIQUIDITY_DEPLETION"),
        ("VISIBLE_REPLENISHMENT", "VISIBLE_REPLENISHMENT"),
        ("LIQUIDITY_DEPLETION", "LIQUIDITY_DEPLETION"),
        ("LIQUIDITY_RELOAD", "LIQUIDITY_RELOAD"),
        ("LIQUIDITY_DEFENSE", "LIQUIDITY_DEFENSE"),
        ("LIQUIDITY_CLEARANCE", "LIQUIDITY_CLEARANCE"),
        ("LIQUIDITY_ACCEPTANCE", "LIQUIDITY_ACCEPTANCE"),
        ("TARGET_CONTINUATION", "TARGET_CONTINUATION"),
        ("TARGET_REJECTION", "TARGET_REJECTION"),
    ]
    for key, label in edge_event_labels:
        ls = edge_stat(key, "long")
        ss = edge_stat(key, "short")
        def edge_cells(st):
            nn = int(st.get("n", 0) or 0)
            if not nn:
                return "--", "--", "--", "--", "--"
            return (
                str(nn),
                "{:.1f}%".format(float(st.get("win_rate", 0) or 0) * 100),
                signed(st.get("mean_move"), 2),
                signed(st.get("mean_mfe"), 2),
                signed(st.get("mean_mae"), 2),
            )
        lvals = edge_cells(ls); svals = edge_cells(ss)
        edge_rows.append((label, lvals, svals))

    def edge_row_html(label, lvals, svals):
        cells = [esc(label)]
        for v in lvals + svals:
            cells.append(esc(v))
        return "<tr>" + "".join("<td>{}</td>".format(v) for v in cells) + "</tr>"

    edge_table_html = "".join(edge_row_html(label, lv, sv) for label, lv, sv in edge_rows)
    edge_note = "Observed forward outcomes at the configured horizon. Not a probability forecast."
    if not any(v[0] != "--" for _, lv, sv in edge_rows for v in (lv, sv)):
        edge_note += " No completed samples are available yet."

    def map_rows_html(rows, side_label):
        if not rows:
            return "<tr><td colspan='6' class='muted'>No qualifying observed liquidity in this direction.</td></tr>"
        out = []
        for x in rows[:3]:
            out.append("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
                esc(num(x.get("price"), 1)),
                esc(num(x.get("distance"), 1)),
                esc(num(x.get("relevant_density", x.get("density")), 0)),
                esc(str(x.get("role", "--"))),
                esc(str(x.get("source", "--"))),
                esc(str(int(x.get("observations", 0) or 0)))))
        return "".join(out)

    up_rows = [x for x in list(profile.get("up_map", []) or []) if x.get("price") is not None]
    dn_rows = [x for x in list(profile.get("down_map", []) or []) if x.get("price") is not None]
    up_rows.sort(key=lambda x: (float(x.get("distance", 1e9) or 1e9), -float(x.get("target_score", 0) or 0)))
    dn_rows.sort(key=lambda x: (float(x.get("distance", 1e9) or 1e9), -float(x.get("target_score", 0) or 0)))

    def target_line(rows):
        if not rows:
            return "--"
        x = rows[0]
        return "{} | +{} pts | D{} | {}".format(
            num(x.get("price"), 1), num(x.get("distance"), 1),
            num(x.get("relevant_density", x.get("density")), 0), str(x.get("source", "--")))

    target_context = target_line(up_rows if action != "SHORT" else dn_rows)
    if action == "WAIT":
        target_context = "LONG: {} | SHORT: {}".format(target_line(up_rows), target_line(dn_rows))

    emp_line = "N {} | win {} | Wilson-LB {} | mean move {} | MFE {} | MAE {}".format(
        n if n else "--",
        "{:.1f}%".format(win * 100) if n else "--",
        "{:.1f}%".format(ci_lo * 100) if n else "--",
        num(mean_move), num(mean_mfe), num(mean_mae))
    ev_text = "NOT CALIBRATED"

    css = """
    <style>
      *{box-sizing:border-box}
      body{margin:0;background:#090a0f;color:#d7d5cd;font-family:Consolas,'Courier New',monospace}
      .wrap{max-width:900px;margin:0 auto;padding:18px}
      .hdr{display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:14px}
      .muted{color:#777}.box{background:#0d0e14;border:1px solid #25263a;border-radius:7px;padding:14px;margin-top:10px}
      .title{font-size:11px;letter-spacing:2px;color:#666}.section-blue{color:#378ADD}.section-amber{color:#EF9F27}
      .action{font-size:46px;font-weight:700;line-height:1.0;margin:5px 0}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
      .kv{padding:9px;border-top:1px solid #1d1e29}.k{font-size:10px;color:#666;letter-spacing:1px}.v{font-size:18px;margin-top:3px}
      .row{display:flex;justify-content:space-between;gap:12px;padding:7px 0;border-bottom:1px solid #171821}.row:last-child{border-bottom:0}
      .good{color:#1D9E75}.bad{color:#E24B4A}.amber{color:#EF9F27}.blue{color:#378ADD}.white{color:#ddd}.big{font-size:24px;font-weight:700}
      .small{font-size:11px;line-height:1.55}.nav a{color:#888;text-decoration:none;border:1px solid #333;padding:4px 9px;margin-left:4px}.nav a.on{color:#1D9E75;border-color:#1D9E75}
      .table-wrap{overflow-x:auto;margin-top:8px}.data-table{width:100%;border-collapse:collapse;font-size:10px;min-width:680px}
      .data-table th{color:#666;text-align:left;font-weight:400;letter-spacing:1px;padding:6px 7px;border-bottom:1px solid #2a2b3c}
      .data-table td{padding:7px;border-bottom:1px solid #171821;white-space:nowrap}.data-table td:first-child{color:#ddd}
      .subhead{font-size:9px;letter-spacing:2px;margin:8px 0 2px}.legend{font-size:9px;color:#555;margin-top:7px;line-height:1.5}
      .bottom-grid{display:grid;grid-template-columns:1fr;gap:10px}.badge{display:inline-block;border:1px solid #333;border-radius:3px;padding:3px 7px;font-size:9px;letter-spacing:1px}
      @media(max-width:650px){.grid{grid-template-columns:1fr 1fr}.hdr{align-items:flex-start}.action{font-size:38px}.wrap{padding:10px}}
    </style>
    """

    target_html = num(target, 1) if target is not None else "--"
    lpL=liquidity_path.get("LONG",{}) or {}
    lpS=liquidity_path.get("SHORT",{}) or {}
    def lpv(x,k,default="--"): return x.get(k,default) if isinstance(x,dict) else default
    lp_long_phase=str(lpv(lpL,"phase","IDLE")); lp_short_phase=str(lpv(lpS,"phase","IDLE"))
    lp_long_class="good" if lp_long_phase in ("CLEARING","ACCEPTED","EXTENDING") else "bad" if lp_long_phase=="REJECTING" else "amber"
    lp_short_class="good" if lp_short_phase in ("CLEARING","ACCEPTED","EXTENDING") else "bad" if lp_short_phase=="REJECTING" else "amber"
    lp_long_target=num(lpv(lpL,"target"),1); lp_short_target=num(lpv(lpS,"target"),1)
    lp_long_density=num(lpv(lpL,"density",0),0); lp_short_density=num(lpv(lpS,"density",0),0)
    lp_long_dist=num(lpv(lpL,"target_distance"),1); lp_short_dist=num(lpv(lpS,"target_distance"),1)
    lp_long_clear="YES" if lpv(lpL,"clearance",False) else "NO"; lp_short_clear="YES" if lpv(lpS,"clearance",False) else "NO"
    lp_long_accept="YES" if lpv(lpL,"accepted",False) else "NO"; lp_short_accept="YES" if lpv(lpS,"accepted",False) else "NO"
    lp_long_reload=num(lpv(lpL,"reload",0),2); lp_short_reload=num(lpv(lpS,"reload",0),2)
    lp_long_reloads=str(lpv(lpL,"reload_count",0)); lp_short_reloads=str(lpv(lpS,"reload_count",0))
    lp_long_def=str(lpv(lpL,"defense_count",0)); lp_short_def=str(lpv(lpS,"defense_count",0))
    lp_long_disp=str(lpv(lpL,"disappearance_count",0)); lp_short_disp=str(lpv(lpS,"disappearance_count",0))
    lp_long_class=str(lpv(lpL,"class","WATCH")); lp_short_class=str(lpv(lpS,"class","WATCH"))
    lp_long_setup=str(lpv(lpL,"setup_state","IDLE")); lp_short_setup=str(lpv(lpS,"setup_state","IDLE"))
    lp_long_build=num(lpv(lpL,"build_score",0),0); lp_short_build=num(lpv(lpS,"build_score",0),0)
    lp_long_fail=num(lpv(lpL,"failure_risk",0),0); lp_short_fail=num(lpv(lpS,"failure_risk",0),0)
    lp_long_micro=num(lpv(lpL,"micro_alignment",0),2); lp_short_micro=num(lpv(lpS,"micro_alignment",0),2)
    lp_long_imb=num(lpv(lpL,"imbalance_alignment",0),2); lp_short_imb=num(lpv(lpS,"imbalance_alignment",0),2)
    lp_long_exh=num(lpv(lpL,"exhaustion_score",0),0); lp_short_exh=num(lpv(lpS,"exhaustion_score",0),0)
    lp_long_exit="YES" if lpv(lpL,"exit_ready",False) else "NO"; lp_short_exit="YES" if lpv(lpS,"exit_ready",False) else "NO"
    lp_long_next=num(lpv(lpL,"next_target"),1) if lpv(lpL,"next_target",None) is not None else "--"
    lp_short_next=num(lpv(lpS,"next_target"),1) if lpv(lpS,"next_target",None) is not None else "--"
    lp_long_nextdist=num(lpv(lpL,"next_target_distance"),1) if lpv(lpL,"next_target_distance",None) is not None else "--"
    lp_short_nextdist=num(lpv(lpS,"next_target_distance"),1) if lpv(lpS,"next_target_distance",None) is not None else "--"
    lp_long_pressure=num(lpv(lpL,"pressure",0),2); lp_short_pressure=num(lpv(lpS,"pressure",0),2)
    _pevs=list(liquidity_path.get("events",[]) or [])
    path_event_line=" | ".join("{} {} @{}".format(e.get("event","--"),e.get("side","--"),num(e.get("target"),1)) for e in _pevs[-3:]) or "No path transition yet"
    trigger_text = "waiting for trigger"
    if trig.get("reason"):
        trigger_text = str(trig.get("reason")[0])
    if action != "WAIT" and entry is not None:
        trigger_text = "entry condition active"

    block_html = "<div class='muted small'>none</div>" if not blocks else "".join(
        "<div class='row'><span class='bad'>BLOCK</span><span class='white'>{}</span></div>".format(esc(b)) for b in blocks[:5])

    return """<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='1;url=?view=trade&sym={symq}'><title>MarketOS v12.3 Trade Card</title>{css}</head><body>
    <div class='wrap'>
      <div class='hdr'>
        <div><div class='big'>{sym}</div><div class='muted'>SPOT {spot} &nbsp;|&nbsp; {live}</div></div>
        <div class='nav'><a class='{nf}' href='?view=trade&sym=NIFTY'>NIFTY</a><a class='{bnf}' href='?view=trade&sym=BANKNIFTY'>BNF</a><a href='?sym={symq}'>FULL</a></div>
      </div>

      <div class='box' style='border:2px solid {action_col}'>
        <div class='title'>DECISION</div>
        <div class='action' style='color:{action_col}'>{action}</div>
        <div class='small white'>{status_text} &nbsp;|&nbsp; {trigger_text}</div>
        <div class='small muted' style='margin-top:6px'>Regime: {phase} &nbsp;|&nbsp; Health: {health} &nbsp;|&nbsp; Data: {dq}</div>
      </div>

      <div class='box'>
        <div class='title'>TRADE PLAN</div>
        <div class='grid'>
          <div class='kv'><div class='k'>ENTRY</div><div class='v'>{entry}</div></div>
          <div class='kv'><div class='k'>STOP</div><div class='v bad'>{stop}</div></div>
          <div class='kv'><div class='k'>TARGET</div><div class='v good'>{target}</div></div>
          <div class='kv'><div class='k'>NET RR</div><div class='v'>{rr}</div></div>
          <div class='kv'><div class='k'>QTY</div><div class='v'>{qty}</div></div>
          <div class='kv'><div class='k'>RISK</div><div class='v'>{risk_amt}</div></div>
        </div>
        <div class='row'><span class='k'>TARGET SOURCE</span><span class='white'>{target_source}</span></div>
        <div class='row'><span class='k'>TARGET EVIDENCE</span><span class='white'>{target_note}</span></div>
        <div class='row'><span class='k'>COST / SLIPPAGE</span><span class='white'>{slippage}</span></div>
        <div class='row'><span class='k'>ECONOMIC EV</span><span class='amber'>{ev}</span></div>
      </div>

      <div class='box'>
        <div class='title'>EVIDENCE THAT CAN CHANGE THE DECISION</div>
        <div class='row'><span>50L pressure</span><span class='{pclass}'>{pressure}</span></div>
        <div class='row'><span>Microprice lead</span><span class='{mclass}'>{microlead}</span></div>
        <div class='row'><span>50L imbalance</span><span>{imb}</span></div>
        <div class='row'><span>Spread</span><span>{spread}</span></div>
        <div class='row'><span>Structural event</span><span class='amber'>{event}</span></div>
        <div class='row'><span>Flow status</span><span class='white'>{flow}</span></div>
        <div class='row'><span>Toxicity / stress</span><span class='{tclass}'>{tox}</span></div>
      </div>

      <div class='box'>
        <div class='title'>INVALIDATION / BLOCKS</div>
        {block_html}
        <div class='small bad' style='margin-top:8px'>{invalidation}</div>
      </div>

      <div class='box' style='border-color:#1D9E75'>
        <div class='title section-blue'>LIQUIDITY PATH / INTERACTION</div>
        <div class='legend'>Map-derived target only. Price + visible-liquidity response determine the path; displayed quantity is not treated as executed volume.</div>
        <div class='grid'>
          <div class='kv'>
            <div class='k'>LONG PATH</div>
            <div class='v {lp_long_class}'>{lp_long_setup}</div>
            <div class='small'>PHASE {lp_long_phase} &nbsp;|&nbsp; TARGET {lp_long_target} &nbsp;|&nbsp; DIST {lp_long_dist}</div>
            <div class='small'>BUILD {lp_long_build} &nbsp;|&nbsp; FAIL-RISK {lp_long_fail} &nbsp;|&nbsp; EXHAUST {lp_long_exh}</div>
            <div class='small'>CLEAR {lp_long_clear} &nbsp;|&nbsp; ACCEPT {lp_long_accept} &nbsp;|&nbsp; MICRO {lp_long_micro}</div>
            <div class='small'>DISAPPEAR {lp_long_disp} &nbsp;|&nbsp; RELOADS {lp_long_reloads} &nbsp;|&nbsp; DEFENSE {lp_long_def}</div>
            <div class='small'>PRESSURE {lp_long_pressure} &nbsp;|&nbsp; EXIT-WATCH {lp_long_exit}</div>
            <div class='small'>NEXT NODE {lp_long_next} &nbsp;|&nbsp; DIST {lp_long_nextdist}</div>
            <div class='small muted'>MAP DENS {lp_long_density} &nbsp;|&nbsp; IMB ALIGN {lp_long_imb}</div>
          </div>
          <div class='kv'>
            <div class='k'>SHORT PATH</div>
            <div class='v {lp_short_class}'>{lp_short_setup}</div>
            <div class='small'>PHASE {lp_short_phase} &nbsp;|&nbsp; TARGET {lp_short_target} &nbsp;|&nbsp; DIST {lp_short_dist}</div>
            <div class='small'>BUILD {lp_short_build} &nbsp;|&nbsp; FAIL-RISK {lp_short_fail} &nbsp;|&nbsp; EXHAUST {lp_short_exh}</div>
            <div class='small'>CLEAR {lp_short_clear} &nbsp;|&nbsp; ACCEPT {lp_short_accept} &nbsp;|&nbsp; MICRO {lp_short_micro}</div>
            <div class='small'>DISAPPEAR {lp_short_disp} &nbsp;|&nbsp; RELOADS {lp_short_reloads} &nbsp;|&nbsp; DEFENSE {lp_short_def}</div>
            <div class='small'>PRESSURE {lp_short_pressure} &nbsp;|&nbsp; EXIT-WATCH {lp_short_exit}</div>
            <div class='small'>NEXT NODE {lp_short_next} &nbsp;|&nbsp; DIST {lp_short_nextdist}</div>
            <div class='small muted'>MAP DENS {lp_short_density} &nbsp;|&nbsp; IMB ALIGN {lp_short_imb}</div>
          </div>
          <div class='kv'>
            <div class='k'>PATH EVIDENCE</div>
            <div class='small'>{path_event_line}</div>
            <div class='small muted' style='margin-top:5px'>Entry-ready is a structural state, not a validated probability.</div>
          </div>
        </div>
      </div>

      <div class='bottom-grid'>
        <div class='box' style='border-color:#3c3489'>
          <div class='title section-blue'>EMPIRICAL EDGE SCOREBOARD — OBSERVED FORWARD OUTCOMES</div>
          <div class='legend'>n / win-rate / mean forward move / MFE / MAE at the configured horizon. This is historical observation, not a probability forecast.</div>
          <div class='table-wrap'>
            <table class='data-table'>
              <thead><tr><th>EVENT</th><th colspan='5' style='color:#1D9E75'>LONG</th><th colspan='5' style='color:#E24B4A'>SHORT</th></tr>
              <tr><th></th><th>N</th><th>WIN</th><th>MEAN</th><th>MFE</th><th>MAE</th><th>N</th><th>WIN</th><th>MEAN</th><th>MFE</th><th>MAE</th></tr></thead>
              <tbody>{edge_table}</tbody>
            </table>
          </div>
          <div class='legend'>{edge_note} &nbsp;|&nbsp; Current setup: <span style='color:{edge_col}'>{edge_label}</span> &nbsp;|&nbsp; {emp_line}</div>
        </div>

        <div class='box' style='border-color:#3c3489'>
          <div class='title section-amber'>PERSISTENT LIQUIDITY MAP</div>
          <div class='legend'>5-point observed zones from the moving 50-level TBT window. The NIFTY ±500 / BANKNIFTY ±1000 figure is a search envelope, not assumed coverage.</div>
          <div class='row'><span>SEARCH ENVELOPE</span><span class='white'>&plusmn;{map_limit} pts</span></div>
          <div class='row'><span>OBSERVED SESSION SPAN</span><span class='white'>{observed_span} pts</span></div>

          <div class='subhead good'>NEXT OVERHEAD LIQUIDITY &nbsp; coverage {upcov}/{map_limit}</div>
          <div class='table-wrap'>
            <table class='data-table' style='min-width:620px'>
              <thead><tr><th>PRICE</th><th>DIST</th><th>DENS</th><th>ROLE</th><th>SRC</th><th>OBS</th></tr></thead>
              <tbody>{up_map_html}</tbody>
            </table>
          </div>

          <div class='subhead bad'>NEXT DOWN-SIDE LIQUIDITY &nbsp; coverage {dncov}/{map_limit}</div>
          <div class='table-wrap'>
            <table class='data-table' style='min-width:620px'>
              <thead><tr><th>PRICE</th><th>DIST</th><th>DENS</th><th>ROLE</th><th>SRC</th><th>OBS</th></tr></thead>
              <tbody>{dn_map_html}</tbody>
            </table>
          </div>

          <div class='row' style='margin-top:8px'><span class='k'>TARGET SOURCE</span><span class='white'>{target_source}</span></div>
          <div class='row'><span class='k'>TARGET CONTEXT</span><span class='good'>{target_context}</span></div>
          <div class='legend'>Only observed zones qualify. Unobserved prices are never invented as targets. The map is the target source; old three-wall S/R is not used by this card.</div>
        </div>
      </div>

      <div class='small muted' style='padding:12px 2px'>Trade Card = decision evidence only. Full dashboard retains diagnostic modules; heuristic scenario weights, composite DOM score, old wall S/R and generic thesis text are not used here.</div>
    </div></body></html>""".format(
        css=css, symq=esc(symq), sym=esc(sym), spot=spot_s, live="LIVE" if live else "CONNECTING",
        nf="on" if str(symq).startswith("NSE:NIFTY") else "", bnf="on" if str(symq).startswith("NSE:BANKNIFTY") else "",
        action_col=action_col, action=action, status_text=esc(status_text), trigger_text=esc(trigger_text),
        phase=esc(str(intel.get("phase", "UNKNOWN"))), health=esc(str(intel.get("health", "UNKNOWN"))), dq=esc(dq_text),
        entry=num(entry), stop=num(stop), target=target_html, rr=num(rr), qty=num(qty,0),
        risk_amt=num(est_risk), target_source=esc(target_source), target_note=esc(target_note),
        slippage=num(slippage), ev=ev_text,
        pclass="good" if pressure>0 else "bad" if pressure<0 else "muted", pressure=signed(pressure),
        mclass="good" if micro_lead>0 else "bad" if micro_lead<0 else "muted", microlead=signed(micro_lead),
        imb=signed(imb,3), spread=num(spread,2), event=esc(event_text), flow=esc(flow_label),
        tclass="bad" if float(tox.get("stress",0) or 0)>=50 else "amber" if float(tox.get("stress",0) or 0)>=30 else "good",
        tox=num(tox.get("stress",0),0), block_html=block_html,
        invalidation=esc(" | ".join(str(x) for x in inval) if inval else "No additional invalidation condition surfaced."),
        edge_table=edge_table_html, edge_note=edge_note, edge_col=edge_col, edge_label=edge_label, emp_line=esc(emp_line),
        map_limit=num(map_limit,0), observed_span=num(observed_span,0), upcov=num(coverage_up,0), dncov=num(coverage_down,0),
        up_map_html=map_rows_html(up_rows,"UP"), dn_map_html=map_rows_html(dn_rows,"DOWN"), target_context=esc(target_context),
        lp_long_class=lp_long_class, lp_short_class=lp_short_class, lp_long_setup=esc(lp_long_setup), lp_short_setup=esc(lp_short_setup),
        lp_long_phase=esc(lp_long_phase), lp_short_phase=esc(lp_short_phase),
        lp_long_target=esc(lp_long_target), lp_short_target=esc(lp_short_target), lp_long_density=esc(lp_long_density), lp_short_density=esc(lp_short_density),
        lp_long_build=esc(lp_long_build), lp_short_build=esc(lp_short_build), lp_long_fail=esc(lp_long_fail), lp_short_fail=esc(lp_short_fail),
        lp_long_micro=esc(lp_long_micro), lp_short_micro=esc(lp_short_micro), lp_long_imb=esc(lp_long_imb), lp_short_imb=esc(lp_short_imb),
        lp_long_dist=esc(lp_long_dist), lp_short_dist=esc(lp_short_dist), lp_long_clear=lp_long_clear, lp_short_clear=lp_short_clear,
        lp_long_accept=lp_long_accept, lp_short_accept=lp_short_accept, lp_long_reload=esc(lp_long_reload), lp_short_reload=esc(lp_short_reload),
        lp_long_disp=esc(lp_long_disp), lp_short_disp=esc(lp_short_disp),
        lp_long_reloads=esc(lp_long_reloads), lp_short_reloads=esc(lp_short_reloads),
        lp_long_def=esc(lp_long_def), lp_short_def=esc(lp_short_def),
        lp_long_exh=esc(lp_long_exh), lp_short_exh=esc(lp_short_exh), lp_long_exit=esc(lp_long_exit), lp_short_exit=esc(lp_short_exit),
        lp_long_next=esc(lp_long_next), lp_short_next=esc(lp_short_next), lp_long_nextdist=esc(lp_long_nextdist), lp_short_nextdist=esc(lp_short_nextdist),
        lp_long_pressure=esc(lp_long_pressure), lp_short_pressure=esc(lp_short_pressure),
        path_event_line=esc(path_event_line)
    )


class ThreadingHTTPServer(__import__("socketserver").ThreadingMixIn,
                         __import__("http.server",fromlist=["HTTPServer"]).HTTPServer):
    daemon_threads = True
    def handle_error(self,req,addr):
        import sys; e=sys.exc_info()[1]
        if isinstance(e,(ConnectionAbortedError,ConnectionResetError,BrokenPipeError,OSError)): return
        print("[SERVER ERR]",type(e).__name__,str(e)[:60])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        import json as _j
        qpath = urlparse(self.path).path
        sym   = parse_qs(urlparse(self.path).query).get("sym",[None])[0]
        view  = parse_qs(urlparse(self.path).query).get("view",[None])[0]

        if qpath == "/data":
            with S_LOCK:
                payload={
                    "spot":S.get("spot"),"bp":S.get("bp",50),"ap":S.get("ap",50),
                    "tb":S.get("tb",0),"ta":S.get("ta",0),
                    "sig":S.get("sig","--"),"nsig":S.get("nsig","--"),
                    "wsig":S.get("wsig","NONE"),"dsig":S.get("dsig","NEUTRAL"),
                    "dc":S.get("dc","neutral"),"dst":S.get("dst",0),
                    "delta":S.get("delta",0),"sess_delta":S.get("sess_delta",0),
                    "delta_trend":S.get("delta_trend","NEUTRAL"),
                    "feed":S.get("feed","--"),"depth_levels":S.get("depth_levels",0),
                    "tick_count":S.get("tick_count",0),"live":S.get("live",False),
                    "last":S.get("last","--"),
                    "etb":S.get("tot_buy_qty",0),"ets":S.get("tot_sell_qty",0),
                    "bids":S.get("bids",[])[:10],"asks":S.get("asks",[])[:10],
                    "alerts":list(S.get("alerts",[]))[:10],
                    "sigs":list(S.get("sigs",[]))[:8],
                    "sup":S.get("sup",[]),"res":S.get("res",[]),
                    "bid_zones":S.get("bid_zones",[]),"ask_zones":S.get("ask_zones",[]),
                    "sup5s":S.get("sup5s",""),"res5s":S.get("res5s",""),
                    "sup30s":S.get("sup30s",""),"res30s":S.get("res30s",""),
                    "absorb":S.get("absorb",{}),"iceberg":S.get("iceberg",{}),
                    "bull":S.get("bull",0),"bear":S.get("bear",0),
                    "vb":S.get("vb"),"va":S.get("va"),
                    "dr":S.get("dr",1.0),"conc":S.get("conc",50),
                    "cvd_trend":S.get("cvd_trend","NEUTRAL"),
                    "cvd_sess":int(S.get("cvd_session",0)),
                    "flow_quality":FLOW_QUALITY,
                    "trade_count":S.get("trade_count",0),
                    "trade_total_qty":S.get("trade_total_qty",0),
                    "trade_classified_qty":S.get("trade_classified_qty",0),
                    "trade_classification_coverage": (S.get("trade_classified_qty",0) / S.get("trade_total_qty",1)) if S.get("trade_total_qty",0) else 0,
                    "trade_unclassified":S.get("trade_unclassified",0),
                    "actual_buy_volume":S.get("actual_buy_volume",0),
                    "actual_sell_volume":S.get("actual_sell_volume",0),
                    "proxy_flow_session":S.get("proxy_cvd_session",0),
                    "last_trade":S.get("last_trade",{}),
                    "quote_available":S.get("quote_available",False),
                    "quote_fields_seen":S.get("quote_fields_seen",[]),
                    "last_sequence":S.get("last_sequence"),
                    "flow_integrity":flow_integrity_snapshot(),
                    "spoof":S.get("spoof_alert",False),
                    "sweep":S.get("sweep",{}),"vacuum":S.get("vacuum",{}),
                    "level_mem":S.get("level_memory",[]),
                    "inst_abs":S.get("inst_abs",[]),"inst_ice":S.get("inst_ice",[]),
                    "inst_sweep":S.get("inst_sweep",[]),"inst_vacuum":S.get("inst_vacuum",[]),
                    "toxicity":S.get("toxicity",{}),"intel":S.get("intel",{}),
                    "flow":S.get("flow",{}),"div":S.get("div",{}),"trigger":S.get("trigger",{}),"decision":S.get("decision",{}),"empirical_edge":S.get("empirical_edge",{}),"risk":S.get("risk",{}),
                    "imp_levels":S.get("imp_levels",[]),
                    "rails":S.get("rails",{}),
                    "interact":S.get("interact",{}),
                    "profile":S.get("profile",{}),
                    "target_source":(S.get("risk",{}) or {}).get("target_source","NONE"),
                    "spoof_cnt":S.get("spoof_count",0),
                }
            body=_j.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(body)))
            self.send_header("Cache-Control","no-cache")
            self.end_headers()
            try: self.wfile.write(body)
            except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError,OSError): pass
            return

        if sym and sym!=S["sym"]: switch_symbol(sym)
        recp = parse_qs(urlparse(self.path).query).get("rec",[None])[0]
        if recp in ("on","1","yes"): set_record(True)
        elif recp in ("off","0","no"): set_record(False)
        if view=="trade":
            self.send_response(200)
            self.send_header("Content-type","text/html; charset=utf-8")
            self.end_headers()
            try:    self.wfile.write(build_trade_card().encode("utf-8"))
            except Exception as e: self.wfile.write(("<pre>Error:"+str(e)+"</pre>").encode())
            return
        self.send_response(200)
        self.send_header("Content-type","text/html; charset=utf-8")
        self.end_headers()
        try:    self.wfile.write(build_page().encode("utf-8"))
        except Exception as e: self.wfile.write(("<pre>Error:"+str(e)+"</pre>").encode())
    def log_message(self,*a): pass
    def log_error(self,*a): pass


# ============================================================
# MARKETOS v12.3.3 — DETERMINISTIC REPLAY PATH
# ============================================================
# The replay path deliberately reuses the production analytical push_update()
# and LiquidityPathEngine/Decision/Outcome state machines. It bypasses:
#   - FYERS authentication / token generation
#   - WebSocket / REST transports
#   - dashboard HTTP server / browser
#   - truth-recorder writes
#   - derived-outcome persistence writes
#   - periodic map/level persistence writes
# It does NOT replace the analytical calculations with replay-specific signals.
# JSONL observations are converted to the same canonical bid/ask dictionaries
# consumed by push_update().

def _replay_levels(rows, side):
    out=[]
    for i, row in enumerate(rows or []):
        try:
            if isinstance(row, dict):
                price=float(row.get("price",0) or 0)
                qty=int(row.get("qty",0) or 0)
                orders=row.get("orders")
                level=int(row.get("level",i) or i)
            else:
                price=float(row[0]); qty=int(row[1]); orders=row[2] if len(row)>2 else None
                level=int(row[3]) if len(row)>3 and row[3] is not None else i
            if price>0 and qty>=0:
                out.append({"price":price,"qty":qty,"orders":orders,"level":level})
        except Exception:
            continue
    if side=="BID":
        out.sort(key=lambda x:x["price"], reverse=True)
    else:
        out.sort(key=lambda x:x["price"])
    return out[:50]


def _replay_trade(rec):
    tr=rec.get("trade")
    if not isinstance(tr,dict):
        return None
    # Preserve the exact quote/trade fields if they exist.
    return dict(tr)


def _replay_feed_meta(rec):
    fm=rec.get("feed_meta")
    if not isinstance(fm,dict):
        fm={}
    else:
        fm=dict(fm)
    # The truth recorder's canonical event time is `t`. Use it as feed_time
    # when the original callback metadata is absent. This prevents replay from
    # falling back to wall-clock time and makes all time-based state machines
    # deterministic.
    if not fm.get("feed_time"):
        fm["feed_time"]=rec.get("t") or rec.get("receive_ts")
    return fm


def _replay_reset_state():
    """Prepare the already-fresh module state for a replay-only run.

    A replay CLI is a new Python process, so the production analytical objects
    created during module initialization are already untouched by live ticks.
    Reconstructing them here would risk omitting one of the production rolling
    statistics/state fields. Instead we disable all external side effects and
    make the time source deterministic while preserving the exact initialized
    analytical state machines.
    """
    global RECORD_FILE, TRUTH_RECORD_ENABLED, REPLAY_MODE, S
    REPLAY_MODE=True
    RECORD_FILE=""
    TRUTH_RECORD_ENABLED=False
    S["sym"]="NIFTY"; S["sym_str"]=NF_SYM; S["feed"]="REPLAY"; S["live"]=False
    # No derived-outcome writes during replay.
    try: OUTCOME_STORE.flush=lambda rows: None
    except Exception: pass
    # No persistent-map/level writes during replay.
    try:
        for _pf in PROFILE.values(): _pf.save_history=lambda *a,**k: None
    except Exception: pass
    try: LEVELS.save=lambda *a,**k: None
    except Exception: pass
    return True


def _replay_snapshot(rec, events, elapsed_ms):
    lp=(S.get("liquidity_path") or {})
    long=dict(lp.get("LONG") or {})
    short=dict(lp.get("SHORT") or {})
    dec=dict(S.get("decision") or {})
    trig=dict(S.get("trigger") or {})
    dq=dict(S.get("data_quality") or {})
    rows=[]
    for side,c in (("LONG",long),("SHORT",short)):
        rows.append({
            "t":rec.get("t"),"timestamp":datetime.fromtimestamp(float(rec.get("t"))).isoformat() if rec.get("t") else "",
            "tick":S.get("tick_count",0),"spot":S.get("spot"),"side":side,
            "phase":c.get("phase",c.get("setup_state","IDLE")),
            "target":c.get("target"),"target_distance":c.get("target_distance"),
            "build_score":c.get("build_score"),"failure_risk":c.get("failure_risk"),
            "exhaustion_score":c.get("exhaustion_score"),"setup_state":c.get("setup_state"),
            "entry_state":c.get("entry_state"),"entry_ready":c.get("entry_ready"),
            "clear":c.get("cleared",c.get("clear",False)),"accepted":c.get("accepted",False),
            "micro_alignment":c.get("micro_alignment",c.get("micro",0)),
            "imbalance_alignment":c.get("imbalance_alignment"),"pressure":c.get("pressure"),
            "disappearance_count":c.get("disappearance_count",c.get("regime_disappearances",0)),
            "reload_count":c.get("reload_count",c.get("regime_reloads",0)),
            "defense_count":c.get("defense_count",c.get("regime_defense",0)),
            "next_target":c.get("next_target"),"next_target_distance":c.get("next_target_distance"),
            "regime_id":c.get("regime_id"),"regime_freshness":c.get("regime_freshness"),
            "regime_committed":c.get("regime_committed"),
            "regime_clearance_emitted":c.get("regime_clearance_emitted"),
            "regime_closed":c.get("regime_closed"),"rearm_count":c.get("rearm_count"),
            "replay_event":events[-1]["event"] if events else "",
            "decision_state":dec.get("state"),"decision_side":dec.get("side"),
            "decision_event":dec.get("event"),"trigger_side":trig.get("side"),
            "trigger_go":trig.get("go"),"flow_quality":S.get("flow",{}).get("flow_quality",FLOW_QUALITY),
            "depth50":dq.get("depth_complete_50"),"inter_update_gap_s":dq.get("inter_update_gap_s"),
            "engine_elapsed_ms":round(elapsed_ms,3)
        })
    return rows


def replay_file(path, audit_path=None, event_path=None, start_ts=None, end_ts=None, progress_every=5000):
    """Replay a canonical MarketOS truth JSONL file through v12.3.3.

    Returns a compact benchmark dictionary. The audit CSV has TWO rows per
    observation (LONG/SHORT), plus every emitted path event in a separate JSONL.
    """
    global REPLAY_MODE
    REPLAY_MODE=True
    _replay_reset_state()
    import csv
    if audit_path is None:
        audit_path=os.path.splitext(path)[0]+"_v1233_replay_audit.csv"
    if event_path is None:
        event_path=os.path.splitext(path)[0]+"_v1233_replay_events.jsonl"
    os.makedirs(os.path.dirname(os.path.abspath(audit_path)) or ".",exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(event_path)) or ".",exist_ok=True)

    fields=None; rows_written=0; events_written=0; processed=0; skipped=0
    first_ts=None; last_ts=None; start_wall=time.perf_counter()
    last_event_count=0
    event_rows=[]
    with open(audit_path,"w",newline="",encoding="utf-8") as af, open(event_path,"w",encoding="utf-8") as ef, open(path,"r",encoding="utf-8") as f:
        writer=None
        for line in f:
            try:
                rec=json.loads(line)
                ts=float(rec.get("t") or rec.get("receive_ts") or 0)
                if start_ts is not None and ts<start_ts: continue
                if end_ts is not None and ts>end_ts: continue
                bids=_replay_levels(rec.get("bids") or rec.get("raw_bids"),"BID")
                asks=_replay_levels(rec.get("asks") or rec.get("raw_asks"),"ASK")
                spot=float(rec.get("spot") or 0)
                if len(bids)<1 or len(asks)<1 or spot<=0:
                    skipped+=1; continue
                # Use the recording's symbol, but never change the analytical
                # instrument based on dashboard toggles.
                sym="BANKNIFTY" if str(rec.get("symbol","NIFTY")).upper().startswith("BANK") else "NIFTY"
                S["sym"]=sym; S["sym_str"]=rec.get("symbol_fyers") or (BNF_SYM if sym=="BANKNIFTY" else NF_SYM)
                tbq=int(rec.get("tbq") or 0); tsq=int(rec.get("tsq") or 0)
                trade=_replay_trade(rec); fm=_replay_feed_meta(rec)
                t0=time.perf_counter()
                push_update(bids,asks,spot,"REPLAY",max(len(bids),len(asks)),tbq,tsq,trade=trade,feed_meta=fm)
                elapsed=(time.perf_counter()-t0)*1000
                processed+=1
                first_ts=ts if first_ts is None else first_ts; last_ts=ts
                path_events=list(PATH_ENGINE[sym].events)
                if len(path_events)>last_event_count:
                    new=path_events[last_event_count:]
                    for ev in new:
                        ev2=dict(ev); ev2["replay_tick"]=S.get("tick_count",0); ev2["replay_spot"]=spot
                        ef.write(json.dumps(ev2,separators=(",",":"),ensure_ascii=False)+"\n")
                        event_rows.append(ev2); events_written+=1
                    last_event_count=len(path_events)
                snap=_replay_snapshot(rec,event_rows[-1:] if event_rows and event_rows[-1].get("replay_tick")==S.get("tick_count",0) else [],elapsed)
                if writer is None:
                    fields=list(snap[0].keys()); writer=csv.DictWriter(af,fieldnames=fields); writer.writeheader()
                for r in snap: writer.writerow(r); rows_written+=1
                if progress_every and processed%progress_every==0:
                    print("[REPLAY] {:,} observations | {:.1f}s | {:.0f} obs/s | events {:,}".format(processed,time.perf_counter()-start_wall,processed/max(0.001,time.perf_counter()-start_wall),events_written),flush=True)
            except Exception as e:
                skipped+=1
                if skipped<=10: print("[REPLAY] skipped line: {}".format(e),flush=True)
    runtime=time.perf_counter()-start_wall
    result={
        "engine":"MARKETOS v12.3.3 deterministic replay",
        "source":os.path.abspath(path),"observations_processed":processed,
        "observations_skipped":skipped,"audit_rows":rows_written,"events":events_written,
        "first_event_ts":first_ts,"last_event_ts":last_ts,"runtime_seconds":round(runtime,3),
        "observations_per_second":round(processed/max(0.001,runtime),2),
        "audit_csv":os.path.abspath(audit_path),"event_jsonl":os.path.abspath(event_path),
        "flow_quality":FLOW_QUALITY,
        "path_event_counts":{},
    }
    for ev in event_rows: result["path_event_counts"][ev.get("event","UNKNOWN")]=result["path_event_counts"].get(ev.get("event","UNKNOWN"),0)+1
    summary_path=os.path.splitext(audit_path)[0]+"_summary.json"
    with open(summary_path,"w",encoding="utf-8") as sf: json.dump(result,sf,indent=2)
    result["summary_json"]=os.path.abspath(summary_path)
    print("[REPLAY COMPLETE] {:,} observations in {:.2f}s ({:.0f} obs/s)".format(processed,runtime,processed/max(.001,runtime)))
    print("[REPLAY OUTPUT] audit: {}".format(os.path.abspath(audit_path)))
    print("[REPLAY OUTPUT] events: {}".format(os.path.abspath(event_path)))
    print("[REPLAY OUTPUT] summary: {}".format(os.path.abspath(summary_path)))
    return result


def _run_replay_cli():
    import argparse
    ap=argparse.ArgumentParser(description="MARKETOS v12.3.3 deterministic replay engine")
    ap.add_argument("--replay",metavar="JSONL",help="Replay a MarketOS truth JSONL file")
    ap.add_argument("--audit",default=None,help="Audit CSV output path")
    ap.add_argument("--events",default=None,help="Path-event JSONL output path")
    ap.add_argument("--start",type=float,default=None,help="Start epoch seconds (inclusive)")
    ap.add_argument("--end",type=float,default=None,help="End epoch seconds (inclusive)")
    ap.add_argument("--progress",type=int,default=5000,help="Progress interval")
    args=ap.parse_args()
    if not args.replay: return False
    print("\n"+"="*70)
    print("  MARKETOS v12.3.3 | DETERMINISTIC REPLAY-ONLY ENGINE")
    print("="*70)
    print("  Network/Auth: OFF")
    print("  Dashboard:    OFF")
    print("  Persistence:  OFF during replay")
    print("  Analytical state machines: SAME production push_update() path")
    print("  Source: {}".format(os.path.abspath(args.replay)))
    replay_file(args.replay,args.audit,args.events,args.start,args.end,args.progress)
    return True


if __name__=="__main__":
    if _run_replay_cli():
        raise SystemExit(0)
    print("\n"+"="*60)
    print("  MARKETOS v12.3 | LIVE DECISION + LIQUIDITY PATH / INTERACTION")
    print("  50-Level TBT WebSocket Edition")
    print("="*60)
    print("  NIFTY    : "+NF_SYM)
    print("  BANKNIFTY: "+BNF_SYM)
    print("  TBT URL  : "+TBT_URL)
    print("  Port     : "+str(PORT))
    print("  Raw TBT  : "+("DIAGNOSTIC ON" if RAW_TBT_ENABLED else "OFF (supported SDK path)"))
    print("  Recorder : "+("ON -> "+(RECORD_FILE or _truth_path()) if TRUTH_RECORD_ENABLED else "OFF"))
    print("  Decision : provisional decisioning + continuous empirical validation; no fabricated CVD")
    print("  Edge rule: validated setups require sufficient forward evidence; provisional setups are labelled")
    print("  Map rule : 500pt NIFTY / 1000pt BANKNIFTY are search envelopes, not guaranteed coverage")

    try:
        _load_runtime_fyers_credentials()
        validate_fyers_credentials()
        access_token = get_token()
        print("  Access token: OK")
    except Exception as e:
        print("\n[FYERS AUTH] STARTUP FAILED — authentication was not continued.")
        print("  " + str(e))
        print("\n  Set these Windows environment variables in the SAME CMD session, or restart and use the startup prompts:")
        print("    set \"FYERS_CLIENT_ID=YOUR_REAL_FYERS_APP_ID\"")
        print("    set \"FYERS_SECRET_KEY=YOUR_REAL_FYERS_APP_SECRET\"")
        print("  The secret is intentionally never printed by MarketOS.")
        print("\n[FYERS AUTH] FULL TRACEBACK:")
        try:
            traceback.print_exc()
        except Exception as trace_err:
            print("  Could not print traceback: " + str(trace_err))
        try:
            with open("marketos_startup_error.log", "a", encoding="utf-8") as ef:
                ef.write("\n" + "=" * 80 + "\nFYERS AUTH STARTUP FAILURE\n")
                ef.write(traceback.format_exc())
        except Exception:
            pass
        try:
            input("\nPress ENTER to close...")
        except (EOFError, KeyboardInterrupt):
            pass
        raise SystemExit(2)

    if access_token:
        full_token = CLIENT_ID+":"+access_token
        try:
            from fyers_apiv3 import fyersModel as _fmodel
            _cli = _fmodel.FyersModel(client_id=CLIENT_ID, token=access_token, is_async=False, log_path="")
            seed_previous_ohlc(_cli, NF_SYM)
            SESSION.save()
        except Exception as _se:
            print("[RAILS] seed skipped:", str(_se)[:40])
        print("\n  Starting TBT WebSocket (50-level)...")
        print("  Fallback chain: TBT → FyersDataSocket → REST polling")
        try:
            start_tbt(full_token)
            time.sleep(3)
        except Exception as tbt_err:
            print("\n[TBT STARTUP ERROR] " + str(tbt_err))
            try:
                traceback.print_exc()
            except Exception:
                pass
            try:
                with open("marketos_startup_error.log", "a", encoding="utf-8") as ef:
                    ef.write("\n" + "=" * 80 + "\nTBT STARTUP FAILURE\n")
                    ef.write(traceback.format_exc())
            except Exception:
                pass
            print("[TBT] Dashboard will remain available for diagnostics.")
    else:
        print("\n  No credentials — dashboard in standby mode.\n")

    url="http://localhost:{}".format(PORT)
    print("  Dashboard: "+url)
    print("  Toggle:    [NIFTY FUT] / [BANKNIFTY FUT]")
    print("  Stop:      Ctrl+C\n")
    threading.Thread(target=lambda:(time.sleep(2),webbrowser.open(url)),daemon=True).start()
    try:
        ThreadingHTTPServer(("localhost", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as server_err:
        print("\n[DASHBOARD] SERVER ERROR: " + str(server_err))
        try:
            traceback.print_exc()
        except Exception:
            pass
        try:
            with open("marketos_startup_error.log", "a", encoding="utf-8") as ef:
                ef.write("\n" + "=" * 80 + "\nDASHBOARD SERVER FAILURE\n")
                ef.write(traceback.format_exc())
        except Exception:
            pass
    finally:
        try:
            input("\nPress ENTER to close...")
        except (EOFError, KeyboardInterrupt):
            pass
