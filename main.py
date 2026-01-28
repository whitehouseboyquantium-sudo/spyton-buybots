
import os
import json
import time
import asyncio
import base64
import re
import threading
import requests
from urllib.parse import urlparse, parse_qs
from typing import Any, Dict, Optional, List, Tuple

from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ============================================================
# SpyTON Detector
# - STON.fi buy detection via STON exported events feed (unchanged)
# - DeDust buy detection via TONAPI pool transactions polling (separate job)
# - Memepad "connect":
#     • accept GasPump/Stonks/Blum links in /addtoken
#     • store WATCH entries if not listed yet
#     • auto-activate when it hits DEX (STON/DeDust)
# - NEW: Blum early posting (approve once -> auto)
#     • /watchlist
#     • /approve <WATCH_ID>
#     • /setaddr <WATCH_ID or blum:slug> <JETTON_ADDRESS>
#
# Leaderboard:
# - 6H movers (DexScreener priceChange.h6) + clickable TG + no preview
# ============================================================

# -------------------- ENV --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
# MASTER SpyTON channel (hard-coded)
MASTER_CHANNEL_ID = -1002379265999
# CHANNEL_ID is kept for backward compatibility but master always receives posts.
CHANNEL_ID = int(os.getenv("CHANNEL_ID", str(MASTER_CHANNEL_ID)))  # -100xxxxxxxxxx
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

TON_PRICE_API = os.getenv("TON_PRICE_API", "")  # optional (coingecko-style)
TRENDING_URL = os.getenv("TRENDING_URL", "https://t.me/SpyTonTrending")
LISTING_URL = os.getenv("LISTING_URL", "https://t.me/TonProjectListing")
# Book Trending link (button)
# Accept either BOOK_TRENDING_URL or BOOK_TRENDING_LINK from Secrets.
# Force-correct any old/typo variants so the button ALWAYS points to:
#   https://t.me/SpyTONTrndBot
BOOK_TRENDING_URL = (
    os.getenv("BOOK_TRENDING_URL")
    or os.getenv("BOOK_TRENDING_LINK")
    or "https://t.me/SpyTONTrndBot"
).strip()

# Normalize common wrong variants (case, trailing slash, etc.)
_bt = BOOK_TRENDING_URL.strip().rstrip("/")
_bt_low = _bt.lower()
if "spytontrendbot" in _bt_low or "spytontrndbot" in _bt_low:
    BOOK_TRENDING_URL = "https://t.me/SpyTONTrndBot"
else:
    BOOK_TRENDING_URL = _bt

HEADER_IMAGE_PATH = os.getenv("HEADER_IMAGE_PATH", "header.png")

# -------------------- CUSTOM EMOJI (OPTIONAL) --------------------
# Put numeric Telegram custom_emoji_id values in Replit Secrets.
# If not set (or invalid), bot falls back to normal Unicode emojis.
SPY_CUSTOM_EMOJI_ID = os.getenv("SPY_CUSTOM_EMOJI_ID", "").strip()

ICON_SWAP_ID    = os.getenv("ICON_SWAP_ID", "").strip()
ICON_WALLET_ID  = os.getenv("ICON_WALLET_ID", "").strip()
ICON_TXN_ID     = os.getenv("ICON_TXN_ID", "").strip()
ICON_POS_ID     = os.getenv("ICON_POS_ID", "").strip()
ICON_HOLDERS_ID = os.getenv("ICON_HOLDERS_ID", "").strip()
ICON_MCAP_ID    = os.getenv("ICON_MCAP_ID", "").strip()
ICON_LIQ_ID     = os.getenv("ICON_LIQ_ID", "").strip()
ICON_PIN_ID     = os.getenv("ICON_PIN_ID", "").strip()
ICON_CHART_ID   = os.getenv("ICON_CHART_ID", "").strip()
ICON_TREND_ID   = os.getenv("ICON_TREND_ID", "").strip()
ICON_POOLS_ID   = os.getenv("ICON_POOLS_ID", "").strip()


TONAPI_KEY = os.getenv("TONAPI_KEY", "")
TONAPI_BASE = os.getenv("TONAPI_BASE", "https://tonapi.io")

DEDUST_ENABLED = os.getenv("DEDUST_ENABLED", "1") == "1"
DEDUST_POLL_LIMIT = int(os.getenv("DEDUST_POLL_LIMIT", "50"))
DEDUST_DEBUG = os.getenv("DEDUST_DEBUG", "0") == "1"
DEDUST_API_BASE = os.getenv("DEDUST_API_BASE", "https://api.dedust.io").rstrip("/")

# Poll intervals (seconds)
STON_POLL_INTERVAL = int(os.getenv("STON_POLL_INTERVAL", "2"))
DEDUST_POLL_INTERVAL = int(os.getenv("DEDUST_POLL_INTERVAL", "3"))
LB_UPDATE_INTERVAL = int(os.getenv("LB_UPDATE_INTERVAL", "60"))
AUTO_RANK_INTERVAL = int(os.getenv("AUTO_RANK_INTERVAL", "30"))

# Memepad auto-activation
MEMEPAD_ACTIVATION_ENABLED = os.getenv("MEMEPAD_ACTIVATION_ENABLED", "1") == "1"
MEMEPAD_ACTIVATION_INTERVAL = int(os.getenv("MEMEPAD_ACTIVATION_INTERVAL", "45"))  # seconds

# -------------------- BLUM EARLY MODE (NEW) --------------------
# If a WATCH entry is source=blum AND approved_early=True AND token_address is set,
# bot will post early buys by scanning TONAPI transactions on the jetton master.
BLUM_EARLY_ENABLED = os.getenv("BLUM_EARLY_ENABLED", "1") == "1"
BLUM_POLL_LIMIT = int(os.getenv("BLUM_POLL_LIMIT", "12"))  # txs pulled per jetton per poll
BLUM_POLL_INTERVAL = int(os.getenv("BLUM_POLL_INTERVAL", "14"))  # seconds
BLUM_DEBUG = os.getenv("BLUM_DEBUG", "0") == "1"

# -------------------- LEADERBOARD FILTERS / MODES --------------------
LB_MIN_LIQ_USD = float(os.getenv("LB_MIN_LIQ_USD", "0"))
LB_MIN_MC_USD = float(os.getenv("LB_MIN_MC_USD", "0"))
LB_WHALE_MC_USD = float(os.getenv("LB_WHALE_MC_USD", "1000000"))
LB_SPLIT_SECTIONS = os.getenv("LB_SPLIT_SECTIONS", "1") == "1"
LB_SHOW_WHALES = os.getenv("LB_SHOW_WHALES", "1") == "1"
LB_MAX_GAINERS = int(os.getenv("LB_MAX_GAINERS", "10"))
LB_MAX_LOSERS = int(os.getenv("LB_MAX_LOSERS", "10"))
LB_MAX_WHALES = int(os.getenv("LB_MAX_WHALES", "10"))

# -------------------- SPEED / POSTING --------------------
# FAST_POST_MODE posts immediately with minimal info, then edits the message
# later with MarketCap/Holders/Liquidity/etc.
FAST_POST_MODE = os.getenv("FAST_POST_MODE", "1") == "1"
# In FAST_POST_MODE, these expensive lookups are moved to the background.
FAST_STATS_TIMEOUT = float(os.getenv("FAST_STATS_TIMEOUT", "3"))
FAST_HOLDERS_ENABLED = os.getenv("FAST_HOLDERS_ENABLED", "0") == "1"  # default off (slow)

# -------------------- STON API --------------------
STON_BASE = "https://api.ston.fi"
LATEST_BLOCK_URL = f"{STON_BASE}/export/dexscreener/v1/latest-block"
EVENTS_URL = f"{STON_BASE}/export/dexscreener/v1/events"
STON_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# -------------------- DEXSCREENER --------------------
DEX_PAIR_URL = "https://api.dexscreener.com/latest/dex/pairs/ton"
DEX_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens"

# -------------------- FILES --------------------
DATA_FILE = "data.json"
STATE_FILE = "state.json"

# -------------------- RUNTIME --------------------
LAST_HTTP_INFO: str = "No requests yet"
LAST_EVENTS_COUNT: int = 0

SEEN_TX_STON: Dict[str, float] = {}
SEEN_TX_DEDUST: Dict[str, float] = {}
SEEN_TX_BLUM: Dict[str, float] = {}
SEEN_TTL_SECONDS = 3600

PAIR_CACHE: Dict[str, Dict[str, Any]] = {}
PAIR_CACHE_TTL = 30

DATA: Dict[str, Any] = {"pairs": {}, "watch": {}}
STATE: Dict[str, Any] = {
    "leaderboard_msg_id": None,
    "ston_last_block": None,
    "dedust_last_id": {},   # { pool_address: last_trade_id }
    "dedust_last_lt": {},   # legacy (unused)
    "blum_last_lt": {},     # { jetton_master: last_lt_int }  (NEW)
}

# ===================== UPTIMEROBOT WEB SERVER =====================
app_web = Flask(__name__)

@app_web.get("/")
def home():
    return "SpyTON detector alive", 200

@app_web.get("/uptimerobot")
def uptimerobot():
    return "ok", 200

@app_web.get("/health")
def health():
    return "healthy", 200

def run_web():
    port = int(os.getenv("PORT", "8080"))
    app_web.run(host="0.0.0.0", port=port, debug=False)

# --- Start web keep-alive immediately (Replit .replit.app) ---
_WEB_STARTED = False

def start_web_server_once():
    global _WEB_STARTED
    if _WEB_STARTED:
        return
    try:
        t = threading.Thread(target=run_web, daemon=True)
        t.start()
        _WEB_STARTED = True
    except Exception as e:
        # Don't crash the bot if web server fails
        print("⚠️ Failed to start keep-alive web server:", e)

start_web_server_once()

# --- Optional self-ping keep-warm loop ---
_PING_STARTED = False

def start_self_ping_once():
    global _PING_STARTED
    if _PING_STARTED:
        return
    public_url = os.getenv("PUBLIC_URL", "").strip()
    if not public_url:
        return

    def _loop():
        base = public_url.rstrip("/")
        urls = [base + "/uptimerobot", base + "/health", base + "/"]
        while True:
            for u in urls:
                try:
                    requests.get(u, timeout=10)
                except Exception:
                    pass
            time.sleep(240)

    try:
        threading.Thread(target=_loop, daemon=True).start()
        _PING_STARTED = True
    except Exception as e:
        print("⚠️ Failed to start self-ping loop:", e)

start_self_ping_once()


# ===================== ASYNC HELPERS =====================
async def _to_thread(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


# ===================== UTIL =====================
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

async def is_chat_admin(context, chat_id: int, user_id: int) -> bool:
    """Return True if user is admin/creator in the given chat."""
    try:
        m = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        status = getattr(m, "status", "")
        return status in ("administrator", "creator")
    except Exception:
        return False

def safe_float(x: Any) -> float:
    try:
        if isinstance(x, str):
            x = x.strip()
        return float(x)
    except:
        return 0.0

def safe_int(x: Any) -> Optional[int]:
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return int(x)
        if isinstance(x, str):
            x = x.strip()
            if not x:
                return None
            return int(float(x))
        return None
    except:
        return None

def short(addr: str) -> str:
    if not addr:
        return "Unknown"
    return addr[:4] + "…" + addr[-4:]


def _to_hex_tx_hash(h: str) -> str:
    """Normalize TON tx hash into 64-char hex if possible.

    Accepts:
      - 64-char hex (optionally prefixed with 0x)
      - base64 / base64url (with or without padding) representing 32 bytes
    Returns empty string if cannot parse.
    """
    if h is None:
        return ""
    # Sometimes callers pass dicts/lists by mistake
    if isinstance(h, dict):
        h = h.get("hash") or h.get("tx_hash") or h.get("txHash") or ""
    if isinstance(h, (list, tuple)) and all(isinstance(x, int) for x in h):
        try:
            b = bytes(h)
            if len(b) == 32:
                return b.hex()
        except Exception:
            return ""
    h = str(h).strip()
    if not h:
        return ""
    if h.startswith("0x") and len(h) == 66:
        h = h[2:]

    if re.fullmatch(r"[0-9a-fA-F]{64}", h):
        return h.lower()

    # Try base64url/base64 decode -> 32 bytes -> hex
    try:
        s = h.replace("-", "+").replace("_", "/")
        pad = "=" * ((4 - (len(s) % 4)) % 4)
        raw = base64.b64decode(s + pad)
        if len(raw) == 32:
            return raw.hex()
    except Exception:
        pass

    return ""
    h = str(h).strip()
    if re.fullmatch(r"[0-9a-fA-F]{64}", h):
        return h.lower()

    # Try base64url/base64 decode -> 32 bytes -> hex
    try:
        s = h.replace("-", "+").replace("_", "/")
        pad = "=" * ((4 - (len(s) % 4)) % 4)
        raw = base64.b64decode(s + pad)
        if len(raw) == 32:
            return raw.hex()
    except Exception:
        pass

    return ""

def make_tx_url(tx_hash: str, fallback_url: str = "") -> str:
    """Return a working explorer link for the given tx hash.

    Prefer Tonviewer (works with tx hash only). If hash cannot be normalized,
    return fallback_url (or empty string).
    """
    hx = _to_hex_tx_hash(tx_hash)
    if hx:
        return f"https://tonviewer.com/transaction/{hx}"
    return (fallback_url or "").strip()

def _get_any(t: Dict[str, Any], keys: List[str], default: str = "") -> str:
    """Safely get the first present value from a dict, supporting common nesting."""
    if not isinstance(t, dict):
        return default
    for k in keys:
        v = t.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)) and v:
            return str(v)
        if isinstance(v, dict):
            hv = v.get("hash") or v.get("tx_hash") or v.get("txHash") or v.get("transactionHash")
            if isinstance(hv, str) and hv.strip():
                return hv.strip()
    return default

def _trade_cursor_id(t: Dict[str, Any]) -> str:
    """Prefer stable trade id/lt for cursor to avoid missing same-second trades."""
    return _get_any(t, ["id", "trade_id", "tradeId", "event_id", "eventId", "seqno", "tx_lt", "lt"], "") or _get_any(
        t, ["txHash", "tx_hash", "hash", "transactionHash"], ""
    )

def _trade_tx_hash(t: Dict[str, Any]) -> str:
    """Extract tx hash from various DeDust trade shapes."""
    if isinstance(t, dict) and isinstance(t.get("transaction"), dict):
        hv = _get_any(t["transaction"], ["hash", "tx_hash", "txHash", "transactionHash"], "")
        if hv:
            return hv
    return _get_any(t, ["tx_hash", "txHash", "hash", "transactionHash", "txHashHex", "tx_hash_hex", "txhash", "txHash64"], "")
def file_exists(path: str) -> bool:
    try:
        return os.path.isfile(path)
    except:
        return False

def money_fmt(x: Optional[float]) -> str:
    if x is None:
        return "—"
    try:
        x = float(x)
    except:
        return "—"
    if x >= 1_000_000_000:
        return f"${x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.2f}K"
    return f"${x:,.0f}"

def _atomic_write(path: str, data: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
    os.replace(tmp, path)

def load_data():
    global DATA
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            DATA = json.load(f)
        if not isinstance(DATA, dict):
            DATA = {"pairs": {}, "watch": {}, "forced_ranks": {}}
        DATA.setdefault("pairs", {})
        DATA.setdefault("watch", {})
        DATA.setdefault("forced_ranks", {})
        if not isinstance(DATA["forced_ranks"], dict):
            DATA["forced_ranks"] = {}
        if not isinstance(DATA["pairs"], dict):
            DATA["pairs"] = {}
        if not isinstance(DATA["watch"], dict):
            DATA["watch"] = {}
        if not isinstance(DATA.get("group_mirrors"), dict):
            DATA["group_mirrors"] = {}
    except:
        DATA = {"pairs": {}, "watch": {}, "forced_ranks": {}, "group_mirrors": {}}

def save_data():
    _atomic_write(DATA_FILE, json.dumps(DATA, ensure_ascii=False, indent=2))

def load_state():
    global STATE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
        if isinstance(s, dict):
            STATE.update(s)
        STATE.setdefault("dedust_last_id", {})
        STATE.setdefault("dedust_last_lt", {})
        STATE.setdefault("blum_last_lt", {})
        if not isinstance(STATE["dedust_last_id"], dict):
            STATE["dedust_last_id"] = {}
        if not isinstance(STATE["dedust_last_lt"], dict):
            STATE["dedust_last_lt"] = {}
        if not isinstance(STATE["blum_last_lt"], dict):
            STATE["blum_last_lt"] = {}
    except:
        STATE = {"leaderboard_msg_id": None, "ston_last_block": None, "dedust_last_id": {}, "dedust_last_lt": {}, "blum_last_lt": {}}

# Auto trend ranks (computed from 6H USD volume)
AUTO_RANKS: Dict[str, int] = {}
AUTO_RANK_TS = 0.0
AUTO_RANK_TTL = int(os.getenv("AUTO_RANK_TTL", "30"))  # seconds

def save_state():
    _atomic_write(STATE_FILE, json.dumps(STATE, ensure_ascii=False, indent=2))

def cleanup_seen():
    now = time.time()
    for cache in (SEEN_TX_STON, SEEN_TX_DEDUST, SEEN_TX_BLUM):
        old = [k for k, ts in cache.items() if now - ts > SEEN_TTL_SECONDS]
        for k in old:
            cache.pop(k, None)

def buy_badge(ton_amt: float) -> str:
    if ton_amt >= 50:
        return "🐳"
    if ton_amt >= 10:
        return "🐟"
    if ton_amt >= 2:
        return "🦐"
    return "🌱"

def ton_price_usd() -> float:
    if not TON_PRICE_API:
        return 0.0
    try:
        r = requests.get(TON_PRICE_API, timeout=10).json()
        return float(r["the-open-network"]["usd"])
    except:
        return 0.0

# Simple cache so we don't block every buy on an external price call
_TON_PRICE_CACHE: Dict[str, float] = {"v": 0.0, "ts": 0.0}

def refresh_ton_price_cache() -> float:
    """Blocking refresh (call in a thread)."""
    now = time.time()
    v = ton_price_usd()
    _TON_PRICE_CACHE["v"] = float(v or 0.0)
    _TON_PRICE_CACHE["ts"] = now
    return float(v or 0.0)

def ton_price_cache_value() -> float:
    """Non-blocking getter used during buy posting."""
    try:
        return float(_TON_PRICE_CACHE.get("v", 0.0))
    except Exception:
        return 0.0

def to_ton_from_nano(nano: Any) -> float:
    try:
        v = int(str(nano))
        return v / 1e9
    except:
        return 0.0

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    if u.startswith("@"):
        return "https://t.me/" + u[1:]
    return u

# ===================== UI / BUTTONS =====================
def book_trending_only_button():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Book Trending", url=BOOK_TRENDING_URL)]
    ])


def buy_alert_keyboard(chart_url: str, pools_url: str) -> InlineKeyboardMarkup:
    # ✅ User requested: Chart/Trending/Pools should be TEXT LINKS inside the message, not buttons.
    # Keep only the Book Trending button.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Book Trending", url=BOOK_TRENDING_URL)]
    ])

def leaderboard_button():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("TON LISTING ↗", url="https://t.me/TonProjectListing")]
    ])

# ===================== MEMEPAD PARSER =====================
TON_ADDR_RE = re.compile(r"\b(EQ|UQ)[A-Za-z0-9_-]{40,}\b")

def _b64url_decode(s: str) -> bytes:
    s = (s or "").strip()
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode())

def parse_memepad_input(text: str) -> Dict[str, Any]:
    t = (text or "").strip()

    m = TON_ADDR_RE.search(t)
    if m:
        return {"source": "unknown", "token_address": m.group(0), "blum_slug": None, "raw": t}

    try:
        u = urlparse(t)
        qs = parse_qs(u.query)
        host = (u.netloc or "").lower()
        path = (u.path or "").lower()

        # GasPump
        if "t.me" in host and "gaspump_bot" in path and "startapp" in qs:
            payload = qs["startapp"][0]
            try:
                data = json.loads(_b64url_decode(payload).decode("utf-8", errors="ignore"))
                addr = data.get("token_address") or data.get("tokenAddress")
                if isinstance(addr, str) and TON_ADDR_RE.match(addr):
                    return {"source": "gaspump", "token_address": addr, "blum_slug": None, "raw": t}
            except Exception:
                pass
            try:
                decoded = _b64url_decode(payload).decode("utf-8", errors="ignore")
                m2 = TON_ADDR_RE.search(decoded)
                if m2:
                    return {"source": "gaspump", "token_address": m2.group(0), "blum_slug": None, "raw": t}
            except Exception:
                pass
            return {"source": "gaspump", "token_address": None, "blum_slug": None, "raw": t}

        # Stonks sniper bot
        if "t.me" in host and "stonks_sniper_bot" in path:
            m3 = TON_ADDR_RE.search(t)
            return {"source": "stonks", "token_address": (m3.group(0) if m3 else None), "blum_slug": None, "raw": t}

        # Blum memepad links:
        # https://t.me/blum/app?startapp=memepadjetton_SYMBOL_xxx-ref_yyy
        # https://t.me/blum/start?startapp=memepadjetton_...
        if "t.me" in host and "/blum/" in f"/{path}/" and "startapp" in qs:
            startapp = qs["startapp"][0]
            if isinstance(startapp, str) and startapp.startswith("memepadjetton_"):
                base_part = startapp.split("-ref", 1)[0]  # keep slug, strip ref
                return {"source": "blum", "token_address": None, "blum_slug": base_part, "raw": t}
            return {"source": "blum", "token_address": None, "blum_slug": None, "raw": t}

    except Exception:
        pass

    return {"source": "unknown", "token_address": None, "blum_slug": None, "raw": t}

# ===================== COMMAND TEXT PARSER (FIXES BLUM "NO RESPONSE") =====================
def parse_addtoken_message_text(msg_text: str) -> Optional[Tuple[str, str, Optional[str]]]:
    """
    Robust parser for /addtoken when Telegram preview/newlines break context.args.
    Returns (raw_input, symbol, telegram_link?)
    """
    if not msg_text:
        return None
    t = msg_text.strip()

    # Remove leading command (/addtoken@botname also)
    # Keep everything after first space or newline.
    m = re.match(r"^/addtoken(?:@\w+)?(?:\s+|\n+)(.+)$", t, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    rest = m.group(1).strip()

    # Normalize whitespace
    parts = re.split(r"\s+", rest)
    parts = [p for p in parts if p.strip()]

    if len(parts) < 2:
        return None

    raw_input = parts[0].strip()
    symbol = parts[1].strip().upper()

    tg = parts[2].strip() if len(parts) >= 3 else None
    tg = normalize_url(tg) if tg else None

    return raw_input, symbol, tg

# ===================== STON API =====================
def ston_latest_block() -> Optional[int]:
    global LAST_HTTP_INFO
    try:
        res = requests.get(LATEST_BLOCK_URL, headers=STON_HEADERS, timeout=12)
        LAST_HTTP_INFO = f"latest-block status={res.status_code}"
        if res.status_code != 200:
            return None
        js = res.json()
        if isinstance(js, dict) and isinstance(js.get("block"), dict):
            return safe_int(js["block"].get("blockNumber"))
        return None
    except Exception as e:
        LAST_HTTP_INFO = f"latest-block error={type(e).__name__}: {e}"
        return None

def ston_events(from_block: int, to_block: int) -> List[Dict[str, Any]]:
    global LAST_HTTP_INFO, LAST_EVENTS_COUNT
    params = {"fromBlock": from_block, "toBlock": to_block}
    try:
        res = requests.get(EVENTS_URL, params=params, headers=STON_HEADERS, timeout=20)
        LAST_HTTP_INFO = f"events status={res.status_code} params={params}"
        if res.status_code != 200:
            LAST_EVENTS_COUNT = 0
            return []
        js = res.json()
        evs: List[Dict[str, Any]] = []
        if isinstance(js, list):
            evs = [x for x in js if isinstance(x, dict)]
        elif isinstance(js, dict) and isinstance(js.get("events"), list):
            evs = [x for x in js["events"] if isinstance(x, dict)]
        LAST_EVENTS_COUNT = len(evs)
        return evs
    except Exception as e:
        LAST_HTTP_INFO = f"events error={type(e).__name__}: {e}"
        LAST_EVENTS_COUNT = 0
        return []

# ===================== DEXSCREENER HELPERS =====================
def fetch_pair_stats(pair_id: str) -> Dict[str, Any]:
    now = time.time()
    cached = PAIR_CACHE.get(pair_id)
    if cached and (now - cached.get("_ts", 0) < PAIR_CACHE_TTL):
        return cached

    out = {"liquidity_usd": None, "marketcap_usd": None, "volume_h6_usd": None, "_ts": now}
    url = f"{DEX_PAIR_URL}/{pair_id}"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code != 200:
            PAIR_CACHE[pair_id] = out
            return out

        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list) or not pairs or not isinstance(pairs[0], dict):
            PAIR_CACHE[pair_id] = out
            return out

        p0 = pairs[0]
        liq = p0.get("liquidity", {})
        if isinstance(liq, dict):
            v = safe_float(liq.get("usd"))
            out["liquidity_usd"] = v if v > 0 else None

        mc_val = safe_float(p0.get("marketCap"))
        fdv_val = safe_float(p0.get("fdv"))
        out["marketcap_usd"] = mc_val if mc_val > 0 else (fdv_val if fdv_val > 0 else None)

        # 6H volume (USD)
        vol = p0.get("volume")
        if isinstance(vol, dict):
            vh6 = vol.get("h6")
            if isinstance(vh6, dict):
                out["volume_h6_usd"] = safe_float(vh6.get("usd"))

    except:
        pass

    PAIR_CACHE[pair_id] = out
    return out



# ===================== TOKEN STATS FALLBACK =====================
TOKEN_STATS_CACHE: Dict[str, Dict[str, Any]] = {}

def fetch_token_stats(token_addr: str) -> Dict[str, Any]:
    """Fallback stats using DexScreener token endpoint.

    Returns liquidity_usd and marketcap_usd derived from the best TON pair for this token.
    Used when pair endpoint returns missing metrics (common for some pools / v2 / wrappers).
    """
    now = time.time()
    cached = TOKEN_STATS_CACHE.get(token_addr)
    if cached and (now - cached.get("_ts", 0) < PAIR_CACHE_TTL):
        return cached

    out = {"liquidity_usd": None, "marketcap_usd": None, "price_usd": None, "_ts": now}
    try:
        url = f"{DEX_TOKEN_URL}/{token_addr}"
        res = requests.get(url, timeout=15)
        if res.status_code != 200:
            TOKEN_STATS_CACHE[token_addr] = out
            return out
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list) or not pairs:
            TOKEN_STATS_CACHE[token_addr] = out
            return out

        best = None
        best_liq = 0.0
        for p in pairs:
            if not isinstance(p, dict):
                continue
            if (p.get("chainId") or "").lower() != "ton":
                continue
            liq = p.get("liquidity") or {}
            liq_usd = safe_float(liq.get("usd")) if isinstance(liq, dict) else 0.0
            if liq_usd > best_liq:
                best_liq = liq_usd
                best = p

        if best:
            out["liquidity_usd"] = best_liq if best_liq > 0 else None
            mc_val = safe_float(best.get("marketCap"))
            fdv_val = safe_float(best.get("fdv"))
            out["marketcap_usd"] = mc_val if mc_val > 0 else (fdv_val if fdv_val > 0 else None)
            price_val = safe_float(best.get("priceUsd"))
            out["price_usd"] = price_val if price_val > 0 else None

    except:
        pass

    TOKEN_STATS_CACHE[token_addr] = out
    return out

# ===================== PAIR META (TON LEG) =====================
PAIR_META_CACHE: Dict[str, Dict[str, Any]] = {}

def fetch_pair_meta(pair_id: str) -> Dict[str, Any]:
    """Fetch base/quote symbols from DexScreener pair endpoint."""
    now = time.time()
    cached = PAIR_META_CACHE.get(pair_id)
    if cached and (now - cached.get("_ts", 0) < PAIR_CACHE_TTL):
        return cached
    out = {"base_sym": None, "quote_sym": None, "dex_id": None, "_ts": now}
    try:
        url = f"{DEX_PAIR_URL}/{pair_id}"
        res = requests.get(url, timeout=15)
        if res.status_code != 200:
            PAIR_META_CACHE[pair_id] = out
            return out
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list) or not pairs or not isinstance(pairs[0], dict):
            PAIR_META_CACHE[pair_id] = out
            return out
        p0 = pairs[0]
        base = p0.get("baseToken") or {}
        quote = p0.get("quoteToken") or {}
        out["base_sym"] = (base.get("symbol") or "").upper() or None
        out["quote_sym"] = (quote.get("symbol") or "").upper() or None
        out["dex_id"] = (p0.get("dexId") or "") or None
    except:
        pass
    PAIR_META_CACHE[pair_id] = out
    return out


def dex_label_from_dex_id(dex_id: str) -> str:
    """Human label used inside the message title."""
    d = (dex_id or "").lower()
    if "dedust" in d:
        return "DeDust"
    if "ston" in d:
        if "v2" in d or "stonfi2" in d or "stonfi-v2" in d:
            return "Stonfi v2"
        return "STON.fi"
    return "DEX"

def ensure_pair_ton_leg(pair_id: str) -> Optional[int]:
    """Store which token leg is TON for STON events: 0=base(amount0), 1=quote(amount1)."""
    rec = DATA.get("pairs", {}).get(pair_id)
    if not isinstance(rec, dict):
        return None
    ton_leg = rec.get("ton_leg")
    if ton_leg in (0, 1):
        return int(ton_leg)
    meta = fetch_pair_meta(pair_id)
    # Cache human DEX label for multi-dex title (STON.fi / Stonfi v2 / DeDust)
    if isinstance(meta, dict) and not rec.get("dex_label"):
        rec["dex_label"] = dex_label_from_dex_id(meta.get("dex_id") or "")
    base_sym = meta.get("base_sym")
    quote_sym = meta.get("quote_sym")
    if base_sym == "TON":
        ton_leg = 0
    elif quote_sym == "TON":
        ton_leg = 1
    else:
        ton_leg = None
    if ton_leg is not None:
        rec["ton_leg"] = ton_leg
        # persist quietly
        try:
            save_data()
        except:
            pass
    return ton_leg
def find_pair_for_token_on_dex(token_address: str, want_dex: str) -> Optional[str]:
    url = f"{DEX_TOKEN_URL}/{token_address}"
    try:
        res = requests.get(url, timeout=20)
        if res.status_code != 200:
            return None
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list):
            return None

        want = want_dex.lower()
        best_pair_id = None
        best_score = -1.0

        for p in pairs:
            if not isinstance(p, dict):
                continue
            dex_id = (p.get("dexId") or "").lower()
            chain_id = (p.get("chainId") or "").lower()
            if chain_id != "ton":
                continue

            if want == "stonfi" and "ston" not in dex_id:
                continue
            if want == "dedust" and "dedust" not in dex_id:
                continue

            base = p.get("baseToken") or {}
            quote = p.get("quoteToken") or {}
            base_sym = (base.get("symbol") or "").upper()
            quote_sym = (quote.get("symbol") or "").upper()

            if base_sym == "TON" or quote_sym == "TON":
                pair_id = (p.get("pairAddress") or p.get("pairId") or p.get("pair") or "").strip()
                if not pair_id:
                    u = (p.get("url") or "")
                    if "/ton/" in u:
                        pair_id = u.split("/ton/")[-1].split("?")[0].strip()
                if not pair_id:
                    continue

                # Choose "best" pool: prefer higher liquidity (USD) then volume (24h)
                liq = 0.0
                vol = 0.0
                try:
                    liq = float(((p.get("liquidity") or {}).get("usd") or 0) or 0)
                except:
                    liq = 0.0
                try:
                    vol = float(((p.get("volume") or {}).get("h24") or 0) or 0)
                except:
                    vol = 0.0
                score = liq * 1_000_000 + vol
                if score > best_score:
                    best_score = score
                    best_pair_id = pair_id

        return best_pair_id
    except:
        return None

def find_stonfi_ton_pair_for_token(token_address: str) -> Optional[str]:
    return find_pair_for_token_on_dex(token_address, "stonfi")

def find_dedust_ton_pair_for_token(token_address: str) -> Optional[str]:
    return find_pair_for_token_on_dex(token_address, "dedust")

def fetch_token_telegram_url_from_dexscreener(token_address: str) -> Optional[str]:
    if not token_address:
        return None
    url = f"{DEX_TOKEN_URL}/{token_address}"
    try:
        res = requests.get(url, timeout=20)
        if res.status_code != 200:
            return None
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list):
            return None

        for p in pairs:
            if not isinstance(p, dict):
                continue
            info = p.get("info") or {}
            if not isinstance(info, dict):
                continue
            socials = info.get("socials") or []
            if not isinstance(socials, list):
                continue

            for s in socials:
                if not isinstance(s, dict):
                    continue
                stype = (s.get("type") or "").lower()
                link = (s.get("url") or "").strip()
                if stype == "telegram" and link.startswith("http"):
                    return link
    except:
        pass
    return None

def fetch_pair_change(pair_id: str, tf: str = "h6") -> Optional[float]:
    try:
        url = f"{DEX_PAIR_URL}/{pair_id}"
        res = requests.get(url, timeout=15)
        if res.status_code != 200:
            return None
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list) or not pairs or not isinstance(pairs[0], dict):
            return None

        p0 = pairs[0]
        pc = p0.get("priceChange") or {}
        if not isinstance(pc, dict):
            return None

        v = pc.get(tf)
        if v is None:
            return None
        return float(v)
    except:
        return None

# ===================== TONAPI =====================
def tonapi_headers() -> Dict[str, str]:
    if not TONAPI_KEY:
        return {"Accept": "application/json"}
    return {"Authorization": f"Bearer {TONAPI_KEY}", "Accept": "application/json"}

def tonapi_get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        res = requests.get(url, headers=tonapi_headers(), params=params, timeout=20)
        if res.status_code == 401 and TONAPI_KEY:
            res = requests.get(url, headers={"X-API-Key": TONAPI_KEY, "Accept": "application/json"}, params=params, timeout=20)
        if res.status_code != 200:
            return None
        js = res.json()
        return js if isinstance(js, dict) else None
    except:
        return None


# ===================== Jetton meta cache (decimals) =====================
JETTON_DECIMALS_CACHE: Dict[str, int] = {}

def get_jetton_decimals(jetton_master: str) -> int:
    """Best-effort decimals lookup via TonAPI. Defaults to 9."""
    if not jetton_master:
        return 9
    if jetton_master in JETTON_DECIMALS_CACHE:
        return JETTON_DECIMALS_CACHE[jetton_master]
    dec = 9
    try:
        js = tonapi_get(f"{TONAPI_BASE.rstrip('/')}/v2/jettons/{jetton_master}")
        if isinstance(js, dict):
            md = js.get("metadata")
            if isinstance(md, dict):
                d = md.get("decimals")
                if isinstance(d, str) and d.isdigit():
                    dec = int(d)
                elif isinstance(d, int):
                    dec = d
    except:
        pass
    JETTON_DECIMALS_CACHE[jetton_master] = dec
    return dec


def dedust_fetch_trades(pool_addr: str, limit: int = 25) -> List[Dict[str, Any]]:
    """Fetch recent trades for a DeDust pool.
    Uses api.dedust.io (public). Response schema can change; parsing is tolerant.
    """
    if not pool_addr:
        return []
    url = f"{DEDUST_API_BASE}/v2/pools/{pool_addr}/trades"
    try:
        res = requests.get(url, params={"limit": limit}, timeout=20)
        if res.status_code != 200:
            return []
        js = res.json()
        if isinstance(js, list):
            return [t for t in js if isinstance(t, dict)]
        if isinstance(js, dict):
            arr = js.get("trades") or js.get("items") or js.get("data")
            if isinstance(arr, list):
                return [t for t in arr if isinstance(t, dict)]
    except:
        pass
    return []


def is_ton_asset(asset_obj: Any) -> bool:
    """Heuristic detection of native TON side in DeDust payloads.

    DeDust schemas vary by endpoint/version. We treat the asset as TON if:
    - type indicates native/ton
    - symbol/ticker is TON
    - it explicitly flags native
    """
    if not isinstance(asset_obj, dict):
        return False

    t = str(asset_obj.get("type") or asset_obj.get("kind") or "").lower()
    if t in ("native", "ton", "native_ton", "native-ton"):
        return True

    if asset_obj.get("is_native") is True or asset_obj.get("isNative") is True:
        return True

    sym = str(asset_obj.get("symbol") or asset_obj.get("ticker") or "").upper()
    if sym == "TON":
        return True

    # Some payloads nest metadata
    meta = asset_obj.get("meta")
    if isinstance(meta, dict):
        sym2 = str(meta.get("symbol") or meta.get("ticker") or "").upper()
        if sym2 == "TON":
            return True

    return False


def extract_jetton_master(asset_obj: Any) -> str:
    """Extract jetton master address from DeDust asset objects."""
    if not isinstance(asset_obj, dict):
        return ""

    # Common keys
    for k in ("address", "master", "master_address", "jetton_master", "jettonMaster"):
        v = asset_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Nested variations
    for k in ("jetton", "token", "contract", "meta"):
        sub = asset_obj.get(k)
        if isinstance(sub, dict):
            for kk in ("address", "master", "master_address", "jetton_master", "jettonMaster"):
                v = sub.get(kk)
                if isinstance(v, str) and v.strip():
                    return v.strip()

    return ""

def fetch_holders_count_tonapi(jetton_address: str) -> Optional[int]:
    if not TONAPI_KEY or not jetton_address:
        return None

    url = f"{TONAPI_BASE.rstrip('/')}/v2/jettons/{jetton_address}"
    js = tonapi_get(url)
    if not js:
        return None

    for k in ("holders_count", "holdersCount"):
        v = js.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)

    stats = js.get("stats")
    if isinstance(stats, dict):
        v = stats.get("holders_count") or stats.get("holdersCount")
        if isinstance(v, int):
            return v

    return None

def tonapi_account_transactions(address: str, limit: int = 10) -> List[Dict[str, Any]]:
    url = f"{TONAPI_BASE.rstrip('/')}/v2/blockchain/accounts/{address}/transactions"
    js = tonapi_get(url, params={"limit": limit})
    txs = js.get("transactions") if js else None
    if isinstance(txs, list):
        return [t for t in txs if isinstance(t, dict)]
    return []

# ===================== BUY DETECTION: STON (TONAPI FAST PATH) =====================
def _tx_lt(tx: Dict[str, Any]) -> int:
    tid = tx.get("transaction_id")
    for k in ("lt",):
        v = tx.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    if isinstance(tid, dict):
        v = tid.get("lt")
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    return 0

def _tx_hash(tx: Dict[str, Any]) -> str:
    h = (tx.get("hash") or tx.get("id") or "").strip()
    if not h:
        tid = tx.get("transaction_id")
        if isinstance(tid, dict):
            h = (tid.get("hash") or "").strip()
    return h

def stonfi_extract_buys_from_tonapi_tx(tx: Dict[str, Any], token_addr: str) -> List[Dict[str, Any]]:
    """Heuristic buy parser from TonAPI tx actions.
    We treat BUY as TON -> our jetton (token_addr).
    """
    out: List[Dict[str, Any]] = []
    actions = tx.get("actions")
    if not isinstance(actions, list):
        actions = []
    tx_hash = _tx_hash(tx)

    for a in actions:
        if not isinstance(a, dict):
            continue
        at = _action_type(a).lower()
        if "swap" not in at and "dex" not in at:
            continue

        # Filter for STON.fi if possible
        dex = a.get("dex")
        dex_name = ""
        if isinstance(dex, dict):
            dex_name = str(dex.get("name") or dex.get("title") or dex.get("id") or "").lower()
        if dex_name and ("ston" not in dex_name and "stonfi" not in dex_name and "ston.fi" not in dex_name):
            # if dex is present and not ston, skip
            continue

        buyer = (
            a.get("user")
            or a.get("sender")
            or a.get("initiator")
            or a.get("from")
            or a.get("account")
        )
        if isinstance(buyer, dict):
            buyer = buyer.get("address") or buyer.get("account")

        # Most common tonapi swap fields
        ton_in = a.get("ton_in") or a.get("tonIn") or a.get("in_ton") or a.get("inTon") or a.get("amount_ton_in")
        jet_out = a.get("jetton_out") or a.get("jettonOut") or a.get("out_jetton") or a.get("outJetton") or a.get("amount_jetton_out")

        # Try to ensure output jetton matches our token
        out_master = (
            a.get("jetton_master")
            or a.get("jettonMaster")
            or a.get("jetton")
            or (a.get("out") if isinstance(a.get("out"), str) else None)
        )
        if isinstance(out_master, dict):
            out_master = out_master.get("address") or out_master.get("master")

        # Some schemas nest assets
        if not out_master:
            asset_out = a.get("assetOut") or a.get("asset_out") or a.get("outAsset") or a.get("out_asset")
            out_master = extract_jetton_master(asset_out) if isinstance(asset_out, dict) else ""

        if out_master and token_addr and out_master != token_addr:
            continue

        ton_spent = 0.0
        if ton_in is not None:
            if isinstance(ton_in, (int, float)):
                ton_spent = to_ton_from_nano(ton_in) if float(ton_in) > 1e6 else float(ton_in)
            elif isinstance(ton_in, str):
                ton_spent = to_ton_from_nano(ton_in) if ton_in.isdigit() else safe_float(ton_in)

        token_received = 0.0
        if jet_out is not None:
            if isinstance(jet_out, (int, float)):
                token_received = float(jet_out)
            elif isinstance(jet_out, str):
                token_received = float(jet_out) if jet_out.replace(".", "", 1).isdigit() else safe_float(jet_out)

        # Normalize token amount if it looks like nano-jettons
        if token_addr and token_received > 0:
            dec = get_jetton_decimals(token_addr)
            if token_received > 10 ** (dec + 1):
                token_received = token_received / (10 ** dec)

        if ton_spent > 0 and token_received > 0:
            if isinstance(buyer, str) and buyer:
                out.append({"buyer": buyer, "ton": ton_spent, "token_amt": token_received, "tx": tx_hash, "lt": _tx_lt(tx)})
    return out

async def ston_tracker_job_fast(context: ContextTypes.DEFAULT_TYPE):
    """FAST STON tracker using TonAPI pool transactions (lower latency than export feed)."""
    if not TONAPI_KEY:
        return

    try:
        cleanup_seen()
        load_data()
        load_state()

        last_lt_map = STATE.get("ston_last_lt_map")
        if not isinstance(last_lt_map, dict):
            last_lt_map = {}
            STATE["ston_last_lt_map"] = last_lt_map

        # Collect stonfi pools
        pools = []
        for pool, rec in DATA.get("pairs", {}).items():
            if not isinstance(rec, dict):
                continue
            if str(rec.get("dex", "")).lower() != "stonfi":
                continue
            token_addr = (rec.get("token_address") or "").strip()
            if not token_addr:
                continue
            pools.append((pool, rec, token_addr))

        if not pools:
            return

        sem = asyncio.Semaphore(int(os.getenv("STON_CONCURRENCY", "16")))

        async def _fetch_pool(pool_addr: str):
            async with sem:
                txs = await _to_thread(tonapi_account_transactions, pool_addr, 25)
                return pool_addr, txs

        fetch_tasks = [asyncio.create_task(_fetch_pool(p[0])) for p in pools]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        txs_by_pool: Dict[str, List[Dict[str, Any]]] = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            pool_addr, txs = r
            if isinstance(pool_addr, str) and isinstance(txs, list):
                txs_by_pool[pool_addr] = [t for t in txs if isinstance(t, dict)]

        for pool_addr, rec, token_addr in pools:
            txs = txs_by_pool.get(pool_addr) or []
            if not txs:
                continue

            last_lt = last_lt_map.get(pool_addr, 0)
            try:
                last_lt = int(last_lt) if str(last_lt).isdigit() else int(last_lt or 0)
            except Exception:
                last_lt = 0

            # tonapi returns newest-first
            fresh_txs = []
            newest_lt = 0
            for tx in txs:
                lt = _tx_lt(tx)
                if lt > newest_lt:
                    newest_lt = lt
                if last_lt and lt <= last_lt:
                    continue
                fresh_txs.append(tx)

            if newest_lt:
                last_lt_map[pool_addr] = newest_lt
                save_state()

            if not fresh_txs:
                continue

            # process oldest -> newest
            fresh_txs.sort(key=_tx_lt)

            sym = (rec.get("symbol") or "?").strip().upper()
            buyer_map = rec.get("buyers")
            if not isinstance(buyer_map, dict):
                buyer_map = {}
                rec["buyers"] = buyer_map

            for tx in fresh_txs:
                buys = stonfi_extract_buys_from_tonapi_tx(tx, token_addr)
                if not buys:
                    continue
                for buy in buys:
                    txh = (buy.get("tx") or "").strip()
                    if not txh:
                        continue
                    seen_key = f"ston:{pool_addr}:{txh}"
                    if seen_key in SEEN_TX_STON:
                        continue
                    SEEN_TX_STON[seen_key] = time.time()

                    buyer = (buy.get("buyer") or "").strip()
                    ton_amt = safe_float(buy.get("ton"))
                    token_amt = safe_float(buy.get("token_amt"))

                    pos_txt = "New Holder!" if buyer and buyer not in buyer_map else "Existing Holder"
                    if buyer:
                        buyer_map[buyer] = int(buyer_map.get(buyer, 0)) + 1
                        save_data()

                    await post_buy_message(
                        context=context,
                        sym=sym,
                        token_addr=token_addr,
                        pair_id=pool_addr,
                        buyer=buyer,
                        tx_hash=txh,
                        ton_amt=ton_amt,
                        token_amt=token_amt,
                        pos_txt=pos_txt,
                        source_label=(rec.get("dex_label") or "STON.fi"),
                    )
    except Exception as e:
        log.exception("ston_tracker_job_fast error: %s", e)


# ===================== BUY DETECTION: STON =====================

# ===================== BUY DETECTION: STON =====================
def extract_buy_from_ston_event(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """STON.fi buy-only parser.
    We only post real BUYS: TON -> TOKEN.
    """
    if (ev.get("eventType") or "").lower() != "swap":
        return None

    pair_id = (ev.get("pairId") or "").strip()
    if not pair_id:
        return None

    if pair_id not in DATA.get("pairs", {}):
        return None

    rec = DATA["pairs"].get(pair_id, {})
    if str(rec.get("dex", "stonfi")).lower() != "stonfi":
        return None

    tx = (ev.get("txnId") or "").strip()
    if not tx:
        return None

    maker = (ev.get("maker") or "").strip()

    # Raw amounts from STON exported events feed (usually in human units already)
    a0_in = safe_float(ev.get("amount0In"))
    a0_out = safe_float(ev.get("amount0Out"))
    a1_in = safe_float(ev.get("amount1In"))
    a1_out = safe_float(ev.get("amount1Out"))

    # Determine which leg is TON via DexScreener metadata (cached)
    ton_leg = ensure_pair_ton_leg(pair_id)

    # ✅ BUY ONLY logic:
    # If TON is leg 0 => BUY = amount0In (TON in) and amount1Out (token out)
    # If TON is leg 1 => BUY = amount1In (TON in) and amount0Out (token out)
    ton_spent = 0.0
    token_received = 0.0

    if ton_leg == 0:
        if a0_in > 0 and a1_out > 0:
            ton_spent = a0_in
            token_received = a1_out
        else:
            return None  # ignore SELL or weird swaps
    elif ton_leg == 1:
        if a1_in > 0 and a0_out > 0:
            ton_spent = a1_in
            token_received = a0_out
        else:
            return None
    else:
        # If we cannot determine TON leg, be safe: do NOT post.
        return None

    if ton_spent <= 0 or token_received <= 0:
        return None

    return {"pair_id": pair_id, "tx": tx, "buyer": maker, "ton": ton_spent, "token_amt": token_received}


# ===================== BUY DETECTION: DEDUST =====================
def _action_type(a: Dict[str, Any]) -> str:
    t = a.get("type") or a.get("action") or a.get("name") or ""
    return str(t)

def dedust_extract_buys_from_tonapi_tx(tx: Dict[str, Any], pool: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    tx_hash = (tx.get("hash") or tx.get("transaction_id") or tx.get("id") or "").strip()
    if not tx_hash:
        tid = tx.get("transaction_id")
        if isinstance(tid, dict):
            tx_hash = (tid.get("hash") or "").strip()

    actions = tx.get("actions")
    if not isinstance(actions, list):
        actions = []

    if actions:
        for a in actions:
            if not isinstance(a, dict):
                continue
            at = _action_type(a).lower()

            if "swap" not in at and "dex" not in at:
                continue

            buyer = (
                a.get("user")
                or a.get("sender")
                or a.get("initiator")
                or (a.get("source") if isinstance(a.get("source"), str) else None)
            )

            if isinstance(buyer, dict):
                buyer = buyer.get("address") or buyer.get("account") or buyer.get("wallet")

            ton_in = a.get("ton_in") or a.get("tonIn") or a.get("in_ton") or a.get("inTon")
            jet_out = a.get("jetton_out") or a.get("jettonOut") or a.get("out_jetton") or a.get("outJetton")

            ton_spent = 0.0
            if ton_in is not None:
                ton_spent = to_ton_from_nano(ton_in) if str(ton_in).isdigit() else safe_float(ton_in)

            token_received = safe_float(jet_out) if jet_out is not None else 0.0

            if ton_spent > 0 and token_received > 0 and isinstance(buyer, str) and buyer:
                out.append({"buyer": buyer, "ton": ton_spent, "token_amt": token_received, "tx": tx_hash})
        if out:
            return out

    return []

# ===================== BUY DETECTION: BLUM (EARLY) =====================
def blum_extract_buys_from_jetton_master_tx(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Heuristic:
      - find actions that look like jetton mint/transfer -> (recipient, amount)
      - sum TON transfers from same recipient wallet in that tx as ton_spent
    This won't be perfect for every memepad, but works often enough for early alerts.
    """
    actions = tx.get("actions")
    if not isinstance(actions, list):
        actions = []

    # transaction hash + lt
    tx_hash = (tx.get("hash") or "").strip()
    tid = tx.get("transaction_id")
    if not tx_hash and isinstance(tid, dict):
        tx_hash = (tid.get("hash") or "").strip()

    lt_i = 0
    lt = tx.get("lt")
    if isinstance(lt, int):
        lt_i = lt
    elif isinstance(lt, str) and lt.isdigit():
        lt_i = int(lt)
    elif isinstance(tid, dict) and str(tid.get("lt", "")).isdigit():
        lt_i = int(tid.get("lt"))

    # collect received jettons by recipient
    received: List[Tuple[str, float]] = []

    for a in actions:
        if not isinstance(a, dict):
            continue
        at = _action_type(a).lower()

        # common-ish names on tonapi: "JettonTransfer", "JettonMint", etc
        if "jetton" not in at:
            continue
        if "transfer" not in at and "mint" not in at:
            continue

        # Try common fields
        recipient = (
            a.get("recipient")
            or a.get("receiver")
            or a.get("to")
            or a.get("destination")
        )
        if isinstance(recipient, dict):
            recipient = recipient.get("address") or recipient.get("account")

        amount = (
            a.get("amount")
            or a.get("jetton_amount")
            or a.get("jettonAmount")
            or a.get("value")
        )

        amt = 0.0
        # amount might be string int in nano-jettons; we don't know decimals here
        # BUT tonapi sometimes provides "amount" already normalized.
        if amount is None:
            amt = 0.0
        else:
            if isinstance(amount, (int, float)):
                amt = float(amount)
            elif isinstance(amount, str):
                # if huge integer, keep as float (still ok for display)
                try:
                    amt = float(amount)
                except:
                    amt = 0.0

        if isinstance(recipient, str) and recipient and amt > 0:
            received.append((recipient, amt))

    if not received:
        return []

    # build TON spent estimate per recipient: sum TON transfers where sender == recipient
    ton_out_by_sender: Dict[str, float] = {}
    for a in actions:
        if not isinstance(a, dict):
            continue
        at = _action_type(a).lower()
        if "ton" not in at and "transfer" not in at:
            continue

        sender = a.get("sender") or a.get("from") or a.get("source")
        if isinstance(sender, dict):
            sender = sender.get("address") or sender.get("account")

        amt = a.get("amount") or a.get("value") or a.get("ton_amount") or a.get("tonAmount")
        ton_amt = 0.0
        if amt is not None:
            if isinstance(amt, (int, float)):
                # assume nano if large
                ton_amt = to_ton_from_nano(amt) if float(amt) > 1e6 else float(amt)
            elif isinstance(amt, str):
                if amt.isdigit():
                    ton_amt = to_ton_from_nano(amt)
                else:
                    ton_amt = safe_float(amt)

        if isinstance(sender, str) and sender and ton_amt > 0:
            ton_out_by_sender[sender] = ton_out_by_sender.get(sender, 0.0) + ton_amt

    buys: List[Dict[str, Any]] = []
    for (buyer, token_amt) in received:
        buys.append({
            "buyer": buyer,
            "token_amt": token_amt,
            "ton": ton_out_by_sender.get(buyer, 0.0),
            "tx": tx_hash,
            "lt": lt_i,
        })

    return buys

def tg_emoji(emoji_id: str, fallback: str) -> str:
    # Return Telegram custom emoji HTML tag if a VALID numeric id is provided, else fallback.
    emoji_id = (emoji_id or "").strip()
    if not emoji_id.isdigit():
        return fallback
    return f"<tg-emoji emoji-id=\"{emoji_id}\">{fallback}</tg-emoji>"

def strength_count_from_ton(ton_amt: float) -> int:
    # Map TON amount to 1..28 strength icons.
    try:
        t = float(ton_amt or 0.0)
    except Exception:
        t = 0.0
    return max(1, min(28, int(t // 2) + 1))

def build_strength_bar(ton_amt: float) -> str:
    # Two-line strength bar (up to 28 icons). No empty squares.
    filled = strength_count_from_ton(ton_amt)
    icon = tg_emoji(SPY_CUSTOM_EMOJI_ID, "🟢")
    icons = [icon] * filled
    line1 = "".join(icons[:14])
    line2 = "".join(icons[14:28])
    return f"{line1}\n{line2}\n" if line2 else f"{line1}\n"

# ===================== MESSAGE SENDER =====================
async def post_buy_message(
    context: ContextTypes.DEFAULT_TYPE,
    sym: str,
    token_addr: str,
    pair_id: str,
    buyer: str,
    tx_hash: str,
    ton_amt: float,
    token_amt: float,
    pos_txt: str,
    source_label: str = "DEX",
):
    # Build links early (no network)
    chart_url = f"https://www.geckoterminal.com/ton/tokens/{token_addr}" if token_addr else f"https://dexscreener.com/ton/{pair_id}"
    pools_url = f"https://dexscreener.com/ton/{pair_id}"
    buyer_url = f"https://tonviewer.com/{buyer}" if buyer else ""
    tx_url = make_tx_url(tx_hash)

    badge = buy_badge(ton_amt) if ton_amt > 0 else "✨"
    lbl = (source_label or "").strip()

    # Get TG link if available
    rec = DATA["pairs"].get(pair_id, {})
    tg_url = rec.get("telegram")
    if not tg_url and token_addr:
        for _wid, w in DATA.get("watch", {}).items():
            if isinstance(w, dict) and (w.get("token_address") or "").strip() == (token_addr or "").strip():
                tg_url = w.get("telegram")
                break

    # Compose function so we can send fast then edit later
    def _compose(ton_usd_val: float, stats: Dict[str, Any], holders_count: Optional[int]) -> Tuple[str, str]:
        usd_val = ton_amt * ton_usd_val if ton_usd_val > 0 and ton_amt > 0 else 0.0
        usd_part = f" (${usd_val:,.2f})" if usd_val else ""

        mc_txt = money_fmt(stats.get("marketcap_usd"))
        liq_txt = money_fmt(stats.get("liquidity_usd"))

        holders_line = f"{tg_emoji(ICON_HOLDERS_ID, '👥')} Holders <b>{holders_count}</b>\n" if isinstance(holders_count, int) else ""

        title_core = f"{badge} {sym} Buy!"
        if lbl:
            title_core = f"{badge} {sym} Buy! — {lbl}"
        title = f"<a href='{tg_url}'><b>{title_core}</b></a>" if tg_url else f"<b>{title_core}</b>"

        ton_line = f"{tg_emoji(ICON_SWAP_ID, '🔁')} <b>{ton_amt:.2f} TON</b>{usd_part}\n" if ton_amt > 0 else ""
        token_line = f"{tg_emoji(ICON_SWAP_ID, '🔁')} <b>{token_amt:,.6f} {sym}</b>\n" if token_amt > 0 else ""

        forced_rank = get_forced_rank(sym)
        auto_rank = get_auto_rank(sym)
        show_rank = forced_rank or auto_rank
        trend_rank_line = (f"\n\n🟢 <b>#{show_rank}</b> On <a href='{TRENDING_URL}'>SpyTON Trending</a>" if show_rank else "")

        text = (
            f"{title}\n"
            f"{build_strength_bar(ton_amt)}\n"
            f"{ton_line}"
            f"{token_line}"
            f"{tg_emoji(ICON_WALLET_ID, '👤')} <a href='{buyer_url}'>{short(buyer)}</a> | {tg_emoji(ICON_TXN_ID, '🔗')} <a href='{tx_url}'>Txn</a>\n"
            f"{tg_emoji(ICON_POS_ID, '⬆️')} Position: <b>{pos_txt}</b>\n"
            f"{holders_line}"
        )

        is_blum = (source_label or "").strip().lower() == "blum"
        if not is_blum:
            text += (
                f"{tg_emoji(ICON_MCAP_ID, '💸')} Market Cap <b>{mc_txt}</b>\n"
                f"{tg_emoji(ICON_LIQ_ID, '🌊')} Liquidity <b>{liq_txt}</b>\n\n"
                f"{tg_emoji(ICON_PIN_ID, '📌')} <a href='{LISTING_URL}'>Ton Listing</a>\n"
                f"{tg_emoji(ICON_CHART_ID, '📊')} <a href='{chart_url}'>Chart</a> | "
                f"{tg_emoji(ICON_TREND_ID, '🔥')} <a href='{TRENDING_URL}'>Trending</a> | "
                f"{tg_emoji(ICON_POOLS_ID, '🆕')} <a href='{pools_url}'>Pools</a>"
            )
        else:
            text += (
                f"\n{tg_emoji(ICON_PIN_ID, '📌')} <a href='{LISTING_URL}'>Ton Listing</a>\n"
                f"{tg_emoji(ICON_CHART_ID, '📊')} <a href='{chart_url}'>Chart</a> | "
                f"{tg_emoji(ICON_TREND_ID, '🔥')} <a href='{TRENDING_URL}'>Trending</a>"
            )

        text += trend_rank_line

        # GROUP STYLE (exact template user wants)
        dex_lbl_plain = (lbl or source_label or "DEX").strip() or "DEX"
        grp_pos = "New!" if "new" in (pos_txt or "").lower() else "Old!"
        price_val = stats.get("price_usd")
        price_txt = f"{price_val:.6f}".rstrip("0").rstrip(".") if isinstance(price_val, (int, float)) and price_val > 0 else "—"
        mc_val_raw = stats.get("marketcap_usd")
        mc_group = f"{mc_val_raw:,.0f}" if isinstance(mc_val_raw, (int, float)) and mc_val_raw > 0 else "—"
        usd_group = f"{usd_val:,.2f}" if usd_val else ""
        buyer_group = short(buyer)
        group_text = (
            f"🚀 {sym} TOKEN Buy! — {dex_lbl_plain}\n"
            f"✅ LISTED!\n\n"
            f"{'💡'*10}\n\n"
            f"💰 {ton_amt:.2f} TON ({'$' + usd_group if usd_group else '$0'})\n"
            f"📦 {token_amt:,.2f} {sym}\n"
            f"👤 {buyer_group} | {grp_pos}\n"
            f"💵 Price: ${price_txt}\n"
            f"🏦 MarketCap: ${mc_group}\n\n"
            f"❤️ <a href='{LISTING_URL}'>TonListing</a> | 📊 <a href='{chart_url}'>Chart</a>"
        )
        return text, group_text

    # FAST: send immediately with placeholders, then edit with enriched stats
    ton_usd = ton_price_cache_value()
    stats: Dict[str, Any] = {"marketcap_usd": None, "liquidity_usd": None, "price_usd": None}
    holders_count: Optional[int] = None

    if not FAST_POST_MODE:
        # Original (slower) behavior: fetch before sending
        stats = (await _to_thread(fetch_pair_stats, pair_id)) if source_label == "DEX" else {"marketcap_usd": None, "liquidity_usd": None}
        if token_addr and (stats.get("marketcap_usd") is None or stats.get("liquidity_usd") is None):
            tstats = await _to_thread(fetch_token_stats, token_addr)
            if stats.get("marketcap_usd") is None:
                stats["marketcap_usd"] = tstats.get("marketcap_usd")
            if stats.get("liquidity_usd") is None:
                stats["liquidity_usd"] = tstats.get("liquidity_usd")
            if stats.get("price_usd") is None:
                stats["price_usd"] = tstats.get("price_usd")

        if token_addr:
            holders_count = await _to_thread(fetch_holders_count_tonapi, token_addr)

    text, group_text = _compose(ton_usd, stats, holders_count)


    # Targets: always master channel + any configured group mirrors for this token/pair
    load_data()
    targets: List[int] = [MASTER_CHANNEL_ID]
    mirrors = DATA.get("group_mirrors", {})
    if isinstance(mirrors, dict):
        for cid_str, cfg in mirrors.items():
            if not isinstance(cfg, dict):
                continue
            try:
                cid = int(cid_str)
            except:
                continue
            taddr = (cfg.get("token_address") or "").strip()
            pid = (cfg.get("pair_id") or "").strip()
            if (token_addr and taddr and token_addr.strip() == taddr) or (pair_id and pid and pair_id.strip() == pid):
                if cid not in targets and cid != MASTER_CHANNEL_ID:
                    targets.append(cid)

    sent_refs: List[Tuple[int, int, bool]] = []  # (chat_id, message_id, used_photo)

    async def _send_message(chat_id: int):
        """Send a buy alert. Master channel gets full SpyTON style; groups get compact style."""

        if chat_id == MASTER_CHANNEL_ID:
            if file_exists(HEADER_IMAGE_PATH):
                try:
                    with open(HEADER_IMAGE_PATH, "rb") as img:
                        msg = await context.bot.send_photo(
                            chat_id=chat_id,
                            photo=img,
                            caption=text,
                            parse_mode="HTML",
                            reply_markup=buy_alert_keyboard(chart_url, pools_url),
                        )
                        sent_refs.append((chat_id, msg.message_id, True))
                        return
                except Exception:
                    pass

            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=buy_alert_keyboard(chart_url, pools_url),
                disable_web_page_preview=True,
            )
            sent_refs.append((chat_id, msg.message_id, False))
            return

        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=group_text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        sent_refs.append((chat_id, msg.message_id, False))

    # Send to master and mirrors
    for chat_id in targets:
        try:
            await _send_message(chat_id)
        except Exception:
            continue

    # Background enrichment: fetch stats/holders and edit messages
    if FAST_POST_MODE and sent_refs:
        async def _enrich_and_edit():
            try:
                # Stats (use timeouts so we never block posting)
                enriched_stats = dict(stats)
                if token_addr:
                    try:
                        tstats = await asyncio.wait_for(_to_thread(fetch_token_stats, token_addr), timeout=FAST_STATS_TIMEOUT)
                        if isinstance(tstats, dict):
                            for k in ("marketcap_usd", "liquidity_usd", "price_usd"):
                                if enriched_stats.get(k) is None and tstats.get(k) is not None:
                                    enriched_stats[k] = tstats.get(k)
                    except Exception:
                        pass

                # Optional holders (slow; default off)
                enriched_holders = None
                if FAST_HOLDERS_ENABLED and token_addr:
                    try:
                        enriched_holders = await asyncio.wait_for(_to_thread(fetch_holders_count_tonapi, token_addr), timeout=FAST_STATS_TIMEOUT)
                    except Exception:
                        enriched_holders = None

                # Recompose with enriched data
                new_text, new_group_text = _compose(ton_usd, enriched_stats, enriched_holders)

                for cid, mid, used_photo in sent_refs:
                    try:
                        if cid == MASTER_CHANNEL_ID:
                            if used_photo:
                                await context.bot.edit_message_caption(
                                    chat_id=cid,
                                    message_id=mid,
                                    caption=new_text,
                                    parse_mode="HTML",
                                    reply_markup=buy_alert_keyboard(chart_url, pools_url),
                                )
                            else:
                                await context.bot.edit_message_text(
                                    chat_id=cid,
                                    message_id=mid,
                                    text=new_text,
                                    parse_mode="HTML",
                                    reply_markup=buy_alert_keyboard(chart_url, pools_url),
                                    disable_web_page_preview=True,
                                )
                        else:
                            await context.bot.edit_message_text(
                                chat_id=cid,
                                message_id=mid,
                                text=new_group_text,
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )
                    except Exception:
                        continue
            except Exception:
                return

        asyncio.create_task(_enrich_and_edit())

# ===================== LEADERBOARD (6H movers) =====================

async def update_leaderboard(context: ContextTypes.DEFAULT_TYPE):
    """Auto-updating Top Movers leaderboard (Top 1–10) in Crypton-style format."""
    lb_id = STATE.get("leaderboard_msg_id")
    if not lb_id:
        return

    load_data()
    # Refresh auto ranks from volume
    refresh_auto_ranks(force=True)
    TF_PRIMARY = "h6"

    items: List[Dict[str, Any]] = []

    for pid, rec in DATA.get("pairs", {}).items():
        if not isinstance(rec, dict):
            continue

        sym = (rec.get("symbol") or "?").strip().upper()
        token_addr = (rec.get("token_address") or "").strip()

        # Try auto-fetch TG link if missing
        tg_url = rec.get("telegram")
        if not tg_url and token_addr:
            tg_found = fetch_token_telegram_url_from_dexscreener(token_addr)
            if tg_found:
                rec["telegram"] = tg_found
                tg_url = tg_found
                try:
                    save_data()
                except:
                    pass

        # Fetch price change (fallback to h1 if h6 missing)
        ch = fetch_pair_change(pid, TF_PRIMARY)
        if ch is None:
            ch = fetch_pair_change(pid, "h1")
        if ch is None:
            continue

        stats = fetch_pair_stats(pid)
        liq = stats.get("liquidity_usd")
        mc = stats.get("marketcap_usd")

        # Filters (can be lowered via env vars)
        if liq is not None and liq < LB_MIN_LIQ_USD:
            continue
        if mc is not None and mc < LB_MIN_MC_USD:
            continue

        items.append({
            "pair_id": pid,
            "sym": sym,
            "ch": float(ch),
            "mc": mc,
            "liq": liq,
            "tg": tg_url,
        })

    # Dedup by token address if possible
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        key = (DATA.get("pairs", {}).get(it["pair_id"], {}).get("token_address") or f"PAIR::{it['pair_id']}").strip()
        grouped.setdefault(key, []).append(it)

    deduped: List[Dict[str, Any]] = []
    for _k, arr in grouped.items():
        # choose most liquid among duplicates
        chosen = sorted(arr, key=lambda x: (x["liq"] or 0.0, x["mc"] or 0.0, abs(x["ch"])), reverse=True)[0]
        deduped.append(chosen)

    # Sort by absolute move (so big gainers and losers show)
    deduped.sort(key=lambda x: abs(x["ch"]), reverse=True)
    top = deduped[:10]

    def fmt_pct(v: float) -> str:
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.0f}%"

    def sym_link(sym: str, tg: Optional[str]) -> str:
        if tg and isinstance(tg, str) and tg.startswith("http"):
            return f"<a href='{tg}'>${sym}</a>"
        return f"${sym}"

    nums = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    text = "TON TRENDING\n🟢 @Spytontrending\n\n"

    if not top:
        text += "(No data yet)"
    else:
        for i, it in enumerate(top):
            line = f"{nums[i]} - {sym_link(it['sym'], it.get('tg'))} | {fmt_pct(it['ch'])}"
            text += line + "\n"
            if i == 2:
                text += "------------------------------\n"

    try:
        await context.bot.edit_message_text(
            chat_id=CHANNEL_ID,
            message_id=lb_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=leaderboard_button(),
        )
    except Exception:
        pass


# ===================== JOB: MEMEPAD AUTO-ACTIVATION =====================
async def memepad_activation_job(context: ContextTypes.DEFAULT_TYPE):
    if not MEMEPAD_ACTIVATION_ENABLED:
        return

    load_data()
    watch = DATA.get("watch", {})
    if not isinstance(watch, dict) or not watch:
        return

    changed = False
    to_remove: List[str] = []

    for watch_id, rec in watch.items():
        if not isinstance(rec, dict):
            continue

        token_address = (rec.get("token_address") or "").strip()
        symbol = (rec.get("symbol") or "?").strip().upper()
        tg_link = rec.get("telegram")
        source = rec.get("source") or "memepad"

        if not token_address:
            continue

        pair_id = await _to_thread(find_stonfi_ton_pair_for_token, token_address)
        dex = "stonfi"
        if not pair_id:
            pair_id = await _to_thread(find_dedust_ton_pair_for_token, token_address)
            dex = "dedust"

        if not pair_id:
            continue

        old = DATA["pairs"].get(pair_id, {})
        meta = await _to_thread(fetch_pair_meta, pair_id)
        dex_label = None
        if dex == "dedust":
            dex_label = "DeDust"
        elif isinstance(meta, dict):
            dex_label = dex_label_from_dex_id(meta.get("dex_id") or "")
        DATA["pairs"][pair_id] = {
            "symbol": symbol or old.get("symbol", "?"),
            "token_address": token_address,
            "telegram": tg_link or old.get("telegram"),
            "dex": dex,
            "dex_label": dex_label or old.get("dex_label") or ("DeDust" if dex == "dedust" else "STON.fi"),
            "buyers": old.get("buyers", {}) if isinstance(old.get("buyers"), dict) else {},
        }

        # Update any group mirrors watching this token
        mirrors = DATA.get("group_mirrors", {})
        if isinstance(mirrors, dict):
            for _cid, cfg in mirrors.items():
                if not isinstance(cfg, dict):
                    continue
                if (cfg.get("token_address") or "").strip() == token_address:
                    cfg["pair_id"] = pair_id
                    cfg["dex"] = dex
                    cfg["updated_ts"] = int(time.time())
        
        to_remove.append(watch_id)
        changed = True

        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"✅ Activated {symbol}\n"
                    f"Source: {source}\n"
                    f"DEX: {dex}\n"
                    f"Token: <code>{token_address}</code>\n"
                    f"Pair/Pool: <code>{pair_id}</code>\n"
                    f"Now posting buys automatically."
                ),
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        except Exception:
            pass

    for k in to_remove:
        watch.pop(k, None)

    if changed:
        save_data()

# ===================== JOB: BLUM EARLY TRACKER (NEW) =====================
async def blum_early_tracker_job(context: ContextTypes.DEFAULT_TYPE):
    if not BLUM_EARLY_ENABLED:
        return
    if not TONAPI_KEY:
        return

    cleanup_seen()
    load_data()
    load_state()

    watch = DATA.get("watch", {})
    if not isinstance(watch, dict) or not watch:
        return

    blum_last_lt = STATE.get("blum_last_lt", {})
    if not isinstance(blum_last_lt, dict):
        blum_last_lt = {}
        STATE["blum_last_lt"] = blum_last_lt

    changed = False

    # Scan only approved blum watch entries
    for wid, rec in watch.items():
        if not isinstance(rec, dict):
            continue
        if (rec.get("source") or "").lower() != "blum":
            continue
        if not rec.get("approved_early", False):
            continue

        token_addr = (rec.get("token_address") or "").strip()
        if not token_addr:
            continue

        sym = (rec.get("symbol") or "?").strip().upper()
        tg_link = rec.get("telegram")

        txs = await _to_thread(tonapi_account_transactions, token_addr, BLUM_POLL_LIMIT)
        if not txs:
            continue

        last_lt = safe_int(blum_last_lt.get(token_addr)) or 0

        parsed: List[Tuple[int, str, Dict[str, Any]]] = []
        for tx in txs:
            lt = tx.get("lt")
            if isinstance(lt, str) and lt.isdigit():
                lt_i = int(lt)
            elif isinstance(lt, int):
                lt_i = lt
            else:
                tid = tx.get("transaction_id")
                lt_i = int(tid.get("lt")) if isinstance(tid, dict) and str(tid.get("lt", "")).isdigit() else 0

            h = (tx.get("hash") or "")
            if not h:
                tid = tx.get("transaction_id")
                if isinstance(tid, dict):
                    h = tid.get("hash") or ""
            parsed.append((lt_i, str(h), tx))

        parsed.sort(key=lambda x: x[0])

        newest_seen_lt = last_lt

        for lt_i, h, tx in parsed:
            if lt_i <= last_lt:
                continue

            key = f"BLUM:{token_addr}:{h or lt_i}"
            if key in SEEN_TX_BLUM:
                continue
            SEEN_TX_BLUM[key] = time.time()

            if BLUM_DEBUG:
                print(f"[BLUM] jetton={token_addr} lt={lt_i} hash={h}")

            buys = blum_extract_buys_from_jetton_master_tx(tx)
            if not buys:
                newest_seen_lt = max(newest_seen_lt, lt_i)
                continue

            # buyers tracking under WATCH record
            buyers_map = rec.get("buyers")
            if not isinstance(buyers_map, dict):
                buyers_map = {}
                rec["buyers"] = buyers_map

            for b in buys:
                buyer = (b.get("buyer") or "").strip()
                token_amt = float(b.get("token_amt") or 0.0)
                ton_amt = float(b.get("ton") or 0.0)
                tx_hash = (b.get("tx") or h or "").strip()

                if not buyer or token_amt <= 0:
                    continue

                is_new = buyer not in buyers_map
                buyers_map[buyer] = int(buyers_map.get(buyer, 0)) + 1
                pos_txt = "New Holder!" if is_new else "Existing Holder"

                # post (pair_id is token_addr for early mode)
                await post_buy_message(
                    context=context,
                    sym=sym,
                    token_addr=token_addr,
                    pair_id=token_addr,
                    buyer=buyer,
                    tx_hash=tx_hash,
                    ton_amt=ton_amt,
                    token_amt=token_amt,
                    pos_txt=pos_txt,
                    source_label="Blum",
                )

                rec["last_buy_ts"] = int(time.time())
                changed = True

            newest_seen_lt = max(newest_seen_lt, lt_i)

        if newest_seen_lt > last_lt:
            blum_last_lt[token_addr] = newest_seen_lt
            STATE["blum_last_lt"] = blum_last_lt
            save_state()

    if changed:
        save_data()

# ===================== COMMANDS =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🟢 SpyTON Detector\n\n"
        "Add token:\n"
        "/addtoken <JETTON_ADDRESS_OR_MEMEPAD_LINK> <SYMBOL> [TELEGRAM_LINK]\n"
        "• Finds STON.fi pair first, then DeDust pair.\n"
        "• If not listed yet, it will WATCH and auto-activate on listing.\n\n"
        "Blum early:\n"
        "/watchlist\n"
        "/approve <WATCH_ID>\n"
        "/setaddr <WATCH_ID or blum:memepadjetton_xxx> <JETTON_ADDRESS>\n\n"
        "Edit TG:\n"
        "/edittg <PAIR_ID> <TELEGRAM_LINK>\n\n"
        "Leaderboard:\n"
        "/setleaderboard (creates leaderboard post, then pin it)\n\n"
        "Other:\n"
        "/listpairs\n"
        "/delpair <PAIR_ID>\n"
        "/status\n"
    )


# =========================
# Forced Trend Ranks (Admin)
# =========================

def get_forced_rank(symbol: str) -> Optional[int]:
    """Return forced rank for a symbol if set."""
    try:
        load_data()
        fr = DATA.get("forced_ranks", {})
        if isinstance(fr, dict):
            v = fr.get(symbol.upper())
            if isinstance(v, int) and v > 0:
                return v
    except:
        pass
    return None

def set_forced_rank(symbol: str, rank: int):
    load_data()
    DATA.setdefault("forced_ranks", {})
    DATA["forced_ranks"][symbol.upper()] = int(rank)
    save_data()


def refresh_auto_ranks(force: bool = False) -> Dict[str, int]:
    """Compute ranks from 6H USD volume across all tracked pairs.
    Rank 1 = highest volume.
    Cached for AUTO_RANK_TTL seconds.
    """
    global AUTO_RANKS, AUTO_RANK_TS

    now = time.time()
    if (not force) and AUTO_RANKS and (now - AUTO_RANK_TS < AUTO_RANK_TTL):
        return AUTO_RANKS

    load_data()
    vol_by_sym: Dict[str, float] = {}

    for pid, rec in DATA.get("pairs", {}).items():
        if not isinstance(rec, dict):
            continue
        sym = (rec.get("symbol") or "").strip().upper()
        if not sym:
            continue
        stats = fetch_pair_stats(pid)
        v = safe_float(stats.get("volume_h6_usd"))
        if v is None or v <= 0:
            continue
        vol_by_sym[sym] = vol_by_sym.get(sym, 0.0) + float(v)

    ranked = sorted(vol_by_sym.items(), key=lambda x: x[1], reverse=True)
    AUTO_RANKS = {sym: i + 1 for i, (sym, _v) in enumerate(ranked)}
    AUTO_RANK_TS = now
    return AUTO_RANKS


def get_auto_rank(symbol: str) -> Optional[int]:
    try:
        refresh_auto_ranks(force=False)
        return AUTO_RANKS.get(symbol.upper())
    except:
        return None


def clear_forced_rank(symbol: str):
    load_data()
    fr = DATA.get("forced_ranks", {})
    if isinstance(fr, dict) and symbol.upper() in fr:
        del fr[symbol.upper()]
        save_data()

def list_forced_ranks() -> dict:
    load_data()
    fr = DATA.get("forced_ranks", {})
    return fr if isinstance(fr, dict) else {}

def _is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID)

async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True if caller is admin in the current group/supergroup chat."""
    try:
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user:
            return False
        if chat.type not in ("group", "supergroup"):
            return False
        cm = await context.bot.get_chat_member(chat.id, user.id)
        status = getattr(cm, "status", None)
        return status in ("administrator", "creator")
    except Exception:
        return False


async def setrank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/setrank <SYMBOL> <RANK>  (Admin only)"""
    if not _is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /setrank <SYMBOL> <RANK>\nExample: /setrank LABR 6")
        return
    symbol = context.args[0].upper().strip()
    try:
        rank = int(context.args[1])
    except:
        await update.message.reply_text("Rank must be a number.\nExample: /setrank LABR 6")
        return
    if rank < 1 or rank > 999:
        await update.message.reply_text("Rank must be between 1 and 999.")
        return
    set_forced_rank(symbol, rank)
    await update.message.reply_text(f"✅ Forced rank set: {symbol} = #{rank} On SpyTON Trending")


async def clearrank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/clearrank <SYMBOL>  (Admin only)"""
    if not _is_admin(update):
        return
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /clearrank <SYMBOL>\nExample: /clearrank LABR")
        return
    symbol = context.args[0].upper().strip()
    clear_forced_rank(symbol)
    await update.message.reply_text(f"✅ Forced rank cleared for {symbol}")


async def ranks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ranks  (Admin only)"""
    if not _is_admin(update):
        return
    fr = list_forced_ranks()
    if not fr:
        await update.message.reply_text("No forced ranks set.")
        return
    lines = ["📌 Forced Trend Ranks (SpyTON Trending):\n"]
    for sym, rk in sorted(fr.items(), key=lambda x: int(x[1]) if str(x[1]).isdigit() else 9999):
        lines.append(f"✅ {sym} = #{rk}")
    await update.message.reply_text("\n".join(lines))

async def addtoken(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Permission: your ADMIN_ID can always add tokens.
    # In groups/supergroups, allow that group's admins to configure their token too.
    chat = update.effective_chat
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        if not chat or chat.type not in ("group", "supergroup"):
            return
        if not await is_chat_admin(context, chat.id, uid):
            return

    # robust parse (fixes Blum link "no response" / args issues)
    parsed_msg = parse_addtoken_message_text(update.message.text or "")
    if not parsed_msg:
        await update.message.reply_text("Usage: /addtoken <JETTON_ADDRESS_OR_MEMEPAD_LINK> <SYMBOL> [TELEGRAM_LINK]")
        return

    raw_input, symbol, tg_link = parsed_msg
    tg_link = normalize_url(tg_link) if tg_link else None

    parsed = parse_memepad_input(raw_input)
    token_address = parsed.get("token_address")
    source = parsed.get("source", "unknown")
    blum_slug = parsed.get("blum_slug")

    load_data()

    # If link doesn't include token address, store as watch by slug (Blum)
    if not token_address:
        # In groups we require jetton master address (so the group can mirror buys reliably)
        if chat and chat.type in ("group", "supergroup"):
            await update.message.reply_text("❌ In groups, please use the Jetton master address.\nUsage: /addtoken <JETTON_ADDRESS> <SYMBOL> [TELEGRAM_LINK]")
            return
        watch_id = f"{source}:{blum_slug or raw_input}"
        DATA.setdefault("watch", {})
        DATA["watch"][watch_id] = {
            "source": source,
            "symbol": symbol,
            "token_address": None,
            "blum_slug": blum_slug,
            "telegram": tg_link,
            "raw": raw_input,
            "approved_early": False,  # NEW
            "added_ts": int(time.time()),
        }
        save_data()

        extra = ""
        if source == "blum":
            extra = (
                "\n\n💡 Blum detected.\n"
                "If you later get the Jetton master address, set it:\n"
                f"/setaddr {watch_id} <JETTON_ADDRESS>\n"
                "Then approve once to post early buys:\n"
                f"/approve {watch_id}"
            )

        await update.message.reply_text(
            f"🟡 Added to WATCH (Memepad)\n"
            f"Source: <b>{source}</b>\n"
            f"Symbol: <b>{symbol}</b>\n"
            f"Info: <code>{blum_slug or 'no-address-in-link'}</code>\n"
            f"Telegram: {tg_link or 'NONE'}\n\n"
            f"✅ Bot will auto-activate when it reaches STON.fi or DeDust."
            f"{extra}",
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        return

    # Token address was provided: try find DEX pair now
    pair_id = find_stonfi_ton_pair_for_token(token_address)
    dex = "stonfi"
    if not pair_id:
        pair_id = find_dedust_ton_pair_for_token(token_address)
        dex = "dedust"

    # Not yet on DEX => WATCH (pending)
    if not pair_id:
        watch_id = f"{source}:{token_address}"
        DATA.setdefault("watch", {})
        DATA["watch"][watch_id] = {
            "source": source,
            "symbol": symbol,
            "token_address": token_address,
            "blum_slug": blum_slug,
            "telegram": tg_link,
            "raw": raw_input,
            "approved_early": False,  # NEW (approve once for early blum)
            "added_ts": int(time.time()),
        }
        save_data()

        # If configured inside a group, store mirror settings now (pair_id will be filled when activated)
        if chat and chat.type in ("group", "supergroup"):
            DATA.setdefault("group_mirrors", {})
            DATA["group_mirrors"][str(chat.id)] = {
                "symbol": symbol,
                "token_address": token_address,
                "pair_id": None,
                "dex": None,
                "telegram": tg_link,
                "updated_ts": int(time.time()),
            }
            save_data()

        note = ""
        if source == "blum":
            note = (
                "\n\n💡 If this is Blum and you want early posts:\n"
                f"/approve {watch_id}"
            )

        await update.message.reply_text(
            f"🟡 Saved {symbol} as WATCHING (not yet on DEX)\n"
            f"Source: <b>{source}</b>\n"
            f"Token: <code>{token_address}</code>\n"
            f"Telegram: {tg_link or 'AUTO (DexScreener) / NONE'}\n"
            f"Status: <b>PENDING</b>\n\n"
            f"✅ Bot will auto-activate when it lists on STON.fi or DeDust."
            f"{note}",
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        return

    # On DEX => add to pairs
    old = DATA["pairs"].get(pair_id, {})
    ton_leg = ensure_pair_ton_leg(pair_id)
    meta = fetch_pair_meta(pair_id)
    dex_label = None
    if dex == "dedust":
        dex_label = "DeDust"
    elif isinstance(meta, dict):
        dex_label = dex_label_from_dex_id(meta.get("dex_id") or "")
    DATA["pairs"][pair_id] = {
        "symbol": symbol,
        "token_address": token_address,
        "telegram": tg_link or old.get("telegram"),
        "dex": dex,
        "dex_label": dex_label or old.get("dex_label") or ("DeDust" if dex == "dedust" else "STON.fi"),
        "ton_leg": ton_leg,
        "pool": pair_id,
        "buyers": old.get("buyers", {}) if isinstance(old.get("buyers"), dict) else {},
    }
    # If configured inside a group, store mirror settings for that group
    if chat and chat.type in ("group", "supergroup"):
        DATA.setdefault("group_mirrors", {})
        DATA["group_mirrors"][str(chat.id)] = {
            "symbol": symbol,
            "token_address": token_address,
            "pair_id": pair_id,
            "dex": dex,
            "telegram": tg_link,
            "updated_ts": int(time.time()),
        }
        save_data()

    save_data()

    await update.message.reply_text(
        f"✅ Added {symbol}\n"
        f"DEX: <b>{dex}</b>\n"
        f"Token: <code>{token_address}</code>\n"
        f"Pair/Pool: <code>{pair_id}</code>\n"
        f"Telegram: {tg_link or 'AUTO (DexScreener) / NONE'}\n\n"
        f"Notes:\n"
        f"• STON.fi buys post via STON events feed.\n"
        f"• DeDust buys post via DeDust trades API.",
        parse_mode="HTML",
        disable_web_page_preview=True
    )

async def watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    load_data()
    watch = DATA.get("watch", {})
    if not isinstance(watch, dict) or not watch:
        await update.message.reply_text("Watchlist empty.", disable_web_page_preview=True)
        return

    lines = ["🟡 <b>WATCHLIST</b>\n"]
    for wid, r in watch.items():
        if not isinstance(r, dict):
            continue
        sym = r.get("symbol", "?")
        src = r.get("source", "unknown")
        slug = r.get("blum_slug") or ""
        tok = r.get("token_address") or ""
        approved = "✅" if r.get("approved_early") else "⏳"
        lines.append(
            f"{approved} <b>{sym}</b> | <code>{wid}</code>\n"
            f"   src: {src} {('('+slug+')') if slug else ''}\n"
            f"   token: <code>{tok or 'NONE'}</code>\n"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)

async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /approve <WATCH_ID>", disable_web_page_preview=True)
        return

    wid = context.args[0].strip()
    load_data()
    watch = DATA.get("watch", {})
    if wid not in watch or not isinstance(watch.get(wid), dict):
        await update.message.reply_text("❌ WATCH_ID not found. Use /watchlist", disable_web_page_preview=True)
        return

    rec = watch[wid]
    rec["approved_early"] = True

    # must have token_address for early tracking
    if not (rec.get("token_address") or "").strip():
        save_data()
        await update.message.reply_text(
            "✅ Approved.\n⚠️ But token_address is missing.\nUse /setaddr first:\n/setaddr <WATCH_ID> <JETTON_ADDRESS>",
            disable_web_page_preview=True
        )
        return

    save_data()
    await update.message.reply_text(
        f"✅ Approved {rec.get('symbol','?')} for early posting.\n"
        f"Bot will now post buys automatically (no more approval prompts).",
        disable_web_page_preview=True
    )

async def setaddr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /setaddr <WATCH_ID or blum:memepadjetton_xxx> <JETTON_ADDRESS>", disable_web_page_preview=True)
        return

    key = context.args[0].strip()
    jetton = context.args[1].strip()

    if not TON_ADDR_RE.match(jetton):
        await update.message.reply_text("❌ Invalid jetton address (must start with EQ.. or UQ..)", disable_web_page_preview=True)
        return

    load_data()
    watch = DATA.get("watch", {})
    if not isinstance(watch, dict) or not watch:
        await update.message.reply_text("Watchlist empty.", disable_web_page_preview=True)
        return

    # allow input like: blum:memepadjetton_XXX
    target_wid = None
    if key in watch:
        target_wid = key
    else:
        # search by "source:slug" or by slug inside record
        # common key you typed: blum:memepadjetton_LUCKYSX_EhzZT
        for wid, rec in watch.items():
            if not isinstance(rec, dict):
                continue
            if wid == key:
                target_wid = wid
                break
            if (rec.get("source") == "blum") and (("blum:" + (rec.get("blum_slug") or "")) == key):
                target_wid = wid
                break
            if (rec.get("blum_slug") or "") == key.replace("blum:", ""):
                target_wid = wid
                break

    if not target_wid:
        await update.message.reply_text("❌ Could not find that watch entry. Use /watchlist", disable_web_page_preview=True)
        return

    watch[target_wid]["token_address"] = jetton
    save_data()

    await update.message.reply_text(
        f"✅ Set token address\n"
        f"WATCH_ID: <code>{target_wid}</code>\n"
        f"Jetton: <code>{jetton}</code>\n\n"
        f"If this is Blum and you want early posts:\n"
        f"/approve {target_wid}",
        parse_mode="HTML",
        disable_web_page_preview=True
    )

async def edittg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /edittg <PAIR_ID> <TELEGRAM_LINK>")
        return

    pair_id = context.args[0].strip()
    tg_link = normalize_url(context.args[1].strip())

    load_data()
    if pair_id not in DATA.get("pairs", {}):
        await update.message.reply_text("❌ Pair not found. Use /listpairs.")
        return

    DATA["pairs"][pair_id]["telegram"] = tg_link
    save_data()
    await update.message.reply_text(f"✅ Updated TG for {pair_id}\n{tg_link}", disable_web_page_preview=True)

async def delpair(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /delpair <PAIR_ID>")
        return
    pair_id = context.args[0].strip()
    load_data()
    if pair_id in DATA.get("pairs", {}):
        DATA["pairs"].pop(pair_id, None)
        save_data()
        await update.message.reply_text("✅ Removed pair.", disable_web_page_preview=True)
    else:
        await update.message.reply_text("Pair not found.", disable_web_page_preview=True)

async def listpairs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load_data()
    pairs = DATA.get("pairs", {})
    if not pairs:
        await update.message.reply_text("No pairs tracked.", disable_web_page_preview=True)
        return
    text = "📡 Tracked:\n\n"
    for pid, d in pairs.items():
        sym = d.get("symbol", "?")
        tok = d.get("token_address", "")
        tg = d.get("telegram") or ""
        dex = d.get("dex", "stonfi")
        text += f"• {sym} — <code>{pid}</code> (<b>{dex}</b>)\n"
        if tok:
            text += f"   token: <code>{tok}</code>\n"
        if tg:
            text += f"   tg: {tg}\n"
    await update.message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)

async def setleaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load_state()
    msg = await context.bot.send_message(
        chat_id=CHANNEL_ID,
        text="🟢 <b>SPYTON TRENDING</b> 💎\n\n(No data yet)",
        parse_mode="HTML",
        reply_markup=leaderboard_button(),
        disable_web_page_preview=True
    )
    STATE["leaderboard_msg_id"] = msg.message_id
    save_state()
    await update.message.reply_text("✅ Leaderboard created. Pin it in the channel.", disable_web_page_preview=True)
    await update_leaderboard(context)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load_data()
    load_state()
    watch_count = len(DATA.get("watch", {})) if isinstance(DATA.get("watch"), dict) else 0
    approved_blum = 0
    if isinstance(DATA.get("watch"), dict):
        for _wid, r in DATA["watch"].items():
            if isinstance(r, dict) and (r.get("source") == "blum") and r.get("approved_early"):
                approved_blum += 1

    await update.message.reply_text(
        f"Tracked pairs: {len(DATA.get('pairs',{}))}\n"
        f"Watchlist: {watch_count}\n"
        f"Blum approved: {approved_blum}\n"
        f"Leaderboard: {'SET' if STATE.get('leaderboard_msg_id') else 'NOT SET'}\n"
        f"STON last block: {STATE.get('ston_last_block') if STATE.get('ston_last_block') is not None else 'NOT SET'}\n"
        f"Events pulled last: {LAST_EVENTS_COUNT}\n"
        f"HTTP: {LAST_HTTP_INFO}\n"
        f"Header image: {'FOUND' if file_exists(HEADER_IMAGE_PATH) else 'MISSING'} ({HEADER_IMAGE_PATH})\n"
        f"TONAPI_KEY: {'SET' if TONAPI_KEY else 'NOT SET'}\n"
        f"DeDust enabled: {'YES' if DEDUST_ENABLED else 'NO'}\n"
        f"DeDust pools tracked: {sum(1 for _pid, rec in DATA.get('pairs', {}).items() if str(rec.get('dex','')).lower()=='dedust')}\n"
        f"Blum early enabled: {'YES' if BLUM_EARLY_ENABLED else 'NO'}\n"
        f"\nLeaderboard filters:\n"
        f"LB_MIN_LIQ_USD: {LB_MIN_LIQ_USD}\n"
        f"LB_MIN_MC_USD: {LB_MIN_MC_USD}\n"
        f"LB_WHALE_MC_USD: {LB_WHALE_MC_USD}\n"
        f"LB_SPLIT_SECTIONS: {'YES' if LB_SPLIT_SECTIONS else 'NO'}\n",
        disable_web_page_preview=True
    )

# ===================== MAIN =====================

# ===================== JOBS =====================
async def ton_price_cache_job(context: ContextTypes.DEFAULT_TYPE):
    """Keep TON USD price cached so buy posts don't wait on external API."""
    if not TON_PRICE_API:
        return
    try:
        await _to_thread(refresh_ton_price_cache)
    except Exception:
        return

async def auto_ranks_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        await _to_thread(refresh_auto_ranks, True)
    except Exception:
        pass

async def ston_tracker_job(context: ContextTypes.DEFAULT_TYPE):
    """Poll STON exported events feed and post BUY-ONLY swaps for tracked STON pairs."""

    # FAST PATH: TonAPI pool tx polling (lower latency than export feed)
    if TONAPI_KEY:
        try:
            await ston_tracker_job_fast(context)
            return
        except Exception as e:
            log.exception("ston_tracker_job_fast failed, falling back: %s", e)
    try:
        cleanup_seen()
        load_data()
        load_state()

        latest = await _to_thread(ston_latest_block)
        if not latest:
            return

        last = STATE.get("ston_last_block")
        if not isinstance(last, int) or last <= 0:
            # First run: start near tip so we don't spam old history
            STATE["ston_last_block"] = max(0, int(latest) - 2)
            save_state()
            return

        from_block = int(last) + 1
        to_block = int(latest)

        # Avoid huge ranges
        if to_block - from_block > 50:
            from_block = to_block - 50

        evs = await _to_thread(ston_events, from_block, to_block)
        STATE["ston_last_block"] = to_block
        save_state()

        if not evs:
            return

        for ev in evs:
            if not isinstance(ev, dict):
                continue

            buy = extract_buy_from_ston_event(ev)
            if not buy:
                continue

            tx = buy.get("tx") or ""
            if not tx or tx in SEEN_TX_STON:
                continue

            pair_id = buy["pair_id"]
            rec = DATA["pairs"].get(pair_id, {})
            sym = (rec.get("symbol") or "?").strip().upper()
            token_addr = (rec.get("token_address") or "").strip()

            buyer = buy.get("buyer") or ""
            ton_amt = safe_float(buy.get("ton"))
            token_amt = safe_float(buy.get("token_amt"))

            # Position = New/Existing holder (based on seen buyers)
            buyers_map = rec.get("buyers")
            if not isinstance(buyers_map, dict):
                buyers_map = {}
                rec["buyers"] = buyers_map

            pos_txt = "New Holder!" if buyer and buyer not in buyers_map else "Existing Holder"
            if buyer:
                buyers_map[buyer] = int(buyers_map.get(buyer, 0)) + 1
                save_data()

            SEEN_TX_STON[tx] = time.time()

            # Post message with header
            await post_buy_message(
                context=context,
                sym=sym,
                token_addr=token_addr,
                pair_id=pair_id,
                buyer=buyer,
                tx_hash=tx,
                ton_amt=ton_amt,
                token_amt=token_amt,
                pos_txt=pos_txt,
                source_label=(rec.get("dex_label") or "STON.fi"),
            )
    except Exception as e:
        log.exception("ston_tracker_job error: %s", e)



async def dedust_tracker_job(context: ContextTypes.DEFAULT_TYPE):
    """Poll DeDust trades via the public DeDust API and post BUY-ONLY swaps.

    Fixes:
    - No more cursor based only on timestamp (multiple trades can share the same second -> missed buys)
    - Uses last seen trade id/hash per pool
    - Network calls run in a thread (requests won't block the asyncio loop -> less lag)
    """
    if not DEDUST_ENABLED:
        return
    try:
        cleanup_seen()
        load_data()
        load_state()

        last_id_map = STATE.get("dedust_last_id")
        if not isinstance(last_id_map, dict):
            last_id_map = {}
            STATE["dedust_last_id"] = last_id_map

        # keep legacy key present but unused
        STATE.setdefault("dedust_last_lt", {})

        # Collect pools first so we can fetch in parallel (prevents multi-minute loops)
        pools: List[Tuple[str, Dict[str, Any], str, str]] = []  # (pool, rec, sym, token_addr)
        for pool, rec in DATA.get("pairs", {}).items():
            if not isinstance(rec, dict):
                continue
            if str(rec.get("dex", "")).lower() != "dedust":
                continue
            sym = (rec.get("symbol") or "?").strip().upper()
            token_addr = (rec.get("token_address") or "").strip()
            if not token_addr:
                continue
            pools.append((pool, rec, sym, token_addr))

        if not pools:
            return

        sem = asyncio.Semaphore(int(os.getenv("DEDUST_CONCURRENCY", "16")))

        async def _fetch_pool(pool_addr: str):
            async with sem:
                trades = await _to_thread(dedust_fetch_trades, pool_addr, DEDUST_POLL_LIMIT)
                return pool_addr, trades

        results = await asyncio.gather(*[asyncio.create_task(_fetch_pool(p[0])) for p in pools], return_exceptions=True)
        trades_by_pool: Dict[str, List[Dict[str, Any]]] = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            pool_addr, trades = r
            if isinstance(pool_addr, str) and isinstance(trades, list):
                trades_by_pool[pool_addr] = [t for t in trades if isinstance(t, dict)]

        for pool, rec, sym, token_addr in pools:
            last_id = str(last_id_map.get(pool, "") or "").strip()
            trades = trades_by_pool.get(pool) or []
            if not trades:
                continue

            
            # Collect trades newer than last_id (walk newest->oldest until we hit last_id)
            fresh: List[Dict[str, Any]] = []
            for t in trades:
                if not isinstance(t, dict):
                    continue
                tid = _trade_cursor_id(t)
                if last_id and tid and tid == last_id:
                    break
                fresh.append(t)

            if not fresh:
                newest_tid = _trade_cursor_id(trades[0]) if isinstance(trades[0], dict) else ""
                if newest_tid and newest_tid != last_id:
                    last_id_map[pool] = newest_tid
                    save_state()
                continue

            # Post in chronological order (oldest -> newest)
            fresh = list(reversed(fresh))

            newest_tid = _trade_cursor_id(trades[0]) if isinstance(trades[0], dict) else ""
            if newest_tid:
                last_id_map[pool] = newest_tid
                save_state()

            for t in fresh:
                a_in = t.get("assetIn") or t.get("asset_in") or t.get("inAsset") or t.get("in_asset") or {}
                a_out = t.get("assetOut") or t.get("asset_out") or t.get("outAsset") or t.get("out_asset") or {}
                amt_in = t.get("amountIn") or t.get("amount_in") or t.get("inAmount") or t.get("in_amount") or 0
                amt_out = t.get("amountOut") or t.get("amount_out") or t.get("outAmount") or t.get("out_amount") or 0

                # BUY = TON -> Jetton (our token)
                if not is_ton_asset(a_in):
                    continue

                out_addr = extract_jetton_master(a_out)
                if out_addr and out_addr != token_addr:
                    continue

                ton_amt = safe_float(amt_in)
                if ton_amt > 1e6:
                    ton_amt = ton_amt / 1e9

                dec = get_jetton_decimals(token_addr)
                token_amt = safe_float(amt_out)
                if token_amt > 10 ** (dec + 1):
                    token_amt = token_amt / (10 ** dec)

                h = _trade_cursor_id(t)
                if not h:
                    ts = t.get("timestamp") or t.get("time") or t.get("createdAt") or t.get("created_at") or ""
                    h = f"dedust:{pool}:{ts}:{ton_amt}:{token_amt}"
                txh = _trade_tx_hash(t)
                if h in SEEN_TX_DEDUST:
                    continue

                buyer = (t.get("sender") or t.get("trader") or t.get("buyer") or t.get("from") or "").strip()

                buyers_map = rec.get("buyers")
                if not isinstance(buyers_map, dict):
                    buyers_map = {}
                    rec["buyers"] = buyers_map

                pos_txt = "New Holder!" if buyer and buyer not in buyers_map else "Existing Holder"
                if buyer:
                    buyers_map[buyer] = int(buyers_map.get(buyer, 0)) + 1
                    save_data()

                SEEN_TX_DEDUST[h] = __import__('time').time()

                await post_buy_message(
                    context=context,
                    sym=sym,
                    token_addr=token_addr,
                    pair_id=pool,
                    buyer=buyer,
                    tx_hash=txh,
                    ton_amt=ton_amt,
                    token_amt=token_amt,
                    pos_txt=pos_txt,
                    source_label=(rec.get("dex_label") or "DeDust"),
                )

    except Exception as e:
        log.exception("dedust_tracker_job error: %s", e)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN")
    if CHANNEL_ID == 0:
        raise RuntimeError("Missing CHANNEL_ID")
    if ADMIN_ID == 0:
        raise RuntimeError("Missing ADMIN_ID")

    # Start web server once (for UptimeRobot / Replit public URL)
    global _WEB_STARTED
    try:
        _WEB_STARTED
    except NameError:
        _WEB_STARTED = False

    if not _WEB_STARTED:
        try:
            t = threading.Thread(target=run_web, daemon=True)
            t.start()
            _WEB_STARTED = True
        except Exception as e:
            log.exception("Failed to start web server: %s", e)

    # Keep-alive self ping (helps Replit stay awake)
    global _PING_STARTED
    try:
        _PING_STARTED
    except NameError:
        _PING_STARTED = False

    PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()
    if PUBLIC_URL and not _PING_STARTED:
        def _self_ping_loop():
            # ping both endpoints for reliability
            base = PUBLIC_URL.rstrip("/")
            urls = [base + "/uptimerobot", base + "/health"]
            while True:
                for url in urls:
                    try:
                        requests.get(url, timeout=10)
                    except Exception:
                        pass
                time.sleep(240)  # 4 minutes

        threading.Thread(target=_self_ping_loop, daemon=True).start()
        _PING_STARTED = True

    # Resilient runner: if anything crashes, restart polling
    while True:
        try:
            load_data()
            load_state()

            bot = ApplicationBuilder().token(BOT_TOKEN).build()

            bot.add_handler(CommandHandler("start", start))
            bot.add_handler(CommandHandler("addtoken", addtoken))
            bot.add_handler(CommandHandler("setrank", setrank))
            bot.add_handler(CommandHandler("clearrank", clearrank))
            bot.add_handler(CommandHandler("ranks", ranks))
            bot.add_handler(CommandHandler("watchlist", watchlist))
            bot.add_handler(CommandHandler("approve", approve))
            bot.add_handler(CommandHandler("setaddr", setaddr))
            bot.add_handler(CommandHandler("edittg", edittg))
            bot.add_handler(CommandHandler("delpair", delpair))
            bot.add_handler(CommandHandler("listpairs", listpairs))
            bot.add_handler(CommandHandler("setleaderboard", setleaderboard))
            bot.add_handler(CommandHandler("status", status))

            # Warm TON price cache (so posts are instant)
            bot.job_queue.run_repeating(ton_price_cache_job, interval=60, first=1)

            # Auto ranks (volume-based)
            bot.job_queue.run_repeating(auto_ranks_job, interval=AUTO_RANK_INTERVAL, first=3)

            # Leaderboard auto-update
            bot.job_queue.run_repeating(update_leaderboard, interval=LB_UPDATE_INTERVAL, first=10)

            # Trackers
            bot.job_queue.run_repeating(ston_tracker_job, interval=STON_POLL_INTERVAL, first=2)
            bot.job_queue.run_repeating(dedust_tracker_job, interval=DEDUST_POLL_INTERVAL, first=5)
            bot.job_queue.run_repeating(memepad_activation_job, interval=MEMEPAD_ACTIVATION_INTERVAL, first=10)
            bot.job_queue.run_repeating(blum_early_tracker_job, interval=BLUM_POLL_INTERVAL, first=12)

            print("🟢 SpyTON Detector running…")
            bot.run_polling()
            break
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.exception("Bot crashed, restarting in 5s: %s", e)
            time.sleep(5)
            continue
if __name__ == "__main__":
    main()