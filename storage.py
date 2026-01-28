import json
import os

TOKENS_FILE = "tokens.json"
HEADERS_FILE = "headers.json"
SEEN_FILE = "seen.json"


def load_tokens():
    if not os.path.exists(TOKENS_FILE):
        return {}
    with open(TOKENS_FILE, "r") as f:
        return json.load(f)


def save_tokens(tokens: dict):
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f, indent=2)


def _load_headers():
    if not os.path.exists(HEADERS_FILE):
        return {}
    with open(HEADERS_FILE, "r") as f:
        return json.load(f)


def _save_headers(h: dict):
    with open(HEADERS_FILE, "w") as f:
        json.dump(h, f, indent=2)


def set_header_file_id(symbol: str, file_id: str):
    h = _load_headers()
    h[symbol.upper()] = file_id
    _save_headers(h)


def get_header_file_id(symbol: str):
    h = _load_headers()
    return h.get(symbol.upper())


def _load_seen():
    if not os.path.exists(SEEN_FILE):
        return {}
    with open(SEEN_FILE, "r") as f:
        return json.load(f)


def _save_seen(s: dict):
    with open(SEEN_FILE, "w") as f:
        json.dump(s, f, indent=2)


def is_new_buy(symbol: str, volume_usd: float) -> bool:
    """
    Your original logic used volume as the key.
    We keep it (so we don't break your working flow),
    but store it persistently so restarts don’t repeat.
    """
    symbol = symbol.upper()
    seen = _load_seen()
    last = seen.get(symbol)

    key = f"{float(volume_usd):.6f}"
    if last == key:
        return False

    seen[symbol] = key
    _save_seen(seen)
    return True