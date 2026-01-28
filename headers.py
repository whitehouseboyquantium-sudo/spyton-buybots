import json
import os

FILE = "headers.json"


def _load():
    if not os.path.exists(FILE):
        return {}
    with open(FILE, "r") as f:
        return json.load(f)


def _save(d):
    with open(FILE, "w") as f:
        json.dump(d, f, indent=2)


def set_header(symbol: str, file_id: str):
    d = _load()
    d[symbol.upper()] = file_id
    _save(d)


def get_header(symbol: str):
    d = _load()
    return d.get(symbol.upper())