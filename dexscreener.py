import requests

DEX_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"


def check_token_buys(token_address: str, min_usd: float):
    """
    Returns list of buy-like items from DexScreener token endpoint.
    We keep it conservative: we just pull pair metrics and simulate a 'buy'
    based on latest volume changes.
    If your current version already works, keep yours.
    """
    try:
        r = requests.get(DEX_URL.format(token_address), timeout=10)
        if r.status_code != 200:
            return []

        data = r.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return []

        # choose best TON pair by liquidity
        best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))

        # DexS doesn't give exact tx here; your old logic uses volumeUsd as a trigger.
        volume_usd = float((best.get("volume") or {}).get("h24", 0) or 0)
        price_usd = float(best.get("priceUsd") or 0)
        liquidity = float((best.get("liquidity") or {}).get("usd", 0) or 0)
        fdv = float(best.get("fdv") or 0)
        url = best.get("url") or ""

        # basic spam gate
        if volume_usd < float(min_usd):
            return []

        return [{
            "volumeUsd": volume_usd,
            "priceUsd": price_usd,
            "liquidity": liquidity,
            "fdv": fdv,
            "url": url
        }]

    except Exception:
        return []