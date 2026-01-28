"""SpyTON BuyBot message formatter.

This formatter is designed to match the style in your screenshot:

TON TRENDING
🦀 HOOLI Buy!
↔ 5.00 TON
↔ 15.690000 HOOLI
👤 EQD1...epl7 | Txn
⬆️ Position: New Holder!
💰 Market Cap ...
🌊 Liquidity ...

And buttons:
Chart | Trending | Pools
Book Trending
"""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def usd(v: float) -> str:
    return f"${v:,.2f}"


def short_addr(a: str, left: int = 4, right: int = 4) -> str:
    if not a:
        return ""
    a = str(a)
    if len(a) <= left + right + 3:
        return a
    return f"{a[:left]}...{a[-right:]}"


def _num(v: float | None, decimals: int = 2) -> str:
    if v is None:
        return ""
    return f"{v:,.{decimals}f}"


def build_buy_message(
    symbol: str,
    volume_usd: float,
    price_usd: float,
    liquidity_usd: float,
    mcap_usd: float,
    pair_url: str,
    # Optional data (if you have real swap parsing later)
    amount_ton: float | None = None,
 c 
    buyer: str = "",b
    tx_url:  str = "",
    position: str = "",
    # Button URLs
    trending_url: str = "https://t.me/SpyTonTrending",
    pools_url: str | None = None,
    # Book Trending button (force correct casing)
    book_trending_url: str = "https://t.me/SpyTONTrndBot",
):
    """Return (html_text, InlineKeyboardMarkup)"""

    symbol = (symbol or "").upper()

    # ---------- lines ----------
    title_line = f"<b>TON TRENDING</b>\n🦀 <b>{symbol} Buy!</b>\n"

    # Amount lines (TON + token) – only show when provided
    amount_lines = ""
    if amount_ton is not None:
        amount_lines += f"↔ <b>{_num(amount_ton, 2)} TON</b>\n"
    else:
        # fallback so it never looks empty
        amount_lines += f"↔ <b>{usd(volume_usd)}</b>\n"

    if amount_token is not None:
        amount_lines += f"↔ <b>{_num(amount_token, 6)} {symbol}</b>\n"

    buyer_short = short_addr(buyer) if buyer else "Wallet"
    # Make wallet clickable when tx_url exists
    if tx_url:
        wallet_line = f"👤 <a href=\"{tx_url}\">{buyer_short}</a> | <a href=\"{tx_url}\">Txn</a>\n"
    else:
        wallet_line = f"👤 {buyer_short} | Txn\n"

    # Position line
    pos_line = ""
    if position:
        # expected values: "New Holder!" / "Existing Holder"
        pos_line = f"⬆️ Position: <b>{position}</b>\n"

    stats_lines = (
        f"💰 Market Cap <b>{usd(mcap_usd)}</b>\n"
        f"🌊 Liquidity <b>{usd(liquidity_usd)}</b>"
    )

    text = title_line + "\n" + amount_lines + wallet_line + pos_line + stats_lines

    # ---------- buttons ----------
    pools_url = pools_url or pair_url

    buttons = InlineKeyboardMarkup(
        [
                       [InlineKeyboardButton("Book Trending", url=book_trending_url)],
        ]
    )

    return text, buttons
