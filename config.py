import os

# =========================
# Telegram
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")  # set in Replit Secrets
TRENDING_CHANNEL = "@SpyTonTrending"  # or channel ID

# =========================
# Buy filter
# =========================
MIN_USD_BUY = 5        # minimum buy size in USD
POLL_INTERVAL = 8      # seconds

# =========================
# UI / Style
# =========================
AD_TEXT = "You can book an ad here"
TRENDING_URL = "https://t.me/SpyTonTrending"

EMOJI = "🟢"
USD_PER_EMOJI = 15
MAX_EMOJI = 60