from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    def __init__(self):
        self.enabled_venues = self._csv("ENABLED_VENUES", "mock")
        
        env_symbols = os.getenv("SYMBOLS", "")
        if env_symbols.strip():
            self.symbols = self._csv("SYMBOLS", "BTC,ETH")
        else:
            from app.symbols_config import get_all_symbols
            self.symbols = get_all_symbols()
        
        self.fee_buffer = float(os.getenv("FEE_BUFFER", "0.0001"))
        
        # Tier thresholds
        self.established_min_spread = float(os.getenv("ESTABLISHED_MIN_SPREAD", "0.0001"))  # 0.01%
        self.emerging_min_spread = float(os.getenv("EMERGING_MIN_SPREAD", "0.0002"))  # 0.02%
        self.established_min_hours = float(os.getenv("ESTABLISHED_MIN_HOURS", "48"))  # 48 hours
        
        # Legacy
        self.min_net_spread = float(os.getenv("MIN_NET_SPREAD", "0.0001"))
        self.cooldown_seconds = int(os.getenv("COOLDOWN_SECONDS", "1800"))
        
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_free_channel_id = os.getenv("TELEGRAM_FREE_CHANNEL_ID", "")
        self.telegram_pro_channel_id = os.getenv("TELEGRAM_PRO_CHANNEL_ID", "")
        
        # Timing - leaderboard now every 30 minutes
        self.fetch_interval_seconds = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
        self.leaderboard_interval_seconds = int(os.getenv("LEADERBOARD_INTERVAL_SECONDS", "1800"))  # 30 min
        
        self.database_url = os.getenv("DATABASE_URL", "sqlite:////data/funding_arb.db")

    def _csv(self, key: str, default: str) -> list[str]:
        return [v.strip() for v in os.getenv(key, default).split(",") if v.strip()]

@lru_cache
def get_settings() -> Settings:
    return Settings()
