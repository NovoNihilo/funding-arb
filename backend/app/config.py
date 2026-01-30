"""
Configuration module - all settings from environment variables (.env)
Single source of truth for all configurable values.
"""
from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(self):
        # ============ Venues ============
        self.enabled_venues = self._csv("ENABLED_VENUES", "hyperliquid,paradex,grvt,extended,variational")
        
        # ============ Symbols ============
        env_symbols = os.getenv("SYMBOLS", "")
        if env_symbols.strip():
            self.symbols = self._csv("SYMBOLS", "BTC,ETH")
        else:
            from app.symbols_config import get_all_symbols
            self.symbols = get_all_symbols()
        
        # ============ Timing ============
        self.fetch_interval_seconds = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
        self.leaderboard_interval_seconds = int(os.getenv("LEADERBOARD_INTERVAL_SECONDS", "1800"))
        self.venue_timeout_seconds = int(os.getenv("VENUE_TIMEOUT_SECONDS", "15"))
        
        # ============ Thresholds ============
        self.fee_buffer = float(os.getenv("FEE_BUFFER", "0.0001"))
        self.established_min_spread = float(os.getenv("ESTABLISHED_MIN_SPREAD", "0.0001"))
        self.emerging_min_spread = float(os.getenv("EMERGING_MIN_SPREAD", "0.0002"))
        self.established_min_hours = float(os.getenv("ESTABLISHED_MIN_HOURS", "48"))
        
        # ============ Telegram ============
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_free_channel_id = os.getenv("TELEGRAM_FREE_CHANNEL_ID", "")
        self.telegram_pro_channel_id = os.getenv("TELEGRAM_PRO_CHANNEL_ID", "")
        
        # ============ Database ============
        self.database_url = os.getenv("DATABASE_URL", "sqlite:////data/funding_arb.db")
        
        # ============ Logging ============
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_venue_details = os.getenv("LOG_VENUE_DETAILS", "false").lower() == "true"

    def _csv(self, key: str, default: str) -> list[str]:
        return [v.strip() for v in os.getenv(key, default).split(",") if v.strip()]
    
    def validate(self) -> list[str]:
        """Validate settings and return list of warnings."""
        warnings = []
        if not self.telegram_bot_token:
            warnings.append("TELEGRAM_BOT_TOKEN not set - alerts will not be sent")
        if not self.telegram_free_channel_id:
            warnings.append("TELEGRAM_FREE_CHANNEL_ID not set - alerts will not be sent")
        if self.fetch_interval_seconds < 30:
            warnings.append(f"FETCH_INTERVAL_SECONDS={self.fetch_interval_seconds} is very low")
        return warnings


@lru_cache
def get_settings() -> Settings:
    return Settings()
