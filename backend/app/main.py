"""Funding Arbitrage Alert Bot - Main Application"""
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.scheduler import init_scheduler, shutdown_scheduler, leaderboard_job
from app.db.init_db import init_db
from app.db.repository import get_latest_funding_by_symbol, get_recent_events
from app.telegram_bot import TelegramBot, handle_callback, get_leaderboard_state


def log(msg: str, level: str = "INFO"):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log("========== Starting Funding Arb Bot ==========")
    try:
        init_db()
        log("Database initialized")
        
        settings = get_settings()
        log(f"Venues: {settings.enabled_venues}")
        log(f"Symbols: {len(settings.symbols)} configured")
        log(f"Timing: fetch={settings.fetch_interval_seconds}s, leaderboard={settings.leaderboard_interval_seconds}s")
        
        for warning in settings.validate():
            log(f"CONFIG WARNING: {warning}", level="WARN")
        
        # Set up webhook if URL is configured
        webhook_url = settings.telegram_webhook_url
        if webhook_url and settings.telegram_bot_token:
            bot = TelegramBot(settings.telegram_bot_token)
            await bot.set_webhook(f"{webhook_url}/webhook/telegram")
            log(f"Webhook set: {webhook_url}/webhook/telegram")
        
        init_scheduler(settings.fetch_interval_seconds, settings.leaderboard_interval_seconds)
        log("========== Startup Complete ==========")
    except Exception as e:
        log(f"STARTUP ERROR: {e}", level="ERROR")
        import traceback
        traceback.print_exc()
        raise
    
    yield
    
    log("========== Shutting Down ==========")
    shutdown_scheduler()


app = FastAPI(title="Funding Arb Alerts", version="2.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/config")
def get_config():
    settings = get_settings()
    return {
        "venues": settings.enabled_venues,
        "symbol_count": len(settings.symbols),
        "fetch_interval_seconds": settings.fetch_interval_seconds,
        "leaderboard_interval_seconds": settings.leaderboard_interval_seconds,
        "established_min_spread": settings.established_min_spread,
        "emerging_min_spread": settings.emerging_min_spread,
        "telegram_configured": bool(settings.telegram_bot_token and settings.telegram_free_channel_id),
        "webhook_configured": bool(settings.telegram_webhook_url),
    }


@app.get("/snapshots/latest")
def snapshots_latest():
    settings = get_settings()
    data = get_latest_funding_by_symbol(settings.symbols)
    return {"snapshots": data}


@app.get("/events/recent")
def events_recent(limit: int = Query(default=50, ge=1, le=500)):
    events = get_recent_events(limit=limit)
    return {
        "count": len(events),
        "events": [
            {
                "id": e.id, "ts": e.ts.isoformat(), "symbol": e.symbol,
                "short_venue": e.short_venue, "long_venue": e.long_venue,
                "spread": e.spread, "net_spread": e.net_spread,
            }
            for e in events
        ],
    }


@app.post("/leaderboard/send")
async def send_leaderboard_now():
    log("[api] Manual leaderboard trigger")
    try:
        await leaderboard_job()
        return {"status": "ok", "message": "Leaderboard job completed"}
    except Exception as e:
        log(f"[api] Error: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """
    Webhook endpoint for Telegram bot updates.
    Handles inline keyboard button callbacks.
    """
    try:
        update = await request.json()
        
        # Handle callback queries (button presses)
        if "callback_query" in update:
            settings = get_settings()
            bot = TelegramBot(settings.telegram_bot_token)
            await handle_callback(bot, update["callback_query"])
        
        return {"ok": True}
    
    except Exception as e:
        log(f"[webhook] Error: {e}", level="ERROR")
        return {"ok": False, "error": str(e)}


@app.get("/leaderboard/state")
def get_state():
    """Debug endpoint to check current leaderboard state."""
    state = get_leaderboard_state()
    return {
        "last_updated": state.last_updated.isoformat() if state.last_updated else None,
        "established_symbols": list(state.established.keys()),
        "emerging_symbols": list(state.emerging.keys()),
        "message_id": state.message_id,
        "chat_id": state.chat_id,
    }
