"""Funding Arbitrage Alert Bot - Main Application"""
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.scheduler import init_scheduler, shutdown_scheduler, leaderboard_job
from app.db.init_db import init_db
from app.db.repository import get_latest_funding_by_symbol, get_recent_events


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


app = FastAPI(title="Funding Arb Alerts", version="1.0.0", lifespan=lifespan)


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
