from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from app.config import get_settings
from app.scheduler import init_scheduler, shutdown_scheduler
from app.db.init_db import init_db
from app.db.repository import get_latest_funding_by_symbol, get_recent_events


def log(msg: str):
    print(msg, flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log("[main] Starting up...")
    try:
        init_db()
        log("[main] DB initialized")
        settings = get_settings()
        log(f"[main] Settings loaded: venues={settings.enabled_venues}, symbols count={len(settings.symbols)}")
        log(f"[main] Thresholds: established={settings.established_min_spread}, emerging={settings.emerging_min_spread}")
        log(f"[main] Timing: fetch={settings.fetch_interval_seconds}s, leaderboard={settings.leaderboard_interval_seconds}s")
        init_scheduler(settings.fetch_interval_seconds, settings.leaderboard_interval_seconds)
        log("[main] Scheduler initialized")
    except Exception as e:
        log(f"[main] Startup error: {e}")
        import traceback
        traceback.print_exc()
    yield
    shutdown_scheduler()
    log("[main] Shutdown complete")


app = FastAPI(title="Funding Arb Alerts", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


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
                "id": e.id,
                "ts": e.ts.isoformat(),
                "symbol": e.symbol,
                "short_venue": e.short_venue,
                "long_venue": e.long_venue,
                "spread": e.spread,
                "net_spread": e.net_spread,
            }
            for e in events
        ],
    }
