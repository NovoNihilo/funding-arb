"""
Scheduler module - handles periodic data fetching and leaderboard generation.

Key optimizations:
- Job coalescing (skip missed runs instead of queuing)
- Timeout protection on jobs
- Error isolation per venue
"""
import asyncio
from datetime import datetime
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

from app.config import get_settings
from app.venues import get_enabled_connectors
from app.venues.base import FundingData
from app.db.repository import (
    insert_snapshot,
    insert_event,
    insert_spread_history,
    get_extended_spread_stats,
    get_all_active_established,
    upsert_established_position,
    mark_position_exited,
    make_position_key,
)
from app.arb_engine import (
    compute_all_arbs,
    format_leaderboard,
    format_exit_alert,
    LeaderboardEntry,
)
from app.telegram_sender import send_message

scheduler: Optional[AsyncIOScheduler] = None


def log(msg: str, level: str = "INFO"):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


def job_error_listener(event):
    log(f"Job {event.job_id} failed: {event.exception}", level="ERROR")


def job_missed_listener(event):
    log(f"Job {event.job_id} missed - will run at next interval", level="WARN")


async def fetch_single_venue(connector, symbols: list[str], timeout: int) -> tuple[str, dict]:
    """Fetch from single venue with timeout protection."""
    venue_name = connector.venue_name
    try:
        data = await asyncio.wait_for(
            connector.fetch_funding_with_prices(symbols),
            timeout=timeout
        )
        return venue_name, data
    except asyncio.TimeoutError:
        log(f"[{venue_name}] Timeout after {timeout}s", level="WARN")
        return venue_name, {}
    except Exception as e:
        log(f"[{venue_name}] Fetch error: {e}", level="WARN")
        return venue_name, {}


async def fetch_funding_data(connectors, symbols: list[str], timeout: int) -> tuple[dict, dict, dict]:
    """Fetch from all venues concurrently with timeout protection."""
    venue_data = {}
    venue_funding_map = {}
    venue_price_map = {}
    
    tasks = [fetch_single_venue(c, symbols, timeout) for c in connectors]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, Exception):
            log(f"Unexpected error: {result}", level="ERROR")
            continue
        venue_name, data = result
        if data:
            venue_data[venue_name] = data
            venue_funding_map[venue_name] = {s: d.funding_rate for s, d in data.items()}
            venue_price_map[venue_name] = {
                s: d.mark_price for s, d in data.items() if d.mark_price is not None
            }
    
    settings = get_settings()
    if settings.log_venue_details:
        for venue_name, data in venue_data.items():
            log(f"[fetch] {venue_name}: {len(data)} symbols")
    else:
        log(f"[fetch] Got data from {len(venue_data)}/{len(connectors)} venues")
    
    return venue_data, venue_funding_map, venue_price_map


def store_snapshots_with_prices(venue_data: dict[str, dict[str, FundingData]], ts: datetime):
    count = 0
    for venue, symbols in venue_data.items():
        for symbol, data in symbols.items():
            try:
                insert_snapshot(
                    venue=venue, symbol=symbol, funding_rate=data.funding_rate,
                    ts=ts, mark_price=data.mark_price, index_price=data.index_price,
                )
                count += 1
            except Exception as e:
                log(f"Error storing snapshot {venue}/{symbol}: {e}", level="ERROR")
    log(f"[store] {count} snapshots saved")


def store_spread_history(opportunities: list, ts: datetime):
    count = 0
    for opp in opportunities:
        try:
            insert_spread_history(
                symbol=opp.symbol, short_venue=opp.short_venue, long_venue=opp.long_venue,
                spread=opp.spread, net_spread=opp.net_spread, ts=ts,
                price_spread_pct=opp.price_spread_pct,
            )
            count += 1
        except Exception as e:
            log(f"Error storing spread {opp.symbol}: {e}", level="ERROR")
    log(f"[store] {count} spread records saved")


def classify_opportunities(opportunities: list, settings) -> tuple[dict, dict, set]:
    established = {}
    emerging = {}
    established_keys = set()
    
    for opp in opportunities:
        stats = get_extended_spread_stats(
            opp.symbol, opp.short_venue, opp.long_venue,
            settings.established_min_spread, settings.fee_buffer,
        )
        
        if stats and stats["duration_hours"] >= settings.established_min_hours:
            if opp.net_spread >= settings.established_min_spread:
                entry = LeaderboardEntry(
                    opp=opp, duration_hours=stats["duration_hours"], trend=stats["trend"],
                    avg_spread=stats["avg_spread"], min_spread=stats["min_spread"],
                    max_spread=stats["max_spread"], data_points=stats["data_points"],
                    price_spread_pct=stats.get("price_spread_pct"),
                    price_spread_avg=stats.get("price_spread_avg"),
                )
                if opp.symbol not in established:
                    established[opp.symbol] = []
                established[opp.symbol].append(entry)
                
                key = make_position_key(opp.symbol, opp.short_venue, opp.long_venue)
                established_keys.add(key)
                upsert_established_position(opp.symbol, opp.short_venue, opp.long_venue, opp.net_spread)
            continue
        
        stats = get_extended_spread_stats(
            opp.symbol, opp.short_venue, opp.long_venue,
            settings.emerging_min_spread, settings.fee_buffer,
        )
        
        if stats and opp.net_spread >= settings.emerging_min_spread:
            entry = LeaderboardEntry(
                opp=opp, duration_hours=stats["duration_hours"], trend=stats["trend"],
                avg_spread=stats["avg_spread"], min_spread=stats["min_spread"],
                max_spread=stats["max_spread"], data_points=stats["data_points"],
                price_spread_pct=stats.get("price_spread_pct"),
                price_spread_avg=stats.get("price_spread_avg"),
            )
            if opp.symbol not in emerging:
                emerging[opp.symbol] = []
            emerging[opp.symbol].append(entry)
    
    return established, emerging, established_keys


async def check_exit_alerts(established_keys: set, venue_funding_map: dict, settings):
    active_positions = get_all_active_established()
    if not active_positions:
        return
    
    log(f"[exit_alerts] Checking {len(active_positions)} active positions")
    exit_count = 0
    
    for position in active_positions:
        if position.key in established_keys:
            continue
        
        short_rate = venue_funding_map.get(position.short_venue, {}).get(position.symbol)
        long_rate = venue_funding_map.get(position.long_venue, {}).get(position.symbol)
        
        if short_rate is None or long_rate is None:
            continue
        
        current_spread = short_rate - long_rate - settings.fee_buffer
        duration_hours = (datetime.utcnow() - position.established_at).total_seconds() / 3600
        
        msg = format_exit_alert(
            symbol=position.symbol, short_venue=position.short_venue,
            long_venue=position.long_venue, current_spread=current_spread,
            previous_spread=position.last_seen_spread, duration_hours=duration_hours,
        )
        
        if settings.telegram_free_channel_id:
            success = await send_message(settings.telegram_bot_token, settings.telegram_free_channel_id, msg)
            if success:
                mark_position_exited(position.key)
                insert_event(
                    symbol=position.symbol, short_venue=position.short_venue,
                    long_venue=position.long_venue, spread=current_spread + settings.fee_buffer,
                    net_spread=current_spread, message=msg,
                )
                exit_count += 1
                log(f"[exit_alerts] Sent for {position.key}")
    
    if exit_count > 0:
        log(f"[exit_alerts] Sent {exit_count} exit alerts")


async def fetch_job():
    log("[fetch_job] ========== Starting ==========")
    start_time = datetime.utcnow()
    settings = get_settings()
    ts = datetime.utcnow()

    connectors = get_enabled_connectors(settings.enabled_venues)
    if not connectors:
        log("[fetch_job] No venues enabled", level="WARN")
        return

    venue_data, venue_funding_map, venue_price_map = await fetch_funding_data(
        connectors, settings.symbols, settings.venue_timeout_seconds
    )
    
    if not venue_funding_map:
        log("[fetch_job] No data fetched", level="WARN")
        return

    store_snapshots_with_prices(venue_data, ts)

    all_opportunities = compute_all_arbs(
        settings.symbols, venue_funding_map, settings.fee_buffer,
        min_spread=0, venue_price_map=venue_price_map,
    )
    
    valid_opps = [o for o in all_opportunities if o.spread > 0]
    store_spread_history(valid_opps, ts)
    log(f"[fetch_job] {len(valid_opps)} opportunities tracked")

    active_positions = get_all_active_established()
    if active_positions:
        qualifying_opps = compute_all_arbs(
            settings.symbols, venue_funding_map, settings.fee_buffer,
            min_spread=settings.established_min_spread, venue_price_map=venue_price_map,
        )
        _, _, established_keys = classify_opportunities(qualifying_opps, settings)
        await check_exit_alerts(established_keys, venue_funding_map, settings)

    elapsed = (datetime.utcnow() - start_time).total_seconds()
    log(f"[fetch_job] ========== Complete ({elapsed:.1f}s) ==========")


async def leaderboard_job():
    log("[leaderboard_job] ========== Starting ==========")
    start_time = datetime.utcnow()
    settings = get_settings()

    connectors = get_enabled_connectors(settings.enabled_venues)
    if not connectors:
        log("[leaderboard_job] No venues enabled", level="WARN")
        return

    venue_data, venue_funding_map, venue_price_map = await fetch_funding_data(
        connectors, settings.symbols, settings.venue_timeout_seconds
    )
    
    if not venue_funding_map:
        log("[leaderboard_job] No data fetched", level="WARN")
        return

    all_opps = compute_all_arbs(
        settings.symbols, venue_funding_map, settings.fee_buffer,
        min_spread=settings.established_min_spread, venue_price_map=venue_price_map,
    )
    log(f"[leaderboard_job] {len(all_opps)} opportunities above threshold")

    established, emerging, established_keys = classify_opportunities(all_opps, settings)
    
    est_count = sum(len(v) for v in established.values())
    emg_count = sum(len(v) for v in emerging.values())
    log(f"[leaderboard_job] Classified: {est_count} established, {emg_count} emerging")

    await check_exit_alerts(established_keys, venue_funding_map, settings)

    msg = format_leaderboard(established, emerging)
    
    if settings.telegram_free_channel_id:
        success = await send_message(settings.telegram_bot_token, settings.telegram_free_channel_id, msg)
        if success:
            log("[leaderboard_job] Telegram send SUCCESS")
            insert_event(symbol="LEADERBOARD", short_venue="", long_venue="", spread=0, net_spread=0, message=msg)
        else:
            log("[leaderboard_job] Telegram send FAILED", level="ERROR")
    else:
        log("[leaderboard_job] No Telegram channel configured", level="WARN")

    elapsed = (datetime.utcnow() - start_time).total_seconds()
    log(f"[leaderboard_job] ========== Complete ({elapsed:.1f}s) ==========")


def init_scheduler(fetch_interval: int, leaderboard_interval: int):
    global scheduler
    
    log(f"[scheduler] Init: fetch={fetch_interval}s, leaderboard={leaderboard_interval}s")
    
    job_defaults = {
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 30,
    }
    
    scheduler = AsyncIOScheduler(job_defaults=job_defaults)
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
    scheduler.add_listener(job_missed_listener, EVENT_JOB_MISSED)
    
    scheduler.add_job(
        fetch_job, "interval", seconds=fetch_interval,
        id="fetch_job", name="Funding Rate Fetch", next_run_time=datetime.utcnow(),
    )
    
    scheduler.add_job(
        leaderboard_job, "interval", seconds=leaderboard_interval,
        id="leaderboard_job", name="Leaderboard Generation", next_run_time=datetime.utcnow(),
    )
    
    scheduler.start()
    log("[scheduler] Started")


def shutdown_scheduler():
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        log("[scheduler] Shutdown")
