from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
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

scheduler = AsyncIOScheduler()


def log(msg: str):
    print(msg, flush=True)


async def fetch_funding_data(connectors, symbols: list[str]) -> tuple[dict, dict, dict]:
    venue_data = {}
    venue_funding_map = {}
    venue_price_map = {}
    
    for connector in connectors:
        try:
            data = await connector.fetch_funding_with_prices(symbols)
            venue_data[connector.venue_name] = data
            venue_funding_map[connector.venue_name] = {s: d.funding_rate for s, d in data.items()}
            venue_price_map[connector.venue_name] = {
                s: d.mark_price for s, d in data.items() if d.mark_price is not None
            }
            log(f"[fetch] {connector.venue_name}: {len(data)} symbols")
        except Exception as e:
            log(f"[fetch] {connector.venue_name} error: {e}")
    
    return venue_data, venue_funding_map, venue_price_map


def store_snapshots_with_prices(venue_data: dict[str, dict[str, FundingData]], ts: datetime):
    count = 0
    for venue, symbols in venue_data.items():
        for symbol, data in symbols.items():
            insert_snapshot(
                venue=venue,
                symbol=symbol,
                funding_rate=data.funding_rate,
                ts=ts,
                mark_price=data.mark_price,
                index_price=data.index_price,
            )
            count += 1
    log(f"[store] {count} snapshots saved")


def store_spread_history(opportunities: list, ts: datetime):
    count = 0
    for opp in opportunities:
        insert_spread_history(
            symbol=opp.symbol,
            short_venue=opp.short_venue,
            long_venue=opp.long_venue,
            spread=opp.spread,
            net_spread=opp.net_spread,
            ts=ts,
            price_spread_pct=opp.price_spread_pct,
        )
        count += 1
    log(f"[store] {count} spread records saved")


def classify_opportunities(opportunities: list, settings) -> tuple[dict, dict, set]:
    established = {}
    emerging = {}
    established_keys = set()
    
    for opp in opportunities:
        # Check Established tier (48h+, >= 0.01%)
        stats = get_extended_spread_stats(
            opp.symbol,
            opp.short_venue,
            opp.long_venue,
            settings.established_min_spread,
            settings.fee_buffer,
        )
        
        if stats and stats["duration_hours"] >= settings.established_min_hours:
            if opp.net_spread >= settings.established_min_spread:
                entry = LeaderboardEntry(
                    opp=opp,
                    duration_hours=stats["duration_hours"],
                    trend=stats["trend"],
                    avg_spread=stats["avg_spread"],
                    min_spread=stats["min_spread"],
                    max_spread=stats["max_spread"],
                    data_points=stats["data_points"],
                    price_spread_pct=stats.get("price_spread_pct"),
                    price_spread_avg=stats.get("price_spread_avg"),
                )
                if opp.symbol not in established:
                    established[opp.symbol] = []
                established[opp.symbol].append(entry)
                
                key = make_position_key(opp.symbol, opp.short_venue, opp.long_venue)
                established_keys.add(key)
                
                upsert_established_position(
                    opp.symbol,
                    opp.short_venue,
                    opp.long_venue,
                    opp.net_spread,
                )
            continue
        
        # Check Emerging tier (<48h, >= 0.02%)
        stats = get_extended_spread_stats(
            opp.symbol,
            opp.short_venue,
            opp.long_venue,
            settings.emerging_min_spread,
            settings.fee_buffer,
        )
        
        if stats and opp.net_spread >= settings.emerging_min_spread:
            entry = LeaderboardEntry(
                opp=opp,
                duration_hours=stats["duration_hours"],
                trend=stats["trend"],
                avg_spread=stats["avg_spread"],
                min_spread=stats["min_spread"],
                max_spread=stats["max_spread"],
                data_points=stats["data_points"],
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
    
    log(f"[exit_alerts] Checking {len(active_positions)} active positions...")
    exit_count = 0
    
    for position in active_positions:
        if position.key in established_keys:
            continue
        
        symbol = position.symbol
        short_venue = position.short_venue
        long_venue = position.long_venue
        
        short_rate = venue_funding_map.get(short_venue, {}).get(symbol)
        long_rate = venue_funding_map.get(long_venue, {}).get(symbol)
        
        if short_rate is None or long_rate is None:
            continue
        
        current_spread = short_rate - long_rate - settings.fee_buffer
        duration_hours = (datetime.utcnow() - position.established_at).total_seconds() / 3600
        
        log(f"[exit_alerts] {position.key}: was {position.last_seen_spread:.6f}, now {current_spread:.6f}")
        
        msg = format_exit_alert(
            symbol=symbol,
            short_venue=short_venue,
            long_venue=long_venue,
            current_spread=current_spread,
            previous_spread=position.last_seen_spread,
            duration_hours=duration_hours,
        )
        
        if settings.telegram_free_channel_id:
            success = await send_message(
                settings.telegram_bot_token,
                settings.telegram_free_channel_id,
                msg
            )
            if success:
                mark_position_exited(position.key)
                insert_event(
                    symbol=symbol,
                    short_venue=short_venue,
                    long_venue=long_venue,
                    spread=current_spread + settings.fee_buffer,
                    net_spread=current_spread,
                    message=msg,
                )
                exit_count += 1
                log(f"[exit_alerts] Sent exit alert for {position.key}")
    
    if exit_count > 0:
        log(f"[exit_alerts] Sent {exit_count} exit alerts")


async def fetch_job():
    log("[fetch_job] ========== Starting ==========")
    settings = get_settings()
    ts = datetime.utcnow()

    connectors = get_enabled_connectors(settings.enabled_venues)
    if not connectors:
        log("[fetch_job] No venues enabled")
        return

    venue_data, venue_funding_map, venue_price_map = await fetch_funding_data(
        connectors, settings.symbols
    )
    
    if not venue_funding_map:
        log("[fetch_job] No data fetched")
        return

    store_snapshots_with_prices(venue_data, ts)

    all_opportunities = compute_all_arbs(
        settings.symbols,
        venue_funding_map,
        settings.fee_buffer,
        min_spread=0,
        venue_price_map=venue_price_map,
    )
    
    valid_opps = [o for o in all_opportunities if o.spread > 0]
    store_spread_history(valid_opps, ts)
    log(f"[fetch_job] {len(valid_opps)} opportunities tracked")

    # Only check exit alerts if we have established positions
    active_positions = get_all_active_established()
    if active_positions:
        qualifying_opps = compute_all_arbs(
            settings.symbols,
            venue_funding_map,
            settings.fee_buffer,
            min_spread=settings.established_min_spread,
            venue_price_map=venue_price_map,
        )
        _, _, established_keys = classify_opportunities(qualifying_opps, settings)
        await check_exit_alerts(established_keys, venue_funding_map, settings)

    log("[fetch_job] ========== Complete ==========")


async def leaderboard_job():
    log("[leaderboard_job] ========== Starting ==========")
    settings = get_settings()

    connectors = get_enabled_connectors(settings.enabled_venues)
    if not connectors:
        log("[leaderboard_job] No venues enabled")
        return

    venue_data, venue_funding_map, venue_price_map = await fetch_funding_data(
        connectors, settings.symbols
    )
    
    if not venue_funding_map:
        log("[leaderboard_job] No data fetched")
        return

    all_opps = compute_all_arbs(
        settings.symbols,
        venue_funding_map,
        settings.fee_buffer,
        min_spread=settings.established_min_spread,
        venue_price_map=venue_price_map,
    )
    log(f"[leaderboard_job] {len(all_opps)} opportunities above min threshold")

    established, emerging, established_keys = classify_opportunities(all_opps, settings)
    
    est_count = sum(len(v) for v in established.values())
    emg_count = sum(len(v) for v in emerging.values())
    log(f"[leaderboard_job] Classified: {est_count} established, {emg_count} emerging")

    # Check exit alerts
    await check_exit_alerts(established_keys, venue_funding_map, settings)

    msg = format_leaderboard(established, emerging)
    
    if settings.telegram_free_channel_id:
        success = await send_message(
            settings.telegram_bot_token,
            settings.telegram_free_channel_id,
            msg
        )
        if success:
            log("[leaderboard_job] Sent SUCCESS")
            insert_event(
                symbol="LEADERBOARD",
                short_venue="",
                long_venue="",
                spread=0,
                net_spread=0,
                message=msg,
            )
        else:
            log("[leaderboard_job] Sent FAILED")

    log("[leaderboard_job] ========== Complete ==========")


def init_scheduler(fetch_interval: int, leaderboard_interval: int):
    log(f"[scheduler] Init: fetch={fetch_interval}s, leaderboard={leaderboard_interval}s")
    
    scheduler.add_job(
        fetch_job,
        "interval",
        seconds=fetch_interval,
        id="fetch_job",
        next_run_time=datetime.utcnow(),
    )
    
    scheduler.add_job(
        leaderboard_job,
        "interval",
        seconds=leaderboard_interval,
        id="leaderboard_job",
        next_run_time=datetime.utcnow(),
    )
    
    scheduler.start()
    log("[scheduler] Started")


def shutdown_scheduler():
    scheduler.shutdown(wait=False)
    log("[scheduler] Shutdown")
