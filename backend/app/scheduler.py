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
    get_continuous_duration,
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


async def fetch_all_funding_with_prices(connectors, symbols: list[str]) -> tuple[dict, dict]:
    venue_funding_map = {}
    venue_price_map = {}
    
    for connector in connectors:
        try:
            data = await connector.fetch_funding_with_prices(symbols)
            venue_funding_map[connector.venue_name] = {s: d.funding_rate for s, d in data.items()}
            venue_price_map[connector.venue_name] = {
                s: d.mark_price for s, d in data.items() if d.mark_price is not None
            }
            log(f"[fetch] {connector.venue_name}: {len(data)} symbols, {len(venue_price_map[connector.venue_name])} prices")
        except Exception as e:
            log(f"[fetch] {connector.venue_name} error: {e}")
    
    return venue_funding_map, venue_price_map


def store_snapshots_with_prices(
    venue_data: dict[str, dict[str, FundingData]],
    ts: datetime
):
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
    log(f"[store] {count} snapshots saved (with prices)")


def store_spread_history_with_prices(opportunities: list, ts: datetime):
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
    log(f"[store] {len(opportunities)} spread records saved")


def classify_opportunities(opportunities: list, settings) -> tuple[dict, dict, set]:
    """
    Classify opportunities into established and emerging tiers.
    Also returns set of currently established position keys.
    """
    established = {}
    emerging = {}
    established_keys = set()
    
    for opp in opportunities:
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
                
                # Track this as established
                key = make_position_key(opp.symbol, opp.short_venue, opp.long_venue)
                established_keys.add(key)
                
                # Update/create established position record
                upsert_established_position(
                    opp.symbol,
                    opp.short_venue,
                    opp.long_venue,
                    opp.net_spread,
                )
            continue
        
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


async def check_exit_alerts(
    current_established_keys: set,
    venue_funding_map: dict,
    settings,
):
    """
    Check for established positions that have fallen below threshold.
    Send exit alerts for positions that need to be closed.
    """
    log("[exit_alerts] Checking for positions to exit...")
    
    # Get all positions we previously marked as established
    active_positions = get_all_active_established()
    log(f"[exit_alerts] {len(active_positions)} active established positions in DB")
    
    exit_count = 0
    
    for position in active_positions:
        key = position.key
        
        # If still in current established, skip
        if key in current_established_keys:
            continue
        
        # Position was established but is no longer - check current spread
        symbol = position.symbol
        short_venue = position.short_venue
        long_venue = position.long_venue
        
        # Get current rates
        short_rate = venue_funding_map.get(short_venue, {}).get(symbol)
        long_rate = venue_funding_map.get(long_venue, {}).get(symbol)
        
        if short_rate is None or long_rate is None:
            log(f"[exit_alerts] {key}: Missing rate data, skipping")
            continue
        
        current_spread = short_rate - long_rate - settings.fee_buffer
        
        # Calculate how long it was active
        duration_hours = (datetime.utcnow() - position.established_at).total_seconds() / 3600
        
        log(f"[exit_alerts] {key}: Previous spread={position.last_seen_spread:.6f}, Current={current_spread:.6f}")
        
        # Send exit alert
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
                log(f"[exit_alerts] Sent exit alert for {key}")
                mark_position_exited(key)
                insert_event(
                    symbol=symbol,
                    short_venue=short_venue,
                    long_venue=long_venue,
                    spread=current_spread + settings.fee_buffer,
                    net_spread=current_spread,
                    message=msg,
                )
                exit_count += 1
            else:
                log(f"[exit_alerts] Failed to send exit alert for {key}")
    
    log(f"[exit_alerts] Sent {exit_count} exit alerts")


async def send_leaderboard(settings):
    log("[leaderboard] Building leaderboard...")
    
    connectors = get_enabled_connectors(settings.enabled_venues)
    if not connectors:
        log("[leaderboard] No venues enabled")
        return
    
    venue_funding_map, venue_price_map = await fetch_all_funding_with_prices(connectors, settings.symbols)
    if not venue_funding_map:
        log("[leaderboard] No funding data")
        return
    
    all_opps = compute_all_arbs(
        settings.symbols,
        venue_funding_map,
        settings.fee_buffer,
        min_spread=settings.established_min_spread,
        venue_price_map=venue_price_map,
    )
    log(f"[leaderboard] {len(all_opps)} total opportunities found")
    
    established, emerging, established_keys = classify_opportunities(all_opps, settings)
    
    est_count = sum(len(v) for v in established.values())
    emg_count = sum(len(v) for v in emerging.values())
    log(f"[leaderboard] Classified: {est_count} established, {emg_count} emerging")
    
    # Check for exit alerts (positions that were established but no longer are)
    await check_exit_alerts(established_keys, venue_funding_map, settings)
    
    msg = format_leaderboard(established, emerging)
    
    if settings.telegram_free_channel_id:
        log("[leaderboard] Sending to free channel...")
        success = await send_message(settings.telegram_bot_token, settings.telegram_free_channel_id, msg)
        if success:
            log("[leaderboard] Sent to free channel SUCCESS")
            insert_event(
                symbol="LEADERBOARD",
                short_venue="",
                long_venue="",
                spread=0,
                net_spread=0,
                message=msg,
            )
        else:
            log("[leaderboard] Sent to free channel FAILED")


async def fetch_job():
    log("[fetch_job] ========== Starting fetch job ==========")
    settings = get_settings()
    ts = datetime.utcnow()

    connectors = get_enabled_connectors(settings.enabled_venues)
    log(f"[fetch_job] Loaded {len(connectors)} connectors")
    if not connectors:
        log("[fetch_job] No venues enabled, skipping")
        return

    venue_data = {}
    venue_funding_map = {}
    venue_price_map = {}
    
    for connector in connectors:
        try:
            data = await connector.fetch_funding_with_prices(settings.symbols)
            venue_data[connector.venue_name] = data
            venue_funding_map[connector.venue_name] = {s: d.funding_rate for s, d in data.items()}
            venue_price_map[connector.venue_name] = {
                s: d.mark_price for s, d in data.items() if d.mark_price is not None
            }
            log(f"[fetch] {connector.venue_name}: {len(data)} symbols")
        except Exception as e:
            log(f"[fetch] {connector.venue_name} error: {e}")
    
    if not venue_funding_map:
        log("[fetch_job] No funding data fetched, skipping")
        return

    store_snapshots_with_prices(venue_data, ts)

    all_opportunities = compute_all_arbs(
        settings.symbols,
        venue_funding_map,
        settings.fee_buffer,
        min_spread=0,
        venue_price_map=venue_price_map,
    )
    log(f"[fetch_job] {len(all_opportunities)} opportunities computed")

    valid_opps = [o for o in all_opportunities if o.spread > 0]
    store_spread_history_with_prices(valid_opps, ts)

    # Check exit alerts on every fetch (real-time alerts)
    # First, get current established opportunities
    all_opps_for_classification = compute_all_arbs(
        settings.symbols,
        venue_funding_map,
        settings.fee_buffer,
        min_spread=settings.established_min_spread,
        venue_price_map=venue_price_map,
    )
    _, _, established_keys = classify_opportunities(all_opps_for_classification, settings)
    
    await check_exit_alerts(established_keys, venue_funding_map, settings)

    log("[fetch_job] ========== Fetch job complete ==========")


async def leaderboard_job():
    log("[leaderboard_job] ========== Starting leaderboard job ==========")
    settings = get_settings()
    await send_leaderboard(settings)
    log("[leaderboard_job] ========== Leaderboard job complete ==========")


def init_scheduler(fetch_interval: int, leaderboard_interval: int):
    log(f"[scheduler] Initializing: fetch={fetch_interval}s, leaderboard={leaderboard_interval}s")
    
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
    log("[scheduler] Started successfully")


def shutdown_scheduler():
    scheduler.shutdown(wait=False)
    log("[scheduler] Shutdown")
