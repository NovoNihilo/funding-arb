import sys
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import get_settings
from app.venues import get_enabled_connectors
from app.db.repository import (
    insert_snapshot,
    get_alert_state,
    upsert_alert_state,
    insert_event,
)
from app.arb_engine import (
    compute_arbs,
    filter_by_min_spread,
    make_cooldown_key,
    is_cooldown_passed,
    format_telegram_message,
)
from app.telegram_sender import send_message

scheduler = AsyncIOScheduler()


def log(msg: str):
    print(msg, flush=True)


async def fetch_all_funding(connectors, symbols: list[str]) -> dict[str, dict[str, float]]:
    venue_funding_map = {}

    for connector in connectors:
        try:
            rates = await connector.fetch_funding(symbols)
            venue_funding_map[connector.venue_name] = rates
            log(f"[fetch] {connector.venue_name}: {len(rates)} symbols - {rates}")
        except Exception as e:
            log(f"[fetch] {connector.venue_name} error: {e}")

    return venue_funding_map


def store_snapshots(venue_funding_map: dict[str, dict[str, float]], ts: datetime):
    count = 0
    for venue, rates in venue_funding_map.items():
        for symbol, rate in rates.items():
            insert_snapshot(venue, symbol, rate, ts)
            count += 1
    log(f"[store] {count} snapshots saved")


async def process_alerts(
    opportunities,
    settings,
    channel_id: str,
    channel_name: str,
):
    now = datetime.utcnow()
    sent_count = 0

    for opp in opportunities:
        key = make_cooldown_key(opp.symbol, opp.short_venue, opp.long_venue, channel_name)
        state = get_alert_state(key)
        last_triggered = state.last_triggered_at if state else None

        if not is_cooldown_passed(last_triggered, settings.cooldown_seconds, now):
            log(f"[alert] {opp.symbol} cooldown active, skipping")
            continue

        msg = format_telegram_message(opp)
        log(f"[alert] Sending {opp.symbol} to {channel_name}...")
        success = await send_message(settings.telegram_bot_token, channel_id, msg)

        if success:
            upsert_alert_state(key, now)
            insert_event(
                symbol=opp.symbol,
                short_venue=opp.short_venue,
                long_venue=opp.long_venue,
                spread=opp.spread,
                net_spread=opp.net_spread,
                message=msg,
            )
            sent_count += 1
            log(f"[alert] Sent {opp.symbol} to {channel_name} SUCCESS")
        else:
            log(f"[alert] Sent {opp.symbol} to {channel_name} FAILED")

    return sent_count


async def funding_job():
    log("[job] ========== Starting funding job ==========")
    settings = get_settings()
    ts = datetime.utcnow()

    connectors = get_enabled_connectors(settings.enabled_venues)
    log(f"[job] Loaded {len(connectors)} connectors")
    if not connectors:
        log("[job] No venues enabled, skipping")
        return

    venue_funding_map = await fetch_all_funding(connectors, settings.symbols)
    if not venue_funding_map:
        log("[job] No funding data fetched, skipping")
        return

    store_snapshots(venue_funding_map, ts)

    opportunities = compute_arbs(
        settings.symbols,
        venue_funding_map,
        settings.fee_buffer,
    )
    log(f"[job] {len(opportunities)} raw opportunities")

    filtered = filter_by_min_spread(opportunities, settings.min_net_spread)
    log(f"[job] {len(filtered)} opportunities above threshold (min={settings.min_net_spread})")

    if not filtered:
        log("[job] No opportunities meet threshold, done")
        return

    if settings.telegram_free_channel_id:
        await process_alerts(
            filtered,
            settings,
            settings.telegram_free_channel_id,
            "free",
        )

    if settings.telegram_pro_channel_id and settings.telegram_pro_channel_id != settings.telegram_free_channel_id:
        await process_alerts(
            filtered,
            settings,
            settings.telegram_pro_channel_id,
            "pro",
        )

    log("[job] ========== Funding job complete ==========")


def init_scheduler(interval_seconds: int):
    log(f"[scheduler] Initializing with interval={interval_seconds}s")
    scheduler.add_job(
        funding_job,
        "interval",
        seconds=interval_seconds,
        id="funding_job",
        next_run_time=datetime.utcnow(),
    )
    scheduler.start()
    log(f"[scheduler] Started successfully")


def shutdown_scheduler():
    scheduler.shutdown(wait=False)
    log("[scheduler] Shutdown")
