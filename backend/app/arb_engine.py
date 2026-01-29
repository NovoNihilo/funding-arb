from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


@dataclass
class ArbOpportunity:
    symbol: str
    short_venue: str
    short_funding: float
    long_venue: str
    long_funding: float
    spread: float
    net_spread: float
    short_price: Optional[float] = None
    long_price: Optional[float] = None
    price_spread_pct: Optional[float] = None


def compute_all_arbs(
    symbols: list[str],
    venue_funding_map: dict[str, dict[str, float]],
    fee_buffer: float,
    min_spread: float = 0.0,
    venue_price_map: dict[str, dict[str, float]] = None,
) -> list[ArbOpportunity]:
    opportunities = []
    venues = list(venue_funding_map.keys())

    for symbol in symbols:
        symbol_venues = []
        for venue in venues:
            if symbol in venue_funding_map[venue]:
                symbol_venues.append((venue, venue_funding_map[venue][symbol]))

        if len(symbol_venues) < 2:
            continue

        for i, (short_venue, short_rate) in enumerate(symbol_venues):
            for j, (long_venue, long_rate) in enumerate(symbol_venues):
                if i == j:
                    continue
                
                spread = short_rate - long_rate
                if spread <= 0:
                    continue
                    
                net_spread = spread - fee_buffer
                if net_spread < min_spread:
                    continue

                short_price = None
                long_price = None
                price_spread_pct = None
                
                if venue_price_map:
                    short_price = venue_price_map.get(short_venue, {}).get(symbol)
                    long_price = venue_price_map.get(long_venue, {}).get(symbol)
                    
                    if short_price and long_price:
                        avg_price = (short_price + long_price) / 2
                        price_spread_pct = abs(short_price - long_price) / avg_price

                opportunities.append(
                    ArbOpportunity(
                        symbol=symbol,
                        short_venue=short_venue,
                        short_funding=short_rate,
                        long_venue=long_venue,
                        long_funding=long_rate,
                        spread=spread,
                        net_spread=net_spread,
                        short_price=short_price,
                        long_price=long_price,
                        price_spread_pct=price_spread_pct,
                    )
                )

    return opportunities


def compute_arbs(
    symbols: list[str],
    venue_funding_map: dict[str, dict[str, float]],
    fee_buffer: float,
) -> list[ArbOpportunity]:
    opportunities = []
    for symbol in symbols:
        venue_rates: list[tuple[str, float]] = []
        for venue, rates in venue_funding_map.items():
            if symbol in rates:
                venue_rates.append((venue, rates[symbol]))
        if len(venue_rates) < 2:
            continue
        best_short = max(venue_rates, key=lambda x: x[1])
        best_long = min(venue_rates, key=lambda x: x[1])
        if best_short[0] == best_long[0]:
            continue
        spread = best_short[1] - best_long[1]
        net_spread = spread - fee_buffer
        opportunities.append(
            ArbOpportunity(
                symbol=symbol,
                short_venue=best_short[0],
                short_funding=best_short[1],
                long_venue=best_long[0],
                long_funding=best_long[1],
                spread=spread,
                net_spread=net_spread,
            )
        )
    return opportunities


def filter_by_min_spread(
    opportunities: list[ArbOpportunity],
    min_net_spread: float,
) -> list[ArbOpportunity]:
    return [opp for opp in opportunities if opp.net_spread >= min_net_spread]


def make_cooldown_key(
    symbol: str,
    short_venue: str,
    long_venue: str,
    channel: str,
) -> str:
    return f"{symbol}:{short_venue}:{long_venue}:{channel}"


def is_cooldown_passed(
    last_triggered: datetime | None,
    cooldown_seconds: int,
    now: datetime | None = None,
) -> bool:
    if last_triggered is None:
        return True
    now = now or datetime.utcnow()
    return (now - last_triggered) >= timedelta(seconds=cooldown_seconds)


def format_funding_rate(rate: float) -> str:
    return f"{rate * 100:.4f}%"


def format_rate_compact(rate: float) -> str:
    pct = rate * 100
    if abs(pct) >= 1:
        return f"{pct:.2f}%"
    elif abs(pct) >= 0.1:
        return f"{pct:.3f}%"
    else:
        return f"{pct:.4f}%"


def format_price_spread(pct: float) -> str:
    bps = pct * 10000
    if bps < 100:
        return f"{bps:.1f}bps"
    else:
        return f"{pct * 100:.2f}%"


def estimate_apr(spread: float, interval_hours: float = 8) -> float:
    periods_per_year = 365 * 24 / interval_hours
    return spread * periods_per_year


def estimate_daily_return(spread: float, position_size: float = 10000) -> float:
    daily_rate = spread * 3
    return position_size * daily_rate


def get_trend_emoji(trend: str) -> str:
    if trend == "widening":
        return "📈"
    elif trend == "narrowing":
        return "📉"
    elif trend == "stable":
        return "➡️"
    else:
        return "🆕"


def format_duration(hours: float) -> str:
    if hours < 1:
        return f"{int(hours * 60)}m"
    elif hours < 48:
        return f"{hours:.1f}h"
    else:
        days = hours / 24
        return f"{days:.1f}d"


def format_apr_compact(apr: float) -> str:
    pct = apr * 100
    if pct >= 1000:
        return f"{pct/1000:.1f}k%"
    elif pct >= 100:
        return f"{pct:.0f}%"
    else:
        return f"{pct:.1f}%"


def venue_abbrev(venue: str) -> str:
    abbrevs = {
        "hyperliquid": "HL",
        "paradex": "PDX",
        "grvt": "GV",
        "extended": "EXT",
        "variational": "VAR",
    }
    return abbrevs.get(venue.lower(), venue[:3].upper())


def format_telegram_message(
    opp: ArbOpportunity,
    interval_hours: float = 8,
    spread_stats: dict = None,
) -> str:
    apr = estimate_apr(opp.spread, interval_hours)
    daily_return = estimate_daily_return(opp.net_spread)
    msg = (
        f"🔔 <b>Funding Arb: {opp.symbol}</b>\n"
        f"\n"
        f"📉 <b>Short:</b> {opp.short_venue} @ {format_funding_rate(opp.short_funding)}\n"
        f"📈 <b>Long:</b> {opp.long_venue} @ {format_funding_rate(opp.long_funding)}\n"
        f"\n"
        f"💰 <b>Spread:</b> {format_funding_rate(opp.spread)}\n"
        f"💵 <b>Net Spread:</b> {format_funding_rate(opp.net_spread)}\n"
        f"📊 <b>Est. APR:</b> {apr * 100:.1f}%\n"
        f"💵 <b>$10k/day:</b> ${daily_return:.2f}"
    )
    if spread_stats:
        trend_emoji = get_trend_emoji(spread_stats["trend"])
        duration = format_duration(spread_stats["duration_hours"])
        msg += (
            f"\n\n"
            f"📊 <b>Trend:</b> {spread_stats['trend'].capitalize()} {trend_emoji}\n"
            f"⏱️ <b>Active:</b> {duration}\n"
            f"📈 <b>24h Avg:</b> {format_funding_rate(spread_stats['avg_24h'])}"
        )
        if spread_stats["data_points"] >= 3:
            msg += (
                f"\n📉 <b>24h Range:</b> {format_funding_rate(spread_stats['min_24h'])} - "
                f"{format_funding_rate(spread_stats['max_24h'])}"
            )
    return msg


def format_exit_alert(
    symbol: str,
    short_venue: str,
    long_venue: str,
    current_spread: float,
    previous_spread: float,
    duration_hours: float,
) -> str:
    """Format exit alert for established position that's no longer profitable."""
    current_apr = estimate_apr(current_spread)
    previous_apr = estimate_apr(previous_spread)
    
    msg = (
        f"🚨 <b>EXIT ALERT: {symbol}</b>\n"
        f"\n"
        f"📉{venue_abbrev(short_venue)} → 📈{venue_abbrev(long_venue)}\n"
        f"\n"
        f"⚠️ <b>Spread collapsed below threshold!</b>\n"
        f"\n"
        f"📊 Previous: {format_rate_compact(previous_spread)} ({format_apr_compact(previous_apr)} APR)\n"
        f"📉 Current: {format_rate_compact(current_spread)} ({format_apr_compact(current_apr)} APR)\n"
        f"⏱️ Was active: {format_duration(duration_hours)}\n"
        f"\n"
        f"<i>Consider closing this position</i>"
    )
    return msg


@dataclass
class LeaderboardEntry:
    opp: ArbOpportunity
    duration_hours: float
    trend: str
    avg_spread: float
    min_spread: float
    max_spread: float
    data_points: int
    price_spread_pct: Optional[float] = None
    price_spread_avg: Optional[float] = None


def format_leaderboard_entry(entry: LeaderboardEntry, rank: int = None) -> str:
    """Format single entry in a clean, readable format."""
    opp = entry.opp
    apr = estimate_apr(opp.net_spread)
    apr_min = estimate_apr(entry.min_spread)
    apr_max = estimate_apr(entry.max_spread)
    apr_avg = estimate_apr(entry.avg_spread)
    trend_emoji = get_trend_emoji(entry.trend)
    
    lines = []
    
    # Direction line
    direction = f"📉{venue_abbrev(opp.short_venue)} → 📈{venue_abbrev(opp.long_venue)}"
    lines.append(direction)
    
    # Current spread and APR
    lines.append(f"💰 {format_rate_compact(opp.net_spread)} ({format_apr_compact(apr)} APR)")
    
    # APR range and average if we have enough data
    if entry.data_points >= 3:
        lines.append(f"📊 Avg: {format_apr_compact(apr_avg)} | Range: {format_apr_compact(apr_min)}-{format_apr_compact(apr_max)}")
    
    # Price spread line
    if entry.price_spread_pct is not None:
        price_line = f"💱 Price Δ: {format_price_spread(entry.price_spread_pct)}"
        if entry.price_spread_avg is not None and entry.data_points >= 3:
            price_line += f" (avg: {format_price_spread(entry.price_spread_avg)})"
        lines.append(price_line)
    
    # Duration and trend
    lines.append(f"⏱️ {format_duration(entry.duration_hours)} {trend_emoji}")
    
    return "\n".join(lines)


def format_leaderboard(
    established: dict[str, list[LeaderboardEntry]],
    emerging: dict[str, list[LeaderboardEntry]],
) -> str:
    """Format full leaderboard message."""
    lines = []
    lines.append("📊 <b>FUNDING ARB LEADERBOARD</b>")
    lines.append(f"🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    lines.append("")
    
    # Established section
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("🏆 <b>ESTABLISHED</b>")
    lines.append("<i>≥48h active | ≥0.01% spread</i>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    
    if established:
        sorted_symbols = sorted(
            established.keys(),
            key=lambda s: max(estimate_apr(e.opp.net_spread) for e in established[s]),
            reverse=True
        )
        for symbol in sorted_symbols:
            entries = established[symbol]
            entries.sort(key=lambda e: e.opp.net_spread, reverse=True)
            lines.append("")
            lines.append(f"<b>🪙 {symbol}</b>")
            for i, entry in enumerate(entries[:3]):
                lines.append("")
                lines.append(format_leaderboard_entry(entry, i+1))
    else:
        lines.append("")
        lines.append("<i>No opportunities qualify yet</i>")
    
    lines.append("")
    lines.append("")
    
    # Emerging section
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚡ <b>EMERGING</b>")
    lines.append("<i>&lt;48h active | ≥0.02% spread</i>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    
    if emerging:
        sorted_symbols = sorted(
            emerging.keys(),
            key=lambda s: max(estimate_apr(e.opp.net_spread) for e in emerging[s]),
            reverse=True
        )
        for symbol in sorted_symbols:
            entries = emerging[symbol]
            entries.sort(key=lambda e: e.opp.net_spread, reverse=True)
            lines.append("")
            lines.append(f"<b>🪙 {symbol}</b>")
            for i, entry in enumerate(entries[:3]):
                lines.append("")
                lines.append(format_leaderboard_entry(entry, i+1))
    else:
        lines.append("")
        lines.append("<i>No new opportunities detected</i>")
    
    # Footer
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("<i>HL=Hyperliquid | PDX=Paradex | GV=GRVT</i>")
    lines.append("<i>EXT=Extended | VAR=Variational</i>")
    
    return "\n".join(lines)
