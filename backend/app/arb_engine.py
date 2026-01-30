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


def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol names for comparison across venues.
    Strips HIP-3 dex suffixes: BTC_hyna -> BTC, SOL_flx -> SOL
    """
    if "_" in symbol:
        return symbol.split("_")[0]
    return symbol


def get_venue_display_name(venue: str, original_symbol: str) -> str:
    """
    Get the display venue name, appending HIP-3 dex suffix if present.
    
    Examples:
    - ("hyperliquid", "BTC") -> "hyperliquid"
    - ("hyperliquid", "BTC_hyna") -> "hyperliquid_hyna"
    """
    if "_" in original_symbol:
        dex_suffix = original_symbol.split("_")[1]
        return f"{venue}_{dex_suffix}"
    return venue


def compute_all_arbs(
    symbols: list[str],
    venue_funding_map: dict[str, dict[str, float]],
    fee_buffer: float,
    min_spread: float = 0.0,
    venue_price_map: dict[str, dict[str, float]] = None,
) -> list[ArbOpportunity]:
    """
    Compute ALL arbitrage opportunities across all venue pairs.
    
    Handles HIP-3 symbols by normalizing them (e.g., SOL_hyna treated same as SOL).
    """
    opportunities = []
    
    # Build a map: normalized_symbol -> list of {venue, original_symbol, rate, price}
    symbol_venue_rates = {}
    
    for venue, rates in venue_funding_map.items():
        for original_symbol, rate in rates.items():
            normalized = normalize_symbol(original_symbol)
            display_venue = get_venue_display_name(venue, original_symbol)
            
            # Get price if available
            price = None
            if venue_price_map and venue in venue_price_map:
                price = venue_price_map[venue].get(original_symbol)
            
            if normalized not in symbol_venue_rates:
                symbol_venue_rates[normalized] = []
            
            symbol_venue_rates[normalized].append({
                "display_venue": display_venue,
                "original_symbol": original_symbol,
                "base_venue": venue,
                "rate": rate,
                "price": price,
            })
    
    # Compute arbs for each normalized symbol
    for normalized_symbol, venue_list in symbol_venue_rates.items():
        if len(venue_list) < 2:
            continue
        
        for i, short_data in enumerate(venue_list):
            for j, long_data in enumerate(venue_list):
                if i == j:
                    continue
                
                spread = short_data["rate"] - long_data["rate"]
                if spread <= 0:
                    continue
                
                net_spread = spread - fee_buffer
                if net_spread < min_spread:
                    continue
                
                # Calculate price spread if both have prices
                price_spread_pct = None
                if short_data["price"] and long_data["price"]:
                    avg_price = (short_data["price"] + long_data["price"]) / 2
                    price_spread_pct = abs(short_data["price"] - long_data["price"]) / avg_price
                
                opportunities.append(
                    ArbOpportunity(
                        symbol=normalized_symbol,
                        short_venue=short_data["display_venue"],
                        short_funding=short_data["rate"],
                        long_venue=long_data["display_venue"],
                        long_funding=long_data["rate"],
                        spread=spread,
                        net_spread=net_spread,
                        short_price=short_data["price"],
                        long_price=long_data["price"],
                        price_spread_pct=price_spread_pct,
                    )
                )
    
    return opportunities


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
    """Get abbreviated venue name, handling HIP-3 suffixes."""
    abbrevs = {
        "hyperliquid": "HL",
        "paradex": "PDX",
        "grvt": "GV",
        "extended": "EXT",
        "variational": "VAR",
    }
    
    # Handle HIP-3 venues like "hyperliquid_hyna"
    if "_" in venue:
        parts = venue.split("_", 1)
        base_abbrev = abbrevs.get(parts[0].lower(), parts[0][:2].upper())
        return f"{base_abbrev}-{parts[1]}"
    
    return abbrevs.get(venue.lower(), venue[:3].upper())


def format_exit_alert(
    symbol: str,
    short_venue: str,
    long_venue: str,
    current_spread: float,
    previous_spread: float,
    duration_hours: float,
) -> str:
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
    opp = entry.opp
    apr = estimate_apr(opp.net_spread)
    apr_min = estimate_apr(entry.min_spread)
    apr_max = estimate_apr(entry.max_spread)
    apr_avg = estimate_apr(entry.avg_spread)
    trend_emoji = get_trend_emoji(entry.trend)
    
    lines = []
    
    direction = f"📉{venue_abbrev(opp.short_venue)} → 📈{venue_abbrev(opp.long_venue)}"
    lines.append(direction)
    
    lines.append(f"💰 {format_rate_compact(opp.net_spread)} ({format_apr_compact(apr)} APR)")
    
    if entry.data_points >= 3:
        lines.append(f"📊 Avg: {format_apr_compact(apr_avg)} | Range: {format_apr_compact(apr_min)}-{format_apr_compact(apr_max)}")
    
    if entry.price_spread_pct is not None:
        price_line = f"💱 Price Δ: {format_price_spread(entry.price_spread_pct)}"
        if entry.price_spread_avg is not None and entry.data_points >= 3:
            price_line += f" (avg: {format_price_spread(entry.price_spread_avg)})"
        lines.append(price_line)
    
    lines.append(f"⏱️ {format_duration(entry.duration_hours)} {trend_emoji}")
    
    return "\n".join(lines)


def format_leaderboard(
    established: dict[str, list[LeaderboardEntry]],
    emerging: dict[str, list[LeaderboardEntry]],
) -> str:
    lines = []
    lines.append("📊 <b>FUNDING ARB LEADERBOARD</b>")
    lines.append(f"🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    lines.append("")
    
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
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("<i>HL=Hyperliquid | PDX=Paradex | GV=GRVT</i>")
    lines.append("<i>EXT=Extended | VAR=Variational</i>")
    lines.append("<i>HL-hyna/flx/km = HIP-3 dexs</i>")
    
    return "\n".join(lines)
