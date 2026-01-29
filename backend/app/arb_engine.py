from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class ArbOpportunity:
    symbol: str
    short_venue: str
    short_funding: float
    long_venue: str
    long_funding: float
    spread: float
    net_spread: float


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
    elif hours < 24:
        return f"{hours:.1f}h"
    else:
        days = hours / 24
        return f"{days:.1f}d"


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
