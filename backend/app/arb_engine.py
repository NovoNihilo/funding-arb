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


def format_telegram_message(opp: ArbOpportunity, interval_hours: float = 8) -> str:
    apr = estimate_apr(opp.spread, interval_hours)

    return (
        f"🔔 <b>Funding Arb: {opp.symbol}</b>\n"
        f"\n"
        f"📉 <b>Short:</b> {opp.short_venue} @ {format_funding_rate(opp.short_funding)}\n"
        f"📈 <b>Long:</b> {opp.long_venue} @ {format_funding_rate(opp.long_funding)}\n"
        f"\n"
        f"💰 <b>Spread:</b> {format_funding_rate(opp.spread)}\n"
        f"💵 <b>Net Spread:</b> {format_funding_rate(opp.net_spread)}\n"
        f"📊 <b>Est. APR:</b> {apr * 100:.1f}%"
    )
