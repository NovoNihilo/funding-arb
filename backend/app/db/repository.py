from datetime import datetime, timedelta
from sqlmodel import select, and_
from app.db.engine import get_session
from app.db.models import FundingSnapshot, AlertState, AlertEvent, SpreadHistory, EstablishedPosition


def insert_snapshot(venue: str, symbol: str, funding_rate: float, ts: datetime = None,
                    mark_price: float = None, index_price: float = None):
    with get_session() as session:
        snapshot = FundingSnapshot(
            ts=ts or datetime.utcnow(),
            venue=venue,
            symbol=symbol,
            funding_rate=funding_rate,
            mark_price=mark_price,
            index_price=index_price,
        )
        session.add(snapshot)
        session.commit()
        session.refresh(snapshot)
        return snapshot


def get_latest_snapshots(venue: str, symbol: str, limit: int = 1) -> list[FundingSnapshot]:
    with get_session() as session:
        stmt = (
            select(FundingSnapshot)
            .where(FundingSnapshot.venue == venue, FundingSnapshot.symbol == symbol)
            .order_by(FundingSnapshot.ts.desc())
            .limit(limit)
        )
        return list(session.exec(stmt).all())


def get_latest_funding_by_symbol(symbols: list[str]) -> dict[str, dict[str, float]]:
    result = {}
    with get_session() as session:
        for symbol in symbols:
            stmt = (
                select(FundingSnapshot)
                .where(FundingSnapshot.symbol == symbol)
                .order_by(FundingSnapshot.ts.desc())
            )
            snapshots = session.exec(stmt).all()
            seen_venues = set()
            result[symbol] = {}
            for snap in snapshots:
                if snap.venue not in seen_venues:
                    result[symbol][snap.venue] = snap.funding_rate
                    seen_venues.add(snap.venue)
    return result


def get_latest_prices_by_symbol(symbols: list[str]) -> dict[str, dict[str, float]]:
    result = {}
    with get_session() as session:
        for symbol in symbols:
            stmt = (
                select(FundingSnapshot)
                .where(FundingSnapshot.symbol == symbol)
                .order_by(FundingSnapshot.ts.desc())
            )
            snapshots = session.exec(stmt).all()
            seen_venues = set()
            result[symbol] = {}
            for snap in snapshots:
                if snap.venue not in seen_venues and snap.mark_price is not None:
                    result[symbol][snap.venue] = snap.mark_price
                    seen_venues.add(snap.venue)
    return result


def get_alert_state(key: str) -> AlertState | None:
    with get_session() as session:
        return session.get(AlertState, key)


def upsert_alert_state(key: str, triggered_at: datetime):
    with get_session() as session:
        state = session.get(AlertState, key)
        if state:
            state.last_triggered_at = triggered_at
        else:
            state = AlertState(key=key, last_triggered_at=triggered_at)
            session.add(state)
        session.commit()


def insert_event(
    symbol: str,
    short_venue: str,
    long_venue: str,
    spread: float,
    net_spread: float,
    message: str,
    short_funding: float = 0.0,
    long_funding: float = 0.0,
) -> AlertEvent:
    with get_session() as session:
        event = AlertEvent(
            ts=datetime.utcnow(),
            symbol=symbol,
            short_venue=short_venue,
            long_venue=long_venue,
            short_funding=short_funding,
            long_funding=long_funding,
            spread=spread,
            net_spread=net_spread,
            message=message,
        )
        session.add(event)
        session.commit()
        session.refresh(event)
        return event


def get_recent_events(limit: int = 50) -> list[AlertEvent]:
    with get_session() as session:
        stmt = select(AlertEvent).order_by(AlertEvent.ts.desc()).limit(limit)
        return list(session.exec(stmt).all())


def insert_spread_history(
    symbol: str,
    short_venue: str,
    long_venue: str,
    spread: float,
    net_spread: float,
    ts: datetime = None,
    price_spread_pct: float = None,
):
    with get_session() as session:
        record = SpreadHistory(
            ts=ts or datetime.utcnow(),
            symbol=symbol,
            short_venue=short_venue,
            long_venue=long_venue,
            spread=spread,
            net_spread=net_spread,
            price_spread_pct=price_spread_pct,
        )
        session.add(record)
        session.commit()
        return record


def get_spread_history(
    symbol: str,
    short_venue: str,
    long_venue: str,
    hours: int = 24,
) -> list[SpreadHistory]:
    with get_session() as session:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        stmt = (
            select(SpreadHistory)
            .where(
                and_(
                    SpreadHistory.symbol == symbol,
                    SpreadHistory.short_venue == short_venue,
                    SpreadHistory.long_venue == long_venue,
                    SpreadHistory.ts >= cutoff,
                )
            )
            .order_by(SpreadHistory.ts.asc())
        )
        return list(session.exec(stmt).all())


def get_continuous_spread_data(
    symbol: str,
    short_venue: str,
    long_venue: str,
    min_net_spread: float,
) -> tuple[float | None, list[SpreadHistory]]:
    """
    Get continuous duration and ONLY the data points from the current continuous period.
    Returns: (duration_hours, list of history records in the continuous period)
    
    If spread ever dropped below min_net_spread, we only return data AFTER that drop.
    """
    with get_session() as session:
        # Get all history, newest first
        stmt = (
            select(SpreadHistory)
            .where(
                and_(
                    SpreadHistory.symbol == symbol,
                    SpreadHistory.short_venue == short_venue,
                    SpreadHistory.long_venue == long_venue,
                )
            )
            .order_by(SpreadHistory.ts.desc())
        )
        history = list(session.exec(stmt).all())
        
        if not history:
            return None, []
        
        # Check if current is above threshold
        if history[0].net_spread < min_net_spread:
            return None, []
        
        # Walk backwards to find where continuous period started
        continuous_records = [history[0]]
        
        for i in range(1, len(history)):
            record = history[i]
            if record.net_spread < min_net_spread:
                # Found a break - stop here
                break
            continuous_records.append(record)
        
        # Reverse to get chronological order (oldest first)
        continuous_records.reverse()
        
        # Calculate duration from first record in continuous period
        continuous_start = continuous_records[0].ts
        duration_hours = (datetime.utcnow() - continuous_start).total_seconds() / 3600
        
        return duration_hours, continuous_records


def get_extended_spread_stats(
    symbol: str,
    short_venue: str,
    long_venue: str,
    min_net_spread: float,
    fee_buffer: float = 0.0001,
) -> dict | None:
    """
    Get spread stats using ONLY data from the current continuous period.
    Range, avg, etc. are all calculated from when the opportunity started
    being continuously above threshold.
    """
    duration, continuous_history = get_continuous_spread_data(
        symbol, short_venue, long_venue, min_net_spread
    )
    
    if duration is None or not continuous_history:
        return None
    
    # Calculate stats from continuous period only
    spreads = [h.net_spread for h in continuous_history]
    avg = sum(spreads) / len(spreads)
    min_spread = min(spreads)
    max_spread = max(spreads)
    
    # Calculate trend from continuous period
    if len(spreads) >= 6:
        recent_avg = sum(spreads[-3:]) / 3
        earlier_avg = sum(spreads[-6:-3]) / 3
        if recent_avg > earlier_avg * 1.05:
            trend = "widening"
        elif recent_avg < earlier_avg * 0.95:
            trend = "narrowing"
        else:
            trend = "stable"
    elif len(spreads) >= 2:
        if spreads[-1] > spreads[0] * 1.05:
            trend = "widening"
        elif spreads[-1] < spreads[0] * 0.95:
            trend = "narrowing"
        else:
            trend = "stable"
    else:
        trend = "new"
    
    # Price spread stats from continuous period
    price_spreads = [h.price_spread_pct for h in continuous_history if h.price_spread_pct is not None]
    price_spread_current = price_spreads[-1] if price_spreads else None
    price_spread_avg = sum(price_spreads) / len(price_spreads) if price_spreads else None
    
    return {
        "duration_hours": duration,
        "avg_spread": avg,
        "min_spread": min_spread,
        "max_spread": max_spread,
        "trend": trend,
        "data_points": len(spreads),
        "price_spread_pct": price_spread_current,
        "price_spread_avg": price_spread_avg,
    }


# Legacy function - kept for compatibility
def get_continuous_duration(
    symbol: str,
    short_venue: str,
    long_venue: str,
    min_net_spread: float,
    fee_buffer: float = 0.0001,
) -> float | None:
    duration, _ = get_continuous_spread_data(symbol, short_venue, long_venue, min_net_spread)
    return duration


# ============ Established Position Tracking ============

def make_position_key(symbol: str, short_venue: str, long_venue: str) -> str:
    return f"{symbol}:{short_venue}:{long_venue}"


def get_established_position(key: str) -> EstablishedPosition | None:
    with get_session() as session:
        stmt = select(EstablishedPosition).where(EstablishedPosition.key == key)
        return session.exec(stmt).first()


def get_all_active_established() -> list[EstablishedPosition]:
    with get_session() as session:
        stmt = select(EstablishedPosition).where(EstablishedPosition.is_active == True)
        return list(session.exec(stmt).all())


def upsert_established_position(
    symbol: str,
    short_venue: str,
    long_venue: str,
    spread: float,
):
    key = make_position_key(symbol, short_venue, long_venue)
    with get_session() as session:
        existing = session.exec(
            select(EstablishedPosition).where(EstablishedPosition.key == key)
        ).first()
        
        if existing:
            existing.last_seen_spread = spread
            existing.is_active = True
            existing.exit_alerted_at = None
        else:
            position = EstablishedPosition(
                key=key,
                symbol=symbol,
                short_venue=short_venue,
                long_venue=long_venue,
                established_at=datetime.utcnow(),
                last_seen_spread=spread,
                is_active=True,
            )
            session.add(position)
        
        session.commit()


def mark_position_exited(key: str):
    with get_session() as session:
        position = session.exec(
            select(EstablishedPosition).where(EstablishedPosition.key == key)
        ).first()
        
        if position:
            position.is_active = False
            position.exit_alerted_at = datetime.utcnow()
            session.commit()
