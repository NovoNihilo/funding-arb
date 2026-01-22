from datetime import datetime
from sqlmodel import select
from app.db.engine import get_session
from app.db.models import FundingSnapshot, AlertState, AlertEvent


def insert_snapshot(venue: str, symbol: str, funding_rate: float, ts: datetime = None):
    with get_session() as session:
        snapshot = FundingSnapshot(
            ts=ts or datetime.utcnow(),
            venue=venue,
            symbol=symbol,
            funding_rate=funding_rate,
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
    """Returns {symbol: {venue: funding_rate}} for latest snapshot per venue/symbol."""
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
) -> AlertEvent:
    with get_session() as session:
        event = AlertEvent(
            ts=datetime.utcnow(),
            symbol=symbol,
            short_venue=short_venue,
            long_venue=long_venue,
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