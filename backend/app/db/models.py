from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field


class FundingSnapshot(SQLModel, table=True):
    __tablename__ = "funding_snapshots"

    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime = Field(default_factory=datetime.utcnow, index=True)
    venue: str = Field(index=True)
    symbol: str = Field(index=True)
    funding_rate: float
    mark_price: Optional[float] = Field(default=None)
    index_price: Optional[float] = Field(default=None)


class AlertState(SQLModel, table=True):
    __tablename__ = "alert_states"

    key: str = Field(primary_key=True)
    last_triggered_at: datetime


class AlertEvent(SQLModel, table=True):
    __tablename__ = "alert_events"

    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime = Field(default_factory=datetime.utcnow, index=True)
    symbol: str = Field(index=True)
    short_venue: str
    long_venue: str
    short_funding: float
    long_funding: float
    spread: float
    net_spread: float
    message: str


class SpreadHistory(SQLModel, table=True):
    __tablename__ = "spread_history"

    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime = Field(default_factory=datetime.utcnow, index=True)
    symbol: str = Field(index=True)
    short_venue: str = Field(index=True)
    long_venue: str = Field(index=True)
    spread: float
    net_spread: float
    price_spread_pct: Optional[float] = Field(default=None)


class EstablishedPosition(SQLModel, table=True):
    """
    Tracks opportunities that have reached Established status.
    Used to detect when they fall below threshold and need exit alerts.
    """
    __tablename__ = "established_positions"

    id: Optional[int] = Field(default=None, primary_key=True)
    key: str = Field(index=True, unique=True)  # symbol:short_venue:long_venue
    symbol: str = Field(index=True)
    short_venue: str
    long_venue: str
    established_at: datetime = Field(default_factory=datetime.utcnow)
    last_seen_spread: float
    is_active: bool = Field(default=True)  # False when exit alert sent
    exit_alerted_at: Optional[datetime] = Field(default=None)
