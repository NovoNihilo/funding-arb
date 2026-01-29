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
