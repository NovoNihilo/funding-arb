import pytest
from datetime import datetime, timedelta
from sqlmodel import SQLModel, create_engine, Session

# Override engine before importing repository
import app.db.engine as engine_module

_test_engine = create_engine("sqlite:///:memory:")
engine_module._engine = _test_engine


from app.db.models import FundingSnapshot, AlertState, AlertEvent
from app.db import repository


@pytest.fixture(autouse=True)
def setup_db():
    SQLModel.metadata.create_all(_test_engine)
    yield
    SQLModel.metadata.drop_all(_test_engine)


def test_insert_and_get_snapshot():
    snap = repository.insert_snapshot("mock", "BTC", 0.0005)
    assert snap.id is not None
    assert snap.venue == "mock"
    assert snap.symbol == "BTC"
    assert snap.funding_rate == 0.0005

    results = repository.get_latest_snapshots("mock", "BTC", limit=1)
    assert len(results) == 1
    assert results[0].funding_rate == 0.0005


def test_get_latest_funding_by_symbol():
    repository.insert_snapshot("venue_a", "ETH", 0.001)
    repository.insert_snapshot("venue_b", "ETH", -0.0005)
    repository.insert_snapshot("venue_a", "ETH", 0.002)  # newer

    result = repository.get_latest_funding_by_symbol(["ETH"])
    assert "ETH" in result
    assert result["ETH"]["venue_a"] == 0.002
    assert result["ETH"]["venue_b"] == -0.0005


def test_alert_state_upsert():
    key = "BTC:venue_a:venue_b:free"
    now = datetime.utcnow()

    repository.upsert_alert_state(key, now)
    state = repository.get_alert_state(key)
    assert state is not None
    assert state.last_triggered_at == now

    later = now + timedelta(hours=1)
    repository.upsert_alert_state(key, later)
    state = repository.get_alert_state(key)
    assert state.last_triggered_at == later


def test_insert_and_get_events():
    event = repository.insert_event(
        symbol="BTC",
        short_venue="venue_a",
        long_venue="venue_b",
        spread=0.001,
        net_spread=0.0008,
        message="Test alert",
    )
    assert event.id is not None

    events = repository.get_recent_events(limit=10)
    assert len(events) == 1
    assert events[0].symbol == "BTC"