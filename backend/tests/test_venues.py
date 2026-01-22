import pytest
from unittest.mock import patch
from app.venues.mock import MockVenue
from app.venues.hyperliquid import HyperliquidVenue
from app.venues import get_enabled_connectors


@pytest.mark.asyncio
async def test_mock_venue_returns_expected():
    venue = MockVenue()
    assert venue.venue_name == "mock"

    rates = await venue.fetch_funding(["BTC", "ETH", "UNKNOWN"])
    assert rates["BTC"] == 0.0005
    assert rates["ETH"] == -0.0002
    assert "UNKNOWN" not in rates


@pytest.mark.asyncio
async def test_hyperliquid_parsing():
    venue = HyperliquidVenue()
    with patch.object(venue, "fetch_funding", return_value={"BTC": 0.00012345, "ETH": -0.00005}):
        rates = await venue.fetch_funding(["BTC", "ETH"])
        assert rates["BTC"] == 0.00012345
        assert rates["ETH"] == -0.00005


def test_venue_registry():
    connectors = get_enabled_connectors(["mock", "hyperliquid", "unknown"])
    assert len(connectors) == 2
    names = {c.venue_name for c in connectors}
    assert names == {"mock", "hyperliquid"}
