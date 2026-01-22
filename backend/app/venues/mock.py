from app.venues.base import VenueConnector


class MockVenue(VenueConnector):
    MOCK_RATES = {
        "BTC": 0.0010,
        "ETH": 0.0008,
        "SOL": 0.0012,
        "ARB": 0.0005,
    }

    @property
    def venue_name(self) -> str:
        return "mock"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        return {s: self.MOCK_RATES[s] for s in symbols if s in self.MOCK_RATES}
