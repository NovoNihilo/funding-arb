import httpx
from app.venues.base import VenueConnector


class VariationalVenue(VenueConnector):
    """
    Variational (Omni) perpetual funding rates.
    
    IMPORTANT: Variational returns funding_rate as a PERCENTAGE per interval.
    e.g., 0.045 means 4.5% per interval, NOT 0.045 decimal.
    """

    BASE_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io"

    @property
    def venue_name(self) -> str:
        return "variational"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{self.BASE_URL}/metadata/stats")
                resp.raise_for_status()
                data = resp.json()

                listings = data.get("listings", [])

                for listing in listings:
                    ticker = listing.get("ticker", "").upper()
                    funding_rate = listing.get("funding_rate")
                    funding_interval = listing.get("funding_interval_s", 28800)

                    for symbol in symbols:
                        if ticker == symbol:
                            if funding_rate is not None:
                                # funding_rate is already decimal (0.0001 = 0.01%)
                                # Normalize to 8h rate based on funding interval
                                rate = float(funding_rate)
                                normalized_8h = rate * (28800 / funding_interval)
                                result[symbol] = normalized_8h
                            break

        except Exception as e:
            print(f"[variational] fetch error: {e}", flush=True)

        return result
