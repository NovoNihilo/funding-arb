import httpx
from app.venues.base import VenueConnector


class ExtendedVenue(VenueConnector):
    """
    Extended exchange perpetual funding rates.
    Uses GET /api/v1/info/markets - fundingRate is hourly, multiply by 8.
    """

    BASE_URL = "https://api.starknet.extended.exchange/api/v1"

    @property
    def venue_name(self) -> str:
        return "extended"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{self.BASE_URL}/info/markets")
                resp.raise_for_status()
                data = resp.json()

                markets = data.get("data", [])

                for market in markets:
                    market_name = market.get("name", "")
                    market_stats = market.get("marketStats", {})
                    funding_rate = market_stats.get("fundingRate")

                    for symbol in symbols:
                        if market_name == f"{symbol}-USD":
                            if funding_rate is not None:
                                # Hourly rate, multiply by 8 for 8h
                                rate = float(funding_rate)
                                result[symbol] = rate * 8
                            break

        except Exception as e:
            print(f"[extended] fetch error: {e}", flush=True)

        return result
