import httpx
from app.venues.base import VenueConnector


class ExtendedVenue(VenueConnector):
    """
    Extended exchange perpetual funding rates.
    Market format: {SYMBOL}-USD
    fundingRate is hourly, multiply by 8 for 8h.
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
                
                # Build market lookup
                market_map = {}
                for market in markets:
                    name = market.get("name", "")
                    market_map[name] = market

                for symbol in symbols:
                    market = market_map.get(f"{symbol}-USD")
                    if market:
                        stats = market.get("marketStats", {})
                        funding_rate = stats.get("fundingRate")
                        if funding_rate is not None:
                            result[symbol] = float(funding_rate) * 8

        except Exception as e:
            print(f"[extended] fetch error: {e}", flush=True)

        return result
