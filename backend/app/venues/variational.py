import httpx
from app.venues.base import VenueConnector


class VariationalVenue(VenueConnector):
    """
    Variational (Omni) perpetual funding rates.
    
    API returns funding_rate as a percentage value (0.056925 = 0.056925%).
    The rate is already normalized - funding_interval_s only affects payment
    frequency, not the rate magnitude. We just divide by 1000 to get decimal.
    
    Examples:
    - BTC: funding_rate=0.056925, interval=8h → 0.056925/1000 = 0.000057 = 5.7% APR
    - SAHARA: funding_rate=0.1095, interval=1h → 0.1095/1000 = 0.0001095 = 12% APR
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
                
                # Build ticker lookup from API response
                ticker_map = {}
                for listing in listings:
                    ticker = listing.get("ticker", "").upper()
                    ticker_map[ticker] = listing

                for symbol in symbols:
                    listing = ticker_map.get(symbol)
                    if listing:
                        funding_rate = listing.get("funding_rate")
                        
                        if funding_rate is not None:
                            # funding_rate is percentage (0.056925 = 0.056925%)
                            # Divide by 1000 to get decimal 8h-equivalent rate
                            # Do NOT normalize by interval - rate is already comparable
                            result[symbol] = float(funding_rate) / 1000.0

        except Exception as e:
            print(f"[variational] fetch error: {e}", flush=True)

        return result
