import httpx
from app.venues.base import VenueConnector


class ParadexVenue(VenueConnector):
    """
    Paradex perpetual funding rates.
    Market format: {SYMBOL}-USD-PERP
    
    Important: Different assets have different funding periods (1h, 4h, 8h).
    The /v1/markets endpoint provides funding_period_hours per asset.
    The /v1/markets/summary endpoint provides the funding_rate per period.
    
    We fetch both and normalize all rates to 8h equivalent.
    """
    BASE_URL = "https://api.prod.paradex.trade/v1"

    @property
    def venue_name(self) -> str:
        return "paradex"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # First, get funding periods for each market
                markets_resp = await client.get(f"{self.BASE_URL}/markets")
                markets_resp.raise_for_status()
                markets_data = markets_resp.json()
                
                # Build a map of symbol -> funding_period_hours
                funding_periods = {}
                for market in markets_data.get("results", []):
                    symbol = market.get("symbol", "")
                    period = market.get("funding_period_hours", 8)  # Default to 8h
                    funding_periods[symbol] = period
                
                # Now get the funding rates
                summary_resp = await client.get(
                    f"{self.BASE_URL}/markets/summary",
                    params={"market": "ALL"}
                )
                summary_resp.raise_for_status()
                summary_data = summary_resp.json()

                markets = summary_data.get("results", [])

                for market in markets:
                    market_symbol = market.get("symbol", "")
                    funding_rate = market.get("funding_rate")

                    for symbol in symbols:
                        if market_symbol == f"{symbol}-USD-PERP":
                            if funding_rate is not None:
                                # Get the funding period for this asset
                                period_hours = funding_periods.get(market_symbol, 8)
                                
                                # Normalize to 8h: multiply by (8 / period_hours)
                                # e.g., 1h rate * 8 = 8h rate
                                # e.g., 4h rate * 2 = 8h rate
                                # e.g., 8h rate * 1 = 8h rate
                                rate_8h = float(funding_rate) * (8 / period_hours)
                                result[symbol] = rate_8h
                            break

        except Exception as e:
            print(f"[paradex] fetch error: {e}", flush=True)

        return result
