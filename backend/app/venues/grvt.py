import httpx
from app.venues.base import VenueConnector


class GRVTVenue(VenueConnector):
    """
    GRVT (Gravity Markets) perpetual funding rates.
    Uses POST /full/v1/ticker - funding_rate_8h_curr returns PERCENTAGE (0.01 = 0.01%, not 1%)
    Must divide by 100 to get decimal form for comparison with other venues.
    """

    BASE_URL = "https://market-data.grvt.io"

    INSTRUMENTS = {
        "BTC": "BTC_USDT_Perp",
        "ETH": "ETH_USDT_Perp",
        "SOL": "SOL_USDT_Perp",
        "ARB": "ARB_USDT_Perp",
    }

    @property
    def venue_name(self) -> str:
        return "grvt"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                for symbol in symbols:
                    instrument = self.INSTRUMENTS.get(symbol)
                    if not instrument:
                        continue
                    
                    try:
                        resp = await client.post(
                            f"{self.BASE_URL}/full/v1/ticker",
                            json={"instrument": instrument}
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        
                        ticker_result = data.get("result", {})
                        funding_rate = ticker_result.get("funding_rate_8h_curr")
                        
                        if funding_rate is not None:
                            # GRVT returns percentage (0.01 = 0.01%), divide by 100 for decimal
                            result[symbol] = float(funding_rate) / 100.0
                            
                    except Exception as e:
                        print(f"[grvt] error fetching {symbol}: {e}", flush=True)
                        continue

        except Exception as e:
            print(f"[grvt] fetch error: {e}", flush=True)

        return result
