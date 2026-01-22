import httpx
from app.venues.base import VenueConnector


class BinanceVenue(VenueConnector):
    """
    Binance USDT-M Futures funding rates.
    Docs: https://binance-docs.github.io/apidocs/futures/en/#get-funding-rate-history
    """

    BASE_URL = "https://fapi.binance.com"

    @property
    def venue_name(self) -> str:
        return "binance"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Get all premium index data (includes current funding rates)
                resp = await client.get(f"{self.BASE_URL}/fapi/v1/premiumIndex")
                resp.raise_for_status()
                data = resp.json()

                # Build symbol mapping (Binance uses BTCUSDT format)
                for item in data:
                    binance_symbol = item.get("symbol", "")
                    # Convert BTCUSDT -> BTC
                    for symbol in symbols:
                        if binance_symbol == f"{symbol}USDT":
                            funding = item.get("lastFundingRate")
                            if funding is not None:
                                result[symbol] = float(funding)
                            break

        except Exception as e:
            print(f"[binance] fetch error: {e}", flush=True)

        return result
