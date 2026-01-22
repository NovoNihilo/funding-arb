import httpx
from app.venues.base import VenueConnector


class ParadexVenue(VenueConnector):
    """
    Paradex perpetual funding rates.
    Docs: https://docs.paradex.trade/api/prod/markets/get-markets-summary
    """

    BASE_URL = "https://api.prod.paradex.trade/v1"

    @property
    def venue_name(self) -> str:
        return "paradex"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Get all markets summary
                resp = await client.get(
                    f"{self.BASE_URL}/markets/summary",
                    params={"market": "ALL"}
                )
                resp.raise_for_status()
                data = resp.json()

                markets = data.get("results", [])

                for market in markets:
                    # Market symbol format: "BTC-USD-PERP"
                    market_symbol = market.get("symbol", "")
                    funding_rate = market.get("funding_rate")

                    for symbol in symbols:
                        if market_symbol == f"{symbol}-USD-PERP":
                            if funding_rate is not None:
                                result[symbol] = float(funding_rate)
                            break

        except Exception as e:
            print(f"[paradex] fetch error: {e}", flush=True)

        return result
