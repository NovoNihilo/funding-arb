import httpx
from app.venues.base import VenueConnector, FundingData


class VariationalVenue(VenueConnector):
    BASE_URL = "https://omni-client-api.prod.ap-northeast-1.variational.io"

    @property
    def venue_name(self) -> str:
        return "variational"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        data = await self.fetch_funding_with_prices(symbols)
        return {s: d.funding_rate for s, d in data.items()}

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{self.BASE_URL}/metadata/stats")
                resp.raise_for_status()
                data = resp.json()

                listings = data.get("listings", [])
                
                ticker_map = {}
                for listing in listings:
                    ticker = listing.get("ticker", "").upper()
                    ticker_map[ticker] = listing

                for symbol in symbols:
                    listing = ticker_map.get(symbol)
                    if listing:
                        funding_rate = listing.get("funding_rate")
                        mark_price = listing.get("mark_price")
                        
                        if funding_rate is not None:
                            result[symbol] = FundingData(
                                funding_rate=float(funding_rate) / 1000.0,
                                mark_price=float(mark_price) if mark_price else None,
                                index_price=None,  # Not available
                            )

        except Exception as e:
            print(f"[variational] fetch error: {e}", flush=True)

        return result
