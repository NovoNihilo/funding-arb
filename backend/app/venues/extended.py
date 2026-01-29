import httpx
from app.venues.base import VenueConnector, FundingData


class ExtendedVenue(VenueConnector):
    BASE_URL = "https://api.starknet.extended.exchange/api/v1"

    @property
    def venue_name(self) -> str:
        return "extended"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        data = await self.fetch_funding_with_prices(symbols)
        return {s: d.funding_rate for s, d in data.items()}

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{self.BASE_URL}/info/markets")
                resp.raise_for_status()
                data = resp.json()

                markets = data.get("data", [])
                
                market_map = {}
                for market in markets:
                    name = market.get("name", "")
                    market_map[name] = market

                for symbol in symbols:
                    market = market_map.get(f"{symbol}-USD")
                    if market:
                        stats = market.get("marketStats", {})
                        funding_rate = stats.get("fundingRate")
                        mark_price = stats.get("markPrice")
                        index_price = stats.get("indexPrice")
                        
                        if funding_rate is not None:
                            result[symbol] = FundingData(
                                funding_rate=float(funding_rate) * 8,
                                mark_price=float(mark_price) if mark_price else None,
                                index_price=float(index_price) if index_price else None,
                            )

        except Exception as e:
            print(f"[extended] fetch error: {e}", flush=True)

        return result
