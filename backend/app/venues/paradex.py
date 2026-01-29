import httpx
from app.venues.base import VenueConnector, FundingData


class ParadexVenue(VenueConnector):
    BASE_URL = "https://api.prod.paradex.trade/v1"

    @property
    def venue_name(self) -> str:
        return "paradex"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        data = await self.fetch_funding_with_prices(symbols)
        return {s: d.funding_rate for s, d in data.items()}

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Get funding periods
                markets_resp = await client.get(f"{self.BASE_URL}/markets")
                markets_resp.raise_for_status()
                markets_data = markets_resp.json()
                
                funding_periods = {}
                for market in markets_data.get("results", []):
                    symbol = market.get("symbol", "")
                    period = market.get("funding_period_hours", 8)
                    funding_periods[symbol] = period
                
                # Get funding rates and prices
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
                    mark_price = market.get("mark_price")
                    underlying_price = market.get("underlying_price")

                    for symbol in symbols:
                        if market_symbol == f"{symbol}-USD-PERP":
                            if funding_rate is not None:
                                period_hours = funding_periods.get(market_symbol, 8)
                                rate_8h = float(funding_rate) * (8 / period_hours)
                                
                                result[symbol] = FundingData(
                                    funding_rate=rate_8h,
                                    mark_price=float(mark_price) if mark_price else None,
                                    index_price=float(underlying_price) if underlying_price else None,
                                )
                            break

        except Exception as e:
            print(f"[paradex] fetch error: {e}", flush=True)

        return result
