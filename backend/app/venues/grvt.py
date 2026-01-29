import httpx
from app.venues.base import VenueConnector, FundingData


class GRVTVenue(VenueConnector):
    BASE_URL = "https://market-data.grvt.io"

    INSTRUMENTS = {
        "BTC": "BTC_USDT_Perp",
        "ETH": "ETH_USDT_Perp",
        "SOL": "SOL_USDT_Perp",
        "ARB": "ARB_USDT_Perp",
        "OP": "OP_USDT_Perp",
        "STRK": "STRK_USDT_Perp",
        "ZK": "ZK_USDT_Perp",
        "LINK": "LINK_USDT_Perp",
        "AAVE": "AAVE_USDT_Perp",
        "CRV": "CRV_USDT_Perp",
        "LDO": "LDO_USDT_Perp",
        "JUP": "JUP_USDT_Perp",
        "PENGU": "PENGU_USDT_Perp",
        "SUI": "SUI_USDT_Perp",
        "ADA": "ADA_USDT_Perp",
        "XRP": "XRP_USDT_Perp",
        "DOGE": "DOGE_USDT_Perp",
        "PUMP": "PUMP_USDT_Perp",
        "W": "W_USDT_Perp",
        "RESOLV": "RESOLV_USDT_Perp",
    }

    @property
    def venue_name(self) -> str:
        return "grvt"

    @property
    def supported_symbols(self) -> set[str]:
        return set(self.INSTRUMENTS.keys())

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        data = await self.fetch_funding_with_prices(symbols)
        return {s: d.funding_rate for s, d in data.items()}

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        result = {}
        filtered = self.filter_symbols(symbols)
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                for symbol in filtered:
                    instrument = self.INSTRUMENTS.get(symbol)
                    if not instrument:
                        continue
                    
                    try:
                        # Get funding interval
                        instrument_resp = await client.post(
                            f"{self.BASE_URL}/full/v1/instrument",
                            json={"instrument": instrument}
                        )
                        instrument_resp.raise_for_status()
                        instrument_data = instrument_resp.json()
                        interval_hours = instrument_data.get("result", {}).get("funding_interval_hours", 8)
                        
                        # Get ticker data
                        ticker_resp = await client.post(
                            f"{self.BASE_URL}/full/v1/ticker",
                            json={"instrument": instrument}
                        )
                        ticker_resp.raise_for_status()
                        ticker_data = ticker_resp.json()
                        
                        ticker_result = ticker_data.get("result", {})
                        funding_rate = ticker_result.get("funding_rate_8h_curr")
                        mark_price = ticker_result.get("mark_price")
                        index_price = ticker_result.get("index_price")
                        
                        if funding_rate is not None:
                            rate_per_interval = float(funding_rate) / 100.0
                            rate_8h = rate_per_interval * (8 / interval_hours)
                            
                            result[symbol] = FundingData(
                                funding_rate=rate_8h,
                                mark_price=float(mark_price) if mark_price else None,
                                index_price=float(index_price) if index_price else None,
                            )
                            
                    except Exception:
                        continue

        except Exception as e:
            print(f"[grvt] fetch error: {e}", flush=True)

        return result
