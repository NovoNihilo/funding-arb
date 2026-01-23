import httpx
from app.venues.base import VenueConnector


class GRVTVenue(VenueConnector):
    """
    GRVT (Gravity Markets) perpetual funding rates.
    funding_rate_8h_curr is percentage (0.01 = 0.01%), divide by 100.
    """
    BASE_URL = "https://market-data.grvt.io"

    # Map symbols to GRVT instrument names
    # Format is typically {SYMBOL}_USDT_Perp
    INSTRUMENTS = {
        # Majors
        "BTC": "BTC_USDT_Perp",
        "ETH": "ETH_USDT_Perp",
        "SOL": "SOL_USDT_Perp",
        
        # L2/Infrastructure
        "ARB": "ARB_USDT_Perp",
        "OP": "OP_USDT_Perp",
        "STRK": "STRK_USDT_Perp",
        "ZK": "ZK_USDT_Perp",
        "LINEA": "LINEA_USDT_Perp",
        
        # DeFi
        "LINK": "LINK_USDT_Perp",
        "AAVE": "AAVE_USDT_Perp",
        "CRV": "CRV_USDT_Perp",
        "LDO": "LDO_USDT_Perp",
        "JUP": "JUP_USDT_Perp",
        "PENGU": "PENGU_USDT_Perp",
        
        # Alt L1s
        "SUI": "SUI_USDT_Perp",
        "ADA": "ADA_USDT_Perp",
        "XRP": "XRP_USDT_Perp",
        "XLM": "XLM_USDT_Perp",
        "HBAR": "HBAR_USDT_Perp",
        "DOGE": "DOGE_USDT_Perp",
        
        # Memes & New
        "PUMP": "PUMP_USDT_Perp",
        "MON": "MON_USDT_Perp",
        "W": "W_USDT_Perp",
        "WLFI": "WLFI_USDT_Perp",
        "VINE": "VINE_USDT_Perp",
        "KPEPE": "KPEPE_USDT_Perp",
        "KSHIB": "KSHIB_USDT_Perp",
        "KBONK": "KBONK_USDT_Perp",
        
        # Others
        "BLESS": "BLESS_USDT_Perp",
        "SAHARA": "SAHARA_USDT_Perp",
        "RESOLV": "RESOLV_USDT_Perp",
    }

    @property
    def venue_name(self) -> str:
        return "grvt"

    @property
    def supported_symbols(self) -> set[str]:
        return set(self.INSTRUMENTS.keys())

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        filtered = self.filter_symbols(symbols)
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                for symbol in filtered:
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
                            result[symbol] = float(funding_rate) / 100.0
                            
                    except Exception as e:
                        # Silently skip symbols that don't exist on GRVT
                        continue

        except Exception as e:
            print(f"[grvt] fetch error: {e}", flush=True)

        return result
