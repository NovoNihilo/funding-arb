"""
Hyperliquid venue connector supporting both main perps and HIP-3 builder-deployed perps.

FUNDING RATES:
- ALL Hyperliquid funding rates (main AND HIP-3) are HOURLY
- We multiply by 8 to get the standard 8h rate for comparison

Asset naming:
- Main perps: "SOL", "BTC", etc.
- HIP-3: "hyna:SOL", "flx:GOLD", etc. -> we strip prefix and add suffix
"""
import asyncio
import httpx
from app.venues.base import VenueConnector, FundingData


class HyperliquidVenue(VenueConnector):
    BASE_URL = "https://api.hyperliquid.xyz/info"
    
    # HIP-3 dexs to fetch (crypto/commodities, skip pure stock dexs)
    HIP3_DEXS = ["hyna", "flx", "km"]
    
    # Map HIP-3 asset names to our normalized symbols for commodities
    HIP3_NORMALIZE = {
        "GOLD": "XAUUSD",
        "SILVER": "XAGUSD",
        "PLATINUM": "XPTUSD",
        "PALLADIUM": "XPDUSD",
        "COPPER": "XCUUSD",
        "OIL": "XTIUSD",
    }

    @property
    def venue_name(self) -> str:
        return "hyperliquid"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        data = await self.fetch_funding_with_prices(symbols)
        return {s: d.funding_rate for s, d in data.items()}

    async def _fetch_main_perps(self, client: httpx.AsyncClient, symbols: list[str]) -> dict[str, FundingData]:
        """
        Fetch from main Hyperliquid perps.
        Funding is HOURLY - multiply by 8 for 8h rate.
        """
        result = {}
        try:
            resp = await client.post(
                self.BASE_URL,
                json={"type": "metaAndAssetCtxs"},
            )
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list) or len(data) < 2:
                return result

            meta = data[0]
            asset_ctxs = data[1]
            universe = meta.get("universe", [])
            coin_to_idx = {coin["name"]: i for i, coin in enumerate(universe)}

            for symbol in symbols:
                if symbol in coin_to_idx:
                    idx = coin_to_idx[symbol]
                    if idx < len(asset_ctxs):
                        ctx = asset_ctxs[idx]
                        funding = ctx.get("funding")
                        mark_price = ctx.get("markPx")
                        oracle_price = ctx.get("oraclePx")
                        
                        if funding is not None:
                            # HOURLY rate -> multiply by 8 for 8h rate
                            result[symbol] = FundingData(
                                funding_rate=float(funding) * 8,
                                mark_price=float(mark_price) if mark_price else None,
                                index_price=float(oracle_price) if oracle_price else None,
                            )
        except Exception as e:
            print(f"[hyperliquid] Main perps fetch error: {e}", flush=True)
        
        return result

    async def _fetch_hip3_dex(self, client: httpx.AsyncClient, dex: str) -> dict[str, FundingData]:
        """
        Fetch ALL assets from a HIP-3 dex.
        
        Funding is HOURLY - multiply by 8 for 8h rate (same as main perps).
        Asset names come as "hyna:SOL" -> we extract "SOL" and add "_hyna" suffix.
        
        Returns dict with keys like "SOL_hyna", "BTC_hyna", "XAUUSD_flx", etc.
        """
        result = {}
        try:
            resp = await client.post(
                self.BASE_URL,
                json={"type": "metaAndAssetCtxs", "dex": dex},
            )
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list) or len(data) < 2:
                return result

            meta = data[0]
            asset_ctxs = data[1]
            universe = meta.get("universe", [])
            
            for i, coin_info in enumerate(universe):
                if i >= len(asset_ctxs):
                    break
                
                # Skip delisted assets
                if coin_info.get("isDelisted"):
                    continue
                    
                raw_name = coin_info.get("name", "")
                if not raw_name:
                    continue
                
                # Extract base asset name: "hyna:SOL" -> "SOL"
                if ":" in raw_name:
                    base_name = raw_name.split(":")[1]
                else:
                    base_name = raw_name
                
                ctx = asset_ctxs[i]
                funding = ctx.get("funding")
                mark_price = ctx.get("markPx")
                oracle_price = ctx.get("oraclePx")
                
                if funding is None:
                    continue
                
                # Normalize commodity names
                normalized_symbol = self.HIP3_NORMALIZE.get(base_name, base_name)
                
                # Create key with dex suffix: "SOL_hyna", "XAUUSD_flx"
                hip3_key = f"{normalized_symbol}_{dex}"
                
                # HOURLY rate -> multiply by 8 for 8h rate
                result[hip3_key] = FundingData(
                    funding_rate=float(funding) * 8,
                    mark_price=float(mark_price) if mark_price else None,
                    index_price=float(oracle_price) if oracle_price else None,
                )
                        
        except Exception as e:
            print(f"[hyperliquid] HIP-3 dex '{dex}' fetch error: {e}", flush=True)
        
        return result

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        """
        Fetch funding rates from main perps AND all HIP-3 dexs.
        
        Returns dict with:
        - Main perps: "BTC", "ETH", "SOL", etc.
        - HIP-3 perps: "BTC_hyna", "SOL_hyna", "XAUUSD_flx", etc.
        """
        result = {}
        
        async with httpx.AsyncClient(timeout=15) as client:
            # Fetch main perps
            main_result = await self._fetch_main_perps(client, symbols)
            result.update(main_result)
            
            # Fetch ALL assets from each HIP-3 dex
            for dex in self.HIP3_DEXS:
                hip3_result = await self._fetch_hip3_dex(client, dex)
                result.update(hip3_result)
                await asyncio.sleep(0.1)  # Small delay between dex calls
        
        return result
