import httpx
from app.venues.base import VenueConnector, FundingData


class HyperliquidVenue(VenueConnector):
    BASE_URL = "https://api.hyperliquid.xyz/info"

    @property
    def venue_name(self) -> str:
        return "hyperliquid"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        data = await self.fetch_funding_with_prices(symbols)
        return {s: d.funding_rate for s, d in data.items()}

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
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
                                result[symbol] = FundingData(
                                    funding_rate=float(funding) * 8,
                                    mark_price=float(mark_price) if mark_price else None,
                                    index_price=float(oracle_price) if oracle_price else None,
                                )

        except Exception as e:
            print(f"[hyperliquid] fetch error: {e}")

        return result
