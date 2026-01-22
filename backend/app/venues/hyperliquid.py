import httpx
from app.venues.base import VenueConnector


class HyperliquidVenue(VenueConnector):
    BASE_URL = "https://api.hyperliquid.xyz/info"

    @property
    def venue_name(self) -> str:
        return "hyperliquid"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
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
                            if funding is not None:
                                # Hyperliquid returns funding per tick (3 ticks/hour)
                                # Multiply by 24 to get 8h rate (3 ticks * 8 hours)
                                result[symbol] = float(funding) * 24

        except Exception as e:
            print(f"[hyperliquid] fetch error: {e}")

        return result
