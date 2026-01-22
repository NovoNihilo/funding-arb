import httpx
from app.venues.base import VenueConnector


class VertexVenue(VenueConnector):
    """
    Vertex Protocol perpetual funding rates.
    Docs: https://docs.vertexprotocol.com/developer-resources/api/archive-indexer/funding-rate
    """

    # Archive indexer endpoint
    BASE_URL = "https://archive.prod.vertexprotocol.com/v1"

    # Vertex uses product IDs for perps (even numbers starting from 2)
    PRODUCT_IDS = {
        "BTC": 2,
        "ETH": 4,
        "ARB": 6,
        "SOL": 8,
    }

    @property
    def venue_name(self) -> str:
        return "vertex"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}

        # Filter to symbols we have product IDs for
        product_ids = [self.PRODUCT_IDS[s] for s in symbols if s in self.PRODUCT_IDS]
        if not product_ids:
            return result

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Query funding rates for multiple products
                resp = await client.post(
                    self.BASE_URL,
                    json={
                        "funding_rates": {
                            "product_ids": product_ids
                        }
                    }
                )
                resp.raise_for_status()
                data = resp.json()

                # Response format: {"2": {"product_id": 2, "funding_rate_x18": "...", ...}, ...}
                for symbol, product_id in self.PRODUCT_IDS.items():
                    if symbol not in symbols:
                        continue

                    product_data = data.get(str(product_id))
                    if product_data:
                        funding_x18 = product_data.get("funding_rate_x18")
                        if funding_x18:
                            # funding_rate_x18 is multiplied by 10^18, and is 24h rate
                            # Convert to 8h rate
                            rate_24h = int(funding_x18) / 1e18
                            rate_8h = rate_24h / 3  # 24h / 3 = 8h
                            result[symbol] = rate_8h

        except Exception as e:
            print(f"[vertex] fetch error: {e}", flush=True)

        return result
