import httpx
from app.venues.base import VenueConnector


class BybitVenue(VenueConnector):
    """
    Bybit USDT Perpetual funding rates.
    Docs: https://bybit-exchange.github.io/docs/v5/market/tickers
    """

    BASE_URL = "https://api.bybit.com"

    @property
    def venue_name(self) -> str:
        return "bybit"

    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        result = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Get all linear tickers (includes funding rates)
                resp = await client.get(
                    f"{self.BASE_URL}/v5/market/tickers",
                    params={"category": "linear"}
                )
                resp.raise_for_status()
                data = resp.json()

                if data.get("retCode") != 0:
                    print(f"[bybit] API error: {data.get('retMsg')}", flush=True)
                    return result

                tickers = data.get("result", {}).get("list", [])

                for ticker in tickers:
                    bybit_symbol = ticker.get("symbol", "")
                    # Convert BTCUSDT -> BTC
                    for symbol in symbols:
                        if bybit_symbol == f"{symbol}USDT":
                            funding = ticker.get("fundingRate")
                            if funding is not None:
                                result[symbol] = float(funding)
                            break

        except Exception as e:
            print(f"[bybit] fetch error: {e}", flush=True)

        return result
