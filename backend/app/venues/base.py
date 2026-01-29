from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class FundingData:
    """Funding rate and price data for a symbol."""
    funding_rate: float
    mark_price: Optional[float] = None
    index_price: Optional[float] = None


class VenueConnector(ABC):
    @property
    @abstractmethod
    def venue_name(self) -> str:
        pass

    @property
    def supported_symbols(self) -> set[str] | None:
        return None

    def filter_symbols(self, requested: list[str]) -> list[str]:
        supported = self.supported_symbols
        if supported is None:
            return requested
        return [s for s in requested if s in supported]

    @abstractmethod
    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        """Legacy method - returns {symbol: funding_rate}"""
        pass

    async def fetch_funding_with_prices(self, symbols: list[str]) -> dict[str, FundingData]:
        """
        Fetch funding rates and prices.
        Default implementation calls legacy method (no prices).
        Override in subclasses to include price data.
        """
        rates = await self.fetch_funding(symbols)
        return {s: FundingData(funding_rate=r) for s, r in rates.items()}
