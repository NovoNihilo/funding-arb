from abc import ABC, abstractmethod


class VenueConnector(ABC):
    @property
    @abstractmethod
    def venue_name(self) -> str:
        pass

    @abstractmethod
    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        """
        Fetch funding rates for given symbols.
        Returns {symbol: funding_rate} where funding_rate is a decimal (e.g., 0.0001 = 0.01%)
        """
        pass
