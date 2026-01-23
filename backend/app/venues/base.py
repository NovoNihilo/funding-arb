from abc import ABC, abstractmethod


class VenueConnector(ABC):
    @property
    @abstractmethod
    def venue_name(self) -> str:
        pass

    @property
    def supported_symbols(self) -> set[str] | None:
        """
        Override to return set of supported symbols.
        Return None to indicate all requested symbols should be tried.
        """
        return None

    def filter_symbols(self, requested: list[str]) -> list[str]:
        """Filter requested symbols to only those this venue supports."""
        supported = self.supported_symbols
        if supported is None:
            return requested
        return [s for s in requested if s in supported]

    @abstractmethod
    async def fetch_funding(self, symbols: list[str]) -> dict[str, float]:
        """
        Fetch funding rates for given symbols.
        Returns {symbol: funding_rate} where funding_rate is a decimal (e.g., 0.0001 = 0.01%)
        """
        pass
