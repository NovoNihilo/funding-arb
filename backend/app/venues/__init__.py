from app.venues.base import VenueConnector
from app.venues.mock import MockVenue
from app.venues.hyperliquid import HyperliquidVenue
from app.venues.paradex import ParadexVenue
from app.venues.variational import VariationalVenue
from app.venues.extended import ExtendedVenue
from app.venues.grvt import GRVTVenue

VENUE_REGISTRY: dict[str, type[VenueConnector]] = {
    "mock": MockVenue,
    "hyperliquid": HyperliquidVenue,
    "paradex": ParadexVenue,
    "variational": VariationalVenue,
    "extended": ExtendedVenue,
    "grvt": GRVTVenue,
}


def get_enabled_connectors(venue_names: list[str]) -> list[VenueConnector]:
    connectors = []
    for name in venue_names:
        name = name.strip().lower()
        if name in VENUE_REGISTRY:
            connectors.append(VENUE_REGISTRY[name]())
    return connectors
