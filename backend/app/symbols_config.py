"""
Symbol Configuration for Funding Arbitrage Bot

To add a new symbol: Add it to ALL_SYMBOLS list below.
The bot will automatically track it across all venues that support it.

Note: Some symbols have venue-specific variants (e.g., HYPE_hyna for Hyperliquid HIP-3)
These are handled automatically by the venue connectors.
"""

ALL_SYMBOLS = [
    # === MAJORS ===
    "BTC", "ETH", "SOL",
    
    # === L2/Infrastructure ===
    "ARB", "OP", "STRK", "ZK", "LINEA",
    
    # === DeFi ===
    "LINK", "AAVE", "CRV", "LDO", "JUP", "PENGU",
    
    # === Alt L1s ===
    "SUI", "ADA", "XRP", "XLM", "HBAR", "DOGE",
    
    # === Memes & New ===
    "PUMP", "MON", "W", "WLFI", "VINE",
    "KPEPE", "KSHIB", "KBONK",  # 1000x tokens
    "FARTCOIN",
    
    # === Privacy/Legacy ===
    "XMR", "ZEC", "LTC", "BCH",
    
    # === Hyperliquid Ecosystem ===
    "HYPE", "PURR",
    
    # === Other Crypto ===
    "ENA", "LIGHTER", "IP", "XPL",
    
    # === Commodities (for cross-venue arb with PAXG, XAUT, etc.) ===
    "XAUUSD",   # Gold - Hyperliquid HIP-3 has GOLD perps
    "XAGUSD",   # Silver
    "XPTUSD",   # Platinum  
    "XPDUSD",   # Palladium
    "XCUUSD",   # Copper
    
    # === Gold-backed tokens (for arb against XAUUSD) ===
    "PAXG",     # Pax Gold
    "XAUT",     # Tether Gold
    
    # === Legacy/Others ===
    "BLESS", "SAHARA", "RESOLV",
]


# Symbols that exist on HIP-3 dexs with different quote currencies
# The Hyperliquid connector will automatically append dex suffix (e.g., BTC_hyna)
HIP3_TRACKED_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "SUI",
    "HYPE", "PUMP", "FARTCOIN", "XMR", "LTC", "ENA", "BCH", "ZEC",
    "LIGHTER", "IP", "XPL",
    "XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD", "XCUUSD",  # Commodities
]


def get_all_symbols() -> list[str]:
    """Get a copy of all configured symbols."""
    return ALL_SYMBOLS.copy()


def get_hip3_symbols() -> list[str]:
    """Get symbols that should be tracked on HIP-3 dexs."""
    return HIP3_TRACKED_SYMBOLS.copy()
