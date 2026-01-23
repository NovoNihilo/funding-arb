"""
Symbol Configuration for Funding Arbitrage Bot
To add a new symbol: Add it to ALL_SYMBOLS list below.
"""

ALL_SYMBOLS = [
    # Majors
    "BTC", "ETH", "SOL",
    
    # L2/Infrastructure
    "ARB", "OP", "STRK", "ZK", "LINEA",
    
    # DeFi
    "LINK", "AAVE", "CRV", "LDO", "JUP", "PENGU",
    
    # Alt L1s
    "SUI", "ADA", "XRP", "XLM", "HBAR", "DOGE",
    
    # Memes & New
    "PUMP", "MON", "W", "WLFI", "VINE",
    "KPEPE", "KSHIB", "KBONK",  # 1000x tokens
    
    # Others
    "BLESS", "SAHARA", "RESOLV",
]

def get_all_symbols() -> list[str]:
    return ALL_SYMBOLS.copy()
