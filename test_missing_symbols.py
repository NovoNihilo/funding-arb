#!/usr/bin/env python3
"""
Find high-funding HIP-3 assets that we're NOT tracking.
"""
import asyncio
import httpx

BASE_URL = "https://api.hyperliquid.xyz/info"

# Current symbols from symbols_config.py
CURRENT_SYMBOLS = {
    "BTC", "ETH", "SOL", "ARB", "OP", "STRK", "ZK", "LINEA",
    "LINK", "AAVE", "CRV", "LDO", "JUP", "PENGU",
    "SUI", "ADA", "XRP", "XLM", "HBAR", "DOGE",
    "PUMP", "MON", "W", "WLFI", "VINE",
    "KPEPE", "KSHIB", "KBONK", "FARTCOIN",
    "XMR", "ZEC", "LTC", "BCH",
    "HYPE", "PURR", "ENA", "LIGHTER", "IP", "XPL",
    "XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD", "XCUUSD",
    "PAXG", "XAUT", "BLESS", "SAHARA", "RESOLV",
}

# Commodity name mapping
HIP3_NORMALIZE = {
    "GOLD": "XAUUSD",
    "SILVER": "XAGUSD",
    "PLATINUM": "XPTUSD",
    "PALLADIUM": "XPDUSD",
    "COPPER": "XCUUSD",
    "OIL": "XTIUSD",
}

async def fetch_hip3_high_funding():
    """Find all HIP-3 assets with high funding."""
    print("=" * 70)
    print("HIP-3 HIGH FUNDING ASSETS (sorted by absolute funding rate)")
    print("=" * 70)
    
    all_assets = []
    
    async with httpx.AsyncClient(timeout=15) as client:
        for dex in ["hyna", "flx", "km"]:
            resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs", "dex": dex})
            data = resp.json()
            
            meta = data[0]
            asset_ctxs = data[1]
            universe = meta.get("universe", [])
            
            for i, coin_info in enumerate(universe):
                if i >= len(asset_ctxs):
                    break
                
                if coin_info.get("isDelisted"):
                    continue
                    
                raw_name = coin_info.get("name", "")
                base_name = raw_name.split(":")[1] if ":" in raw_name else raw_name
                
                ctx = asset_ctxs[i]
                funding = ctx.get("funding")
                
                if funding is None:
                    continue
                
                funding_8h = float(funding)  # Already 8h rate
                apr = funding_8h * 1095 * 100  # APR as percentage
                
                # Normalize commodity names
                normalized = HIP3_NORMALIZE.get(base_name, base_name)
                
                all_assets.append({
                    "dex": dex,
                    "raw_name": raw_name,
                    "base_name": base_name,
                    "normalized": normalized,
                    "funding_8h": funding_8h,
                    "funding_pct": funding_8h * 100,
                    "apr": apr,
                    "tracked": normalized in CURRENT_SYMBOLS,
                })
            
            await asyncio.sleep(0.1)
    
    # Sort by absolute funding rate
    all_assets.sort(key=lambda x: abs(x["funding_8h"]), reverse=True)
    
    print(f"\n{'Symbol':<12} {'Dex':<6} {'8h Rate':<10} {'APR':<10} {'Tracked?'}")
    print("-" * 55)
    
    missing_high_funding = []
    
    for asset in all_assets:
        status = "✓" if asset["tracked"] else "❌ MISSING"
        print(f"{asset['normalized']:<12} {asset['dex']:<6} {asset['funding_pct']:>8.4f}% {asset['apr']:>8.1f}%  {status}")
        
        if not asset["tracked"] and abs(asset["apr"]) > 20:
            missing_high_funding.append(asset)
    
    if missing_high_funding:
        print("\n" + "=" * 70)
        print("⚠️  HIGH-FUNDING ASSETS NOT BEING TRACKED:")
        print("=" * 70)
        for asset in missing_high_funding:
            print(f"  {asset['normalized']}: {asset['apr']:.1f}% APR on {asset['dex']}")
        
        print("\n📝 Add these to symbols_config.py:")
        missing_symbols = set(a["normalized"] for a in missing_high_funding)
        print(f"  {', '.join(sorted(missing_symbols))}")


async def check_xpl_across_venues():
    """Check XPL specifically across main perps and HIP-3."""
    print("\n" + "=" * 70)
    print("XPL CHECK ACROSS ALL SOURCES")
    print("=" * 70)
    
    async with httpx.AsyncClient(timeout=15) as client:
        # Check main perps
        resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs"})
        data = resp.json()
        
        meta = data[0]
        asset_ctxs = data[1]
        universe = meta.get("universe", [])
        
        xpl_found_main = False
        for i, coin in enumerate(universe):
            if coin["name"] == "XPL":
                ctx = asset_ctxs[i]
                funding = float(ctx.get("funding", 0)) * 8  # Hourly * 8
                print(f"\nXPL on MAIN perps:")
                print(f"  8h rate: {funding * 100:.4f}%")
                print(f"  APR: {funding * 1095 * 100:.1f}%")
                xpl_found_main = True
                break
        
        if not xpl_found_main:
            print("\nXPL NOT FOUND on main perps")
        
        # Check HIP-3
        for dex in ["hyna", "flx", "km"]:
            resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs", "dex": dex})
            data = resp.json()
            
            meta = data[0]
            asset_ctxs = data[1]
            universe = meta.get("universe", [])
            
            for i, coin in enumerate(universe):
                name = coin.get("name", "")
                base = name.split(":")[1] if ":" in name else name
                if base == "XPL":
                    ctx = asset_ctxs[i]
                    funding = float(ctx.get("funding", 0))  # Already 8h
                    print(f"\nXPL on HIP-3 ({dex}):")
                    print(f"  8h rate: {funding * 100:.4f}%")
                    print(f"  APR: {funding * 1095 * 100:.1f}%")
            
            await asyncio.sleep(0.1)


async def main():
    await fetch_hip3_high_funding()
    await check_xpl_across_venues()


if __name__ == "__main__":
    asyncio.run(main())
