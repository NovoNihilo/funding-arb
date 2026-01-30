#!/usr/bin/env python3
"""
Debug script to test Hyperliquid HIP-3 API responses.
"""
import asyncio
import httpx

BASE_URL = "https://api.hyperliquid.xyz/info"

async def fetch_main_perps():
    """Fetch main perps and show SOL data."""
    print("=" * 60)
    print("MAIN HYPERLIQUID PERPS")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs"})
        data = resp.json()
        
        meta = data[0]
        asset_ctxs = data[1]
        universe = meta.get("universe", [])
        
        for i, coin in enumerate(universe):
            if coin["name"] == "SOL":
                ctx = asset_ctxs[i]
                funding = ctx.get("funding")
                # Main perps: funding is HOURLY, multiply by 8 for 8h rate
                funding_8h = float(funding) * 8 if funding else None
                apr = funding_8h * 1095 if funding_8h else None  # 1095 = 3 * 365
                print(f"\nSOL (main perps):")
                print(f"  Raw funding: {funding}")
                print(f"  8h rate: {funding_8h * 100:.4f}%")
                print(f"  APR: {apr * 100:.2f}%")
                print(f"  Mark: ${ctx.get('markPx')}")
                break


async def fetch_hip3_sol():
    """Fetch SOL from hyna and compare rates."""
    print("\n" + "=" * 60)
    print("HIP-3 DEX: hyna (SOL comparison)")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs", "dex": "hyna"})
        data = resp.json()
        
        meta = data[0]
        asset_ctxs = data[1]
        universe = meta.get("universe", [])
        
        for i, coin in enumerate(universe):
            name = coin.get("name", "")
            # Names come as "hyna:SOL" or just "SOL"
            base_name = name.split(":")[-1] if ":" in name else name
            
            if base_name == "SOL":
                ctx = asset_ctxs[i]
                funding = ctx.get("funding")
                
                # Test both interpretations
                print(f"\nSOL on hyna (raw name: '{name}'):")
                print(f"  Raw funding value: {funding}")
                print()
                
                # Interpretation 1: Funding is HOURLY (like main perps)
                rate_hourly = float(funding) if funding else 0
                rate_8h_v1 = rate_hourly * 8
                apr_v1 = rate_8h_v1 * 1095
                print(f"  IF hourly (multiply by 8):")
                print(f"    8h rate: {rate_8h_v1 * 100:.4f}%")
                print(f"    APR: {apr_v1 * 100:.2f}%")
                print()
                
                # Interpretation 2: Funding is already 8h rate
                rate_8h_v2 = float(funding) if funding else 0
                apr_v2 = rate_8h_v2 * 1095
                print(f"  IF already 8h rate (no multiply):")
                print(f"    8h rate: {rate_8h_v2 * 100:.4f}%")
                print(f"    APR: {apr_v2 * 100:.2f}%")
                print()
                
                print(f"  Frontend shows: ~0.0024% hourly, 20.63% APR")
                print(f"  --> This matches 'already 8h rate' interpretation!")
                break


async def fetch_all_hip3_assets():
    """Fetch all HIP-3 assets to see naming pattern."""
    print("\n" + "=" * 60)
    print("ALL HIP-3 ASSETS (checking name format)")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=15) as client:
        for dex in ["hyna", "flx", "km"]:
            resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs", "dex": dex})
            data = resp.json()
            
            meta = data[0]
            universe = meta.get("universe", [])
            
            print(f"\n{dex} asset names (first 5):")
            for coin in universe[:5]:
                print(f"  '{coin.get('name')}'")
            
            await asyncio.sleep(0.1)


async def main():
    print("\n🔍 HYPERLIQUID FUNDING RATE DEBUG\n")
    
    await fetch_main_perps()
    await fetch_hip3_sol()
    await fetch_all_hip3_assets()
    
    print("\n" + "=" * 60)
    print("CONCLUSION")
    print("=" * 60)
    print("""
Based on the data:
1. Main perps: funding is HOURLY -> multiply by 8 for 8h rate
2. HIP-3 dexs: funding is ALREADY 8h rate -> NO multiplication needed
3. Asset names have 'dex:' prefix like 'hyna:SOL'
    """)


if __name__ == "__main__":
    asyncio.run(main())
