#!/usr/bin/env python3
"""Quick test to verify XPL funding rates are correct."""
import asyncio
import httpx

BASE_URL = "https://api.hyperliquid.xyz/info"

async def main():
    print("XPL FUNDING RATE VERIFICATION")
    print("=" * 50)
    
    async with httpx.AsyncClient(timeout=15) as client:
        # Main perps
        resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs"})
        data = resp.json()
        universe = data[0].get("universe", [])
        asset_ctxs = data[1]
        
        for i, coin in enumerate(universe):
            if coin["name"] == "XPL":
                funding = float(asset_ctxs[i].get("funding", 0))
                funding_8h = funding * 8
                apr = funding_8h * 1095 * 100
                print(f"\nXPL MAIN:")
                print(f"  Raw (hourly): {funding * 100:.4f}%")
                print(f"  8h rate: {funding_8h * 100:.4f}%")
                print(f"  APR: {apr:.1f}%")
                break
        
        # HIP-3 hyna
        resp = await client.post(BASE_URL, json={"type": "metaAndAssetCtxs", "dex": "hyna"})
        data = resp.json()
        universe = data[0].get("universe", [])
        asset_ctxs = data[1]
        
        for i, coin in enumerate(universe):
            name = coin.get("name", "")
            base = name.split(":")[1] if ":" in name else name
            if base == "XPL":
                funding = float(asset_ctxs[i].get("funding", 0))
                funding_8h = funding * 8
                apr = funding_8h * 1095 * 100
                print(f"\nXPL HIP-3 (hyna):")
                print(f"  Raw (hourly): {funding * 100:.4f}%")
                print(f"  8h rate: {funding_8h * 100:.4f}%")
                print(f"  APR: {apr:.1f}%")
                break
    
    print("\n" + "=" * 50)
    print("Frontend shows XPL-USDE: 0.0095% hourly, 83.47% APR")
    print("Our 8h rate should be ~0.076%, APR ~83%")

if __name__ == "__main__":
    asyncio.run(main())
