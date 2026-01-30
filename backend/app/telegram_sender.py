"""Telegram message sender with retry logic."""
import asyncio
import httpx


async def send_message(
    bot_token: str,
    channel_id: str,
    text: str,
    parse_mode: str = "HTML",
    max_retries: int = 3,
) -> bool:
    if not bot_token or not channel_id:
        print("[telegram] Missing bot_token or channel_id", flush=True)
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": channel_id, "text": text, "parse_mode": parse_mode}

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload)
                data = resp.json()

                if resp.status_code == 200 and data.get("ok"):
                    return True
                
                if resp.status_code == 429:
                    retry_after = data.get("parameters", {}).get("retry_after", 5)
                    print(f"[telegram] Rate limited, waiting {retry_after}s", flush=True)
                    await asyncio.sleep(retry_after)
                    continue
                
                print(f"[telegram] API error (attempt {attempt + 1}): {data}", flush=True)
                
                if 400 <= resp.status_code < 500 and resp.status_code != 429:
                    return False

        except httpx.TimeoutException:
            print(f"[telegram] Timeout (attempt {attempt + 1})", flush=True)
        except Exception as e:
            print(f"[telegram] Error (attempt {attempt + 1}): {e}", flush=True)
        
        if attempt < max_retries - 1:
            await asyncio.sleep(2 ** attempt)

    return False
