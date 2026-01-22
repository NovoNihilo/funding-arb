import httpx


async def send_message(
    bot_token: str,
    channel_id: str,
    text: str,
    parse_mode: str = "HTML",
) -> bool:
    if not bot_token or not channel_id:
        print("[telegram] Missing bot_token or channel_id")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": channel_id,
        "text": text,
        "parse_mode": parse_mode,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            data = resp.json()

            if resp.status_code == 200 and data.get("ok"):
                return True
            else:
                print(f"[telegram] API error: {data}")
                return False

    except Exception as e:
        print(f"[telegram] Send error: {e}")
        return False
