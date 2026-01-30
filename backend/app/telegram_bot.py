"""
Telegram Bot with Inline Keyboards and Webhook support.

Handles:
- Sending leaderboard messages with interactive buttons
- Processing button callbacks (expand details, pagination, etc.)
- Editing messages in place for smooth UX
"""
import json
import httpx
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

from app.arb_engine import (
    LeaderboardEntry,
    venue_abbrev,
    estimate_apr,
    format_rate_compact,
    format_apr_compact,
    format_duration,
    get_trend_emoji,
)


@dataclass
class CallbackData:
    """Parse and create callback data for inline buttons."""
    action: str  # "view_established", "view_emerging", "view_symbol", "back", "refresh", "page"
    params: dict  # Additional parameters like symbol, page number, etc.
    
    def encode(self) -> str:
        """Encode to callback_data string (max 64 bytes)."""
        # Use short keys to save space
        data = {"a": self.action}
        if self.params:
            data["p"] = self.params
        return json.dumps(data, separators=(',', ':'))
    
    @classmethod
    def decode(cls, data: str) -> "CallbackData":
        """Decode from callback_data string."""
        parsed = json.loads(data)
        return cls(
            action=parsed.get("a", ""),
            params=parsed.get("p", {}),
        )


class TelegramBot:
    """Telegram Bot API wrapper with inline keyboard support."""
    
    BASE_URL = "https://api.telegram.org/bot{token}"
    
    def __init__(self, token: str):
        self.token = token
        self.api_url = self.BASE_URL.format(token=token)
    
    async def send_message(
        self,
        chat_id: str,
        text: str,
        reply_markup: dict = None,
        parse_mode: str = "HTML",
    ) -> Optional[dict]:
        """Send a message with optional inline keyboard."""
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
            }
            if reply_markup:
                payload["reply_markup"] = json.dumps(reply_markup)
            
            resp = await client.post(f"{self.api_url}/sendMessage", data=payload)
            data = resp.json()
            
            if data.get("ok"):
                return data.get("result")
            else:
                print(f"[telegram] Send error: {data}", flush=True)
                return None
    
    async def edit_message(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: dict = None,
        parse_mode: str = "HTML",
    ) -> bool:
        """Edit an existing message."""
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
            }
            if reply_markup:
                payload["reply_markup"] = json.dumps(reply_markup)
            
            resp = await client.post(f"{self.api_url}/editMessageText", data=payload)
            data = resp.json()
            
            if not data.get("ok"):
                # Ignore "message not modified" errors
                if "message is not modified" not in str(data.get("description", "")):
                    print(f"[telegram] Edit error: {data}", flush=True)
                    return False
            return True
    
    async def answer_callback(
        self,
        callback_query_id: str,
        text: str = None,
        show_alert: bool = False,
    ) -> bool:
        """Answer a callback query (acknowledge button press)."""
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {"callback_query_id": callback_query_id}
            if text:
                payload["text"] = text
                payload["show_alert"] = show_alert
            
            resp = await client.post(f"{self.api_url}/answerCallbackQuery", data=payload)
            return resp.json().get("ok", False)
    
    async def set_webhook(self, url: str) -> bool:
        """Set webhook URL for receiving updates."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self.api_url}/setWebhook",
                data={"url": url, "allowed_updates": json.dumps(["callback_query"])}
            )
            data = resp.json()
            print(f"[telegram] Set webhook: {data}", flush=True)
            return data.get("ok", False)
    
    async def delete_webhook(self) -> bool:
        """Delete webhook (for switching to polling or cleanup)."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(f"{self.api_url}/deleteWebhook")
            return resp.json().get("ok", False)


# ============ Message Formatting ============

def build_main_leaderboard_message(
    established: dict[str, list[LeaderboardEntry]],
    emerging: dict[str, list[LeaderboardEntry]],
) -> tuple[str, dict]:
    """
    Build the main leaderboard summary with navigation buttons.
    Returns (message_text, reply_markup)
    """
    est_opps = sum(len(v) for v in established.values())
    est_symbols = len(established)
    emg_opps = sum(len(v) for v in emerging.values())
    emg_symbols = len(emerging)
    
    # Find top opportunities for preview
    top_est = _get_top_opportunities(established, 3)
    top_emg = _get_top_opportunities(emerging, 3)
    
    lines = [
        "📊 <b>FUNDING ARB LEADERBOARD</b>",
        f"�� {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
        "",
    ]
    
    # Established preview
    lines.append(f"━━━ 🏆 <b>ESTABLISHED</b> ━━━")
    lines.append(f"<i>{est_opps} opportunities across {est_symbols} symbols</i>")
    if top_est:
        lines.append("")
        for symbol, entry in top_est:
            apr = estimate_apr(entry.opp.net_spread)
            lines.append(f"  <b>{symbol}</b>: {format_apr_compact(apr)} APR")
    lines.append("")
    
    # Emerging preview
    lines.append(f"━━━ ⚡ <b>EMERGING</b> ━━━")
    lines.append(f"<i>{emg_opps} opportunities across {emg_symbols} symbols</i>")
    if top_emg:
        lines.append("")
        for symbol, entry in top_emg:
            apr = estimate_apr(entry.opp.net_spread)
            lines.append(f"  <b>{symbol}</b>: {format_apr_compact(apr)} APR")
    
    text = "\n".join(lines)
    
    # Build inline keyboard
    keyboard = {
        "inline_keyboard": [
            [
                {"text": f"🏆 Established ({est_symbols})", "callback_data": CallbackData("est", {}).encode()},
                {"text": f"⚡ Emerging ({emg_symbols})", "callback_data": CallbackData("emg", {}).encode()},
            ],
            [
                {"text": "🔄 Refresh", "callback_data": CallbackData("refresh", {}).encode()},
            ]
        ]
    }
    
    return text, keyboard


def build_category_message(
    category: str,  # "est" or "emg"
    entries_by_symbol: dict[str, list[LeaderboardEntry]],
    page: int = 0,
    per_page: int = 10,
) -> tuple[str, dict]:
    """
    Build the category view (Established or Emerging) with symbol buttons.
    """
    title = "🏆 ESTABLISHED" if category == "est" else "⚡ EMERGING"
    subtitle = "≥48h active, ≥0.01% spread" if category == "est" else "<48h active, ≥0.02% spread"
    
    # Sort symbols by best APR
    sorted_symbols = sorted(
        entries_by_symbol.keys(),
        key=lambda s: max(estimate_apr(e.opp.net_spread) for e in entries_by_symbol[s]),
        reverse=True
    )
    
    total_symbols = len(sorted_symbols)
    total_pages = (total_symbols + per_page - 1) // per_page
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * per_page
    end_idx = start_idx + per_page
    page_symbols = sorted_symbols[start_idx:end_idx]
    
    lines = [
        f"<b>{title}</b>",
        f"<i>{subtitle}</i>",
        "",
        f"Page {page + 1}/{total_pages} ({total_symbols} symbols)",
        "",
    ]
    
    for symbol in page_symbols:
        entries = entries_by_symbol[symbol]
        best = max(entries, key=lambda e: e.opp.net_spread)
        apr = estimate_apr(best.opp.net_spread)
        route_count = len(entries)
        
        lines.append(
            f"<b>{symbol}</b>: {format_apr_compact(apr)} APR "
            f"({route_count} route{'s' if route_count > 1 else ''})"
        )
    
    text = "\n".join(lines)
    
    # Build symbol buttons (2 per row)
    symbol_buttons = []
    row = []
    for symbol in page_symbols:
        row.append({
            "text": symbol,
            "callback_data": CallbackData("sym", {"s": symbol, "c": category}).encode()
        })
        if len(row) == 3:
            symbol_buttons.append(row)
            row = []
    if row:
        symbol_buttons.append(row)
    
    # Navigation row
    nav_row = []
    if page > 0:
        nav_row.append({"text": "◀️ Prev", "callback_data": CallbackData("page", {"c": category, "p": page - 1}).encode()})
    nav_row.append({"text": "🏠 Home", "callback_data": CallbackData("home", {}).encode()})
    if page < total_pages - 1:
        nav_row.append({"text": "Next ▶️", "callback_data": CallbackData("page", {"c": category, "p": page + 1}).encode()})
    
    keyboard = {"inline_keyboard": symbol_buttons + [nav_row]}
    
    return text, keyboard


def build_symbol_detail_message(
    symbol: str,
    entries: list[LeaderboardEntry],
    category: str,
    max_routes: int = 8,
) -> tuple[str, dict]:
    """
    Build detailed view for a single symbol showing all routes.
    """
    sorted_entries = sorted(entries, key=lambda e: e.opp.net_spread, reverse=True)[:max_routes]
    
    lines = [
        f"🪙 <b>{symbol}</b> - Arbitrage Routes",
        "",
    ]
    
    for i, entry in enumerate(sorted_entries, 1):
        opp = entry.opp
        apr = estimate_apr(opp.net_spread)
        trend = get_trend_emoji(entry.trend)
        
        lines.append(f"<b>#{i}</b> {venue_abbrev(opp.short_venue)} → {venue_abbrev(opp.long_venue)}")
        lines.append(f"    💰 {format_rate_compact(opp.net_spread)} ({format_apr_compact(apr)} APR)")
        lines.append(f"    ⏱️ {format_duration(entry.duration_hours)} {trend}")
        
        if entry.price_spread_pct is not None:
            bps = entry.price_spread_pct * 10000
            lines.append(f"    💱 Price spread: {bps:.1f} bps")
        
        if entry.data_points >= 3:
            apr_avg = estimate_apr(entry.avg_spread)
            lines.append(f"    📊 Avg: {format_apr_compact(apr_avg)}")
        
        lines.append("")
    
    if len(entries) > max_routes:
        lines.append(f"<i>...and {len(entries) - max_routes} more routes</i>")
    
    text = "\n".join(lines)
    
    # Back button
    keyboard = {
        "inline_keyboard": [
            [{"text": f"◀️ Back to {'Established' if category == 'est' else 'Emerging'}", 
              "callback_data": CallbackData(category, {}).encode()}],
            [{"text": "🏠 Home", "callback_data": CallbackData("home", {}).encode()}],
        ]
    }
    
    return text, keyboard


def _get_top_opportunities(
    entries_by_symbol: dict[str, list[LeaderboardEntry]],
    n: int = 3,
) -> list[tuple[str, LeaderboardEntry]]:
    """Get top N opportunities by APR."""
    best_per_symbol = []
    for symbol, entries in entries_by_symbol.items():
        best = max(entries, key=lambda e: e.opp.net_spread)
        best_per_symbol.append((symbol, best))
    
    best_per_symbol.sort(key=lambda x: estimate_apr(x[1].opp.net_spread), reverse=True)
    return best_per_symbol[:n]


# ============ Callback Handler ============

class LeaderboardState:
    """
    Stores the current leaderboard data for handling callbacks.
    Updated each time we send a new leaderboard.
    """
    def __init__(self):
        self.established: dict[str, list[LeaderboardEntry]] = {}
        self.emerging: dict[str, list[LeaderboardEntry]] = {}
        self.last_updated: datetime = None
        self.message_id: int = None
        self.chat_id: str = None
    
    def update(
        self,
        established: dict[str, list[LeaderboardEntry]],
        emerging: dict[str, list[LeaderboardEntry]],
        message_id: int = None,
        chat_id: str = None,
    ):
        self.established = established
        self.emerging = emerging
        self.last_updated = datetime.utcnow()
        if message_id:
            self.message_id = message_id
        if chat_id:
            self.chat_id = chat_id


# Global state
_leaderboard_state = LeaderboardState()

def get_leaderboard_state() -> LeaderboardState:
    return _leaderboard_state


async def handle_callback(
    bot: TelegramBot,
    callback_query: dict,
) -> bool:
    """
    Handle a callback query from an inline button press.
    Returns True if handled successfully.
    """
    callback_id = callback_query.get("id")
    data_str = callback_query.get("data", "{}")
    message = callback_query.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    
    try:
        cb = CallbackData.decode(data_str)
    except Exception as e:
        print(f"[callback] Failed to decode: {data_str} - {e}", flush=True)
        await bot.answer_callback(callback_id, "Error processing request")
        return False
    
    state = get_leaderboard_state()
    
    # Handle different actions
    if cb.action == "home":
        text, keyboard = build_main_leaderboard_message(state.established, state.emerging)
        await bot.edit_message(chat_id, message_id, text, keyboard)
        await bot.answer_callback(callback_id)
    
    elif cb.action == "est":
        if state.established:
            text, keyboard = build_category_message("est", state.established)
            await bot.edit_message(chat_id, message_id, text, keyboard)
        else:
            await bot.answer_callback(callback_id, "No established opportunities yet", show_alert=True)
            return True
        await bot.answer_callback(callback_id)
    
    elif cb.action == "emg":
        if state.emerging:
            text, keyboard = build_category_message("emg", state.emerging)
            await bot.edit_message(chat_id, message_id, text, keyboard)
        else:
            await bot.answer_callback(callback_id, "No emerging opportunities yet", show_alert=True)
            return True
        await bot.answer_callback(callback_id)
    
    elif cb.action == "sym":
        symbol = cb.params.get("s")
        category = cb.params.get("c", "est")
        entries_map = state.established if category == "est" else state.emerging
        
        if symbol and symbol in entries_map:
            text, keyboard = build_symbol_detail_message(symbol, entries_map[symbol], category)
            await bot.edit_message(chat_id, message_id, text, keyboard)
        else:
            await bot.answer_callback(callback_id, f"Symbol {symbol} not found", show_alert=True)
            return True
        await bot.answer_callback(callback_id)
    
    elif cb.action == "page":
        category = cb.params.get("c", "est")
        page = cb.params.get("p", 0)
        entries_map = state.established if category == "est" else state.emerging
        
        text, keyboard = build_category_message(category, entries_map, page)
        await bot.edit_message(chat_id, message_id, text, keyboard)
        await bot.answer_callback(callback_id)
    
    elif cb.action == "refresh":
        await bot.answer_callback(callback_id, "Leaderboard updates every 30 minutes")
    
    else:
        await bot.answer_callback(callback_id, "Unknown action")
    
    return True
