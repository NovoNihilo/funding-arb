import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from app.telegram_sender import send_message


@pytest.mark.asyncio
async def test_send_message_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"ok": True, "result": {"message_id": 123}}

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response

    with patch("httpx.AsyncClient") as mock_async_client:
        mock_async_client.return_value.__aenter__.return_value = mock_client
        result = await send_message(
            bot_token="test_token",
            channel_id="-100123",
            text="Test message",
        )
        assert result is True


@pytest.mark.asyncio
async def test_send_message_failure():
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {"ok": False, "description": "Bad Request"}

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response

    with patch("httpx.AsyncClient") as mock_async_client:
        mock_async_client.return_value.__aenter__.return_value = mock_client
        result = await send_message(
            bot_token="test_token",
            channel_id="-100123",
            text="Test message",
        )
        assert result is False


@pytest.mark.asyncio
async def test_send_message_missing_config():
    result = await send_message(bot_token="", channel_id="-100123", text="Test")
    assert result is False

    result = await send_message(bot_token="token", channel_id="", text="Test")
    assert result is False
