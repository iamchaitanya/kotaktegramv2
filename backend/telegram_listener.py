"""
Telegram Listener — connects as a user client via Telethon (API ID + Hash)
to listen for messages in channels the user is subscribed to.
"""
import asyncio
import logging
import os
from typing import Callable, Optional

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from .config import Config

log = logging.getLogger(__name__)

# Session file lives next to the DB
SESSION_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "telegram")


class TelegramListener:
    """Connects to Telegram as a user client, listens for channel messages."""

    def __init__(self):
        self.client: Optional[TelegramClient] = None
        self._on_signal_callback: Optional[Callable] = None
        self._running = False
        self._channel_id: Optional[int] = None

    def set_callback(self, callback: Callable):
        """Set the callback that fires when a new message arrives.
        Signature: async callback(text: str, sender: str, timestamp: str)
        """
        self._on_signal_callback = callback

    async def start(self):
        """Start listening to the configured Telegram channel."""
        api_id = Config.TELEGRAM_API_ID
        api_hash = Config.TELEGRAM_API_HASH

        if not api_id or not api_hash:
            log.warning("No TELEGRAM_API_ID / TELEGRAM_API_HASH set — listener disabled")
            return

        try:
            # Use StringSession if available (portable across machines),
            # otherwise fall back to file-based session
            session_string = Config.TELEGRAM_SESSION_STRING
            if session_string:
                log.info("Using StringSession from TELEGRAM_SESSION_STRING")
                session = StringSession(session_string)
            else:
                log.info("No TELEGRAM_SESSION_STRING set — using file-based session (run generate_session.py to create one)")
                os.makedirs(SESSION_PATH, exist_ok=True)
                session = os.path.join(SESSION_PATH, "anon")

            self.client = TelegramClient(session, int(api_id), api_hash)

            await self.client.start()

            # Verify connection
            me = await self.client.get_me()
            log.info(f"Telegram connected as: {me.first_name} (@{me.username})")

            # Resolve channel
            channel_id = Config.TELEGRAM_CHANNEL_ID
            if channel_id:
                try:
                    self._channel_id = int(channel_id)
                except ValueError:
                    # Could be a username like @channelname
                    self._channel_id = channel_id

            # Register message handler
            @self.client.on(events.NewMessage())
            async def handler(event):
                # ── DEBUG LOGGING ──
                try:
                    chat = await event.get_chat()
                    chat_id = getattr(chat, 'id', 'Unknown')
                    chat_title = getattr(chat, 'title', 'Private/Unknown')
                    log.info(f"📩 Raw Telegram Event: [Chat {chat_id} ({chat_title})] {event.message.text[:50]}...")
                except Exception as e:
                    log.debug(f"Error logging raw event: {e}")

                # ── FILTERING ──
                # If TELEGRAM_CHANNEL_ID is set, only process those.
                # If not set (or for temporary debugging), process all.
                if self._channel_id:
                    # Telethon expects comparison with the integer ID
                    if event.chat_id != self._channel_id:
                        return

                await self._handle_message(event)

            self._running = True
            log.info(
                f"Telegram listener active"
                + (f" — monitoring channel {self._channel_id}" if self._channel_id else " — monitoring ALL chats")
            )

            # Keep running until stopped
            # (Telethon runs in the background via its own event loop hooks)

        except Exception as e:
            log.error(f"Failed to start Telegram listener: {e}")
            self._running = False

    async def _handle_message(self, event):
        """Handle every incoming message."""
        msg = event.message
        if not msg or not msg.text:
            return

        # Get sender info
        sender = ""
        if msg.forward:
            # Forwarded message — get original channel/user name
            fwd = msg.forward
            if hasattr(fwd, "chat") and fwd.chat:
                sender = getattr(fwd.chat, "title", "") or getattr(fwd.chat, "username", "") or ""
            elif hasattr(fwd, "sender") and fwd.sender:
                sender = getattr(fwd.sender, "first_name", "") or getattr(fwd.sender, "username", "") or ""
        
        if not sender:
            try:
                chat = await event.get_chat()
                sender = getattr(chat, "title", "") or getattr(chat, "username", "") or ""
            except Exception:
                sender = "Unknown"

        text = msg.text
        timestamp = msg.date.isoformat() if msg.date else ""

        log.info(f"[Telegram] {sender}: {text[:80]}...")

        if self._on_signal_callback:
            try:
                await self._on_signal_callback(text, sender, timestamp)
            except Exception as e:
                log.error(f"Callback error: {e}")

    async def stop(self):
        """Stop the Telegram listener."""
        if self.client and self._running:
            try:
                await self.client.disconnect()
                self._running = False
                log.info("Telegram listener stopped")
            except Exception as e:
                log.error(f"Error stopping listener: {e}")

    @property
    def is_running(self) -> bool:
        return self._running
