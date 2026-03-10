"""
Market Feed — Subscribes to Kotak Neo websocket for real-time tick data.
Provides live LTP to paper and real trading engines.
Stores every tick for backtesting.
"""
import asyncio
import logging
from typing import Callable
from datetime import datetime, timezone

from . import database as db

log = logging.getLogger(__name__)

TICK_BUFFER_SIZE = 50  # Flush to DB every N ticks


class MarketFeed:
    """Manages Kotak Neo websocket subscriptions for live market data.

    Design principle: The Kotak Neo SDK (neo_api_client) internally manages
    its own WebSocket thread lifecycle. We do NOT attempt to reconnect,
    re-create sockets, or call subscribe() from within callbacks.
    We only: (1) set up callbacks once, (2) subscribe to instruments,
    (3) process ticks when they arrive.
    """

    def __init__(self, kotak_trader=None):
        self.kotak = kotak_trader
        self._subscriptions: dict[str, dict] = {}   # token -> {symbol, ltp, ...}
        self._tick_callbacks: list[Callable] = []
        self._running = False
        self._tick_buffer: list[dict] = []
        self._loop = None
        self._pending_subs: list[dict] = []  # Queued until WS is open

    @property
    def is_running(self) -> bool:
        return self._running

    def add_tick_callback(self, callback: Callable):
        """Register a callback for tick updates.
        Signature: async callback(token: str, ltp: float, data: dict)
        """
        self._tick_callbacks.append(callback)

    # ── Lifecycle ──

    def start(self):
        """Set up websocket callbacks with Kotak Neo. Call once after login."""
        try:
            self._loop = asyncio.get_event_loop()
        except RuntimeError:
            pass

        if not self.kotak or not self.kotak.is_authenticated:
            log.warning("Cannot start market feed — Kotak not authenticated")
            return False

        try:
            self.kotak.setup_callbacks(
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open,
            )
            self._running = True
            log.info("Market feed: callbacks registered")
            return True
        except Exception as e:
            log.error(f"Failed to start market feed: {e}")
            return False

    # ── Subscription ──

    def subscribe_instrument(self, token: str, symbol: str, exchange_segment: str = "bse_fo"):
        """Subscribe to a specific instrument for live data."""
        token_str = str(token)

        # Track locally even if already subscribed (idempotent)
        if token_str not in self._subscriptions:
            self._subscriptions[token_str] = {
                "symbol": symbol,
                "ltp": 0,
                "last_update": None,
                "exchange_segment": exchange_segment,
            }

        if self.kotak and self.kotak.is_authenticated:
            sub_item = {"instrument_token": token_str, "exchange_segment": exchange_segment}
            if self._running:
                try:
                    self.kotak.subscribe(instrument_tokens=[sub_item])
                    log.info(f"Subscribed to {symbol} ({token_str}) on {exchange_segment}")
                except Exception as e:
                    log.error(f"Failed to subscribe to {symbol}: {e}")
            else:
                self._pending_subs.append(sub_item)
                log.info(f"Queued subscription for {symbol} ({token_str}) — WS not yet open")

    def subscribe_index(self, token: str, symbol: str):
        """Subscribe to a BSE index (e.g. SENSEX) on bse_cm segment."""
        self.subscribe_instrument(token, symbol, exchange_segment="bse_cm")

    def subscribe_batch(self, tokens: list[dict]):
        """Subscribe to multiple instruments in a single SDK call.
        Each item: {"instrument_token": str, "exchange_segment": str, "symbol": str}
        """
        if not self.kotak or not self.kotak.is_authenticated:
            return

        # Track locally
        for item in tokens:
            tk = str(item["instrument_token"])
            self._subscriptions[tk] = {
                "symbol": item.get("symbol", ""),
                "ltp": 0,
                "last_update": None,
                "exchange_segment": item["exchange_segment"],
            }

        # Single SDK call for items where WS is open
        sub_list = [{"instrument_token": str(t["instrument_token"]), "exchange_segment": t["exchange_segment"]} for t in tokens]
        if self._running:
            try:
                self.kotak.subscribe(instrument_tokens=sub_list)
                log.info(f"Batch-subscribed to {len(sub_list)} instruments")
            except Exception as e:
                log.error(f"Batch subscribe failed: {e}")
        else:
            self._pending_subs.extend(sub_list)
            log.info(f"Queued {len(sub_list)} subscriptions — WS not yet open")

    def unsubscribe_instrument(self, token: str):
        """Unsubscribe from an instrument."""
        token_str = str(token)
        if token_str in self._subscriptions:
            seg = self._subscriptions[token_str].get("exchange_segment", "bse_fo")
            del self._subscriptions[token_str]
            if self.kotak:
                try:
                    self.kotak.unsubscribe([{"instrument_token": token_str, "exchange_segment": seg}])
                except Exception as e:
                    log.error(f"Unsubscribe failed: {e}")

    # ── Data Access ──

    def get_ltp(self, token: str) -> float:
        """Get last traded price for an instrument."""
        return self._subscriptions.get(str(token), {}).get("ltp", 0)

    def get_all_ticks(self) -> dict:
        """Return all current subscriptions and their latest data."""
        return dict(self._subscriptions)

    # ── Kotak SDK Callbacks (called from SDK's own background thread) ──

    def _on_message(self, message):
        """Handle websocket tick messages from Kotak Neo."""
        try:
            if isinstance(message, list):
                for tick in message:
                    self._process_tick(tick)
            elif isinstance(message, dict):
                if "data" in message and isinstance(message["data"], list):
                    for tick in message["data"]:
                        self._process_tick(tick)
                else:
                    self._process_tick(message)
        except Exception as e:
            log.error(f"Error processing tick: {e}")

    def _on_error(self, error):
        log.error(f"Market feed WS error: {error}")

    def _on_close(self, message):
        log.warning(f"Market feed WS closed: {message}")
        self._running = False

    def _on_open(self, message):
        log.info(f"Market feed WS opened: {message}")
        self._running = True
        # Flush any subscriptions that were queued before WS was open
        if self._pending_subs and self.kotak:
            log.info(f"Flushing {len(self._pending_subs)} queued subscriptions...")
            try:
                self.kotak.subscribe(instrument_tokens=self._pending_subs)
                log.info(f"Flushed {len(self._pending_subs)} queued subscriptions")
            except Exception as e:
                log.error(f"Failed to flush queued subscriptions: {e}")
            self._pending_subs.clear()

    # ── Tick Processing ──

    def _process_tick(self, tick: dict):
        """Process a single tick and fire callbacks."""
        token = str(tick.get("tk") or tick.get("instrument_token", ""))
        ltp_val = tick.get("ltp", tick.get("last_traded_price"))

        if ltp_val is not None:
            ltp = float(ltp_val)
        else:
            ltp = 0

        if not token or token not in self._subscriptions:
            return

        # Update local state (skip zero-LTP ticks)
        if ltp > 0:
            self._subscriptions[token]["ltp"] = ltp
        self._subscriptions[token]["last_update"] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

        # Buffer for DB storage
        self._tick_buffer.append({
            "instrument_token": token,
            "symbol": self._subscriptions[token].get("symbol", ""),
            "ltp": ltp,
            "volume": tick.get("v", tick.get("volume", 0)),
            "open": tick.get("o", tick.get("open", 0)),
            "high": tick.get("h", tick.get("high", 0)),
            "low": tick.get("l", tick.get("low", 0)),
            "close": tick.get("c", tick.get("close", 0)),
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        })
        if len(self._tick_buffer) >= TICK_BUFFER_SIZE:
            self._flush_tick_buffer()

        # Inject symbol into tick data for downstream
        tick["symbol"] = self._subscriptions[token].get("symbol", "")

        # Fire callbacks only for valid LTP
        if ltp > 0:
            for cb in self._tick_callbacks:
                if self._loop:
                    asyncio.run_coroutine_threadsafe(cb(token, ltp, tick), self._loop)
                else:
                    try:
                        asyncio.get_event_loop().create_task(cb(token, ltp, tick))
                    except RuntimeError:
                        pass

    def _flush_tick_buffer(self):
        """Flush buffered ticks to database."""
        if not self._tick_buffer:
            return
        ticks_to_save = list(self._tick_buffer)
        self._tick_buffer.clear()
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(db.save_ticks_batch(ticks_to_save))
        except RuntimeError:
            pass
