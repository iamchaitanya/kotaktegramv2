"""
Market Feed — Subscribes to Kotak Neo websocket for real-time tick data.
Provides live LTP to paper and real trading engines.
Stores every tick for backtesting.

PATCHES APPLIED:
 [1] Singleton _loop captured via get_running_loop() at start() time
 [2] _flush_tick_buffer uses stored _loop instead of deprecated get_event_loop()
 [3] _on_close triggers automatic reconnect via background thread
 [4] stop() method for clean shutdown / pre-refresh teardown
 [5] Reconnect has exponential backoff with max 60s cap
"""
import asyncio
import logging
import threading
import time
from typing import Callable
from datetime import datetime, timezone

from . import database as db

log = logging.getLogger(__name__)

TICK_BUFFER_SIZE = 50  # Flush to DB every N ticks
RECONNECT_BASE_DELAY = 5    # Initial reconnect wait (seconds)
RECONNECT_MAX_DELAY = 60    # Maximum reconnect wait (seconds)


class MarketFeed:
    """Manages Kotak Neo websocket subscriptions for live market data.

    Design principle: The Kotak Neo SDK (neo_api_client) internally manages
    its own WebSocket thread lifecycle. We set up callbacks once, subscribe
    to instruments, and process ticks when they arrive. On disconnect,
    we attempt automatic reconnection with exponential backoff.
    """

    def __init__(self, kotak_trader=None):
        self.kotak = kotak_trader
        self._subscriptions: dict[str, dict] = {}   # token -> {symbol, ltp, ...}
        self._tick_callbacks: list[Callable] = []
        self._raw_tick_callbacks: list[Callable] = []
        self._running = False
        self._tick_buffer: list[dict] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending_subs: list[dict] = []
        self._reconnect_attempts = 0
        self._should_reconnect = True  # Set to False during intentional stop()

    @property
    def is_running(self) -> bool:
        return self._running

    def add_tick_callback(self, callback: Callable):
        """Register an async callback for tick updates.
        Signature: async callback(token: str, ltp: float, data: dict)
        """
        self._tick_callbacks.append(callback)

    def add_raw_tick_callback(self, callback: Callable):
        """Register a SYNC callback, called directly from the SDK background thread.
        Signature: callback(token: str, ltp: float, data: dict)
        """
        self._raw_tick_callbacks.append(callback)

    # ── Lifecycle ──

    def start(self):
        """Set up websocket callbacks with Kotak Neo. Call once after login."""
        # [1] Capture the running loop reliably — used for threadsafe scheduling
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
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
            self._should_reconnect = True
            self._reconnect_attempts = 0
            log.info("Market feed: callbacks registered")
            return True
        except Exception as e:
            log.error(f"Failed to start market feed: {e}")
            return False

    def stop(self):
        """[4] Intentionally stop the market feed (no reconnect)."""
        self._should_reconnect = False
        self._running = False
        self._reconnect_attempts = 0
        log.info("Market feed stopped intentionally")

    # ── Subscription ──

    def subscribe_instrument(self, token: str, symbol: str, exchange_segment: str = "bse_fo"):
        """Subscribe to a specific instrument for live data."""
        token_str = str(token)

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
        """Subscribe to multiple instruments in a single SDK call."""
        if not self.kotak or not self.kotak.is_authenticated:
            return

        for item in tokens:
            tk = str(item["instrument_token"])
            self._subscriptions[tk] = {
                "symbol": item.get("symbol", ""),
                "ltp": 0,
                "last_update": None,
                "exchange_segment": item["exchange_segment"],
            }

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

    def _resubscribe_all(self):
        """Re-subscribe to all tracked instruments after reconnect."""
        if not self.kotak or not self._subscriptions:
            return
        sub_list = [
            {"instrument_token": tk, "exchange_segment": info.get("exchange_segment", "bse_fo")}
            for tk, info in self._subscriptions.items()
        ]
        try:
            self.kotak.subscribe(instrument_tokens=sub_list)
            log.info(f"Re-subscribed to {len(sub_list)} instruments after reconnect")
        except Exception as e:
            log.error(f"Re-subscribe after reconnect failed: {e}")

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
        """[3] When WS closes, attempt automatic reconnect in a background thread."""
        log.warning(f"Market feed WS closed: {message}")
        self._running = False

        if not self._should_reconnect:
            log.info("Reconnect disabled — not attempting reconnect")
            return

        # Spawn a background thread to avoid blocking the SDK's thread
        thread = threading.Thread(target=self._reconnect_loop, daemon=True)
        thread.start()

    def _reconnect_loop(self):
        """[3][5] Background thread: re-login + restart + re-subscribe with exponential backoff."""
        while self._should_reconnect and not self._running:
            self._reconnect_attempts += 1
            # Exponential backoff: 5s, 10s, 20s, 40s, capped at 60s
            delay = min(
                RECONNECT_BASE_DELAY * (2 ** (self._reconnect_attempts - 1)),
                RECONNECT_MAX_DELAY,
            )
            log.info(
                f"Market feed reconnect attempt #{self._reconnect_attempts} "
                f"in {delay}s..."
            )
            time.sleep(delay)

            if not self._should_reconnect:
                break

            try:
                # Re-authenticate (Kotak sessions may expire on WS drop)
                if self.kotak:
                    login_result = self.kotak.login()
                    if isinstance(login_result, dict) and login_result.get("status") == "ok":
                        log.info("Kotak re-login successful for reconnect")
                    else:
                        log.warning(f"Kotak re-login failed: {login_result}")
                        continue

                # Re-register callbacks and restart
                if self.start():
                    log.info("✅ Market feed reconnected successfully!")
                    # Re-subscribe happens in _on_open when the WS actually opens
                    # But also queue all known subscriptions
                    self._pending_subs = [
                        {"instrument_token": tk, "exchange_segment": info.get("exchange_segment", "bse_fo")}
                        for tk, info in self._subscriptions.items()
                    ]
                    self._reconnect_attempts = 0
                    break
                else:
                    log.warning("Market feed start() failed — will retry")
            except Exception as e:
                log.error(f"Reconnect attempt #{self._reconnect_attempts} failed: {e}")

    def _on_open(self, message):
        log.info(f"Market feed WS opened: {message}")
        self._running = True
        self._reconnect_attempts = 0
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

        # Fire sync raw callbacks directly (thread-safe, no event loop needed)
        if ltp > 0:
            for cb in self._raw_tick_callbacks:
                try:
                    cb(token, ltp, tick)
                except Exception as e:
                    log.error(f"Raw tick callback error: {e}")

        # [1] Fire async callbacks via run_coroutine_threadsafe using stored _loop
        if ltp > 0:
            for cb in self._tick_callbacks:
                if self._loop and not self._loop.is_closed():
                    asyncio.run_coroutine_threadsafe(cb(token, ltp, tick), self._loop)

    def _flush_tick_buffer(self):
        """[2] Flush buffered ticks to database using stored _loop."""
        if not self._tick_buffer:
            return
        ticks_to_save = list(self._tick_buffer)
        self._tick_buffer.clear()
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(db.save_ticks_batch(ticks_to_save), self._loop)

