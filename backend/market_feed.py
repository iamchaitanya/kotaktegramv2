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
 [6] Reconnect lock prevents multiple concurrent reconnect threads
 [7] start() no longer resets _should_reconnect — stop() is always respected
 [8] Pending subs use a set-merge so reconnects never lose subscriptions
 [9] WS open confirmation timeout — if _on_open never fires, retry
[10] Tick buffer flushed on disconnect so no ticks are lost
[11] Heartbeat watchdog — detects silent dead feed and forces reconnect
"""
import asyncio
import logging
import threading
import time
from typing import Callable
from datetime import datetime, timezone

from . import database as db

log = logging.getLogger(__name__)

TICK_BUFFER_SIZE = 50        # Flush to DB every N ticks
RECONNECT_BASE_DELAY = 5     # Initial reconnect wait (seconds)
RECONNECT_MAX_DELAY = 60     # Maximum reconnect wait (seconds)
WS_OPEN_TIMEOUT = 30         # [9]  Seconds to wait for _on_open before retrying
HEARTBEAT_INTERVAL = 30      # [11] Seconds between watchdog checks
HEARTBEAT_STALE_THRESHOLD = 120  # [11] Seconds without a tick = dead feed


class MarketFeed:
    """Manages Kotak Neo websocket subscriptions for live market data."""

    def __init__(self, kotak_trader=None):
        self.kotak = kotak_trader
        self._subscriptions: dict[str, dict] = {}
        self._tick_callbacks: list[Callable] = []
        self._raw_tick_callbacks: list[Callable] = []
        self._running = False
        self._tick_buffer: list[dict] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending_subs: list[dict] = []
        self._reconnect_attempts = 0
        self._should_reconnect = True
        self._reconnect_lock = threading.Lock()       # [6]
        self._ws_open_event = threading.Event()       # [9]
        self._last_tick_time: float = 0.0             # [11]
        self._heartbeat_thread: threading.Thread | None = None  # [11]
        self._started_once = False                    # tracks if start() was ever called

    @property
    def is_running(self) -> bool:
        return self._running

    def add_tick_callback(self, callback: Callable):
        self._tick_callbacks.append(callback)

    def add_raw_tick_callback(self, callback: Callable):
        self._raw_tick_callbacks.append(callback)

    # ── Lifecycle ──

    def start(self):
        """Set up websocket callbacks with Kotak Neo. Call once after login."""
        # [1] Capture the running loop
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

        # [7] Never override an intentional stop()
        # Only set _should_reconnect=True on the very first start
        if not self._started_once:
            self._should_reconnect = True
            self._started_once = True

        try:
            self._ws_open_event.clear()  # [9] Reset before each start attempt
            self.kotak.setup_callbacks(
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open,
            )
            self._running = True
            self._reconnect_attempts = 0
            log.info("Market feed: callbacks registered")

            # [11] Start heartbeat watchdog on first start
            if self._heartbeat_thread is None or not self._heartbeat_thread.is_alive():
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_watchdog, daemon=True
                )
                self._heartbeat_thread.start()
                log.info("Heartbeat watchdog started")

            return True
        except Exception as e:
            log.error(f"Failed to start market feed: {e}")
            return False

    def stop(self):
        """[4] Intentionally stop the market feed (no reconnect)."""
        self._should_reconnect = False  # [7] Must be set BEFORE _running=False
        self._running = False
        self._reconnect_attempts = 0
        self._flush_tick_buffer()       # [10] Flush remaining ticks on stop
        log.info("Market feed stopped intentionally")

    # ── Subscription ──

    def subscribe_instrument(self, token: str, symbol: str, exchange_segment: str = "bse_fo"):
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
                # [8] Avoid duplicate pending subs
                if sub_item not in self._pending_subs:
                    self._pending_subs.append(sub_item)
                log.info(f"Queued subscription for {symbol} ({token_str}) — WS not yet open")

    def subscribe_index(self, token: str, symbol: str):
        self.subscribe_instrument(token, symbol, exchange_segment="bse_cm")

    def subscribe_batch(self, tokens: list[dict]):
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
        sub_list = [
            {"instrument_token": str(t["instrument_token"]), "exchange_segment": t["exchange_segment"]}
            for t in tokens
        ]
        if self._running:
            try:
                self.kotak.subscribe(instrument_tokens=sub_list)
                log.info(f"Batch-subscribed to {len(sub_list)} instruments")
            except Exception as e:
                log.error(f"Batch subscribe failed: {e}")
        else:
            # [8] Merge without duplicates
            existing = {(s["instrument_token"], s["exchange_segment"]) for s in self._pending_subs}
            for s in sub_list:
                if (s["instrument_token"], s["exchange_segment"]) not in existing:
                    self._pending_subs.append(s)
            log.info(f"Queued {len(sub_list)} subscriptions — WS not yet open")

    def unsubscribe_instrument(self, token: str):
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
        return self._subscriptions.get(str(token), {}).get("ltp", 0)

    def get_all_ticks(self) -> dict:
        return dict(self._subscriptions)

    # ── Kotak SDK Callbacks ──

    def _on_message(self, message):
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
        """[3][6][10] WS closed — flush buffer, then reconnect if allowed."""
        log.warning(f"Market feed WS closed: {message}")
        self._running = False
        self._flush_tick_buffer()  # [10] Don't lose buffered ticks

        if not self._should_reconnect:
            log.info("Reconnect disabled — not attempting reconnect")
            return

        # [6] Only one reconnect thread at a time
        if self._reconnect_lock.locked():
            log.warning("Reconnect already in progress — ignoring duplicate close event")
            return

        thread = threading.Thread(target=self._reconnect_loop, daemon=True)
        thread.start()

    def _reconnect_loop(self):
        """[3][5][6][7][9] Reconnect with exponential backoff. Holds lock for duration."""
        with self._reconnect_lock:
            while self._should_reconnect and not self._running:
                self._reconnect_attempts += 1
                delay = min(
                    RECONNECT_BASE_DELAY * (2 ** (self._reconnect_attempts - 1)),
                    RECONNECT_MAX_DELAY,
                )
                log.info(f"Market feed reconnect attempt #{self._reconnect_attempts} in {delay}s...")
                time.sleep(delay)

                if not self._should_reconnect:
                    break

                try:
                    # [7] Re-login only if session truly dropped
                    if self.kotak:
                        login_result = self.kotak.login()
                        # kotak_trader.login() always returns {"status": "ok"} on success
                        if isinstance(login_result, dict) and login_result.get("status") == "ok":
                            log.info("Kotak re-login successful for reconnect")
                        else:
                            log.warning(f"Kotak re-login failed: {login_result}")
                            continue

                    if not self.start():
                        log.warning("Market feed start() failed — will retry")
                        continue

                    # [9] Wait for _on_open confirmation before declaring success
                    log.info(f"Waiting up to {WS_OPEN_TIMEOUT}s for WS open confirmation...")
                    opened = self._ws_open_event.wait(timeout=WS_OPEN_TIMEOUT)
                    if not opened:
                        log.warning(f"WS did not open within {WS_OPEN_TIMEOUT}s — retrying")
                        self._running = False
                        continue

                    # [8] Queue ALL known subscriptions for _on_open to flush
                    known_subs = [
                        {"instrument_token": tk, "exchange_segment": info.get("exchange_segment", "bse_fo")}
                        for tk, info in self._subscriptions.items()
                    ]
                    existing_keys = {(s["instrument_token"], s["exchange_segment"]) for s in self._pending_subs}
                    for s in known_subs:
                        if (s["instrument_token"], s["exchange_segment"]) not in existing_keys:
                            self._pending_subs.append(s)

                    log.info("✅ Market feed reconnected successfully!")
                    self._reconnect_attempts = 0
                    break

                except Exception as e:
                    log.error(f"Reconnect attempt #{self._reconnect_attempts} failed: {e}")

    def _on_open(self, message):
        """[9] WS opened — signal the reconnect loop and flush pending subs."""
        log.info(f"Market feed WS opened: {message}")
        self._running = True
        self._reconnect_attempts = 0
        self._last_tick_time = time.time()  # [11] Reset watchdog on open
        self._ws_open_event.set()           # [9] Signal reconnect loop

        if self._pending_subs and self.kotak:
            subs_to_flush = list(self._pending_subs)
            self._pending_subs.clear()
            log.info(f"Flushing {len(subs_to_flush)} queued subscriptions...")
            try:
                self.kotak.subscribe(instrument_tokens=subs_to_flush)
                log.info(f"Flushed {len(subs_to_flush)} queued subscriptions")
            except Exception as e:
                log.error(f"Failed to flush queued subscriptions: {e}")
                # [8] Put them back so next reconnect retries them
                self._pending_subs.extend(subs_to_flush)

    # ── Heartbeat Watchdog ──

    def _heartbeat_watchdog(self):
        """[11] Periodically checks if ticks are still arriving.
        If feed goes silent for HEARTBEAT_STALE_THRESHOLD seconds during
        market hours, forces a reconnect.
        """
        log.info("Heartbeat watchdog running")
        while True:
            time.sleep(HEARTBEAT_INTERVAL)

            if not self._should_reconnect:
                log.info("Heartbeat watchdog stopping — feed intentionally stopped")
                break

            if not self._running:
                continue  # Reconnect loop will handle it

            if self._last_tick_time == 0:
                continue  # No ticks received yet since startup

            # Only check during market hours (9:00 - 15:35 IST = 3:30 - 10:05 UTC)
            now_utc = datetime.now(timezone.utc)
            utc_hour = now_utc.hour + now_utc.minute / 60
            if not (3.5 <= utc_hour <= 10.1):
                continue  # Outside market hours — silence is expected

            elapsed = time.time() - self._last_tick_time
            if elapsed > HEARTBEAT_STALE_THRESHOLD:
                log.warning(
                    f"⚠️ No ticks received for {elapsed:.0f}s — feed appears dead. "
                    f"Forcing reconnect..."
                )
                self._running = False
                self._flush_tick_buffer()  # [10]

                if not self._reconnect_lock.locked():
                    thread = threading.Thread(target=self._reconnect_loop, daemon=True)
                    thread.start()
                else:
                    log.info("Reconnect already in progress — watchdog skipping")

    # ── Tick Processing ──

    def _process_tick(self, tick: dict):
        token = str(tick.get("tk") or tick.get("instrument_token", ""))
        ltp_val = tick.get("ltp", tick.get("last_traded_price"))

        ltp = float(ltp_val) if ltp_val is not None else 0

        if not token or token not in self._subscriptions:
            return

        if ltp > 0:
            self._subscriptions[token]["ltp"] = ltp
            self._last_tick_time = time.time()  # [11] Update watchdog timestamp

        self._subscriptions[token]["last_update"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )

        self._tick_buffer.append({
            "instrument_token": token,
            "symbol": self._subscriptions[token].get("symbol", ""),
            "ltp": ltp,
            "volume": tick.get("v", tick.get("volume", 0)),
            "open": tick.get("o", tick.get("open", 0)),
            "high": tick.get("h", tick.get("high", 0)),
            "low": tick.get("l", tick.get("low", 0)),
            "close": tick.get("c", tick.get("close", 0)),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
        if len(self._tick_buffer) >= TICK_BUFFER_SIZE:
            self._flush_tick_buffer()

        tick["symbol"] = self._subscriptions[token].get("symbol", "")

        if ltp > 0:
            for cb in self._raw_tick_callbacks:
                try:
                    cb(token, ltp, tick)
                except Exception as e:
                    log.error(f"Raw tick callback error: {e}")

        if ltp > 0:
            for cb in self._tick_callbacks:
                if self._loop and not self._loop.is_closed():
                    asyncio.run_coroutine_threadsafe(cb(token, ltp, tick), self._loop)

    def _flush_tick_buffer(self):
        """[2][10] Flush buffered ticks to database."""
        if not self._tick_buffer:
            return
        ticks_to_save = list(self._tick_buffer)
        self._tick_buffer.clear()
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(db.save_ticks_batch(ticks_to_save), self._loop)
        else:
            log.warning(f"Cannot flush {len(ticks_to_save)} ticks — event loop unavailable")