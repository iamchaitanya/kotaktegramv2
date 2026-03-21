"""
Market Feed — Subscribes to Kotak Neo websocket for real-time tick data.
Provides live LTP to paper and real trading engines.
Stores every tick for backtesting.

PATCHES APPLIED:
 [1] Singleton _loop captured via get_running_loop() at start() time
 [2] _flush_tick_buffer uses stored _loop instead of deprecated get_event_loop()
 [3] REMOVED — our reconnect loop ripped out. SDK's run_forever(reconnect=5) handles reconnect.
 [4] stop() method for clean shutdown / pre-refresh teardown
 [5] REMOVED — no reconnect loop to backoff
 [6] REMOVED — no reconnect loop to lock
 [7] REMOVED — no reconnect loop to guard
 [8] Pending subs use a set-merge so reconnects never lose subscriptions
 [9] REMOVED — no reconnect loop waiting for open event
[10] Tick buffer flushed on disconnect so no ticks are lost
[11] Heartbeat watchdog — detects silent dead feed. On stale: force-closes WS so
     SDK's own reconnect=5 fires a fresh connection.
[12] SDK owns reconnect. _on_close is now OBSERVATION ONLY — no thread spawning.
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
        self._last_tick_time: float = 0.0             # [11]
        self._heartbeat_thread: threading.Thread | None = None  # [11]
        self._started_once = False

    @property
    def is_running(self) -> bool:
        return self._running

    def add_tick_callback(self, callback: Callable):
        self._tick_callbacks.append(callback)

    def add_raw_tick_callback(self, callback: Callable):
        self._raw_tick_callbacks.append(callback)

    # ── Lifecycle ──

    def start(self):
        """Set up websocket callbacks with Kotak Neo. Call ONCE after login.
        The SDK's run_forever(reconnect=5) handles all subsequent reconnects —
        do NOT call start() again on disconnect.
        """
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

        if self._started_once:
            log.warning("start() called more than once — ignoring. SDK handles reconnect internally.")
            return False
        self._started_once = True

        try:
            self.kotak.setup_callbacks(
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open,
            )
            self._running = True
            log.info("Market feed: callbacks registered, SDK will maintain connection")

            # [11] Start heartbeat watchdog once
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
        self._running = False
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
        """[12] WS closed — observation only. SDK's run_forever(reconnect=5) will reconnect.
        We flush the tick buffer and update state. No thread spawning here.
        """
        log.warning(f"WS closed: {message} — SDK will auto-reconnect in ~5s")
        self._running = False
        self._flush_tick_buffer()  # [10] Don't lose buffered ticks
        # SDK reconnects automatically — _on_open will fire when it's back up

    def _on_open(self, message):
        """WS opened (initial or SDK auto-reconnect) — update state and flush pending subs.

        CRITICAL: Do NOT call kotak.subscribe() here directly.
        The SDK's NeoWebSocket.on_hsm_message handles the 'cn' handshake and then
        calls subscribe_scripts() automatically for any items already in sub_list.
        Calling subscribe() here risks spawning a second WS thread if is_hsw_open
        is not yet 1 (handshake not complete), which overwrites the global ws and
        causes the death loop.

        Instead: merge known subscriptions into _pending_subs, then let
        _flush_pending_subs_when_ready() poll until is_hsw_open==1 before sending.
        """
        log.info(f"Market feed WS opened: {message}")
        self._running = True
        self._last_tick_time = time.time()  # [11] Reset watchdog on open

        # Merge all known subscriptions into pending so they get flushed once ready
        known_subs = [
            {"instrument_token": tk, "exchange_segment": info.get("exchange_segment", "bse_fo")}
            for tk, info in self._subscriptions.items()
        ]
        existing_keys = {(s["instrument_token"], s["exchange_segment"]) for s in self._pending_subs}
        for s in known_subs:
            if (s["instrument_token"], s["exchange_segment"]) not in existing_keys:
                self._pending_subs.append(s)

        if self._pending_subs:
            thread = threading.Thread(target=self._flush_pending_subs_when_ready, daemon=True)
            thread.start()

    def _flush_pending_subs_when_ready(self):
        """Poll until NeoWebSocket.is_hsw_open==1, then send pending subscriptions.
        This avoids calling subscribe() before the SDK handshake is complete,
        which would spawn a second WS thread and corrupt the global ws variable.
        """
        deadline = time.time() + 15  # wait up to 15s for handshake
        while time.time() < deadline:
            try:
                neo_ws = self.kotak.client.NeoWebSocket
                if neo_ws and neo_ws.is_hsw_open == 1:
                    break
            except Exception:
                pass
            time.sleep(0.1)
        else:
            log.warning("NeoWebSocket did not reach is_hsw_open=1 within 15s — skipping flush")
            return

        if not self._pending_subs or not self.kotak:
            return

        subs_to_flush = list(self._pending_subs)
        self._pending_subs.clear()
        log.info(f"Flushing {len(subs_to_flush)} subscriptions (is_hsw_open=1 confirmed)...")
        try:
            self.kotak.subscribe(instrument_tokens=subs_to_flush)
            log.info(f"✅ Flushed {len(subs_to_flush)} subscriptions")
        except Exception as e:
            log.error(f"Failed to flush subscriptions: {e}")
            self._pending_subs.extend(subs_to_flush)  # [8] Put back for next open

    # ── Heartbeat Watchdog ──

    def _heartbeat_watchdog(self):
        """[11] Periodically checks if ticks are still arriving.
        If feed goes silent for HEARTBEAT_STALE_THRESHOLD seconds during market hours,
        closes the WS so the SDK's own reconnect=5 kicks in fresh.
        We do NOT spawn our own reconnect — we just poke the SDK's.
        """
        log.info("Heartbeat watchdog running")
        while True:
            time.sleep(HEARTBEAT_INTERVAL)

            if not self._running:
                continue  # SDK will reconnect, _on_open will set _running=True again

            if self._last_tick_time == 0:
                continue  # No ticks received yet since startup

            # Only check during market hours (9:00–15:35 IST = 3:30–10:05 UTC)
            now_utc = datetime.now(timezone.utc)
            utc_hour = now_utc.hour + now_utc.minute / 60
            if not (3.5 <= utc_hour <= 10.1):
                continue

            elapsed = time.time() - self._last_tick_time
            if elapsed > HEARTBEAT_STALE_THRESHOLD:
                log.warning(
                    f"⚠️ No ticks for {elapsed:.0f}s — feed appears dead. "
                    f"Closing WS so SDK reconnect fires..."
                )
                self._running = False
                self._flush_tick_buffer()
                # Force-close the underlying websocket so SDK's reconnect=5 triggers
                try:
                    from neo_api_client.HSWebSocketLib import ws as sdk_ws
                    if sdk_ws:
                        sdk_ws.close()
                        log.info("Forced WS close — SDK will reconnect in ~5s")
                except Exception as e:
                    log.warning(f"Could not force-close SDK WS: {e}")
                # Reset so watchdog doesn't fire again immediately
                self._last_tick_time = time.time()

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