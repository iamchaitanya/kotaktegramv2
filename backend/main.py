"""
Main FastAPI application — REST API + WebSocket for the trading platform.

PATCHES APPLIED:
 [1] exit_position broadcasts position_update (status:'closed') instead of new_trade
 [2] Kotak login/init/download run in executor to avoid blocking the event loop
 [3] kill_switch init payload explicitly includes strategy via get_status()
 [4] Strategy persisted to JSON file so it survives server restarts
 [5] set_strategy broadcasts settings_update to all connected clients
 [6] clear_data clears in-memory paper trader state (positions + orders)
 [7] CORS allow_credentials removed when allow_origins='*' (invalid combo)
 [8] daily_contract_refresh stops market feed before restarting
 [9] WebSocket init payload explicitly passes strategy via get_status()
[10] kotak_auto_login typo fixed
[11] Strategy loaded from disk on startup so manager.strategy is never stale
[12] compareMode added to StrategyRequest; get_trades supports date filter
[FIX #1 ] exit_position: removed duplicate WS broadcast — close_position() is single source
[FIX #4 ] kotak_login and complete_2fa HTTP endpoints wrapped in run_in_executor
[FIX #5 ] enqueue_tick: QueueFull caught and logged; warning at 80% capacity
[FIX #6 ] daily_contract_refresh: cancels orphan tick drain task before restart, recreates after
[FIX #9 ] telegram_health_check: exponential backoff + circuit breaker after 3 failures
[FIX #18] WS broadcast: iterate copy, catch all exceptions per-connection, discard dead sockets
[FIX #23] log.exception() used throughout — no bare except or log.error for exceptions
[FIX #24] entryTimerMins, exitTimerMins, signalTrailInitialSL, signalTrailInitialSLPoints added to strategy
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta, time as dt_time
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import Config
from .trade_manager import TradeManager
from .telegram_listener import TelegramListener
from . import database as db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pid=%(process)d] [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ── Strategy persistence ──────────────────────────────────────────────────────
STRATEGY_FILE = os.path.join(os.path.dirname(__file__), "..", "strategy.json")

STRATEGY_DEFAULTS = {
    "lots":                      1,
    "entryLogic":                "code",
    "entryAvgPick":              "avg",
    "entryFixed":                None,
    "trailingSL":                "code",
    "slFixed":                   None,
    "activationPoints":          5.0,
    "trailGap":                  2.0,
    "compareMode":               False,
    "entryTimerMins":            10,
    "exitTimerMins":             10,
    "signalTrailInitialSL":      "telegram",
    "signalTrailInitialSLPoints": 5.0,
}


def load_strategy() -> dict:
    """Load strategy from disk, falling back to defaults."""
    try:
        if os.path.exists(STRATEGY_FILE):
            with open(STRATEGY_FILE) as f:
                saved = json.load(f)
                return {**STRATEGY_DEFAULTS, **saved}
    except Exception:
        log.exception("Could not load strategy from disk")  # [FIX #23]
    return dict(STRATEGY_DEFAULTS)


def save_strategy(strategy: dict):
    """Persist strategy to disk."""
    try:
        with open(STRATEGY_FILE, "w") as f:
            json.dump(strategy, f, indent=2)
    except Exception:
        log.exception("Could not save strategy to disk")  # [FIX #23]


# ── Global instances ──────────────────────────────────────────────────────────
manager  = TradeManager()
telegram = TelegramListener()

# [4][11] Load persisted strategy immediately on import
manager.strategy = load_strategy()

# Stop-trading flag — when True, incoming signals are NOT traded.
# Existing positions and pending orders continue to be managed normally.
_stop_trading: bool = False


# ── WebSocket Connection Manager ──────────────────────────────────────────────
def _json_safe(obj):
    """Recursively convert datetime objects to ISO strings for JSON serialisation."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat().replace("+00:00", "Z")
    return obj


class ConnectionManager:
    """Manages active WebSocket connections with per-socket send locks."""

    def __init__(self):
        # Each socket gets its own asyncio.Lock so concurrent sends don't interleave
        self._connections: dict[WebSocket, asyncio.Lock] = {}

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections[ws] = asyncio.Lock()
        log.info("WS client connected (%d total)", len(self._connections))

    def disconnect(self, ws: WebSocket):
        self._connections.pop(ws, None)
        log.info("WS client disconnected (%d total)", len(self._connections))

    async def send(self, ws: WebSocket, data: dict):
        lock = self._connections.get(ws)
        if not lock:
            return
        async with lock:
            try:
                await ws.send_json(_json_safe(data))
            except Exception:
                log.exception("WS send failed — removing dead connection")  # [FIX #23]
                self.disconnect(ws)

    async def broadcast(self, data: dict):
        """[FIX #18] Iterate a snapshot; catch per-connection exceptions; discard dead sockets."""
        safe_data = _json_safe(data)
        msg_type  = data.get("type", "?")
        if msg_type not in ("instrument_ltp", "index_ltp"):
            log.info("WS broadcast [%s] to %d clients", msg_type, len(self._connections))

        dead = []
        # Snapshot so disconnect() during iteration is safe
        for ws, lock in list(self._connections.items()):
            try:
                async with lock:
                    await ws.send_json(safe_data)
            except Exception:
                log.exception("WS broadcast failed for a client — marking dead")  # [FIX #23]
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    @property
    def active(self):
        return list(self._connections.keys())


ws_manager = ConnectionManager()


def _today_ist() -> str:
    IST = ZoneInfo("Asia/Kolkata")
    return datetime.now(IST).strftime("%Y-%m-%d")


# ── App Lifecycle ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "Starting trading platform — mode=%s, kotak_env=%s",
        Config.TRADING_MODE, Config.kotak_env(),
    )
    await db.init_db()
    await manager.paper_trader.rehydrate_from_db()
    manager.set_ws_broadcast(ws_manager.broadcast)
    async def _gated_process_message(text, sender, timestamp):
        """Gate process_message on the stop_trading flag.
        Message is still saved/parsed (visible in UI) but no trade
        is placed when _stop_trading is True.
        """
        manager.stop_trading = _stop_trading
        return await manager.process_message(text=text, sender=sender, timestamp=timestamp)

    telegram.set_callback(_gated_process_message)
    asyncio.create_task(telegram.start())

    # Background: check order/position timeouts every 10 s
    async def timeout_checker():
        while True:
            try:
                await manager.paper_trader.check_timeouts()
            except Exception:
                log.exception("Timeout checker error")  # [FIX #23]
            await asyncio.sleep(10)

    asyncio.create_task(timeout_checker())

    # ── Shared tick pipeline (lifespan scope) ────────────────────────────────
    SENSEX_INDEX_TOKENS = {"1", "999901", "50060"}
    tick_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    _main_loop = asyncio.get_running_loop()

    def enqueue_tick(token, ltp, data):
        """[FIX #5] Catch QueueFull; warn at 80% capacity."""
        try:
            _main_loop.call_soon_threadsafe(tick_queue.put_nowait, (token, ltp, data))
            qsize = tick_queue.qsize()
            if qsize > 4000:
                log.warning(
                    "Tick queue at %d/5000 (%.0f%%) — consider increasing maxsize",
                    qsize, qsize / 5000 * 100,
                )
        except asyncio.QueueFull:
            log.warning(
                "Tick queue full — tick dropped (token=%s ltp=%s). "
                "Increase maxsize or reduce DB write frequency.",
                token, ltp,
            )

    async def tick_consumer():
        """Drain tick_queue and forward LTP events to WebSocket clients."""
        while True:
            try:
                token, ltp, data = await asyncio.wait_for(tick_queue.get(), timeout=1.0)
                symbol = data.get("symbol", "")
                if token in SENSEX_INDEX_TOKENS or symbol == "SENSEX":
                    await ws_manager.broadcast({
                        "type": "index_ltp",
                        "data": {"symbol": "SENSEX", "ltp": ltp},
                    })
                elif symbol:
                    await ws_manager.broadcast({
                        "type": "instrument_ltp",
                        "data": {"symbol": symbol, "ltp": ltp},
                    })
                if symbol:
                    await db.update_signal_ltp(symbol, ltp)
                tick_queue.task_done()
            except asyncio.TimeoutError:
                pass
            except Exception:
                log.exception("Tick consumer error")  # [FIX #23]

    # [FIX #6] Drain task reference — shared so daily_contract_refresh can cancel/recreate
    _tick_drain_task: list[asyncio.Task] = []

    async def kotak_auto_login():
        if not any(Config.kotak_env().values()):
            log.info("Skipping Kotak auto-login — env not configured.")
            return
        if manager.kotak.is_authenticated and manager.kotak.session_active:
            log.info("Skipping Kotak auto-login — session already active.")
            return

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, manager.initialize_kotak)
        except Exception:
            log.exception("Kotak initialize failed")  # [FIX #23]
            return

        log.info("Kotak Neo client initialized — attempting auto-login...")
        try:
            manager.kotak_login_state      = "logging_in"
            manager.kotak_last_login_error = None

            login_result = await loop.run_in_executor(None, manager.login_kotak)

            if login_result.get("status") == "ok":
                manager.kotak_login_state = "logged_in"
                log.info("Kotak Neo auto-login successful")

                manager.market_feed.start()
                manager.market_feed.add_tick_callback(manager.paper_trader.on_tick)
                manager.market_feed.add_raw_tick_callback(enqueue_tick)

                task = asyncio.create_task(tick_consumer())
                _tick_drain_task.clear()
                _tick_drain_task.append(task)
                log.info("Tick consumer task started")

                cached = await loop.run_in_executor(None, manager.contract_master.load_cached)
                if cached:
                    log.info("Pre-loaded %d contracts from cached CSV", len(manager.contract_master.get_all()))

                await asyncio.sleep(3)
                manager.market_feed.subscribe_batch([
                    {"instrument_token": "1",      "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    {"instrument_token": "999901",  "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    {"instrument_token": "50060",   "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                ])
                await loop.run_in_executor(None, manager.download_contracts)
                await manager.resubscribe_recent_signals(limit=20)
            else:
                manager.kotak_login_state      = "login_failed"
                manager.kotak_last_login_error = login_result.get("message")
                log.warning("Kotak auto-login failed: %s", login_result.get("message"))
        except Exception:
            manager.kotak_login_state      = "login_failed"
            manager.kotak_last_login_error = "Unexpected error — see logs"
            log.exception("Kotak auto-login error")  # [FIX #23]

    asyncio.create_task(kotak_auto_login())

    # ── Daily contract refresh ────────────────────────────────────────────────
    IST         = ZoneInfo("Asia/Kolkata")
    REFRESH_TIME = dt_time(8, 50)

    async def daily_contract_refresh():
        """[FIX #6] Refreshes contracts at 08:50 IST."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                now_ist = datetime.now(IST)
                target  = datetime.combine(now_ist.date(), REFRESH_TIME, tzinfo=IST)
                if now_ist >= target:
                    target += timedelta(days=1)
                wait_secs = (target - now_ist).total_seconds()
                log.info(
                    "Next contract master refresh at %s (%.1fh from now)",
                    target.strftime("%Y-%m-%d %H:%M IST"), wait_secs / 3600,
                )
                await asyncio.sleep(wait_secs)

                if not Config.KOTAK_CONSUMER_KEY:
                    continue

                log.info("08:50 IST — refreshing contract master...")
                try:
                    await loop.run_in_executor(None, manager.initialize_kotak)
                except Exception:
                    log.exception("daily_contract_refresh: initialize_kotak failed")

                def force_relogin():
                    manager.kotak.is_authenticated = False
                    manager.kotak.session_active   = False
                    return manager.login_kotak()

                login_result = await loop.run_in_executor(None, force_relogin)

                if login_result.get("status") == "ok":
                    if _tick_drain_task:
                        old_task = _tick_drain_task[0]
                        old_task.cancel()
                        try:
                            await old_task
                        except asyncio.CancelledError:
                            pass
                        _tick_drain_task.clear()
                        log.info("daily_contract_refresh: old tick drain task cancelled")

                    try:
                        manager.market_feed.stop()
                    except Exception:
                        log.exception("daily_contract_refresh: market_feed.stop() failed")

                    manager.market_feed._started_once = False
                    manager.market_feed.start()
                    manager.market_feed.add_tick_callback(manager.paper_trader.on_tick)
                    manager.market_feed.add_raw_tick_callback(enqueue_tick)

                    new_task = asyncio.create_task(tick_consumer())
                    _tick_drain_task.clear()
                    _tick_drain_task.append(new_task)
                    log.info("daily_contract_refresh: tick pipeline restarted")

                    manager.market_feed.subscribe_batch([
                        {"instrument_token": "1",      "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                        {"instrument_token": "999901",  "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                        {"instrument_token": "50060",   "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    ])
                    await loop.run_in_executor(None, manager.download_contracts)
                    await manager.resubscribe_recent_signals(limit=20)
                    log.info("Daily contract master refresh complete")
                else:
                    log.warning("Daily refresh login failed: %s", login_result.get("message"))
            except Exception:
                log.exception("Daily contract refresh error")  # [FIX #23]
                await asyncio.sleep(3600)

    asyncio.create_task(daily_contract_refresh())

    # ── Telegram health check ─────────────────────────────────────────────────
    async def telegram_health_check():
        """[FIX #9] Exponential backoff + circuit breaker after 3 consecutive failures."""
        consecutive_failures = 0
        MAX_FAILURES         = 3
        BASE_BACKOFF_SECS    = 60
        MAX_BACKOFF_SECS     = 600

        while True:
            await asyncio.sleep(BASE_BACKOFF_SECS)

            if not (telegram.is_running and telegram.client):
                continue

            try:
                await telegram.client.get_me()
                if consecutive_failures > 0:
                    log.info("Telegram health check recovered after %d failure(s)", consecutive_failures)
                consecutive_failures = 0

            except Exception:
                consecutive_failures += 1
                backoff = min(BASE_BACKOFF_SECS * (2 ** (consecutive_failures - 1)), MAX_BACKOFF_SECS)
                log.exception(
                    "Telegram health check failed (attempt %d/%d) — reconnecting in %ds",
                    consecutive_failures, MAX_FAILURES, backoff,
                )

                if consecutive_failures >= MAX_FAILURES:
                    log.error(
                        "ALERT: Telegram health check failed %d consecutive times — "
                        "manual intervention may be required. Backing off %ds.",
                        consecutive_failures, backoff,
                    )

                try:
                    await telegram.stop()
                except Exception:
                    log.exception("telegram_health_check: stop() failed")
                try:
                    await telegram.start()
                    log.info("Telegram reconnect succeeded")
                except Exception:
                    log.exception("Telegram reconnect failed")

                await asyncio.sleep(backoff)

    asyncio.create_task(telegram_health_check())

    log.info("Trading platform started")
    yield

    await telegram.stop()
    manager.market_feed.stop()

    if _tick_drain_task:
        _tick_drain_task[0].cancel()

    await db.close_connections()
    log.info("Trading platform stopped")


# ── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Telegram Kotak Trader",
    description="Telegram signal → Kotak Neo trading bridge",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


# ── Pydantic Models ───────────────────────────────────────────────────────────
class ModeRequest(BaseModel):
    mode: str

class OTPRequest(BaseModel):
    otp: Optional[str] = None

class TestSignalRequest(BaseModel):
    text: str
    sender: str = "Test"

class LotSizeRequest(BaseModel):
    lots: int

class ClearRequest(BaseModel):
    date: Optional[str] = None   # "YYYY-MM-DD" clears that day; None clears all

class StopTradingRequest(BaseModel):
    enabled: bool

class StrategyRequest(BaseModel):
    lots:                      int             = 1
    entryLogic:                str             = "code"
    entryAvgPick:              str             = "avg"
    entryFixed:                Optional[float] = None
    trailingSL:                str             = "code"
    slFixed:                   Optional[float] = None
    compareMode:               bool            = False
    activationPoints:          Optional[float] = 5.0
    trailGap:                  Optional[float] = 2.0
    entryTimerMins:            int             = 10
    exitTimerMins:             int             = 10
    signalTrailInitialSL:      str             = "telegram"
    signalTrailInitialSLPoints: Optional[float] = 5.0


# ── REST Endpoints ────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    status = manager.get_status()
    status["telegram"]    = telegram.is_running
    status["ws_clients"]  = len(ws_manager.active)
    status["stop_trading"] = _stop_trading
    return status


@app.get("/api/messages")
async def get_messages(
    limit: int           = Query(100, ge=1, le=500),
    date:  Optional[str] = Query(None),
):
    if date is None:
        date = _today_ist()
    elif date == "all":
        date = None
    return await db.get_messages(limit=limit, date=date)


@app.get("/api/signals")
async def get_signals(
    limit: int           = Query(100, ge=1, le=500),
    date:  Optional[str] = Query(None),
):
    if date is None:
        date = _today_ist()
    elif date == "all":
        date = None
    return await db.get_signals(limit=limit, date=date)


@app.get("/api/trades")
async def get_trades(
    mode:  Optional[str] = None,
    limit: int           = Query(200, ge=1, le=1000),
    date:  Optional[str] = Query(None, description="YYYY-MM-DD or 'all'. Defaults to today (IST)."),
):
    if date is None:
        date = _today_ist()
    elif date == "all":
        date = None
    return await db.get_trades(mode=mode, limit=limit, date=date)


@app.get("/api/positions")
async def get_positions(mode: Optional[str] = None, status: str = "open"):
    return await db.get_positions(mode=mode, status=status)


@app.get("/api/pnl")
async def get_pnl():
    return manager.paper_trader.get_pnl_summary()


@app.post("/api/mode")
async def set_mode(req: ModeRequest):
    result = manager.set_mode(req.mode)
    await ws_manager.broadcast({"type": "mode_change", "data": result})
    return result


@app.post("/api/positions/{position_id}/exit")
async def exit_position(position_id: int):
    """[FIX #1] close_position() already broadcasts position_update — no duplicate here."""
    result = await manager.paper_trader.close_position(position_id)
    return result


@app.post("/api/kill")
async def kill_switch():
    result  = await manager.paper_trader.square_off_all()
    _today  = _today_ist()
    await ws_manager.broadcast({"type": "init", "data": {
        "status":    manager.get_status(),
        "messages":  await db.get_messages(limit=200, date=None),
        "signals":   await db.get_signals(limit=200, date=None),
        "trades":    await db.get_trades(limit=50, date=_today),
        "positions": await db.get_positions(status=None),
    }})
    return result


@app.get("/api/settings")
async def get_settings():
    return {"lot_size": manager.lot_size, "strategy": manager.strategy}


@app.post("/api/settings/lot-size")
async def set_lot_size(req: LotSizeRequest):
    result = manager.set_lot_size(req.lots)
    await ws_manager.broadcast({"type": "settings_update", "data": result})
    return result


@app.post("/api/settings/strategy")
async def set_strategy(req: StrategyRequest):
    manager.strategy = req.dict()
    if req.lots and req.lots != manager.lot_size:
        manager.set_lot_size(req.lots)
    save_strategy(manager.strategy)
    await ws_manager.broadcast({"type": "settings_update", "data": {
        "strategy": manager.strategy,
        "lot_size": manager.lot_size,
    }})
    return {"status": "ok", "strategy": manager.strategy}


# ── Kotak Auth ────────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
async def kotak_login():
    """[FIX #4] Blocking SDK calls run in executor — event loop stays responsive."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, manager.initialize_kotak)
    return await loop.run_in_executor(None, manager.login_kotak)


@app.post("/api/auth/2fa")
async def kotak_2fa(req: OTPRequest):
    """[FIX #4] complete_2fa internally runs download_contracts in executor (already patched)."""
    return await manager.complete_2fa(req.otp)


# ── Test & Maintenance ────────────────────────────────────────────────────────

@app.post("/api/clear")
async def clear_data(req: ClearRequest):
    """Clear dashboard data.
    req.date = "YYYY-MM-DD" → clears only that day's records.
    req.date = None          → clears everything (all history).
    In-memory state (pending orders, open positions) is only wiped on full clear.
    """
    await db.clear_all_data(date=req.date)
    if req.date is None:
        # Full clear — reset in-memory state too
        manager._processed_signals.clear()
        manager.paper_trader._pending_orders.clear()
        if hasattr(manager.paper_trader, "_open_positions"):
            manager.paper_trader._open_positions.clear()
        if hasattr(manager.paper_trader, "_positions"):
            manager.paper_trader._positions.clear()
    msg = f"Data for {req.date} cleared" if req.date else "All dashboard data cleared"
    return {"status": "ok", "message": msg}


@app.post("/api/stop-trading")
async def set_stop_trading(req: StopTradingRequest):
    """Toggle the stop-trading flag.
    When enabled=True, incoming Telegram signals are ignored (not traded).
    Existing positions and pending orders continue to be managed normally.
    """
    global _stop_trading
    _stop_trading = req.enabled
    manager.stop_trading = req.enabled   # propagate to TradeManager if it checks this flag
    log.info("Stop trading set to %s", _stop_trading)
    await ws_manager.broadcast({"type": "stop_trading_update", "data": {"enabled": _stop_trading}})
    return {"status": "ok", "enabled": _stop_trading}


@app.post("/api/test-signal")
async def test_signal(req: TestSignalRequest):
    if _stop_trading:
        return {"status": "ignored", "reason": "Stop trading is enabled"}
    return await manager.process_message(
        text=req.text,
        sender=req.sender,
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        today = _today_ist()
        await ws_manager.send(ws, {
            "type": "init",
            "data": {
                "status":    manager.get_status(),
                "messages":  await db.get_messages(limit=200, date=None),
                "signals":   await db.get_signals(limit=200, date=None),
                "trades":    await db.get_trades(limit=50, date=today),
                "positions": await db.get_positions(status=None),
            },
        })

        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await ws_manager.send(ws, {"type": "pong"})
            except json.JSONDecodeError:
                pass

    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        log.exception("WS handler error")  # [FIX #23]
        ws_manager.disconnect(ws)


# ── Mount frontend static assets ─────────────────────────────────────────────
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")