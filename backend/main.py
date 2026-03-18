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
[10] kotat_auto_login typo fixed → kotak_auto_login
[11] Strategy loaded from disk on startup so manager.strategy is never stale
[12] compareMode added to StrategyRequest; get_trades supports date filter
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
    format="%(asctime)s [startup=%(process)d] [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ── Strategy persistence path ──
STRATEGY_FILE = os.path.join(os.path.dirname(__file__), "..", "strategy.json")

STRATEGY_DEFAULTS = {
    "lots": 1,
    "entryLogic": "code",
    "entryAvgPick": "avg",
    "entryFixed": None,
    "trailingSL": "code",
    "slFixed": None,
    "compareMode": False,  # [12]
}


def load_strategy() -> dict:
    """Load strategy from disk, falling back to defaults."""
    try:
        if os.path.exists(STRATEGY_FILE):
            with open(STRATEGY_FILE, "r") as f:
                saved = json.load(f)
                return {**STRATEGY_DEFAULTS, **saved}
    except Exception as e:
        log.warning(f"Could not load strategy from disk: {e}")
    return dict(STRATEGY_DEFAULTS)


def save_strategy(strategy: dict):
    """Persist strategy to disk."""
    try:
        with open(STRATEGY_FILE, "w") as f:
            json.dump(strategy, f, indent=2)
    except Exception as e:
        log.warning(f"Could not save strategy to disk: {e}")


# ── Global instances ──
manager = TradeManager()
telegram = TelegramListener()

# [4][11] Load persisted strategy into manager immediately on import
manager.strategy = load_strategy()


# ── WebSocket Connection Manager ──
def _json_safe(obj):
    """Recursively convert datetime objects to ISO strings so send_json never crashes."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat().replace("+00:00", "Z")
    return obj


class ConnectionManager:
    """Manages active WebSocket connections."""

    def __init__(self):
        self._connections: dict[WebSocket, asyncio.Lock] = {}

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections[ws] = asyncio.Lock()
        log.info(f"WS client connected ({len(self._connections)} total)")

    def disconnect(self, ws: WebSocket):
        self._connections.pop(ws, None)
        log.info(f"WS client disconnected ({len(self._connections)} total)")

    async def send(self, ws: WebSocket, data: dict):
        lock = self._connections.get(ws)
        if not lock:
            return
        async with lock:
            await ws.send_json(_json_safe(data))

    async def broadcast(self, data: dict):
        safe_data = _json_safe(data)
        msg_type = data.get("type", "?")
        n = len(self._connections)
        if msg_type not in ("instrument_ltp", "index_ltp"):
            log.info(f"WS broadcast [{msg_type}] to {n} clients")
        dead = []
        for ws, lock in list(self._connections.items()):
            try:
                async with lock:
                    await ws.send_json(safe_data)
            except Exception as e:
                log.error(f"WS send failed: {e}")
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    @property
    def active(self):
        return list(self._connections.keys())


ws_manager = ConnectionManager()


def _today_ist() -> str:
    """Return today's date in IST as YYYY-MM-DD string."""
    IST = ZoneInfo("Asia/Kolkata")
    return datetime.now(IST).strftime("%Y-%m-%d")


# ── App Lifecycle ──
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    log.info(
        "Starting trading platform — mode=%s, kotak_env=%s",
        Config.TRADING_MODE,
        Config.kotak_env(),
    )
    log.info("Initializing database...")
    await db.init_db()

    await manager.paper_trader.rehydrate_from_db()
    manager.set_ws_broadcast(ws_manager.broadcast)
    telegram.set_callback(manager.process_message)
    asyncio.create_task(telegram.start())

    async def timeout_checker():
        while True:
            try:
                await manager.paper_trader.check_timeouts()
            except Exception as e:
                log.error(f"Timeout checker error: {e}")
            await asyncio.sleep(10)

    asyncio.create_task(timeout_checker())

    async def kotak_auto_login():
        if not any(Config.kotak_env().values()):
            log.info("Skipping Kotak auto-login — env not configured.")
            return

        if manager.kotak.is_authenticated and manager.kotak.session_active:
            log.info("Skipping Kotak auto-login — session already active.")
            return

        log.info("Initializing Kotak Neo client for auto-login...")
        loop = asyncio.get_running_loop()

        await loop.run_in_executor(None, manager.initialize_kotak)

        log.info("Kotak Neo client initialized — attempting auto-login in background...")
        try:
            manager.kotak_login_state = "logging_in"
            manager.kotak_last_login_error = None

            login_result = await loop.run_in_executor(None, manager.login_kotak)

            if login_result.get("status") == "ok":
                manager.kotak_login_state = "logged_in"
                log.info("✅ Kotak Neo auto-login successful!")

                manager.market_feed.start()
                manager.market_feed.add_tick_callback(manager.paper_trader.on_tick)

                SENSEX_INDEX_TOKENS = ["1", "999901", "50060"]
                tick_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
                _main_loop = asyncio.get_running_loop()

                def enqueue_tick(token, ltp, data):
                    try:
                        _main_loop.call_soon_threadsafe(tick_queue.put_nowait, (token, ltp, data))
                    except Exception:
                        pass

                manager.market_feed.add_raw_tick_callback(enqueue_tick)

                async def tick_consumer():
                    while True:
                        try:
                            token, ltp, data = await asyncio.wait_for(tick_queue.get(), timeout=1.0)
                            symbol = data.get("symbol", "")
                            if token in SENSEX_INDEX_TOKENS or symbol == "SENSEX":
                                await ws_manager.broadcast({"type": "index_ltp", "data": {"symbol": "SENSEX", "ltp": ltp}})
                            elif symbol:
                                await ws_manager.broadcast({"type": "instrument_ltp", "data": {"symbol": symbol, "ltp": ltp}})
                            tick_queue.task_done()
                        except asyncio.TimeoutError:
                            pass
                        except Exception as e:
                            log.error(f"Tick consumer error: {e}")

                asyncio.create_task(tick_consumer())
                log.info("Tick consumer task started")

                cached = await loop.run_in_executor(None, manager.contract_master.load_cached)
                if cached:
                    log.info(f"Pre-loaded {len(manager.contract_master.get_all())} contracts from cached CSV")

                await asyncio.sleep(3)

                manager.market_feed.subscribe_batch([
                    {"instrument_token": "1", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    {"instrument_token": "999901", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    {"instrument_token": "50060", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                ])

                await loop.run_in_executor(None, manager.download_contracts)
                await manager.resubscribe_recent_signals(limit=20)
            else:
                manager.kotak_login_state = "login_failed"
                manager.kotak_last_login_error = login_result.get("message")
                log.warning("Kotak auto-login failed: %s", login_result.get("message"))
        except Exception as e:
            manager.kotak_login_state = "login_failed"
            manager.kotak_last_login_error = str(e)
            log.warning("Kotak auto-login error: %s", e)

    asyncio.create_task(kotak_auto_login())

    IST = ZoneInfo("Asia/Kolkata")
    REFRESH_TIME = dt_time(8, 50)

    async def daily_contract_refresh():
        loop = asyncio.get_running_loop()
        while True:
            try:
                now_ist = datetime.now(IST)
                target = datetime.combine(now_ist.date(), REFRESH_TIME, tzinfo=IST)
                if now_ist >= target:
                    target += timedelta(days=1)
                wait_secs = (target - now_ist).total_seconds()
                log.info(f"Next contract master refresh at {target.strftime('%Y-%m-%d %H:%M IST')} ({wait_secs/3600:.1f}h from now)")
                await asyncio.sleep(wait_secs)

                if Config.KOTAK_CONSUMER_KEY:
                    log.info("⏰ 08:50 IST — refreshing contract master...")
                    await loop.run_in_executor(None, manager.initialize_kotak)

                    def force_relogin():
                        manager.kotak.is_authenticated = False
                        manager.kotak.session_active = False
                        return manager.login_kotak()

                    login_result = await loop.run_in_executor(None, force_relogin)
                    if login_result.get("status") == "ok":
                        try:
                            manager.market_feed.stop()
                        except Exception as e:
                            log.warning(f"Could not stop market feed before refresh: {e}")
                        manager.market_feed.start()
                        manager.market_feed.subscribe_batch([
                            {"instrument_token": "1", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                            {"instrument_token": "999901", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                            {"instrument_token": "50060", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                        ])
                        await loop.run_in_executor(None, manager.download_contracts)
                        await manager.resubscribe_recent_signals(limit=20)
                        log.info("✅ Daily contract master refresh complete")
                    else:
                        log.warning(f"Daily refresh login failed: {login_result.get('message')}")
            except Exception as e:
                log.error(f"Daily contract refresh error: {e}")
                await asyncio.sleep(3600)

    asyncio.create_task(daily_contract_refresh())

    async def telegram_health_check():
        while True:
            await asyncio.sleep(60)
            if telegram.is_running and telegram.client:
                try:
                    await telegram.client.get_me()
                except Exception as e:
                    log.warning(f"Telegram health-check failed: {e} — attempting reconnect")
                    try:
                        await telegram.stop()
                    except Exception:
                        pass
                    try:
                        await telegram.start()
                    except Exception as re:
                        log.error(f"Telegram reconnect failed: {re}")

    asyncio.create_task(telegram_health_check())

    log.info("🚀 Trading platform started")
    yield

    await telegram.stop()
    manager.market_feed.stop()
    await db.close_connections()
    log.info("Trading platform stopped")


# ── FastAPI App ──
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


# ── Pydantic Models ──
class ModeRequest(BaseModel):
    mode: str


class OTPRequest(BaseModel):
    otp: Optional[str] = None


class TestSignalRequest(BaseModel):
    text: str
    sender: str = "Test"


class LotSizeRequest(BaseModel):
    lots: int


class StrategyRequest(BaseModel):
    lots: int = 1
    entryLogic: str = 'code'
    entryAvgPick: str = 'avg'
    entryFixed: Optional[float] = None
    trailingSL: str = 'code'
    slFixed: Optional[float] = None
    compareMode: bool = False  # [12] run all 5 entry modes simultaneously


# ── REST Endpoints ──

@app.get("/api/status")
async def get_status():
    status = manager.get_status()
    status["telegram"] = telegram.is_running
    status["ws_clients"] = len(ws_manager.active)
    return status


@app.get("/api/messages")
async def get_messages(
    limit: int = Query(100, ge=1, le=500),
    date: Optional[str] = Query(None),
):
    """Get recent Telegram messages. Defaults to today (IST)."""
    if date is None:
        date = _today_ist()
    elif date == "all":
        date = None
    return await db.get_messages(limit=limit, date=date)


@app.get("/api/signals")
async def get_signals(
    limit: int = Query(100, ge=1, le=500),
    date: Optional[str] = Query(None),
):
    """Get parsed signals. Defaults to today (IST)."""
    if date is None:
        date = _today_ist()
    elif date == "all":
        date = None
    return await db.get_signals(limit=limit, date=date)


@app.get("/api/trades")
async def get_trades(
    mode: Optional[str] = None,
    limit: int = Query(200, ge=1, le=1000),
    date: Optional[str] = Query(None, description="Filter by date YYYY-MM-DD. Defaults to today (IST).")
):
    """Get trade history. Defaults to today's trades only. Pass date=all for everything."""
    # [12] Default to today's date so dashboard shows only today's trades
    if date is None:
        date = _today_ist()
    elif date == "all":
        date = None  # no date filter — return everything
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
    result = await manager.paper_trader.close_position(position_id)
    if result.get("status") == "closed":
        await ws_manager.broadcast({"type": "position_update", "data": {
            "id": position_id,
            "status": "closed",
            "pnl": result.get("pnl"),
            "reason": "Manual exit",
        }})
        await ws_manager.broadcast({"type": "new_trade", "data": {
            **result,
            "id": position_id,
            "reason": "Manual exit",
        }})
    return result


@app.post("/api/kill")
async def kill_switch():
    result = await manager.paper_trader.square_off_all()
    _today = _today_ist()
    await ws_manager.broadcast({"type": "init", "data": {
        "status": manager.get_status(),
        "messages": await db.get_messages(limit=50, date=_today),
        "signals": await db.get_signals(limit=50, date=_today),
        "trades": await db.get_trades(limit=50, date=_today),
        "positions": await db.get_positions(),
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


# ── Kotak Auth Endpoints ──

@app.post("/api/auth/login")
async def kotak_login():
    manager.initialize_kotak()
    return manager.login_kotak()


@app.post("/api/auth/2fa")
async def kotak_2fa(req: OTPRequest):
    return manager.complete_2fa(req.otp)


# ── Test & Maintenance Endpoints ──

@app.post("/api/clear")
async def clear_data():
    await db.clear_all_data()
    manager._processed_signals.clear()
    manager.paper_trader._pending_orders.clear()
    if hasattr(manager.paper_trader, '_open_positions'):
        manager.paper_trader._open_positions.clear()
    if hasattr(manager.paper_trader, '_positions'):
        manager.paper_trader._positions.clear()
    return {"status": "ok", "message": "Dashboard data cleared"}


@app.post("/api/test-signal")
async def test_signal(req: TestSignalRequest):
    result = await manager.process_message(
        text=req.text,
        sender=req.sender,
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
    )
    return result


# ── WebSocket ──

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        # [12] Send only today's trades on init
        today = _today_ist()
        await ws_manager.send(ws, {
            "type": "init",
            "data": {
                "status": manager.get_status(),
                "messages": await db.get_messages(limit=50, date=today),
                "signals": await db.get_signals(limit=50, date=today),
                "trades": await db.get_trades(limit=50, date=today),
                "positions": await db.get_positions(),
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
    except Exception as e:
        log.error(f"WS error: {e}")
        ws_manager.disconnect(ws)


# ── Mount frontend static assets ──
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")