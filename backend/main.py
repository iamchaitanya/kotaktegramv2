"""
Main FastAPI application — REST API + WebSocket for the trading platform.
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
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ── Global instances ──
manager = TradeManager()
telegram = TelegramListener()


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
    """Manages active WebSocket connections.
    Uses per-connection asyncio.Lock to prevent concurrent writes.
    """

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
        """Send data to a specific WebSocket, serialized via its lock."""
        lock = self._connections.get(ws)
        if not lock:
            return
        async with lock:
            await ws.send_json(_json_safe(data))

    async def broadcast(self, data: dict):
        """Send data to ALL connected WebSocket clients."""
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

# ── App Lifecycle ──
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    # Startup
    log.info("Initializing database...")
    await db.init_db()

    # Wire up WebSocket broadcasting
    manager.set_ws_broadcast(ws_manager.broadcast)

    # Wire up Telegram → TradeManager pipeline
    telegram.set_callback(manager.process_message)

    # Start Telegram listener in background
    asyncio.create_task(telegram.start())

    # Background task: check for expired orders/positions every 10 seconds
    async def timeout_checker():
        while True:
            try:
                await manager.paper_trader.check_timeouts()
            except Exception as e:
                log.error(f"Timeout checker error: {e}")
            await asyncio.sleep(10)
    asyncio.create_task(timeout_checker())

    # Auto-login to Kotak Neo (non-blocking)
    # Auto-login to Kotak Neo (non-blocking background task)
    async def kotat_auto_login():
        if not Config.KOTAK_CONSUMER_KEY:
            return
            
        manager.initialize_kotak()
        log.info("Kotak Neo client initialized — attempting auto-login in background...")
        try:
            # Note: manager.login_kotak is a synchronous wrapper around the SDK's login flow
            # We run it in the main loop for now as it handles its own background threads,
            # but wrapping it in this async task prevents it from blocking the lifespan 'yield'.
            login_result = manager.login_kotak()
            if login_result.get("status") == "ok":
                log.info("✅ Kotak Neo auto-login successful!")
                # Start market feed (registers WS callbacks)
                manager.market_feed.start()
                manager.market_feed.add_tick_callback(manager.paper_trader.on_tick)

                # Use an asyncio.Queue to decouple high-frequency Kotak ticks
                # from the main event loop.
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

                await asyncio.sleep(3)

                manager.market_feed.subscribe_batch([
                    {"instrument_token": "1", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    {"instrument_token": "999901", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                    {"instrument_token": "50060", "exchange_segment": "bse_cm", "symbol": "SENSEX"},
                ])

                manager.download_contracts()
                await manager.resubscribe_recent_signals(limit=20)
            else:
                log.warning(f"Kotak auto-login failed: {login_result.get('message')} — use Settings to login manually")
        except Exception as e:
            log.warning(f"Kotak auto-login error: {e} — use Settings to login manually")

    asyncio.create_task(kotat_auto_login())

    # Background task: refresh contract master daily at 08:50 IST
    IST = ZoneInfo("Asia/Kolkata")
    REFRESH_TIME = dt_time(8, 50)  # 08:50 AM IST

    async def daily_contract_refresh():
        while True:
            try:
                now_ist = datetime.now(IST)
                target = datetime.combine(now_ist.date(), REFRESH_TIME, tzinfo=IST)
                # If we've already passed 08:50 today, schedule for tomorrow
                if now_ist >= target:
                    target += timedelta(days=1)
                wait_secs = (target - now_ist).total_seconds()
                log.info(f"Next contract master refresh at {target.strftime('%Y-%m-%d %H:%M IST')} ({wait_secs/3600:.1f}h from now)")
                await asyncio.sleep(wait_secs)

                # Re-login and download fresh contracts
                if Config.KOTAK_CONSUMER_KEY:
                    log.info("⏰ 08:50 IST — refreshing contract master...")
                    manager.initialize_kotak()
                    login_result = manager.login_kotak()
                    if login_result.get("status") == "ok":
                        manager.market_feed.start()
                        manager.download_contracts()
                        log.info("✅ Daily contract master refresh complete")
                    else:
                        log.warning(f"Daily refresh login failed: {login_result.get('message')}")
            except Exception as e:
                log.error(f"Daily contract refresh error: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour on failure

    asyncio.create_task(daily_contract_refresh())

    log.info("🚀 Trading platform started")
    yield

    # Shutdown
    await telegram.stop()
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
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Frontend — serve static files ──
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

@app.get("/", include_in_schema=False)
async def serve_index():
    """Serve the frontend dashboard."""
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


# ── Pydantic Models ──
class ModeRequest(BaseModel):
    mode: str  # 'paper' or 'real'


class OTPRequest(BaseModel):
    otp: Optional[str] = None


class TestSignalRequest(BaseModel):
    text: str
    sender: str = "Test"


class LotSizeRequest(BaseModel):
    lots: int


class StrategyRequest(BaseModel):
    lots: int = 1
    entryLogic: str = 'code'    # 'code' | 'avg_signal' | 'fixed'
    entryAvgPick: str = 'avg'   # 'low' | 'avg' | 'high'  (for avg_signal mode)
    entryFixed: Optional[float] = None
    trailingSL: str = 'code'    # 'code' | 'signal' | 'ltp' | 'fixed'
    slFixed: Optional[float] = None


# ── REST Endpoints ──

@app.get("/api/status")
async def get_status():
    """System health & connection status."""
    status = manager.get_status()
    status["telegram"] = telegram.is_running
    status["ws_clients"] = len(ws_manager.active)
    status["lot_size"] = manager.lot_size
    status["strategy"] = manager.strategy
    return status


@app.get("/api/messages")
async def get_messages(limit: int = Query(100, ge=1, le=500)):
    """Get recent Telegram messages."""
    return await db.get_messages(limit)


@app.get("/api/signals")
async def get_signals(limit: int = Query(100, ge=1, le=500)):
    """Get parsed signals."""
    return await db.get_signals(limit)


@app.get("/api/trades")
async def get_trades(mode: Optional[str] = None, limit: int = Query(100, ge=1, le=500)):
    """Get trade history."""
    return await db.get_trades(mode=mode, limit=limit)


@app.get("/api/positions")
async def get_positions(mode: Optional[str] = None, status: str = "open"):
    """Get current positions."""
    return await db.get_positions(mode=mode, status=status)


@app.get("/api/pnl")
async def get_pnl():
    """Get P&L summary."""
    return manager.paper_trader.get_pnl_summary()


@app.post("/api/mode")
async def set_mode(req: ModeRequest):
    """Switch between paper and real trading mode."""
    result = manager.set_mode(req.mode)
    await ws_manager.broadcast({"type": "mode_change", "data": result})
    return result


# ── Position & Order Controls ──

@app.post("/api/positions/{position_id}/exit")
async def exit_position(position_id: int):
    """Manually close a single position at current price."""
    result = await manager.paper_trader.close_position(position_id)
    if result.get("status") == "closed":
        await ws_manager.broadcast({"type": "new_trade", "data": {
            **result, "id": position_id, "reason": "Manual exit"
        }})
    return result


@app.post("/api/kill")
async def kill_switch():
    """Square off ALL open positions and cancel ALL pending orders."""
    result = await manager.paper_trader.square_off_all()
    # Refresh frontend state
    await ws_manager.broadcast({"type": "init", "data": {
        "status": manager.get_status(),
        "messages": await db.get_messages(50),
        "signals": await db.get_signals(50),
        "trades": await db.get_trades(limit=50),
        "positions": await db.get_positions(),
    }})
    return result


@app.get("/api/settings")
async def get_settings():
    """Get current trading settings."""
    return {"lot_size": manager.lot_size}


@app.post("/api/settings/lot-size")
async def set_lot_size(req: LotSizeRequest):
    """Update the number of lots for upcoming trades."""
    result = manager.set_lot_size(req.lots)
    await ws_manager.broadcast({"type": "settings_update", "data": result})
    return result


@app.post("/api/settings/strategy")
async def set_strategy(req: StrategyRequest):
    """Save trading strategy settings (entry logic + trailing SL mode)."""
    manager.strategy = req.dict()
    # Also update lot size if provided
    if req.lots and req.lots != manager.lot_size:
        manager.set_lot_size(req.lots)
    return {"status": "ok", "strategy": manager.strategy}


# ── Kotak Auth Endpoints ──

@app.post("/api/auth/login")
async def kotak_login():
    """Step 1: Login to Kotak Neo."""
    manager.initialize_kotak()
    return manager.login_kotak()


@app.post("/api/auth/2fa")
async def kotak_2fa(req: OTPRequest):
    """Step 2: Complete 2FA with OTP/TOTP."""
    return manager.complete_2fa(req.otp)


# ── Test & Maintenance Endpoints ──

@app.post("/api/clear")
async def clear_data():
    """Clear all UI-visible dashboard data. (Ticks are saved)."""
    await db.clear_all_data()
    # Clear in-memory deduplication flags
    manager._processed_signals.clear()
    manager.paper_trader._pending_orders.clear()
    return {"status": "ok", "message": "Dashboard data cleared"}

@app.post("/api/test-signal")
async def test_signal(req: TestSignalRequest):
    """Inject a test signal (bypasses Telegram)."""
    result = await manager.process_message(
        text=req.text,
        sender=req.sender,
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
    )
    return result


# ── WebSocket ──

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Real-time updates for the frontend dashboard."""
    await ws_manager.connect(ws)
    try:
        # Send initial state
        await ws_manager.send(ws, {
            "type": "init",
            "data": {
                "status": manager.get_status(),
                "messages": await db.get_messages(50),
                "signals": await db.get_signals(50),
                "trades": await db.get_trades(limit=50),
                "positions": await db.get_positions(),
            },
        })

        # Keep connection alive and handle client messages (pings etc.)
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


# ── Mount frontend static assets (CSS, JS, images) ──
# Must be after all API routes so it doesn't shadow /api/* or /ws
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
