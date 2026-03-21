"""
Trade Manager — Central orchestrator that routes parsed signals to Paper or Real trading engine.
Handles mode switching, duplicate detection, and market hours validation.

PATCHES APPLIED:
 [1] _cancel_pending_duplicate dead code removed; inline logic consolidated
 [2] get_status() now includes strategy and lot_size directly
 [3] _processed_signals keyed by date-strike-option_type to allow next-day reuse
 [4] entryAvgPick added to default strategy dict in __init__
 [5] _is_market_open() uses IST timezone explicitly instead of naive local time
 [6] complete_2fa offloads download_contracts to executor to avoid blocking event loop
 [7] sig_hash only added to _processed_signals after successful trade execution
 [8] Redundant int() casts removed in resubscribe_recent_signals
 [9] set_ws_broadcast comment clarified
[10] new_message broadcast uses raw_text key for consistency with DB schema
[11] compareMode added — spawns all 5 entry strategies simultaneously for comparison
[14] signal_trail SL mode added to strategy defaults with activationPoints and trailGap
"""
import asyncio
import logging
from datetime import datetime, date, time, timezone
from typing import Optional, Callable, Any, Dict
from zoneinfo import ZoneInfo

from .config import Config
from .signal_parser import parse_signal
from .paper_trader import PaperTrader
from .kotak_trader import KotakTrader
from .market_feed import MarketFeed
from .contract_master import ContractMaster
from . import database as db

log = logging.getLogger(__name__)

# [5] Use explicit IST timezone for all market hours checks
IST = ZoneInfo("Asia/Kolkata")

# Indian market hours (IST)
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)


class TradeManager:
    """Orchestrates the full signal → trade pipeline."""

    def __init__(self):
        self.mode = Config.TRADING_MODE  # 'paper' or 'real'
        self.kotak = KotakTrader()
        self.market_feed = MarketFeed(self.kotak)
        self.paper_trader = PaperTrader(self.market_feed)
        self.paper_trader._on_trade_expired = self.on_trade_expired
        self.contract_master = ContractMaster()
        self.lot_size = int(Config.DEFAULT_LOT_SIZE)

        # [4] entryAvgPick added to defaults so all keys are always present
        # [11] compareMode: when True, all 5 entry strategies run simultaneously
        # [14] activationPoints / trailGap: used by signal_trail SL mode
        self.strategy: Dict[str, Any] = {
            'lots': 1,
            'entryLogic': 'code',          # 'code' | 'avg_signal' | 'fixed'
            'entryAvgPick': 'avg',         # 'low' | 'avg' | 'high'  (for avg_signal mode)
            'entryFixed': None,
            'trailingSL': 'code',          # 'code' | 'signal' | 'ltp' | 'fixed' | 'signal_trail'
            'slFixed': None,
            'activationPoints': 5.0,       # [14] signal_trail: pts above entry to activate trailing
            'trailGap': 2.0,               # [14] signal_trail: pts behind LTP to trail SL
            'compareMode': False,          # [11] when True, runs all 5 entry modes simultaneously
        }

        # [3] Keyed as "YYYY-MM-DD-strike-option_type" so the same strike is
        # allowed again on a new trading day without needing a manual reset.
        self._processed_signals: set[str] = set()
        self._ws_broadcast: Optional[Callable] = None
        self.kotak_login_state: str = "idle"
        self.kotak_last_login_error: Optional[str] = None

    def _sig_hash(self, strike, option_type: str) -> str:
        """[3] Build a date-scoped dedup key so signals recur correctly next day."""
        today = date.today().isoformat()
        return f"{today}-{strike}-{option_type.upper()}"

    def on_trade_expired(self, trading_symbol: str):
        """Called by paper_trader when a pending order expires — unlock that strike for re-entry."""
        import re
        m = re.search(r'(\d{4,6})(CE|PE)$', trading_symbol.upper())
        if m:
            strike, option_type = m.group(1), m.group(2)
            sig_hash = self._sig_hash(strike, option_type)
            self._processed_signals.discard(sig_hash)
            log.info(f"Trade expired — unlocked {sig_hash} for re-entry")

    def set_lot_size(self, lots: int):
        """Update the number of lots for upcoming trades."""
        self.lot_size = max(1, int(lots))
        log.info(f"Lot size updated to {self.lot_size}")
        return {"status": "ok", "lot_size": self.lot_size}

    def set_ws_broadcast(self, broadcast_fn):
        """Set the WebSocket broadcast function for real-time frontend updates.
        Passed to PaperTrader as well so position/order updates reach the frontend directly.
        """
        self._ws_broadcast = broadcast_fn
        self.paper_trader.set_ws_broadcast(broadcast_fn)

    async def _broadcast(self, event_type: str, data: dict):
        """Broadcast event to all WebSocket clients."""
        if self._ws_broadcast:
            try:
                await self._ws_broadcast({"type": event_type, "data": data})
            except Exception as e:
                log.error(f"Broadcast error: {e}")

    # ── Signal Processing Pipeline ──

    async def process_message(self, text: str, sender: str = "", timestamp: str = ""):
        """Full pipeline: receive message → parse → execute trade → broadcast."""
        # 1. Save raw message
        msg_id = await db.save_message(text, sender=sender)

        # [10] Use raw_text key to match what db.get_messages() returns on page load
        await self._broadcast("new_message", {
            "id": msg_id,
            "raw_text": text,
            "sender": sender,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        })

        # 2. Parse signal
        signal = parse_signal(text)
        signal_dict = signal.to_dict()

        # 3. Save parsed signal
        signal_id = await db.save_signal(msg_id, signal_dict)
        signal_dict["id"] = signal_id
        signal_dict["message_id"] = msg_id

        # Get initial LTP if available
        initial_ltp = 0
        if self.market_feed and self.contract_master:
            op_type = signal_dict.get('option_type') or ''
            lookup = self.contract_master.lookup(str(signal_dict.get('strike') or ''), op_type)
            if lookup:
                token = lookup['instrument_token']
                initial_ltp = self.market_feed.get_ltp(token)

        signal_dict["created_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        signal_dict["live_ltp"] = initial_ltp
        await self._broadcast("new_signal", signal_dict)

        if signal.status != "valid":
            log.info(f"Signal ignored: {signal.reason}")
            return {"message_id": msg_id, "signal": signal_dict, "trade": None}

        # Subscribe to market feed immediately for valid signals
        if self.market_feed and self.contract_master:
            op_type = signal_dict.get('option_type') or ''
            lookup = self.contract_master.lookup(str(signal_dict.get('strike') or ''), op_type)
            if lookup:
                token = lookup['instrument_token']
                symbol = lookup.get('trading_symbol', f"SENSEX{signal_dict['strike']}{signal_dict['option_type']}")
                self.market_feed.subscribe_instrument(token, symbol)
                log.info(f"Subscribed market feed for {symbol} (token: {token})")
            else:
                log.warning(
                    f"Could not find instrument token for {signal_dict.get('strike')} "
                    f"{signal_dict.get('option_type')} — ticks may not arrive"
                )

        # 4. Duplicate check — skip if a position is already open for this strike
        # In compare mode allow multiple positions per strike (each variant is independent)
        if not self.strategy.get('compareMode'):
            open_positions = await db.get_positions(mode=self.mode, status="open")
            for pos in open_positions:
                if (str(pos.get('strike')) == str(signal.strike) and
                        str(pos.get('option_type', '')).upper() == signal.option_type.upper()):
                    log.info(f"Signal skipped — position already open for {signal.strike}-{signal.option_type}")
                    return {"message_id": msg_id, "signal": signal_dict, "trade": None, "skipped": "position_open"}

        # [1] Consolidated pending-order replacement
        sig_suffix = f"{signal.strike}{signal.option_type}".upper()
        pending_trades = [t for t in await db.get_trades(mode=self.mode) if t.get("status") == "pending"]
        order_replaced = False
        replaced_signal_id = None

        for order in pending_trades:
            order_sym = order.get('trading_symbol', '').upper()
            if order_sym.endswith(sig_suffix):
                await db.update_trade(order["id"], {"status": "replaced"})
                self.paper_trader._pending_orders = [
                    po for po in self.paper_trader._pending_orders
                    if po.get("trade_id") != order["id"]
                ]
                log.info(f"Cancelled old pending order {order['id']} for {sig_suffix}")
                replaced_signal_id = order.get("signal_id")
                await self._broadcast("order_update", {
                    "id": order["id"],
                    "signal_id": replaced_signal_id,
                    "status": "replaced",
                    "status_note": "Replaced by newer signal",
                })
                order_replaced = True
                break

        # [3] Use date-scoped hash to allow the same strike on a new trading day
        sig_hash = self._sig_hash(signal.strike, signal.option_type)
        if sig_hash in self._processed_signals and not order_replaced and not self.strategy.get('compareMode'):
            log.info(f"Duplicate signal skipped (already processed today): {sig_hash}")
            return {"message_id": msg_id, "signal": signal_dict, "trade": None, "skipped": "duplicate"}

        # 5. Market hours check (warn but don't block paper trades)
        if not self._is_market_open() and self.mode == "real":
            log.warning("Market is closed — skipping real trade")
            return {"message_id": msg_id, "signal": signal_dict, "trade": None, "skipped": "market_closed"}

        # 6. Execute trade
        trade_result: Dict[str, Any] = await self._execute_trade(signal_dict, signal_id)

        # [7] Only mark as processed after a successful execution to allow retries on error
        if trade_result.get("status") not in ("error", None):
            if not self.strategy.get('compareMode'):
                self._processed_signals.add(sig_hash)
        else:
            log.warning(f"Trade execution failed for {sig_hash} — not marking as processed so retry is allowed")

        order_dict = trade_result.get("order")
        order_sym_out = order_dict.get("trading_symbol", "") if isinstance(order_dict, dict) else ""

        broadcast_data = {
            **trade_result,
            "signal_id": signal_id,
            "trading_symbol": trade_result.get("trading_symbol") or order_sym_out,
        }

        await self._broadcast("new_trade", broadcast_data)

        return {"message_id": msg_id, "signal": signal_dict, "trade": trade_result}

    async def _execute_trade(self, signal: dict, signal_id: int) -> dict:
        """Route signal to the appropriate trading engine."""
        if self.mode == "paper":
            return await self._execute_paper(signal, signal_id)
        elif self.mode == "real":
            return await self._execute_real(signal, signal_id)
        else:
            return {"status": "error", "message": f"Unknown mode: {self.mode}"}

    async def _execute_paper(self, signal: dict, signal_id: int) -> dict:
        """Execute via paper trading engine.
        If compareMode is enabled, spawns all 5 entry strategies simultaneously
        so results can be compared in the trade log.
        """
        try:
            if self.strategy.get('compareMode'):
                return await self._execute_paper_compare(signal, signal_id)
            result = await self.paper_trader.place_order(
                signal, signal_id,
                lot_size=self.lot_size,
                strategy=self.strategy,
            )
            log.info(f"Paper trade: {result}")
            return result
        except Exception as e:
            log.error(f"Paper trade error: {e}")
            return {"status": "error", "message": str(e)}

    async def _execute_paper_compare(self, signal: dict, signal_id: int) -> dict:
        """[11] Run all 5 entry modes simultaneously on the same signal for comparison.
        Each order is tagged with its entry_label so the trade log can show them separately.
        """
        hi = float(signal.get("entry_high") or 0)
        lo = float(signal.get("entry_low") or 0)
        live_ltp = float(signal.get("live_ltp") or 0)

        variants = [
            {"entryLogic": "code",       "entryAvgPick": "avg",  "entryFixed": None,           "label": "code"},
            {"entryLogic": "avg_signal", "entryAvgPick": "high", "entryFixed": None,           "label": "high"},
            {"entryLogic": "avg_signal", "entryAvgPick": "low",  "entryFixed": None,           "label": "low"},
            {"entryLogic": "avg_signal", "entryAvgPick": "avg",  "entryFixed": None,           "label": "avg"},
            {"entryLogic": "fixed",      "entryAvgPick": "avg",  "entryFixed": live_ltp or hi, "label": "fixed"},
        ]

        results = []
        for variant in variants:
            strat = {**self.strategy, **variant}
            tagged_signal = {**signal, "entry_label": variant["label"]}
            try:
                result = await self.paper_trader.place_order(
                    tagged_signal, signal_id,
                    lot_size=self.lot_size,
                    strategy=strat,
                )
                result["entry_label"] = variant["label"]
                results.append(result)
                log.info(f"Compare mode — placed [{variant['label']}] order: {result.get('trade_id')}")
            except Exception as e:
                log.error(f"Compare mode error for [{variant['label']}]: {e}")

        return {
            "status": "pending",
            "compare_mode": True,
            "variants": results,
            "trade_id": results[0]["trade_id"] if results else None,
        }

    async def _execute_real(self, signal: dict, signal_id: int) -> dict:
        """Execute via Kotak Neo real trading."""
        if not self.kotak.is_authenticated:
            return {"status": "error", "message": "Kotak Neo not authenticated"}

        try:
            scrip = self.kotak.search_scrip(
                symbol="SENSEX",
                option_type=signal["option_type"],
                strike_price=signal["strike"],
            )

            trading_symbol = ""
            if scrip and isinstance(scrip, dict):
                instruments = scrip.get("data", [])
                if instruments:
                    trading_symbol = instruments[0].get("pTrdSymbol", "")

            if not trading_symbol:
                trading_symbol = f"SENSEX{signal['strike']}{signal['option_type']}"
                log.warning(f"Using constructed symbol: {trading_symbol}")

            entry_price = signal.get("entry_high", signal.get("entry_low", 0))
            result = self.kotak.place_order(
                exchange_segment="bse_fo",
                trading_symbol=trading_symbol,
                transaction_type="B",
                order_type="L",
                quantity=self.lot_size,
                price=entry_price,
            )

            trade_data = {
                "mode": "real",
                "exchange_segment": "bse_fo",
                "trading_symbol": trading_symbol,
                "transaction_type": "B",
                "order_type": "L",
                "quantity": self.lot_size,
                "price": entry_price,
                "status": "placed" if result.get("status") == "ok" else "failed",
                "order_id": result.get("data", {}).get("nOrdNo", ""),
                "notes": f"Real BUY {signal['strike']} {signal['option_type']} @ {entry_price}",
            }
            trade_id = await db.save_trade(signal_id, trade_data)
            trade_data["trade_id"] = trade_id

            return {**trade_data, **result}
        except Exception as e:
            log.error(f"Real trade error: {e}")
            return {"status": "error", "message": str(e)}

    async def resubscribe_recent_signals(self, limit: int = 20):
        """Re-subscribe to the market feed for recently processed signals (useful on startup)."""
        if not self.market_feed or not self.contract_master:
            return

        recent_signals = await db.get_signals(limit=limit)
        subs_count = 0
        for sig in recent_signals:
            if sig.get('status') == 'valid' and sig.get('strike') and sig.get('option_type'):
                lookup = self.contract_master.lookup(str(sig['strike']), sig['option_type'])
                if lookup:
                    token = lookup['instrument_token']
                    symbol = lookup.get('trading_symbol', f"SENSEX{sig['strike']}{sig['option_type']}")
                    if str(token) not in self.market_feed._subscriptions:
                        self.market_feed.subscribe_instrument(token, symbol)
                        subs_count += 1
        if subs_count > 0:
            log.info(f"Re-subscribed to {subs_count} recent signals from database")

    # ── Mode Management ──

    def set_mode(self, mode: str) -> dict:
        """Switch between paper and real trading."""
        if mode not in ("paper", "real"):
            return {"status": "error", "message": "Mode must be 'paper' or 'real'"}

        old_mode = self.mode
        self.mode = mode
        log.info(f"Trading mode changed: {old_mode} → {mode}")
        return {"status": "ok", "old_mode": old_mode, "new_mode": mode}

    # ── Kotak Auth ──

    def initialize_kotak(self) -> dict:
        """Initialize Kotak Neo client."""
        return {"initialized": self.kotak.initialize()}

    def login_kotak(self) -> dict:
        """Login to Kotak Neo."""
        return self.kotak.login()

    async def complete_2fa(self, otp: Optional[str] = None) -> dict:
        """Complete Kotak Neo 2FA."""
        result = self.kotak.complete_2fa(otp)
        if result.get("status") == "ok":
            self.market_feed.start()
            self.market_feed.add_tick_callback(self.paper_trader.on_tick)
            # [6] Offload blocking contract download to executor
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.download_contracts)
            await self.resubscribe_recent_signals(limit=20)
        return result

    def download_contracts(self):
        """Download and cache the daily contract master."""
        if self.kotak.is_authenticated:
            success = self.contract_master.download(self.kotak)
            if success:
                log.info(f"Contract master loaded: {len(self.contract_master.get_all())} SENSEX contracts")
            else:
                log.warning("Contract master download failed — using fallback symbol construction")

    # ── Utilities ──

    def _is_market_open(self) -> bool:
        """[5] Check if Indian stock market is currently open (uses explicit IST timezone)."""
        now_ist = datetime.now(IST).time()
        return MARKET_OPEN <= now_ist <= MARKET_CLOSE

    def get_status(self) -> dict:
        """[2] Return full system status including lot_size and strategy."""
        kotak_status = self.kotak.get_status()
        if self.kotak_login_state != "idle":
            kotak_status["login_state"] = self.kotak_login_state
        if self.kotak_last_login_error:
            kotak_status["last_error"] = self.kotak_last_login_error

        return {
            "mode": self.mode,
            "market_open": self._is_market_open(),
            "kotak": kotak_status,
            "market_feed": self.market_feed.is_running,
            "telegram": True,  # Overwritten by main.py with live value
            "paper_trader": self.paper_trader.get_pnl_summary(),
            "lot_size": self.lot_size,
            "strategy": self.strategy,
        }

    def clear_duplicates(self):
        """Reset the duplicate signal tracker (e.g. for a new day)."""
        self._processed_signals.clear()