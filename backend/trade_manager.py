"""
Trade Manager — Central orchestrator that routes parsed signals to Paper or Real trading engine.
Handles mode switching, duplicate detection, and market hours validation.
"""
import asyncio
import logging
from datetime import datetime, time, timezone
from typing import Optional

from .config import Config
from .signal_parser import parse_signal
from .paper_trader import PaperTrader
from .kotak_trader import KotakTrader
from .market_feed import MarketFeed
from .contract_master import ContractMaster
from . import database as db

log = logging.getLogger(__name__)

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
        self.contract_master = ContractMaster()
        self.lot_size = int(Config.DEFAULT_LOT_SIZE)
        self.strategy = {
            'lots': 1,
            'entryLogic': 'code',   # 'code' | 'avg_signal' | 'fixed'
            'entryFixed': None,
            'trailingSL': 'code',   # 'code' | 'signal' | 'ltp' | 'fixed'
            'slFixed': None,
        }

        self._processed_signals: set[str] = set()  # dedup hashes
        self._ws_broadcast: Optional[callable] = None  # WebSocket broadcaster

    def set_lot_size(self, lots: int):
        """Update the number of lots for upcoming trades."""
        self.lot_size = max(1, int(lots))
        log.info(f"Lot size updated to {self.lot_size}")
        return {"status": "ok", "lot_size": self.lot_size}

    def set_ws_broadcast(self, broadcast_fn):
        """Set the WebSocket broadcast function for real-time frontend updates."""
        self._ws_broadcast = broadcast_fn
        self.paper_trader.set_ws_broadcast(broadcast_fn) # Pass to paper trader too

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

        # Broadcast new message to frontend
        await self._broadcast("new_message", {
            "id": msg_id, "text": text, "sender": sender,
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

        # Broadcast parsed signal
        # Use ISO format with Z to ensure frontend JS parses as UTC correctly
        signal_dict["created_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        signal_dict["live_ltp"] = initial_ltp
        await self._broadcast("new_signal", signal_dict)

        if signal.status != "valid":
            log.info(f"Signal ignored: {signal.reason}")
            return {"message_id": msg_id, "signal": signal_dict, "trade": None}

        # Subscribe to market feed immediately for valid signals so LTP streams to frontend
        if self.market_feed and self.contract_master:
            op_type = signal_dict.get('option_type') or ''
            lookup = self.contract_master.lookup(str(signal_dict.get('strike') or ''), op_type)
            if lookup:
                token = lookup['instrument_token']
                symbol = lookup.get('trading_symbol', f"SENSEX{signal_dict['strike']}{signal_dict['option_type']}")
                self.market_feed.subscribe_instrument(token, symbol)
                log.info(f"Subscribed market feed for {symbol} (token: {token}) right after parse")
            else:
                log.warning(f"Could not find instrument token for {signal_dict.get('strike')} {signal_dict.get('option_type')} — ticks may not arrive")

        # 4. Duplicate check — by strike + option_type only (ignore entry price changes)
        sig_hash = f"{signal.strike}-{signal.option_type}"

        # Check DB for existing OPEN positions for this strike
        open_positions = await db.get_positions(mode=self.mode, status="open")
        for pos in open_positions:
            if str(pos.get('strike')) == str(signal.strike) and str(pos.get('option_type', '')).upper() == signal.option_type.upper():
                log.info(f"Signal skipped — position already open for {sig_hash}")
                return {"message_id": msg_id, "signal": signal_dict, "trade": None, "skipped": "position_open"}

        pending_trades = [t for t in await db.get_trades(mode=self.mode) if t.get("status") == "pending"]
        order_replaced = False
        replaced_signal_id = None
        
        sig_suffix = f"{signal.strike}{signal.option_type}".upper()
        
        for order in pending_trades:
            order_sym = order.get('trading_symbol', '').upper()
            
            # Match if trading symbol ends with strike+option_type (handles both SENSEX78500CE and SENSEX2631278500CE)
            if order_sym.endswith(sig_suffix):
                # Cancel the old pending order in DB and memory
                await db.update_trade(order["id"], {"status": "replaced"})
                self.paper_trader._pending_orders = [po for po in self.paper_trader._pending_orders if po.get("trade_id") != order["id"]]
                log.info(f"Cancelled old pending order {order['id']} for {sig_hash}")
                replaced_signal_id = order.get("signal_id")
                await self._broadcast("order_update", {
                    "id": order["id"],
                    "signal_id": replaced_signal_id,  # <─ so frontend can update the OLD signal card
                    "status": "replaced",
                    "status_note": "Replaced by newer signal"
                })
                order_replaced = True
                break
        
        # Check if we already processed this strike today (and it didn't just replace a pending)
        if sig_hash in self._processed_signals and not order_replaced:
            log.info(f"Duplicate signal skipped (already processed/closed): {sig_hash}")
            return {"message_id": msg_id, "signal": signal_dict, "trade": None, "skipped": "duplicate"}

        self._processed_signals.add(sig_hash)

        # 5. Market hours check (warn but don't block paper trades)
        if not self._is_market_open() and self.mode == "real":
            log.warning("Market is closed — skipping real trade")
            return {"message_id": msg_id, "signal": signal_dict, "trade": None, "skipped": "market_closed"}

        # 6. Execute trade
        trade_result = await self._execute_trade(signal_dict, signal_id)

        # Enrich broadcast with signal_id so frontend can update signal cards
        broadcast_data = {
            **trade_result,
            "signal_id": signal_id,                                   # always include
            "trading_symbol": trade_result.get("trading_symbol") or  # flat access
                              (trade_result.get("order") or {}).get("trading_symbol", ""),
        }

        # Broadcast trade execution
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

    async def _cancel_pending_duplicate(self, sig_hash: str) -> bool:
        """Cancel a pending order that matches the signal hash (strike-type). Returns True if cancelled."""
        for order in list(self.paper_trader._pending_orders):
            order_hash = f"{order.get('strike')}-{order.get('option_type')}"
            if order_hash == sig_hash and order.get("status") == "pending":
                # Cancel the old pending order
                self.paper_trader._pending_orders.remove(order)
                await db.update_trade(order["trade_id"], {"status": "replaced"})
                log.info(f"Cancelled old pending order {order['trade_id']} for {order['trading_symbol']}")
                await self._broadcast("order_update", {
                    "id": order["trade_id"],
                    "signal_id": order.get("signal_id"),
                    "status": "replaced",
                    "status_note": "Replaced by newer signal"
                })
                return True
        return False


    async def _execute_paper(self, signal: dict, signal_id: int) -> dict:
        """Execute via paper trading engine."""
        try:
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

    async def _execute_real(self, signal: dict, signal_id: int) -> dict:
        """Execute via Kotak Neo real trading."""
        if not self.kotak.is_authenticated:
            return {"status": "error", "message": "Kotak Neo not authenticated"}

        try:
            # Search for the correct trading symbol
            scrip = self.kotak.search_scrip(
                symbol="SENSEX",
                option_type=signal["option_type"],
                strike_price=signal["strike"],
            )

            trading_symbol = ""
            if scrip and isinstance(scrip, dict):
                # Extract trading symbol from search result
                instruments = scrip.get("data", [])
                if instruments:
                    trading_symbol = instruments[0].get("pTrdSymbol", "")

            if not trading_symbol:
                trading_symbol = f"SENSEX{signal['strike']}{signal['option_type']}"
                log.warning(f"Using constructed symbol: {trading_symbol}")

            # Place the order
            entry_price = signal.get("entry_high", signal.get("entry_low", 0))
            result = self.kotak.place_order(
                exchange_segment="bse_fo",
                trading_symbol=trading_symbol,
                transaction_type="B",
                order_type="L",
                quantity=Config.DEFAULT_LOT_SIZE,
                price=entry_price,
            )

            # Save trade to DB
            trade_data = {
                "mode": "real",
                "exchange_segment": "bse_fo",
                "trading_symbol": trading_symbol,
                "transaction_type": "B",
                "order_type": "L",
                "quantity": Config.DEFAULT_LOT_SIZE,
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
        count = 0
        for sig in recent_signals:
            if sig.get('status') == 'valid' and sig.get('strike') and sig.get('option_type'):
                lookup = self.contract_master.lookup(str(sig['strike']), sig['option_type'])
                if lookup:
                    token = lookup['instrument_token']
                    symbol = lookup.get('trading_symbol', f"SENSEX{sig['strike']}{sig['option_type']}")
                    # Avoid duplicate subscription calls if already tracked
                    if str(token) not in self.market_feed._subscriptions:
                        self.market_feed.subscribe_instrument(token, symbol)
                        count += 1
        if count > 0:
            log.info(f"Re-subscribed to {count} recent signals from database")

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

    async def complete_2fa(self, otp: str = None) -> dict:
        """Complete Kotak Neo 2FA."""
        result = self.kotak.complete_2fa(otp)
        if result.get("status") == "ok":
            # Start market feed after auth
            self.market_feed.start()
            # Register paper trader for tick updates
            self.market_feed.add_tick_callback(self.paper_trader.on_tick)
            # Download contract master
            self.download_contracts()
            # Re-hydrate subscriptions for recently active signal cards
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
        """Check if Indian stock market is currently open."""
        now = datetime.now().time()
        return MARKET_OPEN <= now <= MARKET_CLOSE

    def get_status(self) -> dict:
        """Return full system status."""
        return {
            "mode": self.mode,
            "market_open": self._is_market_open(),
            "kotak": self.kotak.get_status(),
            "market_feed": self.market_feed.is_running,
            "telegram": True,  # Updated by main.py
            "paper_trader": self.paper_trader.get_pnl_summary(),
        }

    def clear_duplicates(self):
        """Reset the duplicate signal tracker (e.g. for a new day)."""
        self._processed_signals.clear()
