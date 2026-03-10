"""
Paper Trader — Simulates order execution using real market ticks.
Fills when LTP enters the entry range, tracks virtual P&L.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable

from . import database as db

log = logging.getLogger(__name__)

# Strategy constants
ENTRY_TIMEOUT_MINS = 10   # Max wait for bounce-entry condition
POSITION_TIMEOUT_MINS = 10  # Max hold before forced exit at LTP


class PaperTrader:
    """Paper trading engine using live ticks for realistic simulation."""

    def __init__(self, market_feed=None):
        self.market_feed = market_feed
        self._pending_orders: list[dict] = []   # waiting for fill
        self._open_positions: list[dict] = []   # active positions
        self._fill_callbacks: list = []
        self._ws_broadcast: Optional[Callable] = None

    def set_ws_broadcast(self, broadcast_fn):
        """Set broadcaster for real-time frontend updates."""
        self._ws_broadcast = broadcast_fn

    async def _broadcast(self, event_type: str, data: dict):
        if self._ws_broadcast:
            try:
                await self._ws_broadcast({"type": event_type, "data": data})
            except Exception as e:
                log.error(f"PaperTrader broadcast error: {e}")

    def add_fill_callback(self, callback):
        """Register callback for when a paper order fills.
        Signature: async callback(trade: dict, position: dict)
        """
        self._fill_callbacks.append(callback)

    async def check_timeouts(self):
        """Check for expired pending orders and timed-out positions.
        This runs independently of tick data — called from a background task.
        """
        now = datetime.now(timezone.utc)

        # 1. Expire pending orders past 10-min entry timeout
        expired = []
        for order in self._pending_orders:
            created_at = order.get("created_at")
            if created_at and (now - created_at) > timedelta(minutes=ENTRY_TIMEOUT_MINS):
                log.warning(f"TIMEOUT: Expiring pending order {order['trading_symbol']}")
                await db.update_trade(order["trade_id"], {"status": "expired"})
                expired.append(order)
                await self._broadcast("order_update", {
                    "id": order["trade_id"],
                    "status": "expired",
                    "status_note": "Entry timeout (10 min) — discarded"
                })
        for order in expired:
            if order in self._pending_orders:
                self._pending_orders.remove(order)

        # 2. Force-close open positions past 10-min hold timeout
        closed = []
        for pos in self._open_positions:
            opened_at = pos.get("opened_at")
            if opened_at:
                # opened_at may be a string (ISO) or datetime — handle both
                if isinstance(opened_at, str):
                    opened_at = datetime.fromisoformat(opened_at.replace("Z", "+00:00")).replace(tzinfo=None)
                if (now - opened_at) > timedelta(minutes=POSITION_TIMEOUT_MINS):
                    exit_price = pos.get("current_price", pos["entry_price"])
                    log.warning(f"TIMEOUT: Force-closing {pos['trading_symbol']} @ {exit_price}")
                    result = await self.close_position(pos["id"], exit_price=exit_price)
                    closed.append(pos)
                    await self._broadcast("new_trade", {
                        **result, "status": "closed", "trading_symbol": pos["trading_symbol"],
                        "reason": "10-min hold timeout"
                    })
        # close_position already removes from _open_positions

    async def place_order(self, signal: dict, signal_id: int, lot_size: int = None, strategy: dict = None) -> dict:
        """Place a paper order that will fill when LTP enters entry range."""
        from .config import Config
        qty = lot_size or int(Config.DEFAULT_LOT_SIZE)
        strategy = strategy or {}

        # Resolve SL mode and store params on the order for use at fill time
        sl_mode = strategy.get('trailingSL', 'code')  # code | signal | ltp | fixed
        sl_points = strategy.get('slFixed') or 5       # points away (for 'fixed' mode)

        # For 'signal' mode: gap = entry_low - signal stoploss
        signal_stoploss = signal.get('stoploss')   # parsed from Telegram message
        entry_low = signal.get('entry_low', 0)
        entry_high = signal.get('entry_high', 0)
        sl_gap = None
        if sl_mode == 'signal' and signal_stoploss and entry_low:
            sl_gap = float(entry_low) - float(signal_stoploss)
            if sl_gap <= 0:
                log.warning(f"Signal SL gap <= 0 ({sl_gap}), falling back to code mode")
                sl_mode = 'code'
                sl_gap = None

        # ── Resolve entry price from entryLogic strategy ──
        entry_logic = strategy.get('entryLogic', 'code')
        avg_pick = strategy.get('entryAvgPick', 'avg')  # 'low' | 'avg' | 'high'

        if entry_logic == 'fixed' and strategy.get('entryFixed'):
            order_price = float(strategy['entryFixed'])
            log.info(f"Entry mode=fixed: price={order_price}")
        elif entry_logic == 'avg_signal':
            hi = float(entry_high or 0)
            lo = float(entry_low or 0)
            if avg_pick == 'low':
                order_price = lo
            elif avg_pick == 'high':
                order_price = hi
            else:  # 'avg'
                order_price = (lo + hi) / 2.0 if (lo and hi) else (lo or hi)
            log.info(f"Entry mode=avg_signal ({avg_pick}): price={order_price}")
        else:  # 'code' — default: use entry_high (bounce fills from top of range down)
            order_price = float(entry_high or entry_low or 0)
            log.info(f"Entry mode=code: price={order_price}")

        order = {
            "signal_id": signal_id,
            "mode": "paper",
            "exchange_segment": "bse_fo",
            "trading_symbol": f"SENSEX{signal['strike']}{signal['option_type']}",
            "transaction_type": "B",
            "order_type": "L",
            "quantity": qty,
            "price": order_price,
            "trigger_price": 0,
            "status": "pending",
            "order_id": f"PAPER-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{signal_id}",
            "notes": f"Paper BUY {signal['strike']} {signal['option_type']} @ {signal.get('entry_low')}-{signal.get('entry_high')} [{entry_logic}]",
            "entry_low": entry_low,
            "entry_high": entry_high,
            "strike": signal.get("strike"),
            "option_type": signal.get("option_type"),
            "created_at": datetime.now(timezone.utc),
            # SL strategy params — baked in at order time, applied at fill time
            "sl_mode": sl_mode,
            "sl_gap": sl_gap,
            "sl_points": float(sl_points),
        }

        # Save to DB
        trade_id = await db.save_trade(signal_id, order)
        order["trade_id"] = trade_id

        # Always queue as pending — real market ticks will trigger the bounce-entry fill
        self._pending_orders.append(order)
        log.info(f"Paper order pending: {order['trading_symbol']} — SL mode: {sl_mode} — waiting for LTP in [{order['entry_low']}, {order['entry_high']}]")

        if not self.market_feed or not self.market_feed.is_running:
            log.warning("Market feed not running — order will stay pending until feed connects")

        return {"status": "pending", "trade_id": trade_id, "order": order}

    def _symbol_matches(self, order_symbol: str, tick_symbol: str, tick_data: dict) -> bool:
        """Check if a tick's symbol matches an order/position's trading_symbol.
        
        Kotak Neo sends symbols with expiry dates like 'SENSEX2631278500CE'
        while our orders store simplified symbols like 'SENSEX78500CE'.
        We need fuzzy matching to bridge this gap.
        """
        if not order_symbol or not tick_symbol:
            return False
            
        order_upper = order_symbol.upper().strip()
        tick_upper = tick_symbol.upper().strip()
        
        # Exact match
        if order_upper == tick_upper:
            return True
        
        # Fuzzy match: order is "SENSEX78500CE", tick is "SENSEX2631278500CE"
        # Check if tick starts with the same index and ends with the same strike+type
        # Extract index prefix and strike+type suffix from order symbol
        import re
        order_match = re.match(r'^([A-Z]+?)(\d{5}(?:CE|PE))$', order_upper)
        if order_match:
            idx_prefix = order_match.group(1)   # e.g. "SENSEX"
            strike_suffix = order_match.group(2) # e.g. "78500CE"
            if tick_upper.startswith(idx_prefix) and tick_upper.endswith(strike_suffix):
                return True
        
        # Also match by instrument token if available
        token = str(tick_data.get("tk") or tick_data.get("instrument_token", ""))
        if token and self.market_feed:
            sub = self.market_feed._subscriptions.get(token, {})
            sub_symbol = sub.get("symbol", "")
            if sub_symbol:
                # The subscription symbol will be full like "SENSEX2631278500CE"
                # Check if it matches our order via the same fuzzy logic
                sub_upper = sub_symbol.upper().strip()
                if order_match:
                    if sub_upper.startswith(order_match.group(1)) and sub_upper.endswith(order_match.group(2)):
                        return True
        
        return False

    async def on_tick(self, token: str, ltp: float, data: dict):
        """Called on every market tick — handles pending fills, position updates, and timeouts."""
        now = datetime.now(timezone.utc)
        tick_symbol = data.get("symbol", "")

        # 1. Update Open Positions (Trailing SL & PNL & 10-min timeout)
        exited = []
        for pos in self._open_positions:
            if not self._symbol_matches(pos["trading_symbol"], tick_symbol, data):
                continue
            # Check 10-min position hold timeout
            opened_at = pos.get("opened_at")
            if opened_at:
                # opened_at may be a string (ISO) or datetime — normalize
                if isinstance(opened_at, str):
                    opened_at = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
                if opened_at.tzinfo is None:
                    opened_at = opened_at.replace(tzinfo=timezone.utc)
                if (now - opened_at) > timedelta(minutes=POSITION_TIMEOUT_MINS):
                    log.warning(f"10-MIN TIMEOUT: Force-selling {pos['trading_symbol']} @ {ltp}")
                    result = await self.close_position(pos["id"], exit_price=ltp)
                    pos["status"] = "closed"
                    exited.append(pos)
                    await self._broadcast("new_trade", {
                        **result, "status": "closed", "trading_symbol": pos["trading_symbol"],
                        "reason": "10-min hold timeout"
                    })
                else:
                    await self._process_position_tick(pos, ltp)
                    if pos.get("status") == "closed":
                        exited.append(pos)
            else:
                await self._process_position_tick(pos, ltp)
                if pos.get("status") == "closed":
                    exited.append(pos)
        
        for pos in exited:
            if pos in self._open_positions:
                self._open_positions.remove(pos)

        # 2. Check Pending Orders (Bounce Entry & 10-min entry timeout)
        filled = []
        expired = []
        for order in self._pending_orders:
            # Check 10-min entry timeout (regardless of tick symbol)
            created_at = order.get("created_at")
            if created_at and (now - created_at) > timedelta(minutes=ENTRY_TIMEOUT_MINS):
                log.warning(f"10-MIN ENTRY TIMEOUT: Discarding {order['trading_symbol']}")
                await db.update_trade(order["trade_id"], {"status": "expired"})
                expired.append(order)
                await self._broadcast("order_update", {
                    "id": order["trade_id"],
                    "signal_id": order.get("signal_id"),
                    "status": "expired",
                    "status_note": "Entry timeout (10 min)"
                })
                continue

            # Only process LTP if tick matches this order's instrument
            if not self._symbol_matches(order["trading_symbol"], tick_symbol, data):
                continue

            # Check if LTP is within entry range or we are already tracking min_ltp
            in_range = order["entry_low"] <= ltp <= order["entry_high"]
            
            # Start/Update tracking the lowest price for the bounce
            if in_range or order.get("min_ltp") is not None:
                if order.get("min_ltp") is None or ltp < order["min_ltp"]:
                    order["min_ltp"] = ltp
                    await db.update_trade(order["trade_id"], {"min_ltp": ltp})
                    log.info(f"New low for {order['trading_symbol']}: {ltp}")
                    await self._broadcast("order_update", {
                        "id": order["trade_id"],
                        "signal_id": order.get("signal_id"),
                        "min_ltp": ltp,
                        "status_note": f"Tracking bounce from {ltp}"
                    })

            # Rule: Entry only if LTP bounces back 5 points from the lowest price
            if order.get("min_ltp") is not None:
                if ltp >= order["min_ltp"] + 5:
                    result = await self._fill_order(order, ltp)
                    filled.append(order)
                    log.info(f"Paper order FILLED (Bounce-back): {order['trading_symbol']} @ {ltp} (Min was {order['min_ltp']})")
                    result["signal_id"] = order.get("signal_id")
                    await self._broadcast("new_trade", result)

        # Remove filled and expired orders
        for order in filled + expired:
            if order in self._pending_orders:
                self._pending_orders.remove(order)

    async def _process_position_tick(self, pos: dict, ltp: float):
        """Update trailing SL and PNL for a single position on each tick."""
        if ltp <= 0:
            return

        pos["current_price"] = ltp
        pos["pnl"] = (ltp - pos["entry_price"]) * pos["quantity"]
        sl_mode = pos.get("sl_mode", "code")

        new_sl = None

        if sl_mode == "ltp":
            # ── Last LTP mode: SL trails to previous tick's LTP ──
            # On every tick, SL = last seen LTP (1 tick behind)
            prev = pos.get("prev_ltp", pos["entry_price"])
            new_sl = prev  # SL advances to where we just were
            pos["prev_ltp"] = ltp
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp

        elif sl_mode == "signal":
            # ── Signal gap mode: maintain constant gap below LTP peak ──
            # gap = entry_low - signal_stoploss (computed at order time)
            sl_gap = pos.get("sl_gap") or 30.0
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp
                new_sl = ltp - sl_gap
        
        elif sl_mode == "fixed":
            # ── Points away mode: trail by N pts below LTP peak ──
            sl_points = pos.get("sl_points", 5.0)
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp
                new_sl = ltp - sl_points

        else:  # 'code' (default) — stepped 2pt trailing
            # ── Code mode: step SL up by 2pts for every 2pts gain ──
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp
                entry_price = pos["entry_price"]
                steps = int((ltp - entry_price) // 2)
                if steps > 0:
                    initial_sl_offset = max(5.0, 0.03 * entry_price)
                    new_sl = (entry_price - initial_sl_offset) + (steps * 2)

        # Apply new SL (only ever moves UP)
        if new_sl is not None:
            current_sl = pos.get("trailing_sl", 0)
            if new_sl > current_sl:
                pos["trailing_sl"] = new_sl
                log.info(f"[{sl_mode}] SL → {new_sl:.2f} for {pos['trading_symbol']}")
                await self._broadcast("position_update", {
                    "id": pos["id"],
                    "trailing_sl": new_sl,
                    "max_ltp": pos.get("max_ltp", ltp),
                    "status_note": f"SL trailed to {new_sl:.2f} [{sl_mode}]"
                })

        # Check SL hit
        if ltp <= pos.get("trailing_sl", 0):
            log.warning(f"STOP LOSS HIT [{sl_mode}]: {pos['trading_symbol']} @ {ltp} (SL was {pos['trailing_sl']:.2f})")
            result = await self.close_position(pos["id"], exit_price=ltp)
            pos["status"] = "closed"
            await self._broadcast("new_trade", {**result, "status": "closed", "trading_symbol": pos["trading_symbol"]})
        else:
            await db.update_position(pos["id"], {
                "current_price": ltp,
                "pnl": pos["pnl"],
                "max_ltp": pos.get("max_ltp", ltp),
                "trailing_sl": pos["trailing_sl"],
            })
            await self._broadcast("position_update", {
                "id": pos["id"],
                "current_price": ltp,
                "pnl": pos["pnl"]
            })

    async def _fill_order(self, order: dict, fill_price: float) -> dict:
        """Fill a paper order at the given price and set initial SL based on strategy mode."""
        trade_id = order.get("trade_id")
        sl_mode = order.get("sl_mode", "code")
        sl_gap = order.get("sl_gap")       # for 'signal' mode
        sl_points = order.get("sl_points", 5.0)  # for 'fixed' (points) mode

        # ── Calculate initial Stop Loss by mode ──
        if sl_mode == "signal" and sl_gap is not None:
            # Constant gap trailing: SL = fill_price - gap (gap derived from signal)
            initial_sl = fill_price - sl_gap
            log.info(f"SL mode=signal: gap={sl_gap:.2f}, initial SL={initial_sl:.2f}")
        elif sl_mode == "ltp":
            # SL starts at fill price (break-even immediately), trails tick-by-tick
            initial_sl = fill_price
            log.info(f"SL mode=ltp: SL starts at fill price {fill_price}")
        elif sl_mode == "fixed":
            # N points below fill price, trails by same N points
            initial_sl = fill_price - sl_points
            log.info(f"SL mode=fixed ({sl_points}pts): initial SL={initial_sl:.2f}")
        else:  # 'code' (default)
            # Stepped SL: fill_price - max(5, 3%)
            sl_points_code = max(5.0, 0.03 * fill_price)
            initial_sl = fill_price - sl_points_code
            log.info(f"SL mode=code: initial SL={initial_sl:.2f}")

        # Update trade in DB
        await db.update_trade(trade_id, {
            "status": "filled",
            "fill_price": fill_price,
            "fill_time": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        })

        # Create position — store sl_mode and params so trailing logic is locked per-position
        pos_data = {
            "mode": "paper",
            "trading_symbol": order["trading_symbol"],
            "strike": order.get("strike"),
            "option_type": order.get("option_type"),
            "quantity": order["quantity"],
            "entry_price": fill_price,
            "max_ltp": fill_price,
            "trailing_sl": initial_sl,
            # Strategy params — immutable per position
            "sl_mode": sl_mode,
            "sl_gap": sl_gap,
            "sl_points": sl_points,
            "prev_ltp": fill_price,   # for 'ltp' mode
        }
        position_id = await db.save_position(trade_id, pos_data)

        position = {
            **pos_data,
            "id": position_id,
            "trade_id": trade_id,
            "current_price": fill_price,
            "pnl": 0,
            "opened_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        self._open_positions.append(position)
        log.info(f"Position opened | mode={sl_mode} | Entry={fill_price} | Initial SL={initial_sl:.2f}")

        # Fire callbacks
        trade = {**order, "status": "filled", "fill_price": fill_price}
        for cb in self._fill_callbacks:
            try:
                await cb(trade, position)
            except Exception as e:
                log.error(f"Fill callback error: {e}")

        return {
            "status": "filled",
            "trade_id": trade_id,
            "fill_price": fill_price,
            "position_id": position_id,
            **position
        }

    async def update_pnl(self):
        """Update P&L and check for Trailing SL exit."""
        exited = []
        for pos in self._open_positions:
            if self.market_feed:
                # Find the instrument token for this symbol
                for token, data in self.market_feed.get_all_ticks().items():
                    if data.get("symbol") == pos["trading_symbol"]:
                        ltp = data.get("ltp", 0)
                        if ltp <= 0: continue
                        
                        pos["current_price"] = ltp
                        pos["pnl"] = (ltp - pos["entry_price"]) * pos["quantity"]

                        # --- Trailing SL Strategy ---
                        
                        # 1. Update Max LTP tracking
                        if ltp > pos.get("max_ltp", 0):
                            old_max = pos.get("max_ltp", pos["entry_price"])
                            pos["max_ltp"] = ltp
                            
                            # 2. Stepped SL: For every 2 points max_ltp moves up from entry, move SL up by 2
                            # Check how many 2-point steps we've climbed
                            entry_price = pos["entry_price"]
                            steps = int((ltp - entry_price) // 2)
                            if steps > 0:
                                new_sl = (pos["entry_price"] - max(5.0, 0.03 * pos["entry_price"])) + (steps * 2)
                                # SL cannot move downwards
                                if new_sl > pos.get("trailing_sl", 0):
                                    pos["trailing_sl"] = new_sl
                                    log.info(f"Trailing SL moved UP to {new_sl:.2f} for {pos['trading_symbol']}")

                        # 3. Check for SL Hit
                        if ltp <= pos.get("trailing_sl", 0):
                            log.warning(f"STOP LOSS HIT for {pos['trading_symbol']} @ {ltp} (SL was {pos['trailing_sl']:.2f})")
                            await self.close_position(pos["id"], exit_price=ltp)
                            exited.append(pos)
                        else:
                            # Normal update if not exited
                            await db.update_position(pos["id"], {
                                "current_price": ltp,
                                "pnl": pos["pnl"],
                                "max_ltp": pos["max_ltp"],
                                "trailing_sl": pos["trailing_sl"],
                            })
                        break
        
        # Remove closed positions from active list
        for pos in exited:
            if pos in self._open_positions:
                self._open_positions.remove(pos)

    async def close_position(self, position_id: int, exit_price: float = None) -> dict:
        """Close a paper position."""
        for pos in self._open_positions:
            if pos["id"] == position_id:
                price = exit_price or pos.get("current_price", pos["entry_price"])
                pnl = (price - pos["entry_price"]) * pos["quantity"]

                await db.update_position(position_id, {
                    "status": "closed",
                    "current_price": price,
                    "pnl": pnl,
                    "closed_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                })

                # Update associated trade
                await db.update_trade(pos["trade_id"], {"pnl": pnl})

                self._open_positions.remove(pos)
                return {"status": "closed", "pnl": pnl, "exit_price": price}

        return {"status": "error", "message": "Position not found"}

    def get_pending_orders(self) -> list[dict]:
        return list(self._pending_orders)

    def get_open_positions(self) -> list[dict]:
        return list(self._open_positions)

    def get_pnl_summary(self) -> dict:
        total_pnl = sum(p.get("pnl", 0) for p in self._open_positions)
        return {
            "open_positions": len(self._open_positions),
            "pending_orders": len(self._pending_orders),
            "total_unrealized_pnl": total_pnl,
        }

    async def square_off_all(self) -> dict:
        """Close all open positions at current price and cancel all pending orders."""
        results = []

        # Close all open positions
        for pos in list(self._open_positions):
            price = pos.get("current_price", pos.get("entry_price", 0))
            result = await self.close_position(pos["id"], exit_price=price)
            results.append({"position_id": pos["id"], "symbol": pos.get("trading_symbol"), **result})
            await self._broadcast("new_trade", {
                **result, "status": "closed", "trading_symbol": pos.get("trading_symbol"),
                "reason": "Kill switch"
            })

        # Cancel all pending orders
        cancelled = 0
        for order in list(self._pending_orders):
            await db.update_trade(order["trade_id"], {"status": "cancelled"})
            cancelled += 1
            await self._broadcast("order_update", {
                "id": order["trade_id"],
                "status": "cancelled",
                "status_note": "Cancelled by kill switch"
            })
        self._pending_orders.clear()

        log.info(f"KILL SWITCH: Closed {len(results)} positions, cancelled {cancelled} pending orders")
        return {
            "status": "ok",
            "positions_closed": len(results),
            "orders_cancelled": cancelled,
            "results": results,
        }
