"""
Paper Trader — Simulates order execution using real market ticks.
Fills when LTP enters the entry range, tracks virtual P&L.

PATCHES APPLIED:
 [1] update_pnl() dead code removed — on_tick() owns all SL/PNL logic
 [2] check_timeouts() datetime comparison fixed — no longer strips timezone
 [3] close_position() now broadcasts position_update with status:'closed'
 [4] import re moved to top of file — out of hot tick path
 [5] Double-close race condition guarded — pos marked 'closed' before first await
 [6] opened_at stored as datetime object in memory; only serialized for DB/WS
 [7] square_off_all iterates a snapshot; close_position guard prevents double-remove
 [8] Bounce threshold configurable via strategy dict (default 5)
 [9] trading_symbol construction uses explicit int+upper cast
[10] get_pnl_summary comment added re: asyncio single-thread safety
[11] Bounce entry only for 'code' mode; 'fixed'/'avg_signal' fill on direct price touch
[12] _on_trade_expired initialized in __init__ — prevents AttributeError on order expiry
[13] rehydrate_from_db restores entry_low/entry_high from notes — not just price; 'fixed'/'avg_signal' fill on direct price touch
"""
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable

from . import database as db

log = logging.getLogger(__name__)

# Strategy constants
ENTRY_TIMEOUT_MINS = 10    # Max wait for bounce-entry condition
POSITION_TIMEOUT_MINS = 10  # Max hold before forced exit at LTP
DEFAULT_BOUNCE_POINTS = 5   # [8] Default bounce threshold — overridable via strategy


class PaperTrader:
    """Paper trading engine using live ticks for realistic simulation."""

    def __init__(self, market_feed=None):
        self.market_feed = market_feed
        self._pending_orders: list[dict] = []   # waiting for fill
        self._open_positions: list[dict] = []   # active positions
        self._fill_callbacks: list = []
        self._ws_broadcast: Optional[Callable] = None
        self._on_trade_expired: Optional[Callable] = None  # [12] always initialized — prevents AttributeError

    def set_ws_broadcast(self, broadcast_fn):
        """Set broadcaster for real-time frontend updates.
        Also used by TradeManager to propagate position/order events to the frontend.
        """
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
        Called from a background task every 10 seconds, independent of tick data.
        """
        now = datetime.now(timezone.utc)

        # 1. Expire pending orders past 10-min entry timeout
        expired = []
        for order in self._pending_orders:
            created_at = order.get("created_at")
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
            if created_at and (now - created_at) > timedelta(minutes=ENTRY_TIMEOUT_MINS):
                log.warning(f"TIMEOUT: Expiring pending order {order['trading_symbol']}")
                await db.update_trade(order["trade_id"], {"status": "expired"})
                expired.append(order)
                await self._broadcast("order_update", {
                    "id": order["trade_id"],
                    "signal_id": order.get("signal_id"),
                    "status": "expired",
                    "status_note": "Entry timeout (10 min) — discarded",
                })
                if self._on_trade_expired:
                    self._on_trade_expired(order["trading_symbol"])
        for order in expired:
            if order in self._pending_orders:
                self._pending_orders.remove(order)

        # 2. Force-close open positions past 10-min hold timeout
        for pos in list(self._open_positions):
            # [5] Skip if already being closed by a concurrent tick
            if pos.get("status") == "closed":
                continue

            opened_at = pos.get("opened_at")
            if not opened_at:
                continue

            # [6] opened_at is stored as datetime in memory; handle legacy string just in case
            if isinstance(opened_at, str):
                opened_at = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
                if opened_at.tzinfo is None:
                    opened_at = opened_at.replace(tzinfo=timezone.utc)

            # [2] Both sides are now timezone-aware — no TypeError
            if (now - opened_at) > timedelta(minutes=POSITION_TIMEOUT_MINS):
                exit_price = pos.get("current_price", pos["entry_price"])
                log.warning(f"TIMEOUT: Force-closing {pos['trading_symbol']} @ {exit_price}")
                result = await self.close_position(pos["id"], exit_price=exit_price)
                if result.get("status") == "closed":
                    await self._broadcast("new_trade", {
                        **result,
                        "status": "closed",
                        "trading_symbol": pos["trading_symbol"],
                        "reason": "10-min hold timeout",
                    })

    async def place_order(self, signal: dict, signal_id: int, lot_size: int = None, strategy: dict = None) -> dict:
        """Place a paper order that will fill when LTP enters entry range."""
        from .config import Config
        qty = (lot_size or int(Config.DEFAULT_LOT_SIZE)) * 20  # 1 lot = 20 units for SENSEX
        strategy = strategy or {}

        sl_mode = strategy.get('trailingSL', 'code')
        sl_points = strategy.get('slFixed') or 5

        signal_stoploss = signal.get('stoploss')
        entry_low = signal.get('entry_low', 0)
        entry_high = signal.get('entry_high', 0)
        sl_gap = None
        if sl_mode == 'signal' and signal_stoploss and entry_low:
            sl_gap = float(entry_low) - float(signal_stoploss)
            if sl_gap <= 0:
                log.warning(f"Signal SL gap <= 0 ({sl_gap}), falling back to code mode")
                sl_mode = 'code'
                sl_gap = None

        entry_logic = strategy.get('entryLogic', 'code')
        avg_pick = strategy.get('entryAvgPick', 'avg')

        if entry_logic == 'fixed':
            if strategy.get('entryFixed'):
                order_price = float(strategy['entryFixed'])
                log.info(f"Entry mode=fixed: price={order_price}")
            else:
                # Default to live LTP at signal arrival time — best available price reference
                live_ltp = float(signal.get('live_ltp') or 0)
                order_price = live_ltp if live_ltp > 0 else float(entry_high or entry_low or 0)
                log.info(f"Entry mode=fixed: no entryFixed set — using live LTP {order_price}")
        elif entry_logic == 'avg_signal':
            hi = float(entry_high or 0)
            lo = float(entry_low or 0)
            if avg_pick == 'low':
                order_price = lo
            elif avg_pick == 'high':
                order_price = hi
            else:
                order_price = (lo + hi) / 2.0 if (lo and hi) else (lo or hi)
            log.info(f"Entry mode=avg_signal ({avg_pick}): price={order_price}")
        else:
            order_price = float(entry_high or entry_low or 0)
            log.info(f"Entry mode=code: price={order_price}")

        # [8] Bounce threshold from strategy, with sensible default
        bounce_points = float(strategy.get('bouncePoints') or DEFAULT_BOUNCE_POINTS)

        # [9] Explicit int+upper cast for safe symbol construction
        trading_symbol = f"SENSEX{int(signal['strike'])}{str(signal.get('option_type', '')).upper()}"

        order = {
            "signal_id": signal_id,
            "mode": "paper",
            "exchange_segment": "bse_fo",
            "trading_symbol": trading_symbol,
            "transaction_type": "B",
            "order_type": "L",
            "quantity": qty,
            "price": order_price,
            "trigger_price": 0,
            "status": "pending",
            "order_id": f"PAPER-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{signal_id}",
            "notes": f"Paper BUY {signal['strike']} {signal.get('option_type')} @ {entry_low}-{entry_high} [{entry_logic}]",
            "entry_low": entry_low,
            "entry_high": entry_high,
            "strike": signal.get("strike"),
            "option_type": signal.get("option_type"),
            # [6] Store as datetime object — serialized only when sent to DB or WS
            "created_at": datetime.now(timezone.utc),
            "sl_mode": sl_mode,
            "sl_gap": sl_gap,
            "sl_points": float(sl_points),
            "bounce_points": bounce_points,  # [8]
            # [11] Store entry_logic and avg_pick so on_tick can apply correct fill strategy
            "entry_logic": entry_logic,
            "avg_pick": avg_pick,
            # [11] price_side set lazily after 3 confirmed ticks in on_tick() — no CMP needed
            "price_side": None,
            "entry_label": signal.get("entry_label"),  # [12] set by compare mode
            "price_side_candidate": None,
            "price_side_confirm_count": 0,
        }

        trade_id = await db.save_trade(signal_id, order)
        order["trade_id"] = trade_id

        self._pending_orders.append(order)
        log.info(
            f"Paper order pending: {order['trading_symbol']} — "
            f"Entry mode: {entry_logic} — SL mode: {sl_mode} — waiting for LTP"
        )

        if not self.market_feed or not self.market_feed.is_running:
            log.warning("Market feed not running — order will stay pending until feed connects")

        return {"status": "pending", "trade_id": trade_id, "order": order}

    def _symbol_matches(self, order_symbol: str, tick_symbol: str, tick_data: dict) -> bool:
        """Check if a tick's symbol matches an order/position's trading_symbol.

        Kotak Neo sends symbols with expiry dates like 'SENSEX2631278500CE'
        while our orders store simplified symbols like 'SENSEX78500CE'.
        We use fuzzy matching to bridge this gap.
        """
        # [4] re is now imported at module level — no per-call import overhead
        if not order_symbol or not tick_symbol:
            return False

        order_upper = order_symbol.upper().strip()
        tick_upper = tick_symbol.upper().strip()

        if order_upper == tick_upper:
            return True

        order_match = re.match(r'^([A-Z]+?)(\d{5}(?:CE|PE))$', order_upper)
        if order_match:
            idx_prefix = order_match.group(1)
            strike_suffix = order_match.group(2)
            if tick_upper.startswith(idx_prefix) and tick_upper.endswith(strike_suffix):
                return True

        token = str(tick_data.get("tk") or tick_data.get("instrument_token", ""))
        if token and self.market_feed:
            sub = self.market_feed._subscriptions.get(token, {})
            sub_symbol = sub.get("symbol", "")
            if sub_symbol and order_match:
                sub_upper = sub_symbol.upper().strip()
                if sub_upper.startswith(order_match.group(1)) and sub_upper.endswith(order_match.group(2)):
                    return True

        return False

    async def on_tick(self, token: str, ltp: float, data: dict):
        """Called on every market tick — handles pending fills, position updates, and timeouts."""
        now = datetime.now(timezone.utc)
        tick_symbol = data.get("symbol", "")

        # 1. Update Open Positions (Trailing SL, PNL, 10-min timeout)
        for pos in list(self._open_positions):
            # [5] Skip if already closed by a concurrent path (timeout, manual exit, etc.)
            if pos.get("status") == "closed":
                continue

            if not self._symbol_matches(pos["trading_symbol"], tick_symbol, data):
                continue

            opened_at = pos.get("opened_at")
            if opened_at:
                # [6] opened_at is a datetime in memory; handle legacy string defensively
                if isinstance(opened_at, str):
                    opened_at = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
                    if opened_at.tzinfo is None:
                        opened_at = opened_at.replace(tzinfo=timezone.utc)
                    pos["opened_at"] = opened_at  # normalize in-place

                if (now - opened_at) > timedelta(minutes=POSITION_TIMEOUT_MINS):
                    log.warning(f"10-MIN TIMEOUT: Force-selling {pos['trading_symbol']} @ {ltp}")
                    result = await self.close_position(pos["id"], exit_price=ltp)
                    if result.get("status") == "closed":
                        await self._broadcast("new_trade", {
                            **result,
                            "status": "closed",
                            "trading_symbol": pos["trading_symbol"],
                            "reason": "10-min hold timeout",
                        })
                    continue

            await self._process_position_tick(pos, ltp)

        # 2. Check Pending Orders
        filled = []
        expired = []
        for order in self._pending_orders:
            created_at = order.get("created_at")
            # [6] Normalize created_at if it arrived as a string
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                order["created_at"] = created_at

            if created_at and (now - created_at) > timedelta(minutes=ENTRY_TIMEOUT_MINS):
                log.warning(f"10-MIN ENTRY TIMEOUT: Discarding {order['trading_symbol']}")
                await db.update_trade(order["trade_id"], {"status": "expired"})
                expired.append(order)
                await self._broadcast("order_update", {
                    "id": order["trade_id"],
                    "signal_id": order.get("signal_id"),
                    "status": "expired",
                    "status_note": "Entry timeout (10 min)",
                })
                if self._on_trade_expired:
                    self._on_trade_expired(order["trading_symbol"])
                continue

            if not self._symbol_matches(order["trading_symbol"], tick_symbol, data):
                continue

            # [11] Fill strategy depends on entry_logic
            entry_logic = order.get("entry_logic", "code")
            fill_price_target = order.get("price")  # pre-computed at place_order time

            if entry_logic == "code":
                # Bounce-back logic: wait for LTP to dip into range, record the low,
                # then fill only when price recovers by bounce_points — avoids chasing.
                in_range = order["entry_low"] <= ltp <= order["entry_high"]

                if in_range or order.get("min_ltp") is not None:
                    if order.get("min_ltp") is None or ltp < order["min_ltp"]:
                        order["min_ltp"] = ltp
                        await db.update_trade(order["trade_id"], {"min_ltp": ltp})
                        log.info(f"New low for {order['trading_symbol']}: {ltp}")
                        await self._broadcast("order_update", {
                            "id": order["trade_id"],
                            "signal_id": order.get("signal_id"),
                            "min_ltp": ltp,
                            "status_note": f"Tracking bounce from {ltp}",
                        })

                # [8] Use per-order bounce_points (set from strategy at place_order time)
                bounce_points = order.get("bounce_points", DEFAULT_BOUNCE_POINTS)
                if order.get("min_ltp") is not None:
                    if ltp >= order["min_ltp"] + bounce_points:
                        result = await self._fill_order(order, ltp)
                        filled.append(order)
                        log.info(
                            f"Paper order FILLED (Bounce-back): {order['trading_symbol']} "
                            f"@ {ltp} (Min was {order['min_ltp']})"
                        )
                        result["signal_id"] = order.get("signal_id")
                        await self._broadcast("new_trade", result)

            else:
                # [11] Direct touch fill for 'fixed' and 'avg_signal' (high / low / avg).
                # Fill when LTP touches the target from either direction:
                #   price_side='above' (CMP > target at order time) -> fill when ltp drops to target
                #   price_side='below' (CMP < target at order time) -> fill when ltp rises to target
                if fill_price_target:
                    # [11] Confirm price_side over 3 consecutive ticks before activating fill.
                    # Prevents bad side detection from a stale/noisy tick at order creation.
                    CONFIRM_TICKS = 3

                    # Step 1: determine which side of target this tick is on
                    if ltp > fill_price_target:
                        this_side = "above"
                    elif ltp < fill_price_target:
                        this_side = "below"
                    else:
                        this_side = "touch"

                    # Step 2: build confirmation only if price_side not yet locked
                    if order.get("price_side") is None:
                        if this_side == order.get("price_side_candidate"):
                            order["price_side_confirm_count"] += 1
                        else:
                            # New candidate — reset counter
                            order["price_side_candidate"] = this_side
                            order["price_side_confirm_count"] = 1

                        if order["price_side_confirm_count"] >= CONFIRM_TICKS:
                            order["price_side"] = order["price_side_candidate"]
                            log.info(
                                f"price_side locked as [{order['price_side']}] "
                                f"for {order['trading_symbol']} after {CONFIRM_TICKS} ticks"
                            )

                    # Step 3: only fill once price_side is locked
                    price_side = order.get("price_side")
                    if price_side:
                        touched = (
                            price_side == "touch" or
                            (price_side == "above" and ltp <= fill_price_target) or
                            (price_side == "below" and ltp >= fill_price_target)
                        )
                        if touched:
                            result = await self._fill_order(order, ltp)
                            filled.append(order)
                            log.info(
                                f"Paper order FILLED ({entry_logic}/{order.get('avg_pick', '-')}): "
                                f"{order['trading_symbol']} @ {ltp} (Target={fill_price_target}, side={price_side})"
                            )
                            result["signal_id"] = order.get("signal_id")
                            await self._broadcast("new_trade", result)

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
            prev = pos.get("prev_ltp", pos["entry_price"])
            new_sl = prev
            pos["prev_ltp"] = ltp
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp

        elif sl_mode == "signal":
            sl_gap = pos.get("sl_gap") or 30.0
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp
                new_sl = ltp - sl_gap

        elif sl_mode == "fixed":
            sl_points = pos.get("sl_points", 5.0)
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp
                new_sl = ltp - sl_points

        else:  # 'code' — stepped 2pt trailing
            if ltp > pos.get("max_ltp", 0):
                pos["max_ltp"] = ltp
                entry_price = pos["entry_price"]
                steps = int((ltp - entry_price) // 2)
                if steps > 0:
                    initial_sl_offset = max(5.0, 0.03 * entry_price)
                    new_sl = (entry_price - initial_sl_offset) + (steps * 2)

        # Apply new SL — only ever moves UP
        if new_sl is not None:
            current_sl = pos.get("trailing_sl", 0)
            if new_sl > current_sl:
                pos["trailing_sl"] = new_sl
                log.info(f"[{sl_mode}] SL → {new_sl:.2f} for {pos['trading_symbol']}")
                await self._broadcast("position_update", {
                    "id": pos["id"],
                    "trailing_sl": new_sl,
                    "max_ltp": pos.get("max_ltp", ltp),
                    "status_note": f"SL trailed to {new_sl:.2f} [{sl_mode}]",
                })

        # Check SL hit
        if ltp <= pos.get("trailing_sl", 0):
            log.warning(
                f"STOP LOSS HIT [{sl_mode}]: {pos['trading_symbol']} "
                f"@ {ltp} (SL was {pos['trailing_sl']:.2f})"
            )
            result = await self.close_position(pos["id"], exit_price=ltp)
            if result.get("status") == "closed":
                await self._broadcast("new_trade", {
                    **result,
                    "status": "closed",
                    "trading_symbol": pos["trading_symbol"],
                })
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
                "pnl": pos["pnl"],
            })

    async def _fill_order(self, order: dict, fill_price: float) -> dict:
        """Fill a paper order at the given price and set initial SL based on strategy mode."""
        trade_id = order.get("trade_id")
        sl_mode = order.get("sl_mode", "code")
        sl_gap = order.get("sl_gap")
        sl_points = order.get("sl_points", 5.0)

        if sl_mode == "signal" and sl_gap is not None:
            initial_sl = fill_price - sl_gap
            log.info(f"SL mode=signal: gap={sl_gap:.2f}, initial SL={initial_sl:.2f}")
        elif sl_mode == "ltp":
            initial_sl = fill_price
            log.info(f"SL mode=ltp: SL starts at fill price {fill_price}")
        elif sl_mode == "fixed":
            initial_sl = fill_price - sl_points
            log.info(f"SL mode=fixed ({sl_points}pts): initial SL={initial_sl:.2f}")
        else:  # 'code'
            sl_points_code = max(5.0, 0.03 * fill_price)
            initial_sl = fill_price - sl_points_code
            log.info(f"SL mode=code: initial SL={initial_sl:.2f}")

        await db.update_trade(trade_id, {
            "status": "filled",
            "fill_price": fill_price,
            "fill_time": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        })

        now_utc = datetime.now(timezone.utc)

        pos_data = {
            "mode": "paper",
            "trading_symbol": order["trading_symbol"],
            "strike": order.get("strike"),
            "option_type": order.get("option_type"),
            "quantity": order["quantity"],
            "entry_price": fill_price,
            "max_ltp": fill_price,
            "trailing_sl": initial_sl,
            "sl_mode": sl_mode,
            "sl_gap": sl_gap,
            "sl_points": sl_points,
            "prev_ltp": fill_price,
        }
        position_id = await db.save_position(trade_id, pos_data)

        position = {
            **pos_data,
            "id": position_id,
            "trade_id": trade_id,
            "current_price": fill_price,
            "pnl": 0,
            # [6] Store as datetime object in memory — serialized to ISO only for DB/WS
            "opened_at": now_utc,
            "status": "open",
        }

        self._open_positions.append(position)
        log.info(f"Position opened | mode={sl_mode} | Entry={fill_price} | Initial SL={initial_sl:.2f}")

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
            # [6] Serialize opened_at to ISO string for WS broadcast only
            **{**position, "opened_at": now_utc.isoformat().replace("+00:00", "Z")},
        }

    async def close_position(self, position_id: int, exit_price: float = None) -> dict:
        """Close a paper position and broadcast position_update to the frontend."""
        for pos in self._open_positions:
            if pos["id"] == position_id:
                # [5] Mark closed immediately before any await to prevent double-close races
                if pos.get("status") == "closed":
                    return {"status": "error", "message": "Already closed"}
                pos["status"] = "closed"

                price = exit_price or pos.get("current_price", pos["entry_price"])
                pnl = (price - pos["entry_price"]) * pos["quantity"]

                await db.update_position(position_id, {
                    "status": "closed",
                    "current_price": price,
                    "pnl": pnl,
                    "closed_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                })
                await db.update_trade(pos["trade_id"], {"pnl": pnl, "exit_price": price})

                # [7] Remove after DB writes to avoid race with square_off_all snapshot
                if pos in self._open_positions:
                    self._open_positions.remove(pos)

                # [3] Broadcast position_update so frontend removes the card via WS event
                await self._broadcast("position_update", {
                    "id": position_id,
                    "status": "closed",
                    "pnl": pnl,
                    "exit_price": price,
                })

                return {"status": "closed", "pnl": pnl, "exit_price": price}

        return {"status": "error", "message": "Position not found"}

    async def rehydrate_from_db(self):
        """Restore pending orders and open positions from the database.
        Called on startup so the paper trader survives server restarts.
        """
        # Restore pending orders
        pending_trades = await db.get_trades(mode="paper")
        restored_orders = 0
        for t in pending_trades:
            if t.get("status") == "pending":
                order = {
                    "trade_id": t["id"],
                    "signal_id": t.get("signal_id"),
                    "mode": "paper",
                    "exchange_segment": t.get("exchange_segment", "bse_fo"),
                    "trading_symbol": t.get("trading_symbol", ""),
                    "transaction_type": t.get("transaction_type", "B"),
                    "order_type": t.get("order_type", "L"),
                    "quantity": t.get("quantity", 0),
                    "price": t.get("price", 0),
                    "trigger_price": t.get("trigger_price", 0),
                    "status": "pending",
                    "order_id": t.get("order_id", ""),
                    "notes": t.get("notes", ""),
                    # [13] Parse entry_low/entry_high from notes: "@ {low}-{high} [{logic}]"
                    "entry_low": t.get("price", 0),   # overridden below if notes parse succeeds
                    "entry_high": t.get("price", 0),  # overridden below if notes parse succeeds
                    "strike": "",
                    "option_type": "",
                    "created_at": t.get("created_at", ""),
                    "min_ltp": t.get("min_ltp"),
                    "sl_mode": "code",
                    "sl_gap": None,
                    "sl_points": 5.0,
                    "bounce_points": DEFAULT_BOUNCE_POINTS,
                    # [12] Rehydrated orders restore entry_logic from notes field
                    # notes format: "Paper BUY {strike} {option_type} @ {low}-{high} [{entry_logic}]"
                    "entry_logic": "code",   # default, overridden below
                    "avg_pick": "avg",
                    "entry_label": t.get("entry_label"),
                    "price_side": None,
                    "price_side_candidate": None,
                    "price_side_confirm_count": 0,
                }
                # Extract entry_logic from notes e.g. "[avg_signal]" or "[code]" or "[fixed]"
                notes_str = t.get("notes", "")
                # [13] Restore entry range from notes e.g. "@ 295-300 [code]"
                range_match = re.search(r'@ ([\d.]+)-([\d.]+)', notes_str)
                if range_match:
                    order["entry_low"] = float(range_match.group(1))
                    order["entry_high"] = float(range_match.group(2))
                    log.info(f"Rehydrated entry range for trade {t['id']}: {order['entry_low']}-{order['entry_high']}")
                notes_match = re.search(r'\[([a-z_]+)\]', notes_str)
                if notes_match:
                    parsed_logic = notes_match.group(1)
                    if parsed_logic in ("code", "avg_signal", "fixed"):
                        order["entry_logic"] = parsed_logic
                # Extract avg_pick from entry_label e.g. "high", "low", "avg"
                label = t.get("entry_label", "")
                if label in ("high", "low", "avg"):
                    order["avg_pick"] = label
                    order["entry_logic"] = "avg_signal"
                # Extract strike/option_type from trading_symbol (e.g. SENSEX78500CE)
                sym = t.get("trading_symbol", "")
                match = re.match(r'^[A-Z]+?(\d{5})(CE|PE)$', sym.upper())
                if match:
                    order["strike"] = match.group(1)
                    order["option_type"] = match.group(2)
                self._pending_orders.append(order)
                restored_orders += 1

        # Restore open positions
        open_positions = await db.get_positions(mode="paper", status="open")
        restored_positions = 0
        for p in open_positions:
            pos = {
                "id": p["id"],
                "trade_id": p.get("trade_id"),
                "mode": "paper",
                "trading_symbol": p.get("trading_symbol", ""),
                "strike": p.get("strike", ""),
                "option_type": p.get("option_type", ""),
                "quantity": p.get("quantity", 0),
                "entry_price": p.get("entry_price", 0),
                "current_price": p.get("current_price", 0),
                "pnl": p.get("pnl", 0),
                "max_ltp": p.get("max_ltp", 0),
                "trailing_sl": p.get("trailing_sl", 0),
                "status": "open",
                "opened_at": p.get("opened_at", ""),
                "sl_mode": "code",
                "sl_gap": None,
                "sl_points": 5.0,
                "prev_ltp": p.get("current_price", p.get("entry_price", 0)),
            }
            self._open_positions.append(pos)
            restored_positions += 1

        if restored_orders or restored_positions:
            log.info(
                f"Rehydrated from DB: {restored_orders} pending orders, "
                f"{restored_positions} open positions"
            )

    def get_pending_orders(self) -> list[dict]:
        return list(self._pending_orders)

    def get_open_positions(self) -> list[dict]:
        return list(self._open_positions)

    def get_pnl_summary(self) -> dict:
        # Safe in single-threaded asyncio — no concurrent mutation between awaits here.
        # [10] If multi-threading is ever introduced, this will need a lock.
        total_pnl = sum(p.get("pnl", 0) for p in self._open_positions)
        return {
            "open_positions": len(self._open_positions),
            "pending_orders": len(self._pending_orders),
            "total_unrealized_pnl": total_pnl,
        }

    async def square_off_all(self) -> dict:
        """Close all open positions at current price and cancel all pending orders."""
        results = []

        # [7] Iterate a snapshot — close_position mutates _open_positions internally
        for pos in list(self._open_positions):
            if pos.get("status") == "closed":
                continue
            price = pos.get("current_price", pos.get("entry_price", 0))
            result = await self.close_position(pos["id"], exit_price=price)
            if result.get("status") == "closed":
                results.append({
                    "position_id": pos["id"],
                    "symbol": pos.get("trading_symbol"),
                    **result,
                })
                await self._broadcast("new_trade", {
                    **result,
                    "status": "closed",
                    "trading_symbol": pos.get("trading_symbol"),
                    "reason": "Kill switch",
                })

        # Cancel all pending orders
        cancelled = 0
        for order in list(self._pending_orders):
            await db.update_trade(order["trade_id"], {"status": "cancelled"})
            cancelled += 1
            await self._broadcast("order_update", {
                "id": order["trade_id"],
                "signal_id": order.get("signal_id"),
                "status": "cancelled",
                "status_note": "Cancelled by kill switch",
            })
        self._pending_orders.clear()

        log.info(f"KILL SWITCH: Closed {len(results)} positions, cancelled {cancelled} pending orders")
        return {
            "status": "ok",
            "positions_closed": len(results),
            "orders_cancelled": cancelled,
            "results": results,
        }