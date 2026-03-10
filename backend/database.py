"""
Database module — SQLite via aiosqlite for async trade/message storage.
"""
import aiosqlite
import json
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "trades.db")
TICKS_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ticks.db")


async def init_db():
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL DEFAULT 'telegram',
                raw_text TEXT NOT NULL,
                sender TEXT,
                timestamp TEXT NOT NULL,
                parsed INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                status TEXT NOT NULL,
                reason TEXT,
                idx TEXT,
                strike TEXT,
                option_type TEXT,
                entry_low REAL,
                entry_high REAL,
                diff REAL,
                stoploss REAL,
                targets TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (message_id) REFERENCES messages(id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                mode TEXT NOT NULL DEFAULT 'paper',
                exchange_segment TEXT,
                trading_symbol TEXT,
                transaction_type TEXT,
                order_type TEXT,
                quantity INTEGER,
                price REAL,
                trigger_price REAL,
                status TEXT NOT NULL DEFAULT 'pending',
                order_id TEXT,
                fill_price REAL,
                fill_time TEXT,
                pnl REAL DEFAULT 0,
                min_ltp REAL,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            );

            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                mode TEXT NOT NULL DEFAULT 'paper',
                trading_symbol TEXT,
                strike TEXT,
                option_type TEXT,
                quantity INTEGER,
                entry_price REAL,
                current_price REAL DEFAULT 0,
                pnl REAL DEFAULT 0,
                max_ltp REAL DEFAULT 0,
                trailing_sl REAL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'open',
                opened_at TEXT NOT NULL DEFAULT (datetime('now')),
                closed_at TEXT,
                FOREIGN KEY (trade_id) REFERENCES trades(id)
            );
        """)
        await db.commit()

    async with aiosqlite.connect(TICKS_DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_token TEXT,
                symbol TEXT,
                ltp REAL NOT NULL,
                volume INTEGER DEFAULT 0,
                open REAL DEFAULT 0,
                high REAL DEFAULT 0,
                low REAL DEFAULT 0,
                close REAL DEFAULT 0,
                timestamp TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
        await db.commit()

async def clear_all_data():
    """Clear all UI-visible data (trades, signals, messages, positions)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM positions")
        await db.execute("DELETE FROM trades")
        await db.execute("DELETE FROM signals")
        await db.execute("DELETE FROM messages")
        await db.execute("DELETE FROM sqlite_sequence WHERE name IN ('positions', 'trades', 'signals', 'messages')")
        await db.commit()


# ── Message CRUD ──

async def save_message(raw_text: str, sender: str = "", source: str = "telegram") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO messages (source, raw_text, sender, timestamp) VALUES (?, ?, ?, ?)",
            (source, raw_text, sender, datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')),
        )
        await db.commit()
        return cursor.lastrowid


async def get_messages(limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM messages ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ── Signal CRUD ──

async def save_signal(message_id: int, parsed: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        # Get values once to ensure order matches placeholders
        status = parsed.get("status")
        reason = parsed.get("reason")
        idx = parsed.get("index")
        strike = parsed.get("strike")
        option_type = parsed.get("option_type")
        entry_low = parsed.get("entry_low")
        entry_high = parsed.get("entry_high")
        diff = parsed.get("diff")
        stoploss = parsed.get("stoploss")
        targets = json.dumps(parsed.get("targets")) if parsed.get("targets") else None

        cursor = await db.execute(
            """INSERT INTO signals
               (message_id, status, reason, idx, strike, option_type, entry_low, entry_high, diff, stoploss, targets)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, status, reason, idx, strike, option_type, entry_low, entry_high, diff, stoploss, targets),
        )
        # Mark message as parsed
        await db.execute("UPDATE messages SET parsed = 1 WHERE id = ?", (message_id,))
        await db.commit()
        return cursor.lastrowid


async def get_signals(limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ── Trade CRUD ──

async def save_trade(signal_id: int, trade_data: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO trades
               (signal_id, mode, exchange_segment, trading_symbol, transaction_type,
                order_type, quantity, price, trigger_price, status, order_id, min_ltp, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                signal_id,
                trade_data.get("mode", "paper"),
                trade_data.get("exchange_segment"),
                trade_data.get("trading_symbol"),
                trade_data.get("transaction_type", "B"),
                trade_data.get("order_type", "L"),
                trade_data.get("quantity"),
                trade_data.get("price"),
                trade_data.get("trigger_price", 0),
                trade_data.get("status", "pending"),
                trade_data.get("order_id"),
                trade_data.get("min_ltp"),
                trade_data.get("notes"),
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def update_trade(trade_id: int, updates: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        set_clauses = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [trade_id]
        await db.execute(f"UPDATE trades SET {set_clauses} WHERE id = ?", values)
        await db.commit()


async def get_trades(mode: str = None, limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if mode:
            cursor = await db.execute(
                "SELECT * FROM trades WHERE mode = ? ORDER BY id DESC LIMIT ?",
                (mode, limit),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ── Position CRUD ──

async def save_position(trade_id: int, pos_data: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO positions
               (trade_id, mode, trading_symbol, strike, option_type,
                quantity, entry_price, max_ltp, trailing_sl, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trade_id,
                pos_data.get("mode", "paper"),
                pos_data.get("trading_symbol"),
                pos_data.get("strike"),
                pos_data.get("option_type"),
                pos_data.get("quantity"),
                pos_data.get("entry_price"),
                pos_data.get("max_ltp", pos_data.get("entry_price", 0)),
                pos_data.get("trailing_sl", 0),
                "open",
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def update_position(position_id: int, updates: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        set_clauses = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [position_id]
        await db.execute(f"UPDATE positions SET {set_clauses} WHERE id = ?", values)
        await db.commit()


async def get_positions(mode: str = None, status: str = "open") -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        conditions = []
        params = []
        if mode:
            conditions.append("mode = ?")
            params.append(mode)
        if status:
            conditions.append("status = ?")
            params.append(status)

        where = " AND ".join(conditions) if conditions else "1=1"
        cursor = await db.execute(
            f"SELECT * FROM positions WHERE {where} ORDER BY id DESC", params
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ── Tick Storage (for backtesting) ──

async def save_tick(tick_data: dict):
    """Save a single tick to the database."""
    async with aiosqlite.connect(TICKS_DB_PATH) as db:
        await db.execute(
            """INSERT INTO ticks (instrument_token, symbol, ltp, volume, open, high, low, close, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                tick_data.get("instrument_token", ""),
                tick_data.get("symbol", ""),
                tick_data.get("ltp", 0),
                tick_data.get("volume", 0),
                tick_data.get("open", 0),
                tick_data.get("high", 0),
                tick_data.get("low", 0),
                tick_data.get("close", 0),
                tick_data.get("timestamp", datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')),
            ),
        )
        await db.commit()


async def save_ticks_batch(ticks: list[dict]):
    """Save a batch of ticks efficiently."""
    if not ticks:
        return
    async with aiosqlite.connect(TICKS_DB_PATH) as db:
        await db.executemany(
            """INSERT INTO ticks (instrument_token, symbol, ltp, volume, open, high, low, close, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    t.get("instrument_token", ""),
                    t.get("symbol", ""),
                    t.get("ltp", 0),
                    t.get("volume", 0),
                    t.get("open", 0),
                    t.get("high", 0),
                    t.get("low", 0),
                    t.get("close", 0),
                    t.get("timestamp", datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')),
                )
                for t in ticks
            ],
        )
        await db.commit()
