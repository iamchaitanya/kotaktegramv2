/**
 * TradeBridge — Frontend Application
 * Connects to FastAPI backend via REST + WebSocket
 * Renders real-time trading dashboard
 *
 * PATCHES APPLIED:
 *  [1] Strategy now syncs from server on init; localStorage used only as fallback
 *  [2] renderMessages() ordering fixed — fragment built in correct order
 *  [3] renderPositions() upserts instead of full innerHTML rebuild (fixes timer flicker)
 *  [4] Timer flicker eliminated by [3]
 *  [5] exitPosition() no longer optimistically removes — waits for position_update WS event
 *  [6] esc() reuses a single cached DOM element instead of creating one per call
 *  [7] pingInterval cleared correctly on reconnect via ws._pingInterval
 *  [8] state.signals sorted by created_at before rendering
 *  [9] Duplicate position insertion guarded correctly using position_id
 * [10] renderTrades() debounced at 100ms
 * [11] Modal overlay close uses event delegation on document instead of per-modal binding
 * [12] compareMode added — spawns all 5 entry strategies simultaneously for comparison
 * [13] new_trade case indentation fixed — illegal break statement resolved
 * [FIX #20] loadStrategy() fetches from server first; localStorage only as offline fallback
 * [FIX #22] renderSignals() upserts by ID — no insertBefore on every render, timers no longer stutter
 * [FIX #24] entryTimerMins, exitTimerMins, signalTrailInitialSL, signalTrailInitialSLPoints added
 */

// ── Globals ──────────────────────────────────────────────────────────────────
const API_BASE = window.location.origin;
const WS_URL   = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws`;

let ws             = null;
let reconnectTimer = null;

const state = {
    messages:     [],
    signals:      [],
    trades:       [],
    positions:    [],
    mode:         'paper',
    lotSize:      1,
    strategy:     {},
    tradeFilter:  'all',
    selectedDate: null,
    wsConnected:  false,
    sensex_ltp:   0,
    stopTrading:  false,
};

// ── DOM Helpers ───────────────────────────────────────────────────────────────
const $  = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);
const esc = (str) => String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

// ── Timer tick (runs every second for countdown tags) ─────────────────────────
let timerTickInterval = null;
function startTimerTick() {
    if (timerTickInterval) return;
    timerTickInterval = setInterval(() => {
        document.querySelectorAll('.timer-tag').forEach(el => {
            const start       = el.dataset.timerStart;
            const mins        = parseFloat(el.dataset.timerMins  || 10);
            const label       = el.dataset.timerLabel            || '';
            const tradeStatus = el.dataset.tradeStatus           || '';
            if (tradeStatus) {
                // Any trade action taken — hide the timer regardless of specific status.
                // This includes 'pending' — once a trade is placed the entry timer
                // is no longer meaningful (the order is already being tracked).
                el.style.display = 'none';
                return;
            }
            const remaining = getCountdown(start, mins);
            if (remaining === null) {
                el.textContent  = `⌛ ${label}: expired`;
                el.style.opacity = '0.5';
            } else {
                el.textContent  = `⏳ ${label}: ${remaining}`;
                el.style.opacity = '1';
            }
        });
    }, 1000);
}

// ── Debounced renderTrades ────────────────────────────────────────────────────
let renderTradesTimer = null;
function renderTradesDebounced() {
    clearTimeout(renderTradesTimer);
    renderTradesTimer = setTimeout(renderTrades, 150);
}

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    loadStrategy();
    fetchInitialData();
    connectWebSocket();
    startTimerTick();
    bindEventListeners();
});

// ── Event Listeners ───────────────────────────────────────────────────────────
function bindEventListeners() {
    const hamburger = $('#btn-hamburger');
    const menu      = $('#header-menu');
    if (hamburger && menu) {
        hamburger.addEventListener('click', () => menu.classList.toggle('open'));
        document.addEventListener('click', (e) => {
            if (!hamburger.contains(e.target) && !menu.contains(e.target)) {
                menu.classList.remove('open');
            }
        });
    }

    $$('.mode-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            if (btn.dataset.mode === 'real') {
                $('#confirm-real-modal').style.display = 'flex';
            } else {
                setMode(btn.dataset.mode);
            }
        });
    });

    $('#btn-confirm-real')?.addEventListener('click', () => {
        setMode('real');
        $('#confirm-real-modal').style.display = 'none';
    });
    $('#btn-cancel-real')?.addEventListener('click', () => {
        $('#confirm-real-modal').style.display = 'none';
    });

    $('#btn-clear')?.addEventListener('click', () => {
        // Reset modal state — no selection, confirm disabled
        $$('input[name="clear-scope"]').forEach(r => r.checked = false);
        const dateRow = $('#clear-date-row');
        if (dateRow) dateRow.style.display = 'none';
        const confirmBtn = $('#btn-confirm-clear');
        if (confirmBtn) confirmBtn.disabled = true;
        const dateInput = $('#clear-date-input');
        if (dateInput) dateInput.value = '';
        $('#clear-modal').style.display = 'flex';
    });

    $('#btn-kill')?.addEventListener('click', async () => {
        if (!confirm('⚠️ KILL SWITCH\n\nThis will:\n• Close ALL open positions at current price\n• Cancel ALL pending orders\n\nAre you sure?')) return;
        try {
            const res  = await fetch(`${API_BASE}/api/kill`, { method: 'POST' });
            const data = await res.json();
            toast(`Killed: ${data.positions_closed} positions closed, ${data.orders_cancelled} orders cancelled`, 'warning');
        } catch {
            toast('Kill switch failed', 'error');
        }
    });

    $('#btn-set-lots')?.addEventListener('click', setLotSize);
    $('#lot-input')?.addEventListener('keydown', (e) => { if (e.key === 'Enter') setLotSize(); });

    $('#btn-settings')?.addEventListener('click', () => {
        $('#settings-modal').style.display = 'flex';
    });
    $('#btn-close-settings')?.addEventListener('click', () => {
        $('#settings-modal').style.display = 'none';
    });

    $('#btn-kotak-login')?.addEventListener('click', kotakLogin);
    $('#btn-submit-otp')?.addEventListener('click', submitOTP);
    $('#btn-send-test')?.addEventListener('click', sendTestSignal);

    const tradeDatePicker = $('#trade-date-picker');
    if (tradeDatePicker) {
        tradeDatePicker.addEventListener('change', () => {
            if (tradeDatePicker.value) loadByDate(tradeDatePicker.value);
        });
    }
    $('#btn-load-all-trades')?.addEventListener('click', () => {
        const picker = $('#trade-date-picker');
        if (picker) picker.value = '';
        loadByDate('all');
    });

    $$('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            $$('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.tradeFilter = btn.dataset.filter;
            renderTrades();
        });
    });

    // [11] Modal overlay close via event delegation
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal-overlay')) {
            e.target.style.display = 'none';
        }
    });

    bindStrategyModal();
    bindClearModal();
    bindStopTrading();
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connectWebSocket() {
    if (ws && ws.readyState === WebSocket.OPEN) return;
    ws = new WebSocket(WS_URL);

    ws._pingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping' }));
        }
    }, 20000);

    ws.onopen = () => {
        state.wsConnected = true;
        updateBadge('badge-ws', true);
        toast('Connected to server', 'success');
        if (reconnectTimer) { clearInterval(reconnectTimer); reconnectTimer = null; }
    };

    ws.onmessage = (event) => {
        try {
            handleWSMessage(JSON.parse(event.data));
        } catch (e) {
            console.error('WS parse error:', e);
        }
    };

    ws.onclose = () => {
        state.wsConnected = false;
        clearInterval(ws._pingInterval);
        updateBadge('badge-ws', false);
        if (!reconnectTimer) {
            reconnectTimer = setInterval(() => {
                console.log('Reconnecting WebSocket...');
                connectWebSocket();
            }, 5000);
        }
    };

    ws.onerror = (err) => console.error('WS error:', err);
}

function handleWSMessage(msg) {
    try {
        if (!msg) return;
        if (msg.type !== 'instrument_ltp' && msg.type !== 'index_ltp') {
            console.log('WS Received:', msg.type, msg.data);
        }

        switch (msg.type) {
            case 'init':
                if (msg.data.trades) {
                    const existingTradeIds = new Set(state.trades.map(t => t.id || t.trade_id));
                    const newTrades = (msg.data.trades || []).filter(t => !existingTradeIds.has(t.id || t.trade_id));
                    state.trades = [...state.trades, ...newTrades];
                }
                if (msg.data.positions) state.positions = msg.data.positions;
                if (msg.data.messages) {
                    const existingMsgIds = new Set(state.messages.map(m => m.id));
                    const newMsgs = (msg.data.messages || []).filter(m => !existingMsgIds.has(m.id));
                    state.messages = [...newMsgs, ...state.messages];
                }
                if (msg.data.signals) {
                    const existingSigIds = new Set(state.signals.map(s => s.id));
                    const newSigs = (msg.data.signals || []).filter(s => !existingSigIds.has(s.id));
                    state.signals = [...newSigs, ...state.signals];
                }
                updateStatusFromData(msg.data.status);
                if (msg.data.strategy) {
                    state.strategy = { ...STRATEGY_DEFAULTS, ...msg.data.strategy };
                    persistStrategy();
                }
                if (msg.data.status?.stop_trading != null) {
                    state.stopTrading = msg.data.status.stop_trading;
                    updateStopTradingUI();
                }
                // Derive after ALL data (trades + signals) is merged
                deriveSignalTradeStatuses();
                renderAll();
                break;

            case 'new_message':
                state.messages.unshift(msg.data);
                renderMessages();
                break;

            case 'new_signal':
                console.log('Adding new signal:', msg.data);
                if (msg.data.strike && msg.data.option_type) {
                    const existingIdx = state.signals.findIndex(s =>
                        String(s.strike) === String(msg.data.strike) &&
                        String(s.option_type).toUpperCase() === String(msg.data.option_type).toUpperCase() &&
                        !['filled', 'closed', 'expired', 'replaced'].includes(s.trade_status)
                    );
                    if (existingIdx !== -1) {
                        state.signals[existingIdx].trade_status = 'replaced';
                        state.signals[existingIdx].status_note  = 'Replaced by newer signal';
                    }
                }
                state.signals.unshift(msg.data);
                renderSignals();
                if (msg.data.status === 'valid') {
                    toast(`Signal: SENSEX ${msg.data.strike} ${msg.data.option_type} @ ${msg.data.entry_low}-${msg.data.entry_high}`, 'info');
                }
                break;

            case 'new_trade':
                if (msg.data) {
                    if (msg.data.status === 'closed') break;

                    const tradesToAdd = msg.data.compare_mode && Array.isArray(msg.data.variants)
                        ? msg.data.variants.map(v => ({ ...(v.order || {}), ...v }))
                        : [msg.data];

                    tradesToAdd.forEach(t => {
                        if (!t || t.status === 'closed') return;
                        // Guard: skip rows with no meaningful trade data — these are
                        // wrapper objects from the broadcast spread (no symbol = ghost row)
                        if (!t.trading_symbol && !t.trade_id && !t.id) return;
                        const tid = t.trade_id || t.id;
                        const idx = state.trades.findIndex(x => (x.trade_id || x.id) === tid);
                        if (idx !== -1) {
                            state.trades[idx] = { ...state.trades[idx], ...t };
                        } else {
                            state.trades.unshift(t);
                        }
                    });

                    const primaryTrade = tradesToAdd[0] || msg.data;

                    if (primaryTrade.status === 'open') {
                        const posId = primaryTrade.position_id;
                        if (posId && !state.positions.some(p => p.position_id === posId || p.id === posId)) {
                            const entryPrice = msg.data.entry_price || msg.data.fill_price;
                            if (!entryPrice) console.warn('[new_trade] No entry/fill price on trade:', primaryTrade);
                            state.positions.unshift({ ...primaryTrade, id: posId, entry_price: entryPrice || 0 });
                            renderPositions();
                        }
                        if (primaryTrade.signal_id) {
                            const sigIdx = state.signals.findIndex(s => s.id === primaryTrade.signal_id);
                            if (sigIdx !== -1) {
                                state.signals[sigIdx].trade_status = 'filled';
                                state.signals[sigIdx].status_note  = `Filled @ ₹${(msg.data.entry_price || msg.data.fill_price || 0).toFixed(2)}`;
                                renderSignals();
                            }
                        }
                    } else if (primaryTrade.status === 'pending' && primaryTrade.signal_id) {
                        const sigIdx = state.signals.findIndex(s => s.id === primaryTrade.signal_id);
                        if (sigIdx !== -1) {
                            state.signals[sigIdx].trade_status = 'pending';
                            renderSignals();
                        }
                    }

                    renderTradesDebounced();
                    const tradeStatus = primaryTrade.status || 'pending';
                    toast(`Trade ${tradeStatus}: ${primaryTrade.trading_symbol || msg.data.trading_symbol || ''}`, tradeStatus === 'filled' ? 'success' : 'info');
                }
                break;

            case 'mode_change':
                state.mode = msg.data.new_mode;
                updateModeUI();
                toast(`Mode: ${msg.data.new_mode.toUpperCase()}`, 'warning');
                break;

            case 'order_update': {
                const upd = msg.data;
                if (upd.signal_id) {
                    const sigIdx = state.signals.findIndex(s => s.id === upd.signal_id);
                    if (sigIdx !== -1) {
                        if (upd.status)      state.signals[sigIdx].trade_status = upd.status;
                        if (upd.status_note) state.signals[sigIdx].status_note  = upd.status_note;
                        if (upd.min_ltp)     state.signals[sigIdx].min_ltp      = upd.min_ltp;
                        renderSignals();
                    }
                }
                const trdIdx = state.trades.findIndex(t => t.id === upd.id || t.trade_id === upd.id);
                if (trdIdx !== -1) {
                    state.trades[trdIdx] = { ...state.trades[trdIdx], ...upd };
                    renderTradesDebounced();
                }
                if (upd.status === 'replaced') {
                    toast(`Order replaced: ${upd.trading_symbol || 'trade #' + upd.id}`, 'info');
                }
                break;
            }

            case 'instrument_ltp':
                if (msg.data.symbol) {
                    const incomingSymbol = msg.data.symbol.toUpperCase();
                    state.signals.forEach(s => {
                        const idxStr    = (s.idx || s.index || 'SENSEX').toUpperCase().replace(/\s/g, '');
                        const suffixStr = `${s.strike}${s.option_type}`.toUpperCase().replace(/\s/g, '');
                        if (incomingSymbol.startsWith(idxStr) && incomingSymbol.endsWith(suffixStr)) {
                            s.live_ltp = msg.data.ltp;
                            const el = document.getElementById(`signal-ltp-${s.id}`);
                            if (el) el.textContent = `₹${s.live_ltp.toFixed(2)}`;
                        }
                    });
                }
                break;

            case 'index_ltp':
                state.sensex_ltp = msg.data.ltp || 0;
                document.querySelectorAll('.signal-sensex-ltp').forEach(el => {
                    el.textContent = (state.sensex_ltp || 0).toFixed(2);
                });
                break;

            case 'position_update': {
                const posIdx = state.positions.findIndex(p => p.id === msg.data.id);
                if (posIdx !== -1) {
                    state.positions[posIdx] = { ...state.positions[posIdx], ...msg.data };
                } else if (msg.data.status === 'open') {
                    state.positions.unshift(msg.data);
                }
                if (msg.data.status === 'closed') {
                    state.positions = state.positions.filter(p => p.id !== msg.data.id);
                }
                renderPositions();
                break;
            }

            case 'settings_update':
                if (msg.data.lot_size != null) {
                    state.lotSize = msg.data.lot_size;
                    const lotInput = $('#lot-input');
                    if (lotInput) lotInput.value = msg.data.lot_size;
                    toast(`Lot size updated to ${msg.data.lot_size}`, 'info');
                }
                break;

            case 'stop_trading_update':
                state.stopTrading = !!msg.data.enabled;
                updateStopTradingUI();
                toast(state.stopTrading ? '⏸ Trading STOPPED — signals ignored' : '▶ Trading RESUMED', state.stopTrading ? 'warning' : 'success');
                break;

            case 'pong':
                break;

            default:
                console.log('Unknown WS message:', msg);
        }
    } catch (err) {
        console.error('Error handling WS message:', err, msg);
    }
}

// ── REST API Calls ────────────────────────────────────────────────────────────
async function fetchInitialData() {
    try {
        const today = new Date().toLocaleDateString('en-CA');
        const [statusRes, msgsRes, sigsRes, tradesRes, posRes] = await Promise.all([
            fetch(`${API_BASE}/api/status`),
            fetch(`${API_BASE}/api/messages?date=all&limit=200`),
            fetch(`${API_BASE}/api/signals?date=all&limit=200`),
            fetch(`${API_BASE}/api/trades?date=${today}&limit=200`),
            fetch(`${API_BASE}/api/positions`),
        ]);

        if (statusRes.ok) {
            const status = await statusRes.json();
            updateStatusFromData(status);
            if (status.lot_size) {
                const lotInput = $('#lot-input');
                if (lotInput) lotInput.value = status.lot_size;
            }
            if (status.strategy) {
                state.strategy = { ...STRATEGY_DEFAULTS, ...status.strategy };
                persistStrategy();
            }
            if (status.stop_trading != null) {
                state.stopTrading = status.stop_trading;
                updateStopTradingUI();
            }
        }
        if (msgsRes.ok)   { state.messages = await msgsRes.json(); state.messages.reverse(); }
        if (sigsRes.ok) {
            state.signals = await sigsRes.json();
            state.signals.forEach(s => { if (s.last_ltp) s.live_ltp = s.last_ltp; });
        }
        if (tradesRes.ok) state.trades = await tradesRes.json();
        // Derive signal trade statuses AFTER both signals and trades are loaded
        deriveSignalTradeStatuses();

        if (!state.selectedDate) {
            state.selectedDate = today;
            const picker = $('#trade-date-picker');
            if (picker) picker.value = today;
        }
        if (posRes.ok) state.positions = await posRes.json();

        renderAll();
    } catch (e) {
        console.error('Failed to fetch initial data:', e);
    }
}

async function setMode(mode) {
    try {
        const res  = await fetch(`${API_BASE}/api/mode`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const data = await res.json();
        if (data.status === 'ok') {
            state.mode = mode;
            updateModeUI();
            toast(`Switched to ${mode.toUpperCase()} mode`, mode === 'real' ? 'warning' : 'success');
        } else {
            toast(data.message || 'Failed to switch mode', 'error');
        }
    } catch { toast('Failed to switch mode', 'error'); }
}

async function kotakLogin() {
    try {
        const res  = await fetch(`${API_BASE}/api/auth/login`, { method: 'POST' });
        const data = await res.json();
        if (data.status === 'ok') {
            $('#otp-row').style.display       = 'none';
            $('#kotak-auth-status').textContent = '✅ Authenticated';
            updateBadge('badge-kotak', true);
            toast('Kotak Neo authenticated automatically!', 'success');
        } else {
            $('#kotak-auth-status').textContent = `Error: ${data.message}`;
            toast(data.message, 'error');
        }
    } catch { toast('Login failed', 'error'); }
}

async function submitOTP() {
    const otp = $('#otp-input').value.trim();
    try {
        const res  = await fetch(`${API_BASE}/api/auth/2fa`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ otp: otp || null }),
        });
        const data = await res.json();
        if (data.status === 'ok') {
            $('#otp-row').style.display       = 'none';
            $('#kotak-auth-status').textContent = '✅ Authenticated';
            updateBadge('badge-kotak', true);
            toast('Kotak Neo authenticated!', 'success');
        } else { toast(data.message, 'error'); }
    } catch { toast('2FA failed', 'error'); }
}

async function sendTestSignal() {
    const text = $('#test-signal-input').value.trim();
    if (!text) return toast('Enter a signal message', 'warning');
    try {
        const res    = await fetch(`${API_BASE}/api/test-signal`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text, sender: 'Test' }),
        });
        const result = await res.json();
        if (result.signal?.status === 'valid') {
            toast('Test signal sent and validated!', 'success');
        } else if (result.signal?.status === 'ignored') {
            toast(`Signal ignored: ${result.signal.reason}`, 'warning');
        } else {
            toast('Test signal sent', 'success');
        }
        $('#test-signal-input').value = '';
    } catch { toast('Failed to send test', 'error'); }
}

async function exitPosition(positionId) {
    if (!confirm('Exit this position at current price?')) return;
    try {
        const res  = await fetch(`${API_BASE}/api/positions/${positionId}/exit`, { method: 'POST' });
        const data = await res.json();
        if (data.status === 'closed' || data.status === 'ok') {
            toast('Exit submitted — awaiting confirmation', 'info');
        } else {
            toast(data.message || 'Failed to exit position', 'error');
        }
    } catch { toast('Exit failed', 'error'); }
}

async function setLotSize() {
    const lots = parseInt($('#lot-input').value);
    if (!lots || lots < 1) return toast('Lot size must be at least 1', 'warning');
    try {
        const res  = await fetch(`${API_BASE}/api/settings/lot-size`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ lots }),
        });
        const data = await res.json();
        if (data.status === 'ok') toast(`Lot size set to ${data.lot_size}`, 'success');
        else toast('Failed to set lot size', 'error');
    } catch { toast('Failed to set lot size', 'error'); }
}

// ── Rendering ─────────────────────────────────────────────────────────────────
async function loadByDate(date) {
    try {
        const dateParam = date === 'all' ? 'all' : date;
        const [msgsRes, sigsRes, tradesRes] = await Promise.all([
            fetch(`${API_BASE}/api/messages?date=${dateParam}&limit=200`),
            fetch(`${API_BASE}/api/signals?date=${dateParam}&limit=200`),
            fetch(`${API_BASE}/api/trades?date=${dateParam}&limit=500`),
        ]);
        if (msgsRes.ok)   { state.messages = await msgsRes.json(); state.messages.reverse(); }
        if (sigsRes.ok) {
            state.signals = await sigsRes.json();
            state.signals.forEach(s => { if (s.last_ltp) s.live_ltp = s.last_ltp; });
        }
        if (tradesRes.ok) state.trades = await tradesRes.json();
        // Derive AFTER both signals and trades are loaded
        deriveSignalTradeStatuses();
        state.selectedDate = date;
        renderMessages();
        renderSignals();
        renderTrades();
        toast(`Showing data for: ${date === 'all' ? 'all time' : date}`, 'info');
    } catch { toast('Failed to load data', 'error'); }
}

// ── Derive frontend-only signal states after load/refresh ─────────────────────
/**
 * trade_status is never persisted to DB — it's set by live WS events.
 * After a refresh those events are gone, so we re-derive from available data.
 *
 * Rules:
 *  1. Cross-reference state.trades to find the most recent trade for each signal
 *     and map its status → signal trade_status. This ensures a signal with a
 *     pending/filled/stopped trade correctly hides its countdown timer on load
 *     without needing to wait for a WS event.
 *  2. Mark older signals for the same strike+option_type as 'replaced'.
 */
function deriveSignalTradeStatuses() {
    // Step 1: build signal_id → most recent trade status from state.trades
    // Trades arrive newest-first (DESC by id), so first match per signal_id wins
    const sigTradeStatus = {};
    state.trades.forEach(t => {
        const sid = t.signal_id;
        if (!sid) return;
        if (sigTradeStatus[sid] !== undefined) return; // already have the newest
        sigTradeStatus[sid] = t.status || '';
    });

    // Map trade statuses → signal trade_status labels
    const TRADE_TO_SIGNAL = {
        'filled':    'filled',
        'open':      'filled',
        'closed':    'closed',
        'expired':   'expired',
        'replaced':  'replaced',
        'cancelled': 'cancelled',
        'stopped':   'stopped',
        'ignored':   'ignored',
        'pending':   'pending',
        'failed':    'failed',
    };

    state.signals.forEach(s => {
        if (s.trade_status) return; // already set by a live WS event — keep it
        const mapped = TRADE_TO_SIGNAL[sigTradeStatus[s.id]];
        if (mapped) s.trade_status = mapped;
    });

    // Step 2: mark older signals with same strike+option_type as replaced
    const groups = {};
    state.signals.forEach(s => {
        if (s.status !== 'valid') return;
        const key = `${String(s.strike).toUpperCase()}_${String(s.option_type).toUpperCase()}`;
        if (!groups[key]) groups[key] = [];
        groups[key].push(s);
    });

    Object.values(groups).forEach(group => {
        if (group.length < 2) return;
        group.sort((a, b) => (a.id || 0) - (b.id || 0));
        const TERMINAL = ['filled', 'closed', 'expired', 'replaced', 'pending', 'ignored', 'stopped', 'cancelled'];
        group.slice(0, -1).forEach(s => {
            if (!TERMINAL.includes(s.trade_status || '')) {
                s.trade_status = 'replaced';
                s.status_note  = 'Replaced by newer signal';
            }
        });
    });
}

function renderAll() {
    renderMessages();
    renderSignals();
    renderPositions();
    renderTrades();
}

function renderMessages() {
    const container = $('#messages-list');
    const count     = $('#msg-count');
    if (!container || !count) return;
    count.textContent = state.messages.length;

    if (state.messages.length === 0) {
        container.innerHTML = '<div class="empty-state">Waiting for messages...</div>';
        return;
    }
    if (container.querySelector('.empty-state')) container.innerHTML = '';

    const currentIds = new Set([...container.querySelectorAll('.msg-bubble')].map(el => el.dataset.id));
    const newItems   = state.messages.filter(m => !currentIds.has(String(m.id || m.timestamp)));
    if (newItems.length === 0) return;

    const fragment = document.createDocumentFragment();
    [...newItems].reverse().forEach(m => {
        const id  = m.id || m.timestamp;
        const div = document.createElement('div');
        div.className    = 'msg-bubble';
        div.dataset.id   = id;
        div.innerHTML    = `
            <div class="msg-sender">${esc(m.sender || 'Unknown')}</div>
            <div class="msg-text">${esc(m.raw_text || m.text || '')}</div>
            <div class="msg-time">${formatTime(m.timestamp || m.created_at)}</div>
        `;
        fragment.appendChild(div);
    });
    container.prepend(fragment);
}

/**
 * renderSignals — [FIX #22] Keyed upsert, no insertBefore on every tick.
 * Timer durations read from state.strategy so they reflect user settings. [FIX #24]
 */
function renderSignals() {
    try {
        const container = $('#signals-list');
        const count     = $('#signal-count');
        if (!container || !count) return;
        count.textContent = state.signals.length;

        if (state.signals.length === 0) {
            container.innerHTML = '<div class="empty-state">No signals parsed yet</div>';
            return;
        }
        if (container.querySelector('.empty-state')) container.innerHTML = '';

        // [FIX #24] Read timer durations from strategy (default 10 if not set)
        const entryMins = state.strategy.entryTimerMins ?? 10;

        const sorted = [...state.signals].sort((a, b) => {
            const ta = new Date(a.created_at || a.timestamp || 0).getTime();
            const tb = new Date(b.created_at || b.timestamp || 0).getTime();
            return tb - ta;
        });

        sorted.forEach(s => {
            const status      = s.status      || 'empty';
            const isValid     = status        === 'valid';
            const tradeStatus = s.trade_status || '';
            const timerStart  = s.created_at  || s.timestamp;

            const ltpVal    = s.live_ltp ? `₹${s.live_ltp.toFixed(2)}` : '--';
            const sensexVal = state.sensex_ltp ? state.sensex_ltp.toFixed(2) : '--';

            let targetsText = '--';
            if (s.targets && Array.isArray(s.targets) && s.targets.length > 0) {
                targetsText = s.targets.map(t => '₹' + t).join(', ');
            } else if (typeof s.targets === 'string' && s.targets) {
                try {
                    const tArr = JSON.parse(s.targets);
                    if (Array.isArray(tArr) && tArr.length > 0) targetsText = tArr.map(t => '₹' + t).join(', ');
                } catch { /* leave as '--' */ }
            }

            // Timer shows ONLY when no trade action has been taken yet (tradeStatus is empty).
            // Any non-empty tradeStatus means the signal has been acted on in some way
            // (pending fill, filled, cancelled, stopped, replaced, etc.) — hide the timer.
            const showTimer = isValid && timerStart && !tradeStatus;

            const cardHtml = `
                <div style="display:flex;justify-content:space-between;align-items:start;">
                    <span class="signal-status ${status}">${status}</span>
                    <div style="display:flex;gap:6px;align-items:center;">
                        ${showTimer
                            ? `<span class="timer-tag" data-timer-start="${timerStart}" data-timer-mins="${entryMins}" data-timer-label="Entry" data-trade-status="${tradeStatus}">⏳ Entry: --:--</span>`
                            : ''}
                        ${tradeStatus && tradeStatus !== 'valid' ? `<span class="signal-status ${tradeStatus}">${tradeStatus}</span>` : ''}
                    </div>
                </div>
                ${isValid && s.reason ? `<div class="signal-reason">${esc(s.reason)}</div>` : ''}
                ${isValid ? `
                    <div class="signal-details">
                        <div><span class="label">Index</span><br><span class="value">${esc(s.idx || s.index || '')}</span></div>
                        <div><span class="label">Type</span><br><span class="value">${esc(s.option_type || '')}</span></div>
                        <div><span class="label">Strike</span><br><span class="value">${esc(s.strike || '')}</span></div>
                        <div><span class="label">Entry</span><br><span class="value">₹${s.entry_low || 0} - ₹${s.entry_high || 0}</span></div>
                        <div><span class="label">SENSEX</span><br><span class="value ltp-live signal-sensex-ltp">${sensexVal}</span></div>
                        <div><span class="label">LTP</span><br><span class="value ltp-live" id="signal-ltp-${s.id}">${ltpVal}</span></div>
                        ${s.min_ltp    ? `<div><span class="label">Min LTP</span><br><span class="value">₹${s.min_ltp}</span></div>` : ''}
                        ${s.stoploss   ? `<div><span class="label">SL</span><br><span class="value">₹${s.stoploss}</span></div>` : ''}
                        <div><span class="label">Targets</span><br><span class="value">${targetsText}</span></div>
                    </div>
                ` : ''}
            `;

            const id       = `signal-card-${s.id}`;
            let card       = document.getElementById(id);
            const newClass = `signal-card ${tradeStatus || status}`;

            if (card) {
                if (card.className !== newClass || card.dataset.tradeStatus !== tradeStatus) {
                    card.className           = newClass;
                    card.dataset.tradeStatus = tradeStatus;
                    card.innerHTML           = cardHtml;
                }
            } else {
                card                     = document.createElement('div');
                card.id                  = id;
                card.className           = newClass;
                card.dataset.tradeStatus = tradeStatus;
                card.innerHTML           = cardHtml;
                container.appendChild(card);
            }
        });

        sorted.forEach((s, idx) => {
            const card     = document.getElementById(`signal-card-${s.id}`);
            const expected = container.children[idx];
            if (card && card !== expected) {
                container.insertBefore(card, expected || null);
            }
        });

        const liveIds = new Set(sorted.map(s => `signal-card-${s.id}`));
        [...container.querySelectorAll('.signal-card')].forEach(card => {
            if (!liveIds.has(card.id)) card.remove();
        });

    } catch (err) {
        console.error('Error in renderSignals:', err);
    }
}

function renderPositions() {
    const container = $('#positions-list');
    const count     = $('#pos-count');
    const pnlEl     = $('#pnl-value');
    if (!container || !count || !pnlEl) return;

    const open     = state.positions.filter(p => p.status === 'open');
    count.textContent = open.length;

    const totalPnl = open.reduce((sum, p) => sum + (p.pnl || 0), 0);
    const realisedEl = $('#pnl-realised');
    if (realisedEl) {
        const closed = state.positions.filter(p => p.status === 'closed');
        const realisedPnl = closed.reduce((sum, p) => sum + (p.pnl || 0), 0);
        realisedEl.textContent = `₹${realisedPnl.toFixed(2)}`;
        realisedEl.className = `pnl-value ${realisedPnl > 0 ? 'positive' : realisedPnl < 0 ? 'negative' : ''}`;
    }
    pnlEl.textContent = `₹${totalPnl.toFixed(2)}`;
    pnlEl.className   = `pnl-value ${totalPnl > 0 ? 'positive' : totalPnl < 0 ? 'negative' : ''}`;

    if (open.length === 0) {
        container.innerHTML = '<div class="empty-state">No open positions</div>';
        return;
    }
    if (container.querySelector('.empty-state')) container.innerHTML = '';

    // Remove cards for closed positions
    const openIds = new Set(open.map(p => String(p.id)));
    container.querySelectorAll('.position-card').forEach(card => {
        if (!openIds.has(card.dataset.posId)) card.remove();
    });

    // [FIX #24] Read exit timer duration from strategy
    const exitMins = state.strategy.exitTimerMins ?? 10;

    open.forEach(p => {
        const pnl       = p.pnl || 0;
        const pnlClass  = pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : '';
        const existing  = container.querySelector(`.position-card[data-pos-id="${p.id}"]`);

        if (existing) {
            // [3] Patch only the volatile fields — timer node is untouched
            const pnlDiv = existing.querySelector('.pos-pnl');
            if (pnlDiv) {
                pnlDiv.textContent = `${pnl >= 0 ? '+' : ''}₹${pnl.toFixed(2)}`;
                pnlDiv.className   = `pos-pnl ${pnlClass}`;
            }
            const ltpSpan = existing.querySelector('.pos-meta .mono');
            if (ltpSpan) ltpSpan.textContent = `₹${(p.current_price || 0).toFixed(2)}`;
            const slTag  = existing.querySelector('.sl-tag');
            if (slTag)   slTag.textContent   = `SL: ₹${(p.trailing_sl || 0).toFixed(2)}`;
            const maxTag = existing.querySelector('.max-tag');
            if (maxTag && p.max_ltp) maxTag.textContent = `Max: ₹${p.max_ltp.toFixed(2)}`;
        } else {
            const div           = document.createElement('div');
            div.className       = 'position-card';
            div.dataset.posId   = String(p.id);
            div.innerHTML       = `
                <div class="pos-info">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span class="pos-symbol">${esc(p.trading_symbol || '')}</span>
                        <div style="display:flex;gap:6px;align-items:center;">
                            ${p.opened_at ? `<span class="timer-tag" data-timer-start="${p.opened_at}" data-timer-mins="${exitMins}" data-timer-label="Hold">⏳ Hold: --:--</span>` : ''}
                            <button class="btn btn-exit" onclick="exitPosition(${p.id})" title="Exit this position">❌ Exit</button>
                        </div>
                    </div>
                    <span class="pos-meta">
                        Qty: ${p.quantity || 0} |
                        Entry: ₹${(p.entry_price || 0).toFixed(2)} |
                        LTP: <span class="mono">₹${(p.current_price || 0).toFixed(2)}</span>
                    </span>
                    <div class="pos-strategy">
                        <span class="sl-tag">SL: ₹${(p.trailing_sl || 0).toFixed(2)}</span>
                        ${p.max_ltp ? `<span class="max-tag">Max: ₹${p.max_ltp.toFixed(2)}</span>` : ''}
                    </div>
                </div>
                <div class="pos-pnl ${pnlClass}">${pnl >= 0 ? '+' : ''}₹${pnl.toFixed(2)}</div>
            `;
            container.prepend(div);
        }
    });
}

function renderTrades() {
    const container = $('#trades-list');
    const count     = $('#trade-count');
    if (!container || !count) return;

    const filtered = state.tradeFilter === 'all'
        ? state.trades
        : state.trades.filter(t => t.status === state.tradeFilter);
    count.textContent = filtered.length;

    if (filtered.length === 0) {
        container.innerHTML = '<div class="empty-state">No trades yet</div>';
        return;
    }

    container.innerHTML = `
        <table class="trade-table">
            <thead>
                <tr>
                    <th>Time</th><th>Symbol</th><th>Entry Mode</th><th>Side</th>
                    <th>Qty</th><th>Price</th><th>Fill</th><th>Exit</th>
                    <th>P&L</th><th>Mode</th><th>Status</th>
                </tr>
            </thead>
            <tbody>
                ${filtered.map(t => `
                    <tr>
                        <td class="mono">${formatTime(t.created_at || t.fill_time)}</td>
                        <td class="mono">${esc(t.trading_symbol || '-')}</td>
                        <td><span class="entry-label-tag ${t.entry_label || ''}">${t.entry_label || '-'}</span></td>
                        <td>${t.transaction_type === 'B' ? '🟢 BUY' : '🔴 SELL'}</td>
                        <td class="mono">${t.quantity || '-'}</td>
                        <td class="mono">₹${(t.price || 0).toFixed(2)}</td>
                        <td class="mono">${t.fill_price  ? '₹' + t.fill_price.toFixed(2)          : '-'}</td>
                        <td class="mono">${t.exit_price  ? '₹' + Number(t.exit_price).toFixed(2)  : '-'}</td>
                        <td class="mono" style="color:${(t.pnl || 0) >= 0 ? 'var(--green)' : 'var(--red)'}">
                            ${t.pnl != null ? '₹' + t.pnl.toFixed(2) : '-'}
                        </td>
                        <td>${t.mode === 'paper' ? '📄' : '🔴'} ${t.mode || '-'}</td>
                        <td><span class="trade-status ${t.status || ''}">${t.status || '-'}</span></td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

// ── Status Updates ────────────────────────────────────────────────────────────
function updateStatusFromData(status) {
    if (!status) return;
    state.mode = status.mode || 'paper';
    updateModeUI();
    updateBadge('badge-telegram', status.telegram);

    const kotak      = status.kotak || {};
    const isAuth     = kotak.authenticated;
    updateBadge('badge-kotak', isAuth);

    const statusText = $('#kotak-auth-status');
    const otpRow     = $('#otp-row');
    if (!statusText) return;

    switch (kotak.login_state || (isAuth ? 'logged_in' : 'unknown')) {
        case 'not_configured':
            statusText.textContent = 'Kotak not configured';
            if (otpRow) otpRow.style.display = 'none';
            break;
        case 'logging_in':
            statusText.textContent = 'Logging in to Kotak...';
            if (otpRow) otpRow.style.display = 'none';
            break;
        case 'logged_in':
            statusText.textContent = '✅ Authenticated';
            if (otpRow) otpRow.style.display = 'none';
            break;
        case 'login_failed':
            statusText.textContent = kotak.last_error
                ? `Login failed: ${kotak.last_error}`
                : 'Login failed — see logs';
            if (otpRow) otpRow.style.display = 'block';
            break;
        case 'dependency_missing':
            statusText.textContent = 'Kotak deps missing (neo_api_client/pyotp)';
            if (otpRow) otpRow.style.display = 'none';
            break;
        default:
            statusText.textContent = isAuth ? '✅ Authenticated' : 'Not authenticated';
    }
}

function updateModeUI() {
    $('#btn-paper')?.classList.toggle('active', state.mode === 'paper');
    $('#btn-real')?.classList.toggle('active',  state.mode === 'real');
}

function updateBadge(id, connected) {
    const badge = $(`#${id}`);
    if (!badge) return;
    badge.classList.toggle('badge-connected',    !!connected);
    badge.classList.toggle('badge-disconnected', !connected);
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function formatTime(iso) {
    if (!iso) return '-';
    try {
        let d = iso;
        if (d.includes(' ') && !d.includes('T')) d = d.replace(' ', 'T');
        if (d.endsWith('+00:00')) d = d.replace('+00:00', 'Z');
        if (d.endsWith('-00:00')) d = d.replace('-00:00', 'Z');
        if (!d.endsWith('Z') && !d.includes('+')) d += 'Z';
        return new Date(d).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch { return iso; }
}

function getCountdown(isoStart, durationMinutes) {
    if (!isoStart) return null;
    try {
        let d = isoStart;
        if (d.includes(' ') && !d.includes('T')) { d = d.replace(' ', 'T') + 'Z'; }
        else if (!d.endsWith('Z') && !d.includes('+')) { d += 'Z'; }
        const diff = (new Date(d).getTime() + durationMinutes * 60000) - Date.now();
        if (diff <= 0) return null;
        const m = Math.floor((diff % 3600000) / 60000);
        const s = Math.floor((diff % 60000)   / 1000);
        return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    } catch { return null; }
}

function toast(message, type = 'info') {
    const container = $('#toast-container');
    if (!container) return;
    const el       = document.createElement('div');
    el.className   = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => el.remove(), 4000);
}

// ── Clear Modal ───────────────────────────────────────────────────────────────
function bindClearModal() {
    // Open already handled in bindEventListeners via #btn-clear

    $('#btn-close-clear')?.addEventListener('click', () => {
        $('#clear-modal').style.display = 'none';
    });

    $('#btn-cancel-clear')?.addEventListener('click', () => {
        $('#clear-modal').style.display = 'none';
    });

    // Scope radio buttons — show/hide date picker, enable confirm
    $$('input[name="clear-scope"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const dateRow    = $('#clear-date-row');
            const confirmBtn = $('#btn-confirm-clear');
            if (radio.value === 'date') {
                if (dateRow) dateRow.style.display = 'block';
                // Only enable confirm once a date is actually picked
                const dateInput = $('#clear-date-input');
                if (confirmBtn) confirmBtn.disabled = !dateInput?.value;
            } else {
                if (dateRow) dateRow.style.display = 'none';
                if (confirmBtn) confirmBtn.disabled = false;
            }
        });
    });

    // Enable confirm when a date is chosen
    $('#clear-date-input')?.addEventListener('change', () => {
        const confirmBtn = $('#btn-confirm-clear');
        const val = $('#clear-date-input').value;
        if (confirmBtn) confirmBtn.disabled = !val;
    });

    $('#btn-confirm-clear')?.addEventListener('click', async () => {
        const scope = document.querySelector('input[name="clear-scope"]:checked')?.value;
        if (!scope) return;

        const date = scope === 'date' ? ($('#clear-date-input')?.value || null) : null;
        if (scope === 'date' && !date) {
            return toast('Please pick a date to clear', 'warning');
        }

        const confirmText = date
            ? `Clear all data for ${date}? This cannot be undone.`
            : 'Clear ALL dashboard data? This cannot be undone.';
        if (!confirm(confirmText)) return;

        try {
            await fetch(`${API_BASE}/api/clear`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ date }),
            });

            if (date === null) {
                // Full wipe — reset all state
                state.messages  = [];
                state.signals   = [];
                state.trades    = [];
                state.positions = [];
            } else {
                // Date-specific — filter out records matching that date
                const isSameDate = (ts) => {
                    if (!ts) return false;
                    try {
                        let d = ts;
                        if (d.includes(' ') && !d.includes('T')) d = d.replace(' ', 'T');
                        if (!d.endsWith('Z') && !d.includes('+')) d += 'Z';
                        return new Date(d).toLocaleDateString('en-CA') === date;
                    } catch { return false; }
                };
                state.messages  = state.messages.filter(m => !isSameDate(m.timestamp || m.created_at));
                state.signals   = state.signals.filter(s  => !isSameDate(s.created_at || s.timestamp));
                state.trades    = state.trades.filter(t   => !isSameDate(t.created_at || t.fill_time));
                // Positions: only close today's closed ones — open positions untouched
                state.positions = state.positions.filter(p =>
                    p.status === 'open' || !isSameDate(p.closed_at || p.created_at)
                );
            }

            renderAll();
            $('#clear-modal').style.display = 'none';
            toast(date ? `Data for ${date} cleared` : 'All data cleared', 'info');
        } catch (err) {
            console.error('Clear error:', err);
            toast('Error clearing data', 'error');
        }
    });
}

// ── Stop Trading ──────────────────────────────────────────────────────────────
function updateStopTradingUI() {
    const stopped = state.stopTrading;

    // Desktop header button
    const headerBtn = $('#btn-stop-trading-header');
    if (headerBtn) {
        headerBtn.textContent = stopped ? '⏸ Stopped' : '▶ Trading';
        headerBtn.classList.toggle('btn-stop-trading-active', stopped);
        headerBtn.title = stopped ? 'Trading stopped — click to resume' : 'Trading active — click to stop';
    }

    // Hamburger menu button
    const menuBtn = $('#btn-stop-trading-menu');
    if (menuBtn) {
        menuBtn.textContent = stopped ? '⏸ Stopped' : '▶ Trading';
        menuBtn.classList.toggle('btn-stop-trading-active', stopped);
    }
}

async function toggleStopTrading() {
    const newState = !state.stopTrading;
    try {
        const res  = await fetch(`${API_BASE}/api/stop-trading`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled: newState }),
        });
        const data = await res.json();
        if (data.status === 'ok') {
            state.stopTrading = data.enabled;
            updateStopTradingUI();
            toast(data.enabled ? '⏸ Trading STOPPED — signals will be ignored' : '▶ Trading RESUMED', data.enabled ? 'warning' : 'success');
        }
    } catch {
        toast('Failed to toggle stop trading', 'error');
    }
}

function bindStopTrading() {
    $('#btn-stop-trading-header')?.addEventListener('click', toggleStopTrading);
    $('#btn-stop-trading-menu')?.addEventListener('click',   toggleStopTrading);
}

// ── Strategy ──────────────────────────────────────────────────────────────────
const STRATEGY_DEFAULTS = {
    lots:                      1,
    entryLogic:                'code',
    entryAvgPick:              'avg',
    entryFixed:                null,
    trailingSL:                'code',
    slFixed:                   null,
    activationPoints:          5.0,
    trailGap:                  2.0,
    compareMode:               false,
    entryTimerMins:            10,      // [FIX #24]
    exitTimerMins:             10,      // [FIX #24]
    signalTrailInitialSL:      'telegram',   // [FIX #24]
    signalTrailInitialSLPoints: 5.0,    // [FIX #24]
};

async function loadStrategy() {
    try {
        const saved = localStorage.getItem('tradebridge_strategy');
        if (saved) {
            state.strategy = { ...STRATEGY_DEFAULTS, ...JSON.parse(saved) };
        } else {
            state.strategy = { ...STRATEGY_DEFAULTS };
        }
        updateStrategyButtonBadge();
    } catch {
        state.strategy = { ...STRATEGY_DEFAULTS };
    }

    try {
        const res = await fetch(`${API_BASE}/api/settings`);
        if (res.ok) {
            const data = await res.json();
            if (data.strategy) {
                state.strategy = { ...STRATEGY_DEFAULTS, ...data.strategy };
                persistStrategy();
            }
            if (data.lot_size != null) {
                state.lotSize = data.lot_size;
                const lotInput = $('#lot-input');
                if (lotInput) lotInput.value = data.lot_size;
            }
        }
    } catch {
        console.warn('loadStrategy: server unreachable, using localStorage fallback');
    }

    updateStrategyButtonBadge();
}

function persistStrategy() {
    try {
        localStorage.setItem('tradebridge_strategy', JSON.stringify(state.strategy));
    } catch (e) {
        console.warn('Could not persist strategy to localStorage:', e);
    }
    updateStrategyButtonBadge();
}

function updateStrategyButtonBadge() {
    const btn = $('#btn-strategy');
    if (!btn) return;
    const isDefault = (
        state.strategy.lots        === 1      &&
        state.strategy.entryLogic  === 'code' &&
        state.strategy.trailingSL  === 'code' &&
        !state.strategy.compareMode
    );
    btn.classList.toggle('strategy-active', !isDefault);
    const modeLabel = state.strategy.compareMode ? '🔬 Compare ALL' : state.strategy.entryLogic;
    btn.title = isDefault
        ? 'Strategy Setup'
        : `Strategy: ${state.strategy.lots} lot(s) | Entry: ${modeLabel} | SL: ${state.strategy.trailingSL}`;
}

function toggleEntryLogicSection(compareOn) {
    const section = $('#entry-logic-section');
    if (section) {
        section.style.opacity       = compareOn ? '0.4' : '1';
        section.style.pointerEvents = compareOn ? 'none' : '';
    }
}

function syncStrategyModalToState() {
    const s = state.strategy;

    // Lots
    const sel = $('#strategy-lots-select');
    if (sel) sel.value = s.lots;
    $$('input[name="lots-quick"]').forEach(r => { r.checked = parseInt(r.value) === s.lots; });

    // [FIX #24] Timers
    const entryTimerInput = $('#entry-timer-mins');
    if (entryTimerInput) entryTimerInput.value = s.entryTimerMins ?? 10;
    const exitTimerInput = $('#exit-timer-mins');
    if (exitTimerInput) exitTimerInput.value = s.exitTimerMins ?? 10;

    // Entry logic
    $$('input[name="entry-logic"]').forEach(r => { r.checked = r.value === s.entryLogic; });
    const entryFixedRow = $('#entry-fixed-row');
    if (entryFixedRow) entryFixedRow.style.display = s.entryLogic === 'fixed'       ? 'block' : 'none';
    const entryAvgRow   = $('#entry-avg-row');
    if (entryAvgRow)   entryAvgRow.style.display   = s.entryLogic === 'avg_signal'  ? 'block' : 'none';
    const entryFixedInput = $('#entry-fixed-price');
    if (entryFixedInput && s.entryFixed) entryFixedInput.value = s.entryFixed;
    $$('input[name="entry-avg-pick"]').forEach(r => { r.checked = r.value === (s.entryAvgPick || 'avg'); });

    // Trailing SL
    $$('input[name="trailing-sl"]').forEach(r => { r.checked = r.value === s.trailingSL; });
    const slFixedRow = $('#sl-fixed-row');
    if (slFixedRow) slFixedRow.style.display = s.trailingSL === 'fixed'        ? 'block' : 'none';
    const slSignalTrailRow = $('#sl-signal-trail-row');
    if (slSignalTrailRow) slSignalTrailRow.style.display = s.trailingSL === 'signal_trail' ? 'block' : 'none';
    const slFixedInput = $('#sl-fixed-price');
    if (slFixedInput && s.slFixed) slFixedInput.value = s.slFixed;
    const actInput = $('#sl-activation-points');
    if (actInput) actInput.value = s.activationPoints ?? 5;
    const gapInput = $('#sl-trail-gap');
    if (gapInput) gapInput.value = s.trailGap ?? 2;

    // [FIX #24] Signal trail initial SL
    const initSL = s.signalTrailInitialSL || 'telegram';
    $$('input[name="signal-trail-initial-sl"]').forEach(r => { r.checked = r.value === initSL; });
    const initPointsRow = $('#sl-init-points-row');
    if (initPointsRow) initPointsRow.style.display = initSL === 'points_from_ltp' ? 'block' : 'none';
    const initPointsInput = $('#sl-init-points-value');
    if (initPointsInput) initPointsInput.value = s.signalTrailInitialSLPoints ?? 5;

    // Compare mode
    const compareToggle = $('#strategy-compare-mode');
    if (compareToggle) compareToggle.checked = !!s.compareMode;
    toggleEntryLogicSection(!!s.compareMode);
}

function populateLotDropdown() {
    const sel = $('#strategy-lots-select');
    if (!sel || sel.options.length > 0) return;
    for (let i = 1; i <= 50; i++) {
        const opt         = document.createElement('option');
        opt.value         = i;
        opt.textContent   = `${i} Lot${i > 1 ? 's' : ''}`;
        sel.appendChild(opt);
    }
}

function bindStrategyModal() {
    $('#btn-strategy')?.addEventListener('click', () => {
        populateLotDropdown();
        syncStrategyModalToState();
        $('#strategy-modal').style.display = 'flex';
    });

    $('#btn-close-strategy')?.addEventListener('click', () => {
        $('#strategy-modal').style.display = 'none';
    });

    $('#strategy-lots-select')?.addEventListener('change', () => {
        const val = parseInt($('#strategy-lots-select').value);
        $$('input[name="lots-quick"]').forEach(r => { r.checked = parseInt(r.value) === val; });
        const lotInput = $('#lot-input');
        if (lotInput) lotInput.value = val;
    });

    $$('input[name="lots-quick"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const val = parseInt(radio.value);
            const sel = $('#strategy-lots-select');
            if (sel) sel.value = val;
            const lotInput = $('#lot-input');
            if (lotInput) lotInput.value = val;
        });
    });

    $$('input[name="entry-logic"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const fixedRow = $('#entry-fixed-row');
            const avgRow   = $('#entry-avg-row');
            if (fixedRow) fixedRow.style.display = radio.value === 'fixed'      ? 'block' : 'none';
            if (avgRow)   avgRow.style.display   = radio.value === 'avg_signal' ? 'block' : 'none';
        });
    });

    $$('input[name="trailing-sl"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const fixedRow       = $('#sl-fixed-row');
            const signalTrailRow = $('#sl-signal-trail-row');
            if (fixedRow)       fixedRow.style.display       = radio.value === 'fixed'        ? 'block' : 'none';
            if (signalTrailRow) signalTrailRow.style.display  = radio.value === 'signal_trail' ? 'block' : 'none';
        });
    });

    // [FIX #24] Show/hide points input under signal_trail initial SL
    $$('input[name="signal-trail-initial-sl"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const row = $('#sl-init-points-row');
            if (row) row.style.display = radio.value === 'points_from_ltp' ? 'block' : 'none';
        });
    });

    $('#strategy-compare-mode')?.addEventListener('change', (e) => {
        toggleEntryLogicSection(e.target.checked);
    });

    $('#btn-strategy-reset')?.addEventListener('click', () => {
        state.strategy = { ...STRATEGY_DEFAULTS };
        populateLotDropdown();
        syncStrategyModalToState();
        const lotInput = $('#lot-input');
        if (lotInput) lotInput.value = 1;
        persistStrategy();
        toast('Strategy reset to defaults', 'info');
    });

    $('#btn-strategy-save')?.addEventListener('click', async () => {
        const lots         = parseInt($('#strategy-lots-select')?.value) || 1;
        const entryRadio   = document.querySelector('input[name="entry-logic"]:checked');
        const entryLogic   = entryRadio?.value || 'code';
        const entryFixed   = entryLogic === 'fixed'      ? (parseFloat($('#entry-fixed-price')?.value) || null) : null;
        const avgPickR     = document.querySelector('input[name="entry-avg-pick"]:checked');
        const entryAvgPick = entryLogic === 'avg_signal' ? (avgPickR?.value || 'avg') : 'avg';
        const slRadio      = document.querySelector('input[name="trailing-sl"]:checked');
        const trailingSL   = slRadio?.value || 'code';
        const slFixed      = trailingSL === 'fixed'        ? (parseFloat($('#sl-fixed-price')?.value) || null) : null;
        const activationPoints = trailingSL === 'signal_trail' ? (parseFloat($('#sl-activation-points')?.value) || 5)  : null;
        const trailGap         = trailingSL === 'signal_trail' ? (parseFloat($('#sl-trail-gap')?.value)         || 2)  : null;
        const compareMode  = !!$('#strategy-compare-mode')?.checked;

        // [FIX #24] Timer values
        const entryTimerMins = parseInt($('#entry-timer-mins')?.value) || 10;
        const exitTimerMins  = parseInt($('#exit-timer-mins')?.value)  || 10;

        // [FIX #24] Signal trail initial SL
        const initSLRadio   = document.querySelector('input[name="signal-trail-initial-sl"]:checked');
        const signalTrailInitialSL = initSLRadio?.value || 'telegram';
        const signalTrailInitialSLPoints = signalTrailInitialSL === 'points_from_ltp'
            ? (parseFloat($('#sl-init-points-value')?.value) || 5)
            : null;

        // Validation
        if (!compareMode && entryLogic === 'fixed' && !entryFixed) {
            return toast('Please enter a fixed entry price', 'warning');
        }
        if (trailingSL === 'fixed' && !slFixed) {
            return toast('Please enter a fixed SL price', 'warning');
        }
        if (trailingSL === 'signal_trail' && (!activationPoints || !trailGap)) {
            return toast('Please enter activation pts and trail gap for Signal Trail', 'warning');
        }
        if (trailingSL === 'signal_trail' && signalTrailInitialSL === 'points_from_ltp' && !signalTrailInitialSLPoints) {
            return toast('Please enter points below entry for initial SL', 'warning');
        }
        if (entryTimerMins < 1 || exitTimerMins < 1) {
            return toast('Timer values must be at least 1 minute', 'warning');
        }

        state.strategy = {
            lots, entryLogic, entryAvgPick, entryFixed,
            trailingSL, slFixed, activationPoints, trailGap, compareMode,
            entryTimerMins, exitTimerMins,
            signalTrailInitialSL, signalTrailInitialSLPoints,
        };
        persistStrategy();

        const lotInput = $('#lot-input');
        if (lotInput) lotInput.value = lots;

        try {
            await fetch(`${API_BASE}/api/settings/strategy`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(state.strategy),
            });
        } catch { console.warn('Could not sync strategy to backend'); }

        try {
            await fetch(`${API_BASE}/api/settings/lot-size`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lots }),
            });
        } catch { console.warn('Could not sync lot size to backend'); }

        const modeLabel = compareMode ? '🔬 Compare ALL' : entryLogic;
        toast(`Strategy saved: ${lots} lot(s) | Entry: ${modeLabel} | SL: ${trailingSL}`, 'success');
        $('#strategy-modal').style.display = 'none';
    });
}