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
 */

const protocol = window.location.protocol;
const host = window.location.host;
const API_BASE = `${protocol}//${host}`;
const WS_URL = `${protocol === 'https:' ? 'wss:' : 'ws:'}//${host}/ws`;

// ── State ──
const state = {
    mode: 'paper',
    messages: [],
    signals: [],
    trades: [],
    positions: [],
    tradeFilter: 'all',
    wsConnected: false,
    sensex_ltp: 0,
    strategy: {
        lots: 1,
        entryLogic: 'code',
        entryAvgPick: 'avg',
        entryFixed: null,
        trailingSL: 'code',
        slFixed: null,
    },
};

let ws = null;
let reconnectTimer = null;

// ── DOM References ──
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// ── [6] Cached escape element — avoids creating a new DOM node on every call ──
const _escDiv = document.createElement('div');
function esc(str) {
    _escDiv.textContent = str || '';
    return _escDiv.innerHTML;
}

// ── [10] Debounce helper ──
function debounce(fn, ms) {
    let timer;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), ms);
    };
}
const renderTradesDebounced = debounce(renderTrades, 100);

// ── Init ──
document.addEventListener('DOMContentLoaded', () => {
    loadStrategy();
    bindEvents();
    connectWebSocket();
    fetchInitialData();

    setInterval(() => {
        document.querySelectorAll('[data-timer-start]').forEach(el => {
            const tradeStatus = el.getAttribute('data-trade-status') || '';
            if (['filled', 'closed', 'replaced', 'expired'].includes(tradeStatus)) return;

            const start = el.getAttribute('data-timer-start');
            const mins = parseInt(el.getAttribute('data-timer-mins') || '10');
            const label = el.getAttribute('data-timer-label') || 'Timer';
            const countdown = getCountdown(start, mins);
            if (countdown === null) {
                el.textContent = `❌ EXPIRED`;
                el.classList.add('expired');
            } else {
                el.textContent = `⏳ ${label}: ${countdown}`;
            }
        });
    }, 1000);
});

// ── Event Binding ──
function bindEvents() {
    const hamburger = $('#btn-hamburger');
    const headerMenu = $('#header-menu');
    hamburger.addEventListener('click', () => {
        headerMenu.classList.toggle('open');
        hamburger.classList.toggle('open');
    });
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.header-right') && headerMenu.classList.contains('open')) {
            headerMenu.classList.remove('open');
            hamburger.classList.remove('open');
        }
    });

    $('#btn-paper').addEventListener('click', () => setMode('paper'));
    $('#btn-real').addEventListener('click', () => {
        $('#confirm-real-modal').style.display = 'flex';
    });
    $('#btn-confirm-real').addEventListener('click', () => {
        $('#confirm-real-modal').style.display = 'none';
        setMode('real');
    });
    $('#btn-cancel-real').addEventListener('click', () => {
        $('#confirm-real-modal').style.display = 'none';
    });

    $('#btn-settings').addEventListener('click', () => {
        $('#settings-modal').style.display = 'flex';
    });
    $('#btn-close-settings').addEventListener('click', () => {
        $('#settings-modal').style.display = 'none';
    });

    $$('.panel-header').forEach(header => {
        header.addEventListener('click', () => {
            if (window.innerWidth <= 768) {
                const panel = header.closest('.panel');
                panel.classList.toggle('collapsed');
            }
        });
    });

    $('#btn-clear').addEventListener('click', async () => {
        if (confirm("Are you sure you want to completely clear the dashboard?\n\nThis will delete all messages, signals, trades, and positions.\n\nNote: Backtesting ticks will NOT be deleted.")) {
            try {
                const res = await fetch(`${API_BASE}/api/clear`, { method: 'POST' });
                if (res.ok) {
                    location.reload(true);
                } else {
                    toast('Failed to clear data', 'error');
                }
            } catch (err) {
                console.error('Clear error:', err);
                toast('Error clearing data', 'error');
            }
        }
    });

    $('#btn-kill').addEventListener('click', async () => {
        if (confirm('\u26a0\ufe0f KILL SWITCH\n\nThis will:\n\u2022 Close ALL open positions at current price\n\u2022 Cancel ALL pending orders\n\nAre you sure?')) {
            try {
                const res = await fetch(`${API_BASE}/api/kill`, { method: 'POST' });
                const data = await res.json();
                toast(`Killed: ${data.positions_closed} positions closed, ${data.orders_cancelled} orders cancelled`, 'warning');
            } catch (e) {
                toast('Kill switch failed', 'error');
            }
        }
    });

    $('#btn-set-lots').addEventListener('click', setLotSize);
    $('#lot-input').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') setLotSize();
    });

    $('#btn-kotak-login').addEventListener('click', kotakLogin);
    $('#btn-submit-otp').addEventListener('click', submitOTP);

    $('#btn-send-test').addEventListener('click', sendTestSignal);

    $$('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            $$('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.tradeFilter = btn.dataset.filter;
            renderTrades();
        });
    });

    // [11] Single delegated modal-overlay close handler on document
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal-overlay')) {
            e.target.style.display = 'none';
        }
    });

    bindStrategyModal();
}

// ── WebSocket ──
function connectWebSocket() {
    if (ws && ws.readyState === WebSocket.OPEN) return;

    ws = new WebSocket(WS_URL);

    // [7] Attach pingInterval to the ws instance so onclose can clear the right one
    ws._pingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping' }));
        }
    }, 20000);

    ws.onopen = () => {
        state.wsConnected = true;
        updateBadge('badge-ws', true);
        toast('Connected to server', 'success');
        if (reconnectTimer) {
            clearInterval(reconnectTimer);
            reconnectTimer = null;
        }
    };

    ws.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            handleWSMessage(msg);
        } catch (e) {
            console.error('WS parse error:', e);
        }
    };

    ws.onclose = () => {
        state.wsConnected = false;
        // [7] Clear this connection's ping interval specifically
        clearInterval(ws._pingInterval);
        updateBadge('badge-ws', false);
        if (!reconnectTimer) {
            reconnectTimer = setInterval(() => {
                console.log('Reconnecting WebSocket...');
                connectWebSocket();
            }, 5000);
        }
    };

    ws.onerror = (err) => {
        console.error('WS error:', err);
    };
}

function handleWSMessage(msg) {
    try {
        if (!msg) return;

        if (msg.type !== 'instrument_ltp' && msg.type !== 'index_ltp') {
            console.log("WS Received:", msg.type, msg.data);
        }

        switch (msg.type) {
            case 'init':
                state.messages = msg.data.messages || [];
                state.signals = msg.data.signals || [];
                state.trades = msg.data.trades || [];
                state.positions = msg.data.positions || [];
                updateStatusFromData(msg.data.status);
                // [1] Prefer server-provided strategy over localStorage
                if (msg.data.strategy) {
                    state.strategy = { ...STRATEGY_DEFAULTS, ...msg.data.strategy };
                    persistStrategy();
                }
                renderAll();
                break;

            case 'new_message':
                state.messages.unshift(msg.data);
                renderMessages();
                break;

            case 'new_signal':
                console.log("Adding new signal:", msg.data);
                if (msg.data.strike && msg.data.option_type) {
                    const existingIdx = state.signals.findIndex(s =>
                        String(s.strike) === String(msg.data.strike) &&
                        String(s.option_type).toUpperCase() === String(msg.data.option_type).toUpperCase() &&
                        !['filled', 'closed', 'expired', 'replaced'].includes(s.trade_status)
                    );
                    if (existingIdx !== -1) {
                        state.signals[existingIdx].trade_status = 'replaced';
                        state.signals[existingIdx].status_note = 'Replaced by newer signal';
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
                    const newTradeId = msg.data.trade_id || msg.data.id;
                    const existingIdx = state.trades.findIndex(t => (t.trade_id || t.id) === newTradeId);
                    if (existingIdx !== -1) {
                        state.trades[existingIdx] = { ...state.trades[existingIdx], ...msg.data };
                    } else {
                        state.trades.unshift(msg.data);
                    }

                    if (msg.data.status === 'filled') {
                        // [9] Use position_id exclusively to guard against duplicates
                        const posId = msg.data.position_id;
                        if (posId && !state.positions.some(p => p.position_id === posId || p.id === posId)) {
                            state.positions.unshift(msg.data);
                            renderPositions();
                        }
                        if (msg.data.signal_id) {
                            const sigIdx = state.signals.findIndex(s => s.id === msg.data.signal_id);
                            if (sigIdx !== -1) {
                                state.signals[sigIdx].trade_status = 'filled';
                                state.signals[sigIdx].status_note = `Filled @ ₹${(msg.data.entry_price || msg.data.fill_price || 0).toFixed(2)}`;
                                renderSignals();
                            }
                        }
                    } else if (msg.data.status === 'pending' && msg.data.signal_id) {
                        const sigIdx = state.signals.findIndex(s => s.id === msg.data.signal_id);
                        if (sigIdx !== -1) {
                            state.signals[sigIdx].trade_status = 'pending';
                            renderSignals();
                        }
                    }

                    // [10] Debounced to avoid thrashing the table on rapid events
                    renderTradesDebounced();
                    const tradeStatus = msg.data.status || 'pending';
                    toast(`Trade ${tradeStatus}: ${msg.data.trading_symbol || ''}`, tradeStatus === 'filled' ? 'success' : 'info');
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
                        if (upd.status) state.signals[sigIdx].trade_status = upd.status;
                        if (upd.status_note) state.signals[sigIdx].status_note = upd.status_note;
                        if (upd.min_ltp) state.signals[sigIdx].min_ltp = upd.min_ltp;
                        renderSignals();
                    }
                }

                const trdIdx = state.trades.findIndex(t => (t.id === upd.id) || (t.trade_id === upd.id));
                if (trdIdx !== -1) {
                    state.trades[trdIdx] = { ...state.trades[trdIdx], ...upd };
                    renderTradesDebounced(); // [10]
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
                        const idxStr = (s.idx || s.index || 'SENSEX').toUpperCase().replace(/\s/g, '');
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
                // [5] Authoritative removal — driven by WS, not by exitPosition()
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

            case 'pong':
                break;

            default:
                console.log('Unknown WS message:', msg);
        }
    } catch (err) {
        console.error("Error handling WS message:", err, msg);
    }
}

// ── REST API Calls ──
async function fetchInitialData() {
    try {
        const [statusRes, msgsRes, sigsRes, tradesRes, posRes] = await Promise.all([
            fetch(`${API_BASE}/api/status`),
            fetch(`${API_BASE}/api/messages`),
            fetch(`${API_BASE}/api/signals`),
            fetch(`${API_BASE}/api/trades`),
            fetch(`${API_BASE}/api/positions`),
        ]);

        if (statusRes.ok) {
            const status = await statusRes.json();
            updateStatusFromData(status);
            if (status.lot_size) {
                const lotInput = $('#lot-input');
                if (lotInput) lotInput.value = status.lot_size;
            }
            // [1] Load strategy from server if provided
            if (status.strategy) {
                state.strategy = { ...STRATEGY_DEFAULTS, ...status.strategy };
                persistStrategy();
            }
        }
        if (msgsRes.ok) state.messages = await msgsRes.json();
        if (sigsRes.ok) state.signals = await sigsRes.json();
        if (tradesRes.ok) state.trades = await tradesRes.json();
        if (posRes.ok) state.positions = await posRes.json();

        renderAll();
    } catch (e) {
        console.error('Failed to fetch initial data:', e);
    }
}

async function setMode(mode) {
    try {
        const res = await fetch(`${API_BASE}/api/mode`, {
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
    } catch (e) {
        toast('Failed to switch mode', 'error');
    }
}

async function kotakLogin() {
    try {
        const res = await fetch(`${API_BASE}/api/auth/login`, { method: 'POST' });
        const data = await res.json();
        if (data.status === 'ok') {
            $('#otp-row').style.display = 'none';
            $('#kotak-auth-status').textContent = '✅ Authenticated';
            updateBadge('badge-kotak', true);
            toast('Kotak Neo authenticated automatically!', 'success');
        } else {
            $('#kotak-auth-status').textContent = `Error: ${data.message}`;
            toast(data.message, 'error');
        }
    } catch (e) {
        toast('Login failed', 'error');
    }
}

async function submitOTP() {
    const otp = $('#otp-input').value.trim();
    try {
        const res = await fetch(`${API_BASE}/api/auth/2fa`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ otp: otp || null }),
        });
        const data = await res.json();
        if (data.status === 'ok') {
            $('#otp-row').style.display = 'none';
            $('#kotak-auth-status').textContent = '✅ Authenticated';
            updateBadge('badge-kotak', true);
            toast('Kotak Neo authenticated!', 'success');
        } else {
            toast(data.message, 'error');
        }
    } catch (e) {
        toast('2FA failed', 'error');
    }
}

async function sendTestSignal() {
    const text = $('#test-signal-input').value.trim();
    if (!text) return toast('Enter a signal message', 'warning');

    try {
        const res = await fetch(`${API_BASE}/api/test-signal`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text, sender: 'Test' }),
        });
        const result = await res.json();

        if (result.signal && result.signal.status === 'valid') {
            toast('Test signal sent and validated!', 'success');
        } else if (result.signal && result.signal.status === 'ignored') {
            toast(`Signal ignored: ${result.signal.reason}`, 'warning');
        } else {
            toast('Test signal sent', 'success');
        }

        $('#test-signal-input').value = '';
    } catch (e) {
        toast('Failed to send test', 'error');
    }
}

async function exitPosition(positionId) {
    if (!confirm('Exit this position at current price?')) return;
    try {
        const res = await fetch(`${API_BASE}/api/positions/${positionId}/exit`, { method: 'POST' });
        const data = await res.json();
        if (data.status === 'closed' || data.status === 'ok') {
            // [5] Do NOT remove from state here.
            // The position_update WS event with status:'closed' is the authoritative signal.
            toast(`Exit submitted — awaiting confirmation`, 'info');
        } else {
            toast(data.message || 'Failed to exit position', 'error');
        }
    } catch (e) {
        toast('Exit failed', 'error');
    }
}

async function setLotSize() {
    const lots = parseInt($('#lot-input').value);
    if (!lots || lots < 1) return toast('Lot size must be at least 1', 'warning');
    try {
        const res = await fetch(`${API_BASE}/api/settings/lot-size`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ lots }),
        });
        const data = await res.json();
        if (data.status === 'ok') {
            toast(`Lot size set to ${data.lot_size}`, 'success');
        } else {
            toast('Failed to set lot size', 'error');
        }
    } catch (e) {
        toast('Failed to set lot size', 'error');
    }
}

// ── Rendering ──
function renderAll() {
    renderMessages();
    renderSignals();
    renderPositions();
    renderTrades();
}

// [2] Fragment ordering fix: reverse new items before prepend so newest stays on top
function renderMessages() {
    const container = $('#messages-list');
    const count = $('#msg-count');
    if (!container || !count) return;
    count.textContent = state.messages.length;

    if (state.messages.length === 0) {
        container.innerHTML = '<div class="empty-state">Waiting for messages...</div>';
        return;
    }

    if (container.querySelector('.empty-state')) container.innerHTML = '';

    const currentIds = new Set([...container.querySelectorAll('.msg-bubble')].map(el => el.dataset.id));
    const newItems = state.messages.filter(m => !currentIds.has(String(m.id || m.timestamp)));

    if (newItems.length === 0) return;

    // newItems[0] is newest (unshift order). We want newest at top after prepend.
    // prepend() inserts in document order, so reverse so [0] ends up first.
    const fragment = document.createDocumentFragment();
    [...newItems].reverse().forEach(m => {
        const id = m.id || m.timestamp;
        const div = document.createElement('div');
        div.className = 'msg-bubble';
        div.dataset.id = id;
        div.innerHTML = `
            <div class="msg-sender">${esc(m.sender || 'Unknown')}</div>
            <div class="msg-text">${esc(m.raw_text || m.text || '')}</div>
            <div class="msg-time">${formatTime(m.timestamp || m.created_at)}</div>
        `;
        fragment.appendChild(div);
    });

    container.prepend(fragment);
}

// [8] Signals sorted by created_at before rendering; cards inserted in sorted DOM order
function renderSignals() {
    try {
        const container = $('#signals-list');
        const count = $('#signal-count');
        if (!container || !count) return;
        count.textContent = state.signals.length;

        if (state.signals.length === 0) {
            container.innerHTML = '<div class="empty-state">No signals parsed yet</div>';
            return;
        }

        if (container.querySelector('.empty-state')) container.innerHTML = '';

        const sorted = [...state.signals].sort((a, b) => {
            const ta = new Date(a.created_at || a.timestamp || 0).getTime();
            const tb = new Date(b.created_at || b.timestamp || 0).getTime();
            return tb - ta;
        });

        sorted.forEach((s, idx) => {
            const existing = document.getElementById(`signal-card-${s.id}`);
            const status = s.status || 'empty';
            const isValid = status === 'valid';
            const tradeStatus = s.trade_status || '';
            const timerStart = s.created_at || s.timestamp;

            const ltpVal = s.live_ltp ? `₹${s.live_ltp.toFixed(2)}` : '--';
            const sensexVal = state.sensex_ltp ? state.sensex_ltp.toFixed(2) : '--';

            let targetsText = '--';
            if (s.targets && Array.isArray(s.targets) && s.targets.length > 0) {
                targetsText = s.targets.map(t => '₹' + t).join(', ');
            } else if (typeof s.targets === 'string' && s.targets) {
                try {
                    const tArr = JSON.parse(s.targets);
                    if (Array.isArray(tArr) && tArr.length > 0) {
                        targetsText = tArr.map(t => '₹' + t).join(', ');
                    }
                } catch (e) { }
            }
            const targetsHtml = `<div><span class="label">Targets</span><br><span class="value">${targetsText}</span></div>`;

            const cardHtml = `
                <div style="display: flex; justify-content: space-between; align-items: start;">
                    <span class="signal-status ${s.status}">${s.status}</span>
                    <div style="display: flex; gap: 6px; align-items: center;">
                        ${isValid && timerStart && !['filled', 'closed', 'replaced', 'expired'].includes(tradeStatus)
                    ? `<span class="timer-tag" data-timer-start="${timerStart}" data-timer-mins="10" data-timer-label="Entry" data-trade-status="${tradeStatus}">⏳ Entry: --:--</span>`
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
                        ${s.min_ltp ? `<div><span class="label">Min LTP</span><br><span class="value">₹${s.min_ltp}</span></div>` : ''}
                        ${s.stoploss ? `<div><span class="label">SL</span><br><span class="value">₹${s.stoploss}</span></div>` : ''}
                        ${targetsHtml}
                    </div>
                ` : ''}
            `;

            if (existing) {
                existing.className = `signal-card ${tradeStatus || s.status}`;
                existing.innerHTML = cardHtml;
                // Enforce sorted DOM order
                const currentIndex = [...container.children].indexOf(existing);
                if (currentIndex !== idx) {
                    container.insertBefore(existing, container.children[idx] || null);
                }
            } else {
                const div = document.createElement('div');
                div.id = `signal-card-${s.id}`;
                div.className = `signal-card ${tradeStatus || s.status}`;
                div.innerHTML = cardHtml;
                container.insertBefore(div, container.children[idx] || null);
            }
        });
    } catch (err) {
        console.error("Error in renderSignals:", err);
    }
}

// [3][4] Upserts position cards in-place — preserves timer DOM elements, eliminates flicker
function renderPositions() {
    const container = $('#positions-list');
    const count = $('#pos-count');
    const pnlEl = $('#pnl-value');
    if (!container || !count || !pnlEl) return;

    const open = state.positions.filter(p => p.status === 'open');
    count.textContent = open.length;

    const totalPnl = open.reduce((sum, p) => sum + (p.pnl || 0), 0);
    pnlEl.textContent = `₹${totalPnl.toFixed(2)}`;
    pnlEl.className = `pnl-value ${totalPnl > 0 ? 'positive' : totalPnl < 0 ? 'negative' : ''}`;

    if (open.length === 0) {
        container.innerHTML = '<div class="empty-state">No open positions</div>';
        return;
    }

    if (container.querySelector('.empty-state')) container.innerHTML = '';

    // Remove stale cards
    const openIds = new Set(open.map(p => String(p.id)));
    container.querySelectorAll('.position-card').forEach(card => {
        if (!openIds.has(card.dataset.posId)) card.remove();
    });

    open.forEach(p => {
        const pnl = p.pnl || 0;
        const pnlClass = pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : '';
        const existing = container.querySelector(`.position-card[data-pos-id="${p.id}"]`);

        if (existing) {
            // Surgical updates only — do NOT replace innerHTML so timer elements survive
            const pnlDiv = existing.querySelector('.pos-pnl');
            if (pnlDiv) {
                pnlDiv.textContent = `${pnl >= 0 ? '+' : ''}₹${pnl.toFixed(2)}`;
                pnlDiv.className = `pos-pnl ${pnlClass}`;
            }
            const ltpSpan = existing.querySelector('.pos-meta .mono');
            if (ltpSpan) ltpSpan.textContent = `₹${(p.current_price || 0).toFixed(2)}`;
            const slTag = existing.querySelector('.sl-tag');
            if (slTag) slTag.textContent = `SL: ₹${(p.trailing_sl || 0).toFixed(2)}`;
            const maxTag = existing.querySelector('.max-tag');
            if (maxTag && p.max_ltp) maxTag.textContent = `Max: ₹${p.max_ltp.toFixed(2)}`;
        } else {
            const div = document.createElement('div');
            div.className = 'position-card';
            div.dataset.posId = String(p.id);
            div.innerHTML = `
                <div class="pos-info">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span class="pos-symbol">${esc(p.trading_symbol || '')}</span>
                        <div style="display: flex; gap: 6px; align-items: center;">
                            ${p.opened_at ? `<span class="timer-tag" data-timer-start="${p.opened_at}" data-timer-mins="10" data-timer-label="Hold">⏳ Hold: --:--</span>` : ''}
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
    const count = $('#trade-count');
    if (!container || !count) return;

    let filtered = state.trades;
    if (state.tradeFilter !== 'all') {
        filtered = state.trades.filter(t => t.status === state.tradeFilter);
    }
    count.textContent = filtered.length;

    if (filtered.length === 0) {
        container.innerHTML = '<div class="empty-state">No trades yet</div>';
        return;
    }

    container.innerHTML = `
        <table class="trade-table">
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Fill</th>
                    <th>P&L</th>
                    <th>Mode</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                ${filtered.map(t => `
                    <tr>
                        <td class="mono">${formatTime(t.created_at || t.fill_time)}</td>
                        <td class="mono">${esc(t.trading_symbol || '-')}</td>
                        <td>${t.transaction_type === 'B' ? '🟢 BUY' : '🔴 SELL'}</td>
                        <td class="mono">${t.quantity || '-'}</td>
                        <td class="mono">₹${(t.price || 0).toFixed(2)}</td>
                        <td class="mono">${t.fill_price ? '₹' + t.fill_price.toFixed(2) : '-'}</td>
                        <td class="mono" style="color: ${(t.pnl || 0) >= 0 ? 'var(--green)' : 'var(--red)'}">
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

// ── Status Updates ──
function updateStatusFromData(status) {
    if (!status) return;
    state.mode = status.mode || 'paper';
    updateModeUI();
    updateBadge('badge-telegram', status.telegram);

    const kotak = status.kotak || {};
    const isAuthenticated = kotak.authenticated;
    updateBadge('badge-kotak', isAuthenticated);

    const statusText = $('#kotak-auth-status');
    const otpRow = $('#otp-row');

    if (!statusText) return;

    const loginState = kotak.login_state || (isAuthenticated ? 'logged_in' : 'unknown');
    switch (loginState) {
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
            statusText.textContent = isAuthenticated ? '✅ Authenticated' : 'Not authenticated';
            break;
    }
}

function updateModeUI() {
    const paperBtn = $('#btn-paper');
    const realBtn = $('#btn-real');
    if (paperBtn) paperBtn.classList.toggle('active', state.mode === 'paper');
    if (realBtn) realBtn.classList.toggle('active', state.mode === 'real');
}

function updateBadge(id, connected) {
    const badge = $(`#${id}`);
    if (!badge) return;
    badge.classList.toggle('badge-connected', !!connected);
    badge.classList.toggle('badge-disconnected', !connected);
}

// ── Utilities ──
function formatTime(iso) {
    if (!iso) return '-';
    try {
        let dateStr = iso;
        if (dateStr.includes(' ') && !dateStr.includes('T')) dateStr = dateStr.replace(' ', 'T');
        if (dateStr.endsWith('+00:00')) dateStr = dateStr.replace('+00:00', 'Z');
        if (dateStr.endsWith('-00:00')) dateStr = dateStr.replace('-00:00', 'Z');
        if (!dateStr.endsWith('Z') && !dateStr.includes('+')) dateStr += 'Z';
        const d = new Date(dateStr);
        return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
        return iso;
    }
}

function getCountdown(isoStart, durationMinutes) {
    if (!isoStart) return null;
    try {
        let dateStr = isoStart;
        if (dateStr.includes(' ') && !dateStr.includes('T')) {
            dateStr = dateStr.replace(' ', 'T') + 'Z';
        } else if (!dateStr.endsWith('Z') && !dateStr.includes('+')) {
            dateStr += 'Z';
        }
        const start = new Date(dateStr).getTime();
        const now = new Date().getTime();
        const target = start + (durationMinutes * 60 * 1000);
        const diff = target - now;
        if (diff <= 0) return null;
        const m = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
        const s = Math.floor((diff % (1000 * 60)) / 1000);
        return `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
    } catch {
        return null;
    }
}

function toast(message, type = 'info') {
    const container = $('#toast-container');
    if (!container) return;
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => el.remove(), 4000);
}

// ── Strategy Modal Logic ──

const STRATEGY_DEFAULTS = {
    lots: 1,
    entryLogic: 'code',
    entryAvgPick: 'avg',
    entryFixed: null,
    trailingSL: 'code',
    slFixed: null,
};

// [1] localStorage is the fallback only — server strategy takes precedence (loaded in fetchInitialData / init WS event)
function loadStrategy() {
    try {
        const saved = localStorage.getItem('tradebridge_strategy');
        if (saved) {
            const parsed = JSON.parse(saved);
            state.strategy = { ...STRATEGY_DEFAULTS, ...parsed };
        } else {
            state.strategy = { ...STRATEGY_DEFAULTS };
        }
    } catch (e) {
        state.strategy = { ...STRATEGY_DEFAULTS };
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
        state.strategy.lots === 1 &&
        state.strategy.entryLogic === 'code' &&
        state.strategy.trailingSL === 'code'
    );
    btn.classList.toggle('strategy-active', !isDefault);
    btn.title = isDefault
        ? 'Strategy Setup'
        : `Strategy: ${state.strategy.lots} lot(s) | Entry: ${state.strategy.entryLogic} | SL: ${state.strategy.trailingSL}`;
}

function syncStrategyModalToState() {
    const s = state.strategy;

    const sel = $('#strategy-lots-select');
    if (sel) sel.value = s.lots;

    $$('input[name="lots-quick"]').forEach(r => {
        r.checked = parseInt(r.value) === s.lots;
    });

    $$('input[name="entry-logic"]').forEach(r => {
        r.checked = r.value === s.entryLogic;
    });
    const entryFixedRow = $('#entry-fixed-row');
    if (entryFixedRow) entryFixedRow.style.display = s.entryLogic === 'fixed' ? 'block' : 'none';
    const entryAvgRow = $('#entry-avg-row');
    if (entryAvgRow) entryAvgRow.style.display = s.entryLogic === 'avg_signal' ? 'block' : 'none';
    const entryFixedInput = $('#entry-fixed-price');
    if (entryFixedInput && s.entryFixed) entryFixedInput.value = s.entryFixed;
    $$('input[name="entry-avg-pick"]').forEach(r => {
        r.checked = r.value === (s.entryAvgPick || 'avg');
    });

    $$('input[name="trailing-sl"]').forEach(r => {
        r.checked = r.value === s.trailingSL;
    });
    const slFixedRow = $('#sl-fixed-row');
    if (slFixedRow) slFixedRow.style.display = s.trailingSL === 'fixed' ? 'block' : 'none';
    const slFixedInput = $('#sl-fixed-price');
    if (slFixedInput && s.slFixed) slFixedInput.value = s.slFixed;
}

function populateLotDropdown() {
    const sel = $('#strategy-lots-select');
    if (!sel || sel.options.length > 0) return;
    for (let i = 1; i <= 50; i++) {
        const opt = document.createElement('option');
        opt.value = i;
        opt.textContent = `${i} Lot${i > 1 ? 's' : ''}`;
        sel.appendChild(opt);
    }
}

function bindStrategyModal() {
    const btnStrategy = $('#btn-strategy');
    if (btnStrategy) {
        btnStrategy.addEventListener('click', () => {
            populateLotDropdown();
            syncStrategyModalToState();
            $('#strategy-modal').style.display = 'flex';
        });
    }

    const btnClose = $('#btn-close-strategy');
    if (btnClose) btnClose.addEventListener('click', () => {
        $('#strategy-modal').style.display = 'none';
    });

    const lotsSelect = $('#strategy-lots-select');
    if (lotsSelect) {
        lotsSelect.addEventListener('change', () => {
            const val = parseInt(lotsSelect.value);
            $$('input[name="lots-quick"]').forEach(r => {
                r.checked = parseInt(r.value) === val;
            });
            const lotInput = $('#lot-input');
            if (lotInput) lotInput.value = val;
        });
    }

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
            const avgRow = $('#entry-avg-row');
            if (fixedRow) fixedRow.style.display = radio.value === 'fixed' ? 'block' : 'none';
            if (avgRow) avgRow.style.display = radio.value === 'avg_signal' ? 'block' : 'none';
        });
    });

    $$('input[name="trailing-sl"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const fixedRow = $('#sl-fixed-row');
            if (fixedRow) fixedRow.style.display = radio.value === 'fixed' ? 'block' : 'none';
        });
    });

    const btnReset = $('#btn-strategy-reset');
    if (btnReset) {
        btnReset.addEventListener('click', () => {
            state.strategy = { ...STRATEGY_DEFAULTS };
            populateLotDropdown();
            syncStrategyModalToState();
            const lotInput = $('#lot-input');
            if (lotInput) lotInput.value = 1;
            persistStrategy();
            toast('Strategy reset to defaults', 'info');
        });
    }

    const btnSave = $('#btn-strategy-save');
    if (btnSave) {
        btnSave.addEventListener('click', async () => {
            const sel = $('#strategy-lots-select');
            const lots = sel ? parseInt(sel.value) : 1;

            const entryRadio = document.querySelector('input[name="entry-logic"]:checked');
            const entryLogic = entryRadio ? entryRadio.value : 'code';
            const entryFixedVal = entryLogic === 'fixed' ? (parseFloat($('#entry-fixed-price')?.value) || null) : null;
            const avgPickRadio = document.querySelector('input[name="entry-avg-pick"]:checked');
            const entryAvgPick = entryLogic === 'avg_signal' ? (avgPickRadio?.value || 'avg') : 'avg';

            const slRadio = document.querySelector('input[name="trailing-sl"]:checked');
            const trailingSL = slRadio ? slRadio.value : 'code';
            const slFixedVal = trailingSL === 'fixed' ? (parseFloat($('#sl-fixed-price')?.value) || null) : null;

            if (entryLogic === 'fixed' && !entryFixedVal) {
                toast('Please enter a fixed entry price', 'warning');
                return;
            }
            if (trailingSL === 'fixed' && !slFixedVal) {
                toast('Please enter a fixed SL price', 'warning');
                return;
            }

            state.strategy = { lots, entryLogic, entryAvgPick, entryFixed: entryFixedVal, trailingSL, slFixed: slFixedVal };
            persistStrategy();

            const lotInput = $('#lot-input');
            if (lotInput) lotInput.value = lots;

            try {
                await fetch(`${API_BASE}/api/settings/strategy`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(state.strategy),
                });
            } catch (e) {
                console.warn('Could not sync strategy to backend:', e);
            }

            try {
                await fetch(`${API_BASE}/api/settings/lot-size`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ lots }),
                });
            } catch (e) {
                console.warn('Could not sync lot size to backend:', e);
            }

            toast(`Strategy saved: ${lots} lot(s) | Entry: ${entryLogic} | SL: ${trailingSL}`, 'success');
            $('#strategy-modal').style.display = 'none';
        });
    }
}