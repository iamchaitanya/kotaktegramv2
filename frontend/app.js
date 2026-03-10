/**
 * TradeBridge — Frontend Application
 * Connects to FastAPI backend via REST + WebSocket
 * Renders real-time trading dashboard
 */

const API_BASE = window.location.origin;
const WS_URL = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host + '/ws';

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
};

let ws = null;
let reconnectTimer = null;

// ── DOM References ──
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// ── Init ──
document.addEventListener('DOMContentLoaded', () => {
    bindEvents();
    connectWebSocket();
    fetchInitialData();

    // Update only timer text every second — no full DOM rebuild = no flicker
    setInterval(() => {
        document.querySelectorAll('[data-timer-start]').forEach(el => {
            const start = el.getAttribute('data-timer-start');
            const mins = parseInt(el.getAttribute('data-timer-mins') || '10');
            const label = el.getAttribute('data-timer-label') || 'Timer';
            const countdown = getCountdown(start, mins);
            if (countdown === null) {
                // Timer expired — update timer tag independently
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
    // Hamburger menu toggle (mobile)
    const hamburger = $('#btn-hamburger');
    const headerMenu = $('#header-menu');
    hamburger.addEventListener('click', () => {
        headerMenu.classList.toggle('open');
        hamburger.classList.toggle('open');
    });
    // Close menu on tap outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.header-right') && headerMenu.classList.contains('open')) {
            headerMenu.classList.remove('open');
            hamburger.classList.remove('open');
        }
    });

    // Mode toggle
    $('#btn-paper').addEventListener('click', () => setMode('paper'));
    $('#btn-real').addEventListener('click', () => {
        // Show confirmation for real mode
        $('#confirm-real-modal').style.display = 'flex';
    });

    // Confirm real mode
    $('#btn-confirm-real').addEventListener('click', () => {
        $('#confirm-real-modal').style.display = 'none';
        setMode('real');
    });
    $('#btn-cancel-real').addEventListener('click', () => {
        $('#confirm-real-modal').style.display = 'none';
    });

    // Settings
    $('#btn-settings').addEventListener('click', () => {
        $('#settings-modal').style.display = 'flex';
    });
    $('#btn-close-settings').addEventListener('click', () => {
        $('#settings-modal').style.display = 'none';
    });

    // Clear Dashboard Data
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

    // Kill switch
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

    // Lot size control
    $('#btn-set-lots').addEventListener('click', setLotSize);
    $('#lot-input').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') setLotSize();
    });

    // Kotak login
    $('#btn-kotak-login').addEventListener('click', kotakLogin);
    $('#btn-submit-otp').addEventListener('click', submitOTP);

    // Test signal
    $('#btn-send-test').addEventListener('click', sendTestSignal);

    // Trade filters
    $$('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            $$('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.tradeFilter = btn.dataset.filter;
            renderTrades();
        });
    });

    // Close modals on overlay click
    $$('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) overlay.style.display = 'none';
        });
    });
}

// ── WebSocket ──
function connectWebSocket() {
    if (ws && ws.readyState === WebSocket.OPEN) return;

    ws = new WebSocket(WS_URL);

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
        updateBadge('badge-ws', false);
        // Auto-reconnect
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

        // Log non-tick messages
        if (msg.type !== 'instrument_ltp' && msg.type !== 'index_ltp') {
            console.log("WS Received:", msg.type, msg.data);
        }

        switch (msg.type) {
            case 'init':
                // Full state sync on connect
                state.messages = msg.data.messages || [];
                state.signals = msg.data.signals || [];
                state.trades = msg.data.trades || [];
                state.positions = msg.data.positions || [];
                updateStatusFromData(msg.data.status);
                renderAll();
                break;

            case 'new_message':
                state.messages.unshift(msg.data);
                renderMessages();
                break;

            case 'new_signal':
                console.log("Adding new signal:", msg.data);
                // Deduplicate: replace existing signal for same strike+option_type
                if (msg.data.strike && msg.data.option_type) {
                    const existingIdx = state.signals.findIndex(s =>
                        s.strike === msg.data.strike && s.option_type === msg.data.option_type
                    );
                    if (existingIdx !== -1) {
                        state.signals.splice(existingIdx, 1);
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
                    state.trades.unshift(msg.data);

                    // If this trade is a fill, add it to positions
                    if (msg.data.status === 'filled') {
                        if (!state.positions.some(p => p.id === msg.data.id || p.id === msg.data.position_id)) {
                            state.positions.unshift(msg.data);
                        }
                        // Update matching signal card
                        if (msg.data.signal_id) {
                            const sigIdx = state.signals.findIndex(s => s.id === msg.data.signal_id);
                            if (sigIdx !== -1) {
                                state.signals[sigIdx].trade_status = 'filled';
                                state.signals[sigIdx].status_note = `Filled @ ₹${(msg.data.entry_price || msg.data.fill_price || 0).toFixed(2)}`;
                                renderSignals();
                            }
                        }
                    }

                    renderTrades();
                    renderPositions();
                    const status = msg.data.status || 'pending';
                    toast(`Trade ${status}: ${msg.data.trading_symbol || ''}`, status === 'filled' ? 'success' : 'info');
                }
                break;

            case 'mode_change':
                state.mode = msg.data.new_mode;
                updateModeUI();
                toast(`Mode: ${msg.data.new_mode.toUpperCase()}`, 'warning');
                break;

            case 'order_update':
                // Update signal card via signal_id
                if (msg.data.signal_id) {
                    const sigIdx = state.signals.findIndex(s => s.id === msg.data.signal_id);
                    if (sigIdx !== -1) {
                        if (msg.data.status) state.signals[sigIdx].trade_status = msg.data.status;
                        if (msg.data.status_note) state.signals[sigIdx].status_note = msg.data.status_note;
                        if (msg.data.min_ltp) state.signals[sigIdx].min_ltp = msg.data.min_ltp;
                        renderSignals();
                    }
                }
                // Update trade log
                const trdIdx = state.trades.findIndex(t => t.id === msg.data.id);
                if (trdIdx !== -1) {
                    state.trades[trdIdx] = { ...state.trades[trdIdx], ...msg.data };
                    renderTrades();
                }
                break;

            case 'instrument_ltp':
                // Live LTP for any subscribed instrument — update matching signal cards directly
                if (msg.data.symbol) {
                    const incomingSymbol = msg.data.symbol.toUpperCase();
                    state.signals.forEach(s => {
                        const idxStr = (s.idx || s.index || 'SENSEX').toUpperCase().replace(/\s/g, '');
                        const suffixStr = `${s.strike}${s.option_type}`.toUpperCase().replace(/\s/g, '');
                        
                        // Match e.g. "SENSEX2631278500CE" and "SENSEX78500CE"
                        if (incomingSymbol.startsWith(idxStr) && incomingSymbol.endsWith(suffixStr)) {
                            s.live_ltp = msg.data.ltp;
                            const el = document.getElementById(`signal-ltp-${s.id}`);
                            if (el) el.textContent = `₹${s.live_ltp.toFixed(2)}`;
                        }
                    });
                }
                break;

            case 'index_ltp':
                // SENSEX index spot price — update matching signal cards directly
                state.sensex_ltp = msg.data.ltp || 0;
                const sensexEls = document.querySelectorAll('.signal-sensex-ltp');
                sensexEls.forEach(el => {
                    el.textContent = (state.sensex_ltp || 0).toFixed(2);
                });
                break;

            case 'position_update':
                // Real-time PNL and SL updates
                const posIdx = state.positions.findIndex(p => p.id === msg.data.id);
                if (posIdx !== -1) {
                    state.positions[posIdx] = { ...state.positions[posIdx], ...msg.data };
                    renderPositions();
                }
                break;

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
            // Set lot size from server
            if (status.lot_size) {
                const lotInput = $('#lot-input');
                if (lotInput) lotInput.value = status.lot_size;
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
        await fetch(`${API_BASE}/api/test-signal`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text, sender: 'Test' }),
        });
        toast('Test signal sent', 'success');
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
        if (data.status === 'closed') {
            // Remove from local state
            state.positions = state.positions.filter(p => p.id !== positionId);
            renderPositions();
            toast(`Position closed — P&L: ₹${(data.pnl || 0).toFixed(2)}`, data.pnl >= 0 ? 'success' : 'warning');
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

function renderMessages() {
    const container = $('#messages-list');
    const count = $('#msg-count');
    if (!container || !count) return;
    count.textContent = state.messages.length;

    if (state.messages.length === 0) {
        container.innerHTML = '<div class="empty-state">Waiting for messages...</div>';
        return;
    }

    container.innerHTML = state.messages.map(m => `
        <div class="msg-bubble">
            <div class="msg-sender">${esc(m.sender || 'Unknown')}</div>
            <div class="msg-text">${esc(m.raw_text || m.text || '')}</div>
            <div class="msg-time">${formatTime(m.timestamp || m.created_at)}</div>
        </div>
    `).join('');
}

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

        container.innerHTML = state.signals.map(s => {
            const status = s.status || 'empty';
            const isValid = status === 'valid';
            const tradeStatus = s.trade_status || '';

            let details = '';
            if (isValid) {
                const ltpVal = s.live_ltp ? `₹${s.live_ltp.toFixed(2)}` : '--';
                const sensexVal = state.sensex_ltp ? state.sensex_ltp.toFixed(2) : '--';
                
                const ltpStr = `<div><span class="label">LTP</span><br><span class="value ltp-live" id="signal-ltp-${s.id}">${ltpVal}</span></div>`;
                const sensexStr = `<div><span class="label">SENSEX</span><br><span class="value ltp-live signal-sensex-ltp">${sensexVal}</span></div>`;
                
                details = `
                    <div class="signal-details">
                        <div><span class="label">Index</span><br><span class="value">${esc(s.idx || s.index || '')}</span></div>
                        <div><span class="label">Type</span><br><span class="value">${esc(s.option_type || '')}</span></div>
                        <div><span class="label">Strike</span><br><span class="value">${esc(s.strike || '')}</span></div>
                        <div><span class="label">Entry</span><br><span class="value">₹${s.entry_low || 0} - ₹${s.entry_high || 0}</span></div>
                        ${sensexStr}
                        ${ltpStr}
                        ${s.min_ltp ? `<div><span class="label">Min LTP</span><br><span class="value">₹${s.min_ltp}</span></div>` : ''}
                    </div>
                `;
            }

            const tradeTag = tradeStatus && tradeStatus !== 'valid' 
                ? `<span class="signal-status ${tradeStatus}">${tradeStatus}</span>` 
                : '';

            const timerStart = s.created_at || s.timestamp;
            let timerTag = '';
            if (isValid && timerStart && !['filled', 'closed', 'replaced', 'expired'].includes(tradeStatus)) {
                timerTag = `<span class="timer-tag" data-timer-start="${timerStart}" data-timer-mins="10" data-timer-label="Entry">⏳ Entry: --:--</span>`;
            }

            return `
                <div class="signal-card ${tradeStatus || s.status}" id="signal-card-${s.id}">
                    <div style="display: flex; justify-content: space-between; align-items: start;">
                        <span class="signal-status ${s.status}">${s.status}</span>
                        <div style="display: flex; gap: 6px; align-items: center;">
                            ${timerTag}
                            ${tradeTag}
                        </div>
                    </div>
                    ${s.status_note ? `<div class="signal-note">ℹ️ ${esc(s.status_note)}</div>` : ''}
                    ${s.reason ? `<div class="signal-reason">${esc(s.reason)}</div>` : ''}
                    ${details}
                </div>
            `;
        }).join('');
    } catch (err) {
        console.error("Error in renderSignals:", err);
    }
}

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

    container.innerHTML = open.map(p => {
        const pnl = p.pnl || 0;
        const pnlClass = pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : '';

        let countdownStr = '';
        if (p.opened_at) {
            const cd = getCountdown(p.opened_at, 10);
            if (cd) {
                countdownStr = `<span class="timer-tag" data-timer-start="${p.opened_at}" data-timer-mins="10" data-timer-label="Hold">⏳ Hold: ${cd}</span>`;
            }
        }

        return `
            <div class="position-card">
                <div class="pos-info">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span class="pos-symbol">${esc(p.trading_symbol || '')}</span>
                        <div style="display: flex; gap: 6px; align-items: center;">
                            ${countdownStr}
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
                        ${p.status_note ? `<span class="note-tag">${esc(p.status_note)}</span>` : ''}
                    </div>
                </div>
                <div class="pos-pnl ${pnlClass}">${pnl >= 0 ? '+' : ''}₹${pnl.toFixed(2)}</div>
            </div>
        `;
    }).join('');
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

    const isAuthenticated = status.kotak?.authenticated;
    updateBadge('badge-kotak', isAuthenticated);

    if (isAuthenticated) {
        const otpRow = $('#otp-row');
        if (otpRow) otpRow.style.display = 'none';
        const statusText = $('#kotak-auth-status');
        if (statusText) statusText.textContent = '✅ Authenticated';
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
function esc(str) {
    const div = document.createElement('div');
    div.textContent = str || '';
    return div.innerHTML;
}

function formatTime(iso) {
    if (!iso) return '-';
    try {
        let dateStr = iso;
        if (dateStr.includes(' ') && !dateStr.includes('T')) {
            dateStr = dateStr.replace(' ', 'T');
        }
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
