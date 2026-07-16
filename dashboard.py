"""
HYPERLIQUID DASHBOARD
Mobile-first Flask app — designed for iPhone
Password: hl2026
"""

from flask import Flask, render_template_string, request, session, redirect, url_for, jsonify
import state as S
import os

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "hl2026secret")
PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "hl2026")

HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>HL Trader</title>
<style>
  @import url("https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap");

  :root {
    --bg:       #080B10;
    --surface:  #0F1520;
    --surface2: #161E2E;
    --border:   #1E2D42;
    --green:    #00D68F;
    --red:      #FF4757;
    --gold:     #FFB800;
    --blue:     #3D9EFF;
    --text:     #E8EDF5;
    --muted:    #4A5878;
    --mono:     "JetBrains Mono", monospace;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: "Inter", sans-serif;
    min-height: 100vh;
    padding-bottom: env(safe-area-inset-bottom);
  }

  /* ── LOGIN ── */
  .login-wrap {
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
  }
  .login-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 40px 32px;
    width: 100%;
    max-width: 360px;
    text-align: center;
  }
  .login-logo {
    font-family: var(--mono);
    font-size: 28px;
    font-weight: 700;
    color: var(--green);
    letter-spacing: -1px;
    margin-bottom: 8px;
  }
  .login-sub {
    color: var(--muted);
    font-size: 13px;
    margin-bottom: 32px;
  }
  .login-input {
    width: 100%;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 12px;
    color: var(--text);
    font-size: 16px;
    padding: 14px 16px;
    margin-bottom: 12px;
    outline: none;
    font-family: var(--mono);
    letter-spacing: 2px;
  }
  .login-input:focus { border-color: var(--green); }
  .login-btn {
    width: 100%;
    background: var(--green);
    color: #000;
    border: none;
    border-radius: 12px;
    font-size: 15px;
    font-weight: 700;
    padding: 14px;
    cursor: pointer;
    margin-top: 4px;
  }
  .login-error {
    color: var(--red);
    font-size: 13px;
    margin-top: 12px;
  }

  /* ── HEADER ── */
  .header {
    position: sticky;
    top: 0;
    z-index: 100;
    background: rgba(8,11,16,0.92);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border-bottom: 1px solid var(--border);
    padding: 16px 20px 12px;
    padding-top: calc(16px + env(safe-area-inset-top));
  }
  .header-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .header-logo {
    font-family: var(--mono);
    font-size: 18px;
    font-weight: 700;
    color: var(--green);
    letter-spacing: -0.5px;
  }
  .status-pill {
    display: flex;
    align-items: center;
    gap: 6px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 5px 12px;
    font-size: 12px;
    font-weight: 600;
  }
  .status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--green);
    animation: pulse 2s infinite;
  }
  .status-dot.paused { background: var(--gold); animation: none; }
  .status-dot.stopped { background: var(--red); animation: none; }
  @keyframes pulse {
    0%,100% { opacity:1; } 50% { opacity:0.4; }
  }
  .mode-badge {
    font-size: 10px;
    font-weight: 700;
    padding: 3px 8px;
    border-radius: 6px;
    margin-top: 8px;
    display: inline-block;
    letter-spacing: 0.5px;
  }
  .mode-dry  { background: rgba(61,158,255,0.15); color: var(--blue); border: 1px solid rgba(61,158,255,0.3); }
  .mode-test { background: rgba(255,184,0,0.15);  color: var(--gold); border: 1px solid rgba(255,184,0,0.3); }
  .mode-live { background: rgba(0,214,143,0.15);  color: var(--green);border: 1px solid rgba(0,214,143,0.3); }

  /* ── TABS ── */
  .tabs {
    display: flex;
    overflow-x: auto;
    scrollbar-width: none;
    padding: 0 20px;
    gap: 4px;
    margin-top: 12px;
    border-bottom: 1px solid var(--border);
  }
  .tabs::-webkit-scrollbar { display: none; }
  .tab {
    flex-shrink: 0;
    padding: 8px 16px 10px;
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    cursor: pointer;
    border-bottom: 2px solid transparent;
    white-space: nowrap;
    transition: all 0.2s;
  }
  .tab.active { color: var(--green); border-bottom-color: var(--green); }

  /* ── MAIN ── */
  .main { padding: 20px; }
  .section { display: none; }
  .section.active { display: block; }

  /* ── CARDS ── */
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 16px;
    margin-bottom: 12px;
  }
  .card-label {
    font-size: 10px;
    font-weight: 700;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 6px;
  }
  .card-value {
    font-family: var(--mono);
    font-size: 28px;
    font-weight: 700;
    line-height: 1;
  }
  .card-sub {
    font-size: 12px;
    color: var(--muted);
    margin-top: 4px;
  }
  .green  { color: var(--green); }
  .red    { color: var(--red); }
  .gold   { color: var(--gold); }
  .blue   { color: var(--blue); }
  .muted  { color: var(--muted); }

  /* ── STAT GRID ── */
  .stat-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-bottom: 12px;
  }
  .stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 14px;
  }
  .stat-label {
    font-size: 10px;
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 6px;
  }
  .stat-value {
    font-family: var(--mono);
    font-size: 18px;
    font-weight: 700;
  }

  /* ── POSITION CARD ── */
  .pos-card {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 14px;
    margin-bottom: 10px;
  }
  .pos-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
  }
  .pos-asset {
    font-family: var(--mono);
    font-size: 16px;
    font-weight: 700;
  }
  .pos-dir {
    font-size: 11px;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 6px;
  }
  .pos-long  { background: rgba(0,214,143,0.15); color: var(--green); }
  .pos-short { background: rgba(255,71,87,0.15);  color: var(--red); }
  .pos-row {
    display: flex;
    justify-content: space-between;
    font-size: 12px;
    margin-bottom: 4px;
  }
  .pos-key   { color: var(--muted); }
  .pos-val   { font-family: var(--mono); font-weight: 500; }
  .trail-bar {
    height: 3px;
    background: var(--border);
    border-radius: 2px;
    margin-top: 10px;
    overflow: hidden;
  }
  .trail-fill {
    height: 100%;
    background: var(--green);
    border-radius: 2px;
    transition: width 0.5s;
  }

  /* ── TRADE ROW ── */
  .trade-row {
    display: flex;
    align-items: center;
    padding: 12px 0;
    border-bottom: 1px solid var(--border);
    gap: 12px;
  }
  .trade-row:last-child { border-bottom: none; }
  .trade-icon {
    width: 36px;
    height: 36px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    flex-shrink: 0;
  }
  .trade-icon.win  { background: rgba(0,214,143,0.15); }
  .trade-icon.loss { background: rgba(255,71,87,0.15); }
  .trade-icon.open { background: rgba(61,158,255,0.15); }
  .trade-info { flex: 1; min-width: 0; }
  .trade-asset {
    font-weight: 600;
    font-size: 14px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .trade-time { font-size: 11px; color: var(--muted); margin-top: 2px; }
  .trade-pnl  { font-family: var(--mono); font-weight: 700; font-size: 15px; text-align: right; }

  /* ── TAX SUMMARY ── */
  .tax-breakdown {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow: hidden;
  }
  .tax-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 13px 16px;
    border-bottom: 1px solid var(--border);
  }
  .tax-row:last-child { border-bottom: none; }
  .tax-key  { font-size: 13px; color: var(--muted); }
  .tax-val  { font-family: var(--mono); font-weight: 600; font-size: 14px; }
  .tax-divider {
    padding: 8px 16px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--muted);
    background: var(--bg);
  }

  /* ── DIAGNOSTICS ── */
  .diag-row {
    display: flex;
    gap: 10px;
    padding: 12px 0;
    border-bottom: 1px solid var(--border);
    align-items: flex-start;
  }
  .diag-row:last-child { border-bottom: none; }
  .diag-badge {
    font-size: 10px;
    font-weight: 700;
    padding: 3px 7px;
    border-radius: 6px;
    white-space: nowrap;
    margin-top: 2px;
    letter-spacing: 0.3px;
  }
  .diag-INFO     { background: rgba(61,158,255,0.15);  color: var(--blue); }
  .diag-WARNING  { background: rgba(255,184,0,0.15);   color: var(--gold); }
  .diag-ERROR    { background: rgba(255,71,87,0.15);   color: var(--red); }
  .diag-CRITICAL { background: rgba(255,71,87,0.25);   color: var(--red); border: 1px solid var(--red); }
  .diag-body   { flex: 1; min-width: 0; }
  .diag-event  { font-weight: 600; font-size: 13px; margin-bottom: 2px; }
  .diag-cause  { font-size: 11px; color: var(--muted); margin-bottom: 2px; }
  .diag-action { font-size: 11px; color: var(--blue); }
  .diag-time   { font-size: 10px; color: var(--muted); margin-top: 3px; font-family: var(--mono); }

  /* ── EMPTY STATE ── */
  .empty {
    text-align: center;
    padding: 48px 24px;
    color: var(--muted);
  }
  .empty-icon { font-size: 36px; margin-bottom: 12px; }
  .empty-text { font-size: 14px; }

  /* ── REFRESH BTN ── */
  .refresh-btn {
    position: fixed;
    bottom: calc(24px + env(safe-area-inset-bottom));
    right: 20px;
    width: 48px;
    height: 48px;
    border-radius: 50%;
    background: var(--green);
    color: #000;
    border: none;
    font-size: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    box-shadow: 0 4px 20px rgba(0,214,143,0.4);
    z-index: 50;
  }

  /* ── WEEKLY CHART ── */
  .week-bars {
    display: flex;
    align-items: flex-end;
    gap: 4px;
    height: 80px;
    padding: 0 4px;
    margin-top: 12px;
  }
  .week-bar-wrap { flex: 1; display: flex; flex-direction: column; align-items: center; gap: 4px; }
  .week-bar {
    width: 100%;
    border-radius: 4px 4px 0 0;
    min-height: 4px;
    transition: height 0.4s;
  }
  .week-bar.pos { background: var(--green); opacity: 0.8; }
  .week-bar.neg { background: var(--red);   opacity: 0.8; }
  .week-label   { font-size: 9px; color: var(--muted); font-family: var(--mono); }

  .section-title {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
    margin-bottom: 12px;
  }
</style>
</head>
<body>

{% if not logged_in %}
<div class="login-wrap">
  <div class="login-card">
    <div class="login-logo">HL TRADER</div>
    <div class="login-sub">HyperLiquid Strategy Dashboard</div>
    <form method="POST" action="/login">
      <input class="login-input" type="password" name="password" placeholder="Password" autofocus>
      <button class="login-btn" type="submit">Enter</button>
      {% if error %}<div class="login-error">{{ error }}</div>{% endif %}
    </form>
  </div>
</div>

{% else %}
<!-- HEADER -->
<div class="header">
  <div class="header-row">
    <div class="header-logo">HL TRADER</div>
    <div class="status-pill">
      <div class="status-dot {% if st.status == 'waiting' or st.status == 'checking' %}{% elif st.status == 'stopped' %}stopped{% else %}paused{% endif %}"></div>
      {{ st.status|upper }}
    </div>
  </div>
  <div>
    {% if st.dry_run %}
    <span class="mode-badge mode-dry">DRY RUN</span>
    {% elif st.testnet %}
    <span class="mode-badge mode-test">TESTNET</span>
    {% else %}
    <span class="mode-badge mode-live">● LIVE</span>
    {% endif %}
    <span style="font-size:11px;color:var(--muted);margin-left:8px;">{{ st.leverage }}x · {{ st.last_check or 'starting...' }}</span>
  </div>
  <!-- TABS -->
  <div class="tabs" style="margin:0;padding:0;border:0;margin-top:12px;">
    <div class="tab active" onclick="showTab('overview',this)">Overview</div>
    <div class="tab" onclick="showTab('positions',this)">Positions</div>
    <div class="tab" onclick="showTab('trades',this)">Trades</div>
    <div class="tab" onclick="showTab('tax',this)">Tax</div>
    <div class="tab" onclick="showTab('diagnostics',this)">Diagnostics</div>
  </div>
</div>

<!-- MAIN -->
<div class="main">

  <!-- OVERVIEW -->
  <div id="overview" class="section active">

    {% set pnl = st.tax.total_pnl %}
    {% set net = st.tax.total_net %}
    {% set tax = st.tax.total_tax %}

    <div class="card" style="border-color:{% if pnl >= 0 %}rgba(0,214,143,0.3){% else %}rgba(255,71,87,0.3){% endif %}">
      <div class="card-label">Net P&L (after tax)</div>
      <div class="card-value {% if net >= 0 %}green{% else %}red{% endif %}">
        ${{ "%.2f"|format(net) }}
      </div>
      <div class="card-sub">Gross: ${{ "%.2f"|format(pnl) }} · Tax owed: ${{ "%.2f"|format(tax) }}</div>
    </div>

    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">Balance</div>
        <div class="stat-value">${{ "%.2f"|format(st.balance) }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Open Positions</div>
        <div class="stat-value blue">{{ st.positions|length }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Trades</div>
        <div class="stat-value">{{ st.tax.total_trades }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Win Rate</div>
        {% if st.tax.total_trades > 0 %}
        <div class="stat-value {% if st.tax.winning_trades / st.tax.total_trades >= 0.6 %}green{% else %}gold{% endif %}">
          {{ "%.0f"|format(st.tax.winning_trades / st.tax.total_trades * 100) }}%
        </div>
        {% else %}
        <div class="stat-value muted">—</div>
        {% endif %}
      </div>
    </div>

    <!-- Weekly P&L bars -->
    {% if st.weekly_pnl %}
    <div class="card">
      <div class="card-label">Weekly P&L</div>
      {% set weeks = st.weekly_pnl.items()|list %}
      {% set max_abs = namespace(val=1) %}
      {% for w,v in weeks %}{% if v|abs > max_abs.val %}{% set max_abs.val = v|abs %}{% endif %}{% endfor %}
      <div class="week-bars">
        {% for week, val in weeks[-8:] %}
        {% set h = (val|abs / max_abs.val * 70)|int %}
        <div class="week-bar-wrap">
          <div class="week-bar {% if val >= 0 %}pos{% else %}neg{% endif %}" style="height:{{ h }}px"></div>
          <div class="week-label">W{{ loop.index }}</div>
        </div>
        {% endfor %}
      </div>
    </div>
    {% endif %}

    <div class="card">
      <div class="card-label">System Info</div>
      <div style="font-size:12px;line-height:2;font-family:var(--mono);">
        <div style="display:flex;justify-content:space-between;">
          <span style="color:var(--muted)">Strategy</span>
          <span>EMA 5/13/34</span>
        </div>
        <div style="display:flex;justify-content:space-between;">
          <span style="color:var(--muted)">Stop / Trail</span>
          <span>5% / 1%</span>
        </div>
        <div style="display:flex;justify-content:space-between;">
          <span style="color:var(--muted)">Assets</span>
          <span>BTC · ETH · SOL</span>
        </div>
        <div style="display:flex;justify-content:space-between;">
          <span style="color:var(--muted)">Checks</span>
          <span>Every 15 min</span>
        </div>
        <div style="display:flex;justify-content:space-between;">
          <span style="color:var(--muted)">Cycle</span>
          <span>#{{ st.cycle }}</span>
        </div>
        <div style="display:flex;justify-content:space-between;">
          <span style="color:var(--muted)">Next check</span>
          <span>{{ st.next_check or '—' }}</span>
        </div>
      </div>
    </div>
  </div>

  <!-- POSITIONS -->
  <div id="positions" class="section">
    {% if st.positions %}
    {% for asset, pos in st.positions.items() %}
    <div class="pos-card">
      <div class="pos-header">
        <div class="pos-asset">{{ asset }}-PERP</div>
        <div class="pos-dir {% if pos.direction == 'LONG' %}pos-long{% else %}pos-short{% endif %}">
          {{ pos.direction }}
        </div>
      </div>
      <div class="pos-row">
        <span class="pos-key">Entry</span>
        <span class="pos-val">${{ "{:,.2f}".format(pos.entry) }}</span>
      </div>
      <div class="pos-row">
        <span class="pos-key">Hard Stop</span>
        <span class="pos-val red">${{ "{:,.2f}".format(pos.stop) }}</span>
      </div>
      <div class="pos-row">
        <span class="pos-key">Trail Stop</span>
        <span class="pos-val gold">${{ "{:,.2f}".format(pos.trail_stop) }}</span>
      </div>
      <div class="pos-row">
        <span class="pos-key">Size</span>
        <span class="pos-val">{{ pos.size }}</span>
      </div>
      <div class="trail-bar">
        {% set trail_pct = ((pos.trail_stop - pos.stop) / (pos.entry - pos.stop) * 100)|int if pos.entry != pos.stop else 50 %}
        <div class="trail-fill" style="width:{{ [0,[100,trail_pct]|min]|max }}%"></div>
      </div>
    </div>
    {% endfor %}
    {% else %}
    <div class="empty">
      <div class="empty-icon">📭</div>
      <div class="empty-text">No open positions</div>
      <div style="font-size:12px;color:var(--muted);margin-top:6px;">Waiting for signals...</div>
    </div>
    {% endif %}
  </div>

  <!-- TRADES -->
  <div id="trades" class="section">
    <div class="section-title">Trade History</div>
    {% if st.trades %}
    <div class="card" style="padding:0 16px;">
      {% for t in st.trades[:50] %}
      {% set is_exit = t.action == 'EXIT' %}
      {% set is_win  = t.pnl is not none and t.pnl >= 0 %}
      <div class="trade-row">
        <div class="trade-icon {% if not is_exit %}open{% elif is_win %}win{% else %}loss{% endif %}">
          {% if not is_exit %}📊{% elif is_win %}✅{% else %}❌{% endif %}
        </div>
        <div class="trade-info">
          <div class="trade-asset">
            {{ t.asset }}
            <span style="font-size:11px;font-weight:400;padding:2px 6px;border-radius:4px;
              {% if t.direction=='LONG' %}background:rgba(0,214,143,0.15);color:var(--green)
              {% else %}background:rgba(255,71,87,0.15);color:var(--red){% endif %}">
              {{ t.direction }}
            </span>
            <span style="font-size:10px;color:var(--muted);">{{ t.action }}</span>
          </div>
          <div class="trade-time">
            ${{ "{:,.2f}".format(t.entry) }}
            {% if t.exit %} → ${{ "{:,.2f}".format(t.exit) }}{% endif %}
            · {{ t.reason or '' }}
          </div>
          <div class="trade-time">{{ t.time }}</div>
        </div>
        {% if t.pnl is not none %}
        <div class="trade-pnl {% if is_win %}green{% else %}red{% endif %}">
          ${{ "%+.2f"|format(t.pnl) }}
        </div>
        {% endif %}
      </div>
      {% endfor %}
    </div>
    {% else %}
    <div class="empty">
      <div class="empty-icon">📋</div>
      <div class="empty-text">No trades yet</div>
    </div>
    {% endif %}
  </div>

  <!-- TAX -->
  <div id="tax" class="section">
    <div class="card" style="border-color:rgba(255,184,0,0.3);">
      <div class="card-label">Tax Set-Aside (35%)</div>
      <div class="card-value gold">${{ "%.2f"|format(st.tax.total_tax) }}</div>
      <div class="card-sub">Move this to savings — don't spend it</div>
    </div>

    <div class="tax-breakdown">
      <div class="tax-divider">P&L BREAKDOWN</div>
      <div class="tax-row">
        <span class="tax-key">Gross P&L</span>
        <span class="tax-val {% if st.tax.total_pnl >= 0 %}green{% else %}red{% endif %}">
          ${{ "%+.2f"|format(st.tax.total_pnl) }}
        </span>
      </div>
      <div class="tax-row">
        <span class="tax-key">Tax owed (35%)</span>
        <span class="tax-val red">-${{ "%.2f"|format(st.tax.total_tax) }}</span>
      </div>
      <div class="tax-row" style="background:var(--surface2);">
        <span class="tax-key" style="font-weight:600;color:var(--text)">Net take home</span>
        <span class="tax-val green" style="font-size:16px;">${{ "%+.2f"|format(st.tax.total_net) }}</span>
      </div>

      <div class="tax-divider">TRADE STATS</div>
      <div class="tax-row">
        <span class="tax-key">Total trades</span>
        <span class="tax-val">{{ st.tax.total_trades }}</span>
      </div>
      <div class="tax-row">
        <span class="tax-key">Winning trades</span>
        <span class="tax-val green">{{ st.tax.winning_trades }}</span>
      </div>
      <div class="tax-row">
        <span class="tax-key">Losing trades</span>
        <span class="tax-val red">{{ st.tax.losing_trades }}</span>
      </div>
      {% if st.tax.total_trades > 0 %}
      <div class="tax-row">
        <span class="tax-key">Win rate</span>
        <span class="tax-val">{{ "%.1f"|format(st.tax.winning_trades / st.tax.total_trades * 100) }}%</span>
      </div>
      {% endif %}
    </div>

    <div style="font-size:11px;color:var(--muted);text-align:center;padding:16px 0;line-height:1.6;">
      Section 1256 contracts (60/40 rule)<br>
      Estimate only — consult a CPA for filing<br>
      NY state + NYC local taxes additional ~11%
    </div>
  </div>

  <!-- DIAGNOSTICS -->
  <div id="diagnostics" class="section">
    {% set errors = st.diagnostics|selectattr("level","in",["ERROR","CRITICAL"])|list %}
    {% if errors %}
    <div style="background:rgba(255,71,87,0.1);border:1px solid rgba(255,71,87,0.3);
                border-radius:12px;padding:12px 16px;margin-bottom:12px;
                font-size:13px;color:var(--red);font-weight:600;">
      ⚠️ {{ errors|length }} error(s) require attention
    </div>
    {% endif %}

    <div class="section-title">System Log</div>
    {% if st.diagnostics %}
    <div class="card" style="padding:0 16px;">
      {% for d in st.diagnostics[:50] %}
      <div class="diag-row">
        <span class="diag-badge diag-{{ d.level }}">{{ d.level }}</span>
        <div class="diag-body">
          <div class="diag-event">{{ d.event }}</div>
          <div class="diag-cause">{{ d.cause }}</div>
          <div class="diag-action">→ {{ d.action }}</div>
          <div class="diag-time">{{ d.time }}</div>
        </div>
      </div>
      {% endfor %}
    </div>
    {% else %}
    <div class="empty">
      <div class="empty-icon">✅</div>
      <div class="empty-text">No issues detected</div>
    </div>
    {% endif %}
  </div>

</div><!-- main -->

<button class="refresh-btn" onclick="location.reload()">↻</button>

<script>
function showTab(id, el) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  el.classList.add('active');
}
// Auto-refresh every 60 seconds
setTimeout(() => location.reload(), 60000);
</script>
{% endif %}
</body>
</html>'''

@app.route("/")
def index():
    if not session.get("logged_in"):
        return render_template_string(HTML, logged_in=False, error=None, st=None)
    st = S.load()
    return render_template_string(HTML, logged_in=True, st=st)

@app.route("/login", methods=["POST"])
def login():
    if request.form.get("password") == PASSWORD:
        session["logged_in"] = True
        return redirect(url_for("index"))
    return render_template_string(HTML, logged_in=False, error="Wrong password", st=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))

@app.route("/api/state")
def api_state():
    if not session.get("logged_in"):
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(S.load())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
