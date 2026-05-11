import { useState, useEffect, useCallback, useRef } from 'react';
import MispricingCard from './components/MispricingCard';
import './index.css';

// Dynamically point to the backend whether accessed via localhost or a Tailscale IP
const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000'
  : `http://${window.location.hostname}:8000`;
const POLL_MS = 15_000;

export default function App() {
  const [briefs, setBriefs] = useState([]);
  const [safeMode, setSafeMode] = useState(true);
  const [bankroll, setBankroll] = useState(1000);
  const [loading, setLoading] = useState(true);
  const [lastPoll, setLastPoll] = useState(null);
  const [activeMarketsAsOf, setActiveMarketsAsOf] = useState(null);
  const [activeMarketFilter, setActiveMarketFilter] = useState(false);
  const [toast, setToast] = useState(null);
  const [filter, setFilter] = useState('all');
  const [orders, setOrders] = useState([]);
  const [showPositions, setShowPositions] = useState(false);
  const [sideFilter, setSideFilter] = useState('all');
  const toastTimer = useRef(null);

  const showToast = useCallback((msg) => {
    setToast(msg);
    clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(null), 5000);
  }, []);

  const fetchIntelligence = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/intelligence`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setBriefs(data.briefs ?? []);
      setSafeMode(data.safe_mode ?? true);
      setBankroll(data.bankroll ?? 1000);
      setLastPoll(new Date());
      setActiveMarketsAsOf(data.active_markets_as_of ? new Date(data.active_markets_as_of) : null);
      setActiveMarketFilter(data.active_market_filter ?? false);
    } catch (err) {
      console.error('Poll error:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchOrders = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/portfolio/positions`);
      if (!res.ok) return;
      const data = await res.json();
      const positions = (data.market_positions ?? data.positions ?? [])
        .filter(p => parseFloat(p.position_fp ?? 0) !== 0);
      setOrders(positions);
    } catch (_) { }
  }, []);

  useEffect(() => {
    fetchIntelligence();
    fetchOrders();
    const interval = setInterval(() => {
      fetchIntelligence();
      fetchOrders();
    }, POLL_MS);
    return () => clearInterval(interval);
  }, [fetchIntelligence, fetchOrders]);

  // Derived stats
  const withEdge = briefs.filter(b => b.kelly?.edge_detected);
  const avgConf = briefs.length
    ? (briefs.reduce((s, b) => s + (b.confidence_score || 0), 0) / briefs.length * 100).toFixed(0)
    : 0;
  const totalKelly = withEdge.reduce((s, b) => s + (b.kelly?.suggested_bet_usd ?? 0), 0);

  const sortedBriefs = [...(filter === 'edge' ? withEdge : briefs)]
    .sort((a, b) => new Date(b.ingested_at) - new Date(a.ingested_at));

  const displayBriefs = sortedBriefs.filter(b => {
    if (sideFilter === 'yes') return b.recommended_side === 'yes' && !/no trade/i.test(b.verdict ?? '');
    if (sideFilter === 'no')  return b.recommended_side === 'no'  && !/no trade/i.test(b.verdict ?? '');
    if (sideFilter === 'notrade') return /no trade/i.test(b.verdict ?? '');
    return true;
  });

  // How many minutes ago was latest.parquet written?
  const bronzeAgeMinutes = activeMarketsAsOf
    ? Math.floor((Date.now() - activeMarketsAsOf.getTime()) / 60_000)
    : null;
  const bronzeStale = bronzeAgeMinutes !== null && bronzeAgeMinutes > 20;

  return (
    <>
      {/* HEADER */}
      <header className="header">
        <div className="header-brand">
          <div className="header-logo">P</div>
          <div>
            <div className="header-title">PredictIQ</div>
            <div className="header-subtitle">Predictive Market Movements</div>
          </div>
        </div>

        <div className="header-meta">
          {/* POSITIONS BUTTON */}
          <button
            className="positions-header-btn"
            onClick={() => setShowPositions(v => !v)}
          >
            Positions
            {orders.length > 0 && <span className="positions-badge">{orders.length}</span>}
          </button>

          {lastPoll && (
            <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
              Polled {lastPoll.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </span>
          )}
          {bronzeAgeMinutes !== null && (
            <span
              title={`Active market list last refreshed: ${activeMarketsAsOf?.toLocaleTimeString()}`}
              style={{
                fontSize: '0.7rem',
                fontFamily: 'monospace',
                color: bronzeStale ? 'var(--amber)' : 'var(--green)',
                display: 'flex', alignItems: 'center', gap: '4px',
              }}
            >
              <span style={{ fontSize: '0.6rem' }}>{bronzeStale ? '⚠' : '✓'}</span>
              Markets {bronzeAgeMinutes}m ago
            </span>
          )}
          <div className={`status-pill ${safeMode ? 'safe' : 'live'}`}>
            <div className="pulse-dot" />
            {safeMode ? 'Safe Mode' : 'Live Trading'}
          </div>
          <div className="bankroll-badge">${bankroll.toLocaleString()}</div>
        </div>
      </header>

      {/* POSITIONS OVERLAY */}
      {showPositions && (
        <div className="modal-backdrop" onClick={() => setShowPositions(false)}>
          <div className="modal-card" style={{ maxWidth: '860px' }} onClick={e => e.stopPropagation()}>
            <button className="modal-close" onClick={() => setShowPositions(false)}>×</button>
            <div style={{ padding: '1.2rem 1.5rem 0.5rem' }}>
              <div className="section-heading" style={{ marginBottom: '0.75rem' }}>
                Open Positions ({orders.length})
              </div>
              {safeMode ? (
                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', padding: '1rem 0' }}>
                  Safe Mode — trades are simulated. Switch to live mode to see real Kalshi positions.
                </div>
              ) : orders.length === 0 ? (
                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', padding: '1rem 0' }}>
                  No open positions on Kalshi.
                </div>
              ) : (() => {
                const titleMap = Object.fromEntries(briefs.map(b => [b.ticker, b.title]));
                const cancelOrder = async (ticker) => {
                  try {
                    const res = await fetch(`${API_BASE}/api/portfolio/orders/cancel/${ticker}`, { method: 'DELETE' });
                    const data = await res.json();
                    if (data.count > 0) showToast({ type: 'success', message: `Cancelled ${data.count} order(s) for ${ticker}` });
                    else showToast({ type: 'warning', message: `No resting orders found for ${ticker}` });
                    fetchOrders();
                  } catch (e) {
                    showToast({ type: 'error', message: `Cancel failed: ${e.message}` });
                  }
                };
                return (
                  <div className="orders-table" style={{ marginBottom: '1rem' }}>
                    <div className="orders-header">
                      <span>Market</span><span>Side</span><span>Qty</span>
                      <span>Avg Cost</span><span>Value</span><span>P&L</span><span></span>
                    </div>
                    {orders.map((p) => {
                      const posFp = parseFloat(p.position_fp ?? 0);
                      const side = posFp > 0 ? 'yes' : 'no';
                      const qty = Math.abs(posFp);
                      const cost = parseFloat(p.total_traded_dollars ?? 0);
                      const exposure = parseFloat(p.market_exposure_dollars ?? 0);
                      const avgCents = qty > 0 ? `${((cost / qty) * 100).toFixed(1)}¢` : '—';
                      const pnl = exposure - cost;
                      const pnlColor = pnl >= 0 ? 'var(--green)' : 'var(--red)';
                      const title = titleMap[p.ticker];
                      return (
                        <div key={p.ticker} className="orders-row">
                          <span style={{ overflow: 'hidden' }}>
                            <div className="order-ticker" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.ticker}</div>
                            {title && <div style={{ fontSize: '0.6rem', color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{title}</div>}
                          </span>
                          <span className={`order-side ${side}`}>{side.toUpperCase()}</span>
                          <span>{qty}</span>
                          <span>{avgCents}</span>
                          <span>${exposure.toFixed(2)}</span>
                          <span style={{ color: pnlColor }}>{pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}</span>
                          <span><button className="cancel-btn" onClick={() => cancelOrder(p.ticker)} title="Cancel resting orders">×</button></span>
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
            </div>
          </div>
        </div>
      )}

      <div className="layout">
        {/* STATS BAR */}
        <div className="stats-bar">
          <div className="stat-card">
            <div className="stat-label">Active Signals</div>
            <div className="stat-value blue">{briefs.length}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Edge Detected</div>
            <div className="stat-value green">{withEdge.length}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Avg AI Confidence</div>
            <div className="stat-value amber">{avgConf}%</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total Kelly Exposure</div>
            <div className="stat-value red">${totalKelly.toFixed(0)}</div>
          </div>
          <div className="stat-card" title={activeMarketFilter ? `Filtered against latest.parquet (${bronzeAgeMinutes}m ago)` : 'latest.parquet not found — no active-market filter applied'}>
            <div className="stat-label">Market Filter</div>
            <div className={`stat-value ${!activeMarketFilter ? 'red' : bronzeStale ? 'amber' : 'green'}`}>
              {!activeMarketFilter ? 'OFF' : bronzeStale ? `${bronzeAgeMinutes}m` : `${bronzeAgeMinutes}m ✓`}
            </div>
          </div>
        </div>

        {/* FILTER BAR */}
        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.25rem' }}>
          {['all', 'edge'].map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              style={{
                padding: '6px 16px',
                borderRadius: '20px',
                border: '1px solid',
                fontSize: '0.75rem',
                fontWeight: '600',
                cursor: 'pointer',
                fontFamily: 'Inter, sans-serif',
                letterSpacing: '0.05em',
                textTransform: 'capitalize',
                background: filter === f ? 'var(--blue-dim)' : 'transparent',
                borderColor: filter === f ? 'var(--blue)' : 'var(--border)',
                color: filter === f ? 'var(--blue)' : 'var(--text-muted)',
                transition: 'all 0.15s',
              }}
            >
              {f === 'all' ? `All Signals (${briefs.length})` : `Edge Only (${withEdge.length})`}
            </button>
          ))}
        </div>

        {/* SECTION + SIDE FILTER */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', marginBottom: '0.6rem' }}>
          <div className="section-heading" style={{ marginBottom: 0, flex: 'none' }}>
            Predictive Movement Signals · Awaiting Approval
          </div>
          <div style={{ display: 'flex', gap: '0.35rem', marginLeft: 'auto' }}>
            {[
              { key: 'all',     label: 'All' },
              { key: 'yes',     label: 'Buy YES' },
              { key: 'no',      label: 'Buy NO' },
              { key: 'notrade', label: 'No Trade' },
            ].map(({ key, label }) => (
              <button
                key={key}
                onClick={() => setSideFilter(key)}
                style={{
                  padding: '3px 10px',
                  borderRadius: '12px',
                  border: '1px solid',
                  fontSize: '0.65rem',
                  fontWeight: '600',
                  cursor: 'pointer',
                  fontFamily: 'Inter, sans-serif',
                  letterSpacing: '0.04em',
                  background: sideFilter === key
                    ? key === 'yes' ? 'var(--green-dim)' : key === 'no' ? 'var(--red-dim)' : key === 'notrade' ? 'var(--amber-dim)' : 'var(--blue-dim)'
                    : 'transparent',
                  borderColor: sideFilter === key
                    ? key === 'yes' ? 'var(--green)' : key === 'no' ? 'var(--red)' : key === 'notrade' ? 'var(--amber)' : 'var(--blue)'
                    : 'var(--border)',
                  color: sideFilter === key
                    ? key === 'yes' ? 'var(--green)' : key === 'no' ? 'var(--red)' : key === 'notrade' ? 'var(--amber)' : 'var(--blue)'
                    : 'var(--text-muted)',
                  transition: 'all 0.15s',
                }}
              >{label}</button>
            ))}
          </div>
        </div>

        {/* CONTENT */}
        {loading ? (
          <div className="loading-state">
            <div className="spinner" />
            Connecting to PredictIQ Intelligence API…
          </div>
        ) : displayBriefs.length === 0 ? (
          <div className="empty-state">
            <div className="empty-icon">⚡</div>
            <div className="empty-title">
              {filter === 'edge' ? 'No Edge Signals Right Now' : 'No Movement Signals Yet'}
            </div>
            <div className="empty-subtitle">
              {filter === 'edge'
                ? 'The Kelly Criterion found no positive-edge trades in the current batch. Check back after the next 15-minute inference cycle.'
                : 'The AI inference engine has not generated any briefs yet. Run explain_mispricing.py or wait for the next automated cycle.'}
            </div>
          </div>
        ) : (
          <div className="briefs-grid">
            {displayBriefs.map(brief => (
              <MispricingCard
                key={brief.ticker}
                brief={brief}
                bankroll={bankroll}
                onTradeResult={showToast}
                onTradeComplete={fetchOrders}
              />
            ))}
          </div>
        )}

      </div>

      {/* TOAST */}
      {toast && (
        <div className={`toast ${toast.type}`}>
          {toast.message}
        </div>
      )}
    </>
  );
}
