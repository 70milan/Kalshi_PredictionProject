import { useState, useEffect, useCallback, useRef } from 'react';
import MispricingCard from './components/MispricingCard';
import './index.css';

// Dynamically point to the backend whether accessed via localhost or a Tailscale IP
const API_BASE = window.location.hostname === 'localhost' 
  ? 'http://localhost:8000' 
  : `http://${window.location.hostname}:8000`;
const POLL_MS  = 15_000;

export default function App() {
  const [briefs, setBriefs]       = useState([]);
  const [safeMode, setSafeMode]   = useState(true);
  const [bankroll, setBankroll]   = useState(1000);
  const [loading, setLoading]     = useState(true);
  const [lastPoll, setLastPoll]   = useState(null);
  const [toast, setToast]         = useState(null);
  const [filter, setFilter]       = useState('all');
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
    } catch (err) {
      console.error('Poll error:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchIntelligence();
    const interval = setInterval(fetchIntelligence, POLL_MS);
    return () => clearInterval(interval);
  }, [fetchIntelligence]);

  // Derived stats
  const withEdge    = briefs.filter(b => b.kelly?.edge_detected);
  const avgConf     = briefs.length
    ? (briefs.reduce((s, b) => s + (b.confidence_score || 0), 0) / briefs.length * 100).toFixed(0)
    : 0;
  const totalKelly  = withEdge.reduce((s, b) => s + (b.kelly?.suggested_bet_usd ?? 0), 0);

  const displayBriefs = filter === 'edge' ? withEdge : briefs;

  return (
    <>
      {/* HEADER */}
      <header className="header">
        <div className="header-brand">
          <div className="header-logo">P</div>
          <div>
            <div className="header-title">PredictIQ Terminal</div>
            <div className="header-subtitle">AI Intelligence · HIL Execution</div>
          </div>
        </div>

        <div className="header-meta">
          {lastPoll && (
            <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
              Updated {lastPoll.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </span>
          )}
          <div className={`status-pill ${safeMode ? 'safe' : 'live'}`}>
            <div className="pulse-dot" />
            {safeMode ? 'Safe Mode' : 'Live Trading'}
          </div>
          <div className="bankroll-badge">${bankroll.toLocaleString()}</div>
        </div>
      </header>

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

        {/* SECTION */}
        <div className="section-heading">
          Intelligence Briefs · Awaiting Approval
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
              {filter === 'edge' ? 'No Edge Signals Right Now' : 'No Intelligence Briefs Yet'}
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
