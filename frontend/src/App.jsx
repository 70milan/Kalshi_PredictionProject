import { useState, useEffect, useCallback, useRef } from 'react';
import MispricingCard from './components/MispricingCard';
import './index.css';

// VITE_API_URL is set at build time for the public GitHub Pages build (points to Cloudflare tunnel).
// At runtime, fall back to same-host:8000 for local/Tailscale use.
const API_BASE = import.meta.env.VITE_API_URL
  || (window.location.hostname === 'localhost'
    ? 'http://localhost:8000'
    : `http://${window.location.hostname}:8000`);
const POLL_MS = 15_000;

const ACTION_META = {
  SELL_PROFIT:  { label: 'SELL — Lock Profit',   color: 'var(--green)', bg: 'var(--green-dim)' },
  SELL_LOSS:    { label: 'SELL — Cut Losses',    color: 'var(--red)',   bg: 'var(--red-dim)' },
  SELL_FLIP:    { label: 'SELL — Thesis Flipped', color: 'var(--amber)', bg: 'var(--amber-dim)' },
  SELL_TIMEOUT: { label: 'SELL — Time Decay',    color: 'var(--blue)',  bg: 'var(--blue-dim)' },
  HOLD:         { label: 'HOLD',                  color: 'var(--text-muted)', bg: 'transparent' },
};

export default function App() {
  const [briefs, setBriefs] = useState([]);
  const [readonlyMode, setReadonlyMode] = useState(false);
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
  const [showExits, setShowExits] = useState(false);
  const [exitSignals, setExitSignals] = useState([]);
  const [actionableExits, setActionableExits] = useState(0);
  const [exitsAsOf, setExitsAsOf] = useState(null);
  const [sideFilters, setSideFilters] = useState([]);
  const [showBacktest, setShowBacktest] = useState(false);
  const [backtestData, setBacktestData] = useState(null);
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [backtestCurrentOnly, setBacktestCurrentOnly] = useState(true);
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
      const [posRes, fillRes] = await Promise.all([
        fetch(`${API_BASE}/api/portfolio/positions`),
        fetch(`${API_BASE}/api/portfolio/fills?limit=500`),
      ]);
      if (!posRes.ok) return;
      const posData = await posRes.json();
      const positions = (posData.market_positions ?? posData.positions ?? [])
        .filter(p => parseFloat(p.position_fp ?? 0) !== 0);

      // Build cost basis map from fills as a fallback
      const costMap = {};
      if (fillRes.ok) {
        const fillData = await fillRes.json();
        for (const f of (fillData.fills ?? [])) {
          if (f.action !== 'buy') continue;
          const ticker = f.ticker;
          // Support both _dollars and raw cents fields
          let price = f.side === 'no'
            ? (f.no_price_dollars ?? (f.no_price ? f.no_price / 100 : 0))
            : (f.yes_price_dollars ?? (f.yes_price ? f.yes_price / 100 : 0));
          
          const cost = parseFloat(price) * parseFloat(f.count_fp ?? 0);
          costMap[ticker] = (costMap[ticker] ?? 0) + cost;
        }
      }

      // Attach cost basis to each position
      const enriched = positions.map(p => {
        // Try average_price from Kalshi position first (most accurate)
        let avgPrice = p.average_price_dollars;
        if (avgPrice == null && p.average_price != null) {
          avgPrice = p.average_price / 100;
        }
        
        const qty = Math.abs(parseFloat(p.position_fp ?? 0));
        let costBasis = avgPrice != null ? avgPrice * qty : costMap[p.ticker];
        
        return {
          ...p,
          _cost_basis: costBasis ?? null,
        };
      });
      setOrders(enriched);
    } catch (_) { }
  }, []);

  const fetchExits = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/exits`);
      if (!res.ok) return;
      const data = await res.json();
      setExitSignals(data.signals ?? []);
      setExitsAsOf(data.as_of ? new Date(data.as_of) : null);
      setActionableExits(data.actionable ?? 0);
    } catch (_) { }
  }, []);

  const fetchBacktest = useCallback(async (currentOnly = backtestCurrentOnly) => {
    setBacktestLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/backtest?days=30&current_system_only=${currentOnly}`);
      if (!res.ok) return;
      const data = await res.json();
      setBacktestData(data);
    } catch (_) { }
    finally { setBacktestLoading(false); }
  }, [backtestCurrentOnly]);

  useEffect(() => {
    fetch(`${API_BASE}/api/config`)
      .then(r => r.json())
      .then(d => { setReadonlyMode(d.readonly); setSafeMode(d.safe_mode); })
      .catch(() => {});
  }, []);

  useEffect(() => {
    fetchIntelligence();
    fetchOrders();
    fetchExits();
    const interval = setInterval(() => {
      fetchIntelligence();
      fetchOrders();
      fetchExits();
    }, POLL_MS);
    return () => clearInterval(interval);
  }, [fetchIntelligence, fetchOrders, fetchExits]);

  // Derived stats
  const withEdge = briefs.filter(b => b.kelly?.edge_detected);
  const avgConf = briefs.length
    ? (briefs.reduce((s, b) => s + (b.confidence_score || 0), 0) / briefs.length * 100).toFixed(0)
    : 0;
  const totalKelly = withEdge.reduce((s, b) => s + (b.kelly?.suggested_bet_usd ?? 0), 0);

  const sortedBriefs = [...(filter === 'edge' ? withEdge : briefs)]
    .sort((a, b) => new Date(b.ingested_at) - new Date(a.ingested_at));

  const displayBriefs = sortedBriefs.filter(b => {
    if (sideFilters.length === 0) return true;
    
    const isYes = b.recommended_side === 'yes' && !/no trade/i.test(b.verdict ?? '');
    const isNo = b.recommended_side === 'no' && !/no trade/i.test(b.verdict ?? '');
    const isNoTrade = /no trade/i.test(b.verdict ?? '');

    let show = false;
    if (sideFilters.includes('yes') && isYes) show = true;
    if (sideFilters.includes('no') && isNo) show = true;
    if (sideFilters.includes('notrade') && isNoTrade) show = true;
    
    return show;
  });

  // How many minutes ago was latest.parquet written?
  const bronzeAgeMinutes = activeMarketsAsOf
    ? Math.floor((Date.now() - activeMarketsAsOf.getTime()) / 60_000)
    : null;
  const bronzeStale = bronzeAgeMinutes !== null && bronzeAgeMinutes > 20;

  const exitsAgeMinutes = exitsAsOf
    ? Math.floor((Date.now() - exitsAsOf.getTime()) / 60_000)
    : null;

  return (
    <>
      {readonlyMode && (
        <div style={{
          background: 'var(--red-dim)',
          color: 'var(--red)',
          textAlign: 'center',
          padding: '0.45rem 1rem',
          fontSize: '0.72rem',
          fontWeight: 700,
          letterSpacing: '0.07em',
          borderBottom: '1px solid var(--red)',
          position: 'sticky',
          top: 0,
          zIndex: 1000,
        }}>
          READ-ONLY — Trading disabled on public view
        </div>
      )}
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
          {/* BACKTEST BUTTON */}
          <button
            className="positions-header-btn"
            onClick={() => { setShowBacktest(v => !v); if (!backtestData) fetchBacktest(); }}
          >
            Backtest
          </button>

          {/* EXITS BUTTON */}
          <button
            className="positions-header-btn"
            onClick={() => setShowExits(v => !v)}
          >
            Exits
            {exitSignals.length > 0 && (
              <span className="positions-badge">{exitSignals.length}</span>
            )}
          </button>

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
            <div className="modal-close-bar">
              <button className="modal-close" onClick={() => setShowPositions(false)}>×</button>
            </div>
            <div className="modal-body">
            <div style={{ padding: '0.5rem 1.5rem 0.5rem' }}>
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
                      const exposure = parseFloat(p.market_exposure_dollars ?? 0);
                      const costBasis = p._cost_basis;
                      const avgCents = (costBasis != null && qty > 0)
                        ? `${((costBasis / qty) * 100).toFixed(1)}¢`
                        : '—';
                      const exitSignal = exitSignals.find(s => s.ticker === p.ticker);
                      const pnl = exitSignal?.unrealized_pnl ?? (costBasis != null ? exposure - costBasis : null);
                      const pnlColor = (pnl ?? 0) >= 0 ? 'var(--green)' : 'var(--red)';
                      
                      // Title logic: Use the real title if found, otherwise show only ticker (no doubles)
                      const hasTitle = p.title && p.title !== p.ticker;

                      return (
                        <div key={p.ticker} className="orders-row">
                          <span style={{ overflow: 'hidden', paddingRight: '1rem' }} title={hasTitle ? p.title : p.ticker}>
                            <div className="order-title" style={{
                              fontSize: '0.75rem',
                              fontWeight: 600,
                              color: 'var(--text-primary)',
                              lineHeight: 1.3,
                              fontFamily: 'JetBrains Mono, monospace',
                              whiteSpace: 'nowrap',
                              overflow: 'hidden',
                              textOverflow: 'ellipsis'
                            }}>
                              {p.ticker}
                            </div>
                          </span>
                          <span className={`order-side ${side}`}>{side.toUpperCase()}</span>
                          <span>{qty}</span>
                          <span>{avgCents}</span>
                          <span>${exposure.toFixed(2)}</span>
                          <span style={{ color: pnlColor }}>
                            {pnl == null ? '—' : `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`}
                          </span>
                          <span>{!readonlyMode && <button className="cancel-btn" onClick={() => cancelOrder(p.ticker)} title="Cancel Order(s)">×</button>}</span>
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
            </div>
            </div>{/* end modal-body */}
          </div>
        </div>
      )}

      {/* BACKTEST OVERLAY */}
      {showBacktest && (
        <div className="modal-backdrop" onClick={() => setShowBacktest(false)}>
          <div className="modal-card" style={{ maxWidth: '1100px' }} onClick={e => e.stopPropagation()}>
            <div className="modal-close-bar">
              <button className="modal-close" onClick={() => setShowBacktest(false)}>×</button>
            </div>
            <div className="modal-body">
            <div style={{ padding: '0.5rem 1.5rem 0.5rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
                <div className="section-heading" style={{ marginBottom: 0 }}>Signal Backtest · Last 30 Days</div>
                <label
                  title="Filter to briefs the current pipeline would have written (0.25–0.75 YES bid + positive edge)"
                  style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '0.62rem', color: 'var(--text-secondary)', cursor: 'pointer', userSelect: 'none' }}
                >
                  <input
                    type="checkbox"
                    checked={backtestCurrentOnly}
                    onChange={e => {
                      const v = e.target.checked;
                      setBacktestCurrentOnly(v);
                      fetchBacktest(v);
                    }}
                    style={{ cursor: 'pointer' }}
                  />
                  Current pipeline only
                </label>
                <button
                  onClick={() => fetchBacktest()}
                  style={{ padding: '3px 10px', borderRadius: '5px', border: '1px solid var(--border)', fontSize: '0.62rem', cursor: 'pointer', background: 'transparent', color: 'var(--text-muted)' }}
                >
                  {backtestLoading ? 'Loading…' : 'Refresh'}
                </button>
                {backtestData?.stats?.excluded > 0 && (
                  <span style={{ fontSize: '0.6rem', color: 'var(--text-muted)', fontFamily: 'JetBrains Mono, monospace' }}>
                    {backtestData.stats.excluded} pre-guardrail brief{backtestData.stats.excluded === 1 ? '' : 's'} hidden
                  </span>
                )}
              </div>

              {backtestLoading && !backtestData ? (
                <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', padding: '1rem 0' }}>Computing backtest…</div>
              ) : backtestData ? (() => {
                const s = backtestData.stats ?? {};
                const rows = backtestData.rows ?? [];
                const hitPct = s.hit_rate != null ? (s.hit_rate * 100).toFixed(0) : '—';
                const totalPnl = s.sim_total_pnl ?? 0;
                const openPnl  = s.sim_open_pnl ?? 0;

                const OUTCOME_COLOR = {
                  WIN:     'var(--green)',
                  LOSS:    'var(--red)',
                  OPEN:    'var(--blue)',
                  UNKNOWN: 'var(--text-muted)',
                };

                return (
                  <>
                    {/* STAT STRIP */}
                    <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1.25rem', flexWrap: 'wrap' }}>
                      {[
                        { label: 'Signals', value: s.total ?? 0, color: 'var(--text-primary)' },
                        { label: 'Resolved', value: s.resolved ?? 0, color: 'var(--text-secondary)' },
                        { label: 'Win Rate', value: s.resolved ? `${hitPct}%` : 'N/A', color: s.hit_rate >= 0.55 ? 'var(--green)' : s.hit_rate != null ? 'var(--red)' : 'var(--text-muted)' },
                        { label: 'Open Paper P&L', value: `${openPnl >= 0 ? '+' : ''}$${openPnl.toFixed(2)}`, color: openPnl >= 0 ? 'var(--green)' : 'var(--red)' },
                        { label: 'Total Sim P&L', value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`, color: totalPnl >= 0 ? 'var(--green)' : 'var(--red)' },
                        { label: 'Open Positions', value: s.open ?? 0, color: 'var(--blue)' },
                      ].map(({ label, value, color }) => (
                        <div key={label} style={{ minWidth: '90px' }}>
                          <div style={{ fontSize: '0.58rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '2px' }}>{label}</div>
                          <div style={{ fontSize: '1.05rem', fontWeight: 700, color, fontFamily: 'JetBrains Mono, monospace' }}>{value}</div>
                        </div>
                      ))}
                    </div>

                    {s.resolved === 0 && (
                      <div style={{ fontSize: '0.7rem', color: 'var(--amber)', marginBottom: '0.75rem', padding: '0.5rem 0.75rem', border: '1px solid var(--amber)', borderRadius: '6px', background: 'var(--amber-dim)' }}>
                        No resolved markets yet — system is {s.days} days old. Win rate will populate as markets settle. Paper P&L reflects current price vs entry.
                      </div>
                    )}

                    {/* TABLE */}
                    <div style={{ overflowX: 'auto' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.68rem', fontFamily: 'JetBrains Mono, monospace' }}>
                        <thead>
                          <tr style={{ borderBottom: '1px solid var(--border)', color: 'var(--text-muted)', fontSize: '0.58rem', textTransform: 'uppercase' }}>
                            <th style={{ textAlign: 'left', padding: '6px 8px', fontWeight: 500 }}>Date</th>
                            <th style={{ textAlign: 'left', padding: '6px 8px', fontWeight: 500 }}>Market</th>
                            <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Side</th>
                            <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Entry</th>
                            <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Conf</th>
                            <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Outcome</th>
                            <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Current</th>
                            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Paper P&L</th>
                          </tr>
                        </thead>
                        <tbody>
                          {rows.map((r, i) => {
                            const pnl = r.sim_pnl;
                            const pnlColor = pnl == null ? 'var(--text-muted)' : pnl >= 0 ? 'var(--green)' : 'var(--red)';
                            const entryC = Math.round((r.entry_price ?? 0) * 100);
                            const curC = r.current_price != null ? Math.round(r.current_price * 100) : null;
                            return (
                              <tr key={i} style={{ borderBottom: '1px solid var(--border)', opacity: r.outcome === 'UNKNOWN' ? 0.45 : 1 }}>
                                <td style={{ padding: '5px 8px', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{r.brief_date}</td>
                                <td style={{ padding: '5px 8px', maxWidth: '280px' }}>
                                  <div style={{ fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={r.title || r.ticker}>
                                    {r.ticker}
                                  </div>
                                  {r.title && r.title !== r.ticker && (
                                    <div style={{ fontSize: '0.58rem', color: 'var(--text-muted)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.title}</div>
                                  )}
                                </td>
                                <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                  <span style={{ color: r.recommended_side === 'yes' ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>
                                    {r.recommended_side?.toUpperCase()}
                                  </span>
                                </td>
                                <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-secondary)' }}>{entryC}¢</td>
                                <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-muted)' }}>{Math.round((r.confidence ?? 0) * 100)}%</td>
                                <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                  <span style={{ color: OUTCOME_COLOR[r.outcome] ?? 'var(--text-muted)', fontWeight: 700, fontSize: '0.62rem' }}>{r.outcome}</span>
                                </td>
                                <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-secondary)' }}>{curC != null ? `${curC}¢` : '—'}</td>
                                <td style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 700, color: pnlColor }}>
                                  {pnl == null ? '—' : `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>

                    <div style={{ fontSize: '0.6rem', color: 'var(--text-muted)', marginTop: '0.75rem' }}>
                      Paper P&L simulates Kelly-sized bets (quarter-Kelly, 5% cap) at a $1,000 bankroll. Where no Kelly edge exists, a flat $10 position is assumed. Not financial advice.
                    </div>
                  </>
                );
              })() : null}
            </div>
            </div>{/* end modal-body */}
          </div>
        </div>
      )}

      {/* EXITS OVERLAY */}
      {showExits && (
        <div className="modal-backdrop" onClick={() => setShowExits(false)}>
          <div className="modal-card" style={{ maxWidth: '860px' }} onClick={e => e.stopPropagation()}>
            <div className="modal-close-bar">
              <button className="modal-close" onClick={() => setShowExits(false)}>×</button>
            </div>
            <div className="modal-body">
            <div style={{ padding: '0.5rem 1.5rem 0.5rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.8rem', marginBottom: '0.75rem' }}>
                <div className="section-heading" style={{ marginBottom: 0 }}>
                  Exit Recommendations ({exitSignals.length})
                </div>
                {exitsAgeMinutes !== null && (
                  <span style={{ fontSize: '0.65rem', color: 'var(--text-muted)', fontFamily: 'JetBrains Mono, monospace' }}>
                    evaluated {exitsAgeMinutes}m ago
                  </span>
                )}
              </div>
              
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem', marginBottom: '1rem' }}>
                {exitSignals.map((s, i) => {
                  const meta = ACTION_META[s.action] ?? ACTION_META.HOLD;
                  const pnl = parseFloat(s.unrealized_pnl ?? 0);
                  const pnlPct = parseFloat(s.capture_pct ?? 0) * 100;
                  const entryC = Math.round(parseFloat(s.entry_price ?? 0) * 100);
                  const currentC = Math.round(parseFloat(s.current_price ?? 0) * 100);
                  const isAction = s.action !== 'HOLD';

                  return (
                    <div
                      key={`${s.ticker}-${i}`}
                      style={{
                        background: 'var(--bg-card)',
                        border: `1px solid ${isAction ? meta.color : 'var(--border)'}`,
                        borderLeft: `3px solid ${meta.color}`,
                        borderRadius: 'var(--radius)',
                        padding: '0.75rem 1rem',
                        display: 'grid',
                        gridTemplateColumns: `1.5fr 0.7fr 0.7fr 0.8fr 1.2fr 1.5fr${!readonlyMode && isAction ? ' auto' : ''}`,
                        gap: '0.75rem',
                        alignItems: 'center',
                        fontSize: '0.72rem',
                      }}
                    >
                      <div style={{ minWidth: 0 }} title={s.title || s.ticker}>
                        <div style={{
                          color: 'var(--text-primary)',
                          fontSize: '0.75rem',
                          fontWeight: 600,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          fontFamily: 'JetBrains Mono, monospace',
                        }}>
                          {s.ticker}
                        </div>
                        <div style={{ fontSize: '0.6rem', color: 'var(--text-muted)', marginTop: '2px', fontFamily: 'JetBrains Mono, monospace' }}>
                          {s.side?.toUpperCase()} x {parseFloat(s.qty ?? 0)}
                        </div>
                      </div>

                      <div>
                        <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Entry</div>
                        <div style={{ fontFamily: 'JetBrains Mono, monospace', fontWeight: 600 }}>{entryC}¢</div>
                      </div>

                      <div>
                        <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Now</div>
                        <div style={{ fontFamily: 'JetBrains Mono, monospace', fontWeight: 600 }}>{currentC}¢</div>
                      </div>

                      <div>
                        <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>P&L</div>
                        <div style={{
                          fontFamily: 'JetBrains Mono, monospace',
                          fontWeight: 700,
                          color: pnl >= 0 ? 'var(--green)' : 'var(--red)',
                        }}>
                          {pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}
                          <span style={{ fontSize: '0.55rem', marginLeft: '4px', opacity: 0.7 }}>
                            ({pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(0)}%)
                          </span>
                        </div>
                      </div>

                      <div>
                        <div style={{
                          display: 'inline-block',
                          padding: '2px 8px',
                          borderRadius: '4px',
                          background: meta.bg,
                          color: meta.color,
                          fontSize: '0.6rem',
                          fontWeight: 700,
                          letterSpacing: '0.04em',
                        }}>
                          {meta.label}
                        </div>
                      </div>

                      <div style={{ fontSize: '0.65rem', color: 'var(--text-secondary)', lineHeight: 1.4 }}>
                        {s.reason}
                      </div>

                      {!readonlyMode && isAction && (
                        <div style={{ display: 'flex', alignItems: 'center' }}>
                          <button
                            style={{
                              background: meta.bg,
                              color: meta.color,
                              border: `1px solid ${meta.color}`,
                              borderRadius: '4px',
                              padding: '4px 10px',
                              fontSize: '0.62rem',
                              fontWeight: 700,
                              cursor: 'pointer',
                              whiteSpace: 'nowrap',
                              letterSpacing: '0.04em',
                            }}
                            onClick={async () => {
                              const qty = Math.floor(parseFloat(s.qty ?? 1));
                              const price = parseFloat(s.current_price ?? 0);
                              try {
                                const r = await fetch(`${API_BASE}/api/trade`, {
                                  method: 'POST',
                                  headers: { 'Content-Type': 'application/json' },
                                  body: JSON.stringify({
                                    ticker: s.ticker,
                                    side: s.side,
                                    count: qty,
                                    price_dollars: price,
                                    action: 'sell',
                                  }),
                                });
                                const d = await r.json();
                                if (!r.ok) {
                                  showToast(`Exit failed: ${d.detail ?? r.status}`);
                                } else {
                                  showToast(`Exit submitted: ${qty}x ${s.ticker} @ ${Math.round(price * 100)}¢`);
                                }
                              } catch (e) {
                                showToast('Exit request failed — check API');
                              }
                            }}
                          >
                            EXECUTE EXIT
                          </button>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
            </div>{/* end modal-body */}
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
        <div className="filter-bar" style={{ display: 'flex', gap: '0.4rem', marginBottom: '1.5rem' }}>
          {['all', 'edge'].map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              style={{
                padding: '5px 12px',
                borderRadius: '6px',
                border: '1px solid',
                fontSize: '0.7rem',
                fontWeight: '500',
                cursor: 'pointer',
                fontFamily: 'Inter, sans-serif',
                letterSpacing: '0.02em',
                background: filter === f ? 'var(--bg-surface)' : 'transparent',
                borderColor: filter === f ? 'var(--border-strong)' : 'var(--border)',
                color: filter === f ? 'var(--text-primary)' : 'var(--text-muted)',
                transition: 'all 0.15s',
              }}
            >
              {f === 'all' ? `All Signals · ${briefs.length}` : `Edge Only · ${withEdge.length}`}
            </button>
          ))}
        </div>

        {/* SECTION + SIDE FILTER */}
        <div className="section-row" style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', marginBottom: '0.6rem' }}>
          <div className="section-heading" style={{ marginBottom: 0, flex: 'none' }}>
            Predictive Movement Signals · Awaiting Approval
          </div>
          <div className="side-filter-bar" style={{ display: 'flex', gap: '0.3rem', marginLeft: 'auto' }}>
            <button
              onClick={() => setSideFilters([])}
              style={{
                padding: '3px 10px',
                borderRadius: '5px',
                border: '1px solid',
                fontSize: '0.62rem',
                fontWeight: '500',
                cursor: 'pointer',
                fontFamily: 'Inter, sans-serif',
                letterSpacing: '0.02em',
                background: sideFilters.length === 0 ? 'var(--bg-surface)' : 'transparent',
                borderColor: sideFilters.length === 0 ? 'var(--border-strong)' : 'var(--border)',
                color: sideFilters.length === 0 ? 'var(--text-primary)' : 'var(--text-muted)',
                transition: 'all 0.15s',
              }}
            >All</button>
            {[
              { key: 'yes',     label: 'Buy YES' },
              { key: 'no',      label: 'Buy NO' },
              { key: 'notrade', label: 'No Trade' },
            ].map(({ key, label }) => {
              const isActive = sideFilters.includes(key);
              const accent = key === 'yes' ? 'var(--green)' : key === 'no' ? 'var(--red)' : 'var(--amber)';
              return (
                <button
                  key={key}
                  onClick={() => {
                    setSideFilters(prev =>
                      prev.includes(key)
                        ? prev.filter(k => k !== key)
                        : [...prev, key]
                    );
                  }}
                  style={{
                    padding: '3px 10px',
                    borderRadius: '5px',
                    border: '1px solid',
                    fontSize: '0.62rem',
                    fontWeight: '500',
                    cursor: 'pointer',
                    fontFamily: 'Inter, sans-serif',
                    letterSpacing: '0.02em',
                    background: 'transparent',
                    borderColor: isActive ? accent : 'var(--border)',
                    color: isActive ? accent : 'var(--text-muted)',
                    transition: 'all 0.15s',
                  }}
                >{label}</button>
              );
            })}
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
                readonly={readonlyMode}
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
