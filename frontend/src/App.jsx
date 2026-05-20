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
  const [backtestCurrentOnly, setBacktestCurrentOnly] = useState(true);
  const [positionBacktestData, setPositionBacktestData] = useState(null);
  const [positionBacktestLoading, setPositionBacktestLoading] = useState(false);
  // Tunable exit thresholds for the Backtest tab (percent units, e.g. "40" = 40%).
  // Empty string = use server defaults from inference.exit_evaluator (.env / code).
  const [tpInput, setTpInput] = useState('');
  const [slInput, setSlInput] = useState('');
  // Realistic mode layers walk-forward + Kalshi fees + stale-brief drop on top.
  const [backtestRealistic, setBacktestRealistic] = useState(false);
  const [backtestTab, setBacktestTab] = useState(0); // 0 = strategy sim, 1 = oracle
  const [oracleBacktestData, setOracleBacktestData] = useState(null);
  const [oracleBacktestLoading, setOracleBacktestLoading] = useState(false);
  const [oracleTpInput, setOracleTpInput] = useState('');
  const [oracleSlInput, setOracleSlInput] = useState('');
  const [showFreshBriefs, setShowFreshBriefs] = useState(true);
  const [showMidBriefs, setShowMidBriefs] = useState(false);
  const [showAgedBriefs, setShowAgedBriefs] = useState(false);
  const toastTimer = useRef(null);

  const showToast = useCallback((msg) => {
    setToast(msg);
    clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(null), 5000);
  }, []);

  const fetchIntelligence = useCallback(async () => {
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 10000);
      const res = await fetch(`${API_BASE}/api/intelligence`, { signal: ctrl.signal });
      clearTimeout(timer);
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

  const fetchPositionBacktest = useCallback(async (
    currentOnly = backtestCurrentOnly,
    tpPct = tpInput,
    slPct = slInput,
    realistic = backtestRealistic,
  ) => {
    setPositionBacktestLoading(true);
    try {
      // Convert percent input ("40") to ROI fraction (0.40) for the API.
      const params = new URLSearchParams({ days: '30', current_system_only: String(currentOnly) });
      const tpNum = parseFloat(tpPct);
      const slNum = parseFloat(slPct);
      if (!Number.isNaN(tpNum) && tpNum > 0) params.set('take_profit', String(tpNum / 100));
      if (!Number.isNaN(slNum) && slNum > 0) params.set('stop_loss',   String(slNum / 100));
      if (realistic) params.set('realistic', 'true');
      const res = await fetch(`${API_BASE}/api/backtest/positions?${params.toString()}`);
      if (!res.ok) return;
      const data = await res.json();
      setPositionBacktestData(data);
    } catch (_) { }
    finally { setPositionBacktestLoading(false); }
  }, [backtestCurrentOnly, tpInput, slInput, backtestRealistic]);

  const fetchOracleBacktest = useCallback(async (tpPct = oracleTpInput, slPct = oracleSlInput) => {
    setOracleBacktestLoading(true);
    try {
      const params = new URLSearchParams({ sim_bankroll: '1000' });
      const tpNum = parseFloat(tpPct);
      const slNum = parseFloat(slPct);
      if (!Number.isNaN(tpNum) && tpNum > 0) params.set('take_profit', String(tpNum / 100));
      if (!Number.isNaN(slNum) && slNum > 0) params.set('stop_loss',   String(slNum / 100));
      const res = await fetch(`${API_BASE}/api/backtest/oracle?${params.toString()}`);
      if (!res.ok) return;
      const data = await res.json();
      setOracleBacktestData(data);
    } catch (_) { }
    finally { setOracleBacktestLoading(false); }
  }, [oracleTpInput, oracleSlInput]);

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

  // Split briefs into 3 buckets matching the staleness badge colors:
  //   fresh (<3h, green)  ·  mid (3-6h, amber)  ·  aged (>6h, red)
  const ageMinOf = (b) =>
    b.ingested_at ? (Date.now() - new Date(b.ingested_at).getTime()) / 60_000 : 0;
  const freshBriefs = displayBriefs.filter(b => ageMinOf(b) <= 180);
  const midBriefs   = displayBriefs.filter(b => { const a = ageMinOf(b); return a > 180 && a <= 360; });
  const agedBriefs  = displayBriefs.filter(b => ageMinOf(b) > 360);

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
      {/* HEADER */}
      <header className="header">
        <div className="header-brand">
          <div className="header-logo">K</div>
          <div>
            <div className="header-title">Kalshi Custom Predictions</div>
            <div className="header-subtitle">Predictive Market Movements</div>
          </div>
        </div>

        <div className="header-meta">
          {import.meta.env.VITE_APP_ENV === 'dev' && (
            <span style={{ fontSize: '0.62rem', color: 'var(--amber)', fontFamily: 'monospace', letterSpacing: '0.06em', background: 'var(--amber-dim)', padding: '1px 6px', borderRadius: '4px' }}>
              DEV
            </span>
          )}
          {readonlyMode && (
            <span style={{ fontSize: '0.62rem', color: 'var(--text-muted)', fontFamily: 'monospace', letterSpacing: '0.06em', opacity: 0.7 }}>
              view only
            </span>
          )}
          {/* BACKTEST BUTTON */}
          <button
            className="positions-header-btn"
            onClick={() => { setShowBacktest(v => !v); if (!positionBacktestData) fetchPositionBacktest(); }}
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

              {/* TAB BAR */}
              <div style={{ display: 'flex', gap: '0', marginBottom: '1.25rem', borderBottom: '1px solid var(--border)' }}>
                {[
                  { label: 'Strategy Simulation', subtitle: 'LLM briefs · last 30d' },
                  { label: 'Oracle Backtest', subtitle: '250+ settled markets' },
                ].map((tab, idx) => (
                  <button
                    key={idx}
                    onClick={() => {
                      setBacktestTab(idx);
                      if (idx === 0 && !positionBacktestData) fetchPositionBacktest();
                      if (idx === 1 && !oracleBacktestData) fetchOracleBacktest();
                    }}
                    style={{
                      padding: '0.45rem 1rem', fontSize: '0.68rem', fontWeight: 600,
                      border: 'none', borderBottom: backtestTab === idx ? '2px solid var(--blue)' : '2px solid transparent',
                      background: 'transparent', cursor: 'pointer',
                      color: backtestTab === idx ? 'var(--text-primary)' : 'var(--text-muted)',
                      transition: 'color 0.15s',
                    }}
                  >
                    {tab.label}
                    <span style={{ display: 'block', fontSize: '0.55rem', fontWeight: 400, color: 'var(--text-muted)', marginTop: '1px' }}>{tab.subtitle}</span>
                  </button>
                ))}
              </div>

              {/* ── TAB 0: STRATEGY SIMULATION (LLM briefs) ── */}
              {backtestTab === 0 && (() => {
                const OUTCOME_META = {
                  PROFIT_EXIT:   { label: 'PROFIT EXIT',   color: 'var(--green)' },
                  STOP_EXIT:     { label: 'STOP LOSS',     color: 'var(--red)' },
                  SETTLED_WIN:   { label: 'SETTLED WIN',   color: 'var(--green)' },
                  SETTLED_LOSS:  { label: 'SETTLED LOSS',  color: 'var(--red)' },
                  OPEN:          { label: 'OPEN',          color: 'var(--blue)' },
                };
                return (
                  <>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
                      <label style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '0.62rem', color: 'var(--text-secondary)', cursor: 'pointer', userSelect: 'none' }}>
                        <input type="checkbox" checked={backtestCurrentOnly} onChange={e => { const v = e.target.checked; setBacktestCurrentOnly(v); fetchPositionBacktest(v); }} style={{ cursor: 'pointer' }} />
                        Current pipeline only
                      </label>
                      <label
                        title="Walk-forward only (no peeking pre-brief), subtract Kalshi fees, drop briefs whose stated entry price disagrees with the actual market at brief time by >10c"
                        style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '0.62rem', color: backtestRealistic ? 'var(--amber)' : 'var(--text-secondary)', cursor: 'pointer', userSelect: 'none' }}
                      >
                        <input
                          type="checkbox"
                          checked={backtestRealistic}
                          onChange={e => { const v = e.target.checked; setBacktestRealistic(v); fetchPositionBacktest(backtestCurrentOnly, tpInput, slInput, v); }}
                          style={{ cursor: 'pointer' }}
                        />
                        Realistic mode (walk-forward + fees + drop stale briefs)
                      </label>
                      <button onClick={() => fetchPositionBacktest()} style={{ padding: '3px 10px', borderRadius: '5px', border: '1px solid var(--border)', fontSize: '0.62rem', cursor: 'pointer', background: 'transparent', color: 'var(--text-muted)' }}>
                        {positionBacktestLoading ? 'Loading…' : 'Refresh'}
                      </button>
                      {positionBacktestData?.stats && (() => {
                        const tpDefault = positionBacktestData.stats.take_profit_pct;
                        const slDefault = positionBacktestData.stats.stop_loss_pct;
                        const inputStyle = {
                          width: '52px', padding: '2px 4px', fontSize: '0.62rem',
                          background: 'var(--bg-card)', color: 'var(--text-primary)',
                          border: '1px solid var(--border)', borderRadius: '4px',
                          fontFamily: 'JetBrains Mono, monospace', textAlign: 'center',
                        };
                        const labelStyle = { fontSize: '0.58rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' };
                        const apply = () => fetchPositionBacktest(backtestCurrentOnly, tpInput, slInput);
                        const reset = () => { setTpInput(''); setSlInput(''); fetchPositionBacktest(backtestCurrentOnly, '', ''); };
                        const isOverride = tpInput !== '' || slInput !== '';
                        return (
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                            <span style={labelStyle}>TP %</span>
                            <input
                              type="number" step="1" min="1" max="500"
                              placeholder={`${tpDefault}`}
                              value={tpInput}
                              onChange={e => setTpInput(e.target.value)}
                              onKeyDown={e => { if (e.key === 'Enter') apply(); }}
                              style={inputStyle}
                            />
                            <span style={labelStyle}>SL %</span>
                            <input
                              type="number" step="1" min="1" max="100"
                              placeholder={`${slDefault}`}
                              value={slInput}
                              onChange={e => setSlInput(e.target.value)}
                              onKeyDown={e => { if (e.key === 'Enter') apply(); }}
                              style={inputStyle}
                            />
                            <button
                              onClick={apply}
                              disabled={positionBacktestLoading || !isOverride}
                              style={{
                                padding: '3px 10px', borderRadius: '5px',
                                border: '1px solid var(--border-strong)',
                                fontSize: '0.62rem', cursor: positionBacktestLoading || !isOverride ? 'not-allowed' : 'pointer',
                                background: isOverride ? 'var(--bg-surface)' : 'transparent',
                                color: isOverride ? 'var(--text-primary)' : 'var(--text-muted)',
                                opacity: positionBacktestLoading || !isOverride ? 0.5 : 1,
                              }}
                            >Apply</button>
                            {isOverride && (
                              <button
                                onClick={reset}
                                disabled={positionBacktestLoading}
                                style={{
                                  padding: '3px 8px', borderRadius: '5px',
                                  border: '1px solid var(--border)',
                                  fontSize: '0.6rem', cursor: 'pointer',
                                  background: 'transparent', color: 'var(--text-muted)',
                                }}
                              >Reset</button>
                            )}
                            <span style={{ fontSize: '0.58rem', color: 'var(--text-muted)', fontFamily: 'JetBrains Mono, monospace', marginLeft: 'auto' }}>
                              now: +{tpDefault}% / -{slDefault}%
                            </span>
                          </div>
                        );
                      })()}
                    </div>

                    {positionBacktestLoading && !positionBacktestData ? (
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', padding: '1rem 0' }}>Replaying price history…</div>
                    ) : positionBacktestData ? (() => {
                      const s = positionBacktestData.stats ?? {};
                      const trades = positionBacktestData.trades ?? [];
                      const winRate = s.win_rate != null ? (s.win_rate * 100).toFixed(0) : null;
                      const totalPnl = s.total_pnl ?? 0;
                      return (
                        <>
                          {/* STAT STRIP */}
                          <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1.25rem', flexWrap: 'wrap' }}>
                            {[
                              { label: 'Trades', value: s.total ?? 0, color: 'var(--text-primary)' },
                              { label: 'Wins', value: s.wins ?? 0, color: 'var(--green)' },
                              { label: 'Losses', value: s.losses ?? 0, color: 'var(--red)' },
                              { label: 'Open', value: s.open ?? 0, color: 'var(--blue)' },
                              { label: 'Win Rate', value: winRate != null ? `${winRate}%` : 'N/A', color: s.win_rate >= 0.55 ? 'var(--green)' : s.win_rate != null ? 'var(--red)' : 'var(--text-muted)' },
                              { label: 'Total P&L', value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`, color: totalPnl >= 0 ? 'var(--green)' : 'var(--red)' },
                            ].map(({ label, value, color }) => (
                              <div key={label} style={{ minWidth: '80px' }}>
                                <div style={{ fontSize: '0.58rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '2px' }}>{label}</div>
                                <div style={{ fontSize: '1.05rem', fontWeight: 700, color, fontFamily: 'JetBrains Mono, monospace' }}>{value}</div>
                              </div>
                            ))}
                          </div>

                          {/* TABLE */}
                          <div style={{ overflowX: 'auto' }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.68rem', fontFamily: 'JetBrains Mono, monospace' }}>
                              <thead>
                                <tr style={{ borderBottom: '1px solid var(--border)', color: 'var(--text-muted)', fontSize: '0.58rem', textTransform: 'uppercase' }}>
                                  <th style={{ textAlign: 'left', padding: '6px 8px', fontWeight: 500 }}>Entry</th>
                                  <th style={{ textAlign: 'left', padding: '6px 8px', fontWeight: 500 }}>Market</th>
                                  <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Side</th>
                                  <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Buy</th>
                                  <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Qty</th>
                                  <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Outcome</th>
                                  <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Exit</th>
                                  <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Peak ROI</th>
                                  <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>P&L</th>
                                </tr>
                              </thead>
                              <tbody>
                                {trades.map((t, i) => {
                                  const meta = OUTCOME_META[t.outcome] ?? { label: t.outcome, color: 'var(--text-muted)' };
                                  const pnlColor = t.pnl == null ? 'var(--text-muted)' : t.pnl >= 0 ? 'var(--green)' : 'var(--red)';
                                  const entryC = Math.round((t.entry_price ?? 0) * 100);
                                  const exitC = t.exit_price != null ? Math.round(t.exit_price * 100) : null;
                                  return (
                                    <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                                      <td style={{ padding: '5px 8px', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{t.entry_time ?? '—'}</td>
                                      <td style={{ padding: '5px 8px', maxWidth: '260px' }}>
                                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={t.title || t.ticker}>{t.ticker}</div>
                                        {t.title && t.title !== t.ticker && <div style={{ fontSize: '0.58rem', color: 'var(--text-muted)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{t.title}</div>}
                                      </td>
                                      <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                        <span style={{ color: t.side === 'yes' ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>{t.side?.toUpperCase()}</span>
                                      </td>
                                      <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-secondary)' }}>{entryC}¢</td>
                                      <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-muted)' }}>{t.qty}</td>
                                      <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                        <span style={{ color: meta.color, fontWeight: 700, fontSize: '0.62rem' }}>{meta.label}</span>
                                      </td>
                                      <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-secondary)' }}>
                                        {exitC != null ? `${exitC}¢` : '—'}
                                        {t.exit_time && <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)' }}>{t.exit_time}</div>}
                                      </td>
                                      <td style={{ padding: '5px 8px', textAlign: 'center', color: t.peak_roi_pct >= 20 ? 'var(--green)' : 'var(--text-muted)' }}>
                                        {t.peak_roi_pct != null ? `+${t.peak_roi_pct}%` : '—'}
                                      </td>
                                      <td style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 700, color: pnlColor }}>
                                        {t.pnl == null ? '—' : `${t.pnl >= 0 ? '+' : ''}$${t.pnl.toFixed(2)}`}
                                      </td>
                                    </tr>
                                  );
                                })}
                              </tbody>
                            </table>
                          </div>
                          <div style={{ fontSize: '0.6rem', color: 'var(--text-muted)', marginTop: '0.75rem' }}>
                            Simulates current exit rules against historical price snapshots from entry date. P&L based on actual qty held. Not financial advice.
                          </div>
                        </>
                      );
                    })() : null}
                  </>
                );
              })()}

              {/* ── TAB 1: ORACLE BACKTEST (settled markets) ── */}
              {backtestTab === 1 && (() => {
                const OUTCOME_META = {
                  PROFIT_EXIT:  { label: 'PROFIT EXIT',  color: 'var(--green)' },
                  STOP_EXIT:    { label: 'STOP LOSS',    color: 'var(--red)' },
                  SETTLED_WIN:  { label: 'SETTLED WIN',  color: 'var(--green)' },
                };
                const s = oracleBacktestData?.stats ?? {};
                const trades = oracleBacktestData?.trades ?? [];
                const tpDef = s.take_profit_pct;
                const slDef = s.stop_loss_pct;
                const inputStyle = {
                  width: '52px', padding: '2px 4px', fontSize: '0.62rem',
                  background: 'var(--bg-card)', color: 'var(--text-primary)',
                  border: '1px solid var(--border)', borderRadius: '4px',
                  fontFamily: 'JetBrains Mono, monospace', textAlign: 'center',
                };
                const labelStyle = { fontSize: '0.58rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' };
                const isOverride = oracleTpInput !== '' || oracleSlInput !== '';
                const applyOracle = () => fetchOracleBacktest(oracleTpInput, oracleSlInput);
                const resetOracle = () => { setOracleTpInput(''); setOracleSlInput(''); fetchOracleBacktest('', ''); };
                return (
                  <>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
                      <div style={{ fontSize: '0.65rem', color: 'var(--amber)', fontWeight: 600 }}>
                        Oracle mode — direction always correct (upper bound)
                      </div>
                      <button onClick={applyOracle} style={{ padding: '3px 10px', borderRadius: '5px', border: '1px solid var(--border)', fontSize: '0.62rem', cursor: 'pointer', background: 'transparent', color: 'var(--text-muted)', marginLeft: 'auto' }}>
                        {oracleBacktestLoading ? 'Loading…' : 'Refresh'}
                      </button>
                      {(oracleBacktestData || oracleBacktestLoading) && (
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                          <span style={labelStyle}>TP %</span>
                          <input type="number" step="1" min="1" max="500"
                            placeholder={tpDef ? `${tpDef}` : '40'}
                            value={oracleTpInput}
                            onChange={e => setOracleTpInput(e.target.value)}
                            onKeyDown={e => { if (e.key === 'Enter') applyOracle(); }}
                            style={inputStyle}
                          />
                          <span style={labelStyle}>SL %</span>
                          <input type="number" step="1" min="1" max="100"
                            placeholder={slDef ? `${slDef}` : '17'}
                            value={oracleSlInput}
                            onChange={e => setOracleSlInput(e.target.value)}
                            onKeyDown={e => { if (e.key === 'Enter') applyOracle(); }}
                            style={inputStyle}
                          />
                          <button onClick={applyOracle} disabled={oracleBacktestLoading || !isOverride}
                            style={{ padding: '3px 10px', borderRadius: '5px', border: '1px solid var(--border-strong)', fontSize: '0.62rem', cursor: isOverride ? 'pointer' : 'not-allowed', background: isOverride ? 'var(--bg-surface)' : 'transparent', color: isOverride ? 'var(--text-primary)' : 'var(--text-muted)', opacity: isOverride ? 1 : 0.5 }}
                          >Apply</button>
                          {isOverride && (
                            <button onClick={resetOracle} disabled={oracleBacktestLoading}
                              style={{ padding: '3px 8px', borderRadius: '5px', border: '1px solid var(--border)', fontSize: '0.6rem', cursor: 'pointer', background: 'transparent', color: 'var(--text-muted)' }}
                            >Reset</button>
                          )}
                        </div>
                      )}
                    </div>

                    {oracleBacktestLoading && !oracleBacktestData ? (
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', padding: '1rem 0' }}>Scanning 250+ settled markets…</div>
                    ) : oracleBacktestData ? (
                      <>
                        <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1.25rem', flexWrap: 'wrap' }}>
                          {[
                            { label: 'Trades',     value: s.total ?? 0,                             color: 'var(--text-primary)' },
                            { label: 'Wins',       value: s.wins ?? 0,                              color: 'var(--green)' },
                            { label: 'Stop Exits', value: s.stop_exits ?? 0,                        color: 'var(--red)' },
                            { label: 'Win Rate',   value: s.win_rate != null ? `${(s.win_rate*100).toFixed(0)}%` : 'N/A', color: (s.win_rate ?? 0) >= 0.55 ? 'var(--green)' : 'var(--red)' },
                            { label: 'Avg Win',    value: s.avg_win != null ? `${s.avg_win >= 0 ? '+' : ''}$${s.avg_win?.toFixed(2)}` : 'N/A', color: 'var(--green)' },
                            { label: 'Avg Loss',   value: s.avg_loss != null ? `$${s.avg_loss?.toFixed(2)}` : 'N/A', color: 'var(--red)' },
                            { label: 'R/R',        value: s.rr != null ? `${s.rr}×` : 'N/A',       color: 'var(--text-secondary)' },
                            { label: 'Total P&L',  value: s.total_pnl != null ? `${s.total_pnl >= 0 ? '+' : ''}$${s.total_pnl?.toFixed(2)}` : 'N/A', color: (s.total_pnl ?? 0) >= 0 ? 'var(--green)' : 'var(--red)' },
                          ].map(({ label, value, color }) => (
                            <div key={label} style={{ minWidth: '70px' }}>
                              <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '2px' }}>{label}</div>
                              <div style={{ fontSize: '0.95rem', fontWeight: 700, color, fontFamily: 'JetBrains Mono, monospace' }}>{value}</div>
                            </div>
                          ))}
                        </div>
                        <div style={{ overflowX: 'auto' }}>
                          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.68rem', fontFamily: 'JetBrains Mono, monospace' }}>
                            <thead>
                              <tr style={{ borderBottom: '1px solid var(--border)', color: 'var(--text-muted)', fontSize: '0.58rem', textTransform: 'uppercase' }}>
                                <th style={{ textAlign: 'left',   padding: '6px 8px', fontWeight: 500 }}>Entry</th>
                                <th style={{ textAlign: 'left',   padding: '6px 8px', fontWeight: 500 }}>Market</th>
                                <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Side</th>
                                <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Buy</th>
                                <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Outcome</th>
                                <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Exit</th>
                                <th style={{ textAlign: 'center', padding: '6px 8px', fontWeight: 500 }}>Peak ROI</th>
                                <th style={{ textAlign: 'right',  padding: '6px 8px', fontWeight: 500 }}>P&L</th>
                              </tr>
                            </thead>
                            <tbody>
                              {trades.map((t, i) => {
                                const meta = OUTCOME_META[t.outcome] ?? { label: t.outcome, color: 'var(--text-muted)' };
                                const pnlColor = t.pnl >= 0 ? 'var(--green)' : 'var(--red)';
                                const entryC = Math.round((t.entry_price ?? 0) * 100);
                                const exitC  = t.exit_price != null ? Math.round(t.exit_price * 100) : null;
                                return (
                                  <tr key={i} style={{ borderBottom: '1px solid var(--border)' }}>
                                    <td style={{ padding: '5px 8px', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{t.entry_date ?? '—'}</td>
                                    <td style={{ padding: '5px 8px', maxWidth: '260px' }}>
                                      <div style={{ fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={t.title || t.ticker}>{t.ticker}</div>
                                      {t.title && t.title !== t.ticker && <div style={{ fontSize: '0.58rem', color: 'var(--text-muted)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{t.title}</div>}
                                    </td>
                                    <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                      <span style={{ color: t.side === 'yes' ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>{t.side?.toUpperCase()}</span>
                                    </td>
                                    <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-secondary)' }}>{entryC}¢</td>
                                    <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                      <span style={{ color: meta.color, fontWeight: 700, fontSize: '0.62rem' }}>{meta.label}</span>
                                    </td>
                                    <td style={{ padding: '5px 8px', textAlign: 'center', color: 'var(--text-secondary)' }}>
                                      {exitC != null ? `${exitC}¢` : '—'}
                                      {t.exit_date && <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)' }}>{t.exit_date}</div>}
                                    </td>
                                    <td style={{ padding: '5px 8px', textAlign: 'center', color: t.peak_roi_pct >= 20 ? 'var(--green)' : 'var(--text-muted)' }}>
                                      {t.peak_roi_pct != null ? `+${t.peak_roi_pct}%` : '—'}
                                    </td>
                                    <td style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 700, color: pnlColor }}>
                                      {`${t.pnl >= 0 ? '+' : ''}$${t.pnl.toFixed(2)}`}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                        <div style={{ fontSize: '0.6rem', color: 'var(--text-muted)', marginTop: '0.75rem' }}>
                          Oracle upper bound — assumes perfect direction prediction on every settled market in our bronze snapshots.
                          Real model performance will be lower. Tests whether TP/SL rules add value vs. holding to settlement.
                        </div>
                      </>
                    ) : (
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', padding: '1rem 0' }}>
                        Click Refresh to load oracle simulation.
                      </div>
                    )}
                  </>
                );
              })()}

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
            Connecting…
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
          <>
            {(() => {
              const renderSection = (label, items, open, setOpen, dotColor, marginTop) => {
                const isEmpty = items.length === 0;
                return (
                  <div style={{ marginTop }}>
                    <button
                      onClick={() => !isEmpty && setOpen(v => !v)}
                      disabled={isEmpty}
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        width: '100%',
                        padding: '10px 14px',
                        borderRadius: '6px',
                        border: '1px solid var(--border)',
                        background: 'var(--bg-surface)',
                        color: 'var(--text-secondary)',
                        fontSize: '0.72rem',
                        fontFamily: 'JetBrains Mono, monospace',
                        letterSpacing: '0.04em',
                        cursor: isEmpty ? 'default' : 'pointer',
                        textAlign: 'left',
                        opacity: isEmpty ? 0.55 : 1,
                      }}
                    >
                      <span style={{ color: dotColor, width: '0.7em', display: 'inline-block' }}>
                        {isEmpty ? '·' : (open ? '▾' : '▸')}
                      </span>
                      <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: '50%', background: dotColor }} />
                      <span>{label}</span>
                      <span style={{ color: 'var(--text-muted)' }}>· {items.length} {items.length === 1 ? 'signal' : 'signals'}</span>
                    </button>
                    {open && !isEmpty && (
                      <div className="briefs-grid" style={{ marginTop: '0.75rem' }}>
                        {items.map(brief => (
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
                );
              };
              return (
                <>
                  {renderSection('<3hrs',  freshBriefs, showFreshBriefs, setShowFreshBriefs, 'var(--green)', 0)}
                  {renderSection('3-6hrs', midBriefs,   showMidBriefs,   setShowMidBriefs,   'var(--amber)', '1rem')}
                  {renderSection('>6hrs',  agedBriefs,  showAgedBriefs,  setShowAgedBriefs,  'var(--red)',   '1rem')}
                </>
              );
            })()}
          </>
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
