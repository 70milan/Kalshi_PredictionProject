import { useState, useCallback, useEffect } from 'react';
import { createPortal } from 'react-dom';

const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000'
  : `http://${window.location.hostname}:8000`;

export default function MispricingCard({ brief, bankroll, onTradeResult, onTradeComplete }) {
  const [executing, setExecuting] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [confirm, setConfirm] = useState(null); // { side, priceDollars, recommended }
  const [confirmCount, setConfirmCount] = useState(1);
  const { kelly } = brief;

  const deltaAbs = Math.abs(brief.odds_delta * 100).toFixed(1);
  const deltaSign = brief.odds_delta >= 0 ? '+' : '-';
  const oddsDisplay = `${(brief.current_odds * 100).toFixed(0)}¢`;
  const confidencePct = (brief.confidence_score * 100).toFixed(0);
  const bet = kelly?.suggested_bet_usd ?? 0;
  const safeMode = brief.safe_mode;
  const isNoTrade = /no trade/i.test(brief.verdict ?? '');

  useEffect(() => {
    if (!isExpanded) return;
    const onKey = (e) => { if (e.key === 'Escape') setIsExpanded(false); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isExpanded]);

  // Step 1: clicking the trade button resolves the price and opens the confirm dialog
  const handleTradeClick = useCallback(async (e) => {
    e.stopPropagation();
    if (isNoTrade || executing) return;
    setExecuting(true);
    try {
      const side = brief.recommended_side ?? 'yes';

      let priceDollars =
        side === 'yes'
          ? (brief.live_yes_ask ?? null)
          : (brief.live_yes_bid != null ? parseFloat((1 - brief.live_yes_bid).toFixed(4)) : null);

      if (priceDollars == null) {
        try {
          const mktRes = await fetch(`${API_BASE}/api/market/${brief.ticker}`);
          if (mktRes.ok) {
            const mktData = await mktRes.json();
            const rawAsk = side === 'yes' ? mktData.market?.yes_ask : mktData.market?.no_ask;
            if (rawAsk != null) priceDollars = rawAsk > 1 ? rawAsk / 100 : rawAsk;
          }
        } catch (_) {}
      }
      if (priceDollars == null) {
        priceDollars = side === 'yes' ? brief.current_odds : (1 - brief.current_odds);
      }
      priceDollars = Math.max(0.01, Math.min(0.99, priceDollars));

      const recommended = bet > 0 ? Math.max(1, Math.round(bet / priceDollars)) : 1;
      setConfirmCount(recommended);
      setConfirm({ side, priceDollars, recommended });
    } catch (err) {
      onTradeResult({ type: 'error', message: `Failed to fetch price: ${err.message}` });
    } finally {
      setExecuting(false);
    }
  }, [brief, bet, isNoTrade, executing, onTradeResult]);

  // Step 2: user confirmed count — actually submit
  const handleTradeExecute = useCallback(async () => {
    if (!confirm) return;
    setExecuting(true);
    const { side, priceDollars } = confirm;
    const displayPrice = `${(priceDollars * 100).toFixed(1)}¢`;
    try {
      const res = await fetch(`${API_BASE}/api/trade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: brief.ticker, side, count: confirmCount, price_dollars: priceDollars }),
      });
      const data = await res.json();
      setConfirm(null);
      if (data.status === 'SAFE_MODE_LOGGED') {
        onTradeResult({ type: 'warning', message: `Simulated: ${brief.ticker} [${side.toUpperCase()}] ${confirmCount}x @ ${displayPrice}` });
      } else {
        const orderStatus = data.order_status ?? 'submitted';
        const filled = data.fill_count ?? '0';
        const remaining = data.remaining ?? confirmCount;
        const statusLabel =
          orderStatus === 'resting'   ? `Resting — ${remaining} waiting` :
          orderStatus === 'filled'    ? `Filled: ${filled} contracts` :
          orderStatus === 'cancelled' ? 'Cancelled by exchange' :
          `Status: ${orderStatus}`;
        onTradeResult({
          type: orderStatus === 'filled' ? 'success' : 'warning',
          message: `${brief.ticker} [${side.toUpperCase()}] ${confirmCount}x @ ${displayPrice} — ${statusLabel}`,
        });
      }
    } catch (err) {
      onTradeResult({ type: 'error', message: `Failed: ${err.message}` });
    } finally {
      setExecuting(false);
      onTradeComplete?.();
    }
  }, [confirm, confirmCount, brief, onTradeResult, onTradeComplete]);

  const truncate = (text, n) =>
    text && text.length > n ? text.slice(0, n) + '…' : text ?? '—';

  let verdictText = brief.verdict || '';
  try {
    const parsed = JSON.parse(verdictText);
    verdictText = parsed.analysis || parsed.verdict || parsed.prediction || JSON.stringify(parsed);
  } catch (_) {}

  const tradeBtnClass = `trade-btn ${
    executing ? 'executing' :
    isNoTrade ? 'no-trade'  :
    safeMode
      ? (brief.recommended_side === 'no' ? 'safe-no' : 'safe-yes')
      : (brief.recommended_side === 'no' ? 'buy-no'  : 'buy-yes')
  }`;

  const briefDate = new Date(brief.ingested_at);
  const ageHours = (Date.now() - briefDate.getTime()) / (1000 * 60 * 60);
  const isStale = ageHours > 1.5;
  const staleColor = ageHours < 2 ? '#f59e0b' : ageHours < 6 ? '#f97316' : '#ef4444';

  const tradeBtnLabel =
    executing ? 'Fetching…' :
    isNoTrade ? 'No Trade'  :
    safeMode  ? `Sim ${(brief.recommended_side ?? 'yes').toUpperCase()}` :
                `Buy ${(brief.recommended_side ?? 'yes').toUpperCase()}`;

  return (
    <>
      {/* ── COMPACT CARD (always in grid) ── */}
      <div className="brief-card" onClick={() => setIsExpanded(true)} style={{ cursor: 'pointer' }}>
        <div className="card-header">
          <div style={{ minWidth: 0 }}>
            <div className="card-ticker">{brief.ticker}</div>
            <div className="card-title">{brief.title}</div>
          </div>
          <div className="card-badges" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '4px' }}>
            <div style={{ display: 'flex', gap: '8px' }}>
              <div className={`delta-badge ${brief.odds_delta >= 0 ? 'delta-up' : 'delta-down'}`}>
                {deltaSign}{deltaAbs}%
              </div>
              <div className="odds-badge">{oddsDisplay} YES</div>
            </div>
            {(brief.live_yes_bid != null && brief.live_yes_ask != null) && (
              <div style={{ fontSize: '0.75rem', color: '#9ca3af', fontWeight: '500' }}>
                Live: {Math.round(brief.live_yes_bid * 100)}¢ / {Math.round(brief.live_yes_ask * 100)}¢
              </div>
            )}
          </div>
        </div>

        <div className="card-body">
          <div className="verdict-block">{truncate(verdictText, 100)}</div>
          <div className="kelly-section">
            <div className="kelly-info">
              <div className="kelly-label">Kelly Bet</div>
              <div className="kelly-bet">{bet > 0 ? `$${bet.toFixed(2)}` : 'No Edge'}</div>
            </div>
            <div className="confidence-bar-wrapper">
              <div className="confidence-track">
                <div className="confidence-fill" style={{ width: `${confidencePct}%` }} />
              </div>
              <div className="confidence-label">{confidencePct}% conf</div>
            </div>
          </div>
        </div>

        <div className="card-footer">
          <div className="ingested-at">
            {isStale && <span style={{ color: staleColor, fontWeight: '600', marginRight: '4px' }}>Stale</span>}
            {briefDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </div>
          <button className={tradeBtnClass} onClick={handleTradeClick} disabled={executing || isNoTrade}>
            {tradeBtnLabel}
          </button>
        </div>
      </div>

      {/* ── MODAL OVERLAY (centered, fixed) ── */}
      {isExpanded && createPortal(
        <div className="modal-backdrop" onClick={() => setIsExpanded(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <button className="modal-close" onClick={() => setIsExpanded(false)}>×</button>

            <div className="card-header" style={{ borderRadius: '12px 12px 0 0', paddingRight: '2.5rem' }}>
              <div style={{ minWidth: 0 }}>
                <div className="card-ticker">{brief.ticker}</div>
                <div className="card-title" style={{ WebkitLineClamp: 'unset', display: 'block' }}>{brief.title}</div>
              </div>
              <div className="card-badges" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '4px' }}>
                <div style={{ display: 'flex', gap: '8px' }}>
                  <div className={`delta-badge ${brief.odds_delta >= 0 ? 'delta-up' : 'delta-down'}`}>
                    {deltaSign}{deltaAbs}%
                  </div>
                  <div className="odds-badge">{oddsDisplay} YES</div>
                </div>
                {(brief.live_yes_bid != null && brief.live_yes_ask != null) && (
                  <div style={{ fontSize: '0.75rem', color: '#9ca3af', fontWeight: '500' }}>
                    Live: {Math.round(brief.live_yes_bid * 100)}¢ / {Math.round(brief.live_yes_ask * 100)}¢
                  </div>
                )}
              </div>
            </div>

            <div className="card-body" style={{ gap: '0.75rem' }}>
              <div className="analysis-row">
                <div className="analysis-block bull">
                  <div className="analysis-label">Bull Case</div>
                  <div className="analysis-text" style={{ WebkitLineClamp: 'unset', display: 'block', overflow: 'visible' }}>
                    {brief.bull_case}
                  </div>
                </div>
                <div className="analysis-block bear">
                  <div className="analysis-label">Bear Case</div>
                  <div className="analysis-text" style={{ WebkitLineClamp: 'unset', display: 'block', overflow: 'visible' }}>
                    {brief.bear_case}
                  </div>
                </div>
              </div>

              <div className="verdict-block" style={{ WebkitLineClamp: 'unset', display: 'block', overflow: 'visible' }}>
                {verdictText}
              </div>

              <div className="kelly-section">
                <div className="kelly-info">
                  <div className="kelly-label">Kelly Criterion Bet</div>
                  <div className="kelly-bet">{bet > 0 ? `$${bet.toFixed(2)}` : 'No Edge'}</div>
                  <div className="kelly-reasoning">
                    {kelly?.edge_detected
                      ? `${(kelly.capped_fraction * 100).toFixed(1)}% of $${bankroll?.toFixed(0)} bankroll`
                      : kelly?.reasoning?.slice(0, 80) ?? '—'}
                  </div>
                </div>
                <div className="confidence-bar-wrapper">
                  <div className="confidence-track">
                    <div className="confidence-fill" style={{ width: `${confidencePct}%` }} />
                  </div>
                  <div className="confidence-label">{confidencePct}% confidence</div>
                </div>
              </div>
            </div>

            <div className="card-footer">
              <div className="ingested-at">
                {isStale && <span style={{ color: staleColor, fontWeight: '600', marginRight: '6px' }}>Stale ({Math.floor(ageHours)}h)</span>}
                {briefDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </div>
              <button className={tradeBtnClass} onClick={handleTradeClick} disabled={executing || isNoTrade}>
                {executing ? 'Fetching price…' :
                 isNoTrade ? 'No Trade' :
                 safeMode  ? `Simulate ${(brief.recommended_side ?? 'yes').toUpperCase()}` :
                             `Execute ${(brief.recommended_side ?? 'yes').toUpperCase()}`}
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* ── TRADE CONFIRM DIALOG ── */}
      {confirm && createPortal(
        <div className="modal-backdrop" onClick={() => setConfirm(null)}>
          <div className="modal-card" style={{ maxWidth: '380px' }} onClick={e => e.stopPropagation()}>
            <button className="modal-close" onClick={() => setConfirm(null)}>×</button>
            <div style={{ padding: '1.5rem' }}>
              <div style={{ fontSize: '0.65rem', textTransform: 'uppercase', letterSpacing: '0.1em', color: 'var(--text-muted)', marginBottom: '0.5rem' }}>Confirm Trade</div>
              <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: '0.75rem', color: 'var(--blue)', marginBottom: '0.25rem' }}>{brief.ticker}</div>
              <div style={{ fontSize: '0.8rem', fontWeight: '600', color: 'var(--text-primary)', marginBottom: '1.25rem', lineHeight: 1.4 }}>{brief.title}</div>

              <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1.25rem' }}>
                <div>
                  <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '2px' }}>Side</div>
                  <div className={`order-side ${confirm.side}`} style={{ fontSize: '0.85rem', fontWeight: '700' }}>{confirm.side.toUpperCase()}</div>
                </div>
                <div>
                  <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '2px' }}>Price</div>
                  <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: '0.85rem', fontWeight: '700', color: 'var(--text-primary)' }}>{(confirm.priceDollars * 100).toFixed(1)}¢</div>
                </div>
                <div>
                  <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '2px' }}>Total Cost</div>
                  <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: '0.85rem', fontWeight: '700', color: 'var(--amber)' }}>${(confirmCount * confirm.priceDollars).toFixed(2)}</div>
                </div>
              </div>

              <div style={{ marginBottom: '1.5rem' }}>
                <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '0.5rem' }}>
                  Contracts&nbsp;<span style={{ color: 'var(--blue)' }}>· Kelly recommends {confirm.recommended}</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                  <button
                    onClick={() => setConfirmCount(c => Math.max(1, c - 1))}
                    style={{ width: '32px', height: '32px', borderRadius: '6px', border: '1px solid var(--border)', background: 'var(--bg-surface)', color: 'var(--text-primary)', fontSize: '1.1rem', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                  >−</button>
                  <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: '1.4rem', fontWeight: '700', color: 'var(--text-primary)', minWidth: '2.5rem', textAlign: 'center' }}>{confirmCount}</span>
                  <button
                    onClick={() => setConfirmCount(c => c + 1)}
                    style={{ width: '32px', height: '32px', borderRadius: '6px', border: '1px solid var(--border)', background: 'var(--bg-surface)', color: 'var(--text-primary)', fontSize: '1.1rem', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                  >+</button>
                </div>
              </div>

              <div style={{ display: 'flex', gap: '0.75rem' }}>
                <button
                  onClick={() => setConfirm(null)}
                  style={{ flex: 1, padding: '0.6rem', borderRadius: '8px', border: '1px solid var(--border)', background: 'transparent', color: 'var(--text-muted)', fontSize: '0.78rem', fontWeight: '600', cursor: 'pointer' }}
                >Cancel</button>
                <button
                  onClick={handleTradeExecute}
                  disabled={executing}
                  className={confirm.side === 'no' ? 'trade-btn buy-no' : 'trade-btn buy-yes'}
                  style={{ flex: 2, padding: '0.6rem' }}
                >{executing ? 'Submitting…' : safeMode ? `Simulate ${confirm.side.toUpperCase()} · ${confirmCount}x` : `Execute ${confirm.side.toUpperCase()} · ${confirmCount}x`}</button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}
    </>
  );
}
