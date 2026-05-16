import { useState, useCallback, useEffect } from 'react';
import { createPortal } from 'react-dom';

const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000'
  : `http://${window.location.hostname}:8000`;

export default function MispricingCard({ brief, bankroll, onTradeResult, onTradeComplete, isNew = false }) {
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
  // Strip the leading directive ("Buy NO", "Buy YES", "No Trade") so it doesn't duplicate
  // the bolded prefix we render ourselves
  verdictText = verdictText.replace(/^(Buy\s+(YES|NO)|No\s+Trade)[.\s]*/i, '').trim();

  const tradeBtnClass = `trade-btn ${
    executing ? 'executing' :
    isNoTrade ? 'no-trade'  :
    safeMode
      ? (brief.recommended_side === 'no' ? 'safe-no' : 'safe-yes')
      : (brief.recommended_side === 'no' ? 'buy-no'  : 'buy-yes')
  }`;

  const briefDate = new Date(brief.ingested_at);
  const ageMin = (Date.now() - briefDate.getTime()) / (1000 * 60);
  
  const freshnessColor =
    ageMin <= 30  ? 'var(--blue)'   :
    ageMin <= 90  ? 'var(--green)'  :
    ageMin <= 180 ? 'var(--amber)'  :
    ageMin <= 360 ? '#e89556'       :
                    'var(--red)';

  const freshnessTitle =
    ageMin <= 30  ? 'New (< 30m)'                       :
    ageMin <= 90  ? `Fresh (${Math.floor(ageMin)}m ago)` :
                    `Stale (${(ageMin / 60).toFixed(1)}h ago)`;

  const tradeBtnLabel =
    executing ? 'Fetching…' :
    isNoTrade ? 'No Trade'  :
    safeMode  ? `Sim ${(brief.recommended_side ?? 'yes').toUpperCase()}` :
                `Buy ${(brief.recommended_side ?? 'yes').toUpperCase()}`;

  const verdictPrefix = isNoTrade ? 'No Trade. ' : `Buy ${(brief.recommended_side ?? 'yes').toUpperCase()}. `;

  return (
    <>
      {/* ── COMPACT CARD (always in grid) ── */}
      <div className="brief-card" onClick={() => setIsExpanded(true)} style={{ cursor: 'pointer' }}>
        <div className="card-header">
          {/* Row 1: ticker */}
          <span className="card-ticker">{brief.ticker}</span>

          {/* Row 2: title */}
          <div className="card-title">{brief.title}</div>

          {/* Row 3: delta · YES price · live spread */}
          <div className="ch-price-row">
            <span className={`ch-side-pill ${brief.recommended_side === 'yes' ? 'yes' : 'no'}`}>
              {deltaSign}{deltaAbs}%
            </span>
            <span className="ch-price">{oddsDisplay}</span>
            {brief.live_yes_bid != null && brief.live_yes_ask != null && (() => {
              const side = brief.recommended_side ?? 'yes';
              const bid = side === 'no'
                ? Math.round((1 - brief.live_yes_ask) * 100)
                : Math.round(brief.live_yes_bid * 100);
              const ask = side === 'no'
                ? Math.round((1 - brief.live_yes_bid) * 100)
                : Math.round(brief.live_yes_ask * 100);
              return (
                <span className="ch-live">Live {bid} / {ask}¢</span>
              );
            })()}
          </div>
        </div>

        <div className="card-body">
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
          <div className="ingested-at" title={freshnessTitle}>
            <span className="freshness-dot" style={{ background: freshnessColor }} />
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
            <div className="modal-close-bar">
              <button className="modal-close" onClick={() => setIsExpanded(false)}>×</button>
            </div>
            <div className="modal-body">

            <div className="card-header" style={{ borderRadius: 0 }}>
              <span className="card-ticker">{brief.ticker}</span>
              <div className="card-title" style={{ WebkitLineClamp: 'unset', display: 'block' }}>{brief.title}</div>
              <div className="ch-price-row">
                <span className={`ch-side-pill ${brief.recommended_side === 'yes' ? 'yes' : 'no'}`}>
                  {deltaSign}{deltaAbs}%
                </span>
                <span className="ch-price">{oddsDisplay}</span>
                {brief.live_yes_bid != null && brief.live_yes_ask != null && (() => {
                  const side = brief.recommended_side ?? 'yes';
                  const bid = side === 'no'
                    ? Math.round((1 - brief.live_yes_ask) * 100)
                    : Math.round(brief.live_yes_bid * 100);
                  const ask = side === 'no'
                    ? Math.round((1 - brief.live_yes_bid) * 100)
                    : Math.round(brief.live_yes_bid * 100);
                  return <span className="ch-live">Live {bid} / {ask}¢</span>;
                })()}
              </div>
            </div>

            <div className="card-body" style={{ gap: '0.65rem' }}>

              {/* ── ① SIGNALS ── */}
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                {[
                  { label: 'Spike',     value: brief.max_spike_multiplier != null ? `${Number(brief.max_spike_multiplier).toFixed(1)}×` : null },
                  { label: 'Sentiment', value: brief.sentiment_signal != null ? Number(brief.sentiment_signal).toFixed(2) : null },
                  { label: 'Δ Odds',    value: `${brief.odds_delta >= 0 ? '+' : ''}${(brief.odds_delta * 100).toFixed(1)}%` },
                  { label: 'Score',     value: brief.mispricing_score != null ? Math.round(brief.mispricing_score) : null },
                  { label: 'Conf',      value: `${confidencePct}%` },
                  { label: 'Evidence',  value: brief.rag_score != null ? Number(brief.rag_score).toFixed(2) : null },
                ].filter(p => p.value != null).map(({ label, value }) => (
                  <span key={label} style={{
                    fontSize: '0.62rem',
                    fontFamily: 'JetBrains Mono, monospace',
                    padding: '2px 8px',
                    borderRadius: '4px',
                    background: 'var(--bg-surface)',
                    border: '1px solid var(--border)',
                    color: 'var(--text-secondary)',
                  }}>
                    <span style={{ color: 'var(--text-muted)', marginRight: '4px' }}>{label}</span>{value}
                  </span>
                ))}
              </div>

              {/* ── ② DECISION REASON ── */}
              {(() => {
                const side = brief.recommended_side ?? 'no';
                const accentColor = side === 'yes' ? 'var(--green)' : 'var(--red)';

                // Prefer the LLM-generated decision_reason (available on new briefs).
                // For old briefs, build a factual one-liner from the raw signal values
                // rather than echoing the bear/bull narrative.
                let reasonText = brief.decision_reason || '';
                if (!reasonText) {
                  const parts = [];
                  const sent = brief.sentiment_signal;
                  if (sent != null) {
                    const sentLabel = sent < -0.3 ? 'strongly negative' : sent > 0.3 ? 'strongly positive' : 'neutral';
                    parts.push(`Sentiment ${Number(sent).toFixed(2)} (${sentLabel})`);
                  }
                  const spike = brief.max_spike_multiplier;
                  if (spike != null && spike > 1) parts.push(`news spike ${Number(spike).toFixed(1)}×`);
                  const delta = brief.odds_delta;
                  if (delta != null) parts.push(`odds drift ${delta >= 0 ? '+' : ''}${(delta * 100).toFixed(1)}%`);
                  reasonText = parts.length
                    ? `Signals: ${parts.join(', ')}. Detailed reasoning available after next inference cycle.`
                    : '';
                }

                if (!reasonText) return null;
                return (
                  <div style={{
                    padding: '0.6rem 0.85rem',
                    borderLeft: `3px solid ${accentColor}`,
                    background: 'var(--bg-surface)',
                    borderRadius: '0 6px 6px 0',
                    fontSize: '0.74rem',
                    lineHeight: 1.55,
                    color: 'var(--text-primary)',
                  }}>
                    <div style={{ fontSize: '0.55rem', textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--text-muted)', marginBottom: '4px' }}>
                      Why {side.toUpperCase()}
                    </div>
                    {reasonText}
                  </div>
                );
              })()}

              {/* ── ③ CASES (de-emphasised) ── */}
              <div className="analysis-row" style={{ opacity: 0.75 }}>
                <div className="analysis-block bull">
                  <div className="analysis-label">Bull Case</div>
                  <div className="analysis-text" style={{ WebkitLineClamp: 'unset', display: 'block', overflow: 'visible', fontSize: '0.68rem' }}>
                    {brief.bull_case}
                  </div>
                </div>
                <div className="analysis-block bear">
                  <div className="analysis-label">Bear Case</div>
                  <div className="analysis-text" style={{ WebkitLineClamp: 'unset', display: 'block', overflow: 'visible', fontSize: '0.68rem' }}>
                    {brief.bear_case}
                  </div>
                </div>
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
              <div className="ingested-at" title={freshnessTitle}>
                <span className="freshness-dot" style={{ background: freshnessColor }} />
                {briefDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </div>
              <button className={tradeBtnClass} onClick={handleTradeClick} disabled={executing || isNoTrade}>
                {executing ? 'Fetching price…' :
                 isNoTrade ? 'No Trade' :
                 safeMode  ? `Simulate ${(brief.recommended_side ?? 'yes').toUpperCase()}` :
                             `Execute ${(brief.recommended_side ?? 'yes').toUpperCase()}`}
              </button>
            </div>

            </div>{/* end modal-body */}
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
