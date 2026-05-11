import { useState, useCallback, useEffect } from 'react';
import { createPortal } from 'react-dom';

const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000'
  : `http://${window.location.hostname}:8000`;

export default function MispricingCard({ brief, bankroll, onTradeResult }) {
  const [executing, setExecuting] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const { kelly } = brief;

  const deltaAbs = Math.abs(brief.odds_delta * 100).toFixed(1);
  const deltaSign = brief.odds_delta >= 0 ? '+' : '-';
  const oddsDisplay = `${(brief.current_odds * 100).toFixed(0)}¢`;
  const confidencePct = (brief.confidence_score * 100).toFixed(0);
  const bet = kelly?.suggested_bet_usd ?? 0;
  const safeMode = brief.safe_mode;
  const isNoTrade = /no trade/i.test(brief.verdict ?? '');

  // Close modal on Escape
  useEffect(() => {
    if (!isExpanded) return;
    const onKey = (e) => { if (e.key === 'Escape') setIsExpanded(false); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isExpanded]);

  const handleTrade = useCallback(async (e) => {
    e.stopPropagation();
    if (isNoTrade) return;
    setExecuting(true);
    try {
      const side = brief.recommended_side ?? 'yes';

      let priceInCents;
      try {
        const mktRes = await fetch(`${API_BASE}/api/market/${brief.ticker}`);
        if (mktRes.ok) {
          const mktData = await mktRes.json();
          // To get an instant fill, we must cross the spread and buy at the ASK price.
          const rawAsk = side === 'yes' ? mktData.market?.yes_ask : mktData.market?.no_ask;
          if (rawAsk !== null && rawAsk !== undefined) {
            priceInCents = rawAsk > 1 ? Math.round(rawAsk) : Math.round(rawAsk * 100);
          }
        }
      } catch (_) { /* fall through */ }

      if (!priceInCents) {
        const unitCost = side === 'yes' ? brief.current_odds : (1 - brief.current_odds);
        priceInCents = Math.max(1, Math.round(unitCost * 100));
      }

      const unitCostDollars = priceInCents / 100;
      const contracts = bet > 0 ? Math.max(1, Math.round(bet / unitCostDollars)) : 1;

      const res = await fetch(`${API_BASE}/api/trade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: brief.ticker, side, count: contracts, price_cents: priceInCents }),
      });

      const data = await res.json();
      if (data.status === 'SAFE_MODE_LOGGED') {
        onTradeResult({ type: 'warning', message: `Simulated: ${brief.ticker} [${side.toUpperCase()}] ${contracts}x @ ${priceInCents}¢` });
      } else {
        const orderStatus = data.order_status ?? 'submitted';
        const filled = data.fill_count ?? '0';
        const remaining = data.remaining ?? contracts;
        const statusLabel =
          orderStatus === 'resting'   ? `Resting — ${remaining} waiting` :
          orderStatus === 'filled'    ? `Filled: ${filled} contracts` :
          orderStatus === 'cancelled' ? 'Cancelled by exchange' :
          `Status: ${orderStatus}`;
        onTradeResult({
          type: orderStatus === 'filled' ? 'success' : 'warning',
          message: `${brief.ticker} [${side.toUpperCase()}] @ ${priceInCents}¢ — ${statusLabel}`,
        });
      }
    } catch (err) {
      onTradeResult({ type: 'error', message: `Failed: ${err.message}` });
    } finally {
      setExecuting(false);
    }
  }, [brief, bet, isNoTrade, onTradeResult]);

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
          <div className="card-badges">
            <div className={`delta-badge ${brief.odds_delta >= 0 ? 'delta-up' : 'delta-down'}`}>
              {deltaSign}{deltaAbs}%
            </div>
            <div className="odds-badge">{oddsDisplay} YES</div>
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
            {new Date(brief.ingested_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </div>
          <button className={tradeBtnClass} onClick={handleTrade} disabled={executing || isNoTrade}>
            {tradeBtnLabel}
          </button>
        </div>
      </div>

      {/* ── MODAL OVERLAY (centered, fixed) ── */}
      {isExpanded && createPortal(
        <div className="modal-backdrop" onClick={() => setIsExpanded(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <button className="modal-close" onClick={() => setIsExpanded(false)}>×</button>

            <div className="card-header" style={{ borderRadius: '12px 12px 0 0' }}>
              <div style={{ minWidth: 0 }}>
                <div className="card-ticker">{brief.ticker}</div>
                <div className="card-title" style={{ WebkitLineClamp: 'unset', display: 'block' }}>{brief.title}</div>
              </div>
              <div className="card-badges">
                <div className={`delta-badge ${brief.odds_delta >= 0 ? 'delta-up' : 'delta-down'}`}>
                  {deltaSign}{deltaAbs}%
                </div>
                <div className="odds-badge">{oddsDisplay} YES</div>
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
                {new Date(brief.ingested_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </div>
              <button className={tradeBtnClass} onClick={handleTrade} disabled={executing || isNoTrade}>
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
    </>
  );
}
