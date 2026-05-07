import { useState, useCallback } from 'react';

const API_BASE = window.location.hostname === 'localhost' 
  ? 'http://localhost:8000' 
  : `http://${window.location.hostname}:8000`;


export default function MispricingCard({ brief, bankroll, onTradeResult, onViewDetails }) {
  const [executing, setExecuting] = useState(false);
  const { kelly } = brief;

  const deltaAbs = Math.abs(brief.odds_delta * 100).toFixed(1);
  const deltaSign = brief.odds_delta >= 0 ? '+' : '-';
  const oddsDisplay = `${(brief.current_odds * 100).toFixed(0)}¢`;
  const confidencePct = (brief.confidence_score * 100).toFixed(0);

  const bet = kelly?.suggested_bet_usd ?? 0;
  const safeMode = brief.safe_mode;

  const handleTrade = useCallback(async (e) => {
    e.stopPropagation(); // Prevent modal when clicking trade
    setExecuting(true);
    try {
      const priceInCents = Math.round(brief.current_odds * 100);
      const contracts = bet > 0 ? Math.max(1, Math.round(bet / brief.current_odds)) : 1;

      const res = await fetch(`${API_BASE}/api/trade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker: brief.ticker,
          side: 'yes',
          count: contracts,
          price_cents: priceInCents,
        }),
      });

      const data = await res.json();
      onTradeResult({
        type: data.status === 'SAFE_MODE_LOGGED' ? 'warning' : 'success',
        message: data.status === 'SAFE_MODE_LOGGED'
          ? `Safe Mode: ${brief.ticker} logged (${contracts} contracts @ ${priceInCents}¢)`
          : `Trade submitted: ${brief.ticker}`,
      });

    } catch (err) {
      onTradeResult({ type: 'error', message: `Failed: ${err.message}` });
    } finally {
      setExecuting(false);
    }
  }, [brief, bet, onTradeResult]);

  const truncate = (text, n = 160) =>
    text && text.length > n ? text.slice(0, n) + '…' : text ?? '—';

  // Parse the JSON verdict if Groq returned JSON string
  let verdictText = brief.verdict || '';
  try {
    const parsed = JSON.parse(verdictText);
    verdictText = parsed.analysis || parsed.verdict || parsed.prediction || JSON.stringify(parsed);
  } catch (_) { /* already a plain string */ }

  return (
    <div className="brief-card" onClick={onViewDetails} style={{ cursor: 'pointer' }}>
      {/* HEADER */}
      <div className="card-header">
        <div>
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

      {/* BODY */}
      <div className="card-body">
        {/* Bull / Bear */}
        <div className="analysis-row">
          <div className="analysis-block bull">
            <div className="analysis-label">Bull Case</div>
            <div className="analysis-text">{truncate(brief.bull_case, 120)}</div>
          </div>
          <div className="analysis-block bear">
            <div className="analysis-label">Bear Case</div>
            <div className="analysis-text">{truncate(brief.bear_case, 120)}</div>
          </div>
        </div>

        {/* AI Verdict */}
        <div className="verdict-block">{truncate(verdictText, 200)}</div>

        {/* Kelly Criterion */}
        <div className="kelly-section">
          <div className="kelly-info">
            <div className="kelly-label">Kelly Criterion Bet</div>
            <div className="kelly-bet">
              {bet > 0 ? `$${bet.toFixed(2)}` : 'No Edge'}
            </div>
            <div className="kelly-reasoning">
              {kelly?.edge_detected
                ? `${(kelly.capped_fraction * 100).toFixed(1)}% of $${bankroll?.toFixed(0)} bankroll`
                : kelly?.reasoning?.slice(0, 60) ?? '—'}
            </div>
          </div>
          <div className="confidence-bar-wrapper">
            <div className="confidence-track">
              <div
                className="confidence-fill"
                style={{ width: `${confidencePct}%` }}
              />
            </div>
            <div className="confidence-label">{confidencePct}% confidence</div>
          </div>
        </div>
      </div>

      {/* FOOTER */}
      <div className="card-footer">
        <div className="ingested-at">
          {new Date(brief.ingested_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
        </div>
        <button
          className={`trade-btn ${executing ? 'executing' : safeMode ? 'safe' : 'approve'}`}
          onClick={handleTrade}
          disabled={executing}
        >
          {executing
            ? 'Processing…'
            : safeMode
            ? '🔒 Simulate Trade'
            : '⚡ Execute Trade'}
        </button>
      </div>
    </div>
  );
}
