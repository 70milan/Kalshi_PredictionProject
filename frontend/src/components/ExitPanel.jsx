import { useState, useEffect, useCallback } from 'react';

const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000'
  : `http://${window.location.hostname}:8000`;
const POLL_MS = 30_000;

const ACTION_META = {
  SELL_PROFIT:  { label: 'SELL — Lock Profit',   color: 'var(--green)', bg: 'var(--green-dim)' },
  SELL_LOSS:    { label: 'SELL — Cut Losses',    color: 'var(--red)',   bg: 'var(--red-dim)' },
  SELL_FLIP:    { label: 'SELL — Thesis Flipped', color: 'var(--amber)', bg: 'var(--amber-dim)' },
  SELL_TIMEOUT: { label: 'SELL — Time Decay',    color: 'var(--blue)',  bg: 'var(--blue-dim)' },
  HOLD:         { label: 'HOLD',                  color: 'var(--text-muted)', bg: 'transparent' },
};

export default function ExitPanel() {
  const [signals, setSignals] = useState([]);
  const [asOf, setAsOf] = useState(null);
  const [actionable, setActionable] = useState(0);
  const [collapsed, setCollapsed] = useState(false);

  const fetchExits = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/exits`);
      if (!res.ok) return;
      const data = await res.json();
      setSignals(data.signals ?? []);
      setAsOf(data.as_of ? new Date(data.as_of) : null);
      setActionable(data.actionable ?? 0);
    } catch (_) { }
  }, []);

  useEffect(() => {
    fetchExits();
    const interval = setInterval(fetchExits, POLL_MS);
    return () => clearInterval(interval);
  }, [fetchExits]);

  if (signals.length === 0) return null;

  const ageMin = asOf ? Math.floor((Date.now() - asOf.getTime()) / 60_000) : null;

  return (
    <div style={{ marginBottom: '1.25rem' }}>
      <div
        className="section-row"
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.6rem',
          marginBottom: '0.6rem',
          cursor: 'pointer',
        }}
        onClick={() => setCollapsed(c => !c)}
      >
        <div className="section-heading" style={{ marginBottom: 0, flex: 'none' }}>
          Open Positions · Exit Recommendations
        </div>
        {actionable > 0 && (
          <span style={{
            fontSize: '0.6rem',
            fontWeight: 700,
            padding: '2px 8px',
            borderRadius: '10px',
            background: 'var(--amber-dim)',
            color: 'var(--amber)',
            letterSpacing: '0.06em',
          }}>
            {actionable} ACTION{actionable === 1 ? '' : 'S'} SUGGESTED
          </span>
        )}
        <span style={{
          marginLeft: 'auto',
          fontSize: '0.65rem',
          color: 'var(--text-muted)',
          fontFamily: 'JetBrains Mono, monospace',
        }}>
          {ageMin !== null && `evaluated ${ageMin}m ago`} {collapsed ? '▸' : '▾'}
        </span>
      </div>

      {!collapsed && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
          {signals.map((s, i) => {
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
                  gridTemplateColumns: '2.2fr 0.6fr 0.6fr 0.7fr 1.1fr 1.5fr',
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
                    lineHeight: 1.3,
                    fontFamily: 'JetBrains Mono, monospace',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis'
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
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
