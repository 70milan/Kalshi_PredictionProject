"""
PredictIQ - Exit Evaluator
==========================
Reads position_ledger.parquet, joins live Kalshi prices, and emits
SELL/HOLD recommendations for each open position based on 4 triggers.

Triggers (evaluated in priority order):
  1. PROFIT LOCK   — captured >=80% of max possible gain
  2. STOP LOSS     — market moved >=20c against entry
  3. THESIS FLIP   — today's brief recommends the opposite side
  4. TIME DECAY    — resolves <24h with near-zero P&L

Output: data/gold/exit_signals/exits_<timestamp>.parquet
"""

import os
import duckdb
import pandas as pd
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

PROJECT_ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEDGER_PATH        = os.path.join(PROJECT_ROOT, "data", "gold", "position_ledger.parquet")
LATEST_PARQUET     = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open", "latest.parquet")
BRIEFS_PATH        = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
EXIT_SIGNALS_PATH  = os.path.join(PROJECT_ROOT, "data", "gold", "exit_signals")

# Tunable thresholds (Phase 1 defaults — refine after outcome attribution ships)
PROFIT_LOCK_CAPTURE = 0.80   # take profit at 80% of max possible gain
STOP_LOSS_THRESHOLD = 0.20   # cut at 20c against entry
TIME_DECAY_HOURS    = 24     # resolves within X hours
TIME_DECAY_PNL_BAND = 0.05   # AND P&L within +/-5c = "no momentum, exit"


# ─────────────────────────────────────────────
# EVALUATION
# ─────────────────────────────────────────────

def evaluate_position(pos, live_yes_bid, live_yes_ask, close_time, today_brief):
    """
    Evaluate one position against all 4 triggers in priority order.
    Returns: action, reason, urgency (1-5), suggested_exit_price, unrealized_pnl
    """
    side        = pos['side']
    entry_price = float(pos['entry_price'])
    qty         = float(pos['qty'])

    # Compute the price we'd RECEIVE if we sold now (our side's bid)
    if side == 'yes':
        current_value = live_yes_bid           # sell YES at YES bid
    else:
        current_value = 1.0 - live_yes_ask     # sell NO at NO bid = 1 - YES ask

    unrealized_pnl_per = current_value - entry_price
    max_gain_per       = 1.0 - entry_price
    capture_pct        = (unrealized_pnl_per / max_gain_per) if max_gain_per > 0 else 0.0
    total_pnl          = unrealized_pnl_per * qty

    # TRIGGER 1: PROFIT LOCK
    if capture_pct >= PROFIT_LOCK_CAPTURE:
        return {
            'action':                'SELL_PROFIT',
            'reason':                f"Captured {capture_pct*100:.0f}% of max gain. Lock it in.",
            'urgency':               4,
            'suggested_exit_price':  round(current_value, 4),
            'unrealized_pnl':        round(total_pnl, 4),
            'capture_pct':           round(capture_pct, 4),
        }

    # TRIGGER 2: STOP LOSS
    if unrealized_pnl_per <= -STOP_LOSS_THRESHOLD:
        return {
            'action':                'SELL_LOSS',
            'reason':                f"Market moved {abs(unrealized_pnl_per)*100:.0f}c against entry. Thesis broken.",
            'urgency':               5,
            'suggested_exit_price':  round(current_value, 4),
            'unrealized_pnl':        round(total_pnl, 4),
            'capture_pct':           round(capture_pct, 4),
        }

    # TRIGGER 3: THESIS FLIP — today's brief contradicts our position
    if today_brief is not None:
        new_side = today_brief.get('recommended_side')
        verdict  = str(today_brief.get('verdict', '')).lower()
        is_no_trade = 'no trade' in verdict
        if new_side and new_side != side and not is_no_trade:
            return {
                'action':                'SELL_FLIP',
                'reason':                f"Today's brief recommends {new_side.upper()} — opposite of our {side.upper()} position.",
                'urgency':               3,
                'suggested_exit_price':  round(current_value, 4),
                'unrealized_pnl':        round(total_pnl, 4),
                'capture_pct':           round(capture_pct, 4),
            }

    # TRIGGER 4: TIME DECAY
    if close_time is not None:
        try:
            close_dt = pd.to_datetime(close_time)
            if close_dt.tzinfo is None:
                close_dt = close_dt.tz_localize("UTC")
            hours_to_close = (close_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if 0 < hours_to_close < TIME_DECAY_HOURS and abs(unrealized_pnl_per) < TIME_DECAY_PNL_BAND:
                return {
                    'action':                'SELL_TIMEOUT',
                    'reason':                f"Resolves in {hours_to_close:.0f}h with no momentum. Avoid binary settlement risk.",
                    'urgency':               2,
                    'suggested_exit_price':  round(current_value, 4),
                    'unrealized_pnl':        round(total_pnl, 4),
                    'capture_pct':           round(capture_pct, 4),
                }
        except Exception:
            pass

    # No triggers fired
    return {
        'action':                'HOLD',
        'reason':                f"P&L: ${total_pnl:+.2f} ({capture_pct*100:+.0f}% of max). No exit trigger.",
        'urgency':               0,
        'suggested_exit_price':  round(current_value, 4),
        'unrealized_pnl':        round(total_pnl, 4),
        'capture_pct':           round(capture_pct, 4),
    }


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print(" PredictIQ Exit Evaluator")
    print("=" * 60)

    if not os.path.exists(LEDGER_PATH):
        print("[Exit] No ledger found. Run build_position_ledger.py first.")
        return

    ledger = pd.read_parquet(LEDGER_PATH)
    if ledger.empty:
        print("[Exit] No open positions to evaluate.")
        return

    if not os.path.exists(LATEST_PARQUET):
        print("[Exit] latest.parquet missing -- cannot evaluate without live prices.")
        return

    con = duckdb.connect()

    # Live prices from latest.parquet
    latest_path = LATEST_PARQUET.replace("\\", "/")
    prices = con.execute(f"""
        SELECT ticker, yes_bid_dollars, yes_ask_dollars, status, close_time
        FROM read_parquet('{latest_path}')
    """).df()
    price_map = {r['ticker']: r for _, r in prices.iterrows()}

    # Today's most recent brief per ticker (for thesis-flip detection)
    briefs_glob = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
    brief_map = {}
    try:
        today_briefs = con.execute(f"""
            SELECT ticker, recommended_side, verdict, confidence_score, ingested_at
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY ticker
                    ORDER BY CAST(ingested_at AS TIMESTAMP) DESC
                ) AS rn
                FROM read_parquet('{briefs_glob}', union_by_name=true)
                WHERE CAST(ingested_at AS DATE) = CURRENT_DATE
            )
            WHERE rn = 1
        """).df()
        brief_map = {r['ticker']: r.to_dict() for _, r in today_briefs.iterrows()}
    except Exception as e:
        print(f"[Exit] Could not load today's briefs: {e}")

    con.close()

    # Evaluate each position
    signals = []
    for _, pos in ledger.iterrows():
        ticker = pos['ticker']
        live   = price_map.get(ticker)
        if live is None:
            print(f"[Exit] {ticker}: no live price -- skipping")
            continue

        try:
            signal = evaluate_position(
                pos.to_dict(),
                float(live['yes_bid_dollars']),
                float(live['yes_ask_dollars']),
                live.get('close_time'),
                brief_map.get(ticker),
            )
        except Exception as e:
            print(f"[Exit] {ticker}: evaluation error -- {e}")
            continue

        signal['ticker']        = ticker
        signal['side']          = pos['side']
        signal['qty']           = float(pos['qty'])
        signal['entry_price']   = float(pos['entry_price'])
        signal['current_price'] = signal['suggested_exit_price']
        signal['evaluated_at']  = datetime.now(timezone.utc).isoformat()
        signals.append(signal)

        marker = {
            'SELL_PROFIT':  '[$]',
            'SELL_LOSS':    '[X]',
            'SELL_FLIP':    '[!]',
            'SELL_TIMEOUT': '[T]',
            'HOLD':         '[=]',
        }.get(signal['action'], '[?]')
        print(f"  {marker} {ticker:40s} {signal['action']:12s} -- {signal['reason']}")

    if not signals:
        print("[Exit] No signals generated.")
        return

    os.makedirs(EXIT_SIGNALS_PATH, exist_ok=True)
    out_file = os.path.join(
        EXIT_SIGNALS_PATH,
        f"exits_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    )
    pd.DataFrame(signals).to_parquet(out_file, index=False)
    print(f"\n[Exit] Wrote {len(signals)} signal(s) to {out_file}")

    action_count = sum(1 for s in signals if s['action'] != 'HOLD')
    print(f"[Exit] {action_count}/{len(signals)} positions have exit recommendations")


if __name__ == "__main__":
    main()
