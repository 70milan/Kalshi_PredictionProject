"""
DuckDB-based Sentiment Backtesting Engine for PredictIQ V2

Backtests sentiment-only signals (no LLM) against historical Kalshi markets.
Uses news (VADER) + GDELT aggregated by 15m windows to generate trade signals.
"""

import os
import sys
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"

# Signal thresholds
SENTIMENT_LONG_THRESHOLD = 0.3    # TODO(human): Adjust — bullish sentiment score to enter YES
SENTIMENT_SHORT_THRESHOLD = -0.3  # TODO(human): Adjust — bearish sentiment score to enter NO
CONFIDENCE_MIN = 0.5              # TODO(human): Minimum aggregate signal strength (0-1)

# Trade constraints
LOOKBACK_WINDOW_HOURS = 24        # Aggregate sentiment over this window
LOOKFORWARD_WINDOW_HOURS = 4      # How long to hold position (until exit signal)
MIN_VOLUME_USD = 100              # Ignore illiquid markets
KELLY_FRACTION = 0.15             # Kelly criterion sizing

# ─────────────────────────────────────────────
# DUCK DB SETUP
# ─────────────────────────────────────────────

def connect_db():
    """Initialize DuckDB with Delta + Parquet readers."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL delta; LOAD delta;")
    conn.execute("INSTALL json; LOAD json;")
    return conn

def load_silver_news(conn):
    """Load news sentiment from Silver layer."""
    news_path = str(DATA_DIR / "silver" / "news_articles_enriched")
    query = f"""
    SELECT
        CAST(published_at AS TIMESTAMP) as published_at,
        CAST(ingested_at AS TIMESTAMP) as ingested_at,
        feed_key as source,
        title,
        sentiment_score,
        sentiment_label
    FROM delta_scan('{news_path}')
    WHERE sentiment_score IS NOT NULL
    """
    return conn.execute(query).df()

def load_silver_gdelt_events(conn):
    """Load GDELT events sentiment (if available)."""
    gdelt_path = str(DATA_DIR / "silver" / "gdelt_events_current")
    try:
        query = f"""
        SELECT
            CAST(date AS TIMESTAMP) as event_date,
            CAST(ingested_at AS TIMESTAMP) as ingested_at,
            actor1_name,
            actor2_name,
            goldstein_scale,
            tone
        FROM delta_scan('{gdelt_path}')
        WHERE tone IS NOT NULL
        LIMIT 100000
        """
        return conn.execute(query).df()
    except Exception as e:
        print(f"[Backtest] GDELT Events not available: {e}")
        return None

def load_silver_gdelt_gkg(conn):
    """Load GDELT GKG sentiment (high-signal theme aggregates)."""
    gkg_path = str(DATA_DIR / "silver" / "gdelt_gkg_current")
    try:
        query = f"""
        SELECT
            CAST(date AS TIMESTAMP) as gkg_date,
            CAST(ingested_at AS TIMESTAMP) as ingested_at,
            themes,
            tone
        FROM delta_scan('{gkg_path}')
        WHERE tone IS NOT NULL
        LIMIT 100000
        """
        return conn.execute(query).df()
    except Exception as e:
        print(f"[Backtest] GDELT GKG not available: {e}")
        return None

def load_kalshi_history(conn):
    """Load market snapshots from Silver Kalshi history."""
    kalshi_path = str(DATA_DIR / "silver" / "kalshi_markets_history")
    query = f"""
    SELECT
        CAST(ingested_at AS TIMESTAMP) as ts,
        ticker,
        series_ticker,
        title,
        status,
        CAST(yes_bid AS DOUBLE) as yes_bid,
        CAST(yes_ask AS DOUBLE) as yes_ask,
        CAST(volume AS DOUBLE) as volume,
        CAST(open_interest_fp AS DOUBLE) as open_interest_fp,
        resolution_source as resolved_to,
        CAST(settlement_date AS TIMESTAMP) as settlement_date
    FROM delta_scan('{kalshi_path}')
    WHERE status IN ('open', 'active')
    """
    return conn.execute(query).df()

# ─────────────────────────────────────────────
# SENTIMENT AGGREGATION
# ─────────────────────────────────────────────

def aggregate_sentiment_15m(news_df, gdelt_df=None, gkg_df=None):
    """
    Aggregate sentiment into 15-minute windows.
    Returns DataFrame with (window_time, avg_sentiment, signal_count, confidence).
    """
    # Sentiment windows from news
    news_df = news_df.copy()
    news_df['window'] = news_df['ingested_at'].dt.floor('15min')
    news_agg = news_df.groupby('window').agg({
        'sentiment_score': ['mean', 'std', 'count'],
        'feed_key': 'nunique'
    }).reset_index()
    news_agg.columns = ['window_time', 'sentiment_mean', 'sentiment_std', 'article_count', 'source_count']

    # GDELT adds volume signal
    if gdelt_df is not None and len(gdelt_df) > 0:
        gdelt_df = gdelt_df.copy()
        gdelt_df['window'] = gdelt_df['ingested_at'].dt.floor('15min')
        gdelt_agg = gdelt_df.groupby('window').agg({
            'tone': 'mean',
            'goldstein_scale': 'mean'
        }).reset_index()
        gdelt_agg.columns = ['window_time', 'gdelt_tone', 'goldstein']

        # Join news + GDELT
        news_agg = news_agg.merge(gdelt_agg, on='window_time', how='left')
        # Blend: 70% news VADER, 30% GDELT tone
        news_agg['blended_sentiment'] = (0.7 * news_agg['sentiment_mean'] +
                                         0.3 * (news_agg['gdelt_tone'] / 10.0))
    else:
        news_agg['blended_sentiment'] = news_agg['sentiment_mean']

    # Confidence: how many signals agree + how many sources
    news_agg['confidence'] = (news_agg['source_count'] / 10.0).clip(0, 1)  # Scale: 1-10 sources → 0-1

    return news_agg[['window_time', 'sentiment_mean', 'blended_sentiment', 'confidence', 'article_count']]

# ─────────────────────────────────────────────
# SIGNAL GENERATION & TRADE SIMULATION
# ─────────────────────────────────────────────

def generate_trade_signals(sentiment_agg, kalshi_df):
    """
    Match sentiment windows to Kalshi market snapshots.
    For each signal, look forward LOOKFORWARD_WINDOW_HOURS to find exit prices.
    """
    trades = []

    for _, signal_row in sentiment_agg.iterrows():
        signal_time = signal_row['window_time']
        sentiment = signal_row['blended_sentiment']
        confidence = signal_row['confidence']

        # Find markets within 5 min of sentiment window
        market_entry = kalshi_df[
            (kalshi_df['ts'] >= signal_time) &
            (kalshi_df['ts'] <= signal_time + timedelta(minutes=5))
        ]

        if len(market_entry) == 0:
            continue

        # TODO(human): Implement trade logic here
        # For each market, decide: LONG (buy YES), SHORT (buy NO), or SKIP
        # Consider signal strength, market liquidity, theme relevance

    return pd.DataFrame(trades)

def calculate_pnl(entry_price, exit_price, side, position_size):
    """Calculate PnL for YES/NO position."""
    if side == 'YES':
        return position_size * (exit_price - entry_price)
    else:
        return position_size * ((1 - exit_price) - (1 - entry_price))

# ─────────────────────────────────────────────
# BACKTEST METRICS
# ─────────────────────────────────────────────

def calculate_metrics(trades_df):
    """Compute backtest summary: win rate, Sharpe, max drawdown, profit factor."""
    if len(trades_df) == 0:
        print("[Backtest] No trades generated.")
        return {}

    pnl = trades_df['pnl'].values

    winning_trades = (pnl > 0).sum()
    losing_trades = (pnl < 0).sum()
    total_trades = len(pnl)

    win_rate = winning_trades / total_trades if total_trades > 0 else 0

    # Profit factor = sum(wins) / sum(losses)
    total_wins = pnl[pnl > 0].sum()
    total_losses = abs(pnl[pnl < 0].sum())
    profit_factor = total_wins / total_losses if total_losses > 0 else 0

    # Sharpe ratio (assuming daily rebalance)
    daily_pnl = trades_df.groupby(trades_df['entry_time'].dt.date)['pnl'].sum()
    sharpe = (daily_pnl.mean() / daily_pnl.std()) * np.sqrt(252) if daily_pnl.std() > 0 else 0

    # Max drawdown
    cumulative = pnl.cumsum()
    running_max = np.maximum.accumulate(cumulative)
    drawdown = (cumulative - running_max) / running_max
    max_drawdown = drawdown.min()

    return {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': win_rate,
        'profit_factor': profit_factor,
        'total_pnl': pnl.sum(),
        'avg_win': pnl[pnl > 0].mean() if winning_trades > 0 else 0,
        'avg_loss': pnl[pnl < 0].mean() if losing_trades > 0 else 0,
        'sharpe_ratio': sharpe,
        'max_drawdown': max_drawdown,
    }

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("[Backtest] Initializing DuckDB sentiment backtester...")
    conn = connect_db()

    print("[Backtest] Loading Silver news sentiment...")
    news_df = load_silver_news(conn)
    print(f"[Backtest] Loaded {len(news_df)} news articles")

    print("[Backtest] Loading GDELT data...")
    gdelt_events = load_silver_gdelt_events(conn)
    gkg = load_silver_gdelt_gkg(conn)

    print("[Backtest] Loading Kalshi history...")
    kalshi_df = load_kalshi_history(conn)
    print(f"[Backtest] Loaded {len(kalshi_df)} market snapshots")

    print("[Backtest] Aggregating sentiment into 15m windows...")
    sentiment_agg = aggregate_sentiment_15m(news_df, gdelt_events, gkg)
    print(f"[Backtest] Generated {len(sentiment_agg)} sentiment signals")

    print("[Backtest] Generating trade signals...")
    trades = generate_trade_signals(sentiment_agg, kalshi_df)
    print(f"[Backtest] Generated {len(trades)} trades")

    print("[Backtest] Calculating metrics...")
    metrics = calculate_metrics(trades)

    print("\n" + "="*60)
    print("BACKTEST RESULTS (Sentiment Only, V2)")
    print("="*60)
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"{k:.<40} {v:>15.4f}")
        else:
            print(f"{k:.<40} {v:>15}")
    print("="*60)

    # Save trades to CSV for further analysis
    if len(trades) > 0:
        trades.to_csv(DATA_DIR / "backtest_trades.csv", index=False)
        print(f"\n[Backtest] Trades saved to backtest_trades.csv")

if __name__ == "__main__":
    main()
