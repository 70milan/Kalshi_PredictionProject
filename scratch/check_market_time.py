import duckdb
import os

def check():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    path = os.path.join('data', 'silver', 'kalshi_markets_history')
    ticker = 'KXTRUMPMEET-26MAR-CSCH'
    
    query = f"SELECT ticker, ingested_at FROM delta_scan('{path}') WHERE ticker = '{ticker}' ORDER BY ingested_at DESC LIMIT 5"
    try:
        df = con.execute(query).df()
        print(df.to_string())
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
