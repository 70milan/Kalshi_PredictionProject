---
name: kalshi-api-debug
description: >
  Use this skill whenever working with the Kalshi prediction markets API — including
  ingestion scripts, authentication issues, pagination bugs, filtering problems, field
  discovery, or any situation where Kalshi data is not returning what you expect.
  Trigger on: "kalshi not returning", "kalshi pagination", "kalshi filter", "kalshi category",
  "kalshi fields", "kalshi auth", "kalshi RSA", "kalshi markets empty", "kalshi infinite loop",
  "kalshi series", "kalshi parquet", or any debugging of a Kalshi ingestion pipeline.
  This skill captures hard-won lessons from debugging the Kalshi API in production —
  use it proactively before writing any new Kalshi code, not just when something breaks.
---

# Kalshi API Debugging & Ingestion Skill

Captures lessons learned debugging the Kalshi Elections API for the PredictIQ project.
Use this before writing any new Kalshi ingestion code to avoid known pitfalls.

---

## Critical API Facts (Verified in Production)

### 1. Two Domains — Know Which One You're On
| Domain | Purpose |
|---|---|
| `api.elections.kalshi.com` | Elections + political markets |
| `trading.kalshi.com` | All other markets (sports, crypto, etc.) |

Even on `api.elections.kalshi.com`, **sports parlays and multi-event markets can appear** in results. Do not assume the domain filters for you.

### 2. Category Field Does NOT Exist on Markets
Individual market objects returned from `/trade-api/v2/markets` do NOT have a `category` field.
Passing `category` as a query param is silently ignored by the server.

**Wrong approach:**
```python
params = {"limit": 200, "status": "open", "category": "Politics"}  # ❌ ignored
```

**Correct approach:** Use the `/series` endpoint first (see Step-by-Step below).

### 3. The `category` Field Lives on Series, Not Markets
Hit `/trade-api/v2/series` to get series objects — these DO have a `category` field.
Filter series by category, collect their `ticker` values, then fetch markets using `series_ticker`.

### 4. Known Category Values (Confirmed)
```
Politics          Economics         Sports
Entertainment     Financials        Climate and Weather
Science and Technology
```

### 5. `limit=1000` Does NOT Work
Kalshi silently caps responses at 200 per page regardless of what you pass.
Always use `limit: 200` — passing anything higher causes confusing pagination behavior.

### 6. Price Fields Use `_dollars` Suffix
```python
# Wrong                    # Correct
m.get("yes_bid")          m.get("yes_bid_dollars")
m.get("yes_ask")          m.get("yes_ask_dollars")
m.get("no_bid")           m.get("no_bid_dollars")
m.get("no_ask")           m.get("no_ask_dollars")
```

### 7. Kalshi Has 20,000+ Markets — Always Cap Pagination
Without a safety cap, fetching all markets will run for 5+ minutes.
Always set `MAX_PAGES` or `MAX_PAGES_PER_SERIES` as a hard brake.

### 8. Sports Parlays Have Recognizable Ticker Prefixes
Use ticker-based exclusion as your first filter — it's faster and more reliable than text matching.
```python
TICKER_EXCLUDE = [
    "KXMVESPORTS",   # Multi-event sports parlays
    "KXMVECROSS",    # Cross-category sports parlays
    "KXMVECB",       # College basketball parlays
    "KXMARMAD",      # March Madness
    "KXLOL",         # League of Legends esports
    "KXUCL",         # UEFA Champions League
    "KXCS2",         # Counter-Strike 2
]
```

### 9. Nested Fields Break Parquet
Some Kalshi market fields contain nested dicts/lists (`custom_strike`, `mve_selected_legs`, `price_ranges`).
Always flatten before writing to Parquet:
```python
def flatten_market(m: dict) -> dict:
    return {
        k: (str(v) if isinstance(v, (dict, list)) else v)
        for k, v in m.items()
    }
```

---

## RSA Authentication Pattern

Kalshi uses RSA-SHA256 signatures. Build fresh headers on every request — tokens are timestamp-bound.

```python
def build_headers(method: str, path: str) -> dict:
    timestamp   = str(int(time.time() * 1000))
    message     = timestamp + method + path
    private_key = serialization.load_pem_private_key(
        API_SECRET.encode(), password=None
    )
    signature = private_key.sign(
        message.encode(), padding.PKCS1v15(), hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY":       API_KEY,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "Content-Type":            "application/json",
    }
```

**Auth dependencies:**
```
cryptography
python-dotenv
```

**.env format:**
```
KALSHI_API_KEY=your_key_here
KALSHI_API_SECRET=-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----
```

Note: `API_SECRET` must have literal `\n` replaced:
```python
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")
```

---

## Step-by-Step: Correct Political Market Ingestion

### Step 1 — Fetch Series (Get Real Category Data)
```python
SERIES_ENDPOINT = "/trade-api/v2/series"

data   = requests.get(f"{BASE_URL}{SERIES_ENDPOINT}", headers=build_headers("GET", SERIES_ENDPOINT)).json()
series = data.get("series", [])

# Filter to political series only
political_tickers = [
    s["ticker"] for s in series
    if s.get("category", "") == "Politics"
]
```

### Step 2 — Fetch Markets by Series Ticker
```python
MARKETS_ENDPOINT = "/trade-api/v2/markets"
MAX_PAGES        = 10   # hard brake per series

for series_ticker in political_tickers:
    cursor     = None
    page_count = 0

    while True:
        page_count += 1
        if page_count > MAX_PAGES:
            break

        params = {
            "limit":         200,
            "status":        "open",
            "series_ticker": series_ticker,
        }
        if cursor:
            params["cursor"] = cursor

        data   = requests.get(f"{BASE_URL}{MARKETS_ENDPOINT}", headers=build_headers("GET", MARKETS_ENDPOINT), params=params).json()
        batch  = data.get("markets", [])

        if not batch:
            break

        all_markets.extend(batch)

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.2)
```

### Step 3 — Validate Fields on First Run
Before building any pipeline, always print raw fields from the first market returned:
```python
if batch:
    print("RAW FIELDS:")
    for k, v in batch[0].items():
        print(f"  {k:<40} {str(v)[:80]}")
```
This is mandatory on any new API version or environment. Never assume field names.

### Step 4 — Flatten and Save
```python
flat = [flatten_market(m) for m in all_markets]
df   = pd.DataFrame(flat)

# Always write two files:
# 1. latest.parquet    — overwritten each run, easy for DBeaver/DuckDB ad-hoc queries
# 2. markets_TS.parquet — timestamped, never overwritten, builds your time-series history

conn = duckdb.connect()
conn.execute(f"COPY (SELECT * FROM df) TO 'data/bronze/kalshi_markets/latest.parquet' (FORMAT 'PARQUET')")
conn.execute(f"COPY (SELECT * FROM df) TO 'data/bronze/kalshi_markets/markets_{ts}.parquet' (FORMAT 'PARQUET')")
```

---

## Querying Your Bronze Data

### DuckDB — ad-hoc queries (no Spark needed)
```python
import duckdb
conn = duckdb.connect()

# Latest snapshot
conn.execute("SELECT ticker, title, yes_bid_dollars FROM read_parquet('data/bronze/kalshi_markets/latest.parquet') LIMIT 10").fetchdf()

# Full history (all timestamped files)
conn.execute("SELECT * FROM read_parquet('data/bronze/kalshi_markets/*.parquet', union_by_name=true)").fetchdf()
```

### DBeaver Setup
1. Create DuckDB connection → point to a local `.db` file
2. Run once to register persistent views:
```sql
CREATE OR REPLACE VIEW kalshi_latest AS
SELECT * FROM read_parquet('C:/your/path/data/bronze/kalshi_markets/latest.parquet');

CREATE OR REPLACE VIEW kalshi_history AS
SELECT * FROM read_parquet('C:/your/path/data/bronze/kalshi_markets/*.parquet', union_by_name=true);
```
Views auto-refresh — no reimporting needed.

---

## Debugging Checklist

When Kalshi ingestion behaves unexpectedly, check these in order:

- [ ] **Getting 0 results?** → Print raw category breakdown from `/series` first. Confirm exact category string.
- [ ] **Infinite pagination?** → `limit` is silently capped at 200. Add `MAX_PAGES` brake. Check if cursor is always being returned.
- [ ] **Sports in political results?** → Category filter is not on market objects. Switch to series-based filtering. Add ticker exclusions for `KXMVE*`, `KXMARMAD*`.
- [ ] **Parquet write crashing?** → Nested dict/list fields. Run `flatten_market()` before writing.
- [ ] **Wrong price values?** → Use `yes_bid_dollars` not `yes_bid`. All price fields have `_dollars` suffix.
- [ ] **Auth 401 errors?** → RSA message must be exactly `timestamp + METHOD + path`. No query params in the signed message.
- [ ] **Fields returning None?** → You're checking the wrong field name. Always run raw field print on `batch[0]` first.
- [ ] **Schema mismatch across Parquet files?** → Use `union_by_name=true` in DuckDB reads.

---

## Storage Architecture

```
data/
  bronze/
    kalshi_markets/
      latest.parquet              ← overwritten every run (current snapshot)
      markets_20260317_143022.parquet  ← timestamped history (never overwritten)
      markets_20260317_150022.parquet
      ...
```

**Why two files?**
- `latest.parquet` → fast ad-hoc queries, DBeaver convenience
- `markets_TS.parquet` → time-series odds history needed for mispricing detection

The `kalshi_history` DuckDB view using `*.parquet` wildcard picks up all timestamped files automatically. Every poll adds a new file, query reflects it immediately.

---

## Known Kalshi API Quirks Summary

| Quirk | Impact | Fix |
|---|---|---|
| No `category` on market objects | Filter by category doesn't work | Use `/series` endpoint |
| `limit>200` silently capped | Confusing pagination | Always use `limit=200` |
| Sports mixed into Politics | Garbage in filtered results | Ticker exclusions + series filter |
| Nested fields in some markets | Parquet write crash | `flatten_market()` helper |
| Price fields have `_dollars` suffix | Wrong/null price data | Use correct field names |
| Timestamp in RSA signature | Stale tokens rejected | Build fresh headers per request |
| `\n` literal in .env secrets | PEM key fails to parse | `.replace("\\n", "\n")` on load |
