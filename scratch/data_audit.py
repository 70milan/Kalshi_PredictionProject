import duckdb
import os
import json
from datetime import datetime, timezone, timedelta

# Paths
BASE_DIR = "data"
BRONZE = os.path.join(BASE_DIR, "bronze")
SILVER = os.path.join(BASE_DIR, "silver")
GOLD = os.path.join(BASE_DIR, "gold")

# 2 Hour Staleness Threshold
STALE_THRESHOLD = timedelta(hours=2)
NOW = datetime.now(timezone.utc)

db = duckdb.connect()

def check_staleness(max_ingested_at):
    if not max_ingested_at:
        return "MISSING"
    try:
        # Handle decimal timestamps if they exist
        if isinstance(max_ingested_at, str) and "." in max_ingested_at:
            dt = datetime.fromisoformat(max_ingested_at.split(".")[0]).replace(tzinfo=timezone.utc)
        elif isinstance(max_ingested_at, datetime):
            dt = max_ingested_at.replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(max_ingested_at).replace(tzinfo=timezone.utc)
        
        if NOW - dt > STALE_THRESHOLD:
            return "STALE"
        return "HEALTHY"
    except Exception as e:
        return f"ERROR: {e}"

results = {
    "Bronze": {},
    "Silver": {},
    "Gold": {},
    "Consistency": {}
}

# --- BRONZE AUDIT ---
bronze_sources = {
    "Kalshi": "bronze/kalshi_markets/**/*.parquet",
    "GDELT_Events": "bronze/gdelt/gdelt_events/*.parquet",
    "GDELT_GKG": "bronze/gdelt/gdelt_gkg/*.parquet",
    "BBC": "bronze/bbc/*.parquet",
    "CNN": "bronze/cnn/*.parquet",
    "Fox": "bronze/foxnews/*.parquet",
    "NYPost": "bronze/nypost/*.parquet",
    "Hindu": "bronze/thehindu/*.parquet"
}

for name, path in bronze_sources.items():
    try:
        full_path = os.path.join(BASE_DIR, path).replace("\\", "/")
        # Volume & Timeliness
        q = f"SELECT COUNT(*) as count, MAX(CAST(ingested_at AS TIMESTAMP)) as latest FROM read_parquet('{full_path}', union_by_name=True)"
        row = db.execute(q).fetchone()
        count = row[0]
        latest = row[1]
        
        status = check_staleness(latest)
        
        results["Bronze"][name] = {
            "Rows": count,
            "MaxIngestedAt": str(latest),
            "Status": status
        }
        
        # Completeness Checks
        if name == "Kalshi":
            # Check yes_bid_dollars or closest match
            cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{full_path}', union_by_name=True) LIMIT 0").fetchall()
            col_names = [c[0] for c in cols]
            
            target_col = "yes_bid_dollars" if "yes_bid_dollars" in col_names else ("yes_bid" if "yes_bid" in col_names else None)
            if target_col:
                null_q = f"SELECT COUNT(*) FROM read_parquet('{full_path}', union_by_name=True) WHERE {target_col} IS NULL"
                null_count = db.execute(null_q).fetchone()[0]
                results["Bronze"][name][f"Nulls_{target_col}"] = null_count
            
            if "rules_primary" in col_names:
                null_rule_q = f"SELECT COUNT(*) FROM read_parquet('{full_path}', union_by_name=True) WHERE rules_primary IS NULL"
                null_rule_count = db.execute(null_rule_q).fetchone()[0]
                results["Bronze"][name]["Nulls_rules_primary"] = null_rule_count

        if name == "GDELT_GKG":
            null_gkg = db.execute(f"SELECT COUNT(*) FROM read_parquet('{full_path}', union_by_name=True) WHERE V2Persons IS NULL OR V2Persons = '' OR V2Themes IS NULL OR V2Themes = ''").fetchone()[0]
            results["Bronze"][name]["Missing_Semantic_Info"] = null_gkg

        if name in ["BBC", "CNN", "Fox", "NYPost", "Hindu"]:
            scrape_stats = db.execute(f"SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE scraped = False OR full_text IS NULL OR full_text = '') as failed FROM read_parquet('{full_path}', union_by_name=True)").fetchone()
            total = scrape_stats[0]
            failed = scrape_stats[1]
            fail_pct = (failed / total * 100) if total > 0 else 0
            results["Bronze"][name]["Scrape_Failure_Pct"] = round(fail_pct, 2)

    except Exception as e:
        results["Bronze"][name] = {"Error": str(e)}

# --- SILVER/GOLD AUDIT (Deduplication) ---
silver_gold_sources = {
    "Silver_Kalshi": ("silver/kalshi_markets_current/*.parquet", "ticker"),
    "Silver_GDELT_Events": ("silver/gdelt_events_current/*.parquet", "event_id"),
    "Silver_GDELT_GKG": ("silver/gdelt_gkg_current/*.parquet", "gkg_record_id"),
    "Gold_GDELT": ("gold/gdelt_summaries/*.parquet", "entity_name") 
}

for name, (path, key) in silver_gold_sources.items():
    try:
        full_path = os.path.join(BASE_DIR, path).replace("\\", "/")
        # Deduplication Hack
        dedup_q = f"""
            WITH base AS (
                SELECT *, 
                ROW_NUMBER() OVER(PARTITION BY {key} ORDER BY CAST(ingested_at AS TIMESTAMP) DESC) as rn
                FROM read_parquet('{full_path}', union_by_name=True)
            )
            SELECT COUNT(*) as count, MAX(CAST(ingested_at AS TIMESTAMP)) as latest
            FROM base
            WHERE rn = 1
        """
        row = db.execute(dedup_q).fetchone()
        results[name.split("_")[0]][name] = {
            "Rows_Deduplicated": row[0],
            "MaxIngestedAt": str(row[1]),
            "Status": check_staleness(row[1])
        }
        
        # Consistency Check (Duplicates after Qualify?)
        dup_check = f"""
            SELECT COUNT(*) FROM (
                SELECT {key}, COUNT(*) as c
                FROM (
                    SELECT {key}, ingested_at, ROW_NUMBER() OVER(PARTITION BY {key} ORDER BY CAST(ingested_at AS TIMESTAMP) DESC) as rn
                    FROM read_parquet('{full_path}', union_by_name=True)
                ) t
                WHERE rn = 1
                GROUP BY {key}
                HAVING c > 1
            )
        """
        duplicates = db.execute(dup_check).fetchone()[0]
        results["Consistency"][f"{name}_Duplicate_{key}"] = duplicates

    except Exception as e:
        results[name.split("_")[0]][name] = {"Error": str(e)}

# --- SPECIFIC CONSISTENCY CHECKS ---
# GDELT Bronze Duplicate ID Check
try:
    gdelt_pth = os.path.join(BASE_DIR, "bronze/gdelt/gdelt_events/*.parquet").replace("\\", "/")
    gdelt_dups = db.execute(f"SELECT COUNT(*) FROM (SELECT GLOBALEVENTID, COUNT(*) FROM read_parquet('{gdelt_pth}', union_by_name=True) GROUP BY GLOBALEVENTID HAVING COUNT(*) > 1)").fetchone()[0]
    results["Consistency"]["Bronze_GDELT_Duplicate_GLOBALEVENTID"] = gdelt_dups
except Exception as e:
    results["Consistency"]["Bronze_GDELT_Duplicate_GLOBALEVENTID_Error"] = str(e)

with open("scratch/audit_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("Audit complete. Results written to scratch/audit_results.json")
