import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Test intelligence_briefs
result = con.execute("""
    SELECT ticker, confidence_score, ingested_at
    FROM read_parquet('data/gold/intelligence_briefs/*.parquet')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY ingested_at DESC) = 1
    LIMIT 5
""").df()
print("intelligence_briefs OK:", len(result), "rows")

# Test mispricing_scores via delta_scan
result2 = con.execute("""
    SELECT ticker, mispricing_score
    FROM delta_scan('data/gold/mispricing_scores')
    LIMIT 5
""").df()
print("mispricing_scores OK:", len(result2), "rows")
print(result2)
con.close()
