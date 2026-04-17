import pandas as pd

dates = [
    "Tue, 31 Mar 2026 19:20:13 -0400",
    "2026-04-15 05:08:56",
    "Wed, 15 Apr 2026 10:39:09 GMT"
]

for d in dates:
    try:
        res = pd.to_datetime(d, errors="coerce", utc=True)
        print(f"Input: {d} -> Result: {res}")
    except Exception as e:
        print(f"Input: {d} -> Error: {e}")
