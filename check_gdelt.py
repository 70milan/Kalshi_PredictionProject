import pandas as pd
import os

def check_gdelt_schema():
    path = "data/bronze/gdelt/gdelt_events/latest.parquet"
    if os.path.exists(path):
        df = pd.read_parquet(path)
        print(f"Columns in GDELT Events: {list(df.columns)}")
        print("\nSAMPLE ROW:")
        print(df.iloc[0].to_dict())
    else:
        print(f"GDELT Events latest.parquet not found at {path}")

if __name__ == "__main__":
    check_gdelt_schema()
