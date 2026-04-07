import pandas as pd
import os
import glob

def check_source_schemas():
    paths = {
        "bbc": "data/bronze/bbc/latest.parquet",
        "cnn": "data/bronze/cnn/latest.parquet",
        "fox": "data/bronze/foxnews/latest.parquet",
        "nypost": "data/bronze/nypost/latest.parquet"
    }
    
    for source, path in paths.items():
        if os.path.exists(path):
            df = pd.read_parquet(path)
            print(f"--- {source} ---")
            print(f"Columns: {list(df.columns)}")
            if 'source' in df.columns:
                print(f"Unique sources: {df['source'].unique()}")
            else:
                print("MISSING source column!")
            print(f"Row count: {len(df)}")
            print()
        else:
            print(f"--- {source} MISSING ({path}) ---")

if __name__ == "__main__":
    check_source_schemas()
