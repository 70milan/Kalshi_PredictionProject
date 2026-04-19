import os
import pandas as pd

def check():
    path = os.path.join('data', 'bronze', 'thehindu')
    urls = set()
    total_rows = 0
    
    if not os.path.exists(path):
        print(f"Path not found: {path}")
        return

    files = [f for f in os.listdir(path) if f.endswith('.parquet')]
    print(f"Analyzing {len(files)} files in {path}...")
    
    for f in files:
        try:
            df = pd.read_parquet(os.path.join(path, f))
            total_rows += len(df)
            if 'link' in df.columns:
                urls.update(df['link'].astype(str).tolist())
        except Exception as e:
            pass
            
    print("-" * 40)
    print(f"Total Rows in Bronze  : {total_rows}")
    print(f"Unique URLs in Bronze : {len(urls)}")
    print("-" * 40)

if __name__ == "__main__":
    check()
