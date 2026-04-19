import os
import pandas as pd

def audit():
    sources = ['bbc', 'cnn', 'foxnews', 'nypost', 'nyt', 'thehindu']
    
    print("="*60)
    print(" PREDICTIQ NEWS SOURCE AUDIT")
    print("="*60)
    
    # 1. Audit Bronze
    print("\n[Bronze Inventory]")
    bronze_total = 0
    for s in sources:
        path = os.path.join('data', 'bronze', s)
        count = 0
        if os.path.exists(path):
            files = [f for f in os.listdir(path) if f.endswith('.parquet')]
            for f in files:
                try:
                    df = pd.read_parquet(os.path.join(path, f))
                    count += len(df)
                except:
                    pass
        print(f"  > {s.upper():<10}: {count:>6} articles")
        bronze_total += count
    print(f"  TOTAL BRONZE: {bronze_total}")

    # 2. Audit Silver
    print("\n[Silver Attribution]")
    silver_path = os.path.join('data', 'silver', 'news_articles_enriched')
    if os.path.exists(silver_path):
        try:
            df = pd.read_parquet(silver_path)
            if 'source' in df.columns:
                counts = df['source'].value_counts()
                for src, count in counts.items():
                    print(f"  > {str(src).upper():<10}: {count:>6} articles")
            print("-" * 20)
            print(f"  TOTAL SILVER: {len(df)}")
        except Exception as e:
            print(f"  [Error] Could not read Silver: {e}")
    else:
        print("  [Error] Silver path does not exist.")
    print("="*60 + "\n")

if __name__ == "__main__":
    audit()
