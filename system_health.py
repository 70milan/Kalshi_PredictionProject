import os
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")

WARNING_THRESHOLD_MINS = 30
CRITICAL_THRESHOLD_MINS = 120

def get_latest_file_time(path):
    """Recursively finds the newest file in a directory and returns its modification time (Unix epoch)."""
    if not os.path.exists(path):
        return 0
        
    latest_time = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            filepath = os.path.join(root, file)
            file_time = os.path.getmtime(filepath)
            if file_time > latest_time:
                latest_time = file_time
                
    # If the directory is completely empty, it shouldn't be counted as "fresh"
    if latest_time == 0:
        return 0
        
    return latest_time

def format_time_ago(epoch_time):
    if epoch_time == 0:
        return "Not found    ", "[MISS]"
        
    now = time.time()
    diff_seconds = now - epoch_time
    diff_mins = int(diff_seconds / 60)
    
    if diff_mins < WARNING_THRESHOLD_MINS:
        status = "[ OK ]"
    elif diff_mins < CRITICAL_THRESHOLD_MINS:
        status = "[WARN]"
    else:
        status = "[DEAD]"
        
    if diff_mins == 0:
        time_str = "Just now     "
    elif diff_mins < 60:
        time_str = f"{diff_mins} mins ago   "
    elif diff_mins < 1440:
        hours = int(diff_mins / 60)
        time_str = f"{hours} hours ago  "
    else:
        days = int(diff_mins / 1440)
        time_str = f"{days} days ago   "
        
    # Standardize length to exactly 13 characters for alignment
    time_str = time_str.ljust(13)
    return time_str, status

def check_component(name, rel_path, indent=""):
    full_path = os.path.join(DATA_DIR, rel_path)
    latest_time = get_latest_file_time(full_path)
    time_str, status = format_time_ago(latest_time)
    print(f"{indent}{name:<25} | {time_str} | {status}")

def main():
    print("\n" + "="*60)
    print(" PREDICTIQ SYSTEM HEALTH DASHBOARD")
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f" Checked at: {current_time}")
    print("="*60)
    
    print("\n[BRONZE LAYER] (Raw Data Ingestion)")
    print("-" * 60)
    check_component("Kalshi Active Markets", "bronze/kalshi_markets/open", "  ")
    check_component("Kalshi Daily Settlement", "bronze/kalshi_markets/settled", "  ")
    check_component("Kalshi Closed Markets", "bronze/kalshi_markets/closed", "  ")
    print("")
    check_component("News - BBC", "bronze/bbc", "  ")
    check_component("News - CNN", "bronze/cnn", "  ")
    check_component("News - Fox", "bronze/foxnews", "  ")
    check_component("News - NYT", "bronze/nyt", "  ")
    print("")
    check_component("GDELT - Global Events", "bronze/gdelt/gdelt_events", "  ")
    check_component("GDELT - Global Graph", "bronze/gdelt/gdelt_gkg", "  ")
    
    print("\n[SILVER LAYER] (Cleaned & Standardized)")
    print("-" * 60)
    check_component("Kalshi Delta Table", "silver/kalshi_markets_history", "  ")
    check_component("News Delta Table", "silver/news_articles_enriched", "  ")
    check_component("GDELT Events Delta Table", "silver/gdelt_events_history", "  ")
    check_component("GDELT Graph Delta Table", "silver/gdelt_gkg_history", "  ")
    
    print("\n[GOLD LAYER] (Aggregated Features for Engine)")
    print("-" * 60)
    check_component("Kalshi Odds Summary", "gold/market_summaries", "  ")
    check_component("News Bias Summary", "gold/news_summaries", "  ")
    check_component("GDELT Trend Summary", "gold/gdelt_summaries", "  ")
    
    print("\n" + "="*60)
    print("Legend:")
    print(f"  [ OK ] : Updated within {WARNING_THRESHOLD_MINS} mins")
    print(f"  [WARN] : Updated within {CRITICAL_THRESHOLD_MINS} mins")
    print(f"  [DEAD] : No updates for >{CRITICAL_THRESHOLD_MINS} mins (or missing)")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
