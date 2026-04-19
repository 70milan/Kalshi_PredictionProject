import shutil
import os
import time

def wipe():
    path = os.path.join('data', 'silver', 'news_articles_enriched')
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return

    print(f"Attempting to wipe: {path}")
    
    # Try multiple times with a small delay for stubborn locks
    for i in range(3):
        try:
            shutil.rmtree(path)
            print("Successfully wiped Silver News layer.")
            return
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}")
            if i < 2:
                print("Retrying in 2 seconds...")
                time.sleep(2)
            else:
                print("Could not delete via standard rmtree. Manual intervention may be required if locks persist.")

if __name__ == "__main__":
    wipe()
