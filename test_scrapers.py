import feedparser
import trafilatura

feeds = {
    "Fox News Politics": "http://feeds.foxnews.com/foxnews/politics",
    "Breitbart": "http://feeds.feedburner.com/breitbart",
    "Washington Examiner": "https://www.washingtonexaminer.com/feed/",
    "New York Post (Right Leaning)": "https://nypost.com/politics/feed/"
}

for source, url in feeds.items():
    print(f"\\n--- Testing {source} ---")
    try:
        f = feedparser.parse(url)
        if hasattr(f, "entries") and len(f.entries) > 0:
            entry = f.entries[0]
            print(f"Latest Headline: {entry.title}")
            target_url = entry.link
            
            downloaded = trafilatura.fetch_url(target_url)
            if downloaded:
                text = trafilatura.extract(downloaded)
                if text:
                    print(f"SUCCESS: Extracted {len(text)} characters of full text.")
                    print(f"Sample: {text[:150]}...")
                else:
                    print("FAIL: Downloaded but trafilatura could not extract text.")
            else:
                print("FAIL: Could not fetch URL (Blocked or timeout).")
        else:
            print("FAIL: RSS Feed could not be parsed or has no entries.")
    except Exception as e:
        print(f"ERROR: {e}")
