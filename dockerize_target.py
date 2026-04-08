import os
import shutil
import glob

PROJECT_ROOT = r"c:\Data Engineering\codeprep\predection_project"
NATIVE_DIR = os.path.join(PROJECT_ROOT, "native_ingestion")
DOCKER_DIR = os.path.join(PROJECT_ROOT, "ingestion")

# ─────────────────────────────────────────────
# 1. PULL OVER NEW NEWS SCRIPTS
# ─────────────────────────────────────────────
scripts_to_port = {
    "flat_nypost_ingest.py": "nypost_ingest.py",
    "flat_hindu_ingest.py": "hindu_ingest.py",
    "flat_fox_ingest.py": "fox_ingest.py",
    "flat_cnn_ingest.py": "cnn_ingest.py"
}

for native, docker in scripts_to_port.items():
    src = os.path.join(NATIVE_DIR, native)
    dst = os.path.join(DOCKER_DIR, docker)
    if os.path.exists(src):
        shutil.copy(src, dst)
        print(f"Ported: {native} -> {docker}")

# ─────────────────────────────────────────────
# 2. INJECT DOCKER PARADIGMS INTO ALL SCRIPTS
# ─────────────────────────────────────────────
ingestion_files = glob.glob(os.path.join(DOCKER_DIR, "*.py"))

for f in ingestion_files:
    fname = os.path.basename(f)
    # We DO NOT loop historical ingestion, or test scripts
    if fname in ["test_kalshi_auth.py", "TDB_flat_kalshi_candlesticks.py", "kalshi_markets_historical.py"]:
        print(f"Skipping logic update for {fname}")
        continue

    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # 1. Inject missing 'time' import if needed
    if "import time" not in content and "time." not in content[:500]:
        content = content.replace("import os\n", "import os\nimport time\n")
        
    # 2. Hardcode Project Root for volume mapping
    target_path_str = 'PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))'
    if target_path_str in content:
        content = content.replace(target_path_str, 'PROJECT_ROOT = "/app"')
        
    # 3. Build continuous Polling Execution loop
    if "while True:" not in content and "__main__" in content:
        sleep_time = 86400 if "daily_settlement" in fname else 900
        
        old_main_1 = 'if __name__ == "__main__":\n    main()'
        old_main_2 = 'if __name__ == "__main__":\n    main()\n'
        
        new_main = f'''if __name__ == "__main__":
    print(f"Initializing Docker Polling Service ({{int({sleep_time}/60)}}-min intervals)...")
    while True:
        try:
            main()
        except Exception as e:
            print(f"CRITICAL ERROR in ingestion loop: {{e}}")
        
        print("Sleeping for {sleep_time} seconds before poll...")
        time.sleep({sleep_time})
'''
        if old_main_2 in content:
            content = content.replace(old_main_2, new_main)
        elif old_main_1 in content:
            content = content.replace(old_main_1, new_main)

    with open(f, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Dockerized: {fname} (Loop logic + /app root activated)")
