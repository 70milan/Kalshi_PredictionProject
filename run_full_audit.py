import os
import glob
import duckdb
import subprocess
from datetime import datetime
import ast

ROOT = r"c:\Data Engineering\codeprep\predection_project"
OUT_FILE = os.path.join(ROOT, "audit_output2.txt")

with open(OUT_FILE, "w", encoding="utf-8") as out:
    def printf(msg):
        out.write(str(msg) + "\n")

    # 1. Directory Tree
    def print_tree(dir_path, prefix=""):
        try:
            items = os.listdir(dir_path)
            for item in items:
                if item in [".git", ".venv", ".pytest_cache", "__pycache__", ".vscode", "tmp", "audit_output.txt", "audit_output2.txt"]:
                    continue
                path = os.path.join(dir_path, item)
                if os.path.isdir(path):
                    printf(f"{prefix}+-- {item}/")
                    # don't recurse into data too deep
                    if dir_path != ROOT or item not in ['data']:
                        print_tree(path, prefix + "    ")
                else:
                    try:
                        size_kb = os.path.getsize(path) / 1024
                        printf(f"{prefix}+-- {item} ({size_kb:.2f} KB)")
                    except Exception:
                        pass
        except Exception:
            pass

    printf("=== 1. Directory Tree ===")
    print_tree(ROOT)

    # 2. Scripts Inventory
    printf("\n=== 2. Scripts Inventory ===")
    for root, _, files in os.walk(ROOT):
        if any(ignore in os.path.relpath(root, ROOT) for ignore in [".git", ".venv", "__pycache__", "tmp"]):
            continue
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                try:
                    mtime = os.path.getmtime(path)
                    dt = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                    printf(f"File: {os.path.relpath(path, ROOT)} | Modified: {dt}")
                except Exception:
                    pass

    def run_duckdb_query(query):
        try:
            conn = duckdb.connect()
            df = conn.execute(query).df()
            return df.to_string(index=False)
        except Exception as e:
            return f"Error: {str(e)}"

    # 3. Bronze Layer
    printf("\n=== 3. Bronze Layer ===")
    bronze_dir = os.path.join(ROOT, "data", "bronze")
    if os.path.exists(bronze_dir):
        # find all directories with parquet files
        for root, dirs, files in os.walk(bronze_dir):
            parquets = [f for f in files if f.endswith(".parquet")]
            if parquets:
                sizes = [os.path.getsize(os.path.join(root, f)) for f in parquets]
                total_kb = sum(sizes) / 1024
                rel_val = os.path.relpath(root, bronze_dir).replace('\\', '/')
                printf(f"\nSource: {rel_val} | Files: {len(parquets)} | Total: {total_kb:.2f} KB")
                
                ftq = os.path.join(root, parquets[0]).replace('\\', '/')
                spq = root.replace('\\', '/')
                printf("Schema:"); printf(run_duckdb_query(f"DESCRIBE SELECT * FROM '{ftq}';"))
                printf("Rows:"); printf(run_duckdb_query(f"SELECT count(*) as count FROM '{spq}/*.parquet';"))
                printf("Sample:"); printf(run_duckdb_query(f"SELECT * FROM '{ftq}' LIMIT 3;"))
                # ingested_at might not exist in all
                printf("MIN/MAX ingested_at:"); printf(run_duckdb_query(f"SELECT MIN(ingested_at) as min_dt, MAX(ingested_at) as max_dt FROM '{spq}/*.parquet';"))

    # 4. Reference Files
    printf("\n=== 4. Reference Files ===")
    ref_paths = [os.path.join(ROOT, "reference")]
    for rdir in ref_paths:
        if os.path.exists(rdir):
            for f in os.listdir(rdir):
                path = os.path.join(rdir, f)
                if os.path.isfile(path):
                    printf(f"\nFile: {os.path.relpath(path, ROOT)}")
                    if f.endswith('.csv'):
                        pf = path.replace('\\', '/')
                        printf("Rows:"); printf(run_duckdb_query(f"SELECT count(*) as count FROM read_csv_auto('{pf}');"))

    # 6. Docker Status
    printf("\n=== 6. Docker Status ===")
    for dfile in ["Dockerfile", "docker-compose.yml"]:
        dfpath = os.path.join(ROOT, dfile)
        if os.path.exists(dfpath):
            printf(f"\n--- {dfile} ---")
            with open(dfpath, 'r', encoding='utf-8', errors='ignore') as f:
                printf(f.read()[:500] + "...\n[TRUNCATED]")

    printf("\n--- docker ps ---")
    try:
        res = subprocess.run(["docker", "ps"], capture_output=True, text=True)
        printf(res.stdout if res.returncode == 0 else res.stderr)
    except Exception as e:
        printf(f"Error: {e}")
