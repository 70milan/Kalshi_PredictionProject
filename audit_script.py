import os
import glob
import duckdb
import subprocess
from datetime import datetime
import ast

ROOT = r"c:\Data Engineering\codeprep\predection_project"

with open(os.path.join(ROOT, "audit_output.txt"), "w", encoding="utf-8") as out:
    def printf(msg):
        out.write(str(msg) + "\n")

    # 1. Directory Tree with Sizes
    def print_tree(dir_path, prefix=""):
        try:
            items = os.listdir(dir_path)
            for item in items:
                if item in [".git", ".venv", ".pytest_cache", "__pycache__", ".vscode", "tmp", "audit_output.txt"]:
                    continue
                path = os.path.join(dir_path, item)
                if os.path.isdir(path):
                    printf(f"{prefix}+-- {item}/")
                    print_tree(path, prefix + "    ")
                else:
                    try:
                        size_kb = os.path.getsize(path) / 1024
                        printf(f"{prefix}+-- {item} ({size_kb:.2f} KB)")
                    except Exception as e:
                        printf(f"{prefix}+-- {item} (Error)")
        except Exception as e:
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
                    try:
                        with open(path, 'r', encoding='utf-8', errors='ignore') as pyf:
                            try:
                                tree = ast.parse(pyf.read())
                                funcs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
                            except:
                                funcs = ["Error parsing AST"]
                    except:
                        funcs = ["Error reading file"]
                    printf(f"File: {os.path.relpath(path, ROOT)} | Modified: {dt} | Functions: {funcs}")
                except Exception as e:
                    printf(f"File: {os.path.relpath(path, ROOT)} (Error: {e})")

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
        for source in os.listdir(bronze_dir):
            source_path = os.path.join(bronze_dir, source)
            if os.path.isdir(source_path):
                for subs in os.listdir(source_path):
                    sub_path = os.path.join(source_path, subs)
                    if os.path.isdir(sub_path):
                        files = glob.glob(os.path.join(sub_path, "**/*.parquet"), recursive=True)
                        sizes = [os.path.getsize(f) for f in files]
                        total_kb = sum(sizes) / 1024 if sizes else 0
                        printf(f"\nSource: {source}/{subs} | Files: {len(files)} | Total: {total_kb:.2f} KB")
                        if files:
                            ftq = files[0].replace('\\', '/')
                            spq = sub_path.replace('\\', '/')
                            printf("Schema:"); printf(run_duckdb_query(f"DESCRIBE SELECT * FROM '{ftq}';"))
                            printf("Rows:"); printf(run_duckdb_query(f"SELECT count(*) as count FROM '{spq}/*.parquet';"))
                            printf("Sample:"); printf(run_duckdb_query(f"SELECT * FROM '{ftq}' LIMIT 3;"))
                            printf("MIN/MAX ingested_at:"); printf(run_duckdb_query(f"SELECT MIN(ingested_at) as min_dt, MAX(ingested_at) as max_dt FROM '{spq}/*.parquet' WHERE ingested_at IS NOT NULL;"))

    # 4. Reference Files
    printf("\n=== 4. Reference Files ===")
    ref_paths = [os.path.join(ROOT, "data", "reference"), os.path.join(ROOT, "reference")]
    for rdir in ref_paths:
        if os.path.exists(rdir):
            for f in os.listdir(rdir):
                path = os.path.join(rdir, f)
                if os.path.isfile(path):
                    printf(f"\nFile: {os.path.relpath(path, ROOT)}")
                    if f.endswith('.csv') or f.endswith('.parquet'):
                        reader = "read_csv_auto" if f.endswith(".csv") else "read_parquet"
                        pf = path.replace('\\', '/')
                        printf("Columns:"); printf(run_duckdb_query(f"DESCRIBE SELECT * FROM {reader}('{pf}');"))
                        printf("Rows:"); printf(run_duckdb_query(f"SELECT count(*) as count FROM {reader}('{pf}');"))
                        printf("Sample:"); printf(run_duckdb_query(f"SELECT * FROM {reader}('{pf}') LIMIT 3;"))

    # 6. Docker Status
    printf("\n=== 6. Docker Status ===")
    for dfile in ["Dockerfile", "docker-compose.yml"]:
        dfpath = os.path.join(ROOT, dfile)
        if os.path.exists(dfpath):
            printf(f"\n--- {dfile} ---")
            try:
                with open(dfpath, 'r', encoding='utf-8', errors='ignore') as f:
                    printf(f.read())
            except Exception as e:
                pass

    printf("\n--- docker ps ---")
    try:
        res = subprocess.run(["docker", "ps"], capture_output=True, text=True)
        printf(res.stdout if res.returncode == 0 else res.stderr)
    except Exception as e:
        printf(f"Error: {e}")

    # 7. Agent Skills
    printf("\n=== 7. Agent Skills and Workflow Files ===")
    agent_dir = os.path.join(ROOT, ".agent")
    if os.path.exists(agent_dir):
        for root, _, files in os.walk(agent_dir):
            for f in files:
                printf(os.path.relpath(os.path.join(root, f), ROOT))
    
    chk = os.path.join(ROOT, ".agent", "workflows", "pre-response-checklist.md")
    if os.path.exists(chk):
        printf("\n--- pre-response-checklist.md ---")
        try:
            with open(chk, 'r', encoding='utf-8', errors='ignore') as f:
                printf(f.read())
        except:
             pass
