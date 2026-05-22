import os
import shutil
import glob

ROOT = "/app"
SOURCES = ["bbc", "foxnews", "nypost", "nyt"]

for source in SOURCES:
    src_dir = os.path.join(ROOT, "data", "bronze", source)
    dst_dir = os.path.join(ROOT, "data", "bronze", "news", source)

    if not os.path.isdir(src_dir):
        print(f"[SKIP] Source not found: {src_dir}")
        continue

    os.makedirs(dst_dir, exist_ok=True)

    files = glob.glob(os.path.join(src_dir, "*.parquet"))
    moved = 0
    for f in files:
        if os.path.basename(f) == "latest.parquet":
            continue
        shutil.move(f, dst_dir)
        print(f"Moved: {os.path.basename(f)} → {dst_dir}")
        moved += 1

    print(f"[{source}] {moved} files moved.")

print("\nMigration complete. Old folders still exist but contain only latest.parquet.")
print("You can delete the old top-level folders manually once you confirm silver picked up the data.")
