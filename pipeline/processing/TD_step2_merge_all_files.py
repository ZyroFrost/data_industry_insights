# PURPOSE
# Merge all processed job datasets into a single master file.

# Input:
# - data/data_processing/data_enriched/*_final.csv

# Output:
# - data/data_processing/jobs_master_final.csv

# How to run:
# - python merge_final_files.py

import pandas as pd
import os

# =========================
# PATHS
# =========================

BASE_DIR = os.getcwd()
INPUT_DIR = os.path.join(BASE_DIR, "data/data_processing/data_enriched")
OUTPUT_DIR = os.path.join(BASE_DIR, "data/data_processing/data_enriched")

MASTER_FILE = "jobs_master_final.csv"
MASTER_PATH = os.path.join(OUTPUT_DIR, MASTER_FILE)

# =========================
# MERGE FILES
# =========================

def merge_final_files():
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith("_final.csv")]

    if not files:
        print("⚠️ No _final.csv files found.")
        return

    dfs = []
    for f in files:
        print(f"📄 Loading: {f}")
        df = pd.read_csv(os.path.join(INPUT_DIR, f))
        df["source_file"] = f  # optional: track origin
        dfs.append(df)

    master_df = pd.concat(dfs, ignore_index=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    master_df.to_csv(MASTER_PATH, index=False, encoding="utf-8")

    print("\n✅ MERGE COMPLETE")
    print(f"   Files merged : {len(files)}")
    print(f"   Total rows  : {len(master_df):,}")
    print(f"   Output file : {MASTER_PATH}")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    merge_final_files()