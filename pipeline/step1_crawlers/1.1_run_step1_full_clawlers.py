# -*- coding: utf-8 -*-
"""
STEP 01
- Crawl all data sources
- Count & audit CSV files after crawling
"""

from pathlib import Path
import pandas as pd

# =====================================================
# IMPORT CRAWLERS
# =====================================================
from pipeline.step1_crawlers.api.authenticated import crawl_adzuna_datajobs
from pipeline.step1_crawlers.api.authenticated import crawl_usa_datajobs
from pipeline.step1_crawlers.api.public import crawl_canada_datajobs
from pipeline.step1_crawlers.api.public import crawl_RemoteOK_datajobs

# =====================================================
# PATH CONFIG
# =====================================================
ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "data_processing" / "data_extracted"


# =====================================================
# STEP 01 – CRAWLING
# =====================================================
def run_crawlers():
    print("🚀 STEP 01: Start crawling data sources...\n")

    crawl_adzuna_datajobs()
    crawl_usa_datajobs()
    crawl_canada_datajobs()
    crawl_RemoteOK_datajobs()

    print("\n✅ Crawling completed.\n")


# =====================================================
# CSV AUDIT UTILITIES
# =====================================================
def count_csv_rows(path: Path) -> int:
    """Count rows in CSV (excluding header)"""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f) - 1


def audit_csv_basic(data_dir: Path):
    print("📊 Auditing CSV files...\n")

    records = []

    for csv_file in data_dir.glob("*.csv"):
        try:
            df_head = pd.read_csv(csv_file, nrows=0)

            records.append({
                "file_name": csv_file.name,
                "row_count": count_csv_rows(csv_file),
                "column_count": len(df_head.columns)
            })

        except Exception as e:
            records.append({
                "file_name": csv_file.name,
                "row_count": None,
                "column_count": None
            })

    df = pd.DataFrame(records)

    df["row_count"] = pd.to_numeric(df["row_count"], errors="coerce")
    df["column_count"] = pd.to_numeric(df["column_count"], errors="coerce")

    summary = {
        "total_files": int(len(df)),
        "total_rows": int(df["row_count"].sum()),
        "total_columns": int(df["column_count"].sum()),
        "error_files": int(df["row_count"].isna().sum())
    }

    print("🗂 CSV DETAIL")
    print(df.sort_values("file_name"))

    print("\n📌 SUMMARY")
    for k, v in summary.items():
        print(f"- {k}: {v}")

    return df, summary


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    print("=" * 60)
    print("DATA INDUSTRY INSIGHTS – STEP 01")
    print("=" * 60)

    run_crawlers()
    audit_csv_basic(DATA_DIR)

    print("\n🎉 STEP 01 DONE")