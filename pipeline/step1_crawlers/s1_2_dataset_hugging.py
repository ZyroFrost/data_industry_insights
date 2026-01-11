# -*- coding: utf-8 -*-
"""
STEP 1.5 – INGEST HUGGINGFACE DATASET (data_jobs)

Purpose:
- Load HuggingFace datasets
- Export raw datasets to CSV
- Treat as extracted source (NO cleaning, NO normalization)

IMPORTANT:
- Explicit overwrite check
"""

from datasets import load_dataset
from pathlib import Path
import pandas as pd

# =====================================================
# PATHS
# =====================================================

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.0_data_extracted"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HF_DATA_JOBS_CSV = OUTPUT_DIR / "hf_data_jobs_2023-2025.csv"

# File này ít row với bị trùng với file 1 nên ko xài
# HF_AI_JOBS_CSV = OUTPUT_DIR / "extracted_hf_2025_ai_jobs.csv"

# =====================================================
# HELPERS
# =====================================================

def confirm_overwrite(path: Path, force: bool = False) -> bool:
    if not path.exists():
        return True

    if force:
        print(f"⚠ Overwriting existing file: {path.name}")
        return True

    ans = input(f"⚠ File '{path.name}' already exists. Overwrite? [y/N]: ").strip().lower()
    return ans == "y"


def save_csv_safe(df: pd.DataFrame, path: Path, force: bool = False):
    if not confirm_overwrite(path, force=force):
        print(f"⏭ Skipped: {path.name}")
        return False

    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✓ Saved: {path.name} ({len(df):,} rows)")
    return True


# =====================================================
# MAIN INGEST LOGIC
# =====================================================

def run_huggingface_ingest(force: bool = False):
    print("\n🚀 STEP 1.5 – HuggingFace ingestion\n")

    # Dataset 1: General Data Jobs
    dataset_jobs = load_dataset(
        "lukebarousse/data_jobs",
        split="train"
    )
    df_jobs = dataset_jobs.to_pandas()
    save_csv_safe(df_jobs, HF_DATA_JOBS_CSV, force=force)

    # # Dataset 2: 2025 AI / Data Jobs
    # dataset_ai_jobs = load_dataset(
    #     "princekhunt19/2025-ai-data-jobs-dataset",
    #     split="train"
    # )
    # df_ai_jobs = dataset_ai_jobs.to_pandas()
    # save_csv_safe(df_ai_jobs, HF_AI_JOBS_CSV, force=force)

    # print("\n🎉 HuggingFace ingestion DONE\n")


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    run_huggingface_ingest(force=False)