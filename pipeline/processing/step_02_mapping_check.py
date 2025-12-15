# -*- coding: utf-8 -*-
"""
STEP 02 – Mapping Check
----------------------
Purpose:
- Check all mapped CSV files against ERD ingestion schema
- Ensure:
  + All extracted files have corresponding mapped files
  + Mapped files contain ALL required ingestion columns
  + No unexpected columns (except derived targets)

IMPORTANT:
- Fields with `derive_to` (e.g. salary_min_max) are NOT ingestion fields
- Only real ingestion fields are checked
"""

import json
import sys
from pathlib import Path
import pandas as pd

# ======================================================
# PATH CONFIG
# ======================================================
ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = ROOT / "pipeline" / "tools" / "ERD_schema.json"
EXTRACTED_DIR = ROOT / "data" / "data_processing" / "data_extracted"
MAPPED_DIR = ROOT / "data" / "data_processing" / "data_mapped"

# ======================================================
# LOAD ERD SCHEMA
# ======================================================
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    ERD_SCHEMA = json.load(f)

# ======================================================
# BUILD INGESTION SCHEMA (CRITICAL FIX)
# ======================================================
INGESTION_FIELDS = set()
DERIVED_HELPERS = set()
DERIVED_TARGETS = set()

for col, meta in ERD_SCHEMA.items():
    if meta.get("derive_to"):
        # helper field (e.g. salary_min_max)
        DERIVED_HELPERS.add(col)
        DERIVED_TARGETS.update(meta["derive_to"])
    else:
        INGESTION_FIELDS.add(col)

# remove derived targets from ingestion set if duplicated
INGESTION_FIELDS = INGESTION_FIELDS.union(DERIVED_TARGETS)

# ======================================================
# CHECK FILE COUNTS
# ======================================================
extracted_files = sorted(EXTRACTED_DIR.glob("*.csv"))
mapped_files = sorted(MAPPED_DIR.glob("mapped_*.csv"))

print("\n🔎 STEP 02 – MAPPING CHECK\n")

print(f"📂 Extracted files: {len(extracted_files)}")
print(f"📂 Mapped files    : {len(mapped_files)}")

if len(mapped_files) < len(extracted_files):
    print("❌ ERROR: Some extracted files have NOT been mapped.")
else:
    print("✅ All extracted files have mapped outputs.")

# ======================================================
# CHECK EACH MAPPED FILE
# ======================================================
has_error = False

for mapped_file in mapped_files:
    print(f"\n🔍 Checking schema: {mapped_file.name}")

    try:
        df = pd.read_csv(mapped_file)
    except Exception as e:
        print(f"❌ Failed to read file: {e}")
        has_error = True
        continue

    csv_columns = set(df.columns)

    # ❌ Missing ingestion columns
    missing_cols = INGESTION_FIELDS - csv_columns

    # ⚠ Extra columns (not part of ingestion schema)
    extra_cols = csv_columns - INGESTION_FIELDS

    if missing_cols:
        print("❌ Missing ERD ingestion columns:")
        for c in sorted(missing_cols):
            print(f"   - {c}")
        has_error = True
    else:
        print("✅ No missing ingestion columns")

    if extra_cols:
        print("⚠ Extra columns (ignored):")
        for c in sorted(extra_cols):
            print(f"   - {c}")
    else:
        print("✅ No unexpected columns")

# ======================================================
# FINAL STATUS
# ======================================================
print("\n================ RESULT ================")

if has_error:
    print("❌ STEP 02 FAILED – Mapping incomplete or invalid")
    sys.exit(1)
else:
    print("✅ STEP 02 PASSED – All mappings valid")
    sys.exit(0)