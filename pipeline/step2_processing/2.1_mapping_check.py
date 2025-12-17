# -*- coding: utf-8 -*-
"""
STEP 2.0 – Use Mapping Tool to Map CSV Columns to ERD Schema (tools/column_mapper_app.py)
STEP 2.1 – Mapping Check (this step)

Purpose:
- Simple mapping coverage check
- Count extracted files vs mapped files
- Fail if any extracted file has not been mapped
"""

import sys
from pathlib import Path

# ======================================================
# PATH CONFIG
# ======================================================

ROOT = Path(__file__).resolve().parents[2]

EXTRACTED_DIR = ROOT / "data" / "data_processing" / "s1_data_extracted"
MAPPED_DIR = ROOT / "data" / "data_processing" / "s2.1_data_mapped"

# ======================================================
# COLLECT FILES
# ======================================================

VALID_EXTS = {".csv", ".xlsx"}

extracted_files = sorted(
    f for f in EXTRACTED_DIR.iterdir()
    if f.is_file() and f.suffix.lower() in VALID_EXTS
)

mapped_files = sorted(
    f for f in MAPPED_DIR.iterdir()
    if f.is_file() and f.suffix.lower() in VALID_EXTS
)

extracted_count = len(extracted_files)
mapped_count = len(mapped_files)

# ======================================================
# OUTPUT
# ======================================================

print("\n🔎 STEP 02 – MAPPING CHECK\n")

print(f"📂 Extracted files : {extracted_count}")
print(f"📂 Mapped files    : {mapped_count}")

print(
    f"\n📊 Mapping result  : {mapped_count} / {extracted_count} files mapped"
)

# ======================================================
# FINAL RESULT
# ======================================================

print("\n================ RESULT ================")

if mapped_count < extracted_count:
    missing = extracted_count - mapped_count
    print(
        f"❌ STEP 02 FAILED – {missing} extracted file(s) have NOT been mapped"
    )
    sys.exit(1)
else:
    print("✅ STEP 02 PASSED – All extracted files have been mapped")
    sys.exit(0)