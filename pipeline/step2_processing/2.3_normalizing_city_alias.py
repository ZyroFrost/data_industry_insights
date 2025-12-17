# -*- coding: utf-8 -*-
"""
STEP 2.3 – CITY ALIAS NORMALIZATION
(This step supports STEP 2.4 – enrich_data_country_city.py)

This script normalizes raw city values using a city alias reference table.

It resolves heterogeneous city names (multi-language, exonyms,
job-market usage) into a single canonical city name
(English, GeoNames standard).

Purpose:
- Standardize city values before geo enrichment
- Reduce noise caused by aliases, languages, and variants
- Ensure downstream geo lookup uses canonical city names only

PIPELINE:
STEP 2:
- STEP 2.1 – Column mapping (CSV → ERD schema)
- STEP 2.2 – City alias normalization (this step)
- STEP 2.3 – Reference-based geo enrichment (country / ISO)

INPUT:
- data/data_processing/data_mapped/*.csv or *.xlsx
- data/data_reference/city_alias_reference.csv

OUTPUT:
- data/data_processing/data_city_normalized/normalized_*.xlsx
"""

import pandas as pd
import unicodedata
import re
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.2_data_description_extracted"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.3_data_city_normalized"
REF_DIR = BASE_DIR / "data" / "data_reference"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CITY_ALIAS_PATH = REF_DIR / "city_alias_reference.csv"

# =========================
# HELPERS
# =========================

def normalize_text(x):
    if pd.isna(x):
        return "__NA__"
    x = str(x)
    x = unicodedata.normalize("NFKD", x)
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = re.sub(r"[^\w\s]", "", x)
    return re.sub(r"\s+", " ", x).strip().upper()

# =========================
# LOAD CITY ALIAS REFERENCE
# =========================
if not CITY_ALIAS_PATH.exists():
    raise FileNotFoundError(
        f"Missing city alias reference: {CITY_ALIAS_PATH}\n"
        "Please run STEP 0.2 – build_city_alias_reference.py first."
    )

alias_df = pd.read_csv(CITY_ALIAS_PATH, dtype=str)

# alias_norm -> canonical_city
CITY_ALIAS_LOOKUP = {
    normalize_text(alias): canonical
    for canonical, alias in zip(
        alias_df["canonical_city"],
        alias_df["alias"]
    )
}

CITIES_REF_PATH = REF_DIR / "cities.csv"

cities_df = pd.read_csv(CITIES_REF_PATH, dtype=str)

# normalize city name for lookup
cities_df["name_norm"] = cities_df["city_name"].apply(normalize_text)

# norm_name -> official city name (GeoNames)
CITY_NAME_LOOKUP = dict(
    zip(cities_df["name_norm"], cities_df["city_name"])
)

# =========================
# NORMALIZE SINGLE FILE
# =========================

def normalize_city_file(file_path: Path):
    print(f"🔄 Normalizing city aliases: {file_path.name}")

    # load (csv / xlsx)
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype=str)
    else:
        df = pd.read_excel(file_path, dtype=str)

    if "city" not in df.columns:
        print("  ⚠ No city column found, skipping")
        return

    city_before = df["city"].fillna("__NA__")
    city_norm = city_before.apply(normalize_text)

    normalized_city = []

    for raw, norm in zip(city_before, city_norm):
        if norm == "__NA__":
            normalized_city.append("__NA__")
        elif norm in CITY_ALIAS_LOOKUP:
            alias_canonical = CITY_ALIAS_LOOKUP[norm]
            alias_norm = normalize_text(alias_canonical)

            if alias_norm in CITY_NAME_LOOKUP:
                normalized_city.append(CITY_NAME_LOOKUP[alias_norm])
            else:
                normalized_city.append("__INVALID__")

        else:
            normalized_city.append("__INVALID__")

    df["city"] = normalized_city

    # save output
    stem = file_path.stem

    # remove intermediate step prefixes
    for p in [
        "extracted_desc_",
        "mapped_",
        "normalized_",
    ]:
        stem = stem.replace(p, "")

    output_name = f"normalized_{stem}.csv"
    output_path = OUTPUT_DIR / output_name

    output_path = OUTPUT_DIR / output_name.replace(file_path.suffix, ".csv")
    #df.to_excel(output_path.with_suffix(".xlsx"), index=False)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # log summary
    total = len(df)
    invalid = (df["city"] == "__INVALID__").sum()
    na = (df["city"] == "__NA__").sum()
    valid = total - invalid - na

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - Total rows          : {total}\n"
        f"    - City normalized     : {valid}\n"
        f"    - City empty (__NA__) : {na}\n"
        f"    - City invalid (!)    : {invalid}\n"
        f"  → Folder saved          : {output_path.parent}"
    )

# =========================
# RUN STEP
# =========================

def run():
    files = [f for f in INPUT_DIR.iterdir() if f.is_file()]
    if not files:
        print(f"No input files found in {INPUT_DIR}")
        return

    for f in files:
        normalize_city_file(f)

    print("\n=== STEP 2.2 COMPLETED: CITY ALIAS NORMALIZATION ===")

if __name__ == "__main__":
    run()