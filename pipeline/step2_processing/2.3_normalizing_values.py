# -*- coding: utf-8 -*-
"""
STEP 2.3 – VALUE NORMALIZATION (CITY, COMPANY, EMPLOYMENT, CURRENCY, ETC.)

This step standardizes extracted textual values into
canonical, analysis-ready formats using reference mappings
and lightweight normalization rules.

IMPORTANT:
- This step DOES NOT extract new information from job_description
- It ONLY normalizes values that were already extracted in STEP 2.2
- Existing non-__NA__ values are NEVER overridden with guesses

Handled columns (if present):
- city                → canonical city name (GeoNames standard)
- company_name        → whitespace & formatting normalization
- employment_type     → enum normalization (Full-time, Part-time, Internship, Temporary)
- currency            → ISO currency code (USD, EUR, GBP, ...)
- posting_date        → YYYY-MM-DD
* country and country_iso are not handled here because they depend on the city (normalized city data is required first). See STEP 2.4

Purpose:
- Ensure enum consistency before enrichment & database loading
- Reduce noise from casing, separators, symbols, and variants
- Guarantee downstream steps operate on clean, comparable values

PIPELINE CONTEXT:
STEP 2 – Data Processing
- STEP 2.1 – Column mapping (raw → ERD schema)
- STEP 2.2 – Description signal extraction (weak signals only)
- STEP 2.3 – Value normalization (this step)
- STEP 2.4 – Reference-based geo enrichment (country / ISO / lat-lon)

INPUT:
- data/data_processing/s2.2_data_description_extracted/*.csv

REFERENCE FILES:
- data/data_reference/city_alias_reference.csv
- data/data_reference/cities.csv
- data/data_reference/employment_type_mapping.csv
- data/data_reference/currency_mapping.csv

OUTPUT:
- data/data_processing/s2.3_data_city_company_employment_normalized/normalized_*.csv

DESIGN PRINCIPLES:
1. Normalize, don’t infer
2. Enum-safe outputs only
3. __NA__ stays __NA__
4. No description re-parsing
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
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.3_data_values_normalized"
REF_DIR = BASE_DIR / "data" / "data_reference"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CITY_ALIAS_PATH = REF_DIR / "city_alias_reference.csv"

UNMATCHED_DIR = BASE_DIR / "data" / "data_unmatched_report"
UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)

UNMATCHED_CITY_PATH = UNMATCHED_DIR / "unmatched_city_name.csv"

# =========================
# EXPERIENCE YEAR NORMALIZATION
# =========================

EXP_LEVEL_MAP = {
    "EN": "0",
    "MI": "2",
    "SE": "5",
    "EX": "8",
}

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

def normalize_company_name(x):
    if pd.isna(x):
        return "__NA__"
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    return x

def normalize_currency(x, NA="__NA__"):
    if x == NA or not isinstance(x, str):
        return x

    s = x.strip().lower()

    # remove noise
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)

    # direct mapping
    if s in CURRENCY_LOOKUP:
        return CURRENCY_LOOKUP[s]

    return NA

def normalize_posted_date(x, NA="__NA__"):
    if x == NA or not isinstance(x, str):
        return NA

    s = x.strip()

    try:
        # ISO format: 2025-11-28T21:22:29Z
        if "T" in s:
            return s.split("T")[0]

        # already YYYY-MM-DD
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            return s[:10]

        return NA
    except Exception:
        return NA
    
def normalize_remote_option(x, NA="__NA__"):
    if pd.isna(x):
        return NA

    v = str(x).strip()

    if v in {"", NA}:
        return NA

    # numeric percentage (highest priority)
    if v in {"0", "0.0"}:
        return "Onsite"
    if v in {"50", "50.0"}:
        return "Hybrid"
    if v in {"100", "100.0"}:
        return "Remote"

    # boolean
    if v.upper() == "TRUE":
        return "Remote"
    if v.upper() == "FALSE":
        return "Onsite"

    return "__INVALID__"

def normalize_employment_type(x, NA="__NA__"):
    if pd.isna(x):
        return NA

    s = str(x).strip()
    if s == "" or s.upper() == NA:
        return NA

    key = normalize_text(s)

    if key in EMPLOYMENT_TYPE_LOOKUP:
        return EMPLOYMENT_TYPE_LOOKUP[key]

    return "__INVALID__"
    
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
# LOAD CURRENCY MAPPING
# =========================

CURRENCY_MAP_PATH = REF_DIR / "currency_mapping.csv"

currency_df = pd.read_csv(CURRENCY_MAP_PATH, dtype=str)

CURRENCY_LOOKUP = {}

for _, row in currency_df.iterrows():
    canonical = row["currency"].strip().upper()
    aliases = [a.strip().lower() for a in row["aliases"].split("|")]

    for a in aliases:
        CURRENCY_LOOKUP[a] = canonical

# =========================
# LOAD EMPLOYMENT TYPE MAPPING
# =========================

EMPLOYMENT_MAP_PATH = REF_DIR / "employment_type_mapping.csv"

emp_df = pd.read_csv(EMPLOYMENT_MAP_PATH, dtype=str)

EMPLOYMENT_TYPE_LOOKUP = {}

for _, row in emp_df.iterrows():
    canonical = row["employment_type"].strip()
    keywords = row["keywords"]

    if pd.isna(keywords):
        continue

    for kw in keywords.split("|"):
        key = normalize_text(kw)
        EMPLOYMENT_TYPE_LOOKUP[key] = canonical


# =========================
# NORMALIZE SINGLE FILE (CSV)
# =========================

def normalize_city_file(file_path: Path):
    print(f"🔄 Normalizing city aliases: {file_path.name}")

    # load (csv / xlsx)
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype=str)
    else:
        df = pd.read_excel(file_path, dtype=str)

    # =========================
    # NORMALIZE REQUIRED EXPERIENCE YEARS (ENUM ONLY)
    # =========================
    if "required_exp_years" in df.columns:
        df["required_exp_years"] = (
            df["required_exp_years"]
            .astype(str)
            .str.strip()
            .str.upper()
            .apply(lambda x: EXP_LEVEL_MAP.get(x, x))
        )

    # =========================
    # NORMALIZE REMOTE OPTION (ENUM)
    # =========================
    remote_norm_count = 0
    remote_invalid_count = 0

    if "remote_option" in df.columns:
        before_remote = df["remote_option"].copy()

        for i, val in df["remote_option"].items():
            norm = normalize_remote_option(val, "__NA__")

            if norm == "__INVALID__":
                df.at[i, "remote_option"] = "__INVALID__"
                remote_invalid_count += 1
            else:
                df.at[i, "remote_option"] = norm

        remote_norm_count = (before_remote != df["remote_option"]).sum()

    # =========================
    # NORMALIZE COMPANY NAME (INTERNAL, SIMPLE)
    # =========================
    if "company_name" in df.columns:
        df["company_name"] = df["company_name"].apply(normalize_company_name)

    if "city" not in df.columns:
        print("  ⚠ No city column found, skipping")
        return

    city_before = df["city"].fillna("__NA__")
    df["_city_raw"] = city_before

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
                normalized_city.append("__UNMATCHED__")

        else:
            normalized_city.append("__UNMATCHED__")

    df["city"] = normalized_city

    # =========================
    # NORMALIZE COMPANY NAME (INTERNAL, SIMPLE)
    # =========================
    company_norm_count = 0
    if "company_name" in df.columns:
        before_company = df["company_name"].copy()
        df["company_name"] = df["company_name"].apply(normalize_company_name)
        company_norm_count = (before_company != df["company_name"]).sum()

    # =========================
    # NORMALIZE EMPLOYMENT TYPE (FORMAT ONLY)
    # =========================
    employment_norm_count = 0
    if "employment_type" in df.columns:
        before_emp = df["employment_type"].copy()
        df["employment_type"] = df["employment_type"].apply(
            lambda x: normalize_employment_type(x, "__NA__")
        )
        employment_norm_count = (before_emp != df["employment_type"]).sum()

    # =========================
    # NORMALIZE CURRENCY (ENUM)
    # =========================
    currency_norm_count = 0
    if "currency" in df.columns:
        before_currency = df["currency"].copy()
        df["currency"] = df["currency"].apply(normalize_currency)
        currency_norm_count = (before_currency != df["currency"]).sum()

    # =========================
    # NORMALIZE POSTED DATE
    # =========================
    posted_date_norm_count = 0
    if "posted_date" in df.columns:
        before_date = df["posted_date"].copy()
        df["posted_date"] = df["posted_date"].apply(normalize_posted_date)

        posted_date_norm_count = (
            (before_date != df["posted_date"])
            & (df["posted_date"] != "__NA__")
        ).sum()


    # =========================
    # EXPORT UNMATCHED CITY
    # =========================

    # normalize raw city & country for comparison
    city_raw_norm = df["_city_raw"].apply(normalize_text)
    country_norm = df["country"].apply(normalize_text) if "country" in df.columns else None

    unmatched_mask = df["city"] == "__UNMATCHED__"

    # exclude cases where city == country
    if country_norm is not None:
        unmatched_mask &= city_raw_norm != country_norm

    unmatched_df = df.loc[
        unmatched_mask,
        ["_city_raw"]
    ].copy()

    unmatched_df.rename(
        columns={"_city_raw": "city_raw"},
        inplace=True
    )

    unmatched_df.rename(
        columns={"_city_raw": "city_raw"},
        inplace=True
    )

    if not unmatched_df.empty:
        unmatched_df.insert(0, "__source_name", file_path.name)
        unmatched_df.insert(1, "__source_id", unmatched_df.index.astype(str))

        if UNMATCHED_CITY_PATH.exists():
            existing = pd.read_csv(UNMATCHED_CITY_PATH, dtype=str)
            pd.concat(
                [existing, unmatched_df],
                ignore_index=True
            ).drop_duplicates().to_csv(
                UNMATCHED_CITY_PATH,
                index=False,
                encoding="utf-8-sig"
            )
        else:
            unmatched_df.to_csv(
                UNMATCHED_CITY_PATH,
                index=False,
                encoding="utf-8-sig"
            )

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
    df.drop(columns=["_city_raw"], inplace=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # log summary
    total = len(df)
    unmatched = (df["city"] == "__UNMATCHED__").sum()
    na = (df["city"] == "__NA__").sum()
    valid = total - unmatched - na

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - Total rows              : {total}\n"
        f"    - City normalized         : {valid}\n"
        f"    - City empty (__NA__)     : {na}\n"
        f"    - City unmatched (!)      : {unmatched}\n"
        f"    - Company normalized      : {company_norm_count}\n"
        f"    - Employment normalized   : {employment_norm_count}\n"
        f"    - Currency normalized     : {currency_norm_count}\n"
        f"    - Posted date normalized  : {posted_date_norm_count}\n"
        f"    - Remote normalized       : {remote_norm_count}\n"
        f"    - Remote invalid enum (!) : {remote_invalid_count}\n"
        f"  → Folder saved              : {output_path.parent}"
    )
    return unmatched

# =========================
# RUN STEP
# =========================

def run():
    files = [f for f in INPUT_DIR.iterdir() if f.is_file()]
    total_unmatched_city = 0
    if not files:
        print(f"No input files found in {INPUT_DIR}")
        return

    # =========================
    # RESET UNMATCHED REPORT (PER RUN)
    # =========================
    if UNMATCHED_CITY_PATH.exists():
        UNMATCHED_CITY_PATH.unlink()

    for f in files:
        unmatched = normalize_city_file(f)
        if unmatched:
            total_unmatched_city += unmatched

    if UNMATCHED_CITY_PATH.exists():
        print(f"→ Unmatched city report saved: {UNMATCHED_CITY_PATH}")

    print(f"→ Total unmatched city (all files): {total_unmatched_city}")

    print("\n=== STEP 2.3 COMPLETED: CITY ALIAS NORMALIZATION ===")

if __name__ == "__main__":
    run()