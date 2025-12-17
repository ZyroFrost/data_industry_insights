# -*- coding: utf-8 -*-
"""
STEP 2.2 – EXTRACTING DESCRIPTION SIGNALS
(This step supports STEP 2.3 – city alias normalization & geo enrichment)

This script extracts weak signals from job_description text
to optionally fill structured fields WHEN they are missing (__NA__).

Extracted signals (simple, rule-based only):
- city
- country
- remote_option
- min_salary / max_salary (very coarse)

IMPORTANT RULES:
1. Description-derived values ONLY fill when target column == "__NA__"
2. NEVER override existing structured data
3. Cannot extract → stay silent (no INVALID, no guess)

PIPELINE:
STEP 2:
- STEP 2.1 – Column mapping
- STEP 2.2 – Description signal extraction (this step)
- STEP 2.3 – City alias normalization
- STEP 2.4 – Geo enrichment

INPUT:
- data/data_processing/s2.1_data_mapped/*.csv or *.xlsx

OUTPUT:
- data/data_processing/s2.2_data_description_extracted/extracted_desc_*.csv
"""

import pandas as pd
import re
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.1_data_mapped"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.2_data_description_extracted"
REF_DIR = BASE_DIR / "data" / "data_reference"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# CONSTANTS
# =========================

NA_VALUE = "__NA__"

REMOTE_KEYWORDS = [
    "remote",
    "work from home",
    "wfh",
    "fully remote",
    "hybrid"
]

# ===== SALARY CONTEXT GUARD =====
SALARY_CONTEXT_KEYWORDS = [
    # currency symbols (expanded with common ones)
    "$", "€", "£", "¥", "¢", "₹", "₽", "₩", "₪", "₱", "฿", "₦", "₴", "₺",

    # currency text/codes (common ISO codes and names)
    "usd", "eur", "euro", "gbp", "pound", "aud", "cad", "jpy", "yen", "cny", "yuan", "inr", "rub", "krw", "chf", "sek", "nok", "dkk", "pln", "czk", "huf",
]

SALARY_INDICATORS = [
    "salary", "salaries", "pay", "compensation",
    "wage", "package", "remuneration",
    "brutto", "netto", "gehalt",
    "per month", "per year", "annually", "monthly",
    "€/month", "$/year"
]

NON_SALARY_CONTEXT = [
    "arr", "revenue", "funding", "series",
    "valuation", "investor", "financing",
    "customers", "market", "growth",
    "raised", "million", "billion",

    # allowance / benefit
    "allowance", "packaging", "bonus",
    "incentive", "benefit", "p/a"
]

# =========================
# LOAD REFERENCE (LIGHT USE)
# =========================

countries_df = pd.read_csv(
    REF_DIR / "countries.csv",
    dtype=str
)

COUNTRY_LOOKUP = {
    c.lower(): c
    for c in countries_df["country_name"].dropna().unique()
}

city_alias_df = pd.read_csv(
    REF_DIR / "city_alias_reference.csv",
    dtype=str
)
def normalize_text(x):
    x = str(x)
    x = re.sub(r"[^\w\s]", "", x)
    return re.sub(r"\s+", " ", x).strip().lower()


CITY_ALIAS_LOOKUP = {
    normalize_text(row["alias"]): row["canonical_city"]
    for _, row in city_alias_df.iterrows()
}

# =========================
# HELPERS
# =========================
def normalize_currency(c):
    mapping = {
        "€": "EUR", "eur": "EUR", "euro": "EUR",
        "$": "USD", "usd": "USD",
        "£": "GBP", "gbp": "GBP", "pound": "GBP",
        "¥": "JPY", "jpy": "JPY", "yen": "JPY"
    }
    return mapping.get(c, c.upper())

def extract_remote(text: str):
    return any(k in text for k in REMOTE_KEYWORDS)

def extract_country(text: str):
    for c in COUNTRY_LOOKUP:
        if c in text:
            return COUNTRY_LOOKUP[c]
    return None

def extract_city(text: str):
    for alias, canonical in CITY_ALIAS_LOOKUP.items():
        if alias in text:
            return canonical
    return None

def extract_salary_from_text(desc, cur_min, cur_max, NA="__NA__"):
    text = str(desc).lower()

    def has(val):
        return val != NA and val not in (None, "")

    # ===== 1. TÌM ĐƠN VỊ TIỀN =====
    currency_hits = []
    for k in SALARY_CONTEXT_KEYWORDS:
        start = 0
        while True:
            idx = text.find(k, start)
            if idx == -1:
                break
            currency_hits.append((idx, idx + len(k), k))
            start = idx + len(k)

    if not currency_hits:
        return cur_min, cur_max, NA, False

    extracted_values = []
    detected_currency = NA

    # ===== 2. KHOANH VÙNG THEO ĐƠN VỊ =====
    for start, end, cur in currency_hits:
        left = max(0, start - 20)
        right = min(len(text), end + 20)

        while left > 0 and text[left - 1].isdigit():
            left -= 1
        while right < len(text) and text[right].isdigit():
            right += 1

        window = text[left:right]

        # không có keyword lương → bỏ
        anchor_zone = text[max(0, left - 30):right]
        if not any(k in anchor_zone for k in SALARY_INDICATORS):
            continue

        # dính context công ty / funding → bỏ
        if any(k in window for k in NON_SALARY_CONTEXT):
            continue

        # ===== BẮT SỐ =====
        nums = re.findall(r'\d{1,3}(?:[.,]\d{3})+|\d+', window)
        for n in nums:
            v = int(n.replace(".", "").replace(",", ""))
            if v >= 100:
                extracted_values.append(v)

        if nums and detected_currency == NA:
            detected_currency = normalize_currency(cur)

    if not extracted_values:
        return cur_min, cur_max, NA, False

    lo, hi = min(extracted_values), max(extracted_values)

    if hi - lo > 10_000_000:
        return cur_min, cur_max, NA, False

    if not has(cur_min) and not has(cur_max):
        return lo, hi, detected_currency, True

    return cur_min, cur_max, NA, False

# =========================
# PROCESS SINGLE FILE
# =========================

def extract_from_description(file_path: Path):
    print(f"🔄 Extracting description signals: {file_path.name}")

    # load input
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype=str)
    else:
        df = pd.read_excel(file_path, dtype=str)

    if "job_description" not in df.columns:
        print("  ⚠ No job_description column found, skipping")
        return

    df = df.fillna(NA_VALUE)

    filled_city = 0
    filled_country = 0
    filled_remote = 0
    filled_salary = 0
    filled_currency = 0

    for i, row in df.iterrows():
        desc = row["job_description"]
        if desc == NA_VALUE or not isinstance(desc, str):
            continue

        desc_lower = desc.lower()

        # -------- CITY --------
        if "city" in df.columns and df.at[i, "city"] == NA_VALUE:
            city = extract_city(desc_lower)
            if city:
                df.at[i, "city"] = city
                filled_city += 1

        # -------- COUNTRY --------
        if "country" in df.columns and df.at[i, "country"] == NA_VALUE:
            country = extract_country(desc_lower)
            if country:
                df.at[i, "country"] = country
                filled_country += 1

        # -------- REMOTE --------
        if "remote_option" in df.columns and df.at[i, "remote_option"] == NA_VALUE:
            if extract_remote(desc_lower):
                df.at[i, "remote_option"] = "true"
                filled_remote += 1

        # -------- SALARY (+ CURRENCY) --------
        cur_min = df.at[i, "min_salary"]
        cur_max = df.at[i, "max_salary"]

        new_min, new_max, new_currency, filled = extract_salary_from_text(
            desc,
            cur_min,
            cur_max
        )

        if filled:
            df.at[i, "min_salary"] = new_min
            df.at[i, "max_salary"] = new_max

            if "currency" in df.columns and df.at[i, "currency"] == NA_VALUE:
                df.at[i, "currency"] = new_currency

            filled_salary += 1
            filled_currency += 1

    # =========================
    # SAVE OUTPUT (CSV UTF-8-SIG)
    # =========================

    output_name = (
        file_path.name.replace("mapped_", "extracted_desc_", 1)
        if file_path.name.startswith("mapped_")
        else "extracted_desc_" + file_path.name
    )

    output_path = OUTPUT_DIR / output_name.replace(file_path.suffix, ".csv")

    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )

    # =========================
    # LOG SUMMARY
    # =========================

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - City filled from desc    : {filled_city}\n"
        f"    - Country filled from desc : {filled_country}\n"
        f"    - Remote filled from desc  : {filled_remote}\n"
        f"    - Salary filled from desc  : {filled_salary}\n"
        f"    - Currency filled from desc: {filled_currency}\n"
        f"  → Folder saved               : {OUTPUT_DIR}"
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
        extract_from_description(f)

    print("\n=== STEP 2.2 COMPLETED: DESCRIPTION SIGNAL EXTRACTION ===")

if __name__ == "__main__":
    run()