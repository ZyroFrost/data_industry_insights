# -*- coding: utf-8 -*-
"""
STEP 0.2 – BUILD CITY ALIAS REFERENCE (1 → N)
(This step supports STEP 0.3 – build_geo_reference.py, STEP 2.2 - extracting_description.py, STEP 2.3 - normalizing_city_alias.py, STEP 2.4 - enrich_data_country_city.py)

This script builds a city alias reference table, mapping
ONE canonical city name (English, GeoNames standard)
to MULTIPLE alternative names, including:
- Multi-language names
- Exonyms / historical names
- Job-market usage (districts, metro areas, common variants)

Purpose:
- Normalize heterogeneous city values from real-world job data
- Enable robust city matching across different languages and usages
- Support reference-based geo enrichment (country / city resolution)

PIPELINE CONTEXT:
STEP 0:
- STEP 0.1 – setup_geonames_reference.py
  → provides raw GeoNames data:
    data/data_reference/geonames_raw/alternateNamesV2.txt

INPUT:
- data/data_reference/cities.csv
  (Canonical city list, built from GeoNames cities dataset)
- data/data_reference/geonames_raw/alternateNamesV2.txt
  (Raw alternate names from GeoNames)

OUTPUT:
- data/data_reference/city_alias_reference.csv
  (Canonical city → all known aliases)
"""

import pandas as pd
import unicodedata
import re
from pathlib import Path
from collections import defaultdict

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]
REF_DIR = BASE_DIR / "data" / "data_reference"

CITIES_PATH = REF_DIR / "cities.csv"
ALT_NAMES_TXT_PATH = REF_DIR / "geonames_raw" / "alternateNamesV2.txt"
OUTPUT_PATH = REF_DIR / "city_alias_reference.csv"

# =========================
# GUARD CHECK
# =========================

if not CITIES_PATH.exists():
    raise FileNotFoundError(f"Missing input file: {CITIES_PATH}")

if not ALT_NAMES_TXT_PATH.exists():
    raise FileNotFoundError(
        f"Missing GeoNames raw file: {ALT_NAMES_TXT_PATH}\n"
        "Please run: pipeline/seeds/setup_geonames_reference.py first"
    )

# =========================
# NORMALIZATION
# =========================

def normalize_text(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = unicodedata.normalize("NFKD", x)
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = re.sub(r"[^\w\s]", "", x)
    return re.sub(r"\s+", " ", x).strip().upper()

# =========================
# LOAD DATA
# =========================

# cities.csv (canonical cities)
cities_df = pd.read_csv(CITIES_PATH, dtype=str)

# GeoNames alternateNamesV2.txt
alt_cols = [
    "alternateNameId",
    "geonameid",
    "isolanguage",
    "alternate_name",
    "isPreferredName",
    "isShortName",
    "isColloquial",
    "isHistoric",
    "from",
    "to"
]

alt_df = pd.read_csv(
    ALT_NAMES_TXT_PATH,
    sep="\t",
    names=alt_cols,
    dtype=str,
    low_memory=False
)

# =========================
# BUILD BASE LOOKUP
# =========================

# geoname_id → canonical city (normalized English)
geoname_to_city = {
    str(r.geonameid): normalize_text(r.city_name)
    for r in cities_df.itertuples()
    if pd.notna(r.geonameid) and pd.notna(r.city_name)
}

# canonical city → aliases
city_alias_map = defaultdict(set)

# =========================
# 1️⃣ SELF CANONICAL
# =========================

for city in geoname_to_city.values():
    city_alias_map[city].add(city)

# =========================
# 2️⃣ GEONAMES ALTERNATE NAMES (RAW TXT)
# =========================

for r in alt_df.itertuples():
    gid = str(r.geonameid)

    if gid not in geoname_to_city:
        continue

    alias = normalize_text(r.alternate_name)
    if not alias:
        continue

    canonical = geoname_to_city[gid]

    if alias != canonical:
        city_alias_map[canonical].add(alias)

# =========================
# 3️⃣ COMMON EXONYMS (CURATED)
# =========================

COMMON_EXONYMS = {
    "BEIJING": ["PEKING", "PEKIN"],
    "MUNICH": ["MUENCHEN", "MUNCHEN"],
    "COLOGNE": ["KOLN"],
    "VIENNA": ["WIEN"],
    "PRAGUE": ["PRAHA"],
    "FLORENCE": ["FIRENZE"],
    "GENOA": ["GENOVA"],
    "NAPLES": ["NAPOLI"],
    "MILAN": ["MILANO"],
    "ROME": ["ROMA"],
    "MOSCOW": ["MOSKVA"],
}

for canonical, aliases in COMMON_EXONYMS.items():
    for a in aliases:
        city_alias_map[canonical].add(normalize_text(a))

# =========================
# 4️⃣ JOB-MARKET DISTRICT → METRO (OPTIONAL)
# =========================

JOB_ALIAS = {
    "JOHANNESBURG": ["SANDTON"],
    "LONDON": ["CANARY WHARF"],
    "PARIS": ["LA DEFENSE"],
    "BANGALORE": ["WHITEFIELD"],
    "NEW YORK": ["MANHATTAN", "BROOKLYN"],
}

for canonical, aliases in JOB_ALIAS.items():
    for a in aliases:
        city_alias_map[canonical].add(normalize_text(a))

# =========================
# EXPORT TO CSV (UTF-8-SIG)
# =========================

rows = []
for canonical, aliases in city_alias_map.items():
    for alias in sorted(aliases):
        rows.append({
            "canonical_city": canonical,
            "alias": alias
        })

out_df = pd.DataFrame(rows)

out_df.to_csv(
    OUTPUT_PATH,
    index=False,
    encoding="utf-8-sig"
)

print(
    f"✓ City alias reference built successfully\n"
    f"  - Output file       : {OUTPUT_PATH}\n"
    f"  - Canonical cities  : {out_df['canonical_city'].nunique()}\n"
    f"  - Total aliases     : {len(out_df)}"
)