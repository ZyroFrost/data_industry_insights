# -*- coding: utf-8 -*-
"""
STEP 0.3 – BUILD GEO REFERENCE TABLES (COUNTRIES & CITIES)
(This step supports STEP 2.2 - extracting_description.py, STEP 2.3 - normalizing_city_alias.py, STEP 2.4 - enrich_data_country_city.py)

This script builds standardized geo reference tables used for
reference-based geo enrichment.

It generates:
- A country reference table with ISO code and coordinates
- A city reference table linked to country information

Purpose:
- Provide canonical country and city reference data
- Support country, city, and country_iso enrichment logic

PIPELINE:
STEP 0:
- STEP 0.1 – setup_geonames_reference.py
  → provides raw GeoNames datasets

- STEP 0.2 – build_city_alias_reference.py
  → provides city alias reference (1 → N)

INPUT:
- GeoNames cities dataset (downloaded in STEP 0.1)
- Natural Earth country dataset (downloaded during this step)

OUTPUT:
- data/data_reference/countries.csv
- data/data_reference/cities.csv
"""

import pandas as pd
import geopandas as gpd
import requests
import zipfile
import io
import shutil
from pathlib import Path

# ======================================================
# PATH CONFIG
# ======================================================
ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "data_reference"
OUT_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES_CSV = OUT_DIR / "countries.csv"
CITIES_CSV = OUT_DIR / "cities.csv"

# ======================================================
# GEONAMES (CITY ONLY – KEEP ORIGINAL)
# ======================================================
#CITIES_URL = "https://download.geonames.org/export/dump/cities15000.zip"

# nếu muốn RẤT NHIỀU city (≈ 200k+)
CITIES_URL = "https://download.geonames.org/export/dump/cities500.zip"

# ======================================================
# NATURAL EARTH (COUNTRY ONLY – ONE OFF)
# ======================================================
NE_URL = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
NE_DIR = OUT_DIR / "_tmp_natural_earth"
NE_SHP = NE_DIR / "ne_110m_admin_0_countries.shp"

NE_DIR.mkdir(exist_ok=True)

if not NE_SHP.exists():
    print("⬇️ Downloading Natural Earth countries...")
    r = requests.get(NE_URL, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(NE_DIR)

# ======================================================
# 1. BUILD COUNTRIES (FIXED – EXACT REQUIREMENT)
# ======================================================
print("⬇️ Building countries from Natural Earth...")

world = gpd.read_file(NE_SHP)

# centroid
world["country_latitude"] = world.geometry.centroid.y
world["country_longitude"] = world.geometry.centroid.x

countries_df = world[
    [
        "ISO_A2",
        "NAME",
        "CONTINENT",
        "POP_EST",
        "country_latitude",
        "country_longitude",
    ]
].rename(
    columns={
        "ISO_A2": "country_code",
        "NAME": "country_name",
        "CONTINENT": "continent",
        "POP_EST": "population",
    }
)

# remove invalid ISO
countries_df = countries_df[countries_df["country_code"] != "-99"]
countries_df["country_code"] = countries_df["country_code"].str.upper()

countries_df.to_csv(
    COUNTRIES_CSV,
    index=False,
    encoding="utf-8"
)

print(f"✅ Saved {COUNTRIES_CSV} ({len(countries_df)} countries)")

# cleanup source (one-off seed)
shutil.rmtree(NE_DIR, ignore_errors=True)

# ======================================================
# 2. BUILD CITIES (UNCHANGED)
# ======================================================
print("⬇️ Downloading cities...")

r = requests.get(CITIES_URL, timeout=60)
r.raise_for_status()

z = zipfile.ZipFile(io.BytesIO(r.content))
city_file = z.namelist()[0]

city_cols = [
    "geonameid", "name", "asciiname", "alternatenames",
    "latitude", "longitude", "feature_class", "feature_code",
    "country_code", "cc2", "admin1_code", "admin2_code",
    "admin3_code", "admin4_code", "population",
    "elevation", "dem", "timezone", "modification_date"
]

cities_raw = pd.read_csv(
    z.open(city_file),
    sep="\t",
    names=city_cols,
    dtype=str,
    encoding="utf-8"
)

cities_df = (
    cities_raw[
        [
            "geonameid",
            "asciiname",
            "country_code",
            "latitude",
            "longitude",
            "population",
        ]
    ]
    .rename(columns={"asciiname": "city_name"})
)

# ======================================================
# 3. JOIN COUNTRY NAME (KEEP ORIGINAL BEHAVIOR)
# ======================================================
cities_df = cities_df.merge(
    countries_df[["country_code", "country_name"]],
    on="country_code",
    how="left"
)

# ======================================================
# 4. SAVE CITIES
# ======================================================
cities_df.to_csv(
    CITIES_CSV,
    index=False,
    encoding="utf-8"
)

print(f"✅ Saved {CITIES_CSV} ({len(cities_df)} cities)")
print("🎉 DONE: Geo reference built successfully")