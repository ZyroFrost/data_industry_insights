# -*- coding: utf-8 -*-
"""
Build geo reference data (countries & cities) from GeoNames

Outputs:
- data/data_reference/countries.csv
- data/data_reference/cities.csv

Fixes:
- UTF-8 mojibake issue (Excel-compatible)
- Use asciiname for stable city names
"""

import pandas as pd
import requests
import zipfile
import io
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
# GEONAMES URLS
# ======================================================
COUNTRY_URL = "https://download.geonames.org/export/dump/countryInfo.txt"

# >= 15k population (≈ 24k cities)
CITIES_URL = "https://download.geonames.org/export/dump/cities15000.zip"

# nếu muốn RẤT NHIỀU city (≈ 200k+)
# CITIES_URL = "https://download.geonames.org/export/dump/cities500.zip"

# ======================================================
# 1. BUILD COUNTRIES
# ======================================================
print("⬇️ Downloading countries...")

country_cols = [
    "ISO", "ISO3", "ISO-Numeric", "fips", "Country",
    "Capital", "Area", "Population", "Continent",
    "tld", "CurrencyCode", "CurrencyName", "Phone",
    "PostalCodeFormat", "PostalCodeRegex",
    "Languages", "geonameid", "Neighbours", "EquivalentFipsCode"
]

countries_raw = pd.read_csv(
    COUNTRY_URL,
    sep="\t",
    comment="#",
    names=country_cols,
    dtype=str,
    encoding="utf-8"
)

countries_df = (
    countries_raw[[
        "ISO",
        "Country",
        "Continent",
        "Population"
    ]]
    .rename(columns={
        "ISO": "country_code",
        "Country": "country_name",
        "Continent": "continent",
        "Population": "population"
    })
)

countries_df.to_csv(
    COUNTRIES_CSV,
    index=False,
    encoding="utf-8-sig"   # 👈 Excel-safe
)

print(f"✅ Saved {COUNTRIES_CSV} ({len(countries_df)} countries)")

# ======================================================
# 2. BUILD CITIES
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

# 👉 DÙNG asciiname để tránh lỗi Unicode
cities_df = (
    cities_raw[[
        "asciiname",
        "country_code",
        "latitude",
        "longitude",
        "population"
    ]]
    .rename(columns={
        "asciiname": "city_name"
    })
)

# ======================================================
# 3. JOIN COUNTRY NAME
# ======================================================
cities_df = cities_df.merge(
    countries_df[["country_code", "country_name"]],
    on="country_code",
    how="left"
)

# ======================================================
# 4. SAVE
# ======================================================
cities_df.to_csv(
    CITIES_CSV,
    index=False,
    encoding="utf-8-sig"   # 👈 Excel-safe
)

print(f"✅ Saved {CITIES_CSV} ({len(cities_df)} cities)")
print("🎉 DONE: Geo reference built successfully")