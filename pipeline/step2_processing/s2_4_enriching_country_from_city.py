# -*- coding: utf-8 -*-
"""
STEP 2.4 – REFERENCE-BASED GEO ENRICHMENT (COUNTRY, COUNTRY_ISO)

This step enriches geographic fields using reference tables only.
It standardizes and validates country, country_iso, latitude, longitude,
and population values after city normalization is completed.

IMPORTANT PRINCIPLES:
1. Reference-based only (NO guessing, NO external API calls)
2. NEVER override valid structured data with inferred values
3. City is used only as lookup key (already normalized in STEP 2.3)
4. Unmatched values are explicitly tracked for manual review
"""

import pandas as pd
import unicodedata
import re
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.3_data_values_normalized"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.4_data_country_enriched"
REF_DIR = BASE_DIR / "data" / "data_reference"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOAD REFERENCES
# =========================

cities_df = pd.read_csv(REF_DIR / "cities.csv", dtype=str)
countries_df = pd.read_csv(REF_DIR / "countries.csv", dtype=str)

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
# LOOKUPS
# =========================

CITY_LOOKUP = {
    normalize_text(r.city_name): {
        "country": r.country_name,
        "country_iso": r.country_code
    }
    for r in cities_df.itertuples()
}

COUNTRY_GEO_LOOKUP = {
    normalize_text(r.country_name): {
        "country": r.country_name,
        "country_iso": r.country_code,
        "latitude": r.country_latitude,
        "longitude": r.country_longitude,
        "population": r.population
    }
    for r in countries_df.itertuples()
}

ISO_TO_COUNTRY = {
    str(r.country_code).upper(): r.country_name
    for r in countries_df.itertuples()
    if pd.notna(r.country_code)
}

# =========================
# ENRICH FILE
# =========================

def enrich_file(file_path: Path):
    print(f"🔄 Enriching: {file_path.name}")

    df_raw = pd.read_csv(file_path, dtype=str, encoding="utf-8-sig")

    for col in ["city", "country", "country_iso", "latitude", "longitude", "population"]:
        if col not in df_raw.columns:
            df_raw[col] = "__NA__"
        else:
            df_raw[col] = df_raw[col].fillna("__NA__")

    SOURCE_ID_COL = next((c for c in ["id", "job_id", "source_id"] if c in df_raw.columns), None)

    total_rows = len(df_raw)

    country_before = df_raw["country"]
    iso_before = df_raw["country_iso"]
    lat_before = df_raw["latitude"]
    lon_before = df_raw["longitude"]
    pop_before = df_raw["population"]

    country_norm = country_before.apply(normalize_text)
    iso_norm = iso_before.str.upper()

    # ==================================================
    # BASE VALID MASKS (EXCLUDE __NA__ & __INVALID__)
    # ==================================================

    valid_country_mask = (
        (country_before != "__NA__") &
        (country_before != "__INVALID__")
    )

    valid_iso_mask = (
        (iso_before != "__NA__") &
        (iso_before != "__INVALID__")
    )

    valid_geo_mask = valid_country_mask
    valid_population_mask = valid_country_mask

    # =========================
    # INITIAL AUDIT
    # =========================

    country_empty = (country_before == "__NA__").sum()
    iso_empty = (iso_before == "__NA__").sum()
    pop_empty = (pop_before == "__NA__").sum()

    country_right = (
        valid_country_mask &
        country_norm.isin(COUNTRY_GEO_LOOKUP)
    ).sum()

    country_wrong = (
        valid_country_mask &
        ~country_norm.isin(COUNTRY_GEO_LOOKUP)
    ).sum()

    iso_right = (
        valid_iso_mask &
        iso_norm.isin(ISO_TO_COUNTRY)
    ).sum()

    iso_wrong = (
        valid_iso_mask &
        ~iso_norm.isin(ISO_TO_COUNTRY)
    ).sum()

    pop_right = (
        valid_population_mask &
        (pop_before != "__NA__")
    ).sum()

    pop_wrong = (
        valid_population_mask &
        (pop_before == "__NA__")
    ).sum()

    # ==================================================
    # BASE AUDIT MASK (EXCLUDE __NA__ & __INVALID__)
    # ==================================================

    base_audit_mask = (
        (df_raw["city"] != "__NA__") &
        (df_raw["city"] != "__INVALID__")
    )

    # =========================
    # ENRICH
    # =========================

    df = df_raw.copy()
    city_norm = df["city"].apply(normalize_text)

    for i, c in city_norm.items():
        if c in CITY_LOOKUP:
            df.at[i, "country"] = CITY_LOOKUP[c]["country"]
            df.at[i, "country_iso"] = CITY_LOOKUP[c]["country_iso"]

    for i, iso in df["country_iso"].str.upper().items():
        if iso in ISO_TO_COUNTRY:
            df.at[i, "country"] = ISO_TO_COUNTRY[iso]

    country_after_norm = df["country"].apply(normalize_text)

    for i, c in country_after_norm.items():
        if c in COUNTRY_GEO_LOOKUP:
            geo = COUNTRY_GEO_LOOKUP[c]
            df.at[i, "country"] = geo["country"]
            df.at[i, "country_iso"] = geo["country_iso"]
            df.at[i, "latitude"] = geo["latitude"]
            df.at[i, "longitude"] = geo["longitude"]
            df.at[i, "population"] = geo["population"]

    # =========================
    # POST AUDIT
    # =========================

    country_after_hit = country_after_norm.isin(COUNTRY_GEO_LOOKUP)
    iso_after_hit = df["country_iso"].str.upper().isin(ISO_TO_COUNTRY)

    country_enriched = (
        valid_country_mask &
        ~country_norm.isin(COUNTRY_GEO_LOOKUP) &
        country_after_hit
    ).sum()

    iso_enriched = (
        valid_iso_mask &
        ~iso_norm.isin(ISO_TO_COUNTRY) &
        iso_after_hit
    ).sum()

    country_unmatched = country_wrong - country_enriched
    iso_unmatched = iso_wrong - iso_enriched

    geo_before_ok = (
        valid_geo_mask &
        (lat_before != "__NA__") &
        (lon_before != "__NA__")
    )

    geo_after_ok = (
        valid_geo_mask &
        (df["latitude"] != "__NA__") &
        (df["longitude"] != "__NA__")
    )

    geo_coord_right = geo_before_ok.sum()
    geo_coord_empty = valid_geo_mask.sum() - geo_coord_right
    geo_coord_wrong = geo_coord_empty
    geo_coord_enriched = (~geo_before_ok & geo_after_ok).sum()
    geo_coord_unmatched = geo_coord_wrong - geo_coord_enriched

    pop_enriched = (
        valid_population_mask &
        (pop_before == "__NA__") &
        (df["population"] != "__NA__")
    ).sum()

    pop_unmatched = pop_wrong - pop_enriched

    # =========================
    # UNMATCHED FILE (ANY GEO FIELD FAILED)
    # =========================

    country_unmatched_mask = (
        base_audit_mask &
        ~country_norm.isin(COUNTRY_GEO_LOOKUP) &
        ~country_after_hit
    )

    iso_unmatched_mask = (
        base_audit_mask &
        ~iso_norm.isin(ISO_TO_COUNTRY) &
        ~iso_after_hit
    )

    geo_unmatched_mask = (
        base_audit_mask &
        (
            (df["latitude"] == "__NA__") |
            (df["longitude"] == "__NA__")
        )
    )

    population_unmatched_mask = (
        base_audit_mask &
        (df["population"] == "__NA__")
    )

    # FINAL UNMATCH = ANY FAILED
    unmatched_mask = (
        country_unmatched_mask |
        iso_unmatched_mask |
        geo_unmatched_mask |
        population_unmatched_mask
    )

    unmatched_geo = df.loc[
        unmatched_mask,
        ["city", "country", "country_iso", "latitude", "longitude", "population"]
    ].copy()

    if not unmatched_geo.empty:
        unmatched_geo.insert(0, "__source_name", file_path.name)

        if SOURCE_ID_COL:
            unmatched_geo.insert(
                1,
                "__source_id",
                df.loc[unmatched_mask, SOURCE_ID_COL].astype(str).values
            )
        else:
            unmatched_geo.insert(
                1,
                "__source_id",
                unmatched_geo.index.astype(str)
            )

    # =========================
    # SAVE
    # =========================

    clean_name = file_path.name.replace("normalized_", "", 1)
    output_path = OUTPUT_DIR / f"enriched_{clean_name}"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - Total rows                   : {total_rows}\n"
        f"    - Country right                : {country_right}\n"
        f"      + Country empty (__NA__)     : {country_empty}\n"
        f"      + Country wrong              : {country_wrong}\n"
        f"      + Country enriched           : {country_enriched}\n"
        f"      + Country unmatched !        : {country_unmatched}\n"
        f"    - Country ISO right            : {iso_right}\n"
        f"      + Country ISO empty (__NA__) : {iso_empty}\n"
        f"      + Country ISO wrong          : {iso_wrong}\n"
        f"      + Country ISO enriched       : {iso_enriched}\n"
        f"      + Country ISO unmatched !    : {iso_unmatched}\n"
        f"    - Country lat-lon right        : {geo_coord_right}\n"
        f"      + lat-lon empty (__NA__)     : {geo_coord_empty}\n"
        f"      + lat-lon wrong              : {geo_coord_wrong}\n"
        f"      + lat-lon enriched           : {geo_coord_enriched}\n"
        f"      + lat-lon unmatched !        : {geo_coord_unmatched}\n"
        f"    - Population right             : {pop_right}\n"
        f"      + Population empty (__NA__)  : {pop_empty}\n"
        f"      + Population wrong           : {pop_wrong}\n"
        f"      + Population enriched        : {pop_enriched}\n"
        f"      + Population unmatched !     : {pop_unmatched}"
    )

    return unmatched_geo

# =========================
# RUN
# =========================

def run(target_files: list = None):
    if target_files is None:
        files = list(INPUT_DIR.iterdir())
    else:
        files = target_files
        
    all_unmatched = []

    unmatched_path = REF_DIR / "unmatched_city_country.csv"
    if unmatched_path.exists():
        unmatched_path.unlink()

    for f in files:
        unmatched = enrich_file(f)
        if unmatched is not None and not unmatched.empty:
            all_unmatched.append(unmatched)
        
        if target_files is not None and len(target_files) == 1:
            return

    if all_unmatched:
        pd.concat(all_unmatched, ignore_index=True).drop_duplicates().to_csv(
            unmatched_path, index=False, encoding="utf-8-sig"
        )
    print(
        f"→ Folder saved: {unmatched_path.parent}"
    )

    print("\n=== STEP 2.4 COMPLETED ===")

if __name__ == "__main__":
    run()