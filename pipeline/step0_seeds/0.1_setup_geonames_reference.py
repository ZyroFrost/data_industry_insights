# -*- coding: utf-8 -*-
"""
STEP 0.1 – SETUP GEONAMES REFERENCE (DOWNLOAD ONLY)
(This step supports STEP 0.2 – build_city_alias_reference.py)

This script downloads raw GeoNames reference files and stores them
INSIDE data/data_reference/geonames_raw, exactly as external raw sources.

Downloaded files:
- cities15000.txt
- alternateNamesV2.txt
- iso-languagecodes.txt

Source:
https://download.geonames.org/export/dump/
"""

import requests
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]
REF_DIR = BASE_DIR / "data" / "data_reference"
GEONAMES_RAW_DIR = REF_DIR / "geonames_raw"

GEONAMES_RAW_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# GEONAMES URLS
# =========================

GEONAMES_BASE_URL = "https://download.geonames.org/export/dump"

FILES = {
    "cities15000.txt": f"{GEONAMES_BASE_URL}/cities15000.zip",
    "alternateNamesV2.txt": f"{GEONAMES_BASE_URL}/alternateNamesV2.zip",
}

# =========================
# DOWNLOAD + EXTRACT
# =========================

def download_and_extract(name: str, url: str):
    zip_path = GEONAMES_RAW_DIR / f"{name}.zip"
    out_path = GEONAMES_RAW_DIR / name

    if out_path.exists():
        print(f"✓ Already exists, skipping: {out_path.name}")
        return

    print(f"⬇ Downloading: {name}")
    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    import zipfile
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(GEONAMES_RAW_DIR)

    zip_path.unlink()  # remove zip after extract
    print(f"✓ Saved: {out_path.name}")

# =========================
# RUN
# =========================

def run():
    print("=== SETUP GEONAMES REFERENCE ===")
    for name, url in FILES.items():
        download_and_extract(name, url)
        print("Location path:", GEONAMES_RAW_DIR)
    print("=== DONE ===")

if __name__ == "__main__":
    run()