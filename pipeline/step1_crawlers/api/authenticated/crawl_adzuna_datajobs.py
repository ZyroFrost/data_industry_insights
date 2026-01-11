import os
import json
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# =========================================================
# 0. LOAD ENV
# =========================================================
load_dotenv()
API_KEY = os.getenv("ADZUNA_API_KEY")
APP_ID = os.getenv("ADZUNA_APP_ID")

# =========================================================
# CHUẨN HÓA PATH — FIX HOÀN TOÀN LỖI SAVE FILE RA NGOÀI
# =========================================================
# File này nằm tại:
# pipeline/crawlers/global_api/adzuna_crawler.py
# → parents[2] = root project "data_industry_insights_2020-2025"

ROOT = Path(__file__).resolve().parents[4]

RAW_DIR = ROOT / "data" / "data_raw" / "adzuna_datajobs_2025"
PROCESSING_DIR = ROOT / "data" / "data_processing" / "s2.0_data_extracted"
METADATA_DIR = ROOT / "data" / "metadata" / "source"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================
# 1. LIST 40 COUNTRIES
# =========================================================
COUNTRIES_40 = [
    "gb","us","ca","au","nz","sg","in","za","br","mx",
    "fr","de","es","it","nl","se","no","fi","dk","pl",
    "be","pt","at","ch","ie","cz","hu","ro","sk","si",
    "lt","lv","ee","jp","kr","hk","ar","cl","co","vn"
]

# =========================================================
# 2. CHECK IF COUNTRY ALREADY CRAWLED
# =========================================================
def get_existing_pages_and_jobs(country):
    """
    Return:
    - existing_pages: set[int]
    - job_ids: set[str]
    """
    country_dir = RAW_DIR / country

    if not country_dir.exists():
        return set(), set()

    json_files = list(country_dir.glob("page_*.json"))
    existing_pages = set()
    job_ids = set()

    for file in json_files:
        try:
            page_num = int(file.stem.split("_")[1])
            existing_pages.add(page_num)

            data = json.loads(file.read_text(encoding="utf-8"))
            for job in data.get("results", []):
                job_id = job.get("id")
                if job_id:
                    job_ids.add(job_id)
        except Exception:
            continue

    return existing_pages, job_ids

# =========================================================
# 3. CRAWL 1 COUNTRY
# =========================================================
def crawl_country(country, pages=200):
    print(f"\n🌍 Crawling: {country}")
    save_dir = RAW_DIR / country
    save_dir.mkdir(exist_ok=True)

    # 🔹 Load existing pages + job ids
    existing_pages, existing_job_ids = get_existing_pages_and_jobs(country)
    seen_job_ids = set(existing_job_ids)

    if existing_pages:
        start_page = max(existing_pages) + 1
        if start_page > pages:
            print(f"   ✔ Already crawled full {pages} pages. Skipping.")
            return
        print(f"   ↳ Resume crawl from page {start_page}")
    else:
        start_page = 1
        print("   ↳ First time crawling this country")

    # 🔹 Crawl pages
    for page in range(start_page, pages + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            "app_id": APP_ID,
            "app_key": API_KEY,
            "results_per_page": 50,
            "what": "data",
        }

        response = requests.get(url, params=params)
        try:
            data = response.json()
        except Exception:
            print(f"❌ ERROR parsing JSON at page {page}, country {country}")
            break

        results = data.get("results", [])
        if not results:
            print(f"→ No more results at page {page}.")
            break

        new_results = []
        skipped = 0

        for job in results:
            job_id = job.get("id")

            if not job_id or job_id in seen_job_ids:
                skipped += 1
                continue

            seen_job_ids.add(job_id)
            new_results.append(job)

        if not new_results:
            print(f"   ⏭ Page {page}: all jobs duplicated, skipped.")
            continue

        data["results"] = new_results

        save_path = save_dir / f"page_{page}.json"
        save_path.write_text(json.dumps(data, indent=4), encoding="utf-8")

        print(
            f"   ✓ Saved page {page} "
            f"(new: {len(new_results)}, skipped: {skipped})"
        )

    print(f"✅ DONE {country}")

# =========================================================
# 4. NORMALIZE 1 JOB
# =========================================================
def normalize_job(job):
    loc = job.get("location", {})
    area = loc.get("area", [])

    return {
        "job_id": job.get("id"),
        "title": job.get("title"),
        "company_name": job.get("company", {}).get("display_name"),
        "category": job.get("category", {}).get("label"),
        "category_tag": job.get("category", {}).get("tag"),
        "country": area[0] if len(area) > 0 else None,
        "region": area[1] if len(area) > 1 else None,
        "city": area[-1] if len(area) > 0 else None,
        "location_display": loc.get("display_name"),
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        "salary_predicted": job.get("salary_is_predicted"),
        "contract_type": job.get("contract_type"),
        "contract_time": job.get("contract_time"),
        "created_at": job.get("created"),
        "latitude": job.get("latitude"),
        "longitude": job.get("longitude"),
        "description": job.get("description"),
        "redirect_url": job.get("redirect_url")
    }


# =========================================================
# 5. EXPORT METADATA → data/metadata/adzuna
# =========================================================
def export_metadata(example_json):
    schema = {k: type(v).__name__ for k, v in example_json.items()}
    meta_file = METADATA_DIR / "adzuna_metadata.json"

    meta_file.write_text(json.dumps(schema, indent=4), encoding="utf-8")
    print(f"📄 Metadata exported → {meta_file}")


# =========================================================
# 6. FLATTEN JSON → CSV
# =========================================================
def flatten_all_countries():
    print("\n📌 Flattening ALL countries into processing layer...")

    output_path = PROCESSING_DIR / "adzuna_datajobs_2025.csv"
    rows, example_saved = [], False

    for country_dir in RAW_DIR.iterdir():
        if not country_dir.is_dir():
            continue

        country = country_dir.name
        print(f"🔍 Flattening {country}...")

        json_files = sorted(country_dir.glob("page_*.json"))

        for file in json_files:
            data = json.loads(file.read_text(encoding="utf-8"))

            for job in data.get("results", []):
                row = normalize_job(job)
                row["country_source"] = country
                rows.append(row)

                if not example_saved:
                    export_metadata(job)
                    example_saved = True

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"\n🎉 DONE! Saved {len(df)} rows → {output_path}")
    return df

# =========================================================
# 7. FULL GLOBAL RUN
# =========================================================
def run_adzuna_datajobs_crawler():
    print("\n🚀 RUNNING GLOBAL ADZUNA PIPELINE...\n")

    for country in COUNTRIES_40:
        crawl_country(country)

    flatten_all_countries()

    print("\n🎯 GLOBAL PIPELINE FINISHED — DATA READY.\n")

# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    run_adzuna_datajobs_crawler()
    # print(RAW_DIR)
    # print(PROCESSING_DIR)
    # print(METADATA_DIR)