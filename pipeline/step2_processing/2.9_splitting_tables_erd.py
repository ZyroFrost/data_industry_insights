# -*- coding: utf-8 -*-
"""
STEP 2.9 – SPLIT COMBINED DATA INTO ERD TABLES

Input:
- data/data_processing/s2.6_data_combined/*.csv

Output:
- data/data_processed/*.csv (ERD-ready)

Rules:
- Output ALL 9 ERD tables
- Drop __source_id, __source_name
- Drop job if posted_date is NULL or '__NA__'
- Skill is N–N (split by '|')
- job_skills has ONLY (job_id, skill_id)
"""

import pandas as pd
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.8_data_combined"
OUTPUT_DIR = BASE_DIR / "data" / "data_processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# ENUMS (ERD)
# =========================

SKILL_CATEGORIES = {
    'Programming','Data Engineering','Machine Learning','Cloud',
    'Visualization','Database','DevOps','Analytics'
}

COMPANY_SIZES = {'Startup','Small','Medium','Large','Enterprise'}

INDUSTRIES = {
    'Technology','Finance','Banking','Insurance','Healthcare','Education',
    'E-commerce','Manufacturing','Consulting','Government',
    'Telecommunications','Energy','Retail','Logistics','Real Estate'
}

ROLE_ENUM = {
    'Data Analyst','Business Intelligence Analyst','BI Developer',
    'Analytics Engineer','Data Engineer','Data Scientist',
    'Machine Learning Engineer','AI Engineer','AI Researcher',
    'Applied Scientist','Research Engineer','Data Architect',
    'Data Manager','Data Lead'
}

EDUCATION_LEVELS = {'High School','Bachelor','Master','PhD'}
EMPLOYMENT_TYPES = {'Full-time','Part-time','Internship','Temporary'}
JOB_LEVELS = {'Intern','Junior','Mid','Senior','Lead'}
REMOTE_OPTIONS = {'Onsite','Hybrid','Remote'}

# =========================
# LOAD
# =========================

def load_combined():
    files = list(INPUT_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No combined CSV files found")
    dfs = [pd.read_csv(f, encoding="utf-8-sig") for f in files]
    return pd.concat(dfs, ignore_index=True)

# =========================
# RUN
# =========================

def run():
    df = load_combined()
    total_rows = len(df)

    df = df.drop(columns=["__source_id", "__source_name"], errors="ignore")

    # ID MAPS
    skill_map = {}
    company_map = {}
    location_map = {}
    role_map = {}

    # TABLE BUFFERS (9 TABLES)
    skills = []
    skill_aliases = []     # empty but required
    companies = []
    locations = []
    roles = []
    job_postings = []
    job_skills = []
    job_roles = []
    job_levels = []

    # ID COUNTERS
    skill_id = company_id = location_id = role_id = job_id = 1
    dropped_jobs = 0

    for _, r in df.iterrows():

        # =========================
        # DROP INVALID POSTED_DATE
        # =========================
        if pd.isna(r["posted_date"]) or str(r["posted_date"]).strip() == "__NA__":
            dropped_jobs += 1
            continue

        # =========================
        # COMPANY
        # =========================
        if r["company_name"] not in company_map:
            company_map[r["company_name"]] = company_id
            companies.append({
                "company_id": company_id,
                "company_name": r["company_name"],
                "size": r["company_size"] if r["company_size"] in COMPANY_SIZES else "__NA__",
                "industry": r["industry"] if r["industry"] in INDUSTRIES else "__NA__"
            })
            company_id += 1

        # =========================
        # LOCATION
        # =========================
        loc_key = (r["city"], r["country"], r["country_iso"])
        if loc_key not in location_map:
            location_map[loc_key] = location_id
            locations.append({
                "location_id": location_id,
                "city": r["city"] if pd.notna(r["city"]) else "__NA__",
                "country": r["country"] if pd.notna(r["country"]) else "__NA__",
                "country_iso": r["country_iso"] if pd.notna(r["country_iso"]) else "__NA__",
                "latitude": r["latitude"] if pd.notna(r["latitude"]) else None,
                "longitude": r["longitude"] if pd.notna(r["longitude"]) else None,
                "population": r["population"] if pd.notna(r["population"]) else None
            })
            location_id += 1

        # =========================
        # JOB_POSTINGS
        # =========================
        job_postings.append({
            "job_id": job_id,
            "company_id": company_map[r["company_name"]],
            "location_id": location_map[loc_key],
            "posted_date": r["posted_date"],
            "min_salary": r["min_salary"] if pd.notna(r["min_salary"]) else None,
            "max_salary": r["max_salary"] if pd.notna(r["max_salary"]) else None,
            "currency": r["currency"] if pd.notna(r["currency"]) else "__NA__",
            "required_exp_years": r["required_exp_years"] if pd.notna(r["required_exp_years"]) else None,
            "education_level": r["education_level"] if r["education_level"] in EDUCATION_LEVELS else "__NA__",
            "employment_type": r["employment_type"] if r["employment_type"] in EMPLOYMENT_TYPES else "__NA__",
            "job_description": r["job_description"] if pd.notna(r["job_description"]) else None,
            "remote_option": r["remote_option"] if r["remote_option"] in REMOTE_OPTIONS else "__NA__"
        })

        # =========================
        # SKILLS (N–N)
        # =========================
        if pd.notna(r["skill_name"]):
            raw_skills = [s.strip() for s in str(r["skill_name"]).split("|") if s.strip()]
            for skill in raw_skills:

                if skill not in skill_map:
                    skill_map[skill] = skill_id
                    skills.append({
                        "skill_id": skill_id,
                        "skill_name": skill,
                        "skill_category":
                            r["skill_category"]
                            if r["skill_category"] in SKILL_CATEGORIES
                            else "__NA__"
                    })
                    skill_id += 1

                job_skills.append({
                    "job_id": job_id,
                    "skill_id": skill_map[skill]
                })

        # =========================
        # ROLES (N–N)
        # =========================
        if pd.notna(r["role_name"]):
            for role in [x.strip() for x in str(r["role_name"]).split("|")]:
                if role not in ROLE_ENUM:
                    continue
                if role not in role_map:
                    role_map[role] = role_id
                    roles.append({"role_id": role_id, "role_name": role})
                    role_id += 1
                job_roles.append({"job_id": job_id, "role_id": role_map[role]})

        # =========================
        # LEVEL
        # =========================
        if r["level"] in JOB_LEVELS:
            job_levels.append({"job_id": job_id, "level": r["level"]})

        job_id += 1

    # =========================
    # SAVE – ALL 9 TABLES
    # =========================

    pd.DataFrame(skills).to_csv(OUTPUT_DIR / "skills.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(skill_aliases, columns=["alias", "skill_id"]).to_csv(
        OUTPUT_DIR / "skill_aliases.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(companies).to_csv(OUTPUT_DIR / "companies.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(locations).to_csv(OUTPUT_DIR / "locations.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(roles).to_csv(OUTPUT_DIR / "role_names.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_postings).to_csv(OUTPUT_DIR / "job_postings.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_skills).to_csv(OUTPUT_DIR / "job_skills.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_roles).to_csv(OUTPUT_DIR / "job_roles.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_levels).to_csv(OUTPUT_DIR / "job_levels.csv", index=False, encoding="utf-8-sig")

    print(
        f"✓ STEP 2.9 DONE\n"
        f"  - Input rows      : {total_rows}\n"
        f"  - Dropped jobs   : {dropped_jobs}\n\n"
        f"  - job_postings   : {len(job_postings)}\n"
        f"  - skills         : {len(skills)}\n"
        f"  - skill_aliases  : {len(skill_aliases)}\n"
        f"  - job_skills     : {len(job_skills)}\n"
        f"  - companies      : {len(companies)}\n"
        f"  - locations      : {len(locations)}\n"
        f"  - role_names     : {len(roles)}\n"
        f"  - job_roles      : {len(job_roles)}\n"
        f"  - job_levels     : {len(job_levels)}"
    )

if __name__ == "__main__":
    run()
