
# Steps:
# 1. Load enriched job datasets
# 2. Normalize & standardize role_name using predefined taxonomy + rules
# 3. Save final datasets and generate summary report

# Input:
# - Enriched CSVs:
#   data/data_processing/data_enriched/*_enriched.csv

# Output:
# - Final CSVs:
#   data/data_processing/data_enriched/*_final.csv
# - Normalization report:
#   data/reports/role_normalization_report.txt

# How to run:
# - python <script_name>.py

# Result:
# - Standardized role names + role distribution & unmatched roles report

import pandas as pd
import os
import re
import unicodedata
from collections import Counter

# =========================
# PATHS
# =========================

BASE_DIR = os.getcwd()

INPUT_DIR = os.path.join(BASE_DIR, "data/data_processing/data_enriched")
OUTPUT_DIR = os.path.join(BASE_DIR, "data/data_processing/data_enriched")

# Put report in the same folder as output files
REPORT_DIR = OUTPUT_DIR

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# NORMALIZATION
# =========================

def normalize_role(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# =========================
# ROLE TAXONOMY (OPTIMIZED)
# =========================

def create_role_taxonomy():
    return {

        # -------------------------
        # SOFTWARE / ENGINEERING
        # -------------------------
        "SOFTWARE ENGINEER": [
            "SOFTWARE ENGINEER", "SOFTWARE DEVELOPER",
            "STAFF SOFTWARE ENGINEER", "PRINCIPAL SOFTWARE ENGINEER",
            "ENGINEER I", "ENGINEER II", "ENGINEER III"
        ],

        "FRONTEND DEVELOPER": [
            "FRONTEND DEVELOPER", "FRONT END DEVELOPER",
            "REACT DEVELOPER", "VUE DEVELOPER", "ANGULAR DEVELOPER"
        ],

        "BACKEND DEVELOPER": [
            "BACKEND DEVELOPER", "BACK END DEVELOPER",
            "JAVA DEVELOPER", "PYTHON DEVELOPER", "NODEJS DEVELOPER"
        ],

        "FULLSTACK DEVELOPER": [
            "FULLSTACK DEVELOPER", "FULL STACK DEVELOPER"
        ],

        "DEVOPS ENGINEER": [
            "DEVOPS ENGINEER", "SRE", "SITE RELIABILITY ENGINEER"
        ],

        "QA ENGINEER": [
            "QA ENGINEER", "QA TESTER", "TEST ENGINEER"
        ],

        # -------------------------
        # DATA CORE
        # -------------------------
        "DATA ENGINEER": [
            "DATA ENGINEER", "BIG DATA ENGINEER", "ETL ENGINEER"
        ],

        "DATA SCIENTIST": [
            "DATA SCIENTIST", "SENIOR DATA SCIENTIST"
        ],

        "DATA ANALYST": [
            "DATA ANALYST", "BUSINESS DATA ANALYST"
        ],

        "ANALYTICS ENGINEER": [
            "ANALYTICS ENGINEER"
        ],

        "DATA ARCHITECT": [
            "DATA ARCHITECT", "DATA SOLUTION ARCHITECT"
        ],

        "APPLIED SCIENTIST": [
            "APPLIED SCIENTIST"
        ],

        "RESEARCH ENGINEER": [
            "RESEARCH ENGINEER"
        ],

        "MACHINE LEARNING ENGINEER": [
            "MACHINE LEARNING ENGINEER", "ML ENGINEER", "MLOPS ENGINEER"
        ],

        "AI RESEARCHER": [
            "AI RESEARCHER", "RESEARCH SCIENTIST"
        ],

        # -------------------------
        # MANAGEMENT / LEADERSHIP
        # -------------------------
        "ENGINEERING MANAGER": [
            "ENGINEERING MANAGER", "TECHNICAL MANAGER"
        ],

        "DATA MANAGER": [
            "DATA MANAGER", "DATA ANALYTICS MANAGER"
        ],

        "DATA LEAD": [
            "DATA LEAD", "HEAD OF DATA"
        ],

        "PROJECT MANAGER": [
            "PROJECT MANAGER"
        ],

        "PRODUCT MANAGER": [
            "PRODUCT MANAGER", "PRODUCT OWNER"
        ],

        "BUSINESS ANALYST": [
            "BUSINESS ANALYST"
        ],

        # -------------------------
        # CLOUD / SECURITY
        # -------------------------
        "CLOUD ENGINEER": [
            "CLOUD ENGINEER", "AWS ENGINEER", "AZURE ENGINEER"
        ],

        "SECURITY ENGINEER": [
            "SECURITY ENGINEER", "CYBER SECURITY ENGINEER"
        ],

        # -------------------------
        # INTERN
        # -------------------------
        "DATA INTERN": [
            "DATA ANALYTICS INTERN", "DATA SCIENCE INTERN", "DATA INTERN"
        ]
    }

# =========================
# BUILD ROLE MAP
# =========================

def build_role_mapping():
    role_map = {}
    for standard, variants in create_role_taxonomy().items():
        for v in variants:
            role_map[normalize_role(v)] = standard
    return role_map

# =========================
# STANDARDIZE ROLE
# =========================

def standardize_role_name(role, role_map):
    if pd.isna(role) or str(role).strip() == "":
        return role

    norm = normalize_role(role)

    # Exact match
    if norm in role_map:
        return role_map[norm]

    # Partial match
    for k, v in role_map.items():
        if k in norm or norm in k:
            return v

    # Keyword rules
    keyword_rules = [
        (r"\bSOFTWARE\s+ENGINEER\b|\bSOFTWARE\s+DEVELOPER\b", "SOFTWARE ENGINEER"),
        (r"\bDATA\s+ENGINEER\b", "DATA ENGINEER"),
        (r"\bDATA\s+SCIENTIST\b", "DATA SCIENTIST"),
        (r"\bDATA\s+ANALYST\b", "DATA ANALYST"),
        (r"\bANALYTICS\s+ENGINEER\b", "ANALYTICS ENGINEER"),
        (r"\bDATA\s+ARCHITECT\b", "DATA ARCHITECT"),
        (r"\bAPPLIED\s+SCIENTIST\b", "APPLIED SCIENTIST"),
        (r"\bRESEARCH\s+ENGINEER\b", "RESEARCH ENGINEER"),
        (r"\bMACHINE\s+LEARNING\b|\bML\b(?!OPS)", "MACHINE LEARNING ENGINEER"),
        (r"\bMLOPS\b", "MACHINE LEARNING ENGINEER"),
        (r"\bHEAD\s+OF\s+DATA\b|\bDATA\s+LEAD\b", "DATA LEAD"),
        (r"\bDATA\s+MANAGER\b", "DATA MANAGER"),
        (r"\bENGINEERING\s+MANAGER\b", "ENGINEERING MANAGER"),
        (r"\bDATA\s+.*INTERN\b", "DATA INTERN"),
    ]

    for pattern, std in keyword_rules:
        if re.search(pattern, norm):
            return std

    return role

# =========================
# REPORT
# =========================

def generate_report(stats):
    path = os.path.join(REPORT_DIR, "role_normalization_report.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("="*60 + "\nROLE NORMALIZATION REPORT\n" + "="*60 + "\n\n")
        f.write(f"Total files processed: {stats['total_files']}\n")
        f.write(f"Total rows processed: {stats['total_rows']:,}\n")
        f.write(f"Roles normalized: {stats['normalized']:,}\n")
        f.write(f"Roles unchanged: {stats['unchanged']:,}\n")
        f.write(f"Normalization rate: {stats['normalized']/stats['total_rows']*100:.1f}%\n\n")

        f.write("="*60 + "\nTOP 20 STANDARDIZED ROLES\n" + "="*60 + "\n\n")
        for i, (r, c) in enumerate(stats["role_distribution"].most_common(20), 1):
            f.write(f"{i:2d}. {r:35s} {c:6,d}\n")

        f.write("\n" + "="*60 + "\nUNMATCHED ROLES (Top 50)\n" + "="*60 + "\n\n")
        for i, (r, c) in enumerate(stats["unmatched_roles"].most_common(50), 1):
            f.write(f"{i:2d}. {r} ({c})\n")

    print(f"\n📊 Report saved: {path}")

# =========================
# MAIN
# =========================

def main():
    print("="*60)
    print("STARTING ROLE NAME STANDARDIZATION")
    print("="*60 + "\n")

    role_map = build_role_mapping()
    print(f"✓ Role taxonomy loaded: {len(role_map)} variants mapped to {len(create_role_taxonomy())} standard roles\n")

    stats = {
        "total_files": 0,
        "total_rows": 0,
        "normalized": 0,
        "unchanged": 0,
        "role_distribution": Counter(),
        "unmatched_roles": Counter()
    }

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith("_enriched.csv")]

    for file in files:
        print(f"📄 Processing: {file}")
        df = pd.read_csv(os.path.join(INPUT_DIR, file))

        if "role_name" not in df.columns:
            print("  ⚠️ role_name column not found, skipping.")
            continue

        stats["total_files"] += 1
        stats["total_rows"] += len(df)

        original = df["role_name"].copy()
        df["role_name"] = df["role_name"].apply(lambda x: standardize_role_name(x, role_map))

        changed = (original != df["role_name"]).sum()
        stats["normalized"] += changed
        stats["unchanged"] += len(df) - changed
        stats["role_distribution"].update(df["role_name"].dropna())

        for o, n in zip(original, df["role_name"]):
            if pd.notna(o) and o == n:
                stats["unmatched_roles"][o] += 1

        out_file = file.replace("_enriched.csv", "_final.csv")
        df.to_csv(os.path.join(OUTPUT_DIR, out_file), index=False, encoding="utf-8")
        print(f"  ✓ Saved: {out_file}")
        print(f"  ✓ Normalized: {changed:,} / {len(df):,} rows\n")

    generate_report(stats)
    print("\n✨ DONE")

if __name__ == "__main__":
    main()