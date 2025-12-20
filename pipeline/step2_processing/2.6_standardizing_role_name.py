# -*- coding: utf-8 -*-
"""
STEP 2.6 – ROLE NAME STANDARDIZATION
"""

import pandas as pd
import unicodedata
import re
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.5_data_skill_level_enriched"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.6_data_role_name_standardized"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

UNMATCHED_DIR = BASE_DIR / "data" / "data_unmatched_report"
UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)

UNMATCHED_PATH = UNMATCHED_DIR / "unmatched_role_name.csv"

ROLE_MAPPING_PATH = BASE_DIR / "data" / "data_reference" / "role_names_mapping.csv"

# =========================
# HELPERS
# =========================

def normalize_text(x):
    if pd.isna(x):
        return "__NA__"
    x = str(x)
    x = unicodedata.normalize("NFKD", x)
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = re.sub(r"\d+", " ", x)
    x = re.sub(r"[^\w\s]", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip().lower()

# =========================
# LOAD ROLE TAXONOMY
# =========================

role_df = pd.read_csv(ROLE_MAPPING_PATH, dtype=str).fillna("")

MATCH_TERMS = {}     # canonical -> set(match terms)
EXCLUDE_TERMS = {}   # canonical -> set(exclude terms)

for _, row in role_df.iterrows():
    canonical = row["canonical_role"].strip()
    if not canonical:
        continue

    aliases = [a.strip().lower() for a in row.get("aliases", "").split("|") if a.strip()]
    strong = [s.strip().lower() for s in row.get("strong_terms", "").split("|") if s.strip()]
    keywords = row.get("keywords", "").strip().lower()
    if keywords:
        strong.append(keywords)

    terms = set(aliases + strong)
    terms.add(canonical.lower())

    excludes = {
        e.strip().lower()
        for e in row.get("exclude_terms", "").split("|")
        if e.strip()
    }

    MATCH_TERMS[canonical] = terms
    EXCLUDE_TERMS[canonical] = excludes

# =========================
# UNMATCHED COLLECTOR
# =========================

UNMATCHED_ROWS = []

# =========================
# ROLE CORE DEFINITIONS
# =========================

ROLE_CORE = {
    "analyst": "Data Analyst",
    "engineer": "Data Engineer",
    "scientist": "Data Scientist",
    "architect": "Data Architect",
    "steward": "Data Manager",
    "modeler": "Data Architect",
    "modeller": "Data Architect",
}

DATA_CONTEXT = {
    "data", "analytics", "analysis", "machine", "learning",
    "ai", "platform", "warehouse", "modeling", "modelling"
}

# =========================
# CORE MATCH LOGIC
# =========================

def extract_roles(raw_title):
    norm = normalize_text(raw_title)
    if not norm or norm == "__na__":
        return []

    tokens = norm.split()
    token_set = set(tokens)
    full_text = f" {norm} "

    # =========================
    # PHRASE COMPOSITION (2–3 tokens)
    # =========================

    phrases = set()
    for i in range(len(tokens)):
        for j in range(i + 1, min(i + 4, len(tokens) + 1)):
            phrases.add(" ".join(tokens[i:j]))

    found_roles = set()

    # =========================
    # 1. DIRECT / PHRASE MATCH
    # =========================

    for canonical, term_set in MATCH_TERMS.items():
        matched = False

        for term in term_set:
            if term in phrases:
                matched = True
                break
            if len(term.split()) > 1 and f" {term} " in full_text:
                matched = True
                break
            if term in token_set:
                matched = True
                break

        if not matched:
            continue

        # apply exclude AFTER match
        if EXCLUDE_TERMS.get(canonical) and EXCLUDE_TERMS[canonical].intersection(token_set):
            continue

        found_roles.add(canonical)

    if found_roles:
        return sorted(found_roles)

    # =========================
    # 2. ROLE CORE FALLBACK
    # =========================

    if token_set.intersection(DATA_CONTEXT):
        for core_word, canonical in ROLE_CORE.items():
            if core_word in token_set:
                if EXCLUDE_TERMS.get(canonical) and EXCLUDE_TERMS[canonical].intersection(token_set):
                    continue
                return [canonical]

    # =========================
    # 3. DATA & IA LEADERSHIP (EU / FR)
    # =========================

    leadership_terms = {
        "officer", "director", "directeur",
        "responsable", "chief", "head", "lead"
    }

    hard_exclude = {
        "technician", "center", "centre", "facilities",
        "entry", "clerk", "assistant", "collector",
        "survey", "security", "shift", "hvac", "electrical"
    }

    if (
        "data" in token_set
        and (token_set & leadership_terms)
        and not (token_set & hard_exclude)
    ):
        if "ai" in token_set or "ia" in token_set:
            return ["Data Lead"]
        return ["Data Manager"]

    return []

# =========================
# PROCESS FILE
# =========================

def standardize_role_file(file_path: Path):
    print(f"🔄 Standardizing role names: {file_path.name}")

    df = pd.read_csv(file_path, dtype=str)

    if "role_name" not in df.columns:
        print("  ⚠ No role_name column found, skipping")
        return

    new_roles = []

    for idx, raw in enumerate(df["role_name"].fillna("__NA__")):
        raw_norm = normalize_text(raw)

        if raw_norm == "__na__" or raw == "__NA__":
            new_roles.append("__NA__")
            continue

        roles = extract_roles(raw)

        if roles:
            new_roles.append(" | ".join(roles))
        else:
            new_roles.append("__UNMATCHED__")
            UNMATCHED_ROWS.append({
                "__source_id": df.at[idx, "__source_id"],
                "__source_name": df.at[idx, "__source_name"],
                "role_name": raw
            })

    df["role_name"] = new_roles

    stem = file_path.stem
    for p in ["enriched_", "normalized_", "mapped_", "standardized_"]:
        stem = stem.replace(p, "")

    output_path = OUTPUT_DIR / f"standardized_{stem}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    total = len(df)
    na = (df["role_name"] == "__NA__").sum()
    unmatched = (df["role_name"] == "__UNMATCHED__").sum()

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - Total rows                : {total}\n"
        f"    - Role standardized         : {total - na - unmatched}\n"
        f"    - Role empty (__NA__)       : {na}\n"
        f"    - Role unmatched (!)        : {unmatched}\n"
        f"  → Folder saved                : {output_path.parent}"
    )

# =========================
# RUN
# =========================

def run():
    global UNMATCHED_ROWS
    UNMATCHED_ROWS = []

    files = [f for f in INPUT_DIR.iterdir() if f.is_file() and f.suffix == ".csv"]

    for f in files:
        standardize_role_file(f)

    if UNMATCHED_ROWS:
        df_un = pd.DataFrame(
            UNMATCHED_ROWS,
            columns=["__source_id", "__source_name", "role_name"]
        )

        df_un.to_csv(
            UNMATCHED_PATH,
            index=False,
            encoding="utf-8-sig"
        )

        print(
            f"\n⚠ Unmatched role audit saved:\n"
            f"  - Rows  : {len(df_un)}\n"
            f"  - File   : {UNMATCHED_PATH.name}\n"
            f"  - Folder: {UNMATCHED_PATH.parent}"
        )

    print("\n=== STEP 2.6 COMPLETED: ROLE NAME STANDARDIZATION ===")

if __name__ == "__main__":
    run()