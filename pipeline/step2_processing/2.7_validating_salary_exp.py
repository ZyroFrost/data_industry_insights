# -*- coding: utf-8 -*-
"""
STEP 2.7 – VALIDATING SALARY EXPRESSION

- Validate min_salary / max_salary
- Infer unit by range (hour / week / month / year)
- Convert tạm sang YEAR để check hợp lệ
- Nếu FAIL → ghi đè __INVALID__
- KHÔNG tạo cột mới
- KHÔNG thay đổi schema
"""

import pandas as pd
from pathlib import Path

# ==================================================
# PATHS
# ==================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.6_data_role_name_standardized"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.7_data_salary_exp_validated"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FX_RATE_PATH = BASE_DIR / "data" / "data_reference" / "currency_rates.csv"

# ==================================================
# CONSTANTS
# ==================================================

UNIT_RANGES = {
    "hour":  (15, 250),
    "week":  (400, 8000),
    "month": (1500, 40000),
    "year":  (20000, 500000),
}

UNIT_MULTIPLIER = {
    "hour":  2080,
    "week":  52,
    "month": 12,
    "year":  1,
}

YEARLY_MIN = 20000
YEARLY_MAX = 500000

NA_VALUES = {"__NA__", "", None}

# ==================================================
# HELPERS
# ==================================================

def load_fx_rates(path: Path) -> dict:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    return dict(zip(df["currency"], df["rate_to_usd"]))


def is_na(x):
    return pd.isna(x) or str(x).strip() in NA_VALUES


def to_usd(value, currency, fx_rates):
    try:
        value = float(value)
    except Exception:
        return None

    if is_na(currency) or currency not in fx_rates:
        return value

    return value * fx_rates[currency]


def interval_intersects(a_min, a_max, b_min, b_max):
    return not (a_max < b_min or a_min > b_max)

# ==================================================
# CORE LOGIC
# ==================================================

def process_file(path: Path, fx_rates: dict):
    df = pd.read_csv(path)

    # 🔴 BẮT BUỘC CAST NGAY SAU read_csv
    df["min_salary"] = df["min_salary"].astype("object")
    df["max_salary"] = df["max_salary"].astype("object")

    if "min_salary" not in df.columns or "max_salary" not in df.columns:
        raise ValueError(f"{path.name} missing min_salary / max_salary")

    invalid_count = 0

    for i, row in df.iterrows():
        raw_min = row["min_salary"]
        raw_max = row["max_salary"]
        currency = row.get("currency")

        # Nếu dữ liệu gốc là __NA__ → giữ nguyên, KHÔNG ghi đè
        # Case 1: cả hai đều NA → giữ nguyên
        if raw_min == "__NA__" and raw_max == "__NA__":
            continue

        # Case 2: chỉ một bên NA → INVALID
        if raw_min == "__NA__" or raw_max == "__NA__":
            df.at[i, "min_salary"] = "__INVALID__"
            df.at[i, "max_salary"] = "__INVALID__"
            invalid_count += 1
            continue

        usd_min = to_usd(raw_min, currency, fx_rates)
        usd_max = to_usd(raw_max, currency, fx_rates)

        # Parse fail → INVALID
        if usd_min is None or usd_max is None:
            df.at[i, "min_salary"] = "__INVALID__"
            df.at[i, "max_salary"] = "__INVALID__"
            invalid_count += 1
            continue

        # ZERO or NEGATIVE salary → INVALID
        if usd_min <= 0 or usd_max <= 0:
            df.at[i, "min_salary"] = "__INVALID__"
            df.at[i, "max_salary"] = "__INVALID__"
            invalid_count += 1
            continue

        possible_units = []

        for unit, (u_min, u_max) in UNIT_RANGES.items():
            if interval_intersects(usd_min, usd_max, u_min, u_max):
                possible_units.append(unit)

        if not possible_units:
            df.at[i, "min_salary"] = "__INVALID__"
            df.at[i, "max_salary"] = "__INVALID__"
            invalid_count += 1
            continue

        valid = False

        chosen_unit = None
        chosen_multiplier = None

        for unit in possible_units:
            mult = UNIT_MULTIPLIER[unit]
            y_min = usd_min * mult
            y_max = usd_max * mult

            if y_max >= YEARLY_MIN and y_min <= YEARLY_MAX:
                chosen_unit = unit
                chosen_multiplier = mult
                break

        if not chosen_unit:
            df.at[i, "min_salary"] = "__INVALID__"
            df.at[i, "max_salary"] = "__INVALID__"
            invalid_count += 1
            continue

        # ================================
        # YEAR CANONICALIZATION (QUY VỀ NĂM)
        # ================================

        year_min = usd_min * chosen_multiplier
        year_max = usd_max * chosen_multiplier

        # CHECK RANGE LẦN 2 – YEAR PHẢI NẰM TRỌN TRONG RANGE
        if year_min < YEARLY_MIN or year_max > YEARLY_MAX:
            df.at[i, "min_salary"] = "__INVALID__"
            df.at[i, "max_salary"] = "__INVALID__"
            invalid_count += 1
            continue

        df.at[i, "min_salary"] = round(year_min, 2)
        df.at[i, "max_salary"] = round(year_max, 2)

    # remove previous step prefix (e.g. standardized_, normalized_, enriched_)
    base_name = path.name
    for prefix in [
        "standardized_",
        "normalized_",
        "mapped_",
        "enriched_",
    ]:
        if base_name.startswith(prefix):
            base_name = base_name[len(prefix):]
            break

    output_path = OUTPUT_DIR / f"validated_{base_name}"

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✓ Processed: {path.name}")
    print(f"  - Total rows     : {len(df)}")
    print(f"  - Salary invalid : {invalid_count}")
    print(f"  - Salary valid   : {len(df) - invalid_count}")

# ==================================================
# RUN
# ==================================================

def run():
    fx_rates = load_fx_rates(FX_RATE_PATH)
    files = [f for f in INPUT_DIR.iterdir() if f.suffix.lower() == ".csv"]

    if not files:
        raise RuntimeError("No input CSV files found")

    for f in files:
        process_file(f, fx_rates)
    
    print("\n=== STEP 2.7 COMPLETED: VALIDATION ===")

if __name__ == "__main__":
    run()