# -*- coding: utf-8 -*-
"""
RUN FULL STEP 2 PIPELINE (2.1 → 2.11)

Purpose:
- Sequentially execute STEP 2 scripts
- Allow running from any step range (numeric input)
- No logic duplication
- Fail fast if any step errors
"""

import subprocess
import sys
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parent

# Step index: 1 → 11  (maps to 2.1 → 2.11)
STEPS = {
    1: "s2_1_mapping_check.py",
    2: "s2_2_extracting_description_signals.py",
    3: "s2_3_normalizing_values.py",
    4: "s2_4_enriching_country_from_city.py",
    5: "s2_5_enriching_skill_level_category.py",
    6: "s2_6_standardizing_role_name.py",
    7: "s2_7_validating_salary_exp.py",
    8: "s2_8_combining_data.py",
    9: "s2_9_splitting_tables_erd.py",
}

# =========================
# RUNNER
# =========================

def run_step(step_num: int, script_name: str):
    script_path = BASE_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"❌ Missing script: {script_name}")

    print(f"\n▶ RUNNING STEP {step_num}: {script_name}")
    subprocess.run(
        [sys.executable, str(script_path)],
        check=True
    )
    print(f"✓ DONE STEP {step_num}")

def ask_step_range():
    print("\nAvailable steps:")
    for k, v in STEPS.items():
        print(f"  {k}: {v}")

    start = int(input("\n👉 Run FROM step (number): ").strip())
    end = int(input("👉 Run TO step (number): ").strip())

    if start not in STEPS or end not in STEPS:
        raise ValueError("❌ Invalid step number")

    if start > end:
        raise ValueError("❌ Start step must be <= end step")

    return start, end

def run():
    print("====================================")
    print("🚀 START STEP 2 PIPELINE")
    print("====================================")

    start, end = ask_step_range()

    print(f"\n▶ Executing steps {start} → {end}")

    for step_num in range(start, end + 1):
        run_step(step_num, STEPS[step_num])

    print("\n====================================")
    print("✅ STEP 2 PIPELINE COMPLETED SUCCESSFULLY")
    print("====================================")

if __name__ == "__main__":
    run()