# -*- coding: utf-8 -*-
"""
STEP 4.5 – CREATE CLOUD ANALYTICAL VIEWS

- Create / Replace filtered_jobs_500k (fact view)
- Create / Replace dim_countries (dimension view with flags)
- Used by dashboard for realtime querying
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =====================================================
# DATABASE CONNECTION (REUSE EXISTING PATTERN)
# =====================================================
def create_db_connection():
    """Create database connection using SQLAlchemy engine"""
    load_dotenv()

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_SUPABASE_USER')}:"
        f"{os.getenv('DB_SUPABASE_PASS')}@"
        f"{os.getenv('DB_SUPABASE_HOST')}:"
        f"{os.getenv('DB_SUPABASE_PORT')}/"
        f"{os.getenv('DB_SUPABASE_NAME')}",
        connect_args={"sslmode": "require"}
    )

    print("Engine created and connected to database")
    return engine


# =====================================================
# SQL DEFINITIONS
# =====================================================
CREATE_FILTERED_JOBS_VIEW_SQL = """
CREATE OR REPLACE VIEW filtered_jobs_500k AS
WITH base AS (
    SELECT 
        j.*,
        EXTRACT(YEAR FROM j.posted_date) AS year,
        CASE 
            WHEN j.min_salary IS NOT NULL 
              OR j.max_salary IS NOT NULL 
            THEN 1 
            ELSE 0 
        END AS has_salary
    FROM job_postings j
    INNER JOIN locations l
        ON j.location_id = l.location_id
    WHERE j.posted_date IS NOT NULL
      AND j.job_id IS NOT NULL
      AND j.employment_type IS NOT NULL
      AND j.remote_option IS NOT NULL
      AND j.company_id IS NOT NULL
      AND l.country IS NOT NULL
      AND l.country_iso IS NOT NULL
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY year
            ORDER BY has_salary DESC, posted_date DESC
        ) AS rn
    FROM base
)
SELECT *
FROM ranked
WHERE rn <= 250000;
"""

CREATE_DIM_COUNTRY_VIEW_SQL = """
CREATE OR REPLACE VIEW dim_countries AS
SELECT DISTINCT
    country,
    country_iso,
    LOWER(country_iso) AS country_iso_lower,
    'https://flagcdn.com/w40/' || LOWER(country_iso) || '.png' AS flag_url
FROM locations
WHERE country_iso IS NOT NULL;
"""

# =====================================================
# MAIN
# =====================================================
def run():
    print("🔌 Connecting to cloud database...")
    engine = create_db_connection()

    with engine.begin() as conn:
        print("🚀 Creating / replacing view: filtered_jobs_500k")
        conn.execute(text(CREATE_FILTERED_JOBS_VIEW_SQL))

        print("🚀 Creating / replacing view: dim_countries")
        conn.execute(text(CREATE_DIM_COUNTRY_VIEW_SQL))

        print("✅ All cloud views created successfully")

if __name__ == "__main__":
    run()