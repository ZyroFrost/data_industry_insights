CREATE OR REPLACE VIEW filtered_jobs_500k AS
WITH base AS (
    SELECT 
        j.*,
        EXTRACT(YEAR FROM posted_date) AS year,
        CASE 
            WHEN min_salary IS NOT NULL OR max_salary IS NOT NULL 
            THEN 1 ELSE 0 
        END AS has_salary
    FROM job_postings j
    WHERE posted_date IS NOT NULL
      AND job_id IS NOT NULL
      AND employment_type IS NOT NULL
      AND location_id IS NOT NULL
      AND remote_option IS NOT NULL
      AND company_id IS NOT NULL
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY year
            ORDER BY has_salary DESC, posted_date DESC
        ) AS rn
    FROM base
)
SELECT *
FROM ranked
WHERE rn <= 230000;