CREATE VIEW final_jobs_500k AS
SELECT 
    -- Job_Postings (10 columns + 2 meta)
    f.job_id,
    f.posted_date,
    f.year,
    f.has_salary,
    f.min_salary,
    f.max_salary,
    f.currency,
    f.required_exp_years,
    f.education_level,
    f.employment_type,
    f.job_description,
    f.remote_option,

    -- Companies (4 columns)
    c.company_id,
    c.company_name,
    c.size AS company_size,
    c.industry,

    -- Locations (7 columns)
    l.location_id,
    l.city,
    l.country,
    l.country_iso,
    l.latitude,
    l.longitude,
    l.population,

    -- Aggregated Data
    (
        SELECT STRING_AGG(rn.role_name, '; ' ORDER BY rn.role_name)
        FROM job_roles jr
        JOIN role_names rn ON jr.role_id = rn.role_id
        WHERE jr.job_id = f.job_id
    ) AS roles_list,

    (
        SELECT STRING_AGG(jl.level, '; ' ORDER BY jl.level)
        FROM job_levels jl
        WHERE jl.job_id = f.job_id
    ) AS levels_list,

    (
        SELECT STRING_AGG(s.skill_name, '; ' ORDER BY s.skill_name)
        FROM job_skills js
        JOIN skills s ON js.skill_id = s.skill_id
        WHERE js.job_id = f.job_id
    ) AS skills_list

FROM filtered_jobs_500k f
LEFT JOIN companies c ON f.company_id = c.company_id
LEFT JOIN locations l ON f.location_id = l.location_id;