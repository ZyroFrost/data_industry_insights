CREATE INDEX IF NOT EXISTS idx_jp_posted_date ON job_postings (posted_date DESC);
CREATE INDEX IF NOT EXISTS idx_jp_company     ON job_postings (company_id);
CREATE INDEX IF NOT EXISTS idx_jp_location    ON job_postings (location_id);

CREATE INDEX IF NOT EXISTS idx_js_job_id ON job_skills (job_id);
CREATE INDEX IF NOT EXISTS idx_jr_job_id ON job_roles (job_id);
CREATE INDEX IF NOT EXISTS idx_jl_job_id ON job_levels (job_id);

CREATE INDEX IF NOT EXISTS idx_s_skill_id ON skills (skill_id);
CREATE INDEX IF NOT EXISTS idx_rn_role_id ON role_Names (role_id);