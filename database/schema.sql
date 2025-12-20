-- =========================
-- TABLE: skills
-- =========================
CREATE TABLE skills (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) NOT NULL UNIQUE,
    skill_category VARCHAR(100) NOT NULL
        CHECK (skill_category IN (
            '__NA__',
            'Programming',
            'Data Engineering',
            'Machine Learning',
            'Cloud',
            'Visualization',
            'Database',
            'DevOps',
            'Analytics'
        ))
);

-- =========================
-- TABLE: skill_aliases
-- =========================
CREATE TABLE skill_aliases (
    alias VARCHAR(100) PRIMARY KEY,
    skill_id INT NOT NULL,
    CONSTRAINT fk_skill_aliases_skill
        FOREIGN KEY (skill_id)
        REFERENCES skills(skill_id)
        ON DELETE CASCADE
);

-- =========================
-- TABLE: companies
-- =========================
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    size VARCHAR(50) NOT NULL
        CHECK (size IN (
            '__NA__',
            'Startup',
            'Small',
            'Medium',
            'Large',
            'Enterprise'
        )),
    industry VARCHAR(100) NOT NULL
        CHECK (industry IN (
            '__NA__',
            'Technology',
            'Finance',
            'Banking',
            'Insurance',
            'Healthcare',
            'Education',
            'E-commerce',
            'Manufacturing',
            'Consulting',
            'Government',
            'Telecommunications',
            'Energy',
            'Retail',
            'Logistics',
            'Real Estate'
        ))
);

-- =========================
-- TABLE: locations
-- =========================
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,          -- '__NA__' allowed
    country VARCHAR(100) NOT NULL,       -- '__NA__' allowed
    country_iso CHAR(2) NOT NULL,        -- '__NA__' allowed
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    population INT
);

-- =========================
-- TABLE: role_names
-- =========================
CREATE TABLE role_names (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(255) NOT NULL UNIQUE
        CHECK (role_name IN (
            'Data Analyst',
            'Business Intelligence Analyst',
            'BI Developer',
            'Analytics Engineer',
            'Data Engineer',
            'Data Scientist',
            'Machine Learning Engineer',
            'AI Engineer',
            'AI Researcher',
            'Applied Scientist',
            'Research Engineer',
            'Data Architect',
            'Data Manager',
            'Data Lead'
        ))
);

-- =========================
-- TABLE: job_postings
-- =========================
CREATE TABLE job_postings (
    job_id SERIAL PRIMARY KEY,

    company_id INT NOT NULL,
    location_id INT NOT NULL,

    posted_date DATE NOT NULL,           -- DROP record if missing

    min_salary DECIMAL(10, 2),
    max_salary DECIMAL(10, 2),

    currency VARCHAR(10) NOT NULL,        -- '__NA__' allowed

    required_exp_years INT,

    education_level VARCHAR(50) NOT NULL
        CHECK (education_level IN (
            '__NA__',
            'High School',
            'Bachelor',
            'Master',
            'PhD'
        )),

    employment_type VARCHAR(20) NOT NULL
        CHECK (employment_type IN (
            '__NA__',
            'Full-time',
            'Part-time',
            'Internship',
            'Temporary'
        )),

    job_description TEXT,

    remote_option VARCHAR(20) NOT NULL
        CHECK (remote_option IN (
            '__NA__',
            'Onsite',
            'Hybrid',
            'Remote'
        )),

    CONSTRAINT fk_job_company
        FOREIGN KEY (company_id)
        REFERENCES companies(company_id),

    CONSTRAINT fk_job_location
        FOREIGN KEY (location_id)
        REFERENCES locations(location_id)
);

-- =========================
-- TABLE: job_skills (M:N)
-- =========================
CREATE TABLE job_skills (
    job_id INT NOT NULL,
    skill_id INT NOT NULL,

    skill_level_required VARCHAR(20) NOT NULL
        CHECK (skill_level_required IN (
            '__NA__',
            'Basic',
            'Intermediate',
            'Advanced',
            'Expert'
        )),

    PRIMARY KEY (job_id, skill_id),

    CONSTRAINT fk_job_skills_job
        FOREIGN KEY (job_id)
        REFERENCES job_postings(job_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_job_skills_skill
        FOREIGN KEY (skill_id)
        REFERENCES skills(skill_id)
);

-- =========================
-- TABLE: job_roles (M:N)
-- =========================
CREATE TABLE job_roles (
    job_id INT NOT NULL,
    role_id INT NOT NULL,

    PRIMARY KEY (job_id, role_id),

    CONSTRAINT fk_job_roles_job
        FOREIGN KEY (job_id)
        REFERENCES job_postings(job_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_job_roles_role
        FOREIGN KEY (role_id)
        REFERENCES role_names(role_id)
);

-- =========================
-- TABLE: job_levels
-- =========================
CREATE TABLE job_levels (
    job_id INT NOT NULL,
    level VARCHAR(20) NOT NULL
        CHECK (level IN (
            '__NA__',
            'Intern',
            'Junior',
            'Mid',
            'Senior',
            'Lead'
        )),

    PRIMARY KEY (job_id, level),

    CONSTRAINT fk_job_levels_job
        FOREIGN KEY (job_id)
        REFERENCES job_postings(job_id)
        ON DELETE CASCADE
);

-- =========================
-- INDEXES
-- =========================
CREATE INDEX idx_job_postings_posted_date
    ON job_postings(posted_date);

CREATE INDEX idx_job_postings_company
    ON job_postings(company_id);

CREATE INDEX idx_job_postings_location
    ON job_postings(location_id);