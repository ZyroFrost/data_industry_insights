# Database Schema – Data Industry Insights (2020–2025)

This database stores **cleaned, normalized, and analytics-ready job market data**
for **Data, AI, and Analytics roles** collected from multiple global sources
between **2020 and 2025**.

The schema is designed for **trend analysis, salary insights, skill demand analysis,
and geographic comparisons**.

---

## Design Principles

- All crawling, cleaning, normalization, enrichment, and deduplication are handled in the **Python ETL pipeline**
- The database stores **only clean, validated, and standardized data**
- Fully normalized relational model (3NF)
- Optimized for analytical queries (BI, dashboards, reporting)
- Schema acts as a **strict data contract** between ETL and downstream consumers
- All categorical fields use **database-enforced ENUM constraints**

---

## Schema Overview

### Fact Table

- **job_postings**  
  Central fact table containing **job-level information** such as company,
  location, salary, education requirements, employment type, and posting date.

A single job posting may be associated with **multiple roles, skills, and seniority levels**
via bridge tables.

---

### Dimension Tables

- **skills** – Canonical skill definitions
- **skill_aliases** – Normalized skill name mappings from raw text
- **companies** – Company master data
- **locations** – Geographic information (country-centric; city used as tracking signal)
- **role_names** – Canonical job role taxonomy (**ENUM table**)

---

### Bridge (Many-to-Many) Tables

- **job_skills** – Links jobs to required skills with proficiency level
- **job_roles** – Links jobs to one or more standardized roles
- **job_levels** – Links jobs to one or more seniority levels

---

## Role Modeling Strategy

- Roles are modeled as a **many-to-many relationship**
- A job posting may target **multiple roles simultaneously**
- No concept of *primary* or *secondary* role is assumed
- Role assignment is derived during ETL and enforced via foreign keys

This reflects real-world job postings where hybrid and multi-role hiring is common.

---

## Enumerations & Constraints

- All categorical fields are enforced using **database-level ENUM constraints**
- ENUM values are defined directly in the schema and validated at insert time
- Examples include:
  - Role names
  - Skill categories
  - Skill levels
  - Education levels
  - Employment types
  - Company size and industry

Invalid values are **rejected by the database**, ensuring strict data integrity.

---

## Table Relationships

- `job_postings.company_id` → `companies.company_id`
- `job_postings.location_id` → `locations.location_id`
- `job_postings.job_id` ↔ `role_names.role_id` (via `job_roles`)
- `job_postings.job_id` ↔ `skills.skill_id` (via `job_skills`)
- `job_postings.job_id` ↔ seniority levels (via `job_levels`)

Refer to **erd.png** for the visual relationship diagram.

---

## Data Responsibility Boundaries

| Layer | Responsibility |
|---|---|
| Crawlers | Data collection from APIs and websites |
| ETL (Python) | Parsing, cleaning, normalization, enrichment, deduplication |
| Database | Storage of validated, standardized, analytics-ready data |
| BI / Applications | Analysis, visualization, reporting |

---

## Notes

- Skill names are standardized using the `skills` and `skill_aliases` tables
- Role names must match the canonical taxonomy defined in `role_names`
- Salary fields may be NULL when not provided by the source
- `remote_option = false` represents **“not specified”**, not explicitly on-site
- City values are used primarily for **geo enrichment**, not as a core analytic dimension

---

## Usage

The database is intended to be consumed by:

- Power BI dashboards
- Streamlit analytics applications
- Ad-hoc SQL analysis

This schema is **read-only for analytics consumers**
and **must only be written to by the ETL pipeline**.

---

## Files

| File | Description |
|---|---|
| `schema.sql` | PostgreSQL DDL to create all tables, constraints, and indexes |
| `erd.png` | Entity Relationship Diagram |
| `README.md` | Database schema documentation |

---

### Status

This schema is **stable**, **validated**, and ready for long-term analytical use.