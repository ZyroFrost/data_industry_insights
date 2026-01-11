"""
Dataset Construction Script
Converts job postings data from database to processed CSV files
"""

import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

"""Export datasets to CSV files"""
ROOT = Path.cwd()
OUTPUT_DIR = ROOT / "analysis" / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print("Root directory:", ROOT)
print("Output directory:", OUTPUT_DIR)

output_path_final = OUTPUT_DIR / "final_jobs_500k.csv"
output_path_sample = OUTPUT_DIR / "final_jobs_sample_50k.csv"
    
def create_db_connection():
    """Create database connection using SQLAlchemy engine"""
    load_dotenv()
    
    conn = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_NEON_USER')}:{os.getenv('DB_NEON_PASS')}"
        f"@{os.getenv('DB_NEON_HOST')}:{os.getenv('DB_NEON_PORT')}"
        f"/{os.getenv('DB_NEON_NAME')}",
        connect_args={"sslmode": "require"}
    )
    
    print("Engine created and connected to database")
    return conn

def load_base_job_postings(conn):
    """Load and prepare base job postings data"""
    job_postings = pd.read_sql("""
        SELECT *
        FROM job_postings
        WHERE posted_date IS NOT NULL
          AND job_id IS NOT NULL
          AND employment_type IS NOT NULL
          AND location_id IS NOT NULL
          AND remote_option IS NOT NULL
    """, conn, parse_dates=["posted_date"])
    
    # Add year column
    job_postings["year"] = job_postings["posted_date"].dt.year.astype(int)
    
    # Add has_salary indicator
    job_postings["has_salary"] = (
        job_postings["min_salary"].notna() |
        job_postings["max_salary"].notna()
    ).astype(int)
    
    print(f"{len(job_postings)} rows loaded from job_postings table")
    return job_postings


def balance_jobs_by_year(job_postings, max_per_year=250000):
    """Balance jobs by year using ranking"""
    job_postings = job_postings.sort_values(
        by=["year", "has_salary", "posted_date"],
        ascending=[True, False, False]
    )
    
    job_postings["rn"] = (
        job_postings
        .groupby("year")
        .cumcount() + 1
    )
    
    df_filtered = (
        job_postings[job_postings["rn"] <= max_per_year]
        .drop(columns=["rn", "has_salary", "year"])        
        .copy()
    )
    
    print(f"Filtered to {len(df_filtered)} rows (max {max_per_year} per year)")
    return df_filtered


def load_dimension_tables(conn, job_ids):
    """Load dimension tables for the filtered job IDs"""
    companies = pd.read_sql("SELECT * FROM companies", conn)
    locations = pd.read_sql("SELECT * FROM locations", conn)
    role_names = pd.read_sql("SELECT * FROM role_names", conn)
    skills = pd.read_sql("SELECT * FROM skills", conn)
    
    job_ids_tuple = tuple(job_ids)
    
    job_levels = pd.read_sql(
        "SELECT * FROM job_levels WHERE job_id IN %(job_ids)s",
        conn,
        params={"job_ids": job_ids_tuple}
    )
    
    job_skills = pd.read_sql(
        "SELECT * FROM job_skills WHERE job_id IN %(job_ids)s",
        conn,
        params={"job_ids": job_ids_tuple}
    )
    
    job_roles = pd.read_sql(
        "SELECT * FROM job_roles WHERE job_id IN %(job_ids)s",
        conn,
        params={"job_ids": job_ids_tuple}
    )
    
    print("Dimension tables loaded")
    return companies, locations, role_names, skills, job_levels, job_skills, job_roles


def aggregate_multi_value_fields(job_roles, role_names, job_levels, job_skills, skills):
    """Aggregate roles, levels, and skills into semicolon-separated lists"""
    roles_agg = (
        job_roles
        .merge(role_names, on="role_id", how="left")
        .groupby("job_id")
        .agg(
            role_name=("role_name", lambda x: "; ".join(sorted(x.dropna().unique()))),
            role_id=("role_id", lambda x: "; ".join(sorted(x.dropna().astype(str).unique())))
        )
        .reset_index()
    )
    
    levels_agg = (
        job_levels
        .groupby("job_id")["job_level"]
        .apply(lambda x: "; ".join(sorted(x.dropna().unique())))
        .reset_index()
        .rename(columns={"job_level": "job_level"})
    )
    
    skills_agg = (
        job_skills
        .merge(skills, on="skill_id", how="left")
        .groupby("job_id")
        .agg(
            skill_name=("skill_name", lambda x: "; ".join(sorted(x.dropna().unique()))),
            skill_id=("skill_id", lambda x: "; ".join(sorted(x.dropna().astype(str).unique()))),
            skill_category=("skill_category", lambda x: "; ".join(sorted(x.dropna().unique())))
        )
        .reset_index()
    )
    
    print(f"Aggregated - roles: {len(roles_agg)}, levels: {len(levels_agg)}, skills: {len(skills_agg)}")
    return roles_agg, levels_agg, skills_agg


def join_all_tables(df_filtered, companies, locations, roles_agg, levels_agg, skills_agg):
    """Join all tables to create final dataset"""
    df_final = (
        df_filtered
        .merge(companies, on="company_id", how="left")
        .merge(locations, on="location_id", how="left")
        .merge(roles_agg, on="job_id", how="left")
        .merge(levels_agg, on="job_id", how="left")
        .merge(skills_agg, on="job_id", how="left")
    )
    
    # Reorder columns to match expected structure
    column_order = [
        'job_id', 'company_id', 'location_id', 'posted_date',
        'min_salary', 'max_salary', 'currency', 'required_exp_years',
        'education_level', 'employment_type', 'job_description', 'remote_option',
        'company_name', 'company_size', 'industry',
        'city', 'country', 'country_iso', 'latitude', 'longitude', 'population',
        'role_name', 'job_level', 'skill_name', 'skill_id', 'skill_category', 'role_id'
    ]
    
    df_final = df_final[column_order]
    
    print(f"\nFinal dataset created with {len(df_final)} rows")
    print(f"\n{'#':<4} {'Column':<25} {'Non-Null Count':<20} {'Dtype'}")
    print("-" * 70)
    for idx, col in enumerate(df_final.columns):
        non_null_count = df_final[col].notna().sum()
        dtype = str(df_final[col].dtype)
        print(f"{idx:<4} {col:<25} {non_null_count} non-null{'':<8} {dtype}")
    
    print(f"\ndtypes: {df_final.dtypes.value_counts().to_dict()}")
    print(f"memory usage: {df_final.memory_usage(deep=True).sum() / 1024**2:.1f}+ MB")
    
    return df_final


def create_sample(df_final, sample_size=50000, random_state=42):
    """Create random sample from final dataset"""
    df_sample = df_final.sample(
        n=sample_size,
        random_state=random_state
    ).reset_index(drop=True)
    
    print(f"Sample created with {len(df_sample)} rows")
    return df_sample


def confirm_overwrite(path: Path) -> bool:
    """Prompt user to confirm file overwrite"""
    if path.exists():
        ans = input(f"⚠️ File '{path.name}' đã tồn tại. Ghi đè? (y/n): ").strip().lower()
        return ans == "y"
    return True


def export_datasets(df_final, df_sample):
    # Export final dataset
    if confirm_overwrite(output_path_final):
        df_final.to_csv(
            output_path_final,
            index=False,
            encoding="utf-8-sig"
        )
        print(f"✅ Exported: {output_path_final}")
    else:
        print(f"⏭️ Skip: {output_path_final}")
    
    # Export sample dataset
    if confirm_overwrite(output_path_sample):
        df_sample.to_csv(
            output_path_sample,
            index=False,
            encoding="utf-8-sig"
        )
        print(f"✅ Exported: {output_path_sample}")
    else:
        print(f"⏭️ Skip: {output_path_sample}")


def main():
    """Main execution function"""
    print("="*60)
    print("DATASET CONSTRUCTION PIPELINE")
    print("="*60)
    
    # Step 1: Connect to database
    print("\n[1/8] Connecting to database...")
    conn = create_db_connection()
    
    # Step 2: Load base job postings
    print("\n[2/8] Loading base job postings...")
    job_postings = load_base_job_postings(conn)
    
    # Step 3: Balance jobs by year
    print("\n[3/8] Balancing jobs by year...")
    df_filtered = balance_jobs_by_year(job_postings)
    
    # Step 4: Load dimension tables
    print("\n[4/8] Loading dimension tables...")
    job_ids = set(df_filtered["job_id"])
    companies, locations, role_names, skills, job_levels, job_skills, job_roles = \
        load_dimension_tables(conn, job_ids)
    
    # Step 5: Aggregate multi-value fields
    print("\n[5/8] Aggregating roles, levels, and skills...")
    roles_agg, levels_agg, skills_agg = \
        aggregate_multi_value_fields(job_roles, role_names, job_levels, job_skills, skills)
    
    # Step 6: Join all tables
    print("\n[6/8] Joining all tables...")
    df_final = join_all_tables(df_filtered, companies, locations, roles_agg, levels_agg, skills_agg)
    
    # Step 7: Create sample
    print("\n[7/8] Creating sample dataset...")
    df_sample = create_sample(df_final)
    
    # Step 8: Export datasets
    print("\n[8/8] Exporting datasets...")
    export_datasets(df_final, df_sample)
    
    # Final report
    print("\n" + "="*70)
    print("FINAL DATASET INFO")
    print("="*70)
    print(f"Total rows: {len(df_final)}")
    print(f"Total columns: {len(df_final.columns)}")
    print(f"\n{'#':<4} {'Column':<25} {'Non-Null Count':<20} {'Dtype'}")
    print("-" * 70)
    for idx, col in enumerate(df_final.columns):
        non_null_count = df_final[col].notna().sum()
        dtype = str(df_final[col].dtype)
        print(f"{idx:<4} {col:<25} {non_null_count} non-null{'':<8} {dtype}")
    
    print(f"\ndtypes: {df_final.dtypes.value_counts().to_dict()}")
    print(f"memory usage: {df_final.memory_usage(deep=True).sum() / 1024**2:.1f}+ MB")
    
    print("\n" + "="*70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("="*70)


if __name__ == "__main__":
    main()