import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import os
from typing import Dict, List

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "data_processed"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Mapping file CSV -> table name và columns
TABLE_MAPPINGS = {
    # Các bảng không phụ thuộc (load trước)
    "companies.csv": {
        "table": "Companies",
        "id_column": "company_id",  # Cột ID để mapping
        "columns": ["company_name", "size", "industry"]  # Các cột insert (bỏ ID)
    },
    "locations.csv": {
        "table": "Locations",
        "id_column": "location_id",
        "columns": ["city", "country", "country_iso", "latitude", "longitude", "population"]
    },
    "role_names.csv": {
        "table": "Role_Names",
        "id_column": "role_id",
        "columns": ["role_name"]
    },
    "skills.csv": {
        "table": "Skills",
        "id_column": "skill_id",
        "columns": ["skill_name", "skill_category", "certification_required"],
        "skip": True  # File rỗng, skip
    },
    # Các bảng có foreign key (load sau)
    "job_postings.csv": {
        "table": "Job_Postings",
        "id_column": "job_id",
        "columns": ["company_id", "location_id", "posted_date", "min_salary", 
                   "max_salary", "currency", "required_exp_years", "education_level", 
                   "employment_type", "remote_option", "job_description"]
    },
    "job_roles.csv": {
        "table": "Job_Roles",
        "id_column": None,  # Không có ID tự tăng
        "columns": ["job_id", "role_id"]
    },
    "job_levels.csv": {
        "table": "Job_Levels",
        "id_column": None,
        "columns": ["job_id", "level"],
        "skip": True  # File rỗng, skip
    },
    "job_skills.csv": {
        "table": "Job_Skills",
        "id_column": None,
        "columns": ["job_id", "skill_id", "skill_level_required"],
        "skip": True  # File rỗng, skip
    }
}

# Map ID từ CSV sang ID mới trong DB (sau khi insert)
ID_MAPPING = {
    "company_id": {},
    "location_id": {},
    "role_id": {},
    "skill_id": {},
    "job_id": {}
}

# =========================
# LOAD STATS (FOR FINAL SUMMARY)
# =========================
TABLE_STATS = {
    "Companies": {"inserted": 0, "skipped": 0},
    "Locations": {"inserted": 0, "skipped": 0},
    "Role_Names": {"inserted": 0, "skipped": 0},
    "Skills": {"inserted": 0, "skipped": 0},
    "Skill_Aliases": {"inserted": 0, "skipped": 0},
    "Job_Postings": {"inserted": 0, "skipped": 0},
    "Job_Roles": {"inserted": 0, "skipped": 0},
    "Job_Levels": {"inserted": 0, "skipped": 0},
    "Job_Skills": {"inserted": 0, "skipped": 0},
}

def clean_value(value, column_name=""):
    """Xử lý giá trị NA và chuyển đổi kiểu dữ liệu"""
    # Xử lý các giá trị NA
    if pd.isna(value):
        return None
    
    if isinstance(value, str):
        stripped = value.strip()

        # ===== GIỮ NGUYÊN CÁC MARKER NHẬN DIỆN =====
        if stripped in ["__NA__", "__INVALID__", "__UNMATCHED__"]:
            return stripped

        # ===== CHỈ CÁC GIÁ TRỊ RỖNG THỰC SỰ MỚI LÀ NULL =====
        if stripped in ['_NA_', 'NA', 'nan', 'NaN', '']:
            return None

        # FIX population dạng "8877067.0"
        if column_name == "population":
            try:
                return int(float(stripped))
            except:
                return None

        return stripped

    # Xử lý numpy/pandas types
    if isinstance(value, (np.integer, np.floating)):
        if np.isnan(value):
            return None
        # Convert numpy int64 sang Python int
        if isinstance(value, np.integer):
            return int(value)
        # Convert float
        float_val = float(value)
        # Nếu là số nguyên (như population) thì convert sang int
        if column_name in ['population', 'required_exp_years']:
            return int(float_val)

        return float_val
    
    # Xử lý boolean
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    
    return value

def get_db_connection():
    """Tạo kết nối đến PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None

def truncate_tables(conn):
    """Xóa dữ liệu cũ trong các bảng (theo thứ tự ngược lại)"""
    cursor = conn.cursor()
    tables_order = [
        "Job_Skills", "Job_Levels", "Job_Roles", 
        "Job_Postings", "Skills", "Role_Names", 
        "Locations", "Companies"
    ]
    
    try:
        for table in tables_order:
            cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
            print(f"✓ Đã xóa dữ liệu bảng: {table}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi truncate: {e}")
    finally:
        cursor.close()

def load_csv_to_db(conn, csv_file: str, table_name: str, id_column: str, columns: List[str], skip: bool = False):
    """Load dữ liệu từ CSV vào database"""
    if skip:
        print(f"⏭️  Bỏ qua {csv_file} (file rỗng hoặc không cần thiết)")
        return True
    
    file_path = DATA_DIR / csv_file
    
    if not file_path.exists():
        print(f"⚠️  File không tồn tại: {csv_file}")
        return False
    
    # Kiểm tra file size
    if file_path.stat().st_size < 10:
        print(f"⚠️  File {csv_file} quá nhỏ, bỏ qua")
        return True
    
    try:
        # Đọc CSV với Pandas
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        
        # Normalize column names
        df.columns = df.columns.str.strip()
        
        # Kiểm tra nếu DataFrame rỗng
        if df.empty:
            print(f"⚠️  File {csv_file} không có dữ liệu")
            return True
        
        # Kiểm tra columns có tồn tại không
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            print(f"⚠️  Thiếu columns trong {csv_file}: {missing_cols}")
            print(f"   Available: {list(df.columns)}")
            return False
        
        # Chuẩn bị câu lệnh INSERT
        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Nếu có id_column, cần RETURNING để lấy ID mới
        if id_column:
            insert_query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) RETURNING {id_column}"
        else:
            insert_query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        
        cursor = conn.cursor()
        inserted_count = 0
        skipped_count = 0
        
        # Insert từng row với auto-commit mỗi row
        for idx, row in df.iterrows():
            try:
                # Lấy old_id nếu có
                if table_name == "Job_Postings":
                    old_id = None
                else:
                    old_id = int(row[id_column]) if id_column and id_column in df.columns else None

                
                # Clean values
                values = [clean_value(row[col], col) for col in columns]
                # MAP FK cho Job_Postings

                if table_name == "Job_Postings":
                    # numeric + date columns không được phép nhận marker text
                    for i, col in enumerate(columns):
                        if col in ["min_salary", "max_salary", "required_exp_years", "posted_date"]:
                            if values[i] in ["__NA__", "__INVALID__", "__UNMATCHED__"]:
                                values[i] = None

                    # required_exp_years
                    if values[6] == "__NA__":
                        values[6] = None

                    # nếu map fail thì skip
                    if values[0] is None or values[1] is None:
                        skipped_count += 1
                        continue

                    # remote_option (BOOLEAN NOT NULL)
                    # index = 10
                    if values[10] in [None, "__NA__"]:
                        values[10] = False

                # FIX currency NOT NULL
                if table_name == "Job_Postings":
                    # currency
                    if values[5] is None:
                        values[5] = "USD"

                    # education_level
                    if values[7] is None:
                        values[7] = "__NA__"

                    # employment_type
                    # employment_type ENUM FIX
                    if values[8] is not None:
                        et = str(values[8]).strip().lower()

                        if et in ["full-time", "full time", "permanent"]:
                            values[8] = "Full-time"

                        elif et in ["part-time", "part time"]:
                            values[8] = "Part-time"

                        elif et in ["intern", "internship", "trainee", "working student", "werkstudent"]:
                            values[8] = "Internship"

                        elif et in ["temporary", "contract", "fixed-term", "fixed term", "freelance"]:
                            values[8] = "Temporary"

                        else:
                            values[8] = "__NA__"
                    else:
                        values[8] = "__NA__"


                # FIX NOT NULL ENUM
                if table_name == "Companies":
                    # size
                    if values[1] not in [
                        "__NA__", "Startup", "Small", "Medium", "Large", "Enterprise"
                    ]:
                        values[1] = "__NA__"

                    # industry
                    if values[2] not in [
                        "__NA__", "Technology", "Finance", "Banking", "Insurance",
                        "Healthcare", "Education", "E-commerce", "Manufacturing",
                        "Consulting", "Government", "Telecommunications", "Energy",
                        "Retail", "Logistics", "Real Estate"
                    ]:
                        values[2] = "__NA__"

                # Skip row nếu các cột quan trọng đều None
                # Đối với companies: cần company_name
                if table_name == "Companies" and values[0] is None:
                    skipped_count += 1
                    continue
                # Đối với locations: cần city
                if table_name == "Locations" and values[0] is None:
                    skipped_count += 1
                    continue
                
                cursor.execute(insert_query, values)
                
                # Lấy new_id từ DB (nếu có RETURNING)
                if id_column:
                    new_id = cursor.fetchone()[0]
                    # Lưu mapping: old_id -> new_id
                    if old_id:
                        ID_MAPPING[id_column][old_id] = new_id
                
                # COMMIT NGAY SAU MỖI ROW để tránh transaction bị abort
                conn.commit()
                inserted_count += 1
                
                # Progress indicator mỗi 1000 rows
                if inserted_count % 1000 == 0:
                    print(f"  ... đã insert {inserted_count} rows")
                
            except Exception as row_error:
                # Rollback transaction bị lỗi
                conn.rollback()
                if skipped_count < 5:  # Chỉ print 5 lỗi đầu
                    print(f"⚠️  Row {idx}: {str(row_error)[:100]}")
                skipped_count += 1
                continue
        
        cursor.close()
        TABLE_STATS[table_name]["inserted"] += inserted_count
        TABLE_STATS[table_name]["skipped"] += skipped_count

        print(f"✓ Đã load {inserted_count} rows vào {table_name} (skipped: {skipped_count})")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi load {csv_file}: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_csv_with_fk_mapping(conn, csv_file: str, table_name: str, columns: List[str], skip: bool = False):
    """Load dữ liệu có foreign key, cần map ID từ old sang new"""
    if skip:
        print(f"⏭️  Bỏ qua {csv_file} (file rỗng hoặc không cần thiết)")
        return True
    
    file_path = DATA_DIR / csv_file
    
    if not file_path.exists():
        print(f"⚠️  File không tồn tại: {csv_file}")
        return False
    
    if file_path.stat().st_size < 10:
        print(f"⚠️  File {csv_file} quá nhỏ, bỏ qua")
        return True
    
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
        df.columns = df.columns.str.strip()
        
        if df.empty:
            print(f"⚠️  File {csv_file} không có dữ liệu")
            return True
        
        # Chuẩn bị INSERT
        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        
        cursor = conn.cursor()
        inserted_count = 0
        skipped_count = 0
        
        for idx, row in df.iterrows():
            try:
                values = []
                for col in columns:
                    val = clean_value(row[col], col)
                    if col == "level" and val is None:
                        val = "__NA__"

                    if col == "skill_level_required" and val is None:
                        val = "__NA__"

                    
                    # Map foreign key từ old_id sang new_id
                    if col == "job_id":
                        # job_id là ID logic xuyên suốt pipeline → giữ nguyên
                        values.append(val)
                        continue

                    if col in ID_MAPPING and val is not None:
                        if val not in ID_MAPPING[col]:
                            raise ValueError(f"FK {col}={val} không tồn tại trong mapping")
                        val = ID_MAPPING[col][val]

                    values.append(val)
               
                # Skip nếu thiếu FK quan trọng
                # chỉ skip nếu FK thật sự NULL
                if any(v is None for v in values[:2]):  # job_id, role_id / skill_id
                    skipped_count += 1
                    continue
                
                cursor.execute(insert_query, values)
                
                # COMMIT NGAY SAU MỖI ROW
                conn.commit()
                inserted_count += 1
                
                # Progress indicator
                if inserted_count % 5000 == 0:
                    print(f"  ... đã insert {inserted_count} rows")
                
            except Exception as row_error:
                # Rollback transaction bị lỗi
                conn.rollback()
                if skipped_count < 5:  # Chỉ print 5 lỗi đầu
                    print(f"⚠️  Row {idx}: {str(row_error)[:100]}")
                skipped_count += 1
                continue
        
        cursor.close()
        
        TABLE_STATS[table_name]["inserted"] += inserted_count
        TABLE_STATS[table_name]["skipped"] += skipped_count

        print(f"✓ Đã load {inserted_count} rows vào {table_name} (skipped: {skipped_count})")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi load {csv_file}: {e}")
        return False

def main():
    """Main function để upload tất cả CSV files"""
    print("=" * 60)
    print("🚀 BẮT ĐẦU UPLOAD DỮ LIỆU LÊN DATABASE")
    print("=" * 60)
    
    conn = get_db_connection()
    if not conn:
        return
    
    print("\n📁 Data directory:", DATA_DIR)
    print()
    
    # Option: Xóa dữ liệu cũ
    choice = input("⚠️  Xóa dữ liệu cũ trong database? (y/n): ").lower()
    if choice == 'y':
        truncate_tables(conn)
        print()
    
    success_count = 0
    
    # PHASE 1: Load các bảng parent (không có FK)
    print("\n📦 PHASE 1: Load các bảng parent...")
    print("-" * 60)
    parent_tables = ["companies.csv", "locations.csv", "role_names.csv", "skills.csv"]
    for csv_file in parent_tables:
        if csv_file in TABLE_MAPPINGS:
            config = TABLE_MAPPINGS[csv_file]
            if load_csv_to_db(conn, csv_file, config["table"], 
                             config["id_column"], config["columns"], 
                             config.get("skip", False)):
                success_count += 1
    
    # PHASE 2: Load Job_Postings (cần company_id, location_id)
    print("\n📦 PHASE 2: Load Job_Postings...")
    print("-" * 60)
    config = TABLE_MAPPINGS["job_postings.csv"]
    if load_csv_to_db(conn, "job_postings.csv", config["table"], 
                     config["id_column"], config["columns"], 
                     config.get("skip", False)):
        success_count += 1
    
    # PHASE 3: Load các bảng junction (cần job_id và các FK khác)
    print("\n📦 PHASE 3: Load các bảng junction...")
    print("-" * 60)
    junction_tables = ["job_roles.csv", "job_levels.csv", "job_skills.csv"]
    for csv_file in junction_tables:
        if csv_file in TABLE_MAPPINGS:
            config = TABLE_MAPPINGS[csv_file]
            if load_csv_with_fk_mapping(conn, csv_file, config["table"], 
                                       config["columns"], config.get("skip", False)):
                success_count += 1
    
    conn.close()
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH: {success_count}/{len(TABLE_MAPPINGS)} bảng được xử lý")
    print("=" * 60)

    print("\n📊 LOAD SUMMARY (ALL ERD TABLES)")
    for table, stat in TABLE_STATS.items():
        print(
            f"{table:<15} | "
            f"inserted: {stat['inserted']:<10} | "
            f"skipped: {stat['skipped']}"
        )
 
    # Hiển thị ID mapping summary
    print("\n📊 ID Mapping Summary:")
    for id_col, mapping in ID_MAPPING.items():
        if mapping:
            print(f"  {id_col}: {len(mapping)} mappings")

if __name__ == "__main__":
    main()