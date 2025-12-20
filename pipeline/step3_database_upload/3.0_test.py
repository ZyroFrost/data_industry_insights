import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "data_processed"

csv_files = [
    "companies.csv",
    "locations.csv", 
    "role_names.csv",
    "skills.csv",
    "job_postings.csv",
    "job_roles.csv",
    "job_levels.csv",
    "job_skills.csv"
]

for csv_file in csv_files:
    file_path = DATA_DIR / csv_file
    if file_path.exists():
        print(f"\n{'='*60}")
        print(f"📄 {csv_file}")
        print('='*60)
        
        # Kiểm tra file size
        file_size = file_path.stat().st_size
        print(f"File size: {file_size} bytes")
        
        if file_size == 0:
            print("❌ File rỗng!")
            continue
        
        try:
            # Đọc với error handling
            df = pd.read_csv(file_path, nrows=5, on_bad_lines='skip')
            print(f"Columns ({len(df.columns)}): {list(df.columns)}")
            print(f"Shape: {df.shape}")
            print(f"Total rows in file: {len(pd.read_csv(file_path))}")
            print("\nFirst 3 rows:")
            print(df.head(3))
            print(f"\nData types:")
            print(df.dtypes)
        except pd.errors.EmptyDataError:
            print("❌ File không có dữ liệu hoặc chỉ có header!")
        except Exception as e:
            print(f"❌ Lỗi đọc file: {e}")
            # Thử đọc raw text
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()[:5]
                print(f"First 5 lines (raw):")
                for i, line in enumerate(lines, 1):
                    print(f"  {i}: {line.strip()}")
    else:
        print(f"\n⚠️  {csv_file} không tồn tại")