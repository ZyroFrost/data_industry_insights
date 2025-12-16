# Steps:
# 1. Download job datasets from Google Drive
# 2. Standardize country names (codes, aliases, native names)
# 3. Geo-fill missing country (spatial join) and city (KDTree)
# 4. Save enriched datasets

# Input:
# - Job data (Google Drive): input_files_map
# - Country shapefile: geodata/ne_110m_countries/ne_110m_admin_0_countries.shp
# - City shapefile: geodata/ne_110m_populated_places/ne_110m_populated_places.shp

# Output:
# - Enriched CSV files:
#   data/data_processing/data_enriched/*_enriched.csv

# How to run:
# - python <script_name>.py

# Result:
# - Cleaned & geo-enriched job datasets ready for analysis

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from scipy.spatial import cKDTree
import os
import pycountry
import gdown
import io 
import unicodedata
import re

# =========================
# 1. DECLARE PATHS AND FILES
# =========================

REPO_BASE_DIR = os.getcwd() 

path_parts = REPO_BASE_DIR.split(os.sep)
try:
    if "data_industry_insights-main" in path_parts:
        idx = path_parts.index("data_industry_insights-main") 
        CLEAN_BASE_PATH = os.sep.join(path_parts[:idx+1])
    else:
        CLEAN_BASE_PATH = REPO_BASE_DIR
except ValueError:
    CLEAN_BASE_PATH = REPO_BASE_DIR

BASE_DATA_DIR = os.path.join(CLEAN_BASE_PATH, "geodata")
COUNTRY_SHAPEFILE = os.path.join(BASE_DATA_DIR, "ne_110m_countries", "ne_110m_admin_0_countries.shp")
CITY_SHAPEFILE = os.path.join(BASE_DATA_DIR, "ne_110m_populated_places", "ne_110m_populated_places.shp")

OUTPUT_SUBDIR = "data/data_processing/data_enriched"
OUTPUT_DIR = os.path.join(REPO_BASE_DIR, OUTPUT_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

input_files_map = {
    "mapped_remoteok_datajobs_2025.csv": "1-H8k4W5C1uccPMZdVhGxgkajWZNwp-0I", 
    "mapped_ds_salaries_2020-2022.csv":"1qAM-WRo-95xCndca8U6T0ubGK2FS5BUm", 
    "mapped_DS_AI_ML_jobs_2020-2025.csv":"17RvAns9bWhZoddhsYt54Wj11cJmq-4LS", 
    "mapped_DataScientist.csv":"1Id09fjul6OASM6jxwT_9twTCd2dBa5U-", 
    "mapped_canada_government_datajobs_2020-2025.csv":"1d0QUSt1vNjde-bP9KyScswXmd7X6Xe-g", 
    "mapped_usajobs_2020-2025.csv":"1XoEk_SkJph0hPN--mnYZ5_MgstkTHJQ2", 
    "mapped_adzuna_datajobs_2025.csv":"1Y240a6iJG5jQX-AfxUqS68JIoYSMAVTT" 
}

# =========================
# 2. ENHANCED HELPER FUNCTIONS
# =========================

def normalize_text(text):
    """
    Normalize text by:
    1. Converting to string and stripping whitespace
    2. Removing accents/diacritics
    3. Converting to uppercase
    4. Removing extra spaces
    """
    if pd.isna(text):
        return ''
    
    text = str(text).strip()
    
    # Remove accents (é -> e, ñ -> n, etc.)
    text = unicodedata.normalize('NFKD', text)
    text = ''.join([c for c in text if not unicodedata.combining(c)])
    
    # Convert to uppercase and clean spaces
    text = text.upper()
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def create_country_mapping():
    """
    Create comprehensive country name mapping dictionary.
    Keys are normalized (uppercase, no accents), values are standard English names.
    """
    mapping = {
        # Standard English names (self-mapping for already correct names)
        'UNITED STATES': 'United States',
        'UNITED KINGDOM': 'United Kingdom',
        'GERMANY': 'Germany',
        'FRANCE': 'France',
        'SPAIN': 'Spain',
        'ITALY': 'Italy',
        'NETHERLANDS': 'Netherlands',
        'BELGIUM': 'Belgium',
        'SWITZERLAND': 'Switzerland',
        'AUSTRIA': 'Austria',
        'POLAND': 'Poland',
        'PORTUGAL': 'Portugal',
        'GREECE': 'Greece',
        'SWEDEN': 'Sweden',
        'NORWAY': 'Norway',
        'DENMARK': 'Denmark',
        'FINLAND': 'Finland',
        'IRELAND': 'Ireland',
        'CZECHIA': 'Czechia',
        'HUNGARY': 'Hungary',
        'ROMANIA': 'Romania',
        'BULGARIA': 'Bulgaria',
        'CROATIA': 'Croatia',
        'SLOVAKIA': 'Slovakia',
        'SLOVENIA': 'Slovenia',
        'LITHUANIA': 'Lithuania',
        'LATVIA': 'Latvia',
        'ESTONIA': 'Estonia',
        'RUSSIA': 'Russia',
        'UKRAINE': 'Ukraine',
        'TURKEY': 'Turkey',
        'CHINA': 'China',
        'JAPAN': 'Japan',
        'SOUTH KOREA': 'South Korea',
        'INDIA': 'India',
        'INDONESIA': 'Indonesia',
        'THAILAND': 'Thailand',
        'VIETNAM': 'Vietnam',
        'PHILIPPINES': 'Philippines',
        'MALAYSIA': 'Malaysia',
        'SINGAPORE': 'Singapore',
        'PAKISTAN': 'Pakistan',
        'BANGLADESH': 'Bangladesh',
        'AUSTRALIA': 'Australia',
        'NEW ZEALAND': 'New Zealand',
        'BRAZIL': 'Brazil',
        'MEXICO': 'Mexico',
        'ARGENTINA': 'Argentina',
        'CHILE': 'Chile',
        'COLOMBIA': 'Colombia',
        'PERU': 'Peru',
        'VENEZUELA': 'Venezuela',
        'CANADA': 'Canada',
        'SOUTH AFRICA': 'South Africa',
        'EGYPT': 'Egypt',
        'NIGERIA': 'Nigeria',
        'KENYA': 'Kenya',
        'MOROCCO': 'Morocco',
        'ALGERIA': 'Algeria',
        'TUNISIA': 'Tunisia',
        'SAUDI ARABIA': 'Saudi Arabia',
        'ISRAEL': 'Israel',
        'UNITED ARAB EMIRATES': 'United Arab Emirates',
        'KUWAIT': 'Kuwait',
        'QATAR': 'Qatar',
        'OMAN': 'Oman',
        'BAHRAIN': 'Bahrain',
        'JORDAN': 'Jordan',
        'LEBANON': 'Lebanon',
        'IRAN': 'Iran',
        'IRAQ': 'Iraq',
        'AFGHANISTAN': 'Afghanistan',
        'NEPAL': 'Nepal',
        'SRI LANKA': 'Sri Lanka',
        'MYANMAR': 'Myanmar',
        'CAMBODIA': 'Cambodia',
        'LAOS': 'Laos',
        'MONGOLIA': 'Mongolia',
        'NORTH KOREA': 'North Korea',
        'TAIWAN': 'Taiwan',
        'HONG KONG': 'Hong Kong',
        'MACAU': 'Macau',
        'COSTA RICA': 'Costa Rica',
        'PANAMA': 'Panama',
        'CUBA': 'Cuba',
        'JAMAICA': 'Jamaica',
        'DOMINICAN REPUBLIC': 'Dominican Republic',
        'PUERTO RICO': 'Puerto Rico',
        'TRINIDAD AND TOBAGO': 'Trinidad and Tobago',
        'ECUADOR': 'Ecuador',
        'BOLIVIA': 'Bolivia',
        'PARAGUAY': 'Paraguay',
        'URUGUAY': 'Uruguay',
        'GUYANA': 'Guyana',
        'SURINAME': 'Suriname',
        'HONDURAS': 'Honduras',
        'NICARAGUA': 'Nicaragua',
        'EL SALVADOR': 'El Salvador',
        'GUATEMALA': 'Guatemala',
        'BELIZE': 'Belize',
        

        # Encoding Errors (Windows-1252 artifacts)
        'ESPAA': 'Spain',
        'ESPANA': 'Spain',
        'OSTERREICH': 'Austria',
        'BELGIE': 'Belgium',
        'COTE D\'IVOIRE': 'Côte d\'Ivoire',
        'COTE DIVOIRE': 'Côte d\'Ivoire',
        
        # Native Names - European
        'BELGIE': 'Belgium',
        'BELGIQUE': 'Belgium',
        'BRASIL': 'Brazil',
        'DEUTSCHLAND': 'Germany',
        'ALLEMAGNE': 'Germany',
        'ESPANA': 'Spain',
        'MEXICO': 'Mexico',
        'NEDERLAND': 'Netherlands',
        'PAYS-BAS': 'Netherlands',
        'PAISES BAJOS': 'Netherlands',
        'OSTERREICH': 'Austria',
        'AUTRICHE': 'Austria',
        'POLSKA': 'Poland',
        'POLOGNE': 'Poland',
        'SCHWEIZ': 'Switzerland',
        'SUISSE': 'Switzerland',
        'SVIZZERA': 'Switzerland',
        'SUIZA': 'Switzerland',
        'ITALIA': 'Italy',
        'ITALIE': 'Italy',
        'GRECE': 'Greece',
        'GRECIA': 'Greece',
        'ELLADA': 'Greece',
        
        # Native Names - Asia
        'CHINE': 'China',
        'ZHONGGUO': 'China',
        'NIPPON': 'Japan',
        'JAPON': 'Japan',
        'HANGUK': 'South Korea',
        'COREE DU SUD': 'South Korea',
        'COREA DEL SUR': 'South Korea',
        'BHARAT': 'India',
        'INDE': 'India',
        
        # Native Names - Americas
        'ESTADOS UNIDOS': 'United States',
        'ETATS-UNIS': 'United States',
        'STATI UNITI': 'United States',
        'CANADA': 'Canada',
        
        # Native Names - Eastern Europe
        'CESKA REPUBLIKA': 'Czechia',
        'REPUBLIQUE TCHEQUE': 'Czechia',
        'REPUBLIKA CZECH': 'Czechia',
        'CESKO': 'Czechia',
        'ROSSIYA': 'Russia',
        'RUSSIE': 'Russia',
        'RUSSKAIA FEDERATSIIA': 'Russia',
        'ROSJA': 'Russia',
        'UKRAINA': 'Ukraine',
        'MAGYARORSZAG': 'Hungary',
        'HONGRIE': 'Hungary',
        'ROMANIA': 'Romania',
        'ROUMANIE': 'Romania',
        
        # Common Abbreviations
        'UK': 'United Kingdom',
        'U.K.': 'United Kingdom',
        'GB': 'United Kingdom',
        'US': 'United States',
        'U.S.': 'United States',
        'USA': 'United States',
        'U.S.A.': 'United States',
        'UAE': 'United Arab Emirates',
        'U.A.E.': 'United Arab Emirates',
        'NL': 'Netherlands',
        'CH': 'Switzerland',
        'CZ': 'Czechia',
        'PL': 'Poland',
        'PT': 'Portugal',
        'ES': 'Spain',
        'IT': 'Italy',
        'FR': 'France',
        'DE': 'Germany',
        'AT': 'Austria',
        'BE': 'Belgium',
        'SE': 'Sweden',
        'NO': 'Norway',
        'DK': 'Denmark',
        'FI': 'Finland',
        'IE': 'Ireland',
        'GR': 'Greece',
        'TR': 'Turkey',
        'RU': 'Russia',
        'CN': 'China',
        'JP': 'Japan',
        'KR': 'South Korea',
        'IN': 'India',
        'BR': 'Brazil',
        'MX': 'Mexico',
        'CA': 'Canada',
        'AU': 'Australia',
        'NZ': 'New Zealand',
        'ZA': 'South Africa',
        'AR': 'Argentina',
        'CL': 'Chile',
        'CO': 'Colombia',
        'PE': 'Peru',
        'VE': 'Venezuela',
        'MY': 'Malaysia',
        'SG': 'Singapore',
        'TH': 'Thailand',
        'ID': 'Indonesia',
        'PH': 'Philippines',
        'VN': 'Vietnam',
        'PK': 'Pakistan',
        'BD': 'Bangladesh',
        'EG': 'Egypt',
        'NG': 'Nigeria',
        'KE': 'Kenya',
        'SA': 'Saudi Arabia',
        'IL': 'Israel',
        
        # Alternative English Names
        'UNITED STATES OF AMERICA': 'United States',
        'GREAT BRITAIN': 'United Kingdom',
        'ENGLAND': 'United Kingdom',
        'SCOTLAND': 'United Kingdom',
        'WALES': 'United Kingdom',
        'NORTHERN IRELAND': 'United Kingdom',
        'HOLLAND': 'Netherlands',
        'CZECH REPUBLIC': 'Czechia',
        'SOUTH KOREA': 'South Korea',
        'REPUBLIC OF KOREA': 'South Korea',
        'NORTH KOREA': 'North Korea',
        'DEMOCRATIC PEOPLE\'S REPUBLIC OF KOREA': 'North Korea',
        'PEOPLE\'S REPUBLIC OF CHINA': 'China',
        'RUSSIAN FEDERATION': 'Russia',
        'REPUBLIC OF IRELAND': 'Ireland',
        'HELLENIC REPUBLIC': 'Greece',
        'REPUBLIC OF TURKEY': 'Turkey',
        'TURKIYE': 'Turkey',
        
        # Special Cases with Parentheses
        'VENEZUELA (BOLIVARIAN REPUBLIC OF)': 'Venezuela',
        'BOLIVIA (PLURINATIONAL STATE OF)': 'Bolivia',
        'IRAN (ISLAMIC REPUBLIC OF)': 'Iran',
        'KOREA (REPUBLIC OF)': 'South Korea',
        'KOREA (DEMOCRATIC PEOPLE\'S REPUBLIC OF)': 'North Korea',
        'TANZANIA (UNITED REPUBLIC OF)': 'Tanzania',
        'MACEDONIA (THE FORMER YUGOSLAV REPUBLIC OF)': 'North Macedonia',
        'MOLDOVA (REPUBLIC OF)': 'Moldova',
        'SYRIA (SYRIAN ARAB REPUBLIC)': 'Syria',
        'PALESTINE (STATE OF)': 'Palestine',
        'CONGO (DEMOCRATIC REPUBLIC OF THE)': 'Democratic Republic of the Congo',
        'CONGO (REPUBLIC OF THE)': 'Republic of the Congo',
        
        # Remove "THE" prefix
        'THE NETHERLANDS': 'Netherlands',
        'THE UNITED STATES': 'United States',
        'THE UNITED KINGDOM': 'United Kingdom',
        'THE BAHAMAS': 'Bahamas',
        'THE GAMBIA': 'Gambia',
    }
    
    return mapping


def convert_alpha2_to_name(code):
    """
    Convert 2-letter (Alpha-2) country code to full country name.
    """
    if pd.isna(code) or str(code).strip().upper() in ['', '__NA__', 'N/A']:
        return code
    
    code = str(code).strip().upper()
    
    if len(code) == 2 and code.isalpha():
        try:
            country_object = pycountry.countries.get(alpha_2=code)
            if country_object:
                return country_object.name
            else:
                return code 
        except KeyError:
            return code
    return code


def standardize_country_name(country_value):
    """
    Main standardization function that applies all transformations.
    """
    if pd.isna(country_value) or str(country_value).strip() in ['', '__NA__', 'N/A']:
        return country_value
    
    # Step 1: Convert Alpha-2 codes
    country_value = convert_alpha2_to_name(country_value)
    
    # Step 2: Normalize the text (remove accents, uppercase, clean spaces)
    normalized = normalize_text(country_value)
    
    # Step 3: Apply mapping
    mapping = create_country_mapping()
    if normalized in mapping:
        return mapping[normalized]
    
    # Step 4: Try pycountry fuzzy search as fallback
    try:
        search_results = pycountry.countries.search_fuzzy(str(country_value))
        if search_results:
            return search_results[0].name
    except LookupError:
        pass
    
    # Step 5: Return original if no match found
    return country_value


# =========================
# 3. MAIN PROCESSING FUNCTION
# =========================

def process_data_file(filename, drive_id):
    """
    Downloads, cleans (standardizes names), fills missing geodata, and saves as CSV.
    """
    output_filename = filename.replace('.csv', '_enriched.csv') 
    save_path = os.path.join(OUTPUT_DIR, output_filename)
    temp_file_path = f"temp_{drive_id}.xlsx"

    print(f"\n--- Processing file: {filename} (ID: {drive_id}) ---")
    
    # --- STEP 1: DOWNLOAD & READ ---
    try:
        url = f'https://drive.google.com/uc?id={drive_id}'
        gdown.download(url, output=temp_file_path, quiet=True) 
        data = pd.read_excel(temp_file_path, engine='openpyxl') 
        os.remove(temp_file_path)
        print("-> Step 1: File successfully downloaded and read.")
    except Exception as e:
        print(f"ERROR: Step 1 failed (Download/Read). Error: {e}")
        return

    # Check required columns
    required_cols = ['city', 'country', 'latitude', 'longitude']
    if not all(col in data.columns for col in required_cols):
        print(f"Warning: File {filename} is missing columns {required_cols}. Saving without GeoFill.")
        data.to_csv(save_path, index=False, encoding='utf-8') 
        return

    # Standardize empty values
    data['city'] = data['city'].fillna('').astype(str).str.strip()
    data['country'] = data['country'].fillna('').astype(str).str.strip()

    # --- STEP 2: STANDARDIZE COUNTRY NAMES (HYBRID APPROACH) ---
    try:
        print("-> Step 2: Starting country name standardization...")
        
        # Show sample before standardization
        unique_before = data['country'].unique()[:10]
        print(f"   Sample countries before: {unique_before}")
        
        total_rows = len(data)
        print(f"   Total rows: {total_rows:,}")
        
        # STEP 2A: Fast vectorized mapping (handles 95%+ of cases)
        mapping = create_country_mapping()
        data['country_normalized'] = data['country'].apply(normalize_text)
        data['country_mapped'] = data['country_normalized'].map(mapping)
        
        # Count how many were successfully mapped
        mapped_count = data['country_mapped'].notna().sum()
        print(f"   -> Vectorized mapping: {mapped_count:,}/{total_rows:,} rows ({mapped_count/total_rows*100:.1f}%)")
        
        # STEP 2B: Handle unmapped rows with full standardization (edge cases)
        unmapped_mask = data['country_mapped'].isna()
        unmapped_count = unmapped_mask.sum()
        
        if unmapped_count > 0:
            print(f"   -> Processing {unmapped_count:,} unmapped rows with full standardization...")
            data.loc[unmapped_mask, 'country_mapped'] = data.loc[unmapped_mask, 'country'].apply(standardize_country_name)
        
        # STEP 2C: Update country column and cleanup
        data['country'] = data['country_mapped']
        data.drop(['country_normalized', 'country_mapped'], axis=1, inplace=True)
        
        # Show sample after standardization
        unique_after = data['country'].unique()[:10]
        print(f"   Sample countries after: {unique_after}")
        print("-> Step 2: Country name standardization completed.")
    except Exception as e:
        print(f"ERROR: Step 2 failed. Error: {e}")

    # --- STEP 3: GEOFILL (SPATIAL FILLING) ---
    
    valid_coords = data['latitude'].notna() & data['longitude'].notna() & \
                   (~data['latitude'].astype(str).str.strip().isin(['__NA__', 'N/A'])) & \
                   (~data['longitude'].astype(str).str.strip().isin(['__NA__', 'N/A']))
    
    subset = data[valid_coords].copy()

    if subset.empty:
        print("-> Step 3: No valid coordinates for GeoFill. Skipping.")
        data.to_csv(save_path, index=False, encoding='utf-8') 
        return

    # Create GeoDataFrame
    try:
        geometry = [Point(float(xy[0]), float(xy[1])) 
                    for xy in zip(subset['longitude'], subset['latitude'])]
        geo_df = gpd.GeoDataFrame(subset, geometry=geometry, crs="EPSG:4326")
    except Exception as e:
        print(f"Error creating GeoDataFrame: {e}. Skipping GeoFill.")
        data.to_csv(save_path, index=False, encoding='utf-8')
        return

    # Load Shapefiles
    if not os.path.exists(COUNTRY_SHAPEFILE):
        print(f"ERROR: Shapefile not found at {COUNTRY_SHAPEFILE}. Skipping GeoFill.")
        data.to_csv(save_path, index=False, encoding='utf-8')
        return

    try:
        world = gpd.read_file(COUNTRY_SHAPEFILE)
        cities = gpd.read_file(CITY_SHAPEFILE).to_crs(geo_df.crs)
    except Exception as e:
        print(f"Error reading Shapefiles: {e}. Skipping GeoFill.")
        data.to_csv(save_path, index=False, encoding='utf-8')
        return

    # Helper for missing check
    def is_missing(series):
        return series.str.strip().isin(['', 'N/A', '__NA__']).fillna(False)

    # 3.1. Fill Country (Spatial Join)
    missing_country_mask = is_missing(geo_df['country'])
    if missing_country_mask.any():
        temp = gpd.sjoin(geo_df[missing_country_mask], world[['geometry','NAME']], how='left', predicate='within')
        found = temp['NAME'].dropna()
        geo_df.loc[found.index, 'country'] = found.values
        print(f"-> Step 3.1: Filled {len(found)} missing countries via Spatial Join.") 

    # 3.2. Fill City (K-D Tree)
    missing_city_mask = is_missing(geo_df['city'])
    if missing_city_mask.any():
        city_coords = list(zip(cities.geometry.x, cities.geometry.y))
        tree = cKDTree(city_coords) 
        points = list(zip(geo_df.loc[missing_city_mask].geometry.x, geo_df.loc[missing_city_mask].geometry.y))
        dist, idx = tree.query(points)
        geo_df.loc[missing_city_mask, 'city'] = cities.iloc[idx]['NAME'].values
        print(f"-> Step 3.2: Filled {len(points)} missing cities via Nearest Neighbor.") 

    # Update original data and save
    data.update(geo_df.drop(columns=['geometry']).set_index(geo_df.index))
    data.to_csv(save_path, index=False, encoding='utf-8')
    print(f"-> PROCESSING COMPLETE. Output saved to: {output_filename}")
    print("-" * 50)


# =========================
# 4. EXECUTION LOOP
# =========================

print(f"Starting processing of {len(input_files_map)} files...")

for filename, drive_id in input_files_map.items():
    process_data_file(filename, drive_id)

print(f"\n=== SUCCESS: All 7 files processed and saved to {OUTPUT_SUBDIR} ===")