# main.py

# -------------------------------
# IMPORTS
# -------------------------------
# ETL extract/transform/load functions from your project modules
from etl.extract import load_latest_xlsx_by_modified_date
from etl.transform import clean_raw_dataframe
from etl.loader import load_dataframe
from etl.gsheet import upload_df_to_gsheet, add_zillow_link_column, export_and_process_data, clean_export_dataframe
from etl.loader import run_query

# Google Sheets API libraries for uploading final data
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# -------------------------------
# MAIN ETL PIPELINE
# -------------------------------

# 1. EXTRACT:
# Load the latest raw data file (xlsx) based on modified date.
# This extracts your most recent data snapshot into a DataFrame.
df = load_latest_xlsx_by_modified_date()

# 2. TRANSFORM:
# Apply cleaning and standardization logic to raw data.
# This can include formatting, deduplication, handling missing data, etc.
df_clean = clean_raw_dataframe(df)

# 3. LOAD:
# Load the cleaned dataframe into your database or staging table.
# This prepares it for downstream SQL transformations and BI usage.
load_dataframe(
    df=df_clean,
    table_name="prop_extract",
    schema="stg",
    data_dir="/Users/borismartinez/Documents/real-estate/data",
    load_mode="recent"
)

# 4. DBT:
# (Not shown in code here) 
# Use dbt models to build transformations:
#  - src: sources
#  - int: intermediate cleaning and joining
#  - analytics: final datasets optimized for reporting
# This ensures modular, tested, and version controlled SQL transformations. 

# 5. EXPORT TO GOOGLE SHEETS:
# Upload the cleaned dataframe to a Google Sheet tab for end-users.
# This step applies any final presentation logic (like formatted columns or
# adding hyperlinks) in Python before pushing.
creds_path = "api_access.json"        # Service account credentials JSON
sheet_title = "Lead Generation Tool"  # Exact Google Sheet name
tab_name = "Single Family Leads_1"    # Target worksheet/tab name

# This runs the export query on the final dbt table and applies all formatting + Zillow Link
df_final = export_and_process_data()

upload_df_to_gsheet(df_final, tab_name, creds_path, sheet_title)