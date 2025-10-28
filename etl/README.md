# etl Folder: Extract, Transform, Load Components for Real Estate Data

This folder contains core ETL modules used to handle real estate data extraction, cleaning, loading, and exporting tasks. Each module addresses a specific part of the ETL pipeline, working together to load raw data, clean and transform it, and ultimately upload and share it with the database and external systems.

---



## extract.py

- Provides data extraction functions from local file system.
- `load_latest_xlsx_by_modified_date()`: Finds and loads the most recently modified XLSX file under the data directory, renaming it to a normalized naming format.
- Optionally saves a Parquet version for faster future access.
- `load_all_extracts()`: Loads and merges all XLSX and Parquet extract files for full historical reloads.
- Supports flexible data types and handles multiple file formats.
- Useful for incremental and bulk data extraction workflows.

---

## loader.py

- Similar to `backup_loader.py`, provides utilities to connect to PostgreSQL and run queries.
- Has functions for reading SQL query results into pandas DataFrames and executing SQL commands.
- Supports fast DataFrame loading into Postgres tables using PostgreSQL's `COPY` with CSV through psycopg2.
- Extends loading to allow reading from local data files (`csv`, `xlsx`, `parquet`), with options to load the most recent or all files in a directory.
- Includes column name cleaning and normalization before loading.
- Handles connection parameters via environment variables.

---

## gsheet.py

- Handles reading from and writing DataFrames to Google Sheets using the Google Sheets API (`gspread`).
- `upload_df_to_gsheet()` uploads a DataFrame to a specified sheet/tab, clearing and resizing the target.
- Utility function `add_zillow_link_column()` appends a Zillow property link column to DataFrames based on address components.
- `clean_export_dataframe()` formats DataFrame columns (currencies, dates) for better display when exported.
- `export_and_process_data()` combines loading from DB, cleaning, formatting, adding Zillow links, and reordering columns into an end-to-end export process.
- Helps facilitate sharing processed data with stakeholders via Google Sheets.

---

# Summary

These modules collectively provide a robust ETL pipeline to:

- Extract raw data from local XLSX and Parquet files.
- Clean and standardize data for consistency and quality.
- Connect and execute operations on a PostgreSQL database.
- Load data efficiently into database tables.
- Export enhanced data views formatted for collaboration (Google Sheets, with Zillow links).
- The separation of concerns allows flexible modifications and reuse of extraction, transformation, and loading logic as needed.

---

