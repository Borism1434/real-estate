# %%
# Extract Functions
import sys
import os
import pandas as pd
from datetime import datetime

PROJECT_ROOT = "/Users/borismartinez/Documents/real-estate"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.paths import DATA_DIR, FILENAME_DATE_FORMAT, DEFAULT_EXTRACT_LABEL, PARQUET_ENABLED


# --- Load the most recent XLSX (your original version) ---
def load_latest_xlsx_by_modified_date(dtype=str) -> pd.DataFrame:
    """
    Loads the most recently modified XLSX file from DATA_DIR.
    Saves a .parquet version alongside it.
    """
    xlsx_files = [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".xlsx") and os.path.isfile(os.path.join(DATA_DIR, f))
    ]
    if not xlsx_files:
        raise FileNotFoundError(f"No .xlsx files found in {DATA_DIR}")

    latest_file = max(xlsx_files, key=os.path.getmtime)
    modified_time = os.path.getmtime(latest_file)
    extract_date = datetime.fromtimestamp(modified_time).date()
    date_str = extract_date.strftime(FILENAME_DATE_FORMAT)

    clean_filename = f"{date_str}_{DEFAULT_EXTRACT_LABEL}.xlsx"
    clean_path = os.path.join(DATA_DIR, clean_filename)

    if os.path.basename(latest_file) != clean_filename:
        os.rename(latest_file, clean_path)
        print(f"Renamed '{os.path.basename(latest_file)}' → '{clean_filename}'")
    else:
        clean_path = latest_file

    df = pd.read_excel(clean_path, dtype=dtype)
    df["extract_date"] = extract_date

    if PARQUET_ENABLED:
        import pyarrow as pa
        import pyarrow.parquet as pq
        parquet_path = clean_path.replace(".xlsx", ".parquet")
        table = pa.Table.from_pandas(df, preserve_index=False, safe=False)
        pq.write_table(table, parquet_path)
        print(f"Saved Parquet version: {parquet_path}")

    return df


# --- NEW: Load and merge all XLSX + Parquet files ---
def load_all_extracts(dtype=str) -> pd.DataFrame:
    """
    Loads and merges ALL .xlsx and .parquet extracts in DATA_DIR.
    Useful for full-table reloads (e.g., prop_extract).
    """
    import glob

    file_patterns = ["*.xlsx", "*.parquet"]
    files = []
    for pattern in file_patterns:
        files.extend(glob.glob(os.path.join(DATA_DIR, pattern)))

    if not files:
        raise FileNotFoundError(f"No .xlsx or .parquet files found in {DATA_DIR}")

    files.sort(key=os.path.getmtime, reverse=True)
    print(f"🗂 Found {len(files)} file(s) in {DATA_DIR}")

    dfs = []
    for f in files:
        try:
            if f.endswith(".xlsx"):
                temp_df = pd.read_excel(f, dtype=dtype)
            elif f.endswith(".parquet"):
                temp_df = pd.read_parquet(f)
            else:
                continue
            dfs.append(temp_df)
            print(f"   ✅ {os.path.basename(f)} → {len(temp_df)} rows")
        except Exception as e:
            print(f"❌ Failed to read {f}: {e}")

    df = pd.concat(dfs, ignore_index=True)
    print(f"📈 Combined DataFrame shape: {df.shape}")
    return df