# %%
# loader.py
from sqlalchemy import create_engine, text
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO
from dotenv import load_dotenv
import glob
import re

# Load environment variables from .env
load_dotenv()

# Config dict for connection
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}

# --- SQLAlchemy Engine ---
def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)

# --- psycopg2 raw connection (for copy_expert) ---
def get_psycopg2_conn():
    return psycopg2.connect(
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )

# --- Read query into DataFrame ---
def run_query(query: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# --- Execute raw SQL (DDL or DML) ---
def execute_sql(sql: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql))

# --- Load a DataFrame to a PostgreSQL table (fast) ---
import os
import glob


def load_dataframe(
    df: pd.DataFrame = None,
    table_name: str = "",
    schema: str = "src",
    method: str = "replace",
    data_dir: str = None,
    load_mode: str = "recent"  # or "all"
):
    import re
    import glob


    # --- Helper: clean and normalize column names ---
    def clean_column_names(df):
        df.columns = (
            df.columns
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )

        # Manual corrections for known problematic names
        df.rename(
            columns={
                "mls_agent_e_mail": "mls_agent_email",
                "agent_e_mail": "agent_email",
                "owner_1_e_mail": "owner_1_email",
            },
            inplace=True,
        )
        return df

    # --- Load files if df not provided ---
    if df is None and data_dir:
        file_patterns = ["*.csv", "*.CSV", "*.xlsx", "*.parquet"]
        files = []
        for pattern in file_patterns:
            files.extend(glob.glob(os.path.join(data_dir, pattern)))

        if not files:
            print("❌ No data files (.csv, .xlsx, .parquet) found in directory.")
            return

        files.sort(key=os.path.getmtime, reverse=True)
        print(f"🗂 Found {len(files)} file(s) in {data_dir}")

        if load_mode == "recent":
            files = [files[0]]
            print(f"📄 Loading only most recent file: {os.path.basename(files[0])}")
        else:
            print(f"📚 Loading ALL {len(files)} files (merged into one DataFrame)")

        dfs = []
        for f in files:
            try:
                if f.endswith((".csv", ".CSV")):
                    temp_df = pd.read_csv(f)
                elif f.endswith(".xlsx"):
                    temp_df = pd.read_excel(f)
                elif f.endswith(".parquet"):
                    temp_df = pd.read_parquet(f)
                else:
                    continue
                print(f"   ✅ {os.path.basename(f)} → {len(temp_df)} rows")
                dfs.append(temp_df)
            except Exception as e:
                print(f"❌ Failed to read {f}: {e}")

        df = pd.concat(dfs, ignore_index=True)
        print(f"📈 Combined DataFrame shape: {df.shape}")

    # --- Exit early if still no data ---
    if df is None or df.empty:
        print("⚠️ No valid data to load.")
        return

    # --- Clean and normalize column names ---
    df = clean_column_names(df)

    # --- Replace NaN with None for Postgres ---
    df = df.where(pd.notnull(df), None)

    # --- Load into Postgres ---
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            if method == "replace":
                cur.execute(f"TRUNCATE TABLE {schema}.{table_name};")

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="")
            buffer.seek(0)

            copy_sql = f"""
                COPY {schema}.{table_name} ({', '.join(df.columns)})
                FROM STDIN WITH CSV NULL ''
            """
            try:
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
                print(f"✅ Loaded {len(df)} rows into {schema}.{table_name}")
            except Exception as e:
                conn.rollback()
                print("❌ Load failed:", e)

    # --- Load files if df not provided ---
    if df is None and data_dir:
        file_patterns = ["*.csv", "*.CSV", "*.xlsx", "*.parquet"]
        files = []
        for pattern in file_patterns:
            files.extend(glob.glob(os.path.join(data_dir, pattern)))

        if not files:
            print("❌ No data files (.csv, .xlsx, .parquet) found in directory.")
            return

        files.sort(key=os.path.getmtime, reverse=True)
        print(f"🗂 Found {len(files)} file(s) in {data_dir}")

        if load_mode == "recent":
            files = [files[0]]
            print(f"📄 Loading only most recent file: {os.path.basename(files[0])}")
        else:
            print(f"📚 Loading ALL {len(files)} files (merged into one DataFrame)")

        dfs = []
        for f in files:
            try:
                if f.endswith((".csv", ".CSV")):
                    temp_df = pd.read_csv(f)
                elif f.endswith(".xlsx"):
                    temp_df = pd.read_excel(f)
                elif f.endswith(".parquet"):
                    temp_df = pd.read_parquet(f)
                else:
                    continue
                print(f"   ✅ {os.path.basename(f)} → {len(temp_df)} rows")
                dfs.append(temp_df)
            except Exception as e:
                print(f"❌ Failed to read {f}: {e}")

        df = pd.concat(dfs, ignore_index=True)
        print(f"📈 Combined DataFrame shape: {df.shape}")

    if df is None or df.empty:
        print("⚠️ No valid data to load.")
        return

    df = clean_column_names(df)
    df = df.where(pd.notnull(df), None)

    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            if method == "replace":
                cur.execute(f"TRUNCATE TABLE {schema}.{table_name};")

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="")
            buffer.seek(0)

            copy_sql = f"""
                COPY {schema}.{table_name} ({', '.join(df.columns)})
                FROM STDIN WITH CSV NULL ''
            """
            try:
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
                print(f"✅ Loaded {len(df)} rows into {schema}.{table_name}")
            except Exception as e:
                conn.rollback()
                print("❌ Load failed:", e)






def create_export_log_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS exported_properties_log (
        property_id VARCHAR PRIMARY KEY,
        export_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_sql(ddl)
    print("✅ ensured exported_properties_log table exists")       

                




def insert_uploaded_to_db(df: pd.DataFrame, table_name="stg__list_history", schema="stg"):
    """
    Insert rows from the dataframe into the 'stg__list_history' table.
    Only the 'apn' column is used as a unique listing identifier.
    Duplicate apn values will be ignored based on a unique constraint.
    """
    from psycopg2.extras import execute_values

    # Clean and check column
    df = df.copy()
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    if "apn" not in df.columns:
        raise Exception("Required column 'apn' not found in dataframe.")
    rows = [(apn,) for apn in df["apn"].dropna().astype(str)]

    if not rows:
        print("⚠️ No APNs to insert into stg__list_history.")
        return

    query = f"""
        INSERT INTO {schema}.{table_name} (apn)
        VALUES %s
        ON CONFLICT (apn) DO NOTHING
    """

    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
            conn.commit()
    print(f"✅ Inserted {len(rows)} APNs (deduplicated by DB) into {schema}.{table_name}")










# %%
# Testing Data Upload

# if __name__ == "__main__":
#     # ✅ Sample dummy DataFrame
#     import pandas as pd

#     df_test = pd.DataFrame({
#         "bedrooms": [3, 2, None],
#         "total_bathrooms": [2.5, 1.0, 1.5],
#         "last_sale_amount": [300000, None, 150000],
#         "last_sale_date": ["2021-07-01", "2020-05-12", None]
#     })

#     # 🧪 Make sure your target table exists first
#     # You can create it manually or via SQL below:
#     create_sql = """
#     CREATE TABLE IF NOT EXISTS src.test_real_estate (
#         bedrooms NUMERIC,
#         total_bathrooms NUMERIC,
#         last_sale_amount NUMERIC,
#         last_sale_date DATE
#     );
#     """
#     execute_sql(create_sql)

#     # 🚀 Run loader test
#     load_dataframe(df_test, table_name="test_real_estate", schema="src", method="replace")

#     # ✅ Query to verify insert
#     result = run_query("SELECT * FROM src.test_real_estate;")
#     print("✅ Loaded data:")
#     print(result)


