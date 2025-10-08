# %%
# db.py
from sqlalchemy import create_engine, text
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO
from dotenv import load_dotenv

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
def load_dataframe(df: pd.DataFrame, table_name: str, schema: str = "src", method="replace"):
    if df.empty:
        print("⚠️ DataFrame is empty. Skipping load.")
        return
    
    # Ensure NULL values are proper for Postgres
    df = df.where(pd.notnull(df), None)
    
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            if method == "replace":
                cur.execute(f"TRUNCATE TABLE {schema}.{table_name};")

            # Write CSV buffer
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            copy_sql = f"""
                COPY {schema}.{table_name} ({', '.join(df.columns)})
                FROM STDIN WITH CSV
            """
            try:
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
                print(f"✅ Loaded {len(df)} rows into {schema}.{table_name}")
            except Exception as e:
                conn.rollback()
                print("❌ Load failed:", e)

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


