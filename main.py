# %%
from etl.extract import load_latest_xlsx_by_modified_date

df = load_latest_xlsx_by_modified_date()
df.head()

# %%
from etl.transform import clean_raw_dataframe

df_clean = clean_raw_dataframe(df)

# %%
from etl.backup_loader import load_dataframe

load_dataframe(df_clean, table_name="prop_extract", schema="stg", method="append")


