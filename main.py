# %%
from etl.extract import load_latest_xlsx_by_modified_date

df = load_latest_xlsx_by_modified_date()
df.head()

# from etl.extract import load_all_extracts
# df = load_all_extracts()

# %%
from etl.transform import clean_raw_dataframe

df_clean = clean_raw_dataframe(df)

# %%
from etl.loader import load_dataframe

load_dataframe(
    df = df_clean,
    table_name="prop_extract",
    schema="stg",
    data_dir="/Users/borismartinez/Documents/real-estate/data",
    load_mode="recent"
)



