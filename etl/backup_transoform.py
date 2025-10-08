# %% [markdown]
# # Transform: Prepping the DF before upload to Postgres

# %% [markdown]
# clean_column_names(df)
# 
# Standardizes column names by:
# 	•	Stripping whitespace
# 	•	Lowercasing all names
# 	•	Replacing spaces with underscores
# 	•	Removing non-alphanumeric characters
# 
# ⸻
# 
# clean_raw_dataframe(df)
# 
# Cleans a raw Pandas DataFrame by:
# 	•	Standardizing column names using clean_column_names()
# 	•	Stripping whitespace from all string columns
# 	•	Replacing empty strings and 'n/a' with np.nan
# 	•	Adding an extract_date column if missing
# 	•	Converting selected columns to numeric or datetime types
# 	•	(Optionally) deduplicating rows if needed

# %%
import pandas as pd
import numpy as np
from datetime import datetime

def clean_column_names(df):
    """
    Standardize column names: lowercase, underscores, strip whitespace
    """
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_")
                  .str.replace(r"[^\w_]", "", regex=True)
    )
    return df


def clean_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw dataframe: column names, empty values, data types, etc.
    """
    # 1. Standardize column names
    df = clean_column_names(df)

    # 2. Strip whitespace from string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].astype(str).apply(lambda x: x.str.strip())

    # 3. Replace empty strings or 'n/a' with np.nan
    df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    df.replace("n/a", np.nan, inplace=True)

    # 4. Add extract date (if not already present)
    if "extract_date" not in df.columns:
        df["extract_date"] = pd.Timestamp.today().normalize()

    # 5. Example: convert specific columns (adjust as needed)
    numeric_cols = ["bedrooms", "total_bathrooms", "building_sqft", 
                    "total_assessed_value", "improvement_to_tax_value_", "last_sale_amount",
                    "building_sqft", "lot_size_sqft", "assessed_improvement_value"
                    "loan_1_balance", "loan_1_rate", "loan_2_balance",
                    "loan_2_rate", "loan_3_balance", "loan_3_rate",
                    "loan_4_balance", "loan_4_rate", "total_open_loans",
                    "est_remaining_balance_of_open_loans", "est_value", "est_loantovalue"
                    "est_equity", "mls_amount", "lien_amount", 
                    "prefc_unpaid_balance", "prefc_default_amount", "prefc_auction_opening_bid" 
                    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")  # NA if can't convert

    date_cols = ["effective_year_built", "last_sale_date", "last_sale_recording_date",
                 "prior_sale_date", "loan_1_date", "loan_2_date",
                 "loan_3_date", "loan_4_date", "mls_date",
                 "lien_date", "bk_date", "divorce_date", 
                   "prefc_recording_date", "prefc_auction_date", "date_added_to_list", "extract_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 6. Optional: deduplicate (choose logic)
    # df = df.drop_duplicates(subset=[...])

    return df

# %%
from etl.extract import load_latest_xlsx_by_modified_date

df_raw = load_latest_xlsx_by_modified_date()
df_clean = clean_raw_dataframe(df_raw)

print(df_clean.head())


