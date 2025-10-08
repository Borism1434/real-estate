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
import re


def clean_column_names(df):
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(' ', '_', regex=False)
                  .str.replace('#', 'num', regex=False)
    )
    return df


def clean_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw dataframe: fix column names, handle nulls, convert types, etc.
    """

    # --- Standardize and sanitize column names ---
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )

    # --- Manual renames ---
    df.rename(columns={
        "mls_agent_e_mail": "mls_agent_email",
        "agent_e_mail": "agent_email",
        "owner_1_e_mail": "owner_1_email",
        "pre_fc_auction_date": "prefc_auction_date"
    }, inplace=True)

    # --- Replace blanks / n/a / whitespace ---
    df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    df.replace(["n/a", "N/A", "na", "NA", ""], np.nan, inplace=True)

    # --- Add extract_date if missing ---
    if "extract_date" not in df.columns:
        df["extract_date"] = pd.Timestamp.today().normalize()

    # --- Numeric columns ---
    numeric_cols = [
        "bedrooms", "total_bathrooms", "building_sqft", "total_assessed_value",
        "improvement_to_tax_value", "last_sale_amount", "lot_size_sqft",
        "assessed_improvement_value", "loan_1_balance", "loan_1_rate",
        "loan_2_balance", "loan_2_rate", "loan_3_balance", "loan_3_rate",
        "loan_4_balance", "loan_4_rate", "total_open_loans",
        "est_remaining_balance_of_open_loans", "est_value", "est_loantovalue",
        "est_equity", "mls_amount", "lien_amount", "prefc_unpaid_balance",
        "prefc_default_amount", "prefc_auction_opening_bid"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Handle effective_year_built (Int64) ---
    if "effective_year_built" in df.columns:
        df["effective_year_built"] = pd.to_numeric(df["effective_year_built"], errors="coerce")
        df["effective_year_built"] = df["effective_year_built"].apply(
            lambda x: int(x) if pd.notnull(x) and not np.isnan(x) else None
        ).astype("Int64")

    # --- Convert date-like columns ---
    date_cols = [
        "last_sale_date", "last_sale_recording_date", "prior_sale_date",
        "loan_1_date", "loan_2_date", "loan_3_date", "loan_4_date",
        "mls_date", "lien_date", "bk_date", "divorce_date",
        "pre_fc_recording_date", "prefc_auction_date",
        "date_added_to_list", "extract_date"
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # --- Replace NaN/NaT with None ---
    df = df.replace({np.nan: None, pd.NaT: None})

    # ✅ NEW FIX: ensure no "None" strings go into CSV
    # Replace actual None values (Python object) with empty string before COPY
    df = df.applymap(lambda x: "" if x is None else x)

        # --- Ensure missing values are truly blank strings ---
    df = df.replace({np.nan: "", pd.NaT: "", None: ""})

    df.rename(columns={"prefc_recording_date": "pre_fc_recording_date"}, inplace=True)

    return df


