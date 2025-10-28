# %%
from dotenv import load_dotenv
import os
import sys
import pandas as pd

# Load environment variables
load_dotenv(dotenv_path=os.path.expanduser("~/Documents/real-estate/.env"))

# Add project root path for imports
project_root = os.path.abspath("..")
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# %%
from etl.loader import run_query

# Hardcoded list of columns to analyze
columns_to_analyze = [
    "price_per_sqft",
    "diff",
    "est_value",
    # Add other columns here
]

# Create SQL Select query for specified columns from analytics.analytics_single_prop
cols_str = ", ".join(columns_to_analyze)
query = f"SELECT {cols_str} FROM analytics.analytics_single_prop"

# Run query and get DataFrame
df = run_query(query)

# Function for basic terminal-friendly EDA summary
def basic_eda(df):
    for col in df.columns:
        print(f"\nColumn: {col}")
        print(f"  Type: {df[col].dtype}")
        print(f"  Missing: {df[col].isna().sum()}")
        print(f"  Unique: {df[col].nunique()}")
        if pd.api.types.is_numeric_dtype(df[col]):
            print(f"  Mean: {df[col].mean()}")
            print(f"  Std: {df[col].std()}")
            print(f"  Min: {df[col].min()}")
            print(f"  25%: {df[col].quantile(0.25)}")
            print(f"  50%: {df[col].median()}")
            print(f"  75%: {df[col].quantile(0.75)}")
            print(f"  Max: {df[col].max()}")
        else:
            print(f"  Top 5 values:\n{df[col].value_counts().head()}")

# Run and print the EDA summary
basic_eda(df)