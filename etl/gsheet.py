# %%
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from etl.loader import run_query
import pandas as pd
import datetime


def upload_df_to_gsheet(df, tab_name, creds_path, sheet_title, start_cell="A1"):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_title)
    worksheet = sheet.worksheet(tab_name)

    # Clear the sheet starting from A1, optional
    # worksheet.clear()

    # Upload with formulas being parsed correctly
    set_with_dataframe(worksheet, df, row=1, col=1, include_index=False, include_column_header=True, resize=True)


# %%
def add_zillow_link_column(df):
    def build_zillow_url(row):
        # Adjust based on your actual column names
        address = str(row.get("address", "")).strip().replace(" ", "-").replace(".", "")
        city = str(row.get("city", "Las Vegas")).strip().replace(" ", "-")
        state = str(row.get("state", "NV")).strip()
        zip_code = str(row.get("zip", "")).strip()
        components = [address, city, state, zip_code]
        url_slug = "-".join([c for c in components if c])  # drop empty parts
        return f'=HYPERLINK("https://www.zillow.com/homes/{url_slug}_rb/", "View")'

    df = df.copy()  # avoid modifying original df inplace
    df["Zillow Link"] = df.apply(build_zillow_url, axis=1)
    # Move "Zillow Link" to be the first column
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("Zillow Link")))
    df = df[cols]
    return df


def clean_export_dataframe(df):
    df = df.copy()

    # Format currency columns with commas
    for col in ['mls_amount', 'price_per_sqft', 'building_sqft']:
        if col in df.columns:
            df[col] = df[col].round().astype('Int64').map("{:,}".format)

    # Round score columns if present
    score_cols = ['total_score']
    for col in score_cols:
        if col in df.columns:
            df[col] = df[col].round(1)

    # Convert date/datetime to string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].apply(lambda x: isinstance(x, (datetime.date, pd.Timestamp))).any():
            df[col] = df[col].astype(str)

    return df

def export_and_process_data(query=None):
    default_query = """
    SELECT
    *
    FROM analytics.analytics_single_prop
    ORDER BY total_score DESC
    """
    df = run_query(query or default_query)
    df_cleaned = clean_export_dataframe(df)
    df_linked = add_zillow_link_column(df_cleaned)

    # --- New formatting code ---

    # Convert 'zip' to int without decimals (if exists)
    if 'zip' in df_linked.columns:
        df_linked['zip'] = pd.to_numeric(df_linked['zip'], errors='coerce').fillna(0).astype(int)

    # Convert all columns ending with '_date' to date (no time)
    date_cols = [col for col in df_linked.columns if col.endswith('_date')]
    for col in date_cols:
        df_linked[col] = pd.to_datetime(df_linked[col], errors='coerce').dt.date

    # Format improvement_to_tax_value as dollars with commas (if exists)
    if 'improvement_to_tax_value' in df_linked.columns:
        df_linked['improvement_to_tax_value'] = (
            pd.to_numeric(df_linked['improvement_to_tax_value'], errors='coerce')
            .fillna(0)
            .map("${:,.0f}".format)
        )

    # --- End formatting code ---

    desired_order = [
        "Zillow Link",
        "price_per_sqft",
        "mls_amount",
        "est_value",
        "diff",
        "perc_price_inc",
        "listed_price_inc",
        "total_score",
        "mls_days_on_market",
        "address",
        "building_sqft",
        "lot_size_sqft",
        "lot_coverage_ratio",
        "lot_size_per_building_sqft",
        "mls_date",
        "last_sale_date",
        "last_sale_amount",
        "total_open_loans",
        "total_loan_balance",
        "lien_amount",
        "est_equity_calc",
        "ltv_calc",
        "effective_year_built",
        "is_owner_occupied",
        "owner_1_first_name",
        "owner_1_last_name",
        "is_vacant",
        "total_condition",
        "mls_agent_name",
        "mls_agent_phone",
        "mls_agent_email",
        "mls_brokerage_name",
        "mls_brokerage_phone"
    ]
    existing_columns = [col for col in desired_order if col in df_linked.columns]
    df_final = df_linked[existing_columns]
    return df_final



# Example use:
# df_export = export_and_process_data()