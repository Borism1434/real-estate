# %%
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from etl.loader import run_query
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import time
from gspread_formatting import (
    Borders,
    Border,
    CellFormat,
    format_cell_range,
    Color,
    cellFormat,
    textFormat,
    numberFormat,
    BooleanCondition, 
    DataValidationRule,
    set_data_validation_for_cell_range
)

def add_column_right_border(worksheet, df, col_name, start_row=1, end_row=1000):
    """
    Adds a solid right border to a column titled col_name (by name, not index).
    """
    try:
        col_idx = df.columns.get_loc(col_name) + 1  # 1-based index
        # The range for the column (A1 notation)
        col_a1 = gspread.utils.rowcol_to_a1(start_row, col_idx)
        col_a1_end = gspread.utils.rowcol_to_a1(end_row, col_idx)
        rng = f"{col_a1}:{col_a1_end}"

        # Set only the right border
        border_style = Borders(
            right=Border("SOLID", Color(0, 0, 0))
        )
        fmt = CellFormat(borders=border_style)
        format_cell_range(worksheet, rng, fmt)
    except Exception as e:
        print(f"Could not add border after col {col_name}: {e}")

# Example usage after upload:
# for col in ["mls_amount", "address"]:
#     add_column_right_border(worksheet, df_final, col)

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
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64').map("{:,}".format)

    # Round score columns if present
    score_cols = ['total_score']
    for col in score_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(1)


    # Convert date/datetime to string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].apply(lambda x: isinstance(x, (datetime.date, pd.Timestamp))).any():
            df[col] = df[col].astype(str)

    return df

def export_and_process_data(query=None):
    default_query = """
    select
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
        "zip",
        "total_score",
        "price_per_sqft",
        "mls_amount",
        "est_value",
        "diff",
        "address",
        "building_sqft",
        "lot_size_sqft",
        "total_loan_balance",
        "last_sale_amount",
        "lien_amount",
        "est_equity_calc",
        "perc_price_inc",
        "total_open_loans",
        "listed_price_inc",
        "mls_days_on_market",
        "lot_coverage_ratio",
        "lot_size_per_building_sqft",
        "mls_date",
        "last_sale_date",
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
        "mls_brokerage_phone",
        "apn"
    ]
    existing_columns = [col for col in desired_order if col in df_linked.columns]
    df_final = df_linked[existing_columns]
    return df_final

def create_new_tab(sheet_title, creds_path, prefix="Export"):
    """
    Creates a new tab in the Google Sheet with a unique name based on timestamp.
    Adds basic formatting for headers and account/percent columns.
    Returns the new tab name.
    """
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_title)

    # Generate tab name with date and time for uniqueness
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
    tab_name = f"{prefix}_{timestamp}"
    tab_name = tab_name[:99]  # Sheets limit

    # Create the new sheet/tab
    worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="20")

    # ---- Formatting (after data uploaded) ----
    # We'll use a separate function after you upload the dataframe to format

    return tab_name


def add_checkbox_column(df):
    df = df.copy()
    df.insert(0, 'Interested', "")  # Use "" (empty string) for header so it doesn't get validated as checkbox
    return df


def format_tab(worksheet, df, currency_cols=None, percent_cols=None, int_cols=None, border_after_cols=None, add_checkboxes=False):
    """
    Apply bold header, currency, percent, and integer formatting to the worksheet.
    Add a right border after each column in border_after_cols.
    """
    from gspread_formatting import (
        cellFormat, textFormat, numberFormat, Borders, Border, Color,
        DataValidationRule, BooleanCondition, set_data_validation_for_cell_range, format_cell_range
    )
    
    # Bold all column names (header row 1)
    format_cell_range(worksheet, '1:1',
                      cellFormat(textFormat=textFormat(bold=True)))
    
    n_rows = len(df) + 1  # +1 for header row
    
    def col_range(col_name):
        col_idx = df.columns.get_loc(col_name) + 1
        # from row 2 to last data row
        start_a1 = gspread.utils.rowcol_to_a1(2, col_idx)
        end_a1 = gspread.utils.rowcol_to_a1(n_rows, col_idx)
        return f"{start_a1}:{end_a1}"
    
    # Format currency columns
    if currency_cols:
        for col in currency_cols:
            try:
                rng = col_range(col)
                format_cell_range(worksheet, rng,
                                  cellFormat(numberFormat=numberFormat(type='NUMBER', pattern='"$"#,##0')))
            except Exception as e:
                print(f"Error formatting currency col {col}: {e}")
    
    # Format percent columns
    if percent_cols:
        for col in percent_cols:
            try:
                rng = col_range(col)
                format_cell_range(worksheet, rng,
                                  cellFormat(numberFormat=numberFormat(type='PERCENT', pattern='0%')))
            except Exception as e:
                print(f"Error formatting percent col {col}: {e}")
    
    # Format integer columns
    if int_cols:
        for col in int_cols:
            try:
                rng = col_range(col)
                format_cell_range(worksheet, rng,
                                  cellFormat(numberFormat=numberFormat(type='NUMBER', pattern='#,##0')))
            except Exception as e:
                print(f"Error formatting integer col {col}: {e}")
    
    # Add right border after specified columns
    if border_after_cols:
        for col in border_after_cols:
            try:
                col_idx = df.columns.get_loc(col) + 1
                # from first row (header) to last data row
                range_a1 = f"{gspread.utils.rowcol_to_a1(1, col_idx)}:{gspread.utils.rowcol_to_a1(n_rows, col_idx)}"
                border_style = Borders(
                    right=Border("Double", Color(0, 0, 0), width=2)
                )
                fmt = cellFormat(borders=border_style)
                format_cell_range(worksheet, range_a1, fmt)
            except Exception as e:
                print(f"Error setting border after col {col}: {e}")
    
    # Add checkbox column at column A if requested
    if add_checkboxes:
        try:
            if len(df) > 0:
                if len(df) > 0:
                    n_rows = len(df) + 1  # Header + data rows
                    checkbox_range = f"A2:A{n_rows}"
                    # apply data validation...
                else:
                    print("No data rows, skipping checkbox formatting")
                rule = DataValidationRule(
                    BooleanCondition('BOOLEAN', []),
                    showCustomUi=True
                )
                set_data_validation_for_cell_range(worksheet, checkbox_range, rule)
            else:
                print("No rows to add checkboxes")
        except Exception as e:
            print(f"Error adding checkboxes: {e}")

# Example use:
# df_export = export_and_process_data()
