# %%
# config/paths.py

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

FILENAME_DATE_FORMAT = "%Y%m%d"
DEFAULT_EXTRACT_LABEL = "extract"
PARQUET_ENABLED = True
