# Real Estate Data Automation Pipeline

This project automates the extraction, transformation, and loading (ETL) of real estate data from the Propstream database. It is designed to streamline the frequent retrieval of customized real estate property lists, making the data readily accessible and easy to analyze.

## Project Overview

- **Data Source:** Propstream real estate database.
- **Automation Goal:** Automate regular pulls of real estate property lists based on custom criteria.
- **Data Pipeline:** Extracts data, cleans and transforms it, and loads it efficiently into a PostgreSQL database.
- **Output:** Processed datasets exported to Google Sheets for easy collaboration and real-time viewing.

## Key Features

- **Dynamic Custom List Pulls:** Hardcoded or configurable SQL queries allow tailored data extraction.
- **Data Cleaning:** Includes standardization of column names, handling missing values, and type conversions.
- **Efficient Data Loading:** Uses PostgreSQL’s optimized bulk loading methods.
- **Google Sheets Integration:** Final datasets are formatted and pushed to Google Sheets with added usability features like Zillow property links.
- **Incremental and Batch Processing:** Supports loading either recent extracts or entire historical datasets.

## Directory Structure

- `/etl` — Core ETL modules for extract, transform, and load operations.
- `/analytics` — Scripts for data querying, exploratory data analysis, and reporting.
- `/model` — dbt (data build tool) project containing SQL models for transforming raw loaded data into curated analytics tables.
- `/config` — Configuration files and environment variables for database and other settings.

## Usage Workflow

1. Extract raw XLSX or Parquet data files from data sources.
2. Clean and transform datasets to ensure data quality and consistency.
3. Load cleaned data into the PostgreSQL database.
4. Run custom queries to generate target lists.
5. Export results to Google Sheets, including clickable property URLs.

## Long-Term Vision

Deliver a fully automated, end-to-end pipeline that empowers you to perform custom list pulls without manual data handling, providing seamless, up-to-date access to real estate insights via Google Sheets.
