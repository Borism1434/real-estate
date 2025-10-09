{{ config(materialized='table') }}

-- Final export-ready properties from intermediate scoring logic.
-- Includes core fields and scoring metrics for downstream analysis.
-- Intended to be exported to Google Sheets or visualization layer.

SELECT
    total_score,
    address,
    zip, 
    mls_amount,
    est_value,
    diff,
    perc_price_inc,
    listed_price_inc,
    has_hoa,
    bedrooms,
    total_bathrooms,
    building_sqft,
    lot_size_sqft,
    round(price_per_sqft,1) as price_per_sqft,
    lot_coverage_ratio,
    round(lot_size_per_building_sqft, 1) as lot_size_per_building_sqft,
    mls_days_on_market,
    mls_date,
    last_sale_date,
    last_sale_amount,
    total_open_loans,
    total_loan_balance,
    lien_amount,
    est_equity_calc,
    round(ltv_calc, 2) as ltv_calc,
    effective_year_built,
    is_owner_occupied,
    owner_1_first_name,
    owner_1_last_name,
    is_vacant,
    total_condition,
    mls_agent_name,
    mls_agent_phone,
    mls_agent_email,
    mls_brokerage_name,
    mls_brokerage_phone

FROM {{ ref('int__strategy_single') }}
ORDER BY total_score DESC
