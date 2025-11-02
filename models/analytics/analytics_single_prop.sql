{{ config(materialized='table') }}

-- Final export-ready properties from intermediate scoring logic.
-- Includes core fields and scoring metrics for downstream analysis.
-- Intended to be exported to Google Sheets or visualization layer.

with all_props as (
select *
FROM {{ ref('int__strategy_single') }}
),

already_listed as (
    select *
    from {{ ref('stg__list_history') }}
)

select 
    p.total_score,
    p.address,
    p.zip, 
    p.mls_amount,
    p.est_value,
    p.diff,
    p.perc_price_inc,
    p.listed_price_inc,
    p.has_hoa,
    p.bedrooms,
    p.total_bathrooms,
    p.building_sqft,
    p.lot_size_sqft,
    round(p.price_per_sqft,1) as price_per_sqft,
    p.lot_coverage_ratio,
    round(p.lot_size_per_building_sqft, 1) as lot_size_per_building_sqft,
    p.mls_days_on_market,
    p.mls_date,
    p.last_sale_date,
    p.last_sale_amount,
    p.total_open_loans,
    p.total_loan_balance,
    p.lien_amount,
    p.est_equity_calc,
    round(p.ltv_calc, 2) as ltv_calc,
    p.effective_year_built,
    p.is_owner_occupied,
    p.owner_1_first_name,
    p.owner_1_last_name,
    p.is_vacant,
    p.total_condition,
    p.mls_agent_name,
    p.mls_agent_phone,
    p.mls_agent_email,
    p.mls_brokerage_name,
    p.mls_brokerage_phone,
    p.apn
from all_props p
left join already_listed a on p.apn = a.apn
where a.apn is null
ORDER BY total_score DESC

