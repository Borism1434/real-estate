-- models/prep/prep__property_leads.sql
{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('propstream', 'prop_extract') }}
),

base_with_metrics as (
    select
        *,
        -- 🧮 Derived Financial Metrics
        coalesce(loan_1_balance, 0) + coalesce(loan_2_balance, 0) + coalesce(loan_3_balance, 0) + coalesce(loan_4_balance, 0) as total_loan_balance,

        COALESCE(mls_amount, 0) - COALESCE(est_value, 0) AS diff,

        mls_amount - last_sale_amount AS listed_price_inc,
        CASE 
            WHEN last_sale_amount IS NOT NULL AND last_sale_amount > 0 THEN 
                ROUND((mls_amount - last_sale_amount) * 1.0 / last_sale_amount, 2)
            ELSE NULL
        END AS perc_price_inc,

        -- Equity = estimated value - total loans
        est_value - (
            coalesce(loan_1_balance, 0) + coalesce(loan_2_balance, 0) + coalesce(loan_3_balance, 0) + coalesce(loan_4_balance, 0)
        ) as est_equity_calc,

        -- LTV = total loans / estimated value
        case 
            when est_value is not null and est_value > 0 then 
                100.0 * (
                    coalesce(loan_1_balance, 0) + coalesce(loan_2_balance, 0) + coalesce(loan_3_balance, 0)  + coalesce(loan_4_balance, 0)
                ) / est_value
            else null
        end as ltv_calc,

        -- 🏗️ Land Use Metrics
        case
            when lot_size_sqft is not null and lot_size_sqft > 0 and building_sqft is not null and building_sqft > 0
                then building_sqft * 1.0 / lot_size_sqft
            else null
        end as lot_coverage_ratio,
        
        mls_amount / NULLIF(building_sqft, 0) AS price_per_sqft,

        case 
        when mls_date is null then null
        when mls_date > current_date then 0
            else current_date - mls_date
        end as mls_days_on_market,

        case
            when building_sqft is not null and building_sqft > 0 and lot_size_sqft is not null
                then lot_size_sqft * 1.0 / building_sqft
            else null
        end as lot_size_per_building_sqft
    from source
),

flags as (
    select
        *,
        -- 🏁 Lead Flags
        lower(vacant) in ('yes', 'true') as is_vacant,
        lower(owner_occupied) in ('yes', 'true') as is_owner_occupied,
        lower(hoa_present) in ('yes', 'true')  as has_hoa,
        total_loan_balance > 0 as has_mortgage,
        total_open_loans > 1 as has_multiple_loans,
        est_equity_calc >= 100000 as high_equity_flag,
        est_equity_calc between 0 and 60000 as low_equity_flag,
        lower(mailing_state) != lower(state) as out_of_state_owner,
        last_sale_date >= current_date - interval '5 years' as recent_sale_flag,
        last_sale_date is null or last_sale_date < current_date - interval '10 years' as no_recent_sale_flag,
        ltv_calc is not null and ltv_calc <= 50 as low_ltv_flag,

        
        mls_days_on_market < 10 as new_listing_flag,
        mls_days_on_market between 11 and 60 as mid_term_listing_flag,
        mls_days_on_market > 60 as stale_listing_flag,
        lower(mls_status) = 'active' AS is_active_listing, 
        
        
                
        lower(property_type) in (
            'duplex',
            'triplex',
            'quadruplex',
            '2 units',
            '3 units',
            '4 units',
            'duplex (2 units, any combination)',
            'triplex (3 units, any combination)',
            'quadruplex (4 units, any combination)'
            ) as is_multifamily_flag,

        -- LTV Tiering
        case
            when ltv_calc is null then 'unknown'
            when ltv_calc <= 30 then 'very_low'
            when ltv_calc <= 50 then 'low'
            when ltv_calc <= 70 then 'medium'
            else 'high'
        end as ltv_tier,

        -- Lot Utilization 
        lot_coverage_ratio is not null and lot_coverage_ratio < 0.15 as lot_coverage_low_flag,
        lot_coverage_ratio is not null and lot_coverage_ratio > 0.75 as lot_coverage_high_flag,
        lot_size_per_building_sqft is not null and lot_size_per_building_sqft > 10 as lot_efficiency_flag,

        -- property valuation flag
        (est_value - total_assessed_value) > 100000 as under_assessed_flag,
        assessed_improvement_value / nullif(total_assessed_value, 0) < 0.5 as low_improvement_value_flag,
        -- Continuous value: years since last sale
        DATE_PART('year', current_date) - DATE_PART('year', last_sale_date) AS years_since_last_sale,

        -- Long-held flag: held 15+ years
        last_sale_date IS NOT NULL 
        AND last_sale_date < current_date - INTERVAL '15 years' AS long_held_flag,

        -- Short-held flag: held less than 4 years
        last_sale_date IS NOT NULL 
        AND last_sale_date >= current_date - INTERVAL '4 years' AS short_held_flag,
        
        case
            when est_value is not null and est_value > 0 then
                total_assessed_value / est_value
            else null
        end as assessed_ratio,

        case 
            when est_value > 0 then 
                GREATEST((est_value - total_assessed_value) / est_value, 0)
            else null
        end as under_assessed_score,

        CASE 
            WHEN last_sale_date > CURRENT_DATE - INTERVAL '12 months' AND mls_amount > last_sale_amount * 1.3 THEN 1
            ELSE 0
        END AS flipped_flag

        

    from base_with_metrics
)

select * 
from flags
