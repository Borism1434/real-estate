{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    from {{ source('propstream', 'stg__deduped_src') }}
),

-- -- GENERAL SCORE (0–100)
-- base_score =
--   + price_score
--   + equity_score
--   + mls_days_score
--   + under_assessed_bonus
--   + long_held_bonus
--   - hoa_penalty
--   + distress_flags_bonus

base_with_scores AS (
    SELECT 
        *,
        -- Compute Price Score (Base - Penalties)
        GREATEST(
            -- Price Tier Bonus
            CASE
                WHEN mls_amount <= 150000 THEN 40
                WHEN mls_amount <= 200000 THEN 35
                WHEN mls_amount <= 250000 THEN 25
                WHEN mls_amount <= 300000 THEN 15
                WHEN mls_amount <= 400000 THEN 0
                WHEN mls_amount > 300000 AND mls_amount <= 400000 THEN -10
                ELSE 5
            END

            -- Bedroom Penalty
            - CASE 
                WHEN bedrooms = 1 THEN 40 
                WHEN bedrooms = 2 THEN 10 
                ELSE 0 
            END

            -- Absolute Size Penalty
            - CASE 
                WHEN building_sqft IS NOT NULL AND building_sqft < 1200 THEN 25
                ELSE 0
            END

            -- New: Enhanced Price Per Sqft Influence
            + CASE 
                WHEN price_per_sqft IS NULL THEN 0
                WHEN price_per_sqft <= 100 THEN 60
                WHEN price_per_sqft <= 125 THEN 50
                WHEN price_per_sqft <= 150 THEN 40
                WHEN price_per_sqft <= 175 THEN 30
                WHEN price_per_sqft <= 200 THEN 20
                WHEN price_per_sqft <= 225 THEN -10
                WHEN price_per_sqft <= 250 THEN -20
                ELSE -30
            END,

            0
        ) AS price_score,

        -- Equity Score
        CASE 
            WHEN est_equity_calc >= 200000 THEN 30
            WHEN est_equity_calc >= 100000 THEN 20
            WHEN est_equity_calc >= 50000 THEN 10
            ELSE 0
        END AS equity_score,

        -- MLS Days Score
        CASE 
            WHEN mls_days_on_market > 180 THEN 20
            WHEN mls_days_on_market > 90 THEN 15
            WHEN mls_days_on_market > 30 THEN 10
            ELSE 0
        END AS mls_days_score,

        CASE 
            WHEN mls_days_on_market <= 2 THEN 25
            WHEN mls_days_on_market <= 7 THEN 10
            WHEN mls_days_on_market <= 14 THEN 5
        ELSE 0
        END AS mls_fresh_bonus,

        -- Bonuses and Penalties
        CASE WHEN under_assessed_flag THEN 10 ELSE 0 END AS under_assessed_bonus,
        CASE WHEN low_improvement_value_flag THEN 10 ELSE 0 END AS low_improvement_bonus,
        CASE WHEN long_held_flag THEN 10 ELSE 0 END AS long_held_bonus,
        CASE WHEN has_hoa THEN -50 ELSE 0 END AS hoa_penalty

    FROM base
    WHERE 
        is_active_listing = TRUE 
        AND is_multifamily_flag = FALSE
        AND mls_date >= '2025-01-01' AND mls_date < '2026-01-01'
        
),

-- Final CTE to calculate total_score using price_score from above
scored_listings AS (
    SELECT *,
        (
            price_score +
            equity_score +
            mls_days_score +
            hoa_penalty

        ) AS total_score
    FROM base_with_scores
    where property_type NOT IN (
    'Condominium (Residential)',
    'Quadruplex (4 units, any combination)',
    'Commercial (General)',
    'Vacant Land (General)',
    'Duplex (2 units, any combination)',
    'Mobile home',
    'Townhouse (Residential)'
)
)

-- Final output
SELECT *
FROM scored_listings
ORDER BY total_score DESC
