{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    from {{ ref('stg__deduped_src') }}
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

base_w_scores AS (
    SELECT *,
    
    -- Price Score (Max 35 for SFR, Max 30 for MF)
    CASE 
        WHEN NOT is_multifamily_flag AND mls_amount <= 200000 THEN 35
        WHEN NOT is_multifamily_flag AND mls_amount <= 250000 THEN 30
        WHEN NOT is_multifamily_flag AND mls_amount <= 300000 THEN 20
        WHEN is_multifamily_flag AND mls_amount <= 300000 THEN 30
        WHEN is_multifamily_flag AND mls_amount <= 400000 THEN 20
        ELSE 0
    END AS price_score,

    -- Equity Score (Max 20)
    CASE 
        WHEN est_equity_calc >= 200000 THEN 20
        WHEN est_equity_calc >= 100000 THEN 15
        WHEN est_equity_calc >= 50000 THEN 10
        ELSE 0
    END AS equity_score,

    -- MLS Days Score (Max 15)
    CASE 
        WHEN mls_days_on_market > 180 THEN 15
        WHEN mls_days_on_market > 90 THEN 10
        WHEN mls_days_on_market > 30 THEN 5
        ELSE 0
    END AS mls_days_score,

    -- Under-assessed Bonus (Max 10)
    CASE WHEN under_assessed_flag THEN 10 ELSE 0 END AS under_assessed_bonus,

    -- Low Improvement Ratio Bonus (Max 5)
    CASE WHEN low_improvement_value_flag THEN 5 ELSE 0 END AS low_improvement_bonus,

    -- Long-held Property Bonus (Max 5)
    CASE WHEN long_held_flag THEN 5 ELSE 0 END AS long_held_bonus,

    -- HOA Penalty (Max -20)
    CASE WHEN has_hoa THEN -20 ELSE 0 END AS hoa_penalty,

    -- Total Score (Max theoretical total ~ 85)
    (
        -- Reusing the same logic for price score
        CASE 
            WHEN NOT is_multifamily_flag AND mls_amount <= 200000 THEN 35
            WHEN NOT is_multifamily_flag AND mls_amount <= 250000 THEN 30
            WHEN NOT is_multifamily_flag AND mls_amount <= 300000 THEN 20
            WHEN is_multifamily_flag AND mls_amount <= 300000 THEN 30
            WHEN is_multifamily_flag AND mls_amount <= 400000 THEN 20
            ELSE 0
        END
        +
        CASE 
            WHEN est_equity_calc >= 200000 THEN 20
            WHEN est_equity_calc >= 100000 THEN 15
            WHEN est_equity_calc >= 50000 THEN 10
            ELSE 0
        END
        +
        CASE 
            WHEN mls_days_on_market > 180 THEN 15
            WHEN mls_days_on_market > 90 THEN 10
            WHEN mls_days_on_market > 30 THEN 5
            ELSE 0
        END
        +
        CASE WHEN under_assessed_flag THEN 10 ELSE 0 END
        +
        CASE WHEN low_improvement_value_flag THEN 5 ELSE 0 END
        +
        CASE WHEN long_held_flag THEN 5 ELSE 0 END
        +
        CASE WHEN has_hoa THEN -20 ELSE 0 END
    ) AS total_score

    FROM base
    WHERE is_active_listing = TRUE 
    and is_multifamily_flag = TRUE
    and mls_date >= '2025-01-01' AND mls_date < '2026-01-01'
    order by total_score desc
)

SELECT *
FROM base_w_scores