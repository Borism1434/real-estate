{{ config(materialized='table') }}

WITH deduped AS (
    SELECT DISTINCT ON (apn, address)
        *
    from {{ ref('stg__property_listings') }}
    ORDER BY apn, address, extract_date DESC
)

SELECT * FROM deduped