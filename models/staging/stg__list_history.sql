{{ config(materialized='table') }}

select *
from stg.stg__list_history