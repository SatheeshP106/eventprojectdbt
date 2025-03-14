
{{
    config(
        materialized='table',
        schema="mart"
    )
}}

SELECT 
    *
FROM staging.country_region