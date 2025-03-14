{{
    config(
        materialized='table',
        schema="mart"
    )
}}
WITH product_revenue AS (
    SELECT 
        PRODUCT_ID,
        SUM(REVENUE) AS total_revenue
    FROM {{ ref('stg_event_booking_transactions') }}
    GROUP BY PRODUCT_ID
)
SELECT 
    PRODUCT_ID,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM product_revenue