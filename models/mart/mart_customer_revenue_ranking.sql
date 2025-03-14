{{
    config(
        materialized='table',
        schema="mart"
    )
}}
WITH customer_revenue AS (
    SELECT 
        CUSTOMER_ID,
        SUM(REVENUE) AS total_revenue
    FROM {{ ref('stg_event_booking_transactions') }}
    GROUP BY CUSTOMER_ID
)
SELECT 
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    CUSTOMER_ID,
    total_revenue
FROM customer_revenue