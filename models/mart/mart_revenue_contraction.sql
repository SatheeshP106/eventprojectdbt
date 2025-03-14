{{
    config(
        materialized='table',
        schema="mart"
    )
}}
WITH contraction_of_revenue AS (
    SELECT
        PAYMENT_MONTH,
        SUM(CASE WHEN REVENUE < 0 THEN REVENUE ELSE 0 END) AS revenue_lost
    FROM {{ ref('stg_event_booking_transactions') }}
    GROUP BY PAYMENT_MONTH
)
 
SELECT * FROM contraction_of_revenue