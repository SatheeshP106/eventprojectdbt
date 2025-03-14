{{ config(
    materialized='table',
    schema='mart'
) }}

WITH customer_first_purchase AS (
    SELECT 
        CUSTOMER_ID,
        MIN(PAYMENT_MONTH) AS first_purchase_date
    FROM {{ ref('stg_event_booking_transactions') }}
    GROUP BY CUSTOMER_ID
),
customer_fiscal_year AS (
    SELECT 
        CUSTOMER_ID,
        TO_CHAR(first_purchase_date, 'YYYY') AS fiscal_year
    FROM customer_first_purchase
)
SELECT 
    fiscal_year,
    COUNT(DISTINCT CUSTOMER_ID) AS new_logos_count
FROM customer_fiscal_year
GROUP BY fiscal_year
ORDER BY fiscal_year ASC