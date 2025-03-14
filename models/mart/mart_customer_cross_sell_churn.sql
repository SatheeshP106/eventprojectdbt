{{
    config(
        materialized='table',
        schema="mart"
    )
}}
WITH transactions AS (
    SELECT CUSTOMER_ID, PRODUCT_ID, PAYMENT_MONTH
    FROM {{ ref('stg_event_booking_transactions') }}
),
cross_sell AS (
    SELECT CUSTOMER_ID, COUNT(DISTINCT PRODUCT_ID) AS cross_sell_count
    FROM transactions
    GROUP BY CUSTOMER_ID
),
churned_products AS (
    SELECT 
        CUSTOMER_ID, 
        PRODUCT_ID, 
        MAX(PAYMENT_MONTH) AS last_purchase
    FROM transactions
    GROUP BY CUSTOMER_ID, PRODUCT_ID
)
SELECT 
    cs.CUSTOMER_ID,
    cs.cross_sell_count,
    COUNT(DISTINCT cp.PRODUCT_ID) AS churned_products
FROM cross_sell cs
LEFT JOIN churned_products cp ON cs.CUSTOMER_ID = cp.CUSTOMER_ID
GROUP BY cs.CUSTOMER_ID, cs.cross_sell_count
