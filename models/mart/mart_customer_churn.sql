{{ config(
    materialized='table',
    schema='mart'
) }}
 
WITH customer_transactions AS (
    SELECT DISTINCT CUSTOMER_ID, PAYMENT_MONTH
    FROM {{ ref('stg_event_booking_transactions') }}
),
 
customer_activity_period AS (
    SELECT
        CUSTOMER_ID,
        MIN(PAYMENT_MONTH) AS first_purchase_month,
        MAX(PAYMENT_MONTH) AS last_purchase_month
    FROM customer_transactions
    GROUP BY CUSTOMER_ID
),
 
customer_churn_metrics AS (
    SELECT
        PAYMENT_MONTH,
        COUNT(DISTINCT CASE WHEN first_purchase_month = PAYMENT_MONTH THEN CUSTOMER_ID END) AS new_customers,
        COUNT(DISTINCT CASE WHEN last_purchase_month = PAYMENT_MONTH THEN CUSTOMER_ID END) AS churned_customers
    FROM customer_transactions
    JOIN customer_activity_period USING (CUSTOMER_ID)
    GROUP BY PAYMENT_MONTH
)
 
SELECT * FROM customer_churn_metrics