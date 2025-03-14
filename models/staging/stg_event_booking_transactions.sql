WITH transactions_raw AS (
    SELECT * FROM {{ source('staging', 'transactions') }}
),

cleaned_transactions AS (
    SELECT 
        companies::STRING AS company,
        {{ cast('customer_id', 'STRING') }} AS customer_id,   
        {{ cast('product_id', 'STRING') }} AS product_id,
        TRY_CAST(payment_month AS DATE) AS payment_month, 
        COALESCE(revenue, 0)::FLOAT AS revenue,
        COALESCE(revenue_type, 0)::INTEGER AS revenue_type,
        COALESCE(quantity, 0)::INTEGER AS quantity
    FROM transactions_raw
    WHERE customer_id IS NOT NULL AND product_id IS NOT NULL
)

SELECT * FROM cleaned_transactions