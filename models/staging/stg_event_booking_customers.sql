-- WITH customers_raw AS (
--     SELECT * FROM {{ source('staging', 'customers') }}
-- ),

-- cleaned_customers AS (
--     SELECT 
--         TRIM(LOWER(company)) AS company,
--         customer_id::STRING AS customer_id,
--         COALESCE(TRIM(customername), 'Unknown') AS customer_name
--     FROM customers_raw
--     WHERE customer_id IS NOT NULL
-- )

-- SELECT * FROM cleaned_customers


WITH customers_raw AS (
    SELECT * FROM {{ source('staging', 'customers') }}
),

cleaned_customers AS (
    SELECT 
        {{ trim_and_clean('company') }} AS company,
        customer_id::STRING AS customer_id,
        COALESCE(TRIM(customername), 'Unknown') AS customer_name
    FROM customers_raw
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned_customers