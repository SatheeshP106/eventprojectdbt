WITH products_raw AS (
    SELECT * FROM {{ source('staging', 'products') }}
),

cleaned_products AS (
    SELECT 
        product_id::STRING AS product_id,
        {{ trim_and_clean('PRODUCT_Family') }} AS product_family,
        {{ trim_and_clean('product_sub_family') }} AS product_sub_family
    FROM products_raw
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned_products