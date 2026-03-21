{{ config(materialized='table') }}

WITH staging_products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT 
    product_id,
    product_name,
    category_name,
    unit_price
FROM staging_products