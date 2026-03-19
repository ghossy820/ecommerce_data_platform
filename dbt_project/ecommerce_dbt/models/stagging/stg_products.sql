{{ config(materialized='table') }}

WITH raw_products AS (
    SELECT * FROM read_parquet('s3://lakehouse/bronze/products/*/*/*/*.parquet')
),

cleaned_products AS (
    SELECT 
        CAST(product_id AS INTEGER) AS product_id,
        CAST(product_name AS VARCHAR) AS product_name,
        CAST(category_name AS VARCHAR) AS category_name,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price
    FROM raw_products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) = 1
)

SELECT * FROM cleaned_products