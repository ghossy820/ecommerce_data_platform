{{ config(materialized='table') }}

WITH raw_orders AS (
    SELECT * FROM read_parquet('s3://lakehouse/bronze/sales_orders/*/*/*/*.parquet')
),

cleaned_orders AS (
    SELECT 
        CAST(order_id AS VARCHAR) AS order_id,
        CAST(customer_id AS INTEGER) AS customer_id,
        CAST(order_date AS TIMESTAMP) AS order_date,
        CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
        CAST(order_status AS VARCHAR) AS order_status,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        
        CASE 
            WHEN CAST(total_amount AS DECIMAL(10,2)) >= 0 THEN true 
            ELSE false 
        END AS is_valid_amount
        
    FROM raw_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
)

SELECT * FROM cleaned_orders