{{ config(materialized='table') }}

WITH staging_orders AS (
    SELECT * FROM {{ ref('stg_sales_orders') }}
    WHERE is_valid_amount = true 
)

SELECT 
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS transaction_date,
    order_status,
    
    total_amount AS gross_revenue_amount,
    
    CASE 
        WHEN order_status = 'completed' THEN total_amount
        ELSE 0 
    END AS net_revenue_amount,
    
    CASE 
        WHEN order_status = 'refunded' THEN total_amount
        ELSE 0 
    END AS refunded_amount

FROM staging_orders