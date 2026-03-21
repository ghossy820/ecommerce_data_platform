{{ config(materialized='table') }}

WITH staging_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT 
    customer_id,
    full_name,
    email_address,
    CAST(created_at AS DATE) AS registration_date
FROM staging_customers
WHERE is_valid_email = true