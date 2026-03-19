{{ config(materialized='table') }}

WITH raw_customers AS (
    SELECT * FROM read_parquet('s3://lakehouse/bronze/customers/*/*/*/*.parquet')
),

cleaned_customers AS (
    SELECT 
        CAST(customer_id AS INTEGER) AS customer_id,
        CAST(full_name AS VARCHAR) AS full_name,
        CAST(email_address AS VARCHAR) AS email_address,
        CAST(created_at AS TIMESTAMP) AS created_at,
        
        CASE 
            WHEN email_address LIKE '%@%' THEN true 
            ELSE false 
        END AS is_valid_email
        
    FROM raw_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) = 1
)

SELECT * FROM cleaned_customers