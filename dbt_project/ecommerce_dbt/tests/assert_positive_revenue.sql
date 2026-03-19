SELECT
    order_id,
    net_revenue_amount
FROM {{ ref('fact_sales') }}
WHERE net_revenue_amount < 0