{{ config(materialized='view', tags=['gold','time']) }}

-- ================================================================================
-- Gold View: new_customers_over_time
-- Business Question: How does the distribution of new customers change over time?
-- Description: Monthly new customer acquisition trends
-- Source: customer_analytics
-- ================================================================================

SELECT
    DATE_TRUNC('month', registration_date) AS month,
    COUNT(*)                                AS new_customers
FROM {{ ref('customer_analytics') }}
GROUP BY month
ORDER BY month 