{{ config(materialized='view', tags=['gold','time','operator']) }}

-- ================================================================================
-- Gold View: acquisition_trends_operator
-- Business Question: What are customer acquisition trends by operator?
-- Description: Customer acquisition trends by operator over time
-- Source: customer_analytics
-- ================================================================================

SELECT
    DATE_TRUNC('month', registration_date) AS month,
    operator,
    COUNT(*) AS new_customers
FROM {{ ref('customer_analytics') }}
GROUP BY month, operator
ORDER BY month, operator 