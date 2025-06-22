{{ config(materialized='view', tags=['gold','status']) }}

-- ================================================================================
-- Gold View: customer_status_distribution
-- Business Question: What percentage of customers are active/suspended/inactive?
-- Description: Customer distribution by status with percentages
-- Source: customer_analytics
-- ================================================================================

SELECT
    customer_status,
    COUNT(*) AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_total
FROM {{ ref('customer_analytics') }}
GROUP BY customer_status
ORDER BY customers DESC 