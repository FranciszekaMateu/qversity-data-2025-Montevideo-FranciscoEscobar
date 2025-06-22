-- ================================================================================
-- Gold View: revenue_stats_plan_operator
-- Business Question: How do the mean and median monthly revenues per user compare across different plan types and operators?
-- Description: Revenue statistics (mean, median, mode) by plan type and operator
-- Source: customer_analytics 
-- ================================================================================

{{ config(materialized='view', tags=['gold','revenue','stats']) }}

WITH base AS (
    SELECT plan_type, operator, arpu_estimated AS revenue
    FROM {{ ref('customer_analytics') }}
    WHERE arpu_estimated IS NOT NULL
)

SELECT
    plan_type,
    operator,
    ROUND(AVG(revenue),2)             AS mean_revenue,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) AS median_revenue,
    MODE() WITHIN GROUP (ORDER BY revenue)               AS mode_revenue,
    COUNT(*)                             AS customers
FROM base
GROUP BY plan_type, operator
ORDER BY mean_revenue DESC 