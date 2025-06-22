{{ config(materialized='view', tags=['gold','kpi','arpu']) }}

-- ================================================================================
-- Gold View: arpu_by_plan_type
-- Business Question: What is the average revenue per user (ARPU) by plan type?
-- Description: ARPU analysis by plan type
-- Source: customer_analytics
-- ================================================================================

SELECT
    plan_type,
    ROUND(AVG(arpu_estimated), 2) AS arpu
FROM {{ ref('customer_analytics') }}
WHERE arpu_estimated IS NOT NULL
GROUP BY plan_type
ORDER BY arpu DESC 