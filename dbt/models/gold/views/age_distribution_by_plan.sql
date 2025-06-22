{{ config(materialized='view', tags=['gold','age','plan']) }}

-- ================================================================================
-- Gold View: age_distribution_by_plan
-- Business Question: What is the age distribution of customers by plan type?
-- Description: Age distribution analysis by plan type
-- Source: customer_analytics
-- ================================================================================

SELECT
    plan_type,
    age_bucket,
    COUNT(*) AS customers
FROM {{ ref('customer_analytics') }}
GROUP BY plan_type, age_bucket
ORDER BY plan_type, age_bucket 