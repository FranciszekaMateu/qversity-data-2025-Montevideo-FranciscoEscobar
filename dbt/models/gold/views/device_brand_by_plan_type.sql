{{ config(materialized='view', tags=['gold','device','brand','plan']) }}

-- ================================================================================
-- Gold View: device_brand_by_plan_type
-- Business Question: What is device brand preference by plan type?
-- Description: Device brand popularity analysis by plan type
-- Source: customer_analytics
-- ================================================================================

SELECT
    plan_type,
    device_brand,
    COUNT(*) AS customers
FROM {{ ref('customer_analytics') }}
WHERE device_brand IS NOT NULL AND device_brand <> 'Unknown'
GROUP BY plan_type, device_brand
ORDER BY plan_type, customers DESC 