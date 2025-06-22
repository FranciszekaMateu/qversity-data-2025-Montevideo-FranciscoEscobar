{{ config(materialized='view', tags=['gold','device','brand']) }}

-- ================================================================================
-- Gold View: device_brand_by_country_operator
-- Business Question: What is device brand preference by country/operator?
-- Description: Device brand popularity analysis by country and operator
-- Source: customer_analytics
-- ================================================================================

SELECT
    country,
    operator,
    device_brand,
    COUNT(*) AS customers
FROM {{ ref('customer_analytics') }}
WHERE device_brand IS NOT NULL AND device_brand <> 'Unknown'
GROUP BY country, operator, device_brand
ORDER BY customers DESC 