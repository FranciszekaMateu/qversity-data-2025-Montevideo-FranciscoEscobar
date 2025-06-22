{{ config(materialized='view', tags=['gold','device','brand']) }}

-- ================================================================================
-- Gold View: device_brand_popularity
-- Business Question: What are the most popular device brands?
-- Description: Overall device brand popularity ranking
-- Source: customer_analytics
-- ================================================================================

SELECT
    device_brand,
    COUNT(*) AS customers
FROM {{ ref('customer_analytics') }}
WHERE device_brand IS NOT NULL AND device_brand <> 'Unknown'
GROUP BY device_brand
ORDER BY customers DESC
