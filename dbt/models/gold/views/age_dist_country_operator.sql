{{ config(materialized='view', tags=['gold','age','country_operator']) }}

-- ================================================================================
-- Gold View: age_dist_country_operator
-- Business Question: What is the age distribution by country and operator?
-- Description: Age distribution analysis by country and operator
-- Source: customer_analytics
-- ================================================================================

SELECT
    country,
    operator,
    age_bucket,
    COUNT(*) AS customers
FROM {{ ref('customer_analytics') }}
GROUP BY country, operator, age_bucket
ORDER BY country, operator, age_bucket 