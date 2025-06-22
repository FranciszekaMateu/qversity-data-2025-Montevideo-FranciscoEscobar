{{ config(materialized='view', tags=['gold','distribution','geo']) }}

-- ================================================================================
-- Gold View: customers_by_geo
-- Business Question: What is the distribution of customers by location?
-- Description: Customer distribution by country and city
-- Source: customer_analytics
-- ================================================================================

SELECT
    country,
    city,
    COUNT(*) AS customers
FROM {{ ref('customer_analytics') }}
GROUP BY country, city
ORDER BY customers DESC
