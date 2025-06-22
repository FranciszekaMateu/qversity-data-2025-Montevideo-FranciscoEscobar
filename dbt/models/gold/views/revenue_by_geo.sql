{{ config(materialized='view', tags=['gold','revenue','geo']) }}

-- ================================================================================
-- Gold View: revenue_by_geo
-- Business Question: What is the revenue distribution by geographic location?
-- Description: Revenue distribution by country and city
-- Source: customer_analytics
-- ================================================================================
SELECT
    country,
    city,
    SUM(total_revenue) AS total_revenue,
    COUNT(*)           AS customers,
    ROUND(AVG(arpu_estimated),2) AS avg_arpu
FROM {{ ref('customer_analytics') }}
GROUP BY country, city
ORDER BY total_revenue DESC 