{{ config(materialized='view', tags=['gold','services']) }}

-- ================================================================================
-- Gold View: services_popularity
-- Business Question: Which services are most commonly contracted?
-- Description: Individual service popularity by number of customers
-- Source: Silver tables 
-- ================================================================================

SELECT
    s.service_code,
    COUNT(DISTINCT cs.customer_id) AS customers,
    ROUND(COUNT(DISTINCT cs.customer_id) * 100.0 / 
          (SELECT COUNT(DISTINCT customer_id) FROM {{ ref('customer_analytics') }}), 2) AS penetration_pct
FROM {{ ref('stg_customer_services') }} cs
JOIN {{ ref('stg_dim_services') }} s ON cs.service_id = s.service_id
GROUP BY s.service_code
ORDER BY customers DESC 