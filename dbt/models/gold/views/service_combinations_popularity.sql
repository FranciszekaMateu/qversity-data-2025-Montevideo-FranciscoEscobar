{{ config(materialized='view', tags=['gold','services','combination']) }}

-- ================================================================================
-- Gold View: service_combinations_popularity
-- Business Question: What service combinations are most popular?
-- Description: Service combination popularity
-- Source: Silver tables + customer_analytics 
-- ================================================================================

WITH service_combos AS (
    SELECT
        cs.customer_id,
        ca.total_revenue,
        ca.arpu_estimated,
        string_agg(s.service_code, ',' ORDER BY s.service_code) AS service_combo,
        COUNT(DISTINCT cs.service_id) AS services_count
    FROM {{ ref('stg_customer_services') }} cs
    JOIN {{ ref('stg_dim_services') }} s ON cs.service_id = s.service_id
    JOIN {{ ref('customer_analytics') }} ca ON cs.customer_id = ca.customer_id
    GROUP BY cs.customer_id, ca.total_revenue, ca.arpu_estimated
)

SELECT
    service_combo,
    services_count,
    COUNT(*) AS customers,
    SUM(COALESCE(total_revenue, 0)) AS total_revenue,
    ROUND(AVG(COALESCE(arpu_estimated, 0)), 2) AS avg_arpu,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_customers
FROM service_combos
GROUP BY service_combo, services_count
ORDER BY customers DESC 