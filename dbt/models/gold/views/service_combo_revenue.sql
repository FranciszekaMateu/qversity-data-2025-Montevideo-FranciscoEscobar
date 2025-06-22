{{ config(materialized='view', tags=['gold','services','revenue']) }}

-- ================================================================================
-- Gold View: service_combo_revenue
-- Business Question: Which service combinations drive highest revenue?
-- Description: Revenue by service combinations
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
    LEFT JOIN {{ ref('customer_analytics') }} ca ON cs.customer_id = ca.customer_id
    GROUP BY cs.customer_id, ca.total_revenue, ca.arpu_estimated
)

SELECT
    service_combo,
    services_count,
    SUM(COALESCE(total_revenue, 0)) AS total_revenue,
    ROUND(AVG(COALESCE(arpu_estimated, 0)), 2) AS avg_arpu,
    COUNT(*) AS customers
FROM service_combos
GROUP BY service_combo, services_count
ORDER BY total_revenue DESC 