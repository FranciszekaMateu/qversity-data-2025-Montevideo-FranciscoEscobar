{{ config(materialized='view', tags=['gold','distribution','operator']) }}

-- ================================================================================
-- Gold View: customers_by_operator
-- Business Question: How are customers distributed across different operators?
-- Description: Customer distribution and ARPU by operator
-- Source: customer_analytics
-- ================================================================================

SELECT
    operator,
    COUNT(*)           AS customers,
    ROUND(AVG(arpu_estimated),2) AS avg_arpu
FROM {{ ref('customer_analytics') }}
GROUP BY operator
ORDER BY customers DESC
