{{ config(materialized='view', tags=['gold','credit_score']) }}

-- ================================================================================
-- Gold View: credit_score_distribution
-- Business Question: What is customer segmentation by credit score ranges?
-- Description: Customer distribution and ARPU by credit score ranges
-- Source: customer_analytics
-- ================================================================================

SELECT
    credit_score_range,
    COUNT(*) AS customers,
    ROUND(AVG(arpu_estimated),2) AS avg_arpu
FROM {{ ref('customer_analytics') }}
GROUP BY credit_score_range
ORDER BY customers DESC 