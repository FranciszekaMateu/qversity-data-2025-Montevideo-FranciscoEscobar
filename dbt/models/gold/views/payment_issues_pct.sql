{{ config(materialized='view', tags=['gold','payments']) }}

-- ================================================================================
-- Gold View: payment_issues_pct
-- Business Question: What percentage of customers have payment issues?
-- Description: Percentage of customers with payment issues
-- Source: customer_analytics
-- ================================================================================

SELECT
    ROUND(SUM(CASE WHEN has_payment_issues THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS pct_payment_issues
FROM {{ ref('customer_analytics') }} 