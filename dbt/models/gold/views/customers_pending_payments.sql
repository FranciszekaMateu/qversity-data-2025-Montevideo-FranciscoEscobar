{{ config(materialized='view', tags=['gold','payments']) }}

-- ================================================================================
-- Gold View: customers_pending_payments
-- Business Question: Which customers have pending payments?
-- Description: List of customers with pending payments and their details
-- Source: customer_analytics
-- ================================================================================

SELECT
    customer_id,
    plan_type,
    operator,
    country,
    city,
    total_revenue,
    arpu_estimated
FROM {{ ref('customer_analytics') }}
WHERE has_pending_payments
