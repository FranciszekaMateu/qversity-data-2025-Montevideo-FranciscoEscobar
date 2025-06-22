{{ config(materialized='view', tags=['gold','credit_score','payments']) }}

-- ================================================================================
-- Gold View: credit_score_vs_payment_behavior
-- Business Question: How does credit score correlate with payment behavior?
-- Description: Payment status distribution by credit score range.
-- Shows the percentage of COMPLETED, FAILED, PENDING and LATE payments by range.
-- ================================================================================

WITH payments_enriched AS (
    SELECT
        ca.credit_score_range,
        p.payment_status
    FROM {{ ref('stg_payment_history') }} p
    JOIN {{ ref('customer_analytics') }} ca
      ON p.customer_id = ca.customer_id
)

SELECT
    credit_score_range,
    COUNT(*)                                         AS total_payments,
    ROUND(100.0 * SUM(CASE WHEN payment_status = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_completed,
    ROUND(100.0 * SUM(CASE WHEN payment_status = 'FAILED'    THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_failed,
    ROUND(100.0 * SUM(CASE WHEN payment_status = 'PENDING'   THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_pending,
    ROUND(100.0 * SUM(CASE WHEN payment_status = 'LATE'      THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_late
FROM payments_enriched
GROUP BY credit_score_range
ORDER BY credit_score_range 