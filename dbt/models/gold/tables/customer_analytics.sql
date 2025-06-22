{{
  config(
    materialized='table',
    tags=['gold', 'analytics', 'customers']
  )
}}

-- ================================================================================
-- Gold Layer: customer_analytics
-- Objetivo  : Proveer una tabla wide con dimensiones y métricas que permitan
--             responder las preguntas de negocio (incluye métricas de ARPU real)
-- Requisitos: 
--   * NORMALIZADO - Sin arreglos/arrays.
--   * Basado en modelos silver normalizados.
-- Origen    : stg_mobile_customers_cleaned  (
--             stg_payment_history           
--             stg_customer_services         
--             stg_dim_services              
-- ================================================================================

WITH customer_dim AS (
    SELECT
        customer_id,
        plan_type,
        operator,
        country,
        city,
        device_brand,
        device_model,
        monthly_bill_usd,   
        monthly_data_gb,
        credit_score,
        -- Bucketización de credit score
        CASE 
            WHEN credit_score IS NULL           THEN 'UNKNOWN'
            WHEN credit_score < 580             THEN 'POOR (<580)'
            WHEN credit_score BETWEEN 580 AND 669 THEN 'FAIR (580-669)'
            WHEN credit_score BETWEEN 670 AND 739 THEN 'GOOD (670-739)'
            WHEN credit_score BETWEEN 740 AND 799 THEN 'VERY GOOD (740-799)'
            ELSE 'EXCELLENT (800+)' 
        END AS credit_score_range,
        age,
        CASE 
            WHEN age IS NULL           THEN 'UNKNOWN'
            WHEN age < 18             THEN '0-17'
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+' 
        END AS age_bucket,
        registration_date,
        customer_status,
        _loaded_at
    FROM {{ ref('stg_mobile_customers_cleaned') }}
),

payment_agg AS (
    SELECT
        customer_id,
        COUNT(*) FILTER (WHERE payment_status = 'COMPLETED')               AS payments_count,
        SUM(CASE WHEN payment_status = 'COMPLETED' THEN payment_amount END) AS total_revenue,
        AVG(payment_amount) FILTER (WHERE payment_status = 'COMPLETED')    AS avg_payment_amount,
        MAX(payment_date)                     AS last_payment_date,
        MIN(payment_date)                     AS first_payment_date,
        BOOL_OR(payment_status IN ('FAILED','LATE'))        AS has_payment_issues,
        BOOL_OR(payment_status = 'PENDING')                       AS has_pending_payments
    FROM {{ ref('stg_payment_history') }}
    GROUP BY customer_id
),


services_agg AS (
    SELECT
        cs.customer_id,
        COUNT(DISTINCT cs.service_id) AS total_services_count,
        COUNT(DISTINCT cs.service_id) AS active_services_count
        
    FROM {{ ref('stg_customer_services') }} cs
    GROUP BY cs.customer_id
)

-- -------------------------------------------------------------------------------
-- JOIN principal
-- -------------------------------------------------------------------------------
SELECT
    c.*,
    p.total_revenue                          AS total_revenue,
    p.avg_payment_amount                     AS avg_payment_amount,
    p.payments_count,
    p.first_payment_date,
    p.last_payment_date,
    p.has_payment_issues,
    p.has_pending_payments,
    
    COALESCE(s.total_services_count, 0)      AS total_services_count,
    COALESCE(s.active_services_count, 0)     AS active_services_count,
    
    -- Meses de servicio
    (DATE_PART('month', AGE(CURRENT_DATE::date, registration_date::date)) +
     12 * DATE_PART('year', AGE(CURRENT_DATE::date, registration_date::date)))::INT AS months_of_service,
    
    -- ARPU estimado
    CASE 
        WHEN p.total_revenue IS NOT NULL AND p.total_revenue > 0 AND p.payments_count > 0 
             THEN p.total_revenue / p.payments_count                     
        WHEN c.monthly_bill_usd IS NOT NULL AND c.monthly_bill_usd > 0   
             THEN c.monthly_bill_usd                                      
        ELSE NULL
    END                                         AS arpu_estimated,
    
    CASE 
        WHEN (DATE_PART('month', AGE(CURRENT_DATE::date, registration_date::date)) +
              12 * DATE_PART('year', AGE(CURRENT_DATE::date, registration_date::date))) > 0
             THEN p.total_revenue / NULLIF((DATE_PART('month', AGE(CURRENT_DATE::date, registration_date::date)) +
              12 * DATE_PART('year', AGE(CURRENT_DATE::date, registration_date::date))),0)
        ELSE NULL
     END                                       AS arpu_real
    
FROM customer_dim c
LEFT JOIN payment_agg p USING (customer_id)
LEFT JOIN services_agg s USING (customer_id) 