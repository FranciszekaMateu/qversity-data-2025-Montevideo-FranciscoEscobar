-- ================================================================================
-- Silver Model: stg_customer_services  
-- Description : Many-to-many relationship table between customers and services
-- Purpose     : Normalize the customer-service relationship eliminating arrays
-- ================================================================================

{{ config(
    materialized='table',
    tags=['silver', 'customers', 'services', 'bridge'],
    post_hook=[
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_customer_services_new') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_customer_services_new PRIMARY KEY (customer_id, service_id); END IF; END $$",
        "CREATE INDEX IF NOT EXISTS idx_customer_services_customer_new ON {{ this }} (customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_customer_services_service_new ON {{ this }} (service_id)"
    ]
) }}

-- -------------------------------------------------------------------------------
-- 1. Read contracted_services DIRECTLY from Bronze layer (before cleaning)
-- -------------------------------------------------------------------------------
WITH raw_customer_services AS (
    SELECT 
        -- Extract customer_id the same way as in stg_mobile_customers_cleaned
        CASE 
            WHEN data ? 'customer_id' 
                 AND data->>'customer_id' IS NOT NULL 
                 AND data->>'customer_id' != 'null' THEN 
                CASE 
                    WHEN jsonb_typeof(data->'customer_id') = 'number' THEN CAST(data->>'customer_id' AS INTEGER)
                    WHEN jsonb_typeof(data->'customer_id') = 'string' 
                         AND data->>'customer_id' ~ '^[0-9]+$'        THEN CAST(data->>'customer_id' AS INTEGER)
                    ELSE 9000000 + ROW_NUMBER() OVER (ORDER BY _loaded_at)
                END
            ELSE 9000000 + ROW_NUMBER() OVER (ORDER BY _loaded_at)
        END AS customer_id,
        
        data->>'contracted_services' AS contracted_services,
        
        -- Other necessary fields
        CASE 
            WHEN data->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}T' THEN CAST(data->>'registration_date' AS TIMESTAMP)
            WHEN data->>'registration_date' ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_TIMESTAMP(data->>'registration_date', 'MM-DD-YYYY')
            WHEN data->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(data->>'registration_date' AS DATE)::TIMESTAMP
            ELSE NULL
        END AS registration_date,
        
        CASE 
            WHEN UPPER(TRIM(data->>'status')) IN ('ACTIVE', 'ACTIVO')        THEN 'ACTIVE'
            WHEN UPPER(TRIM(data->>'status')) IN ('INACTIVE', 'INACTIVO')    THEN 'INACTIVE'
            WHEN UPPER(TRIM(data->>'status')) IN ('SUSPENDED', 'SUSPENDIDO') THEN 'SUSPENDED'
            ELSE 'UNKNOWN'
        END AS customer_status,
        
        _loaded_at
    FROM {{ source('bronze_sources', 'raw_mobile_customers') }}
    WHERE data ? 'contracted_services'
      AND data->>'contracted_services' IS NOT NULL 
      AND data->>'contracted_services' != ''
      AND data->>'contracted_services' != '[]'
),

-- -------------------------------------------------------------------------------
-- 2. Parse contracted_services for each customer
-- -------------------------------------------------------------------------------
customer_services_parsed AS (
    SELECT 
        r.customer_id,
        r.registration_date,
        r.customer_status,
        r._loaded_at,
        -- Extract individual services from contracted_services field
        UPPER(TRIM(service_clean)) AS service_code
    FROM raw_customer_services r
    CROSS JOIN LATERAL (
        SELECT UNNEST(
            CASE 
                -- If it looks like JSON array: ["SERVICE1","SERVICE2"]
                WHEN r.contracted_services ~ '^\s*\[' 
                THEN ARRAY(SELECT jsonb_array_elements_text(r.contracted_services::jsonb))
                
                -- If it's delimited string: SERVICE1,SERVICE2  
                ELSE string_to_array(
                    regexp_replace(
                        regexp_replace(r.contracted_services, '[\[\]\"]', '', 'g'),
                        '\s+', '', 'g'
                    ), ','
                )
            END
        ) AS service_clean
    ) parsed
    WHERE TRIM(service_clean) <> ''
),

-- -------------------------------------------------------------------------------
-- 3. Join with dim_services to get service_id
-- -------------------------------------------------------------------------------
customer_services_with_ids AS (
    SELECT 
        csp.customer_id,
        s.service_id,
        csp.service_code,
        csp.registration_date,
        csp.customer_status,
        csp._loaded_at,
        -- Estimated contract date 
        COALESCE(csp.registration_date, CURRENT_TIMESTAMP) AS contracted_date,
        
        -- Service status based on customer status
        CASE 
            WHEN csp.customer_status = 'ACTIVE' THEN 'ACTIVE'
            WHEN csp.customer_status = 'INACTIVE' THEN 'INACTIVE'  
            WHEN csp.customer_status = 'SUSPENDED' THEN 'SUSPENDED'
            ELSE 'UNKNOWN'
        END AS service_status
        
    FROM customer_services_parsed csp
    INNER JOIN {{ ref('stg_dim_services') }} s 
        ON s.service_code = csp.service_code
),

-- -------------------------------------------------------------------------------
-- 4. Deduplication 
-- -------------------------------------------------------------------------------
deduplicated AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id, service_id 
                   ORDER BY _loaded_at DESC, contracted_date DESC
               ) AS row_num
        FROM customer_services_with_ids
    ) ranked
    WHERE row_num = 1
)

-- -------------------------------------------------------------------------------
-- 5. Final result 
-- -------------------------------------------------------------------------------
SELECT
    customer_id,        -- FK → stg_mobile_customers_cleaned.customer_id
    service_id,         -- FK → stg_dim_services.service_id
    service_status AS status,
    contracted_date,
    _loaded_at AS last_updated
FROM deduplicated
ORDER BY customer_id, service_id 