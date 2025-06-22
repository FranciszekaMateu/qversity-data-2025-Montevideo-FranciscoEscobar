-- ================================================================================
-- Silver Model: stg_mobile_customers_cleaned
-- Description : Cleans, normalizes and deduplicates customer data 
{{ config(
    materialized = 'table',
    tags = ['silver', 'customers'],
    post_hook = [
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_customers') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id); END IF; END $$",
        "CREATE INDEX IF NOT EXISTS idx_customers_email          ON {{ this }} (email)",
        "CREATE INDEX IF NOT EXISTS idx_customers_country_city   ON {{ this }} (country, city)",
        "CREATE INDEX IF NOT EXISTS idx_customers_operator       ON {{ this }} (operator)"
    ]
) }}

-- -------------------------------------------------------------------------------
-- 1. Raw data
-- -------------------------------------------------------------------------------
WITH raw_data AS (
    SELECT data, _loaded_at
    FROM {{ source('bronze_sources', 'raw_mobile_customers') }}
),

-- -------------------------------------------------------------------------------
-- 2. Cleaning and transformations with dbt macros
-- -------------------------------------------------------------------------------
cleaned_data AS (
    SELECT
        -- --------------------------------------------------
        -- Unique customer identifier
        -- --------------------------------------------------
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
        END                                                      AS customer_id,

        -- --------------------------------------------------
        -- Basic personal fields
        -- --------------------------------------------------
        TRIM(data->>'first_name')                                 AS first_name,
        TRIM(data->>'last_name')                                  AS last_name,
        LOWER(TRIM(data->>'email'))                               AS email,
        {{ add_country_code_to_phone(
            'data->>\'phone_number\'',
            correct_country_by_city(
                clean_country_names('data->>\'country\''),
                clean_cities('data->>\'city\'')
            )
        ) }}                                                     AS phone_number,

        -- --------------------------------------------------
        -- Age (0-120)
        -- --------------------------------------------------
        CASE 
            WHEN data->>'age' IS NULL OR TRIM(data->>'age') = ''                           THEN NULL
            WHEN data->>'age' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
                CASE 
                    WHEN CAST(ROUND(CAST(data->>'age' AS NUMERIC)) AS INTEGER) BETWEEN 0 AND 120
                    THEN CAST(ROUND(CAST(data->>'age' AS NUMERIC)) AS INTEGER)
                    ELSE NULL
                END
            ELSE NULL
        END                                                      AS age,

        -- --------------------------------------------------
        -- Normalized country and city
        -- --------------------------------------------------
        {{ clean_country_names('data->>\'country\'') }}         AS country_original,
        {{ correct_country_by_city(
            clean_country_names('data->>\'country\''),
            clean_cities('data->>\'city\'')
        ) }}                                                     AS country,
        {{ clean_cities('data->>\'city\'') }}                   AS city,

        -- --------------------------------------------------
        -- Operator and plan
        -- --------------------------------------------------
        {{ clean_operators('data->>\'operator\'') }}            AS operator,
        {{ clean_plan_types('data->>\'plan_type\'') }}          AS plan_type,

        -- --------------------------------------------------
        -- Consumption and billing
        -- --------------------------------------------------
        CASE 
            WHEN data->>'monthly_data_gb' IS NULL OR TRIM(data->>'monthly_data_gb') = ''         THEN NULL
            WHEN data->>'monthly_data_gb' ~ '^[0-9]+(\.[0-9]+)?$' 
                 AND CAST(data->>'monthly_data_gb' AS NUMERIC) >= 0 THEN CAST(data->>'monthly_data_gb' AS NUMERIC)
            ELSE NULL
        END                                                      AS monthly_data_gb,
        CASE 
            WHEN data->>'monthly_bill_usd' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(data->>'monthly_bill_usd' AS NUMERIC)
            ELSE NULL
        END                                                      AS monthly_bill_usd,

        -- --------------------------------------------------
        -- Device
        -- --------------------------------------------------
        {{ clean_device_brands('data->>\'device_brand\'') }}    AS device_brand,
        {{ clean_device_models('data->>\'device_model\'') }}    AS device_model,

        -- --------------------------------------------------
        -- Registration and last payment dates
        -- --------------------------------------------------
        CASE 
            WHEN data->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}T' THEN CAST(data->>'registration_date' AS TIMESTAMP)
            WHEN data->>'registration_date' ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_TIMESTAMP(data->>'registration_date', 'MM-DD-YYYY')
            WHEN data->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(data->>'registration_date' AS DATE)::TIMESTAMP
            ELSE NULL
        END                                                      AS registration_date,
        CASE 
            WHEN data->>'last_payment_date' ~ '^\d{4}-\d{2}-\d{2}T' THEN CAST(data->>'last_payment_date' AS TIMESTAMP)
            WHEN data->>'last_payment_date' ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_TIMESTAMP(data->>'last_payment_date', 'MM-DD-YYYY')
            WHEN data->>'last_payment_date' ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(data->>'last_payment_date' AS DATE)::TIMESTAMP
            ELSE NULL
        END                                                      AS last_payment_date,

        -- --------------------------------------------------
        -- Customer status
        -- --------------------------------------------------
        CASE 
            WHEN UPPER(TRIM(data->>'status')) IN ('ACTIVE', 'ACTIVO')        THEN 'ACTIVE'
            WHEN UPPER(TRIM(data->>'status')) IN ('INACTIVE', 'INACTIVO')    THEN 'INACTIVE'
            WHEN UPPER(TRIM(data->>'status')) IN ('SUSPENDED', 'SUSPENDIDO') THEN 'SUSPENDED'
            ELSE 'UNKNOWN'
        END                                                      AS customer_status,

        -- --------------------------------------------------
        -- Additional fields and metadata 
        -- --------------------------------------------------
        CAST(data->>'credit_limit' AS NUMERIC)                   AS credit_limit,
        CASE 
            WHEN data->>'credit_score' IS NULL OR TRIM(data->>'credit_score') = '' THEN NULL
            WHEN data->>'credit_score' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
                CASE 
                    WHEN CAST(ROUND(CAST(data->>'credit_score' AS NUMERIC)) AS INTEGER) >= 0
                    THEN CAST(ROUND(CAST(data->>'credit_score' AS NUMERIC)) AS INTEGER)
                    ELSE NULL
                END
            ELSE NULL
        END                                                      AS credit_score,
        CAST(data->>'data_usage_current_month' AS NUMERIC)       AS data_usage_current_month,
        CAST(data->>'latitude' AS NUMERIC)                       AS latitude,
        CAST(data->>'longitude' AS NUMERIC)                      AS longitude,
        data->>'record_uuid'                                     AS record_uuid,
        data->'payment_history'                                  AS payment_history,
        _loaded_at
    FROM raw_data
),

-- -------------------------------------------------------------------------------
-- 3. Deduplication
-- -------------------------------------------------------------------------------
deduplicated_data AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS row_num
        FROM cleaned_data
    ) ranked
    WHERE row_num = 1
      AND customer_id IS NOT NULL
      AND registration_date IS NOT NULL
)

-- -------------------------------------------------------------------------------
-- 4. Final selection 
-- -------------------------------------------------------------------------------
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    age,
    country_original,
    country,
    city,
    latitude,
    longitude,
    operator,
    plan_type,
    monthly_data_gb,
    monthly_bill_usd,
    device_brand,
    device_model,
    registration_date,
    last_payment_date,
    customer_status,
    credit_limit,
    credit_score,
    data_usage_current_month,
    record_uuid,
    _loaded_at
FROM deduplicated_data 