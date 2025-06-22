-- ================================================================================
-- Silver Model: stg_payment_history
-- Description : Normalizes and expands the payment history embedded in the
--               raw_mobile_customers collection.

{{ config(
    materialized = 'table',
    tags = ['silver', 'payments'],
    post_hook = [
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_payments') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_payments PRIMARY KEY (payment_id); END IF; END $$",
        "CREATE INDEX IF NOT EXISTS idx_payments_customer_id ON {{ this }} (customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_payments_date        ON {{ this }} (payment_date)",
        "CREATE INDEX IF NOT EXISTS idx_payments_status      ON {{ this }} (payment_status)"
    ]
) }}

-- -------------------------------------------------------------------------------
-- 1. Raw data with basic filters
-- -------------------------------------------------------------------------------
WITH raw_data AS (
    SELECT 
        data,
        -- --------------------------------------------------
        -- Safe extraction of customer_id (must be numeric)
        -- --------------------------------------------------
        CASE 
            WHEN data ? 'customer_id' AND data->>'customer_id' IS NOT NULL AND data->>'customer_id' != 'null' THEN 
                CASE 
                    WHEN jsonb_typeof(data->'customer_id') = 'number' THEN CAST(data->>'customer_id' AS INTEGER)
                    WHEN jsonb_typeof(data->'customer_id') = 'string' AND data->>'customer_id' ~ '^[0-9]+$' THEN CAST(data->>'customer_id' AS INTEGER)
                    ELSE NULL
                END
            ELSE NULL
        END                                               AS customer_id,
        _loaded_at
    FROM {{ source('bronze_sources', 'raw_mobile_customers') }}
    WHERE data ? 'payment_history' 
      AND jsonb_typeof(data->'payment_history') = 'array'
      AND data ? 'customer_id'                             
      AND data->>'customer_id' IS NOT NULL 
      AND data->>'customer_id' != 'null'
      -- ensure record meets valid registration date rules
      AND (
            data->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}T' OR
            data->>'registration_date' ~ '^\d{2}-\d{2}-\d{4}$' OR
            data->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$'
          )
),

-- -------------------------------------------------------------------------------
-- 2. Deduplication 
-- -------------------------------------------------------------------------------
payment_data AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS row_num
        FROM raw_data
    ) ranked
    WHERE row_num = 1
      AND customer_id IS NOT NULL
),

-- -------------------------------------------------------------------------------
-- 3. Expansion of payment_history array
-- -------------------------------------------------------------------------------
payments_expanded AS (
    SELECT 
        pd.customer_id,
        pd._loaded_at,
        payment_item.value AS payment_data
    FROM payment_data pd,
         jsonb_array_elements(pd.data->'payment_history') AS payment_item(value)
    WHERE pd.customer_id IS NOT NULL
),

-- -------------------------------------------------------------------------------
-- 4. Cleaning of each individual payment
-- -------------------------------------------------------------------------------
cleaned_payments AS (
    SELECT
        -- --------------------------------------------------
        -- Unique payment identifier (hash if doesn't exist in source)
        -- --------------------------------------------------
        COALESCE(payment_data->>'payment_id', MD5(customer_id::TEXT || '_' || payment_data::TEXT)) AS payment_id,
        customer_id,

        -- --------------------------------------------------
        -- Payment date
        -- --------------------------------------------------
        CASE 
            WHEN payment_data->>'date' ~ '^\d{4}-\d{2}-\d{2}T' THEN CAST(payment_data->>'date' AS TIMESTAMP)
            WHEN payment_data->>'date' ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_TIMESTAMP(payment_data->>'date', 'MM-DD-YYYY')
            WHEN payment_data->>'date' ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(payment_data->>'date' AS DATE)::TIMESTAMP
            ELSE NULL
        END                                               AS payment_date,

        -- --------------------------------------------------
        -- Payment amount
        -- --------------------------------------------------
        CASE 
            WHEN payment_data->>'amount' IS NULL OR TRIM(payment_data->>'amount') = '' OR payment_data->>'amount' = 'unknown' THEN NULL
            WHEN payment_data->>'amount' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 
                CASE WHEN CAST(payment_data->>'amount' AS NUMERIC) >= 0 THEN CAST(payment_data->>'amount' AS NUMERIC) ELSE NULL END
            ELSE NULL
        END                                               AS payment_amount,

        -- --------------------------------------------------
        -- Payment status
        -- Mapping based on real dataset states: PAID, PENDING, FAILED, LATE
        -- --------------------------------------------------
        CASE 
            WHEN UPPER(TRIM(payment_data->>'status')) IN ('PAID', 'COMPLETED', 'SUCCESS') THEN 'COMPLETED'
            WHEN UPPER(TRIM(payment_data->>'status')) IN ('PENDING', 'PROCESSING')        THEN 'PENDING'
            WHEN UPPER(TRIM(payment_data->>'status')) IN ('FAILED', 'ERROR', 'DECLINED')  THEN 'FAILED'
            WHEN UPPER(TRIM(payment_data->>'status')) =  'LATE'                           THEN 'LATE'
            ELSE 'UNKNOWN'
        END                                               AS payment_status,
        _loaded_at
    FROM payments_expanded
    WHERE payment_data IS NOT NULL
),

-- -------------------------------------------------------------------------------
-- 5. Filtering of valid payments
-- -------------------------------------------------------------------------------
valid_payments AS (
    SELECT *
    FROM cleaned_payments
    WHERE payment_date   IS NOT NULL
      AND payment_amount IS NOT NULL
      AND payment_amount > 0
      AND customer_id    IS NOT NULL
)

-- -------------------------------------------------------------------------------
-- 6. Final selection
-- -------------------------------------------------------------------------------
SELECT
    payment_id,
    customer_id,
    payment_date,
    payment_amount,
    payment_status,
    _loaded_at
FROM valid_payments 