{{ config(materialized='table', tags=['silver', 'services', 'dimension'], post_hook=["DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_stg_dim_services') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_stg_dim_services PRIMARY KEY (service_id); END IF; END $$"]) }}

-- Normalized mobile services dimension
WITH base AS (
    SELECT * FROM (
        VALUES
            (1, 'VOICE'),
            (2, 'SMS'),
            (3, 'DATA'),
            (4, 'ROAMING'),
            (5, 'INTERNATIONAL')
    ) AS t(service_id, service_code)
)
SELECT
    service_id,
    service_code
FROM base 